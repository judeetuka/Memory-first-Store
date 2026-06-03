use crate::object_store::{
    MfsMutableObjectStore, MutableObjectExpiryMeta, MutableObjectTieringRecord, ObjectStoreError,
};
use mfs_core::durability::{WalBackend, WalCodec, WalConfig};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use mfs_db::value::{MfsValue, MfsValueCodec};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_FILE: &str = "MANIFEST";
const WAL_DIR: &str = "wal";
const CHECKPOINT_DIR: &str = "checkpoints";
const TMP_DIR: &str = "tmp";
const TIER_DIR: &str = "tiers";
const COLD_DIR: &str = "cold";
const ACTIVE_WAL_FILE: &str = "active.mfswal";
const COLD_DATA_FILE: &str = "data.mfsobj";
const COLD_INDEX_FILE: &str = "index.mfsidx";
const COLD_MANIFEST_FILE: &str = "MANIFEST";
const COLD_GENERATION_PREFIX: &str = "generation-";
const COLD_GENERATION_DATA_SUFFIX: &str = ".mfsobj";
const COLD_GENERATION_INDEX_SUFFIX: &str = ".mfsidx";
const MANIFEST_BACKUP_FILE: &str = "MANIFEST.bak";
const CHECKPOINT_EXTENSION: &str = "mfssnap";
const CHECKPOINT_PREFIX: &str = "object-";
const CHECKPOINT_FOOTER_MAGIC: &[u8; 8] = b"MFSOCFTR";
const CHECKPOINT_FOOTER_LEN: usize = 8 + 8 + 8 + 8;
const META_KEY_PREFIX: &[u8] = b"\0mfs:object-meta:v1\0";
const COLD_DATA_MAGIC: &[u8; 8] = b"MFSOCLD1";
const COLD_INDEX_MAGIC: &[u8; 8] = b"MFSOIDX1";
const COLD_MANIFEST_MAGIC: &[u8; 8] = b"MFSCMNF1";
const COLD_MANIFEST_VERSION: u32 = 1;

type ObjectFlushRecord = FlushRecord<Vec<u8>, MfsValue>;
type CheckpointRecords = Vec<ObjectFlushRecord>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutableObjectStoreBundleOptions {
    pub keep_last_checkpoints: usize,
}

impl Default for MutableObjectStoreBundleOptions {
    fn default() -> Self {
        Self {
            keep_last_checkpoints: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MutableObjectStoreBundle {
    root: PathBuf,
    options: MutableObjectStoreBundleOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableObjectCheckpoint {
    pub path: PathBuf,
    pub lsn: u64,
    pub records: usize,
}

pub struct MutableObjectRecovery {
    pub store: MfsMutableObjectStore,
    pub checkpoint: Option<MutableObjectCheckpoint>,
    pub wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableObjectColdTier {
    pub data_path: PathBuf,
    pub index_path: PathBuf,
    pub records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableObjectColdGeneration {
    pub id: u64,
    pub records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableObjectColdTombstone {
    pub key: Vec<u8>,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableObjectColdManifest {
    pub generations: Vec<MutableObjectColdGeneration>,
    pub tombstones: Vec<MutableObjectColdTombstone>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MutableObjectColdGcReport {
    pub generations_scanned: usize,
    pub records_scanned: usize,
    pub records_kept: usize,
    pub records_dropped_expired: usize,
    pub generations_removed: usize,
    pub bytes_freed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TieringPolicy {
    pub idle_threshold_ticks: u64,
    pub max_records: usize,
    pub hot_capacity_soft_limit: Option<usize>,
    pub min_clean_age_ticks: u64,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            idle_threshold_ticks: 1024,
            max_records: 128,
            hot_capacity_soft_limit: None,
            min_clean_age_ticks: 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MutableObjectTieringReport {
    pub attempted: usize,
    pub demoted: usize,
    pub skipped_dirty: usize,
    pub skipped_recent: usize,
    pub skipped_capacity: usize,
    pub skipped_empty: usize,
    pub flush_records: usize,
}

pub struct MutableObjectStorePersistence {
    bundle: MutableObjectStoreBundle,
    wal: Option<WalBackend<Vec<u8>, MfsValue, MfsValueCodec>>,
}

impl MutableObjectStorePersistence {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_options(path, MutableObjectStoreBundleOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: MutableObjectStoreBundleOptions,
    ) -> io::Result<Self> {
        let bundle = MutableObjectStoreBundle::open_with_options(path, options)?;
        let wal = bundle.open_wal()?;
        Ok(Self {
            bundle,
            wal: Some(wal),
        })
    }

    pub fn bundle(&self) -> &MutableObjectStoreBundle {
        &self.bundle
    }

    pub fn flush_idle(
        &mut self,
        store: &MfsMutableObjectStore,
        idle_ticks: u64,
        max_records: usize,
    ) -> io::Result<usize> {
        let bundle = self.bundle.clone();
        let mut backend = MetadataWalBackend {
            store,
            bundle: &bundle,
            inner: self.wal_mut()?,
        };
        store.flush_idle(&mut backend, idle_ticks, max_records)
    }

    pub fn demote_by_policy(
        &mut self,
        store: &MfsMutableObjectStore,
        policy: TieringPolicy,
    ) -> io::Result<MutableObjectTieringReport> {
        let flush_records = self.flush_idle(store, 0, usize::MAX)?;
        self.sync_now()?;
        let mut report = self.bundle.demote_by_policy(store, policy)?;
        report.flush_records = flush_records;
        Ok(report)
    }

    pub fn get_value_with_cold_promotion(
        &self,
        store: &MfsMutableObjectStore,
        key: &[u8],
    ) -> io::Result<Option<Arc<MfsValue>>> {
        if let Some(value) = store.get(key) {
            return Ok(Some(value));
        }
        if self.bundle.promote_cold_key(store, key)? {
            return Ok(store.get(key));
        }
        Ok(store.get(key))
    }

    pub fn get_string_with_cold_promotion(
        &self,
        store: &MfsMutableObjectStore,
        key: &[u8],
    ) -> io::Result<Result<Option<String>, ObjectStoreError>> {
        let Some(value) = self.get_value_with_cold_promotion(store, key)? else {
            return Ok(Ok(None));
        };
        match value.as_ref() {
            MfsValue::String(value) => Ok(Ok(Some(value.clone()))),
            other => Ok(Err(ObjectStoreError::WrongType {
                expected: "string",
                actual: other.tag(),
            })),
        }
    }

    pub fn sync_now(&mut self) -> io::Result<()> {
        self.wal_mut()?.sync_now()
    }

    pub fn checkpoint_and_reset_wal(
        &mut self,
        store: &MfsMutableObjectStore,
    ) -> io::Result<MutableObjectCheckpoint> {
        self.flush_idle(store, 0, usize::MAX)?;
        let mut wal = self.take_wal()?;
        wal.sync_now()?;
        drop(wal);

        let checkpoint = self.bundle.write_checkpoint(store)?;
        truncate_file(&self.bundle.wal_path())?;
        self.wal = Some(self.bundle.open_wal()?);
        Ok(checkpoint)
    }

    pub fn recover(&self, expected_entries: usize) -> io::Result<MutableObjectRecovery> {
        self.bundle.recover(expected_entries)
    }

    fn wal_mut(&mut self) -> io::Result<&mut WalBackend<Vec<u8>, MfsValue, MfsValueCodec>> {
        self.wal
            .as_mut()
            .ok_or_else(|| io::Error::other("mutable object WAL handle is closed"))
    }

    fn take_wal(&mut self) -> io::Result<WalBackend<Vec<u8>, MfsValue, MfsValueCodec>> {
        self.wal
            .take()
            .ok_or_else(|| io::Error::other("mutable object WAL handle is closed"))
    }
}

impl MutableObjectStoreBundle {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_options(path, MutableObjectStoreBundleOptions::default())
    }

    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: MutableObjectStoreBundleOptions,
    ) -> io::Result<Self> {
        let bundle = Self {
            root: path.as_ref().to_path_buf(),
            options,
        };
        bundle.ensure_layout()?;
        Ok(bundle)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn wal_path(&self) -> PathBuf {
        self.root.join(WAL_DIR).join(ACTIVE_WAL_FILE)
    }

    pub fn checkpoint_dir(&self) -> PathBuf {
        self.root.join(CHECKPOINT_DIR)
    }

    pub fn cold_dir(&self) -> PathBuf {
        self.root.join(TIER_DIR).join(COLD_DIR)
    }

    pub fn cold_data_path(&self) -> PathBuf {
        self.cold_dir().join(COLD_DATA_FILE)
    }

    pub fn cold_index_path(&self) -> PathBuf {
        self.cold_dir().join(COLD_INDEX_FILE)
    }

    pub fn cold_manifest_path(&self) -> PathBuf {
        self.cold_dir().join(COLD_MANIFEST_FILE)
    }

    pub fn cold_generation_data_path(&self, id: u64) -> PathBuf {
        self.cold_dir()
            .join(cold_generation_file_name(id, COLD_GENERATION_DATA_SUFFIX))
    }

    pub fn cold_generation_index_path(&self, id: u64) -> PathBuf {
        self.cold_dir()
            .join(cold_generation_file_name(id, COLD_GENERATION_INDEX_SUFFIX))
    }

    pub fn open_wal(&self) -> io::Result<WalBackend<Vec<u8>, MfsValue, MfsValueCodec>> {
        self.ensure_layout()?;
        let path = self.wal_path();
        reject_symlink_if_exists(&path)?;
        WalBackend::open(path, MfsValueCodec, WalConfig::default())
    }

    pub fn write_checkpoint(
        &self,
        store: &MfsMutableObjectStore,
    ) -> io::Result<MutableObjectCheckpoint> {
        self.ensure_layout()?;
        let object_records = store.snapshot_records();
        let object_record_count = object_records.len();
        let records = records_with_metadata(store, object_records);
        let lsn = store.durable_high_water_mark();
        let file_name = checkpoint_file_name(lsn);
        let final_path = self.checkpoint_dir().join(&file_name);
        let tmp_path = self.root.join(TMP_DIR).join(format!("{file_name}.tmp"));
        reject_symlink_if_exists(&final_path)?;
        reject_symlink_if_exists(&tmp_path)?;
        let _ = fs::remove_file(&tmp_path);

        {
            let mut checkpoint = WalBackend::open(&tmp_path, MfsValueCodec, WalConfig::default())?;
            checkpoint.flush(&records)?;
            checkpoint.sync_now()?;
        }
        append_checkpoint_footer(&tmp_path, lsn, records.len())?;

        fs::rename(&tmp_path, &final_path)?;
        sync_parent_dir(&final_path)?;
        self.write_manifest(Some((&file_name, lsn)))?;
        self.prune_checkpoints()?;

        Ok(MutableObjectCheckpoint {
            path: final_path,
            lsn,
            records: object_record_count,
        })
    }

    pub fn recover(&self, expected_entries: usize) -> io::Result<MutableObjectRecovery> {
        self.ensure_layout()?;
        let store = MfsMutableObjectStore::with_capacity(expected_entries);
        let checkpoint = self.load_latest_checkpoint(&store)?;
        let checkpoint_lsn = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.lsn)
            .unwrap_or(0);
        let mut wal_records = Vec::new();
        WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(
            self.wal_path(),
            &MfsValueCodec,
            |record| {
                if record.version <= checkpoint_lsn {
                    return;
                }
                wal_records.push(FlushRecord {
                    key: record.key,
                    value: record.value.map(std::sync::Arc::new),
                    version: record.version,
                    op: record.op,
                });
            },
        )?;
        let wal_record_count = apply_persisted_records(&store, wal_records);

        Ok(MutableObjectRecovery {
            store,
            checkpoint,
            wal_records: wal_record_count,
        })
    }

    pub fn write_cold_snapshot(
        &self,
        store: &MfsMutableObjectStore,
    ) -> io::Result<MutableObjectColdTier> {
        self.ensure_layout()?;
        let records = store.snapshot_records();
        write_cold_records(self, store, &records)
    }

    pub fn read_cold_manifest(&self) -> io::Result<Option<MutableObjectColdManifest>> {
        read_cold_manifest(self)
    }

    pub fn write_cold_manifest(&self, manifest: &MutableObjectColdManifest) -> io::Result<()> {
        write_cold_manifest(self, manifest)
    }

    pub fn record_cold_tombstones(
        &self,
        tombstones: impl IntoIterator<Item = MutableObjectColdTombstone>,
    ) -> io::Result<()> {
        record_cold_tombstones(self, tombstones)
    }

    pub fn gc_cold_tier(
        &self,
        store: &MfsMutableObjectStore,
    ) -> io::Result<MutableObjectColdGcReport> {
        gc_cold_tier(self, store)
    }

    pub fn demote_by_policy(
        &self,
        store: &MfsMutableObjectStore,
        policy: TieringPolicy,
    ) -> io::Result<MutableObjectTieringReport> {
        demote_by_policy(self, store, policy)
    }

    pub fn demote_all_to_cold(&self, store: &MfsMutableObjectStore) -> io::Result<usize> {
        let records = store.snapshot_records();
        let cold = write_cold_records(self, store, &records)?;
        for record in records {
            store.evict_clean(&record.key);
        }
        Ok(cold.records)
    }

    pub fn promote_cold_key(&self, store: &MfsMutableObjectStore, key: &[u8]) -> io::Result<bool> {
        let Some((value, meta)) = read_cold_record(self, key)? else {
            return Ok(false);
        };
        let promoted = store
            .try_promote_clean_with_expiry_meta(key.to_vec(), value, meta)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}")))?;
        Ok(promoted)
    }

    pub fn get_with_cold_promotion(
        &self,
        store: &MfsMutableObjectStore,
        key: &[u8],
    ) -> io::Result<Option<Arc<MfsValue>>> {
        if let Some(value) = store.get(key) {
            return Ok(Some(value));
        }
        if self.promote_cold_key(store, key)? {
            return Ok(store.get(key));
        }
        Ok(store.get(key))
    }

    fn ensure_layout(&self) -> io::Result<()> {
        reject_symlink_if_exists(&self.root)?;
        fs::create_dir_all(&self.root)?;
        reject_symlink_if_exists(&self.root)?;
        let wal_dir = self.root.join(WAL_DIR);
        let checkpoint_dir = self.root.join(CHECKPOINT_DIR);
        let tmp_dir = self.root.join(TMP_DIR);
        let tier_dir = self.root.join(TIER_DIR);
        let cold_dir = self.root.join(TIER_DIR).join(COLD_DIR);
        reject_symlink_if_exists(&wal_dir)?;
        reject_symlink_if_exists(&checkpoint_dir)?;
        reject_symlink_if_exists(&tmp_dir)?;
        reject_symlink_if_exists(&tier_dir)?;
        reject_symlink_if_exists(&cold_dir)?;
        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&checkpoint_dir)?;
        fs::create_dir_all(&tmp_dir)?;
        fs::create_dir_all(&cold_dir)?;
        reject_symlink_if_exists(&wal_dir)?;
        reject_symlink_if_exists(&checkpoint_dir)?;
        reject_symlink_if_exists(&tmp_dir)?;
        reject_symlink_if_exists(&tier_dir)?;
        reject_symlink_if_exists(&cold_dir)?;
        self.cleanup_tmp_dir()?;
        if !self.root.join(MANIFEST_FILE).exists() {
            self.write_manifest(None)?;
        }
        Ok(())
    }

    fn cleanup_tmp_dir(&self) -> io::Result<usize> {
        let tmp_dir = self.root.join(TMP_DIR);
        let mut removed = 0usize;
        for entry in fs::read_dir(&tmp_dir)? {
            let path = entry?.path();
            reject_symlink_if_exists(&path)?;
            if path.is_file() {
                fs::remove_file(path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    pub fn prune_checkpoints(&self) -> io::Result<usize> {
        prune_checkpoints(&self.checkpoint_dir(), self.options.keep_last_checkpoints)
    }

    fn write_manifest(&self, checkpoint: Option<(&str, u64)>) -> io::Result<()> {
        let tmp_path = self.root.join(TMP_DIR).join("MANIFEST.tmp");
        let final_path = self.root.join(MANIFEST_FILE);
        reject_symlink_if_exists(&tmp_path)?;
        reject_symlink_if_exists(&final_path)?;
        let mut body = String::new();
        body.push_str("format=1\n");
        body.push_str("store=mutable-object\n");
        body.push_str("wal=wal/active.mfswal\n");
        if let Some((file_name, lsn)) = checkpoint {
            body.push_str("checkpoint=checkpoints/");
            body.push_str(file_name);
            body.push('\n');
            body.push_str("checkpoint_lsn=");
            body.push_str(&lsn.to_string());
            body.push('\n');
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
        drop(file);

        let backup_path = self.root.join(MANIFEST_BACKUP_FILE);
        reject_symlink_if_exists(&backup_path)?;
        match fs::copy(&final_path, &backup_path) {
            Ok(_) => {
                File::open(&backup_path)?.sync_all()?;
                sync_parent_dir(&backup_path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(&tmp_path, &final_path)?;
        sync_parent_dir(&final_path)
    }

    fn load_latest_checkpoint(
        &self,
        store: &MfsMutableObjectStore,
    ) -> io::Result<Option<MutableObjectCheckpoint>> {
        for path in checkpoint_paths_desc(&self.checkpoint_dir())? {
            let Some((checkpoint, records)) = read_checkpoint_records(&path)? else {
                continue;
            };
            apply_persisted_records(store, records);
            return Ok(Some(checkpoint));
        }
        Ok(None)
    }
}

struct MetadataWalBackend<'a> {
    store: &'a MfsMutableObjectStore,
    bundle: &'a MutableObjectStoreBundle,
    inner: &'a mut WalBackend<Vec<u8>, MfsValue, MfsValueCodec>,
}

impl FlushBackend<Vec<u8>, MfsValue> for MetadataWalBackend<'_> {
    type Error = io::Error;

    fn flush(&mut self, records: &[ObjectFlushRecord]) -> Result<(), Self::Error> {
        let expanded = records_with_metadata(self.store, records.to_vec());
        self.inner.flush(&expanded)?;
        let tombstones = records
            .iter()
            .filter(|record| record.op == Operation::Delete)
            .map(|record| MutableObjectColdTombstone {
                key: record.key.clone(),
                version: record.version,
            });
        self.bundle.record_cold_tombstones(tombstones)
    }
}

fn records_with_metadata(
    store: &MfsMutableObjectStore,
    records: Vec<ObjectFlushRecord>,
) -> Vec<ObjectFlushRecord> {
    let mut expanded = Vec::with_capacity(records.len().saturating_mul(2));
    for record in records {
        let key = record.key.clone();
        let op = record.op;
        let version = record.version;
        expanded.push(record);
        match op {
            Operation::Put => {
                if let Some(meta) = store.expiry_meta(&key) {
                    expanded.push(FlushRecord {
                        key: metadata_key(&key),
                        value: Some(std::sync::Arc::new(encode_expiry_meta(meta))),
                        version,
                        op: Operation::Put,
                    });
                }
            }
            Operation::Delete => expanded.push(FlushRecord {
                key: metadata_key(&key),
                value: None,
                version,
                op: Operation::Delete,
            }),
        }
    }
    expanded
}

fn apply_persisted_records(
    store: &MfsMutableObjectStore,
    records: Vec<ObjectFlushRecord>,
) -> usize {
    let mut values: BTreeMap<Vec<u8>, ObjectFlushRecord> = BTreeMap::new();
    let mut metadata: BTreeMap<Vec<u8>, MutableObjectExpiryMeta> = BTreeMap::new();
    for record in records {
        if let Some(key) = object_key_from_metadata_key(&record.key) {
            match record.op {
                Operation::Put => {
                    if let Some(value) = record.value.as_deref()
                        && let Some(meta) = decode_expiry_meta(value)
                    {
                        metadata.insert(key, meta);
                    }
                }
                Operation::Delete => {
                    metadata.remove(&key);
                }
            }
            continue;
        }
        values.insert(record.key.clone(), record);
    }

    let mut applied = 0usize;
    for (key, record) in values {
        match record.op {
            Operation::Put => {
                let Some(value) = record.value else {
                    continue;
                };
                if let Some(meta) = metadata.get(&key).copied()
                    && meta.version == record.version
                {
                    let _ =
                        store.try_load_clean_with_expiry_meta(key, value.as_ref().clone(), meta);
                } else {
                    store.load_clean_versioned(key, value.as_ref().clone(), record.version);
                }
                applied += 1;
            }
            Operation::Delete => {
                store.load_clean_delete_versioned(key, record.version);
                applied += 1;
            }
        }
    }
    applied
}

fn metadata_key(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(META_KEY_PREFIX.len() + key.len());
    out.extend_from_slice(META_KEY_PREFIX);
    out.extend_from_slice(key);
    out
}

fn object_key_from_metadata_key(key: &[u8]) -> Option<Vec<u8>> {
    key.strip_prefix(META_KEY_PREFIX).map(ToOwned::to_owned)
}

fn encode_expiry_meta(meta: MutableObjectExpiryMeta) -> MfsValue {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&meta.version.to_le_bytes());
    bytes.extend_from_slice(&meta.last_touch.to_le_bytes());
    bytes.extend_from_slice(&meta.expires_at.to_le_bytes());
    bytes.extend_from_slice(&meta.tti_ticks.to_le_bytes());
    MfsValue::Bytes(bytes)
}

fn decode_expiry_meta(value: &MfsValue) -> Option<MutableObjectExpiryMeta> {
    let MfsValue::Bytes(bytes) = value else {
        return None;
    };
    if bytes.len() != 32 {
        return None;
    }
    Some(MutableObjectExpiryMeta {
        version: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
        last_touch: u64::from_le_bytes(bytes[8..16].try_into().ok()?),
        expires_at: u64::from_le_bytes(bytes[16..24].try_into().ok()?),
        tti_ticks: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
    })
}

#[derive(Clone)]
struct ColdRecordEntry {
    key: Vec<u8>,
    value: MfsValue,
    meta: MutableObjectExpiryMeta,
}

fn write_cold_records(
    bundle: &MutableObjectStoreBundle,
    store: &MfsMutableObjectStore,
    records: &[ObjectFlushRecord],
) -> io::Result<MutableObjectColdTier> {
    bundle.ensure_layout()?;
    let entries = cold_entries_from_store_records(store, records);
    Ok(
        publish_cold_entries(bundle, &entries)?.unwrap_or_else(|| MutableObjectColdTier {
            data_path: bundle.cold_generation_data_path(0),
            index_path: bundle.cold_generation_index_path(0),
            records: 0,
        }),
    )
}

fn publish_cold_entries(
    bundle: &MutableObjectStoreBundle,
    entries: &[ColdRecordEntry],
) -> io::Result<Option<MutableObjectColdTier>> {
    if entries.is_empty() {
        return Ok(None);
    }

    bundle.ensure_layout()?;
    let mut manifest = read_cold_manifest(bundle)?.unwrap_or_else(empty_cold_manifest);
    let id = next_cold_generation_id(&manifest)?;
    let (generation, data_path, index_path) = write_cold_generation_entries(bundle, entries, id)?;
    let records = generation.records;
    manifest.generations.push(generation);
    write_cold_manifest(bundle, &manifest)?;

    Ok(Some(MutableObjectColdTier {
        data_path,
        index_path,
        records,
    }))
}

fn gc_cold_tier(
    bundle: &MutableObjectStoreBundle,
    store: &MfsMutableObjectStore,
) -> io::Result<MutableObjectColdGcReport> {
    bundle.ensure_layout()?;
    let Some(manifest) = read_cold_manifest(bundle)? else {
        return Ok(MutableObjectColdGcReport::default());
    };

    let now = store.stats().logical_clock;
    let tombstone_versions = manifest.tombstones.iter().fold(
        BTreeMap::<Vec<u8>, u64>::new(),
        |mut versions, tombstone| {
            versions
                .entry(tombstone.key.clone())
                .and_modify(|version| *version = (*version).max(tombstone.version))
                .or_insert(tombstone.version);
            versions
        },
    );
    let mut generations = manifest.generations.iter().collect::<Vec<_>>();
    generations.sort_by_key(|generation| Reverse(generation.id));

    let mut report = MutableObjectColdGcReport::default();
    let mut seen_keys = BTreeSet::new();
    let mut survivors = Vec::new();
    for generation in generations {
        report.generations_scanned += 1;
        let entries = match read_cold_generation_entries(bundle, generation.id) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries {
            report.records_scanned += 1;
            if seen_keys.contains(&entry.key) {
                continue;
            }
            if tombstone_versions
                .get(&entry.key)
                .map(|version| *version >= entry.meta.version)
                .unwrap_or(false)
            {
                seen_keys.insert(entry.key);
                continue;
            }
            seen_keys.insert(entry.key.clone());
            if cold_meta_is_expired(entry.meta, now) {
                report.records_dropped_expired += 1;
                continue;
            }
            report.records_kept += 1;
            survivors.push(entry);
        }
    }

    let mut compacted = MutableObjectColdManifest {
        generations: Vec::new(),
        tombstones: manifest.tombstones.clone(),
    };
    if !survivors.is_empty() {
        let id = next_cold_generation_id(&manifest)?;
        let (generation, _, _) = write_cold_generation_entries(bundle, &survivors, id)?;
        compacted.generations.push(generation);
    }

    write_cold_manifest(bundle, &compacted)?;

    let referenced = compacted
        .generations
        .iter()
        .map(|generation| generation.id)
        .collect::<BTreeSet<_>>();
    for generation in &manifest.generations {
        if referenced.contains(&generation.id) {
            continue;
        }
        report.bytes_freed = report
            .bytes_freed
            .saturating_add(remove_cold_generation_files(bundle, generation.id)?);
        report.generations_removed += 1;
    }
    if report.generations_removed > 0 {
        File::open(bundle.cold_dir())?.sync_all()?;
    }

    Ok(report)
}

fn demote_by_policy(
    bundle: &MutableObjectStoreBundle,
    store: &MfsMutableObjectStore,
    policy: TieringPolicy,
) -> io::Result<MutableObjectTieringReport> {
    let mut report = MutableObjectTieringReport::default();
    if policy.max_records == 0 {
        return Ok(report);
    }

    bundle.ensure_layout()?;
    let snapshot = store.tiering_snapshot();
    report.skipped_empty = snapshot.skipped_empty;

    let mut candidates = Vec::new();
    for record in snapshot.records {
        if record.pending_dirty {
            report.skipped_dirty += 1;
            continue;
        }
        if !tiering_record_is_old_enough(&record, snapshot.now, policy) {
            report.skipped_recent += 1;
            continue;
        }
        candidates.push(record);
    }

    if candidates.is_empty() {
        return Ok(report);
    }

    candidates.sort_by(|left, right| {
        left.meta
            .last_touch
            .cmp(&right.meta.last_touch)
            .then_with(|| left.meta.version.cmp(&right.meta.version))
            .then_with(|| left.key.cmp(&right.key))
    });

    let selected = tiering_selection_limit(policy, snapshot.hot_len).min(candidates.len());
    if selected == 0 {
        report.skipped_capacity = candidates.len();
        return Ok(report);
    }
    report.skipped_capacity = candidates.len().saturating_sub(selected);
    candidates.truncate(selected);
    report.attempted = candidates.len();

    let entries = candidates
        .iter()
        .map(cold_entry_from_tiering_record)
        .collect::<Vec<_>>();
    publish_cold_entries(bundle, &entries)?;

    report.demoted = evict_published_tiering_candidates(bundle, store, &candidates)?;

    Ok(report)
}

fn evict_published_tiering_candidates(
    bundle: &MutableObjectStoreBundle,
    store: &MfsMutableObjectStore,
    candidates: &[MutableObjectTieringRecord],
) -> io::Result<usize> {
    let mut demoted = 0usize;
    let mut failed_evictions = Vec::new();
    for record in candidates {
        if store.evict_clean_versioned(&record.key, record.meta.version) {
            demoted += 1;
        } else {
            failed_evictions.push(MutableObjectColdTombstone {
                key: record.key.clone(),
                version: record.meta.version,
            });
        }
    }
    record_cold_tombstones(bundle, failed_evictions)?;
    Ok(demoted)
}

fn tiering_record_is_old_enough(
    record: &MutableObjectTieringRecord,
    now: u64,
    policy: TieringPolicy,
) -> bool {
    (policy.idle_threshold_ticks == 0
        || now.saturating_sub(record.meta.last_touch) >= policy.idle_threshold_ticks)
        && (policy.min_clean_age_ticks == 0
            || now.saturating_sub(record.meta.version) >= policy.min_clean_age_ticks)
}

fn tiering_selection_limit(policy: TieringPolicy, hot_len: usize) -> usize {
    match policy.hot_capacity_soft_limit {
        Some(limit) if hot_len > limit => policy.max_records.min(hot_len - limit),
        Some(_) if policy.idle_threshold_ticks == 0 && policy.min_clean_age_ticks == 0 => 0,
        _ => policy.max_records,
    }
}

fn cold_entry_from_tiering_record(record: &MutableObjectTieringRecord) -> ColdRecordEntry {
    ColdRecordEntry {
        key: record.key.clone(),
        value: record.value.as_ref().clone(),
        meta: record.meta,
    }
}

fn cold_meta_is_expired(meta: MutableObjectExpiryMeta, now: u64) -> bool {
    (meta.expires_at != 0 && now >= meta.expires_at)
        || (meta.tti_ticks != 0 && now.saturating_sub(meta.last_touch) >= meta.tti_ticks)
}

fn remove_cold_generation_files(bundle: &MutableObjectStoreBundle, id: u64) -> io::Result<u64> {
    let data_path = bundle.cold_generation_data_path(id);
    let index_path = bundle.cold_generation_index_path(id);
    let bytes = file_size_if_exists(&data_path)?.saturating_add(file_size_if_exists(&index_path)?);
    remove_file_if_exists(&data_path)?;
    remove_file_if_exists(&index_path)?;
    Ok(bytes)
}

fn file_size_if_exists(path: &Path) -> io::Result<u64> {
    reject_symlink_if_exists(path)?;
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
        Ok(_) => Ok(0),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn empty_cold_manifest() -> MutableObjectColdManifest {
    MutableObjectColdManifest {
        generations: Vec::new(),
        tombstones: Vec::new(),
    }
}

fn record_cold_tombstones(
    bundle: &MutableObjectStoreBundle,
    tombstones: impl IntoIterator<Item = MutableObjectColdTombstone>,
) -> io::Result<()> {
    let mut incoming = tombstones.into_iter().collect::<Vec<_>>();
    if incoming.is_empty() {
        return Ok(());
    }

    let mut manifest = read_cold_manifest(bundle)?.unwrap_or_else(empty_cold_manifest);
    let mut versions = manifest
        .tombstones
        .drain(..)
        .map(|tombstone| (tombstone.key, tombstone.version))
        .collect::<BTreeMap<_, _>>();
    for tombstone in incoming.drain(..) {
        versions
            .entry(tombstone.key)
            .and_modify(|version| *version = (*version).max(tombstone.version))
            .or_insert(tombstone.version);
    }
    manifest.tombstones = versions
        .into_iter()
        .map(|(key, version)| MutableObjectColdTombstone { key, version })
        .collect();
    write_cold_manifest(bundle, &manifest)
}

fn next_cold_generation_id(manifest: &MutableObjectColdManifest) -> io::Result<u64> {
    manifest
        .generations
        .iter()
        .map(|generation| generation.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| io::Error::other("cold generation id overflow"))
}

#[cfg(test)]
fn write_cold_generation_files(
    bundle: &MutableObjectStoreBundle,
    store: &MfsMutableObjectStore,
    records: &[ObjectFlushRecord],
    id: u64,
) -> io::Result<(MutableObjectColdGeneration, PathBuf, PathBuf)> {
    let entries = cold_entries_from_store_records(store, records);
    write_cold_generation_entries(bundle, &entries, id)
}

fn write_cold_generation_entries(
    bundle: &MutableObjectStoreBundle,
    records: &[ColdRecordEntry],
    id: u64,
) -> io::Result<(MutableObjectColdGeneration, PathBuf, PathBuf)> {
    let data_path = bundle.cold_generation_data_path(id);
    let index_path = bundle.cold_generation_index_path(id);
    reject_symlink_if_exists(&data_path)?;
    reject_symlink_if_exists(&index_path)?;

    let tmp_data = bundle
        .root
        .join(TMP_DIR)
        .join(format!("cold-generation-{id:020}.mfsobj.tmp"));
    let tmp_index = bundle
        .root
        .join(TMP_DIR)
        .join(format!("cold-generation-{id:020}.mfsidx.tmp"));
    let records = write_cold_entries(records, &tmp_data, &tmp_index, &data_path, &index_path)?;

    Ok((
        MutableObjectColdGeneration { id, records },
        data_path,
        index_path,
    ))
}

#[cfg(test)]
fn write_cold_files(
    store: &MfsMutableObjectStore,
    records: &[ObjectFlushRecord],
    tmp_data: &Path,
    tmp_index: &Path,
    data_path: &Path,
    index_path: &Path,
) -> io::Result<usize> {
    let entries = cold_entries_from_store_records(store, records);
    write_cold_entries(&entries, tmp_data, tmp_index, data_path, index_path)
}

fn cold_entries_from_store_records(
    store: &MfsMutableObjectStore,
    records: &[ObjectFlushRecord],
) -> Vec<ColdRecordEntry> {
    records
        .iter()
        .filter_map(|record| {
            let value = record.value.as_deref()?.clone();
            let meta = store
                .expiry_meta(&record.key)
                .unwrap_or(MutableObjectExpiryMeta {
                    version: record.version,
                    last_touch: record.version,
                    expires_at: 0,
                    tti_ticks: 0,
                });
            Some(ColdRecordEntry {
                key: record.key.clone(),
                value,
                meta,
            })
        })
        .collect()
}

fn write_cold_entries(
    records: &[ColdRecordEntry],
    tmp_data: &Path,
    tmp_index: &Path,
    data_path: &Path,
    index_path: &Path,
) -> io::Result<usize> {
    reject_symlink_if_exists(tmp_data)?;
    reject_symlink_if_exists(tmp_index)?;
    let _ = fs::remove_file(tmp_data);
    let _ = fs::remove_file(tmp_index);

    let mut data = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(tmp_data)?;
    data.write_all(COLD_DATA_MAGIC)?;
    let mut entries = Vec::new();
    for record in records {
        let offset = data.stream_position()?;
        let mut encoded_value = Vec::new();
        MfsValueCodec.encode_value(&record.value, &mut encoded_value);
        let meta = record.meta;
        write_len_prefixed(&mut data, &record.key)?;
        data.write_all(&(encoded_value.len() as u32).to_le_bytes())?;
        data.write_all(&meta.version.to_le_bytes())?;
        data.write_all(&meta.last_touch.to_le_bytes())?;
        data.write_all(&meta.expires_at.to_le_bytes())?;
        data.write_all(&meta.tti_ticks.to_le_bytes())?;
        data.write_all(&encoded_value)?;
        let len = data.stream_position()?.saturating_sub(offset);
        entries.push((record.key.clone(), offset, len));
    }
    data.sync_all()?;
    drop(data);

    let mut index = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(tmp_index)?;
    index.write_all(COLD_INDEX_MAGIC)?;
    index.write_all(&(entries.len() as u64).to_le_bytes())?;
    for (key, offset, len) in &entries {
        write_len_prefixed(&mut index, key)?;
        index.write_all(&offset.to_le_bytes())?;
        index.write_all(&len.to_le_bytes())?;
    }
    index.sync_all()?;
    drop(index);

    remove_file_if_exists(data_path)?;
    remove_file_if_exists(index_path)?;
    fs::rename(tmp_data, data_path)?;
    sync_parent_dir(data_path)?;
    fs::rename(tmp_index, index_path)?;
    sync_parent_dir(index_path)?;

    Ok(entries.len())
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    reject_symlink_if_exists(path)?;
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn read_cold_record(
    bundle: &MutableObjectStoreBundle,
    key: &[u8],
) -> io::Result<Option<(MfsValue, MutableObjectExpiryMeta)>> {
    if let Some(manifest) = read_cold_manifest(bundle)? {
        return read_manifest_cold_record(bundle, &manifest, key);
    }

    read_cold_record_from_files(&bundle.cold_index_path(), &bundle.cold_data_path(), key)
}

fn read_manifest_cold_record(
    bundle: &MutableObjectStoreBundle,
    manifest: &MutableObjectColdManifest,
    key: &[u8],
) -> io::Result<Option<(MfsValue, MutableObjectExpiryMeta)>> {
    let mut generations = manifest.generations.iter().collect::<Vec<_>>();
    generations.sort_by_key(|generation| Reverse(generation.id));
    let tombstone_version = manifest
        .tombstones
        .iter()
        .filter(|tombstone| tombstone.key.as_slice() == key)
        .map(|tombstone| tombstone.version)
        .max();

    for generation in generations {
        let data_path = bundle.cold_generation_data_path(generation.id);
        let index_path = bundle.cold_generation_index_path(generation.id);
        match read_cold_record_from_files(&index_path, &data_path, key) {
            Ok(Some((value, meta))) => {
                if tombstone_version
                    .map(|version| version >= meta.version)
                    .unwrap_or(false)
                {
                    continue;
                }
                return Ok(Some((value, meta)));
            }
            Ok(None) | Err(_) => continue,
        }
    }

    Ok(None)
}

fn read_cold_record_from_files(
    index_path: &Path,
    data_path: &Path,
    key: &[u8],
) -> io::Result<Option<(MfsValue, MutableObjectExpiryMeta)>> {
    if !index_path.exists() || !data_path.exists() {
        return Ok(None);
    }
    reject_symlink_if_exists(index_path)?;
    reject_symlink_if_exists(data_path)?;

    let Some((offset, len)) = find_cold_index_entry(index_path, key)? else {
        return Ok(None);
    };
    let mut data = File::open(data_path)?;
    let mut magic = [0u8; 8];
    data.read_exact(&mut magic)?;
    if &magic != COLD_DATA_MAGIC {
        return Ok(None);
    }
    let data_len = data.metadata()?.len();
    Ok(
        read_cold_entry_at(&mut data, data_len, offset, len, Some(key))?
            .map(|entry| (entry.value, entry.meta)),
    )
}

fn read_cold_generation_entries(
    bundle: &MutableObjectStoreBundle,
    id: u64,
) -> io::Result<Vec<ColdRecordEntry>> {
    let index_path = bundle.cold_generation_index_path(id);
    let data_path = bundle.cold_generation_data_path(id);
    read_cold_entries_from_files(&index_path, &data_path)
}

fn read_cold_entries_from_files(
    index_path: &Path,
    data_path: &Path,
) -> io::Result<Vec<ColdRecordEntry>> {
    if !index_path.exists() || !data_path.exists() {
        return Ok(Vec::new());
    }
    reject_symlink_if_exists(index_path)?;
    reject_symlink_if_exists(data_path)?;

    let mut index = File::open(index_path)?;
    let mut index_magic = [0u8; 8];
    index.read_exact(&mut index_magic)?;
    if &index_magic != COLD_INDEX_MAGIC {
        return Ok(Vec::new());
    }
    let entries = usize::try_from(read_u64(&mut index)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "cold index entry count too large",
        )
    })?;

    let mut data = File::open(data_path)?;
    let mut data_magic = [0u8; 8];
    data.read_exact(&mut data_magic)?;
    if &data_magic != COLD_DATA_MAGIC {
        return Ok(Vec::new());
    }
    let data_len = data.metadata()?.len();
    let mut records = Vec::with_capacity(entries);
    for _ in 0..entries {
        let key = read_len_prefixed(&mut index)?;
        let offset = read_u64(&mut index)?;
        let len = read_u64(&mut index)?;
        let Some(record) = read_cold_entry_at(&mut data, data_len, offset, len, Some(&key))? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cold generation index entry mismatch",
            ));
        };
        records.push(record);
    }
    Ok(records)
}

fn read_cold_entry_at(
    mut data: &mut File,
    data_len: u64,
    offset: u64,
    len: u64,
    expected_key: Option<&[u8]>,
) -> io::Result<Option<ColdRecordEntry>> {
    let Some(record_end) = offset.checked_add(len) else {
        return Ok(None);
    };
    if offset < COLD_DATA_MAGIC.len() as u64 || len == 0 || record_end > data_len {
        return Ok(None);
    }
    data.seek(SeekFrom::Start(offset))?;
    let record_key = read_len_prefixed(&mut data)?;
    if expected_key
        .map(|expected_key| record_key.as_slice() != expected_key)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let value_len = read_u32(&mut data)? as usize;
    let version = read_u64(&mut data)?;
    let last_touch = read_u64(&mut data)?;
    let expires_at = read_u64(&mut data)?;
    let tti_ticks = read_u64(&mut data)?;
    let min_len = 4usize
        .saturating_add(record_key.len())
        .saturating_add(4)
        .saturating_add(32)
        .saturating_add(value_len);
    if u64::try_from(min_len).ok() != Some(len) {
        return Ok(None);
    }
    let mut value_bytes = vec![0u8; value_len];
    data.read_exact(&mut value_bytes)?;
    let value = MfsValueCodec.decode_value(&value_bytes)?;
    Ok(Some(ColdRecordEntry {
        key: record_key,
        value,
        meta: MutableObjectExpiryMeta {
            version,
            last_touch,
            expires_at,
            tti_ticks,
        },
    }))
}

fn find_cold_index_entry(path: &Path, key: &[u8]) -> io::Result<Option<(u64, u64)>> {
    let mut index = File::open(path)?;
    let mut magic = [0u8; 8];
    index.read_exact(&mut magic)?;
    if &magic != COLD_INDEX_MAGIC {
        return Ok(None);
    }
    let entries = read_u64(&mut index)?;
    for _ in 0..entries {
        let entry_key = read_len_prefixed(&mut index)?;
        let offset = read_u64(&mut index)?;
        let len = read_u64(&mut index)?;
        if entry_key == key {
            return Ok(Some((offset, len)));
        }
    }
    Ok(None)
}

fn read_cold_manifest(
    bundle: &MutableObjectStoreBundle,
) -> io::Result<Option<MutableObjectColdManifest>> {
    let path = bundle.cold_manifest_path();
    if !path.exists() {
        return Ok(None);
    }
    reject_symlink_if_exists(&path)?;

    let mut manifest = File::open(&path)?;
    let mut magic = [0u8; 8];
    manifest.read_exact(&mut magic)?;
    if &magic != COLD_MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cold manifest magic mismatch",
        ));
    }
    let version = read_u32(&mut manifest)?;
    if version != COLD_MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported cold manifest version",
        ));
    }

    let generation_count = usize::try_from(read_u64(&mut manifest)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "cold generation count too large",
        )
    })?;
    let mut generations = Vec::with_capacity(generation_count);
    for _ in 0..generation_count {
        let id = read_u64(&mut manifest)?;
        let records = usize::try_from(read_u64(&mut manifest)?).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "cold generation record count too large",
            )
        })?;
        generations.push(MutableObjectColdGeneration { id, records });
    }

    let mut tail = Vec::new();
    manifest.read_to_end(&mut tail)?;
    let mut tombstones = Vec::new();
    if !tail.is_empty() {
        let mut tail = std::io::Cursor::new(tail);
        let tombstone_count = usize::try_from(read_u64(&mut tail)?).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "cold tombstone count too large")
        })?;
        tombstones.reserve(tombstone_count);
        for _ in 0..tombstone_count {
            let key = read_len_prefixed(&mut tail)?;
            let version = read_u64(&mut tail)?;
            tombstones.push(MutableObjectColdTombstone { key, version });
        }
        if tail.position() != tail.get_ref().len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing bytes in cold manifest",
            ));
        }
    }

    Ok(Some(MutableObjectColdManifest {
        generations,
        tombstones,
    }))
}

fn write_cold_manifest(
    bundle: &MutableObjectStoreBundle,
    manifest: &MutableObjectColdManifest,
) -> io::Result<()> {
    bundle.ensure_layout()?;
    let final_path = bundle.cold_manifest_path();
    let tmp_path = bundle.root.join(TMP_DIR).join("cold-MANIFEST.tmp");
    reject_symlink_if_exists(&final_path)?;
    reject_symlink_if_exists(&tmp_path)?;
    let _ = fs::remove_file(&tmp_path);

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    file.write_all(COLD_MANIFEST_MAGIC)?;
    file.write_all(&COLD_MANIFEST_VERSION.to_le_bytes())?;
    file.write_all(&(manifest.generations.len() as u64).to_le_bytes())?;
    for generation in &manifest.generations {
        file.write_all(&generation.id.to_le_bytes())?;
        file.write_all(&(generation.records as u64).to_le_bytes())?;
    }
    file.write_all(&(manifest.tombstones.len() as u64).to_le_bytes())?;
    for tombstone in &manifest.tombstones {
        write_len_prefixed(&mut file, &tombstone.key)?;
        file.write_all(&tombstone.version.to_le_bytes())?;
    }
    file.sync_all()?;
    drop(file);

    fs::rename(&tmp_path, &final_path)?;
    sync_parent_dir(&final_path)
}

fn write_len_prefixed(mut writer: impl Write, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "field too large"))?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(bytes)
}

fn read_len_prefixed(mut reader: impl Read) -> io::Result<Vec<u8>> {
    let len = read_u32(&mut reader)? as usize;
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "field too large",
        ));
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_u32(mut reader: impl Read) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(mut reader: impl Read) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn checkpoint_file_name(lsn: u64) -> String {
    format!("{CHECKPOINT_PREFIX}{lsn:020}.{CHECKPOINT_EXTENSION}")
}

fn cold_generation_file_name(id: u64, suffix: &str) -> String {
    format!("{COLD_GENERATION_PREFIX}{id:020}{suffix}")
}

fn checkpoint_lsn_from_path(path: &Path) -> Option<u64> {
    let file_name = path.file_name()?.to_str()?;
    let lsn = file_name
        .strip_prefix(CHECKPOINT_PREFIX)?
        .strip_suffix(&format!(".{CHECKPOINT_EXTENSION}"))?;
    lsn.parse().ok()
}

fn checkpoint_paths_desc(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(lsn) = checkpoint_lsn_from_path(&path) {
            paths.push((lsn, path));
        }
    }
    paths.sort_by_key(|(lsn, _)| Reverse(*lsn));
    Ok(paths.into_iter().map(|(_, path)| path).collect())
}

fn prune_checkpoints(dir: &Path, keep_last: usize) -> io::Result<usize> {
    if keep_last == 0 {
        return Ok(0);
    }
    let mut checkpoints = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(lsn) = checkpoint_lsn_from_path(&path) {
            checkpoints.push((lsn, path));
        }
    }
    checkpoints.sort_by_key(|(lsn, _)| Reverse(*lsn));
    let mut removed = 0usize;
    for (_, path) in checkpoints.into_iter().skip(keep_last) {
        reject_symlink_if_exists(&path)?;
        fs::remove_file(&path)?;
        removed += 1;
    }
    if removed > 0 {
        File::open(dir)?.sync_all()?;
    }
    Ok(removed)
}

fn read_checkpoint_records(
    path: &Path,
) -> io::Result<Option<(MutableObjectCheckpoint, CheckpointRecords)>> {
    reject_symlink_if_exists(path)?;
    let Some(footer) = read_checkpoint_footer(path)? else {
        return Ok(None);
    };
    let file_lsn = checkpoint_lsn_from_path(path).unwrap_or(0);
    if footer.lsn != file_lsn {
        return Ok(None);
    }
    let mut records = Vec::new();
    let mut max_record_lsn = 0u64;
    WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(path, &MfsValueCodec, |record| {
        if let Operation::Put = record.op
            && let Some(value) = record.value
        {
            max_record_lsn = max_record_lsn.max(record.version);
            records.push(FlushRecord {
                key: record.key,
                value: Some(std::sync::Arc::new(value)),
                version: record.version,
                op: Operation::Put,
            });
        }
    })?;

    if records.len() != footer.records || (file_lsn != 0 && max_record_lsn > file_lsn) {
        return Ok(None);
    }

    let object_record_count = records
        .iter()
        .filter(|record| object_key_from_metadata_key(&record.key).is_none())
        .count();

    Ok(Some((
        MutableObjectCheckpoint {
            path: path.to_path_buf(),
            lsn: file_lsn.max(max_record_lsn),
            records: object_record_count,
        },
        records,
    )))
}

struct CheckpointFooter {
    lsn: u64,
    records: usize,
}

fn append_checkpoint_footer(path: &Path, lsn: u64, records: usize) -> io::Result<()> {
    let mut body = Vec::new();
    File::open(path)?.read_to_end(&mut body)?;
    let mut footer = Vec::with_capacity(CHECKPOINT_FOOTER_LEN);
    footer.extend_from_slice(CHECKPOINT_FOOTER_MAGIC);
    footer.extend_from_slice(&lsn.to_le_bytes());
    footer.extend_from_slice(&(records as u64).to_le_bytes());
    footer.extend_from_slice(&checksum64(&body).to_le_bytes());

    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&footer)?;
    file.sync_all()
}

fn read_checkpoint_footer(path: &Path) -> io::Result<Option<CheckpointFooter>> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    if bytes.len() < CHECKPOINT_FOOTER_LEN {
        return Ok(None);
    }
    let footer_start = bytes.len() - CHECKPOINT_FOOTER_LEN;
    let footer = &bytes[footer_start..];
    if &footer[0..8] != CHECKPOINT_FOOTER_MAGIC {
        return Ok(None);
    }
    let lsn = u64::from_le_bytes(footer[8..16].try_into().expect("footer lsn length"));
    let records_u64 = u64::from_le_bytes(footer[16..24].try_into().expect("footer count length"));
    let checksum = u64::from_le_bytes(footer[24..32].try_into().expect("footer checksum length"));
    if checksum64(&bytes[..footer_start]) != checksum {
        return Ok(None);
    }
    let records = usize::try_from(records_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint record count too large",
        )
    })?;
    Ok(Some(CheckpointFooter { lsn, records }))
}

fn checksum64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn truncate_file(path: &Path) -> io::Result<()> {
    reject_symlink_if_exists(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.sync_all()?;
    sync_parent_dir(path)
}

fn reject_symlink_if_exists(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("bundle path must not be a symlink: {}", path.display()),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfs_core::FlushRecord;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct CollectBackend {
        records: Mutex<Vec<FlushRecord<Vec<u8>, MfsValue>>>,
    }

    impl FlushBackend<Vec<u8>, MfsValue> for CollectBackend {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    fn temp_bundle_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after Unix epoch")
            .as_nanos();
        path.push(format!("mfs_mutable_bundle_{name}_{unique}.mfs"));
        path
    }

    fn write_legacy_cold_for_test(
        bundle: &MutableObjectStoreBundle,
        store: &MfsMutableObjectStore,
    ) -> io::Result<usize> {
        let records = store.snapshot_records();
        let tmp_data = bundle.root().join(TMP_DIR).join("legacy-cold-data.tmp");
        let tmp_index = bundle.root().join(TMP_DIR).join("legacy-cold-index.tmp");
        let written = write_cold_files(
            store,
            &records,
            &tmp_data,
            &tmp_index,
            &bundle.cold_data_path(),
            &bundle.cold_index_path(),
        )?;
        for record in records {
            store.evict_clean(&record.key);
        }
        Ok(written)
    }

    fn cold_manifest(generations: Vec<MutableObjectColdGeneration>) -> MutableObjectColdManifest {
        MutableObjectColdManifest {
            generations,
            tombstones: Vec::new(),
        }
    }

    #[test]
    fn bundle_recovers_checkpoint_then_wal_suffix() {
        let path = temp_bundle_path("recover");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"name".to_vec(), "Ada");
            store
                .hash_set(
                    b"profile".to_vec(),
                    b"email".to_vec(),
                    b"ada@example.com".to_vec(),
                )
                .expect("hash set");

            let checkpoint = bundle.write_checkpoint(&store)?;
            assert_eq!(checkpoint.records, 2);
            assert!(bundle.root().join(MANIFEST_FILE).exists());
            assert!(bundle.wal_path().starts_with(bundle.root().join(WAL_DIR)));

            store.set_string(b"name".to_vec(), "Grace");
            store.delete(b"profile".to_vec());
            let mut wal = bundle.open_wal()?;
            assert_eq!(store.flush_idle(&mut wal, 0, usize::MAX)?, 2);
            wal.sync_now()?;

            let recovered = bundle.recover(32)?;
            assert_eq!(recovered.checkpoint.as_ref().map(|c| c.records), Some(2));
            assert_eq!(recovered.wal_records, 2);
            assert_eq!(
                recovered.store.get_string(b"name"),
                Ok(Some("Grace".to_string()))
            );
            assert!(recovered.store.get(b"profile").is_none());

            let mut backend = CollectBackend::default();
            assert_eq!(
                recovered.store.flush_idle(&mut backend, 0, usize::MAX),
                Ok(0)
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn recovered_store_flushes_post_recovery_writes() {
        let path = temp_bundle_path("post_recovery_write");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"before".to_vec(), "checkpointed");
            bundle.write_checkpoint(&store)?;

            let recovered = bundle.recover(32)?;
            recovered.store.set_string(b"after".to_vec(), "new");

            let mut backend = CollectBackend::default();
            assert_eq!(
                recovered.store.flush_idle(&mut backend, 0, usize::MAX),
                Ok(1)
            );
            let records = backend.records.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key, b"after".to_vec());
            assert_eq!(
                records[0].value.as_deref(),
                Some(&MfsValue::String("new".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn bundle_open_removes_stale_tmp_files() {
        let path = temp_bundle_path("stale_tmp");
        let result = (|| -> io::Result<()> {
            let tmp_dir = path.join(TMP_DIR);
            fs::create_dir_all(&tmp_dir)?;
            let stale = tmp_dir.join("old.tmp");
            File::create(&stale)?.sync_all()?;
            assert!(stale.exists());

            let bundle = MutableObjectStoreBundle::open(&path)?;
            assert!(bundle.root().join(MANIFEST_FILE).exists());
            assert!(!stale.exists());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn checkpoint_retention_prunes_old_checkpoints() {
        let path = temp_bundle_path("retention");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open_with_options(
                &path,
                MutableObjectStoreBundleOptions {
                    keep_last_checkpoints: 2,
                },
            )?;
            let store = MfsMutableObjectStore::with_capacity(32);
            for i in 0..4u64 {
                store.set_string(i.to_le_bytes().to_vec(), format!("value-{i}"));
                bundle.write_checkpoint(&store)?;
            }

            let checkpoints = checkpoint_paths_desc(&bundle.checkpoint_dir())?;
            assert_eq!(checkpoints.len(), 2);
            let lsns = checkpoints
                .iter()
                .map(|path| checkpoint_lsn_from_path(path).unwrap())
                .collect::<Vec<_>>();
            assert!(lsns[0] > lsns[1]);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn recover_falls_back_when_latest_checkpoint_is_truncated() {
        let path = temp_bundle_path("corrupt_latest");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open_with_options(
                &path,
                MutableObjectStoreBundleOptions {
                    keep_last_checkpoints: 3,
                },
            )?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"name".to_vec(), "Ada");
            let first = bundle.write_checkpoint(&store)?;

            store.set_string(b"name".to_vec(), "Grace");
            let latest = bundle.write_checkpoint(&store)?;
            assert!(latest.lsn > first.lsn);
            fs::write(&latest.path, b"torn")?;

            let recovered = bundle.recover(32)?;
            assert_eq!(
                recovered.checkpoint.as_ref().map(|c| c.lsn),
                Some(first.lsn)
            );
            assert_eq!(
                recovered.store.get_string(b"name"),
                Ok(Some("Ada".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn checkpoint_lsn_ignores_read_only_clock_ticks() {
        let path = temp_bundle_path("read_clock");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"base".to_vec(), "v1");
            for _ in 0..128 {
                assert_eq!(store.get_string(b"base"), Ok(Some("v1".to_string())));
            }
            let checkpoint = bundle.write_checkpoint(&store)?;

            store.set_string(b"after".to_vec(), "v2");
            let mut wal = bundle.open_wal()?;
            assert_eq!(store.flush_idle(&mut wal, 0, usize::MAX)?, 2);
            wal.sync_now()?;

            let recovered = bundle.recover(32)?;
            assert_eq!(
                recovered.checkpoint.as_ref().map(|c| c.lsn),
                Some(checkpoint.lsn)
            );
            assert_eq!(recovered.wal_records, 1);
            assert_eq!(
                recovered.store.get_string(b"after"),
                Ok(Some("v2".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn coordinated_persistence_checkpoints_and_resets_active_wal() {
        let path = temp_bundle_path("coordinated_reset");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"before".to_vec(), "checkpointed");

            let checkpoint = persistence.checkpoint_and_reset_wal(&store)?;
            assert_eq!(checkpoint.records, 1);
            assert_eq!(fs::metadata(persistence.bundle().wal_path())?.len(), 0);

            store.set_string(b"after".to_vec(), "wal-suffix");
            assert_eq!(persistence.flush_idle(&store, 0, usize::MAX)?, 1);
            persistence.sync_now()?;

            let recovered = persistence.recover(32)?;
            assert_eq!(recovered.checkpoint.as_ref().map(|c| c.records), Some(1));
            assert_eq!(recovered.wal_records, 1);
            assert_eq!(
                recovered.store.get_string(b"before"),
                Ok(Some("checkpointed".to_string()))
            );
            assert_eq!(
                recovered.store.get_string(b"after"),
                Ok(Some("wal-suffix".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn checkpoint_recovery_preserves_ttl_metadata() {
        let path = temp_bundle_path("checkpoint_ttl");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store
                .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::String("expires".to_string()), 2)
                .unwrap();
            persistence.checkpoint_and_reset_wal(&store)?;

            let recovered = persistence.recover(32)?;
            assert_eq!(
                recovered.store.get_string(b"ttl"),
                Ok(Some("expires".to_string()))
            );
            recovered.store.load_clean(b"tick".to_vec(), MfsValue::Null);
            recovered
                .store
                .load_clean(b"tick2".to_vec(), MfsValue::Null);
            assert_eq!(recovered.store.expire(), 1);
            assert!(recovered.store.get(b"ttl").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn wal_recovery_preserves_ttl_metadata() {
        let path = temp_bundle_path("wal_ttl");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store
                .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::String("expires".to_string()), 2)
                .unwrap();
            assert_eq!(persistence.flush_idle(&store, 0, usize::MAX)?, 1);
            persistence.sync_now()?;

            let recovered = persistence.recover(32)?;
            assert_eq!(recovered.wal_records, 1);
            recovered.store.load_clean(b"tick".to_vec(), MfsValue::Null);
            recovered
                .store
                .load_clean(b"tick2".to_vec(), MfsValue::Null);
            assert_eq!(recovered.store.expire(), 1);
            assert!(recovered.store.get(b"ttl").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_manifest_write_read() {
        let path = temp_bundle_path("cold_gen_manifest");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            assert!(bundle.read_cold_manifest()?.is_none());
            assert!(!bundle.cold_manifest_path().exists());

            let mut manifest = cold_manifest(vec![
                MutableObjectColdGeneration { id: 1, records: 3 },
                MutableObjectColdGeneration { id: 2, records: 5 },
            ]);
            manifest.tombstones.push(MutableObjectColdTombstone {
                key: b"deleted".to_vec(),
                version: 9,
            });
            bundle.write_cold_manifest(&manifest)?;

            assert_eq!(bundle.read_cold_manifest()?, Some(manifest));
            assert_eq!(
                bundle.cold_manifest_path(),
                bundle.cold_dir().join("MANIFEST")
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_manifest_reads_legacy_without_tombstones() {
        let path = temp_bundle_path("cold_gen_manifest_legacy");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let mut manifest = File::create(bundle.cold_manifest_path())?;
            manifest.write_all(COLD_MANIFEST_MAGIC)?;
            manifest.write_all(&COLD_MANIFEST_VERSION.to_le_bytes())?;
            manifest.write_all(&1u64.to_le_bytes())?;
            manifest.write_all(&7u64.to_le_bytes())?;
            manifest.write_all(&11u64.to_le_bytes())?;
            manifest.sync_all()?;

            assert_eq!(
                bundle.read_cold_manifest()?,
                Some(cold_manifest(vec![MutableObjectColdGeneration {
                    id: 7,
                    records: 11,
                }]))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_manifest_atomic_publish() {
        let path = temp_bundle_path("cold_gen_atomic");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let first = cold_manifest(vec![MutableObjectColdGeneration { id: 1, records: 2 }]);
            let second = cold_manifest(vec![MutableObjectColdGeneration { id: 7, records: 11 }]);

            bundle.write_cold_manifest(&first)?;
            assert_eq!(bundle.read_cold_manifest()?, Some(first));
            bundle.write_cold_manifest(&second)?;

            assert_eq!(bundle.read_cold_manifest()?, Some(second));
            assert!(
                !bundle
                    .root()
                    .join(TMP_DIR)
                    .join("cold-MANIFEST.tmp")
                    .exists()
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_manifest_stale_tmp_does_not_corrupt_previous() {
        let path = temp_bundle_path("cold_gen_stale_tmp");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let first = cold_manifest(vec![MutableObjectColdGeneration { id: 1, records: 2 }]);
            let second = cold_manifest(vec![MutableObjectColdGeneration { id: 2, records: 4 }]);

            bundle.write_cold_manifest(&first)?;
            let tmp_path = bundle.root().join(TMP_DIR).join("cold-MANIFEST.tmp");
            let mut tmp = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp_path)?;
            tmp.write_all(b"partial junk manifest")?;
            tmp.sync_all()?;
            drop(tmp);

            assert_eq!(bundle.read_cold_manifest()?, Some(first));
            bundle.write_cold_manifest(&second)?;

            assert_eq!(bundle.read_cold_manifest()?, Some(second));
            assert!(!tmp_path.exists());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_path_helpers_are_deterministic() {
        let path = temp_bundle_path("cold_gen_paths");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;

            assert_eq!(
                bundle.cold_generation_data_path(1),
                bundle
                    .cold_dir()
                    .join("generation-00000000000000000001.mfsobj")
            );
            assert_eq!(
                bundle.cold_generation_index_path(1),
                bundle
                    .cold_dir()
                    .join("generation-00000000000000000001.mfsidx")
            );
            assert_eq!(
                bundle.cold_generation_data_path(42),
                bundle
                    .cold_dir()
                    .join("generation-00000000000000000042.mfsobj")
            );
            assert_eq!(
                bundle.cold_generation_index_path(42),
                bundle
                    .cold_dir()
                    .join("generation-00000000000000000042.mfsidx")
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_legacy_compatibility() {
        let path = temp_bundle_path("cold_gen_legacy");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"hot".to_vec(), "value");

            assert_eq!(write_legacy_cold_for_test(&bundle, &store)?, 1);
            assert!(bundle.cold_data_path().exists());
            assert!(bundle.cold_index_path().exists());
            assert!(bundle.read_cold_manifest()?.is_none());

            assert!(bundle.promote_cold_key(&store, b"hot")?);
            assert_eq!(store.get_string(b"hot"), Ok(Some("value".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_fallback_when_latest_index_is_corrupt() {
        let path = temp_bundle_path("cold_gen_corrupt_index");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "old");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.set_string(b"key".to_vec(), "new");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);
            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            let latest = manifest
                .generations
                .iter()
                .map(|generation| generation.id)
                .max()
                .expect("latest generation");
            fs::write(bundle.cold_generation_index_path(latest), b"not an index")?;

            assert!(bundle.promote_cold_key(&store, b"key")?);
            assert_eq!(store.get_string(b"key"), Ok(Some("old".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_fallback_when_latest_data_is_missing() {
        let path = temp_bundle_path("cold_gen_missing_data");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "old");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.set_string(b"key".to_vec(), "new");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);
            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            let latest = manifest
                .generations
                .iter()
                .map(|generation| generation.id)
                .max()
                .expect("latest generation");
            fs::remove_file(bundle.cold_generation_data_path(latest))?;

            assert!(bundle.promote_cold_key(&store, b"key")?);
            assert_eq!(store.get_string(b"key"), Ok(Some("old".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_missing_pair_returns_none_without_hot_delete() {
        let path = temp_bundle_path("cold_gen_missing_pair");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let legacy_store = MfsMutableObjectStore::with_capacity(32);
            legacy_store.set_string(b"key".to_vec(), "legacy");
            assert_eq!(write_legacy_cold_for_test(&bundle, &legacy_store)?, 1);

            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"hot".to_vec(), "keep");
            bundle.write_cold_manifest(&cold_manifest(vec![MutableObjectColdGeneration {
                id: 1,
                records: 1,
            }]))?;
            File::create(bundle.cold_generation_index_path(1))?.sync_all()?;

            assert!(!bundle.promote_cold_key(&store, b"key")?);
            assert!(store.get(b"key").is_none());
            assert_eq!(store.get_string(b"hot"), Ok(Some("keep".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_writes_publish_manifest_last() {
        let path = temp_bundle_path("cold_gen_manifest_last");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "old");

            let cold = bundle.write_cold_snapshot(&store)?;
            assert_eq!(cold.records, 1);
            assert_eq!(cold.data_path, bundle.cold_generation_data_path(1));
            assert_eq!(cold.index_path, bundle.cold_generation_index_path(1));
            assert!(cold.data_path.exists());
            assert!(cold.index_path.exists());
            assert!(!bundle.cold_data_path().exists());
            assert!(!bundle.cold_index_path().exists());
            assert_eq!(
                bundle.read_cold_manifest()?.expect("cold manifest"),
                cold_manifest(vec![MutableObjectColdGeneration { id: 1, records: 1 }])
            );

            store.set_string(b"key".to_vec(), "new");
            let records = store.snapshot_records();
            let (orphan, _, _) = write_cold_generation_files(&bundle, &store, &records, 2)?;
            let (value, _) = read_cold_record(&bundle, b"key")?.expect("manifest generation hit");
            assert_eq!(value, MfsValue::String("old".to_string()));

            let mut manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            manifest.generations.push(orphan);
            bundle.write_cold_manifest(&manifest)?;
            let (value, _) = read_cold_record(&bundle, b"key")?.expect("new generation hit");
            assert_eq!(value, MfsValue::String("new".to_string()));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_demotes_and_promotes_by_key() {
        let path = temp_bundle_path("cold_promote");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"hot".to_vec(), "value");
            store.set_string(b"other".to_vec(), "side");

            assert_eq!(bundle.demote_all_to_cold(&store)?, 2);
            assert!(store.is_empty());
            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            assert_eq!(manifest.generations.len(), 1);
            let generation = &manifest.generations[0];
            assert_eq!(generation.records, 2);
            assert!(bundle.cold_generation_data_path(generation.id).exists());
            assert!(bundle.cold_generation_index_path(generation.id).exists());
            assert!(!bundle.cold_data_path().exists());
            assert!(!bundle.cold_index_path().exists());

            assert!(bundle.promote_cold_key(&store, b"hot")?);
            assert_eq!(store.get_string(b"hot"), Ok(Some("value".to_string())));
            assert!(store.get(b"other").is_none());
            assert!(!bundle.promote_cold_key(&store, b"missing")?);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_preserves_ttl_metadata_on_promotion() {
        let path = temp_bundle_path("cold_ttl");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store
                .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::String("cold".to_string()), 2)
                .unwrap();

            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);
            assert!(bundle.promote_cold_key(&store, b"ttl")?);
            store.load_clean(b"tick".to_vec(), MfsValue::Null);
            store.load_clean(b"tick2".to_vec(), MfsValue::Null);
            assert_eq!(store.expire(), 1);
            assert!(store.get(b"ttl").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_read_through_promotes_on_miss() {
        let path = temp_bundle_path("cold_read_through");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"hot".to_vec(), "value");

            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);
            assert!(store.get(b"hot").is_none());

            let value = bundle
                .get_with_cold_promotion(&store, b"hot")?
                .expect("cold value should promote");
            assert_eq!(value.as_ref(), &MfsValue::String("value".to_string()));
            assert_eq!(store.get_string(b"hot"), Ok(Some("value".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_read_through_returns_none_for_missing_key() {
        let path = temp_bundle_path("cold_missing");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);

            assert!(
                bundle
                    .get_with_cold_promotion(&store, b"missing")?
                    .is_none()
            );
            assert!(!bundle.promote_cold_key(&store, b"missing")?);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn persistence_read_through_promotes_cold_hit() {
        let path = temp_bundle_path("persistence_read_through_hit");
        let result = (|| -> io::Result<()> {
            let persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"hot".to_vec(), MfsValue::String("value".to_string()));

            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);
            assert!(store.get(b"hot").is_none());
            let before = store.stats();
            assert_eq!(before.len, 0);
            assert_eq!(before.dirty, 0);

            let value = persistence
                .get_value_with_cold_promotion(&store, b"hot")?
                .expect("cold value should promote through persistence wrapper");
            assert_eq!(value.as_ref(), &MfsValue::String("value".to_string()));
            assert_eq!(store.stats().dirty, before.dirty);
            assert_eq!(store.stats().len, 1);
            assert_eq!(store.get_string(b"hot"), Ok(Some("value".to_string())));
            assert_eq!(
                persistence
                    .get_string_with_cold_promotion(&store, b"hot")?
                    .expect("promoted value should be a string"),
                Some("value".to_string())
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn mutable_get_does_not_touch_cold_tier() {
        let path = temp_bundle_path("mutable_get_no_cold_touch");
        let result = (|| -> io::Result<()> {
            let persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"cold".to_vec(), MfsValue::String("value".to_string()));
            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);

            let before = store.stats();
            assert_eq!(store.get(b"cold"), None);
            assert_eq!(store.stats().len, before.len);
            assert_eq!(store.stats().dirty, before.dirty);

            let value = persistence
                .get_value_with_cold_promotion(&store, b"cold")?
                .expect("persistence wrapper should promote cold-only key");
            assert_eq!(value.as_ref(), &MfsValue::String("value".to_string()));
            assert_eq!(store.get_string(b"cold"), Ok(Some("value".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn persistence_read_through_missing_key_preserves_hot_stats() {
        let path = temp_bundle_path("persistence_read_through_missing");
        let result = (|| -> io::Result<()> {
            let persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"present".to_vec(), MfsValue::String("value".to_string()));
            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);

            let before = store.stats();
            assert!(
                persistence
                    .get_value_with_cold_promotion(&store, b"missing")?
                    .is_none()
            );
            assert_eq!(
                persistence
                    .get_string_with_cold_promotion(&store, b"missing")?
                    .expect("missing key should not be a wrong type"),
                None
            );
            let after = store.stats();
            assert_eq!(after.len, before.len);
            assert_eq!(after.dirty, before.dirty);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn persistence_read_through_string_reports_wrong_type() {
        let path = temp_bundle_path("persistence_read_through_wrong_type");
        let result = (|| -> io::Result<()> {
            let persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"number".to_vec(), MfsValue::Integer(7));
            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);

            let result = persistence.get_string_with_cold_promotion(&store, b"number")?;
            assert_eq!(
                result,
                Err(ObjectStoreError::WrongType {
                    expected: "string",
                    actual: MfsValue::Integer(7).tag(),
                })
            );
            assert_eq!(store.get(b"number").as_deref(), Some(&MfsValue::Integer(7)));
            assert_eq!(store.stats().dirty, 0);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_promotion_does_not_resurrect_deleted_key() {
        let path = temp_bundle_path("cold_delete_guard");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "old");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.delete(b"key".to_vec());

            assert!(!bundle.promote_cold_key(&store, b"key")?);
            assert!(bundle.get_with_cold_promotion(&store, b"key")?.is_none());
            assert!(store.get(b"key").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_promotion_does_not_overwrite_newer_hot_value() {
        let path = temp_bundle_path("cold_newer_hot_guard");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "old");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.set_string(b"key".to_vec(), "new");

            assert!(!bundle.promote_cold_key(&store, b"key")?);
            assert_eq!(
                bundle.get_with_cold_promotion(&store, b"key")?.as_deref(),
                Some(&MfsValue::String("new".to_string()))
            );
            assert_eq!(store.get_string(b"key"), Ok(Some("new".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_generation_newer_hot_wins() {
        let path = temp_bundle_path("cold_generation_newer_hot");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "v1");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.set_string(b"key".to_vec(), "v2");

            assert!(!bundle.promote_cold_key(&store, b"key")?);
            assert_eq!(
                bundle.get_with_cold_promotion(&store, b"key")?.as_deref(),
                Some(&MfsValue::String("v2".to_string()))
            );
            assert_eq!(store.get_string(b"key"), Ok(Some("v2".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tombstone_survives_restart() {
        let path = temp_bundle_path("cold_tombstone_restart");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "v1");
            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);

            let delete_version = store.delete(b"key".to_vec());
            assert_eq!(persistence.flush_idle(&store, 0, usize::MAX)?, 1);
            persistence.sync_now()?;

            let manifest = persistence
                .bundle()
                .read_cold_manifest()?
                .expect("cold manifest");
            assert_eq!(manifest.generations.len(), 1);
            assert_eq!(
                manifest.tombstones,
                vec![MutableObjectColdTombstone {
                    key: b"key".to_vec(),
                    version: delete_version,
                }]
            );

            let recovered = persistence.recover(32)?;
            assert_eq!(recovered.wal_records, 1);
            assert!(recovered.store.get(b"key").is_none());
            assert!(
                persistence
                    .bundle()
                    .get_with_cold_promotion(&recovered.store, b"key")?
                    .is_none()
            );
            assert!(recovered.store.get(b"key").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tombstone_blocks_legacy_fallback() {
        let path = temp_bundle_path("cold_tombstone_legacy");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "legacy");
            assert_eq!(write_legacy_cold_for_test(persistence.bundle(), &store)?, 1);
            assert!(persistence.bundle().cold_data_path().exists());
            assert!(persistence.bundle().cold_index_path().exists());
            assert!(persistence.bundle().read_cold_manifest()?.is_none());

            let delete_version = store.delete(b"key".to_vec());
            assert_eq!(persistence.flush_idle(&store, 0, usize::MAX)?, 1);
            persistence.sync_now()?;

            let manifest = persistence
                .bundle()
                .read_cold_manifest()?
                .expect("cold manifest");
            assert!(manifest.generations.is_empty());
            assert_eq!(
                manifest.tombstones,
                vec![MutableObjectColdTombstone {
                    key: b"key".to_vec(),
                    version: delete_version,
                }]
            );

            let recovered = persistence.recover(32)?;
            assert!(
                persistence
                    .bundle()
                    .get_with_cold_promotion(&recovered.store, b"key")?
                    .is_none()
            );
            assert!(recovered.store.get(b"key").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_tier_read_through_skips_expired_ttl_record() {
        let path = temp_bundle_path("cold_expired_ttl");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store
                .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::String("cold".to_string()), 2)
                .unwrap();
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            store.load_clean(b"tick".to_vec(), MfsValue::Null);
            store.load_clean(b"tick2".to_vec(), MfsValue::Null);

            assert!(bundle.get_with_cold_promotion(&store, b"ttl")?.is_none());
            assert!(!bundle.promote_cold_key(&store, b"ttl")?);
            assert!(store.get(b"ttl").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_gc_drops_expired_records() {
        let path = temp_bundle_path("cold_gc_expired");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store
                .put_with_ttl_ticks(b"expired".to_vec(), MfsValue::String("old".to_string()), 4)
                .unwrap();
            store.set_string(b"live".to_vec(), "keep");

            assert_eq!(bundle.demote_all_to_cold(&store)?, 2);
            store.load_clean(b"tick-a".to_vec(), MfsValue::Null);
            store.load_clean(b"tick-b".to_vec(), MfsValue::Null);

            let report = bundle.gc_cold_tier(&store)?;
            assert_eq!(report.generations_scanned, 1);
            assert_eq!(report.records_scanned, 2);
            assert_eq!(report.records_kept, 1);
            assert_eq!(report.records_dropped_expired, 1);
            assert_eq!(report.generations_removed, 1);
            assert!(report.bytes_freed > 0);

            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            assert_eq!(manifest.generations.len(), 1);
            assert_eq!(manifest.generations[0].records, 1);
            assert!(bundle.promote_cold_key(&store, b"live")?);
            assert_eq!(store.get_string(b"live"), Ok(Some("keep".to_string())));
            assert!(!bundle.promote_cold_key(&store, b"expired")?);
            assert!(store.get(b"expired").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_gc_preserves_active_generation() {
        let path = temp_bundle_path("cold_gc_active_generation");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"a".to_vec(), "one");
            store.set_string(b"b".to_vec(), "two");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 2);
            let old_manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            let old_generation = old_manifest.generations[0].id;

            let report = bundle.gc_cold_tier(&store)?;
            assert_eq!(report.generations_scanned, 1);
            assert_eq!(report.records_scanned, 2);
            assert_eq!(report.records_kept, 2);
            assert_eq!(report.generations_removed, 1);

            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            assert_eq!(manifest.generations.len(), 1);
            let generation = &manifest.generations[0];
            assert_eq!(generation.records, 2);
            assert!(generation.id > old_generation);
            assert!(bundle.cold_generation_data_path(generation.id).exists());
            assert!(bundle.cold_generation_index_path(generation.id).exists());
            assert!(!bundle.cold_generation_data_path(old_generation).exists());
            assert!(!bundle.cold_generation_index_path(old_generation).exists());

            assert!(bundle.promote_cold_key(&store, b"a")?);
            assert_eq!(store.get_string(b"a"), Ok(Some("one".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_gc_after_restart_preserves_visible_values() {
        let path = temp_bundle_path("cold_gc_restart_visibility");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "v1");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);
            store.set_string(b"key".to_vec(), "v2");
            assert_eq!(bundle.demote_all_to_cold(&store)?, 1);

            let report = bundle.gc_cold_tier(&store)?;
            assert_eq!(report.generations_scanned, 2);
            assert_eq!(report.records_scanned, 2);
            assert_eq!(report.records_kept, 1);
            assert_eq!(report.generations_removed, 2);

            let reopened = MutableObjectStoreBundle::open(&path)?;
            let recovered = reopened.recover(32)?;
            assert!(recovered.store.get(b"key").is_none());
            let value = reopened
                .get_with_cold_promotion(&recovered.store, b"key")?
                .expect("compacted cold value should promote after restart");
            assert_eq!(value.as_ref(), &MfsValue::String("v2".to_string()));
            assert_eq!(
                recovered.store.get_string(b"key"),
                Ok(Some("v2".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_gc_retains_tombstones() {
        let path = temp_bundle_path("cold_gc_tombstones");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "stale");
            assert_eq!(persistence.bundle().demote_all_to_cold(&store)?, 1);
            let delete_version = store.delete(b"key".to_vec());
            assert_eq!(persistence.flush_idle(&store, 0, usize::MAX)?, 1);
            persistence.sync_now()?;

            let report = persistence.bundle().gc_cold_tier(&store)?;
            assert_eq!(report.generations_scanned, 1);
            assert_eq!(report.records_scanned, 1);
            assert_eq!(report.records_kept, 0);
            assert_eq!(report.generations_removed, 1);

            let mut manifest = persistence
                .bundle()
                .read_cold_manifest()?
                .expect("cold manifest");
            assert!(manifest.generations.is_empty());
            assert_eq!(
                manifest.tombstones,
                vec![MutableObjectColdTombstone {
                    key: b"key".to_vec(),
                    version: delete_version,
                }]
            );

            let stale_generation_id = next_cold_generation_id(&manifest)?;
            let stale_entry = ColdRecordEntry {
                key: b"key".to_vec(),
                value: MfsValue::String("stale".to_string()),
                meta: MutableObjectExpiryMeta {
                    version: delete_version.saturating_sub(1),
                    last_touch: delete_version.saturating_sub(1),
                    expires_at: 0,
                    tti_ticks: 0,
                },
            };
            let (generation, _, _) = write_cold_generation_entries(
                persistence.bundle(),
                &[stale_entry],
                stale_generation_id,
            )?;
            manifest.generations.push(generation);
            persistence.bundle().write_cold_manifest(&manifest)?;

            assert!(
                persistence
                    .bundle()
                    .get_with_cold_promotion(&store, b"key")?
                    .is_none()
            );
            assert!(store.get(b"key").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn cold_gc_ignores_legacy_without_manifest() {
        let path = temp_bundle_path("cold_gc_legacy_no_manifest");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"legacy".to_vec(), "value");
            assert_eq!(write_legacy_cold_for_test(&bundle, &store)?, 1);
            assert!(bundle.cold_data_path().exists());
            assert!(bundle.cold_index_path().exists());
            assert!(bundle.read_cold_manifest()?.is_none());

            let report = bundle.gc_cold_tier(&store)?;
            assert_eq!(report, MutableObjectColdGcReport::default());
            assert!(bundle.cold_data_path().exists());
            assert!(bundle.cold_index_path().exists());
            assert!(bundle.read_cold_manifest()?.is_none());
            assert!(bundle.promote_cold_key(&store, b"legacy")?);
            assert_eq!(store.get_string(b"legacy"), Ok(Some("value".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn tiering_policy_skips_dirty_keys() {
        let path = temp_bundle_path("tiering_skip_dirty");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"dirty".to_vec(), "keep-hot");
            store.load_clean(b"clean".to_vec(), MfsValue::String("move-cold".to_string()));

            let report = bundle.demote_by_policy(
                &store,
                TieringPolicy {
                    idle_threshold_ticks: 0,
                    max_records: 16,
                    hot_capacity_soft_limit: None,
                    min_clean_age_ticks: 0,
                },
            )?;

            assert_eq!(report.attempted, 1);
            assert_eq!(report.demoted, 1);
            assert_eq!(report.skipped_dirty, 1);
            assert_eq!(report.flush_records, 0);
            assert_eq!(store.stats().dirty, 1);
            assert_eq!(store.get_string(b"dirty"), Ok(Some("keep-hot".to_string())));
            assert!(store.get(b"clean").is_none());
            assert!(read_cold_record(&bundle, b"dirty")?.is_none());
            assert!(bundle.promote_cold_key(&store, b"clean")?);
            assert_eq!(
                store.get_string(b"clean"),
                Ok(Some("move-cold".to_string()))
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn tiering_policy_failed_write_keeps_hot() {
        let path = temp_bundle_path("tiering_failed_write");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"key".to_vec(), MfsValue::String("still-hot".to_string()));
            fs::create_dir_all(bundle.cold_generation_data_path(1))?;

            let error = bundle
                .demote_by_policy(
                    &store,
                    TieringPolicy {
                        idle_threshold_ticks: 0,
                        max_records: 1,
                        hot_capacity_soft_limit: None,
                        min_clean_age_ticks: 0,
                    },
                )
                .expect_err("directory at generation path should fail cold publish");

            assert!(error.kind() != io::ErrorKind::NotFound);
            assert_eq!(store.get_string(b"key"), Ok(Some("still-hot".to_string())));
            assert!(bundle.read_cold_manifest()?.is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn tiering_policy_failed_evict_blocks_stale_cold_copy() {
        let path = temp_bundle_path("tiering_failed_evict_blocks_stale");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"key".to_vec(), MfsValue::String("v1".to_string()));
            let stale_meta = store.expiry_meta(b"key").expect("clean v1 metadata");
            let stale_candidate = MutableObjectTieringRecord {
                key: b"key".to_vec(),
                value: Arc::new(MfsValue::String("v1".to_string())),
                meta: stale_meta,
                pending_dirty: false,
            };
            let stale_entry = cold_entry_from_tiering_record(&stale_candidate);
            publish_cold_entries(&bundle, &[stale_entry])?.expect("cold generation");

            store.set_string(b"key".to_vec(), "v2");
            assert_eq!(
                evict_published_tiering_candidates(&bundle, &store, &[stale_candidate])?,
                0
            );

            let manifest = bundle.read_cold_manifest()?.expect("cold manifest");
            assert_eq!(
                manifest.tombstones,
                vec![MutableObjectColdTombstone {
                    key: b"key".to_vec(),
                    version: stale_meta.version,
                }]
            );
            assert_eq!(
                bundle.get_with_cold_promotion(&store, b"key")?.as_deref(),
                Some(&MfsValue::String("v2".to_string()))
            );

            let newer_meta = store.expiry_meta(b"key").expect("dirty v2 metadata");
            store.load_clean_delete_versioned(b"key".to_vec(), newer_meta.version + 1);
            assert!(bundle.get_with_cold_promotion(&store, b"key")?.is_none());
            assert!(store.get(b"key").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn tiering_policy_demotes_idle_clean_keys() {
        let path = temp_bundle_path("tiering_idle_clean");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.load_clean(b"idle".to_vec(), MfsValue::String("cold".to_string()));
            store.load_clean(b"recent".to_vec(), MfsValue::String("hot".to_string()));

            let report = bundle.demote_by_policy(
                &store,
                TieringPolicy {
                    idle_threshold_ticks: 2,
                    max_records: 4,
                    hot_capacity_soft_limit: None,
                    min_clean_age_ticks: 0,
                },
            )?;

            assert_eq!(report.attempted, 1);
            assert_eq!(report.demoted, 1);
            assert_eq!(report.skipped_recent, 1);
            assert!(store.get(b"idle").is_none());
            assert_eq!(store.get_string(b"recent"), Ok(Some("hot".to_string())));

            let value = bundle
                .get_with_cold_promotion(&store, b"idle")?
                .expect("idle key should promote from cold tier");
            assert_eq!(value.as_ref(), &MfsValue::String("cold".to_string()));
            assert_eq!(store.get_string(b"idle"), Ok(Some("cold".to_string())));
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn persistence_tiering_policy_flushes_before_demotion() {
        let path = temp_bundle_path("persistence_tiering_flush");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "durable-cold");

            let report = persistence.demote_by_policy(
                &store,
                TieringPolicy {
                    idle_threshold_ticks: 0,
                    max_records: 1,
                    hot_capacity_soft_limit: None,
                    min_clean_age_ticks: 0,
                },
            )?;

            assert_eq!(report.flush_records, 1);
            assert_eq!(report.attempted, 1);
            assert_eq!(report.demoted, 1);
            assert_eq!(report.skipped_dirty, 0);
            assert_eq!(store.stats().dirty, 0);
            assert!(store.get(b"key").is_none());

            let value = persistence
                .bundle()
                .get_with_cold_promotion(&store, b"key")?
                .expect("flushed key should promote from cold tier");
            assert_eq!(
                value.as_ref(),
                &MfsValue::String("durable-cold".to_string())
            );
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn checkpoint_after_delete_does_not_resurrect_old_value() {
        let path = temp_bundle_path("delete_checkpoint");
        let result = (|| -> io::Result<()> {
            let mut persistence = MutableObjectStorePersistence::open_with_options(
                &path,
                MutableObjectStoreBundleOptions {
                    keep_last_checkpoints: 2,
                },
            )?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"key".to_vec(), "live");
            let first = persistence.checkpoint_and_reset_wal(&store)?;

            store.delete(b"key".to_vec());
            let second = persistence.checkpoint_and_reset_wal(&store)?;
            assert!(second.lsn > first.lsn);
            assert_eq!(second.records, 0);

            let recovered = persistence.recover(32)?;
            assert_eq!(
                recovered.checkpoint.as_ref().map(|c| c.lsn),
                Some(second.lsn)
            );
            assert!(recovered.store.get(b"key").is_none());
            assert_eq!(recovered.wal_records, 0);
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[test]
    fn recover_rejects_partial_checkpoint_even_when_max_lsn_survives() {
        let path = temp_bundle_path("partial_max_lsn");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open_with_options(
                &path,
                MutableObjectStoreBundleOptions {
                    keep_last_checkpoints: 3,
                },
            )?;
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"old".to_vec(), "fallback");
            let first = bundle.write_checkpoint(&store)?;

            store.set_string(b"z".to_vec(), "lower-lsn-later-key");
            store.set_string(b"a".to_vec(), "max-lsn-first-key");
            let latest = bundle.write_checkpoint(&store)?;
            assert!(latest.lsn > first.lsn);

            let bytes = fs::read(&latest.path)?;
            let payload_len = u32::from_le_bytes(bytes[4..8].try_into().expect("payload length"));
            let first_record_len = 8 + payload_len as usize + 4;
            fs::write(&latest.path, &bytes[..first_record_len])?;

            let recovered = bundle.recover(32)?;
            assert_eq!(
                recovered.checkpoint.as_ref().map(|c| c.lsn),
                Some(first.lsn)
            );
            assert_eq!(
                recovered.store.get_string(b"old"),
                Ok(Some("fallback".to_string()))
            );
            assert!(recovered.store.get(b"a").is_none());
            assert!(recovered.store.get(b"z").is_none());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn bundle_open_rejects_symlinked_subdirs() {
        use std::os::unix::fs::symlink;

        let path = temp_bundle_path("symlink");
        let outside = temp_bundle_path("outside");
        let result = (|| -> io::Result<()> {
            fs::create_dir_all(&path)?;
            fs::create_dir_all(&outside)?;
            symlink(&outside, path.join(TMP_DIR))?;
            assert!(MutableObjectStoreBundle::open(&path).is_err());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        let _ = fs::remove_dir_all(&outside);
        result.unwrap();
    }

    #[test]
    fn empty_bundle_recovers_empty_store() {
        let path = temp_bundle_path("empty");
        let result = (|| -> io::Result<()> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let recovered = bundle.recover(8)?;
            assert!(recovered.checkpoint.is_none());
            assert_eq!(recovered.wal_records, 0);
            assert!(recovered.store.is_empty());
            Ok(())
        })();
        let _ = fs::remove_dir_all(&path);
        result.unwrap();
    }
}
