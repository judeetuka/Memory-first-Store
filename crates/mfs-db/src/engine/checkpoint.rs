use crate::engine::raw::{RawCollectionSnapshot, RawEngineSnapshot, RawSnapshotRecord};
use crate::engine::{
    CheckpointCorruptionKind, DocumentVersion, EngineConfig, EngineError, EngineResult, Lsn,
    NoSqlEngine, RawKey, RawValue, RawWalReplayStats, replay_raw_wal_after,
};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

pub(crate) const RAW_CHECKPOINT_MAGIC: u32 = u32::from_le_bytes(*b"MFSC");
pub const RAW_CHECKPOINT_FORMAT_VERSION: u16 = 1;

const RAW_CHECKPOINT_EXTENSION: &str = "mfschkp";
const RAW_CHECKPOINT_TMP_EXTENSION: &str = "mfschkp.tmp";
const HEADER_LEN: usize = 14;
const CHECKSUM_LEN: usize = 4;
const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_CHECKPOINT_FIELD_BYTES: usize = 64 * 1024 * 1024;
const MIN_COLLECTION_PAYLOAD_BYTES: usize = 12;
const MIN_RECORD_PAYLOAD_BYTES: usize = 13;
const RECORD_TOMBSTONE: u8 = 0;
const RECORD_VALUE: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCheckpointCollectionMetadata {
    pub name: String,
    pub record_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCheckpointMetadata {
    pub format_version: u16,
    pub checkpoint_lsn: Lsn,
    pub engine_max_collections: usize,
    pub engine_raw_initial_capacity: usize,
    pub collection_count: usize,
    pub record_count: usize,
    pub collections: Vec<RawCheckpointCollectionMetadata>,
}

#[derive(Debug, Clone)]
pub struct RawCheckpointSource {
    pub path: PathBuf,
    pub metadata: RawCheckpointMetadata,
}

#[derive(Debug, Clone)]
pub struct RawCheckpointLoad {
    pub path: PathBuf,
    pub metadata: RawCheckpointMetadata,
    pub engine: NoSqlEngine,
}

#[derive(Debug, Clone)]
pub struct RawRecovery {
    pub engine: NoSqlEngine,
    pub checkpoint: Option<RawCheckpointSource>,
    pub wal: RawWalReplayStats,
}

struct DecodedRawCheckpoint {
    metadata: RawCheckpointMetadata,
    snapshot: RawEngineSnapshot,
}

pub fn raw_checkpoint_path(dir: impl AsRef<Path>, checkpoint_lsn: Lsn) -> PathBuf {
    let mut path = dir.as_ref().to_path_buf();
    path.push(format!(
        "raw-{:020}.{}",
        checkpoint_lsn.get(),
        RAW_CHECKPOINT_EXTENSION
    ));
    path
}

pub fn write_raw_checkpoint_to_dir(
    dir: impl AsRef<Path>,
    engine: &NoSqlEngine,
    checkpoint_lsn: Lsn,
) -> EngineResult<RawCheckpointMetadata> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir).map_err(|e| checkpoint_io(dir, "create checkpoint directory", e))?;
    let path = raw_checkpoint_path(dir, checkpoint_lsn);
    write_raw_checkpoint(path, engine, checkpoint_lsn)
}

pub fn write_raw_checkpoint(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
    checkpoint_lsn: Lsn,
) -> EngineResult<RawCheckpointMetadata> {
    let path = path.as_ref();
    let snapshot = engine.raw_snapshot();
    let metadata = metadata_for_snapshot(engine.config(), checkpoint_lsn, &snapshot);
    let bytes = encode_checkpoint(&metadata, &snapshot)?;
    let tmp_path = temp_path_for(path);

    let result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| checkpoint_io(&tmp_path, "open temporary checkpoint", e))?;
        file.write_all(&bytes)
            .map_err(|e| checkpoint_io(&tmp_path, "write temporary checkpoint", e))?;
        file.sync_all()
            .map_err(|e| checkpoint_io(&tmp_path, "sync temporary checkpoint", e))?;
        drop(file);

        fs::rename(&tmp_path, path).map_err(|e| checkpoint_io(path, "rename checkpoint", e))?;
        sync_parent_dir(path)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result.map(|()| metadata)
}

pub fn read_raw_checkpoint_metadata(path: impl AsRef<Path>) -> EngineResult<RawCheckpointMetadata> {
    read_decoded_checkpoint(path.as_ref()).map(|decoded| decoded.metadata)
}

pub fn load_latest_raw_checkpoint(
    dir: impl AsRef<Path>,
    config: EngineConfig,
) -> EngineResult<Option<RawCheckpointLoad>> {
    let dir = dir.as_ref();
    if !dir.exists() {
        return Ok(None);
    }

    let entries =
        fs::read_dir(dir).map_err(|e| checkpoint_io(dir, "read checkpoint directory", e))?;
    let mut best: Option<(PathBuf, DecodedRawCheckpoint)> = None;

    for entry in entries {
        let entry = entry.map_err(|e| checkpoint_io(dir, "read checkpoint directory entry", e))?;
        let path = entry.path();
        if !is_raw_checkpoint_file(&path) {
            continue;
        }

        match read_decoded_checkpoint(&path) {
            Ok(decoded) => {
                let replace = best.as_ref().is_none_or(|(_, current)| {
                    decoded.metadata.checkpoint_lsn > current.metadata.checkpoint_lsn
                });
                if replace {
                    best = Some((path, decoded));
                }
            }
            Err(EngineError::CheckpointCorruption { .. }) => continue,
            Err(err) => return Err(err),
        }
    }

    let Some((path, decoded)) = best else {
        return Ok(None);
    };
    let engine = NoSqlEngine::from_raw_snapshot(config, decoded.snapshot)?;
    Ok(Some(RawCheckpointLoad {
        path,
        metadata: decoded.metadata,
        engine,
    }))
}

pub fn recover_raw_checkpoint_then_wal(
    checkpoint_dir: impl AsRef<Path>,
    wal_path: impl AsRef<Path>,
    config: EngineConfig,
) -> EngineResult<RawRecovery> {
    let loaded = load_latest_raw_checkpoint(checkpoint_dir, config.clone())?;
    let (engine, checkpoint, after_lsn) = match loaded {
        Some(load) => {
            let after_lsn = load.metadata.checkpoint_lsn;
            let checkpoint = RawCheckpointSource {
                path: load.path,
                metadata: load.metadata,
            };
            (load.engine, Some(checkpoint), after_lsn)
        }
        None => (NoSqlEngine::open_memory(config)?, None, Lsn::ZERO),
    };

    let wal = replay_raw_wal_after(wal_path, &engine, after_lsn)?;
    Ok(RawRecovery {
        engine,
        checkpoint,
        wal,
    })
}

fn metadata_for_snapshot(
    config: &EngineConfig,
    checkpoint_lsn: Lsn,
    snapshot: &RawEngineSnapshot,
) -> RawCheckpointMetadata {
    let collections = snapshot
        .collections
        .iter()
        .map(|collection| RawCheckpointCollectionMetadata {
            name: collection.name.clone(),
            record_count: collection.records.len(),
        })
        .collect::<Vec<_>>();
    let record_count = collections
        .iter()
        .map(|collection| collection.record_count)
        .sum();

    RawCheckpointMetadata {
        format_version: RAW_CHECKPOINT_FORMAT_VERSION,
        checkpoint_lsn,
        engine_max_collections: config.max_collections,
        engine_raw_initial_capacity: config.raw_initial_capacity,
        collection_count: collections.len(),
        record_count,
        collections,
    }
}

fn encode_checkpoint(
    metadata: &RawCheckpointMetadata,
    snapshot: &RawEngineSnapshot,
) -> EngineResult<Vec<u8>> {
    let mut payload = Vec::with_capacity(256);
    payload.extend_from_slice(&metadata.checkpoint_lsn.get().to_le_bytes());
    write_u64(metadata.engine_max_collections, &mut payload)?;
    write_u64(metadata.engine_raw_initial_capacity, &mut payload)?;
    write_u32(snapshot.collections.len(), &mut payload)?;
    write_u64(metadata.record_count, &mut payload)?;

    for collection in &snapshot.collections {
        write_len_prefixed(collection.name.as_bytes(), &mut payload)?;
        write_u64(collection.records.len(), &mut payload)?;
        for record in &collection.records {
            payload.extend_from_slice(&record.version.get().to_le_bytes());
            write_len_prefixed(record.key.as_bytes(), &mut payload)?;
            match &record.value {
                Some(value) => {
                    payload.push(RECORD_VALUE);
                    write_len_prefixed(value.as_bytes(), &mut payload)?;
                }
                None => payload.push(RECORD_TOMBSTONE),
            }
        }
    }

    if payload.len() > MAX_CHECKPOINT_PAYLOAD_BYTES {
        return Err(corrupt(
            Path::new("<encode>"),
            CheckpointCorruptionKind::PayloadTooLarge,
        ));
    }

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + CHECKSUM_LEN);
    out.extend_from_slice(&RAW_CHECKPOINT_MAGIC.to_le_bytes());
    out.extend_from_slice(&RAW_CHECKPOINT_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&payload);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn read_decoded_checkpoint(path: &Path) -> EngineResult<DecodedRawCheckpoint> {
    let mut file = File::open(path).map_err(|e| checkpoint_io(path, "open checkpoint", e))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| checkpoint_io(path, "read checkpoint", e))?;
    decode_checkpoint(path, &bytes)
}

fn decode_checkpoint(path: &Path, bytes: &[u8]) -> EngineResult<DecodedRawCheckpoint> {
    if bytes.len() < HEADER_LEN + CHECKSUM_LEN {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }

    let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("magic length"));
    if magic != RAW_CHECKPOINT_MAGIC {
        return Err(corrupt(path, CheckpointCorruptionKind::BadMagic));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("version length"));
    if version != RAW_CHECKPOINT_FORMAT_VERSION {
        return Err(corrupt(
            path,
            CheckpointCorruptionKind::UnknownFormatVersion,
        ));
    }
    let payload_len = u64::from_le_bytes(bytes[6..14].try_into().expect("payload length"));
    if payload_len > MAX_CHECKPOINT_PAYLOAD_BYTES as u64 {
        return Err(corrupt(path, CheckpointCorruptionKind::PayloadTooLarge));
    }
    let payload_len = payload_len as usize;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .and_then(|len| len.checked_add(CHECKSUM_LEN))
        .ok_or_else(|| corrupt(path, CheckpointCorruptionKind::PayloadTooLarge))?;
    if bytes.len() != expected_len {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }

    let checksum_offset = HEADER_LEN + payload_len;
    let stored_crc = u32::from_le_bytes(
        bytes[checksum_offset..checksum_offset + CHECKSUM_LEN]
            .try_into()
            .expect("checksum length"),
    );
    let crc = crc32c::crc32c(&bytes[..checksum_offset]);
    if crc != stored_crc {
        return Err(corrupt(path, CheckpointCorruptionKind::ChecksumMismatch));
    }

    decode_payload(path, version, &bytes[HEADER_LEN..checksum_offset])
}

fn decode_payload(path: &Path, version: u16, payload: &[u8]) -> EngineResult<DecodedRawCheckpoint> {
    let mut cursor = 0usize;
    let checkpoint_lsn = Lsn::new(read_u64(payload, &mut cursor, path)?);
    let engine_max_collections = read_usize_from_u64(payload, &mut cursor, path)?;
    let engine_raw_initial_capacity = read_usize_from_u64(payload, &mut cursor, path)?;
    let collection_count = read_u32(payload, &mut cursor, path)? as usize;
    let record_count = read_usize_from_u64(payload, &mut cursor, path)?;
    validate_collection_count(path, collection_count, payload.len().saturating_sub(cursor))?;
    validate_record_count(path, record_count, payload.len().saturating_sub(cursor))?;

    let mut collections = Vec::with_capacity(collection_count);
    let mut collection_metadata = Vec::with_capacity(collection_count);
    let mut decoded_records = 0usize;

    for _ in 0..collection_count {
        let collection_name_bytes = read_bytes(payload, &mut cursor, path)?;
        let collection_name = String::from_utf8(collection_name_bytes.to_vec())
            .map_err(|_| corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint))?;
        if collection_name.is_empty() {
            return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
        }

        let collection_record_count = read_usize_from_u64(payload, &mut cursor, path)?;
        let remaining = payload.len().saturating_sub(cursor);
        validate_record_count(path, collection_record_count, remaining)?;
        decoded_records = decoded_records
            .checked_add(collection_record_count)
            .filter(|count| *count <= record_count)
            .ok_or_else(|| corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint))?;
        let mut records = Vec::with_capacity(collection_record_count);
        for _ in 0..collection_record_count {
            let record_version = DocumentVersion::new(read_u64(payload, &mut cursor, path)?);
            if record_version == DocumentVersion::ZERO {
                return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
            }
            let key = RawKey::from(read_bytes(payload, &mut cursor, path)?.to_vec());
            let marker = read_u8(payload, &mut cursor, path)?;
            let value = match marker {
                RECORD_TOMBSTONE => None,
                RECORD_VALUE => Some(RawValue::from(
                    read_bytes(payload, &mut cursor, path)?.to_vec(),
                )),
                _ => return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint)),
            };
            records.push(RawSnapshotRecord {
                key,
                value,
                version: record_version,
            });
        }
        collection_metadata.push(RawCheckpointCollectionMetadata {
            name: collection_name.clone(),
            record_count: records.len(),
        });
        collections.push(RawCollectionSnapshot {
            name: collection_name,
            records,
        });
    }

    if cursor != payload.len() || decoded_records != record_count {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }

    Ok(DecodedRawCheckpoint {
        metadata: RawCheckpointMetadata {
            format_version: version,
            checkpoint_lsn,
            engine_max_collections,
            engine_raw_initial_capacity,
            collection_count,
            record_count,
            collections: collection_metadata,
        },
        snapshot: RawEngineSnapshot { collections },
    })
}

fn write_u32(value: usize, out: &mut Vec<u8>) -> EngineResult<()> {
    let value = u32::try_from(value).map_err(|_| {
        corrupt(
            Path::new("<encode>"),
            CheckpointCorruptionKind::FieldTooLarge,
        )
    })?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_u64(value: usize, out: &mut Vec<u8>) -> EngineResult<()> {
    let value = u64::try_from(value).map_err(|_| {
        corrupt(
            Path::new("<encode>"),
            CheckpointCorruptionKind::FieldTooLarge,
        )
    })?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_len_prefixed(bytes: &[u8], out: &mut Vec<u8>) -> EngineResult<()> {
    if bytes.len() > MAX_CHECKPOINT_FIELD_BYTES {
        return Err(corrupt(
            Path::new("<encode>"),
            CheckpointCorruptionKind::FieldTooLarge,
        ));
    }
    write_u32(bytes.len(), out)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_u8(payload: &[u8], cursor: &mut usize, path: &Path) -> EngineResult<u8> {
    if payload.len().saturating_sub(*cursor) < 1 {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }
    let value = payload[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u32(payload: &[u8], cursor: &mut usize, path: &Path) -> EngineResult<u32> {
    Ok(u32::from_le_bytes(read_exact::<4>(payload, cursor, path)?))
}

fn read_u64(payload: &[u8], cursor: &mut usize, path: &Path) -> EngineResult<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(payload, cursor, path)?))
}

fn read_usize_from_u64(payload: &[u8], cursor: &mut usize, path: &Path) -> EngineResult<usize> {
    usize::try_from(read_u64(payload, cursor, path)?)
        .map_err(|_| corrupt(path, CheckpointCorruptionKind::FieldTooLarge))
}

fn validate_collection_count(
    path: &Path,
    collection_count: usize,
    remaining_payload: usize,
) -> EngineResult<()> {
    if collection_count > remaining_payload / MIN_COLLECTION_PAYLOAD_BYTES {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }
    Ok(())
}

fn validate_record_count(
    path: &Path,
    record_count: usize,
    remaining_payload: usize,
) -> EngineResult<()> {
    if record_count > remaining_payload / MIN_RECORD_PAYLOAD_BYTES {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }
    Ok(())
}

fn read_bytes<'a>(payload: &'a [u8], cursor: &mut usize, path: &Path) -> EngineResult<&'a [u8]> {
    let len = read_u32(payload, cursor, path)? as usize;
    if len > MAX_CHECKPOINT_FIELD_BYTES {
        return Err(corrupt(path, CheckpointCorruptionKind::FieldTooLarge));
    }
    if payload.len().saturating_sub(*cursor) < len {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }
    let out = &payload[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_exact<const N: usize>(
    payload: &[u8],
    cursor: &mut usize,
    path: &Path,
) -> EngineResult<[u8; N]> {
    if payload.len().saturating_sub(*cursor) < N {
        return Err(corrupt(path, CheckpointCorruptionKind::MalformedCheckpoint));
    }
    let out = payload[*cursor..*cursor + N]
        .try_into()
        .expect("fixed field length");
    *cursor += N;
    Ok(out)
}

fn is_raw_checkpoint_file(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some(RAW_CHECKPOINT_EXTENSION)
}

fn temp_path_for(path: &Path) -> PathBuf {
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "raw-checkpoint".into());
    path.with_file_name(format!(
        "{file_name}.{RAW_CHECKPOINT_TMP_EXTENSION}.{pid}.{ts}"
    ))
}

fn sync_parent_dir(path: &Path) -> EngineResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let dir = File::open(parent).map_err(|e| checkpoint_io(parent, "open checkpoint parent", e))?;
    dir.sync_all()
        .map_err(|e| checkpoint_io(parent, "sync checkpoint parent", e))
}

fn corrupt(path: &Path, kind: CheckpointCorruptionKind) -> EngineError {
    EngineError::CheckpointCorruption {
        path: path.display().to_string(),
        kind,
    }
}

fn checkpoint_io(path: &Path, operation: &'static str, error: io::Error) -> EngineError {
    EngineError::CheckpointIo {
        operation,
        path: path.display().to_string(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn checkpoint_with_payload(payload: Vec<u8>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&RAW_CHECKPOINT_MAGIC.to_le_bytes());
        out.extend_from_slice(&RAW_CHECKPOINT_FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        out.extend_from_slice(&payload);
        let crc = crc32c::crc32c(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    fn checkpoint_payload(collection_count: u32, record_count: u64) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&16u64.to_le_bytes());
        payload.extend_from_slice(&16u64.to_le_bytes());
        payload.extend_from_slice(&collection_count.to_le_bytes());
        payload.extend_from_slice(&record_count.to_le_bytes());
        payload
    }

    #[test]
    fn decode_rejects_impossible_collection_count_before_allocating() {
        let bytes = checkpoint_with_payload(checkpoint_payload(1_000_000, 0));
        let err = match decode_checkpoint(Path::new("malformed.mfschkp"), &bytes) {
            Ok(_) => panic!("impossible collection count should be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            corrupt(
                Path::new("malformed.mfschkp"),
                CheckpointCorruptionKind::MalformedCheckpoint,
            )
        );
    }

    #[test]
    fn decode_rejects_impossible_collection_record_count_before_allocating() {
        let mut payload = checkpoint_payload(1, 0);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.push(b'a');
        payload.extend_from_slice(&1_000_000u64.to_le_bytes());

        let bytes = checkpoint_with_payload(payload);
        let err = match decode_checkpoint(Path::new("malformed.mfschkp"), &bytes) {
            Ok(_) => panic!("impossible record count should be rejected"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            corrupt(
                Path::new("malformed.mfschkp"),
                CheckpointCorruptionKind::MalformedCheckpoint,
            )
        );
    }
}
