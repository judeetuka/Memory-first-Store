use crate::engine::index::{
    SchemaCollectionIndexes, SchemaIndexWritePlan, decode_schema_raw_value,
};
use crate::engine::{
    CollectionId, CollectionName, DocumentVersion, DurabilityMode, EngineConfig, EngineError,
    EngineResult, RawKey, RawValue, RawWalSegmentWriter, ReadOptions, ReadResult,
    WriteAcknowledgement, WriteOptions, WriteResult,
};
use crossbeam_utils::CachePadded;
use mfs_core::FastBuildHasher;
use mfs_core::concurrent_map::{ConcurrentMap, InsertOutcome};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::collections::HashMap;
use std::fmt;
use std::hash::BuildHasher;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DEFAULT_RAW_MUTATION_LOCKS: usize = 1024;

#[derive(Clone)]
pub struct NoSqlEngine {
    pub(crate) inner: Arc<EngineInner>,
}

pub(crate) struct EngineInner {
    pub(crate) config: EngineConfig,
    durability: EngineDurability,
    collections: RwLock<HashMap<String, Arc<RawCollection>>>,
    pub(crate) schema_indexes: RwLock<HashMap<String, Arc<SchemaCollectionIndexes>>>,
    next_collection_id: AtomicU64,
}

struct EngineDurability {
    wal_path: Option<PathBuf>,
    wal: Mutex<Option<RawWalSegmentWriter>>,
}

struct RawCollection {
    records: ConcurrentMap<RawKey, RawRecord>,
    write_lock: CachePadded<Mutex<()>>,
    mutation_locks: Box<[CachePadded<Mutex<()>>]>,
    mutation_lock_mask: usize,
    hash_builder: FastBuildHasher,
    record_count: AtomicU64,
}

struct RawWriteContext<'a> {
    collection: &'a str,
    options: WriteOptions,
    default_durability: DurabilityMode,
    durability: &'a EngineDurability,
}

struct RawWriteHooks<P, A> {
    preflight: P,
    after_success: A,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawRecord {
    value: Option<RawValue>,
    version: DocumentVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawEngineSnapshot {
    pub collections: Vec<RawCollectionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawCollectionSnapshot {
    pub name: String,
    pub records: Vec<RawSnapshotRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawSnapshotRecord {
    pub key: RawKey,
    pub value: Option<RawValue>,
    pub version: DocumentVersion,
}

impl fmt::Debug for NoSqlEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoSqlEngine")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

impl NoSqlEngine {
    pub fn open_memory(config: EngineConfig) -> EngineResult<Self> {
        validate_engine_config(&config)?;

        Ok(Self {
            inner: Arc::new(EngineInner {
                durability: EngineDurability::new(&config),
                config,
                collections: RwLock::new(HashMap::new()),
                schema_indexes: RwLock::new(HashMap::new()),
                next_collection_id: AtomicU64::new(1),
            }),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.inner.config
    }

    pub fn create_raw_collection(
        &self,
        name: impl Into<CollectionName>,
    ) -> EngineResult<CollectionId> {
        let name = name.into().into_string();
        let mut collections = self.inner.collections.write();
        if collections.contains_key(&name) {
            return Err(EngineError::CollectionAlreadyExists { collection: name });
        }
        if collections.len() >= self.inner.config.max_collections {
            return Err(EngineError::CollectionLimitExceeded {
                max_collections: self.inner.config.max_collections,
            });
        }

        let id = CollectionId::new(
            self.inner
                .next_collection_id
                .fetch_add(1, Ordering::Relaxed),
        );
        collections.insert(
            name,
            Arc::new(RawCollection::with_capacity(
                self.inner.config.raw_initial_capacity,
            )),
        );
        Ok(id)
    }

    pub fn put_raw(
        &self,
        collection: &str,
        key: RawKey,
        value: RawValue,
        options: WriteOptions,
    ) -> EngineResult<WriteResult> {
        self.put_raw_with_hooks(collection, key, value, options, |_, _| Ok(()), |_, _| {})
    }

    pub(crate) fn put_raw_with_hooks<P, A>(
        &self,
        collection: &str,
        key: RawKey,
        value: RawValue,
        options: WriteOptions,
        preflight: P,
        after_success: A,
    ) -> EngineResult<WriteResult>
    where
        P: FnOnce(Option<&RawValue>, DocumentVersion) -> EngineResult<()>,
        A: FnOnce(Option<&RawValue>, DocumentVersion),
    {
        self.raw_collection(collection)?.put_with_hooks(
            key,
            value,
            RawWriteContext {
                collection,
                options,
                default_durability: self.inner.config.durability,
                durability: &self.inner.durability,
            },
            RawWriteHooks {
                preflight,
                after_success,
            },
        )
    }

    pub fn compare_put_raw(
        &self,
        collection: &str,
        key: RawKey,
        value: RawValue,
        expected_version: DocumentVersion,
    ) -> EngineResult<WriteResult> {
        self.put_raw(
            collection,
            key,
            value,
            WriteOptions {
                expected_version: Some(expected_version),
                ..WriteOptions::default()
            },
        )
    }

    pub fn get_raw(
        &self,
        collection: &str,
        key: &RawKey,
        options: ReadOptions,
    ) -> EngineResult<Option<ReadResult>> {
        let _ = options;
        Ok(self.raw_collection(collection)?.get(key))
    }

    pub fn delete_raw(
        &self,
        collection: &str,
        key: RawKey,
        options: WriteOptions,
    ) -> EngineResult<WriteResult> {
        self.delete_raw_with_hooks(collection, key, options, |_, _| Ok(()), |_, _| {})
    }

    pub(crate) fn delete_raw_with_hooks<P, A>(
        &self,
        collection: &str,
        key: RawKey,
        options: WriteOptions,
        preflight: P,
        after_success: A,
    ) -> EngineResult<WriteResult>
    where
        P: FnOnce(Option<&RawValue>, DocumentVersion) -> EngineResult<()>,
        A: FnOnce(Option<&RawValue>, DocumentVersion),
    {
        self.raw_collection(collection)?.delete_with_hooks(
            key,
            RawWriteContext {
                collection,
                options,
                default_durability: self.inner.config.durability,
                durability: &self.inner.durability,
            },
            RawWriteHooks {
                preflight,
                after_success,
            },
        )
    }

    pub(crate) fn apply_raw_replay_record(
        &self,
        collection: &str,
        key: RawKey,
        value: Option<RawValue>,
        version: DocumentVersion,
    ) -> EngineResult<()> {
        let raw_collection = self.raw_collection(collection)?;
        let schema_state = self.schema_indexes_for_collection(collection);

        match schema_state {
            Some(state) => {
                let _write_unit = state.lock_write_unit();
                let _guard = raw_collection.lock_key(&key);
                let pinned = raw_collection.records.pin();
                let old_record = pinned.get(&key).cloned();
                let old_value = old_record.as_ref().and_then(|record| record.value.as_ref());
                let plan =
                    prepare_schema_replay_plan(self, &state, &key, old_value, value.as_ref())?;

                raw_collection.insert_replay_record_locked(
                    collection,
                    &pinned,
                    key.clone(),
                    value,
                    version,
                )?;
                state.apply_write(&key, plan);
                Ok(())
            }
            None => {
                let _guard = raw_collection.lock_key(&key);
                let pinned = raw_collection.records.pin();
                raw_collection.insert_replay_record_locked(collection, &pinned, key, value, version)
            }
        }
    }

    pub(crate) fn raw_snapshot(&self) -> RawEngineSnapshot {
        let collections = self.inner.collections.read();
        let mut snapshot = Vec::with_capacity(collections.len());
        for (name, collection) in collections.iter() {
            snapshot.push(RawCollectionSnapshot {
                name: name.clone(),
                records: collection.snapshot_records(),
            });
        }
        snapshot.sort_by(|left, right| left.name.cmp(&right.name));
        RawEngineSnapshot {
            collections: snapshot,
        }
    }

    pub(crate) fn from_raw_snapshot(
        config: EngineConfig,
        snapshot: RawEngineSnapshot,
    ) -> EngineResult<Self> {
        validate_engine_config(&config)?;
        if snapshot.collections.len() > config.max_collections {
            return Err(EngineError::CollectionLimitExceeded {
                max_collections: config.max_collections,
            });
        }

        let mut collections = HashMap::with_capacity(snapshot.collections.len());
        for collection in snapshot.collections {
            if collections.contains_key(&collection.name) {
                return Err(EngineError::CollectionAlreadyExists {
                    collection: collection.name,
                });
            }

            let capacity = config
                .raw_initial_capacity
                .max(collection.records.len().saturating_mul(2))
                .max(1);
            let raw_collection = RawCollection::with_capacity(capacity);
            let mut live_count = 0u64;
            for record in collection.records {
                if record.value.is_some() {
                    live_count += 1;
                }
                match raw_collection.records.insert(
                    record.key,
                    RawRecord {
                        value: record.value,
                        version: record.version,
                    },
                ) {
                    InsertOutcome::Inserted | InsertOutcome::Replaced => {}
                    InsertOutcome::Full => {
                        return Err(EngineError::CollectionCapacityFull {
                            collection: collection.name,
                        });
                    }
                }
            }
            raw_collection
                .record_count
                .store(live_count, Ordering::Relaxed);
            collections.insert(collection.name, Arc::new(raw_collection));
        }

        let next_collection_id = collections.len() as u64 + 1;
        Ok(Self {
            inner: Arc::new(EngineInner {
                durability: EngineDurability::new(&config),
                config,
                collections: RwLock::new(collections),
                schema_indexes: RwLock::new(HashMap::new()),
                next_collection_id: AtomicU64::new(next_collection_id),
            }),
        })
    }

    pub fn collection_count(&self, collection: &str) -> EngineResult<u64> {
        Ok(self
            .raw_collection(collection)?
            .record_count
            .load(Ordering::Relaxed))
    }

    fn raw_collection(&self, collection: &str) -> EngineResult<Arc<RawCollection>> {
        self.inner
            .collections
            .read()
            .get(collection)
            .cloned()
            .ok_or_else(|| EngineError::CollectionNotFound {
                collection: collection.to_string(),
            })
    }
}

impl EngineDurability {
    fn new(config: &EngineConfig) -> Self {
        Self {
            wal_path: config.wal_path.clone(),
            wal: Mutex::new(None),
        }
    }

    fn acknowledge_write(
        &self,
        mode: DurabilityMode,
        collection: &str,
        key: &RawKey,
        value: Option<&RawValue>,
        version: DocumentVersion,
    ) -> EngineResult<WriteAcknowledgement> {
        match mode {
            DurabilityMode::MemoryOnly => Ok(WriteAcknowledgement::MemoryOnly),
            DurabilityMode::SnapshotOnly => Ok(WriteAcknowledgement::SnapshotOnly),
            DurabilityMode::WalAsync => {
                let lsn = self.append_raw_wal(collection, key, value, version, false)?;
                Ok(WriteAcknowledgement::WalBuffered { lsn })
            }
            DurabilityMode::WalGroupCommit => {
                let lsn = self.append_raw_wal(collection, key, value, version, true)?;
                Ok(WriteAcknowledgement::WalGroupCommitted { lsn })
            }
            DurabilityMode::WalSync => {
                let lsn = self.append_raw_wal(collection, key, value, version, true)?;
                Ok(WriteAcknowledgement::WalSynced { lsn })
            }
        }
    }

    fn append_raw_wal(
        &self,
        collection: &str,
        key: &RawKey,
        value: Option<&RawValue>,
        version: DocumentVersion,
        sync: bool,
    ) -> EngineResult<crate::engine::Lsn> {
        let mut wal = self.wal.lock();
        if wal.is_none() {
            let path = self.wal_path.as_ref().ok_or(EngineError::InvalidConfig {
                field: "wal_path",
                reason: "required for WAL durability modes",
            })?;
            *wal = Some(RawWalSegmentWriter::open(path)?);
        }

        let writer = wal.as_mut().expect("WAL writer initialized above");
        let lsn = match value {
            Some(value) => writer.append_put_versioned(collection, key, value, version),
            None => writer.append_delete_versioned(collection, key, version),
        }?;
        if sync {
            writer.sync_now()?;
        }
        Ok(lsn)
    }
}

impl RawCollection {
    fn with_capacity(capacity: usize) -> Self {
        let lock_count = DEFAULT_RAW_MUTATION_LOCKS.next_power_of_two();
        let mutation_locks = (0..lock_count)
            .map(|_| CachePadded::new(Mutex::new(())))
            .collect::<Vec<_>>();

        Self {
            records: ConcurrentMap::with_capacity(capacity),
            write_lock: CachePadded::new(Mutex::new(())),
            mutation_locks: mutation_locks.into_boxed_slice(),
            mutation_lock_mask: lock_count - 1,
            hash_builder: FastBuildHasher::default(),
            record_count: AtomicU64::new(0),
        }
    }

    fn put_with_hooks<P, A>(
        &self,
        key: RawKey,
        value: RawValue,
        context: RawWriteContext<'_>,
        hooks: RawWriteHooks<P, A>,
    ) -> EngineResult<WriteResult>
    where
        P: FnOnce(Option<&RawValue>, DocumentVersion) -> EngineResult<()>,
        A: FnOnce(Option<&RawValue>, DocumentVersion),
    {
        self.write_with_hooks(key, Some(value), context, hooks)
    }

    fn delete_with_hooks<P, A>(
        &self,
        key: RawKey,
        context: RawWriteContext<'_>,
        hooks: RawWriteHooks<P, A>,
    ) -> EngineResult<WriteResult>
    where
        P: FnOnce(Option<&RawValue>, DocumentVersion) -> EngineResult<()>,
        A: FnOnce(Option<&RawValue>, DocumentVersion),
    {
        self.write_with_hooks(key, None, context, hooks)
    }

    fn get(&self, key: &RawKey) -> Option<ReadResult> {
        let pinned = self.records.pin();
        let record = pinned.get(key)?;
        record.value.clone().map(|value| ReadResult {
            value,
            version: record.version,
        })
    }

    fn snapshot_records(&self) -> Vec<RawSnapshotRecord> {
        let _guards = self.lock_all_mutation_locks();
        let mut records = Vec::new();
        self.records.for_each(|key, record| {
            records.push(RawSnapshotRecord {
                key: key.clone(),
                value: record.value.clone(),
                version: record.version,
            });
        });
        records.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
        records
    }

    fn write_with_hooks<P, A>(
        &self,
        key: RawKey,
        value: Option<RawValue>,
        context: RawWriteContext<'_>,
        hooks: RawWriteHooks<P, A>,
    ) -> EngineResult<WriteResult>
    where
        P: FnOnce(Option<&RawValue>, DocumentVersion) -> EngineResult<()>,
        A: FnOnce(Option<&RawValue>, DocumentVersion),
    {
        let _write_guard = self.write_lock.lock();
        let _guard = self.lock_key(&key);
        let pinned = self.records.pin();
        let old_record = pinned.get(&key);
        let actual = old_record
            .map(|record| record.version)
            .unwrap_or(DocumentVersion::ZERO);

        if let Some(expected) = context.options.expected_version
            && expected != actual
        {
            return Err(EngineError::Conflict {
                collection: context.collection.to_string(),
                key,
                expected,
                actual,
            });
        }

        let old_value = old_record.and_then(|record| record.value.as_ref());
        let was_live = old_value.is_some();
        (hooks.preflight)(old_value, actual)?;

        let version = next_version(actual);
        if !pinned.can_insert_or_replace(&key) {
            return Err(EngineError::CollectionCapacityFull {
                collection: context.collection.to_string(),
            });
        }
        let durability = context
            .options
            .effective_durability(context.default_durability);
        let acknowledgement = context.durability.acknowledge_write(
            durability,
            context.collection,
            &key,
            value.as_ref(),
            version,
        )?;
        let is_put = value.is_some();
        let outcome = pinned.insert(key.clone(), RawRecord { value, version });
        match outcome {
            InsertOutcome::Inserted | InsertOutcome::Replaced => {
                match is_put {
                    true if !was_live => {
                        self.record_count.fetch_add(1, Ordering::Relaxed);
                    }
                    false if was_live => {
                        self.record_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
                Ok(WriteResult {
                    version,
                    lsn: acknowledgement.lsn(),
                    acknowledgement,
                })
                .inspect(|_| {
                    (hooks.after_success)(old_value, version);
                })
            }
            InsertOutcome::Full => Err(EngineError::CollectionCapacityFull {
                collection: context.collection.to_string(),
            }),
        }
    }

    fn lock_key<'a>(&'a self, key: &RawKey) -> MutexGuard<'a, ()> {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mutation_lock_mask;
        self.mutation_locks[idx].lock()
    }

    fn lock_all_mutation_locks(&self) -> Vec<MutexGuard<'_, ()>> {
        self.mutation_locks
            .iter()
            .map(|mutation_lock| mutation_lock.lock())
            .collect()
    }

    fn insert_replay_record_locked(
        &self,
        collection: &str,
        pinned: &mfs_core::concurrent_map::Pinned<'_, RawKey, RawRecord, FastBuildHasher>,
        key: RawKey,
        value: Option<RawValue>,
        version: DocumentVersion,
    ) -> EngineResult<()> {
        let is_put = value.is_some();
        let (outcome, old_record) =
            pinned.insert_returning_old(key, RawRecord { value, version });
        match outcome {
            InsertOutcome::Inserted | InsertOutcome::Replaced => {
                let was_live = old_record
                    .as_ref()
                    .and_then(|r| r.value.as_ref())
                    .is_some();
                match is_put {
                    true if !was_live => {
                        self.record_count.fetch_add(1, Ordering::Relaxed);
                    }
                    false if was_live => {
                        self.record_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
                Ok(())
            }
            InsertOutcome::Full => Err(EngineError::CollectionCapacityFull {
                collection: collection.to_string(),
            }),
        }
    }
}

fn prepare_schema_replay_plan(
    engine: &NoSqlEngine,
    state: &SchemaCollectionIndexes,
    key: &RawKey,
    old_raw: Option<&RawValue>,
    new_raw: Option<&RawValue>,
) -> EngineResult<SchemaIndexWritePlan> {
    let old_document = old_raw
        .map(|raw| decode_schema_raw_value(state.schema(), raw))
        .transpose()?;

    match new_raw {
        Some(raw) => {
            let new_document = decode_schema_raw_value(state.schema(), raw)?;
            state.prepare_put(engine, key, old_document.as_ref(), &new_document)
        }
        None => state.prepare_delete(engine, old_document.as_ref()),
    }
}

fn validate_engine_config(config: &EngineConfig) -> EngineResult<()> {
    if config.max_collections == 0 {
        return Err(EngineError::InvalidConfig {
            field: "max_collections",
            reason: "must be greater than zero",
        });
    }
    if config.raw_initial_capacity == 0 {
        return Err(EngineError::InvalidConfig {
            field: "raw_initial_capacity",
            reason: "must be greater than zero",
        });
    }
    if matches!(
        config.durability,
        DurabilityMode::WalAsync | DurabilityMode::WalGroupCommit | DurabilityMode::WalSync
    ) && config.wal_path.is_none()
    {
        return Err(EngineError::InvalidConfig {
            field: "wal_path",
            reason: "required for WAL durability modes",
        });
    }
    Ok(())
}

fn next_version(version: DocumentVersion) -> DocumentVersion {
    DocumentVersion::new(version.get() + 1)
}
