//! Compatibility facade for `memory-first-store`.

pub use mfs_compat::{object_store, page_store, page_vfs, schema_flush, schema_store};
pub use mfs_core::{
    CPU_FALLBACK_PATH, CpuDispatchPath, CpuFeatures, DENSE_VALUE_MAX, DenseU64Lane,
    FastBuildHasher, FastHasher, FlushBackend, FlushRecord, MemoryFirstStore, Operation,
    StoreConfig, StoreStats, auto_thread_count, avx2_supported, avx512_supported,
    bounded_reclaim, concurrent_map, cpu_relax, durability, inline_map, lockfree, prefetch_read,
    prefetch_write, s3fifo, sse42_supported, writeback,
};
#[cfg(feature = "experimental")]
pub use mfs_core::{atomic_writeback, slot_writeback};
pub use mfs_store::{store, schema, schema_value, value};
pub use mfs_neural::{
    bucketed_index, dense_kv, dense_writeback_map, inline_handle_index,
    queued_dense_writeback,
};
#[cfg(feature = "experimental")]
pub use mfs_neural::dense_writeback;

#[cfg(feature = "ahash")]
pub use mfs_core::AHashState;

pub use mfs_compat::object_store::{MfsObjectStore, ObjectStoreError};
pub use mfs_compat::page_store::{
    FileId, InMemoryPageStore, LockMode, MfsPageStore, PageStoreError, PageStoreResult,
};
pub use mfs_compat::page_vfs::{MfsPageVfs, PageVfsFile, PageVfsResult};
pub use mfs_compat::schema_flush::{
    SchemaFlushBackend, SchemaFlushError as SqlSchemaFlushError, SchemaFlushRecord, SqlColumn,
    SqlValue, create_index_sql, create_table_sql, delete_sql, delete_values, ensure_schema_sql,
    quote_ident, sql_columns, upsert_sql, upsert_values,
};
pub use mfs_compat::schema_store::{
    SchemaFlushError, SchemaForwardInclude, SchemaIndexKey, SchemaKey, SchemaReverseInclude,
    SchemaStore, SchemaStoreError, extract_primary_key,
};

pub use mfs_store::{
    CheckpointCorruptionKind, CollectionId, CollectionName, DEFAULT_MAX_COLLECTIONS,
    DEFAULT_RAW_INITIAL_CAPACITY, DocumentVersion, DurabilityMode, IndexConsistency, Lsn,
    MAX_BLOB_BYTES, MAX_COLLECTION_ITEMS, MAX_ENCODED_VALUE_BYTES, MAX_SCHEMA_BLOB_BYTES,
    MAX_SCHEMA_COLLECTION_ITEMS, MAX_SCHEMA_VALUE_BYTES, MAX_SCHEMA_VALUE_DEPTH, MfsStore,
    MfsStoreConfig, MfsValue, MfsValueCodec, RAW_CHECKPOINT_FORMAT_VERSION, RAW_WAL_FORMAT_VERSION,
    RawCheckpointCollectionMetadata, RawCheckpointLoad, RawCheckpointMetadata, RawCheckpointSource,
    RawKey, RawRecovery, RawValue, RawWalRecord, RawWalReplayStats, RawWalSegmentReader,
    RawWalSegmentWriter, ReadConsistency, ReadOptions, ReadResult, RecoveryPrecedence, Reference,
    ReferenceLimit, Schema, SchemaError, SchemaField, SchemaFieldType,
    SchemaForwardReferenceInclude, SchemaLookupResult, SchemaReadResult,
    SchemaReverseReferenceInclude, SchemaValue, SchemaValueCodec, SchemaValueError,
    SchemaValueKind, SchemaValueTag, SortedSetEntry, StoreError, StoreMode, StoreNonGoal,
    StoreResult, StoreScope, StoreSemantics, StreamEntry, StreamId, V1_STORE_SEMANTICS,
    ValueTag, WalCorruptionKind, WriteAcknowledgement, WriteAtomicity, WriteConflictPolicy,
    WriteOptions, WriteResult, decode_schema_value, decode_value, encode_schema_value,
    encode_value, load_latest_raw_checkpoint, raw_checkpoint_path, read_raw_checkpoint_metadata,
    recover_raw_checkpoint_then_wal, replay_raw_wal, replay_raw_wal_after, schema_document_raw_key,
    schema_primary_key_raw_key, validate_codec_safe, validate_document, write_raw_checkpoint,
    write_raw_checkpoint_to_dir,
};

pub use mfs_neural::bucketed_index::{BucketedIndex, BucketedInsertOutcome};
pub use mfs_neural::dense_kv::DenseKvMap;
#[cfg(feature = "experimental")]
pub use mfs_neural::dense_writeback::{
    DenseAutoFlusher, DenseWriteBehindStats as DenseU64WriteBehindStats, DenseWriteBehindU64,
};
pub use mfs_neural::dense_writeback_map::{
    DenseMapAutoFlusher, DenseWriteBehindMap, DenseWriteBehindStats as DenseMapWriteBehindStats,
};
pub use mfs_neural::inline_handle_index::InlineHandleIndex;
pub use mfs_neural::queued_dense_writeback::{
    QueuedDenseWriteBehindMap, QueuedWriteError, WriteTicket,
};

#[cfg(test)]
#[test]
fn facade_imports_compile() {
    use crate::dense_kv::DenseKvMap;
    use crate::durability::{U64Codec, WalConfig};
    use crate::store::{MfsStore, MfsStoreConfig, RawKey, RawValue};
    use crate::object_store::MfsObjectStore;
    use crate::page_store::{InMemoryPageStore, MfsPageStore};
    use crate::page_vfs::MfsPageVfs;
    use crate::schema::{Schema, SchemaField, SchemaFieldType};
    use crate::schema_flush::quote_ident;
    use crate::schema_store::SchemaStore;
    use crate::schema_value::SchemaValue;
    use crate::value::{MfsValue, decode_value, encode_value};
    use crate::writeback::{WriteBehindCache, WriteBehindConfig};
    use crate::{
        DENSE_VALUE_MAX, DenseU64Lane, MemoryFirstStore, ReadOptions, StoreConfig, WriteOptions,
    };

    let store = MemoryFirstStore::<u64, u64>::with_config(StoreConfig {
        shards: 2,
        initial_capacity_per_shard: 4,
    });
    assert_eq!(store.put(7, 11), 1);
    assert_eq!(store.read_with(&7, |value| *value), Some(11));

    let writeback = WriteBehindCache::<u64, u64>::with_config(WriteBehindConfig {
        dirty_shards: 1,
        initial_capacity: 8,
        dirty_queue_capacity: 8,
    });
    assert_eq!(writeback.put(8, 13), 1);
    assert_eq!(writeback.get(&8).as_deref(), Some(&13));

    let wal_config = WalConfig::default();
    let _codec = U64Codec;
    assert_eq!(wal_config.sync_threshold_records, 256);

    let lane = DenseU64Lane::with_len(2);
    lane.store(1, DENSE_VALUE_MAX.min(42));
    assert_eq!(lane.load(1), 42);

    let dense = DenseKvMap::<u64, u64>::with_capacity(8);
    dense.put(3, 5).expect("put through neural facade module");
    assert_eq!(dense.get(&3), Some(5));

    let engine = MfsStore::open_memory(MfsStoreConfig {
        raw_initial_capacity: 8,
        ..MfsStoreConfig::default()
    })
    .expect("open memory engine through facade");
    engine
        .create_raw_collection("raw")
        .expect("create raw collection through facade");
    let key = RawKey::from(&b"facade:1"[..]);
    engine
        .put_raw(
            "raw",
            key.clone(),
            RawValue::from(&b"ok"[..]),
            WriteOptions::default(),
        )
        .expect("write through facade DB import");
    let read = engine
        .get_raw("raw", &key, ReadOptions::default())
        .expect("read through facade DB import")
        .expect("facade value exists");
    assert_eq!(read.value.as_bytes(), b"ok");

    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let schema = Schema::new("facade_users", vec![id]);
    let document = SchemaValue::object([("id".to_string(), SchemaValue::String("u1".into()))]);
    assert_eq!(document.validate_against(&schema), Ok(()));

    let mut encoded = Vec::new();
    encode_value(&MfsValue::Integer(7), &mut encoded);
    assert_eq!(decode_value(&encoded).unwrap(), MfsValue::Integer(7));

    let object_store = MfsObjectStore::with_capacity(8);
    object_store.set_string(b"facade".to_vec(), "compat");
    assert_eq!(
        object_store.get_string(b"facade"),
        Ok(Some("compat".to_string()))
    );

    let schema_store = SchemaStore::new();
    assert!(schema_store.collection_names().is_empty());

    let page_store = InMemoryPageStore::new();
    let page_vfs = MfsPageVfs::new(page_store.clone());
    let file = page_vfs.x_open("facade.db").unwrap();
    assert_eq!(page_store.file_size(file.file_id()), Ok(0));

    assert_eq!(quote_ident("facade"), "\"facade\"");
}
