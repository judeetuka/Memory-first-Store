//! NoSQL engine contract and future engine modules.

pub mod checkpoint;
pub mod config;
pub mod error;
pub mod index;
pub mod query;
pub mod raw;
pub mod reference;
pub mod schema_mode;
pub mod semantics;
pub mod types;
pub mod wal;

pub use checkpoint::{
    RAW_CHECKPOINT_FORMAT_VERSION, RawCheckpointCollectionMetadata, RawCheckpointLoad,
    RawCheckpointMetadata, RawCheckpointSource, RawRecovery, load_latest_raw_checkpoint,
    raw_checkpoint_path, read_raw_checkpoint_metadata, recover_raw_checkpoint_then_wal,
    write_raw_checkpoint, write_raw_checkpoint_to_dir,
};
pub use config::{DEFAULT_MAX_COLLECTIONS, DEFAULT_RAW_INITIAL_CAPACITY, EngineConfig};
pub use error::{CheckpointCorruptionKind, EngineError, EngineResult, WalCorruptionKind};
pub use index::SchemaLookupResult;
pub use raw::NoSqlEngine;
pub use reference::{SchemaForwardReferenceInclude, SchemaReverseReferenceInclude};
pub use schema_mode::{SchemaReadResult, schema_document_raw_key, schema_primary_key_raw_key};
pub use semantics::{
    DurabilityMode, EngineMode, EngineNonGoal, EngineScope, EngineSemantics, IndexConsistency,
    ReadConsistency, RecoveryPrecedence, ReferenceLimit, V1_ENGINE_SEMANTICS, WriteAtomicity,
    WriteConflictPolicy,
};
pub use types::{
    CollectionId, CollectionName, DocumentVersion, FieldUpdate, FieldUpdateOp, FilterClause,
    FilterOp, Lsn, QueryOptions, QueryResult, RawKey, RawValue, ReadOptions, ReadResult,
    SortDirection, WriteAcknowledgement, WriteOptions, WriteResult,
};
pub use wal::{
    RAW_WAL_FORMAT_VERSION, RawWalRecord, RawWalReplayStats, RawWalSegmentReader,
    RawWalSegmentWriter, replay_raw_wal, replay_raw_wal_after,
};
