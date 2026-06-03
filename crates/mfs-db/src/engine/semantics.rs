//! Code-facing v1 storage semantics for the NoSQL engine.
//!
//! This module freezes the contract that later engine modules implement. It is
//! API, not a standalone design document.
//!
//! v1 is an embedded, single-process NoSQL engine. It has two front doors over
//! one storage kernel:
//!
//! - raw key-value mode stores opaque byte keys and values;
//! - schema mode validates [`crate::schema::Schema`] metadata and
//!   [`crate::schema_value::SchemaValue`] documents before writing them.
//!
//! Both modes use the same primary-record storage, per-key version clock,
//! durability path, checkpoint path, and recovery path. Schema mode adds
//! validation plus declared secondary indexes and declared references on top of
//! that shared kernel.
//!
//! A write is one engine write unit. The primary record, declared secondary
//! index entries, and bounded reference entries are applied atomically inside
//! that unit. A read sees either the state before the unit or the state after
//! it, never a half-updated index or reference edge.
//!
//! Conflict handling is per key. v1 uses expected-version checks against the
//! current per-key version; it is not full MVCC and does not expose snapshot
//! isolation across a series of reads.
//!
//! Recovery loads the latest valid checkpoint first, then replays WAL records
//! whose LSN is greater than the checkpoint LSN. If no valid checkpoint exists,
//! recovery starts from an empty in-memory state and replays valid WAL records
//! from the beginning.

/// Complete v1 semantics contract for the NoSQL engine surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineSemantics {
    pub scope: EngineScope,
    pub modes: &'static [EngineMode],
    pub write_atomicity: WriteAtomicity,
    pub write_conflict_policies: &'static [WriteConflictPolicy],
    pub read_consistency: &'static [ReadConsistency],
    pub durability_modes: &'static [DurabilityMode],
    pub recovery_precedence: RecoveryPrecedence,
    pub index_consistency: IndexConsistency,
    pub reference_limit: ReferenceLimit,
    pub non_goals: &'static [EngineNonGoal],
}

/// The v1 contract later engine code must preserve.
pub const V1_ENGINE_SEMANTICS: EngineSemantics = EngineSemantics {
    scope: EngineScope::EmbeddedSingleProcess,
    modes: &EngineMode::VARIANTS,
    write_atomicity: WriteAtomicity::PrimaryIndexesAndReferences,
    write_conflict_policies: &WriteConflictPolicy::VARIANTS,
    read_consistency: &ReadConsistency::VARIANTS,
    durability_modes: &DurabilityMode::VARIANTS,
    recovery_precedence: RecoveryPrecedence::LatestCheckpointThenWal,
    index_consistency: IndexConsistency::DeclaredIndexesInWriteUnit,
    reference_limit: ReferenceLimit::DeclaredReferencesWithWriteBound,
    non_goals: &EngineNonGoal::VARIANTS,
};

/// Engine deployment scope for v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineScope {
    /// The engine runs in the caller's process and owns no remote protocol.
    EmbeddedSingleProcess,
}

impl EngineScope {
    pub const VARIANTS: [Self; 1] = [Self::EmbeddedSingleProcess];

    pub const fn name(self) -> &'static str {
        match self {
            Self::EmbeddedSingleProcess => "EmbeddedSingleProcess",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::EmbeddedSingleProcess => {
                "engine calls happen in-process; v1 does not define a network server or distributed behavior"
            }
        }
    }
}

/// Public storage modes backed by the same kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineMode {
    /// Opaque byte keys and values, with no schema validation.
    RawKv,
    /// Schema-checked documents using the existing schema and schema-value types.
    Schema,
}

impl EngineMode {
    pub const VARIANTS: [Self; 2] = [Self::RawKv, Self::Schema];

    pub const fn name(self) -> &'static str {
        match self {
            Self::RawKv => "RawKv",
            Self::Schema => "Schema",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::RawKv => {
                "raw mode stores opaque byte keys and values in the shared storage kernel"
            }
            Self::Schema => {
                "schema mode validates schemas and documents before using the shared storage kernel"
            }
        }
    }
}

/// Atomic scope of one committed engine write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteAtomicity {
    /// Primary data, declared secondary indexes, and bounded references commit as one unit.
    PrimaryIndexesAndReferences,
}

impl WriteAtomicity {
    pub const VARIANTS: [Self; 1] = [Self::PrimaryIndexesAndReferences];

    pub const fn name(self) -> &'static str {
        match self {
            Self::PrimaryIndexesAndReferences => "PrimaryIndexesAndReferences",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::PrimaryIndexesAndReferences => {
                "one write unit includes the primary record, declared secondary indexes, and bounded reference entries"
            }
        }
    }
}

/// Write conflict behavior exposed by v1 mutation APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WriteConflictPolicy {
    /// Apply the mutation to the latest per-key version and advance that version.
    Unconditional,
    /// Apply only if the stored per-key version equals the caller's expected version.
    ExpectedVersion,
    /// Create only when the key has no current live record.
    CreateIfAbsent,
}

impl WriteConflictPolicy {
    pub const VARIANTS: [Self; 3] = [
        Self::Unconditional,
        Self::ExpectedVersion,
        Self::CreateIfAbsent,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Unconditional => "Unconditional",
            Self::ExpectedVersion => "ExpectedVersion",
            Self::CreateIfAbsent => "CreateIfAbsent",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::Unconditional => {
                "the write uses the latest per-key version and advances it after a successful commit"
            }
            Self::ExpectedVersion => {
                "the write succeeds only when the current per-key version equals the expected version"
            }
            Self::CreateIfAbsent => {
                "the write succeeds only when the key has no current live record"
            }
        }
    }
}

/// Read guarantees exposed by v1 operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadConsistency {
    /// A single read returns the latest committed value visible at its operation boundary.
    LatestCommitted,
    /// After a successful write returns, later reads on the same engine handle can observe it
    /// unless another committed write has replaced it.
    ReadOwnWrites,
}

impl ReadConsistency {
    pub const VARIANTS: [Self; 2] = [Self::LatestCommitted, Self::ReadOwnWrites];

    pub const fn name(self) -> &'static str {
        match self {
            Self::LatestCommitted => "LatestCommitted",
            Self::ReadOwnWrites => "ReadOwnWrites",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::LatestCommitted => {
                "a read returns a fully committed state, not a partially applied write unit"
            }
            Self::ReadOwnWrites => {
                "a later read on the same engine handle can observe a completed write unless another write supersedes it"
            }
        }
    }
}

/// Durability acknowledgement modes for committed writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurabilityMode {
    /// Acknowledge after the in-memory write unit commits. No WAL or checkpoint
    /// bytes are promised durable for that write.
    MemoryOnly,
    /// Acknowledge after the in-memory write unit commits and the WAL record is
    /// accepted by the in-process WAL queue. The record is not replayable until
    /// the WAL writer syncs it or a durability barrier completes.
    WalAsync,
    /// Acknowledge after the in-memory write unit commits and the WAL group
    /// containing the record has completed `sync_data`.
    WalGroupCommit,
    /// Acknowledge after the in-memory write unit commits, the WAL record is
    /// appended, and `sync_data` has completed for that record or batch.
    WalSync,
    /// Acknowledge after the in-memory write unit commits. Recovery includes the
    /// write only after a later valid checkpoint contains it.
    SnapshotOnly,
}

impl DurabilityMode {
    pub const VARIANTS: [Self; 5] = [
        Self::MemoryOnly,
        Self::WalAsync,
        Self::WalGroupCommit,
        Self::WalSync,
        Self::SnapshotOnly,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::MemoryOnly => "MemoryOnly",
            Self::WalAsync => "WalAsync",
            Self::WalGroupCommit => "WalGroupCommit",
            Self::WalSync => "WalSync",
            Self::SnapshotOnly => "SnapshotOnly",
        }
    }

    pub const fn acknowledgement(self) -> &'static str {
        match self {
            Self::MemoryOnly => {
                "acknowledge after the in-memory write unit commits; no WAL or checkpoint durability is promised"
            }
            Self::WalAsync => {
                "acknowledge after the WAL record is accepted by the in-process queue; replayability waits for WAL sync or a barrier"
            }
            Self::WalGroupCommit => {
                "acknowledge after the WAL group containing the record has completed sync_data"
            }
            Self::WalSync => {
                "acknowledge after the WAL record is appended and sync_data has completed for the record or batch"
            }
            Self::SnapshotOnly => {
                "acknowledge after the in-memory write unit commits; recovery includes it only after a valid checkpoint contains it"
            }
        }
    }
}

/// Recovery order for durable engine state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryPrecedence {
    /// Load the latest valid checkpoint, then replay WAL records after its LSN.
    LatestCheckpointThenWal,
}

impl RecoveryPrecedence {
    pub const VARIANTS: [Self; 1] = [Self::LatestCheckpointThenWal];

    pub const fn name(self) -> &'static str {
        match self {
            Self::LatestCheckpointThenWal => "LatestCheckpointThenWal",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::LatestCheckpointThenWal => {
                "recover the latest valid checkpoint first, then replay WAL records whose LSN is greater than the checkpoint LSN"
            }
        }
    }
}

/// Consistency for secondary indexes declared by schema mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexConsistency {
    /// Declared secondary index entries are internal participants in the same write unit.
    DeclaredIndexesInWriteUnit,
}

impl IndexConsistency {
    pub const VARIANTS: [Self; 1] = [Self::DeclaredIndexesInWriteUnit];

    pub const fn name(self) -> &'static str {
        match self {
            Self::DeclaredIndexesInWriteUnit => "DeclaredIndexesInWriteUnit",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::DeclaredIndexesInWriteUnit => {
                "declared secondary index entries commit or roll back with the primary record"
            }
        }
    }
}

/// Limits for references maintained by schema mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceLimit {
    /// References are declared in schemas and each write has a bounded reference update set.
    DeclaredReferencesWithWriteBound,
}

impl ReferenceLimit {
    pub const VARIANTS: [Self; 1] = [Self::DeclaredReferencesWithWriteBound];

    pub const fn name(self) -> &'static str {
        match self {
            Self::DeclaredReferencesWithWriteBound => "DeclaredReferencesWithWriteBound",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::DeclaredReferencesWithWriteBound => {
                "reference edges are declared by schema metadata and bounded per write before commit"
            }
        }
    }
}

/// Features intentionally outside the v1 engine contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineNonGoal {
    /// Non-goal: v1 does not provide SQL compatibility.
    SqlCompatibility,
    /// Non-goal: v1 does not provide a network server.
    NetworkServer,
    /// Non-goal: v1 does not provide distributed behavior.
    DistributedBehavior,
    /// Non-goal: v1 does not provide cross-document transactions.
    CrossDocumentTransactions,
    /// Non-goal: v1 does not provide arbitrary joins.
    ArbitraryJoins,
    /// Non-goal: v1 does not provide a query planner.
    QueryPlanner,
    /// Non-goal: v1 does not provide vector or text search.
    VectorOrTextSearch,
    /// Non-goal: v1 does not provide full MVCC.
    FullMvcc,
    /// Non-goal: v1 does not claim full ACID support.
    FullAcid,
    /// Non-goal: v1 does not require handwritten assembly.
    HandwrittenAssembly,
}

impl EngineNonGoal {
    pub const VARIANTS: [Self; 10] = [
        Self::SqlCompatibility,
        Self::NetworkServer,
        Self::DistributedBehavior,
        Self::CrossDocumentTransactions,
        Self::ArbitraryJoins,
        Self::QueryPlanner,
        Self::VectorOrTextSearch,
        Self::FullMvcc,
        Self::FullAcid,
        Self::HandwrittenAssembly,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::SqlCompatibility => "SqlCompatibility",
            Self::NetworkServer => "NetworkServer",
            Self::DistributedBehavior => "DistributedBehavior",
            Self::CrossDocumentTransactions => "CrossDocumentTransactions",
            Self::ArbitraryJoins => "ArbitraryJoins",
            Self::QueryPlanner => "QueryPlanner",
            Self::VectorOrTextSearch => "VectorOrTextSearch",
            Self::FullMvcc => "FullMvcc",
            Self::FullAcid => "FullAcid",
            Self::HandwrittenAssembly => "HandwrittenAssembly",
        }
    }

    pub const fn contract(self) -> &'static str {
        match self {
            Self::SqlCompatibility => {
                "non-goal: SQL compatibility belongs outside the v1 engine core"
            }
            Self::NetworkServer => {
                "non-goal: network server support belongs outside the v1 engine core"
            }
            Self::DistributedBehavior => {
                "non-goal: distributed behavior belongs outside the v1 engine core"
            }
            Self::CrossDocumentTransactions => {
                "non-goal: cross-document transactions belong outside the v1 engine core"
            }
            Self::ArbitraryJoins => "non-goal: arbitrary joins belong outside the v1 engine core",
            Self::QueryPlanner => "non-goal: query planner work belongs outside the v1 engine core",
            Self::VectorOrTextSearch => {
                "non-goal: vector or text search belongs outside the v1 engine core"
            }
            Self::FullMvcc => "non-goal: full MVCC belongs outside the v1 engine core",
            Self::FullAcid => "non-goal: full ACID support is not claimed by the v1 engine core",
            Self::HandwrittenAssembly => {
                "non-goal: handwritten assembly belongs outside the v1 engine core"
            }
        }
    }
}
