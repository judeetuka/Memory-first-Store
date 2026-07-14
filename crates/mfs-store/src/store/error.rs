use std::error::Error;
use std::fmt;

use crate::store::semantics::DurabilityMode;
use crate::store::types::{DocumentVersion, RawKey};
use crate::schema::SchemaError;
use crate::schema_value::{SchemaValueError, SchemaValueKind};

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCorruptionKind {
    BadMagic,
    UnknownFormatVersion,
    ChecksumMismatch,
    MalformedRecord,
    NonMonotonicLsn,
    PayloadTooLarge,
    FieldTooLarge,
    UnknownOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointCorruptionKind {
    BadMagic,
    UnknownFormatVersion,
    ChecksumMismatch,
    MalformedCheckpoint,
    PayloadTooLarge,
    FieldTooLarge,
}

impl CheckpointCorruptionKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::BadMagic => "bad magic",
            Self::UnknownFormatVersion => "unknown format version",
            Self::ChecksumMismatch => "checksum mismatch",
            Self::MalformedCheckpoint => "malformed checkpoint",
            Self::PayloadTooLarge => "payload too large",
            Self::FieldTooLarge => "field too large",
        }
    }
}

impl WalCorruptionKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::BadMagic => "bad magic",
            Self::UnknownFormatVersion => "unknown format version",
            Self::ChecksumMismatch => "checksum mismatch",
            Self::MalformedRecord => "malformed record",
            Self::NonMonotonicLsn => "non-monotonic LSN",
            Self::PayloadTooLarge => "payload too large",
            Self::FieldTooLarge => "field too large",
            Self::UnknownOperation => "unknown operation",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum StoreError {
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },
    CollectionAlreadyExists {
        collection: String,
    },
    CollectionNotFound {
        collection: String,
    },
    CollectionLimitExceeded {
        max_collections: usize,
    },
    CollectionCapacityFull {
        collection: String,
    },
    UnsupportedDurability {
        requested: DurabilityMode,
    },
    Conflict {
        collection: String,
        key: RawKey,
        expected: DocumentVersion,
        actual: DocumentVersion,
    },
    SchemaDefinition {
        collection: String,
        error: SchemaError,
    },
    SchemaDocument {
        collection: String,
        error: SchemaValueError,
    },
    SchemaMissingPrimaryField {
        collection: String,
    },
    SchemaMissingPrimaryKey {
        collection: String,
        field: String,
    },
    SchemaPrimaryKeyTypeMismatch {
        collection: String,
        field: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
    SchemaDecode {
        collection: String,
        message: String,
    },
    SchemaDeclarationMismatch {
        collection: String,
    },
    UnsupportedExactIndex {
        collection: String,
        field: String,
    },
    UnindexedField {
        collection: String,
        field: String,
    },
    SchemaIndexKeyTypeMismatch {
        collection: String,
        field: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
    UniqueIndexConflict {
        collection: String,
        field: String,
        existing: RawKey,
    },
    ReferenceFieldNotFound {
        collection: String,
        field: String,
    },
    ReferenceTargetCollectionNotFound {
        collection: String,
        field: String,
        target_collection: String,
    },
    ReferenceTargetNotPrimary {
        collection: String,
        field: String,
        target_collection: String,
        target_field: String,
    },
    ReferenceTargetMismatch {
        collection: String,
        field: String,
        expected_collection: String,
        actual_collection: String,
    },
    WalCorruption {
        offset: u64,
        kind: WalCorruptionKind,
    },
    WalIo {
        operation: &'static str,
        message: String,
    },
    CheckpointCorruption {
        path: String,
        kind: CheckpointCorruptionKind,
    },
    CheckpointIo {
        operation: &'static str,
        path: String,
        message: String,
    },
    InvalidUpdatePath {
        field: String,
        reason: &'static str,
    },
    PrimaryKeyUpdateForbidden,
    DocumentNotFound {
        collection: String,
    },
    NumericOverflow {
        field: String,
    },
    UpdateTypeMismatch {
        field: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(f, "invalid store config `{field}`: {reason}")
            }
            Self::CollectionAlreadyExists { collection } => {
                write!(f, "collection `{collection}` already exists")
            }
            Self::CollectionNotFound { collection } => {
                write!(f, "collection `{collection}` was not found")
            }
            Self::CollectionLimitExceeded { max_collections } => {
                write!(
                    f,
                    "collection limit exceeded: max_collections={max_collections}"
                )
            }
            Self::CollectionCapacityFull { collection } => {
                write!(f, "collection `{collection}` raw store is full")
            }
            Self::UnsupportedDurability { requested } => {
                write!(
                    f,
                    "durability mode `{}` is not supported by this store configuration",
                    requested.name()
                )
            }
            Self::Conflict {
                collection,
                key,
                expected,
                actual,
            } => write!(
                f,
                "write conflict in collection `{collection}` for {}-byte raw key: expected version {}, actual version {}",
                key.as_bytes().len(),
                expected.get(),
                actual.get()
            ),
            Self::SchemaDefinition { collection, error } => {
                write!(f, "invalid schema `{collection}`: {error}")
            }
            Self::SchemaDocument { collection, error } => {
                write!(f, "invalid schema document for `{collection}`: {error}")
            }
            Self::SchemaMissingPrimaryField { collection } => {
                write!(f, "schema `{collection}` has no primary field")
            }
            Self::SchemaMissingPrimaryKey { collection, field } => {
                write!(
                    f,
                    "schema document for `{collection}` is missing primary key `{field}`"
                )
            }
            Self::SchemaPrimaryKeyTypeMismatch {
                collection,
                field,
                expected,
                actual,
            } => write!(
                f,
                "schema `{collection}` primary key `{field}` expected {expected}, got {actual}"
            ),
            Self::SchemaDecode {
                collection,
                message,
            } => write!(
                f,
                "stored schema document for `{collection}` could not be decoded: {message}"
            ),
            Self::SchemaDeclarationMismatch { collection } => write!(
                f,
                "schema declaration for `{collection}` does not match the registered store schema"
            ),
            Self::UnsupportedExactIndex { collection, field } => write!(
                f,
                "schema `{collection}` field `{field}` cannot use the store exact-match index"
            ),
            Self::UnindexedField { collection, field } => {
                write!(f, "schema `{collection}` field `{field}` is not indexed")
            }
            Self::SchemaIndexKeyTypeMismatch {
                collection,
                field,
                expected,
                actual,
            } => write!(
                f,
                "schema `{collection}` index field `{field}` expected {expected}, got {actual}"
            ),
            Self::UniqueIndexConflict {
                collection,
                field,
                existing,
            } => write!(
                f,
                "unique index conflict in `{collection}`.`{field}` with existing {}-byte primary key",
                existing.as_bytes().len()
            ),
            Self::ReferenceFieldNotFound { collection, field } => write!(
                f,
                "schema `{collection}` field `{field}` is not a declared reference"
            ),
            Self::ReferenceTargetCollectionNotFound {
                collection,
                field,
                target_collection,
            } => write!(
                f,
                "schema `{collection}` reference `{field}` targets unknown collection `{target_collection}`"
            ),
            Self::ReferenceTargetNotPrimary {
                collection,
                field,
                target_collection,
                target_field,
            } => write!(
                f,
                "schema `{collection}` reference `{field}` targets `{target_collection}`.`{target_field}`, which is not the target primary field"
            ),
            Self::ReferenceTargetMismatch {
                collection,
                field,
                expected_collection,
                actual_collection,
            } => write!(
                f,
                "schema `{collection}` reference `{field}` expected target collection `{expected_collection}`, got `{actual_collection}`"
            ),
            Self::WalCorruption { offset, kind } => {
                write!(f, "raw WAL corruption at byte {offset}: {}", kind.name())
            }
            Self::WalIo { operation, message } => {
                write!(f, "raw WAL I/O error during {operation}: {message}")
            }
            Self::CheckpointCorruption { path, kind } => {
                write!(f, "raw checkpoint corruption in `{path}`: {}", kind.name())
            }
            Self::CheckpointIo {
                operation,
                path,
                message,
            } => write!(
                f,
                "raw checkpoint I/O error during {operation} for `{path}`: {message}"
            ),
            Self::InvalidUpdatePath { field, reason } => {
                write!(f, "invalid update path `{field}`: {reason}")
            }
            Self::PrimaryKeyUpdateForbidden => {
                write!(f, "updating a primary key field is forbidden")
            }
            Self::DocumentNotFound { collection } => {
                write!(f, "document not found in collection `{collection}`")
            }
            Self::NumericOverflow { field } => {
                write!(f, "numeric overflow on field `{field}`")
            }
            Self::UpdateTypeMismatch {
                field,
                expected,
                actual,
            } => write!(
                f,
                "update type mismatch for field `{field}`: expected {expected}, got {actual}"
            ),
        }
    }
}

impl Error for StoreError {}
