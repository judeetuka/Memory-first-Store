use crate::store::DurabilityMode;
use crate::schema_value::SchemaValue;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionId(pub u64);

impl CollectionId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionName(String);

impl CollectionName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for CollectionName {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for CollectionName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawKey(Vec<u8>);

impl RawKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for RawKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for RawKey {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<&[u8]> for RawKey {
    fn from(value: &[u8]) -> Self {
        Self::new(value.to_vec())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawValue(Arc<[u8]>);

impl RawValue {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(Arc::from(bytes.into()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl AsRef<[u8]> for RawValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for RawValue {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<&[u8]> for RawValue {
    fn from(value: &[u8]) -> Self {
        Self::new(value.to_vec())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentVersion(pub u64);

impl DocumentVersion {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    pub durability: Option<DurabilityMode>,
    pub expected_version: Option<DocumentVersion>,
}

impl WriteOptions {
    pub const fn with_durability(mut self, durability: DurabilityMode) -> Self {
        self.durability = Some(durability);
        self
    }

    pub const fn effective_durability(self, engine_default: DurabilityMode) -> DurabilityMode {
        match self.durability {
            Some(durability) => durability,
            None => engine_default,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAcknowledgement {
    MemoryOnly,
    WalBuffered { lsn: Lsn },
    WalSynced { lsn: Lsn },
    WalGroupCommitted { lsn: Lsn },
    SnapshotOnly,
}

impl WriteAcknowledgement {
    pub const fn lsn(self) -> Option<Lsn> {
        match self {
            Self::MemoryOnly | Self::SnapshotOnly => None,
            Self::WalBuffered { lsn }
            | Self::WalSynced { lsn }
            | Self::WalGroupCommitted { lsn } => Some(lsn),
        }
    }

    pub const fn guarantee(self) -> &'static str {
        match self {
            Self::MemoryOnly => "memory-visible only; no WAL or checkpoint durability is promised",
            Self::WalBuffered { .. } => {
                "WAL record accepted by the in-process writer buffer; not fsynced or promised replayable"
            }
            Self::WalSynced { .. } => "WAL record has completed sync_data and is replayable",
            Self::WalGroupCommitted { .. } => {
                "WAL group containing the record has completed sync_data and is replayable"
            }
            Self::SnapshotOnly => {
                "memory-visible only; recovery includes it after an explicit checkpoint contains it"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteResult {
    pub version: DocumentVersion,
    pub lsn: Option<Lsn>,
    pub acknowledgement: WriteAcknowledgement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOptions {
    pub read_own_writes: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            read_own_writes: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    pub value: RawValue,
    pub version: DocumentVersion,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldUpdate {
    Set { field: String, value: SchemaValue },
    Unset { field: String },
    Increment { field: String, delta: i64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldUpdateOp {
    pub updates: Vec<FieldUpdate>,
}
