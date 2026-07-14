# Raw KV API

Opaque byte key-value operations on the hot store.

Raw KV mode stores opaque byte keys and byte values with per-key versioning. No schema validation, no secondary indexes. This is the lower-level mode that schema mode builds on top of.

## When to use raw KV

- You need to store arbitrary byte pairs without schema overhead.
- You're building a higher-level abstraction on top of the engine.
- You want maximum write throughput with minimal validation.
- Your data model is flat key-value, not document-oriented.

## Core types

### `RawKey`

```rust
pub struct RawKey(Vec<u8>);

impl RawKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self;
    pub fn as_bytes(&self) -> &[u8];
    pub fn into_bytes(self) -> Vec<u8>;
}

impl From<Vec<u8>> for RawKey { ... }
impl From<&[u8]> for RawKey { ... }
```

### `RawValue`

```rust
pub struct RawValue(Arc<[u8]>);

impl RawValue {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self;
    pub fn as_bytes(&self) -> &[u8];
    pub fn into_bytes(self) -> Vec<u8>;
}

impl From<Vec<u8>> for RawValue { ... }
impl From<&[u8]> for RawValue { ... }
```

`RawValue` uses `Arc<[u8]>` internally so reads can return cheap clones without copying the underlying bytes.

### `DocumentVersion`

```rust
pub struct DocumentVersion(u64);

impl DocumentVersion {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self;
    pub const fn get(self) -> u64;
}
```

Every key has a per-key version counter. Writes advance the version. Use `expected_version` in `WriteOptions` for optimistic concurrency.

### `WriteOptions`

```rust
pub struct WriteOptions {
    pub durability: Option<DurabilityMode>,
    pub expected_version: Option<DocumentVersion>,
}

impl WriteOptions {
    pub const fn with_durability(self, durability: DurabilityMode) -> Self;
    pub const fn effective_durability(self, engine_default: DurabilityMode) -> DurabilityMode;
}
```

- `durability`: override the engine's default durability for this write.
- `expected_version`: if set, the write succeeds only when the current version matches. Returns `StoreError::Conflict` on mismatch.

### `ReadOptions`

```rust
pub struct ReadOptions {
    pub read_own_writes: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self { read_own_writes: true }
    }
}
```

### `WriteResult`

```rust
pub struct WriteResult {
    pub version: DocumentVersion,
    pub lsn: Option<Lsn>,
    pub acknowledgement: WriteAcknowledgement,
}
```

### `ReadResult`

```rust
pub struct ReadResult {
    pub value: RawValue,
    pub version: DocumentVersion,
}
```

### `WriteAcknowledgement`

```rust
pub enum WriteAcknowledgement {
    MemoryOnly,
    WalBuffered { lsn: Lsn },
    WalSynced { lsn: Lsn },
    WalGroupCommitted { lsn: Lsn },
    SnapshotOnly,
}

impl WriteAcknowledgement {
    pub const fn lsn(self) -> Option<Lsn>;
    pub const fn guarantee(self) -> &'static str;
}
```

## API reference

### `MfsStore::open_memory`

```rust
pub fn open_memory(config: MfsStoreConfig) -> StoreResult<Self>
```

Create an in-memory engine with the given config. Validates the config (non-zero `max_collections`, non-zero `raw_initial_capacity`, `wal_path` required for WAL durability modes).

### `MfsStore::create_raw_collection`

```rust
pub fn create_raw_collection(
    &self,
    name: impl Into<CollectionName>,
) -> StoreResult<CollectionId>
```

Create a new raw collection. Returns `CollectionAlreadyExists` if the name is taken, or `CollectionLimitExceeded` if the engine is at capacity.

### `MfsStore::put_raw`

```rust
pub fn put_raw(
    &self,
    collection: &str,
    key: RawKey,
    value: RawValue,
    options: WriteOptions,
) -> StoreResult<WriteResult>
```

Insert or replace a key-value pair. Advances the per-key version. Respects `expected_version` for optimistic concurrency.

### `MfsStore::compare_put_raw`

```rust
pub fn compare_put_raw(
    &self,
    collection: &str,
    key: RawKey,
    value: RawValue,
    expected_version: DocumentVersion,
) -> StoreResult<WriteResult>
```

Convenience wrapper: put with `expected_version` set. Returns `StoreError::Conflict` if the current version doesn't match.

### `MfsStore::get_raw`

```rust
pub fn get_raw(
    &self,
    collection: &str,
    key: &RawKey,
    options: ReadOptions,
) -> StoreResult<Option<ReadResult>>
```

Read a key. Returns `None` if the key doesn't exist or has been deleted.

### `MfsStore::delete_raw`

```rust
pub fn delete_raw(
    &self,
    collection: &str,
    key: RawKey,
    options: WriteOptions,
) -> StoreResult<WriteResult>
```

Delete a key. The key's version advances. The value is set to `None` (tombstone). Respects `expected_version`.

## Conflict handling

v1 uses per-key expected-version checks. It is not full MVCC and does not expose snapshot isolation across a series of reads.

```rust
let put = engine.put_raw("users", key.clone(), value1, WriteOptions::default())?;
assert_eq!(put.version, DocumentVersion::new(1));

// Succeeds: expected version matches current.
let updated = engine.compare_put_raw("users", key.clone(), value2, put.version)?;
assert_eq!(updated.version, DocumentVersion::new(2));

// Fails: expected version is stale.
let stale = engine.compare_put_raw("users", key, value3, put.version);
assert!(matches!(stale, Err(StoreError::Conflict { .. })));
```

## Code example

```rust
use mfs_store::{
    MfsStore, MfsStoreConfig, RawKey, RawValue,
    WriteOptions, ReadOptions, DocumentVersion, DurabilityMode,
};

let config = MfsStoreConfig::default()
    .with_durability(DurabilityMode::WalSync)
    .with_wal_path("data.wal");

let engine = MfsStore::open_memory(config)?;
engine.create_raw_collection("cache")?;

// Put
let key = RawKey::from(&b"session:abc"[..]);
let value = RawValue::from(&b"user_data"[..]);
let result = engine.put_raw("cache", key.clone(), value, WriteOptions::default())?;
assert_eq!(result.version, DocumentVersion::new(1));

// Get
let read = engine.get_raw("cache", &key, ReadOptions::default())?
    .expect("key exists");
assert_eq!(read.value.as_bytes(), b"user_data");
assert_eq!(read.version, DocumentVersion::new(1));

// Conditional update
let updated = engine.compare_put_raw(
    "cache",
    key.clone(),
    RawValue::from(&b"new_data"[..]),
    result.version,
)?;
assert_eq!(updated.version, DocumentVersion::new(2));

// Delete
engine.delete_raw("cache", key, WriteOptions::default())?;
assert!(engine.get_raw("cache", &key, ReadOptions::default())?.is_none());
```

## Cross-links

- [Overview](./overview.md) -- engine contract, durability modes
- [Schema Mode](./schema-mode.md) -- schema-validated documents on top of raw KV
- [WAL](./wal.md) -- write-ahead log for crash recovery
- [Checkpoint](./checkpoint.md) -- full-state snapshots for fast recovery
