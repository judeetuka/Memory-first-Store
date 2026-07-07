# WAL (Write-Ahead Log)

Append-only write-ahead log for crash recovery of the NoSQL engine.

The raw WAL stores engine-level put and delete records with per-key versions. It uses length-prefixed records, hardware-accelerated CRC32C integrity, and monotonically increasing LSNs. Replay stops cleanly at the first invalid record (truncation, torn write, checksum mismatch).

## When to use the WAL

- You need crash recovery for engine writes.
- You want durability guarantees beyond in-memory commits.
- You're pairing the engine with periodic checkpoints to bound recovery time.

## Core types

### `RawWalRecord`

```rust
pub struct RawWalRecord {
    pub lsn: Lsn,
    pub version: DocumentVersion,
    pub collection: String,
    pub op: Operation,
    pub key: RawKey,
    pub value: Option<RawValue>,
}
```

Each record captures one engine write: put (with value) or delete (value is `None`).

### `Lsn`

```rust
pub struct Lsn(u64);

impl Lsn {
    pub const ZERO: Self = Self(0);
    pub const fn new(value: u64) -> Self;
    pub const fn get(self) -> u64;
}
```

Log sequence number. Monotonically increasing within a WAL segment.

### `RawWalReplayStats`

```rust
pub struct RawWalReplayStats {
    pub records: usize,
    pub last_lsn: Lsn,
}
```

### `RawWalSegmentWriter`

```rust
pub struct RawWalSegmentWriter {
    // private fields
}

impl RawWalSegmentWriter {
    pub fn open(path: impl AsRef<Path>) -> EngineResult<Self>;
    pub fn path(&self) -> &Path;
    pub fn append_put(
        &mut self,
        collection: &str,
        key: &RawKey,
        value: &RawValue,
    ) -> EngineResult<Lsn>;
    pub fn append_delete(
        &mut self,
        collection: &str,
        key: &RawKey,
    ) -> EngineResult<Lsn>;
    pub fn flush(&mut self) -> EngineResult<()>;
    pub fn sync_now(&mut self) -> EngineResult<()>;
}
```

The writer tracks per-key versions internally. `append_put` and `append_delete` advance the version for the given collection-key pair and return the assigned LSN.

`sync_now` flushes the buffer and calls `sync_data` (fdatasync) on the underlying file. This is the durability barrier: after `sync_now` returns, the records survive power loss.

### `RawWalSegmentReader`

```rust
pub struct RawWalSegmentReader;

impl RawWalSegmentReader {
    pub fn read_records(path: impl AsRef<Path>) -> EngineResult<Vec<RawWalRecord>>;
    pub fn replay_into(
        path: impl AsRef<Path>,
        engine: &NoSqlEngine,
    ) -> EngineResult<RawWalReplayStats>;
    pub fn replay_after(
        path: impl AsRef<Path>,
        engine: &NoSqlEngine,
        after_lsn: Lsn,
    ) -> EngineResult<RawWalReplayStats>;
}
```

### Free functions

```rust
pub fn replay_raw_wal(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
) -> EngineResult<RawWalReplayStats>;

pub fn replay_raw_wal_after(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
    after_lsn: Lsn,
) -> EngineResult<RawWalReplayStats>;
```

## WAL format

Each record has the following on-disk layout:

| Field | Size | Description |
|---|---|---|
| Magic | 4 bytes | `b"MFSW"` in LE |
| Payload length | 4 bytes | LE u32 |
| Payload | variable | See below |
| CRC32C | 4 bytes | Over magic + length + payload |

The payload contains:

| Field | Size | Description |
|---|---|---|
| Format version | 2 bytes | LE u16, currently `2` |
| LSN | 8 bytes | LE u64 |
| Document version | 8 bytes | LE u64 |
| Collection name | 4 + N bytes | LE u32 length + UTF-8 bytes |
| Operation | 1 byte | `1` = put, `2` = delete |
| Key | 4 + N bytes | LE u32 length + bytes |
| Value | 4 + N bytes | LE u32 length + bytes (empty for delete) |

The format version is `RAW_WAL_FORMAT_VERSION = 2`.

### Corruption handling

The reader detects:

- **Bad magic**: first 4 bytes don't match `b"MFSW"`.
- **Unknown format version**: version field doesn't match `RAW_WAL_FORMAT_VERSION`.
- **Checksum mismatch**: CRC32C doesn't match stored checksum.
- **Malformed record**: payload can't be decoded (truncated, invalid UTF-8, unknown operation).
- **Non-monotonic LSN**: LSN is not strictly greater than the previous record's LSN.
- **Payload too large**: payload exceeds `RAW_WAL_MAX_PAYLOAD_BYTES` (64 MB).
- **Field too large**: a field exceeds `RAW_WAL_MAX_FIELD_BYTES` (64 MB).

Torn writes at the tail of the file (partial records from a crash during append) are handled gracefully: the reader stops at the last complete, valid record. The writer truncates the file to the last good offset on open.

## API reference

### `RawWalSegmentWriter::open`

```rust
pub fn open(path: impl AsRef<Path>) -> EngineResult<Self>
```

Open or create a WAL segment file. If the file exists, scans it to find the last valid record, truncates any torn tail, and resumes LSN numbering from the last record's LSN + 1.

### `RawWalSegmentWriter::append_put`

```rust
pub fn append_put(
    &mut self,
    collection: &str,
    key: &RawKey,
    value: &RawValue,
) -> EngineResult<Lsn>
```

Append a put record. Tracks the per-key version internally and assigns the next version.

### `RawWalSegmentWriter::append_delete`

```rust
pub fn append_delete(
    &mut self,
    collection: &str,
    key: &RawKey,
) -> EngineResult<Lsn>
```

Append a delete record.

### `RawWalSegmentWriter::sync_now`

```rust
pub fn sync_now(&mut self) -> EngineResult<()>
```

Flush the buffer and `sync_data` the file. This is the durability barrier.

### `RawWalSegmentReader::replay_into`

```rust
pub fn replay_into(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
) -> EngineResult<RawWalReplayStats>
```

Replay all valid records into the engine. Creates collections as needed. Applies records in LSN order.

### `RawWalSegmentReader::replay_after`

```rust
pub fn replay_after(
    path: impl AsRef<Path>,
    engine: &NoSqlEngine,
    after_lsn: Lsn,
) -> EngineResult<RawWalReplayStats>
```

Replay only records with LSN greater than `after_lsn`. Used after loading a checkpoint to apply only the WAL suffix.

## Code example

```rust
use mfs_db::{
    NoSqlEngine, EngineConfig, RawKey, RawValue,
    RawWalSegmentWriter, RawWalSegmentReader,
    replay_raw_wal,
};

// Write records to WAL
let mut writer = RawWalSegmentWriter::open("data.wal")?;
let key = RawKey::from(&b"user:1"[..]);
let value = RawValue::from(&b"ada"[..]);

let lsn1 = writer.append_put("users", &key, &value)?;
writer.sync_now()?;  // durability barrier

let lsn2 = writer.append_delete("users", &key)?;
writer.sync_now()?;

// Read records back
let records = RawWalSegmentReader::read_records("data.wal")?;
assert_eq!(records.len(), 2);
assert_eq!(records[0].lsn, lsn1);
assert_eq!(records[1].lsn, lsn2);

// Replay into engine
let engine = NoSqlEngine::open_memory(EngineConfig::default())?;
let stats = replay_raw_wal("data.wal", &engine)?;
assert_eq!(stats.records, 2);
assert_eq!(stats.last_lsn, lsn2);
```

## Integration with engine durability

The engine uses the WAL internally when configured with WAL durability modes:

```rust
use mfs_db::{EngineConfig, DurabilityMode};

let config = EngineConfig::default()
    .with_durability(DurabilityMode::WalSync)
    .with_wal_path("data.wal");

let engine = NoSqlEngine::open_memory(config)?;
```

With `WalSync`, every `put_raw` or `delete_raw` call appends to the WAL and calls `sync_now` before returning. With `WalAsync`, the record is buffered but not synced. With `WalGroupCommit`, the engine batches syncs across multiple writes.

## Cross-links

- [Overview](./overview.md) -- engine contract, durability modes
- [Raw KV API](./raw-kv.md) -- raw key-value operations that produce WAL records
- [Checkpoint](./checkpoint.md) -- full-state snapshots for bounding WAL replay
