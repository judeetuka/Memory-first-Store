# Checkpoint

Full-state snapshots for fast recovery. Checkpoints capture the entire engine state at a point in time, so recovery only needs to replay WAL records after the checkpoint's LSN.

## When to use checkpoints

- You want to bound recovery time. Without checkpoints, recovery replays the entire WAL from the beginning.
- You're running the engine with WAL durability and want faster restarts.
- You need to snapshot engine state for backup or migration.

## Core types

### `RawCheckpointMetadata`

```rust
pub struct RawCheckpointMetadata {
    pub format_version: u16,
    pub checkpoint_lsn: Lsn,
    pub engine_max_collections: usize,
    pub engine_raw_initial_capacity: usize,
    pub collection_count: usize,
    pub record_count: usize,
    pub collections: Vec<RawCheckpointCollectionMetadata>,
}
```

### `RawCheckpointCollectionMetadata`

```rust
pub struct RawCheckpointCollectionMetadata {
    pub name: String,
    pub record_count: usize,
}
```

### `RawCheckpointSource`

```rust
pub struct RawCheckpointSource {
    pub path: PathBuf,
    pub metadata: RawCheckpointMetadata,
}
```

### `RawCheckpointLoad`

```rust
pub struct RawCheckpointLoad {
    pub path: PathBuf,
    pub metadata: RawCheckpointMetadata,
    pub store: MfsStore,
}
```

### `RawRecovery`

```rust
pub struct RawRecovery {
    pub store: MfsStore,
    pub checkpoint: Option<RawCheckpointSource>,
    pub wal: RawWalReplayStats,
}
```

## API reference

### `write_raw_checkpoint_to_dir`

```rust
pub fn write_raw_checkpoint_to_dir(
    dir: impl AsRef<Path>,
    store: &MfsStore,
    checkpoint_lsn: Lsn,
) -> StoreResult<RawCheckpointMetadata>
```

Write a checkpoint of the engine's current state to the given directory. The checkpoint file is named `raw-{lsn:020}.mfschkp`. Creates the directory if it doesn't exist.

The write is atomic: data is written to a temporary file, synced, then renamed into place. If the write fails, the temporary file is cleaned up.

### `write_raw_checkpoint`

```rust
pub fn write_raw_checkpoint(
    path: impl AsRef<Path>,
    store: &MfsStore,
    checkpoint_lsn: Lsn,
) -> StoreResult<RawCheckpointMetadata>
```

Write a checkpoint to a specific file path. Same atomic write semantics as `write_raw_checkpoint_to_dir`.

### `load_latest_raw_checkpoint`

```rust
pub fn load_latest_raw_checkpoint(
    dir: impl AsRef<Path>,
    config: MfsStoreConfig,
) -> StoreResult<Option<RawCheckpointLoad>>
```

Scan a directory for checkpoint files, find the one with the highest LSN, decode it, and reconstruct an engine from its snapshot. Skips corrupted checkpoint files (returns `None` if no valid checkpoint exists).

### `read_raw_checkpoint_metadata`

```rust
pub fn read_raw_checkpoint_metadata(
    path: impl AsRef<Path>,
) -> StoreResult<RawCheckpointMetadata>
```

Read only the metadata from a checkpoint file without reconstructing the engine.

### `recover_raw_checkpoint_then_wal`

```rust
pub fn recover_raw_checkpoint_then_wal(
    checkpoint_dir: impl AsRef<Path>,
    wal_path: impl AsRef<Path>,
    config: MfsStoreConfig,
) -> StoreResult<RawRecovery>
```

Full recovery: load the latest valid checkpoint (if any), then replay WAL records after the checkpoint's LSN. If no valid checkpoint exists, starts from an empty engine and replays the entire WAL.

This is the recommended recovery path. It returns a `RawRecovery` with the reconstructed engine, the checkpoint that was loaded (if any), and the WAL replay stats.

### `raw_checkpoint_path`

```rust
pub fn raw_checkpoint_path(dir: impl AsRef<Path>, checkpoint_lsn: Lsn) -> PathBuf
```

Compute the checkpoint file path for a given LSN: `{dir}/raw-{lsn:020}.mfschkp`.

## Checkpoint format

The on-disk format is:

| Field | Size | Description |
|---|---|---|
| Magic | 4 bytes | `b"MFSC"` in LE |
| Format version | 2 bytes | LE u16, currently `1` |
| Payload length | 8 bytes | LE u64 |
| Payload | variable | See below |
| CRC32C | 4 bytes | Over magic + version + length + payload |

The payload contains:

| Field | Size | Description |
|---|---|---|
| Checkpoint LSN | 8 bytes | LE u64 |
| Engine max collections | 8 bytes | LE u64 |
| Engine raw initial capacity | 8 bytes | LE u64 |
| Collection count | 4 bytes | LE u32 |
| Record count | 8 bytes | LE u64 |
| Collections | variable | Per collection: name + records |

Each collection in the payload:

| Field | Size | Description |
|---|---|---|
| Name | 4 + N bytes | LE u32 length + UTF-8 bytes |
| Record count | 8 bytes | LE u64 |
| Records | variable | Per record: version + key + value marker + value |

Each record:

| Field | Size | Description |
|---|---|---|
| Version | 8 bytes | LE u64 |
| Key | 4 + N bytes | LE u32 length + bytes |
| Marker | 1 byte | `0` = tombstone, `1` = value |
| Value | 4 + N bytes | LE u32 length + bytes (only if marker is `1`) |

The format version is `RAW_CHECKPOINT_FORMAT_VERSION = 1`.

### Limits

```rust
const MAX_CHECKPOINT_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;  // 512 MB
const MAX_CHECKPOINT_FIELD_BYTES: usize = 64 * 1024 * 1024;     // 64 MB
```

### Corruption handling

The decoder detects:

- **Bad magic**: first 4 bytes don't match `b"MFSC"`.
- **Unknown format version**: version field doesn't match `RAW_CHECKPOINT_FORMAT_VERSION`.
- **Checksum mismatch**: CRC32C doesn't match.
- **Malformed checkpoint**: payload can't be decoded (truncated, invalid UTF-8, impossible counts, zero version).
- **Payload too large**: payload exceeds `MAX_CHECKPOINT_PAYLOAD_BYTES`.
- **Field too large**: a field exceeds `MAX_CHECKPOINT_FIELD_BYTES`.

Corrupted checkpoint files are skipped by `load_latest_raw_checkpoint` (the loader continues scanning for the next valid file).

## Code example

```rust
use mfs_store::{
    MfsStore, MfsStoreConfig, RawKey, RawValue,
    WriteOptions, Lsn,
    write_raw_checkpoint_to_dir,
    recover_raw_checkpoint_then_wal,
};

// Create engine and write some data
let config = MfsStoreConfig::default()
    .with_wal_path("data.wal")
    .with_checkpoint_dir("checkpoints");

let engine = MfsStore::open_memory(config.clone())?;
engine.create_raw_collection("users")?;

let key = RawKey::from(&b"user:1"[..]);
let value = RawValue::from(&b"ada"[..]);
let result = engine.put_raw("users", key, value, WriteOptions::default())?;

// Write checkpoint at current LSN
let checkpoint_lsn = result.lsn.unwrap_or(Lsn::ZERO);
let metadata = write_raw_checkpoint_to_dir("checkpoints", &engine, checkpoint_lsn)?;
assert_eq!(metadata.checkpoint_lsn, checkpoint_lsn);
assert_eq!(metadata.collection_count, 1);
assert_eq!(metadata.record_count, 1);

// Later: recover from checkpoint + WAL
let recovery = recover_raw_checkpoint_then_wal(
    "checkpoints",
    "data.wal",
    config,
)?;

let engine = recovery.engine;
assert!(recovery.checkpoint.is_some());
// WAL replay stats show how many records were replayed after the checkpoint
```

## Recovery flow

The standard recovery flow is:

1. Call `recover_raw_checkpoint_then_wal(checkpoint_dir, wal_path, config)`.
2. The function loads the latest valid checkpoint from the directory (if any).
3. It replays WAL records with LSN greater than the checkpoint's LSN.
4. Returns a `RawRecovery` with the reconstructed engine.

If no valid checkpoint exists, the function creates an empty engine and replays the entire WAL from the beginning.

## Checkpoint strategy

Checkpoints are expensive (they snapshot the entire engine state). A common strategy is:

- Write checkpoints periodically (e.g., every N minutes or every M WAL records).
- Keep the last few checkpoints and delete older ones.
- Use `write_raw_checkpoint_to_dir` with an incrementing LSN.

The checkpoint LSN should be the LSN of the last WAL record included in the checkpoint. On recovery, only WAL records after that LSN need to be replayed.

## Cross-links

- [Overview](./overview.md) -- engine contract, recovery precedence
- [WAL](./wal.md) -- write-ahead log that checkpoints complement
- [Raw KV API](./raw-kv.md) -- raw operations that produce WAL records
