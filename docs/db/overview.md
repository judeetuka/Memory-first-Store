# mfs-db Overview

Embedded, single-process NoSQL engine for `memory-first-store`.

`mfs-db` is the optional durable storage layer built on top of `mfs-core`. It provides two access modes over a shared storage kernel: raw key-value storage for opaque byte pairs, and schema mode for validated documents with secondary indexes and declared references.

## What is mfs-db

`mfs-db` is an embedded NoSQL engine. It runs in the caller's process and owns no network protocol. The engine has two front doors:

- **Raw KV mode** stores opaque byte keys and byte values with per-key versioning. No schema validation, no indexes.
- **Schema mode** validates `Schema` definitions and `SchemaValue` documents before writing them to the same underlying storage. Adds secondary indexes and declared references on top of the shared kernel.

Both modes share the same primary-record storage, per-key version clock, durability path, checkpoint path, and recovery path.

## When to use mfs-db

Use `mfs-db` when you need durable, embedded NoSQL storage in the same process as your application. It sits above `mfs-core` in the workspace dependency order:

1. `mfs-core` -- cache primitives, write-behind, reference WAL.
2. `mfs-neural` -- dense numeric layers.
3. `mfs-db` -- durable NoSQL engine.
4. `mfs-compat` -- compatibility and legacy adapters.

Start with `mfs-core` for in-process caching. Add `mfs-db` when you need crash recovery, schema validation, or secondary indexes.

## Engine contract (v1)

The v1 contract is frozen in `EngineSemantics` and defines what later engine modules must preserve:

| Aspect | v1 Contract |
|---|---|
| Scope | Embedded, single process. No network server, no distributed behavior. |
| Write atomicity | Primary record, declared secondary indexes, and bounded references commit as one unit. |
| Write conflicts | Per-key expected-version checks. Not full MVCC. |
| Read consistency | Latest committed value. Read-your-writes on the same handle. |
| Recovery | Load latest valid checkpoint, then replay WAL records after its LSN. |
| Index consistency | Declared secondary indexes commit or roll back with the primary record. |

### Non-goals

v1 intentionally does not provide: SQL compatibility, network server, distributed behavior, cross-document transactions, arbitrary joins, query planner, vector/text search, full MVCC, or full ACID.

## Durability modes

| Mode | Acknowledgement |
|---|---|
| `MemoryOnly` | After in-memory commit. No WAL or checkpoint durability. |
| `WalAsync` | After WAL record accepted by in-process queue. Replayability waits for sync. |
| `WalGroupCommit` | After the WAL group containing the record completes `sync_data`. |
| `WalSync` | After the WAL record is appended and `sync_data` completes. |
| `SnapshotOnly` | After in-memory commit. Recovery includes it only after a checkpoint contains it. |

## Public modules

| Module | Description |
|---|---|
| `engine` | Core engine: `NoSqlEngine`, config, errors, semantics, types |
| `schema` | `Schema`, `SchemaField`, `SchemaFieldType` definitions |
| `schema_value` | `SchemaValue` document type and codec |
| `value` | `MfsValue` Redis-like value model for object-store API |

## Quick start

```rust
use mfs_db::{
    NoSqlEngine, EngineConfig, RawKey, RawValue,
    WriteOptions, ReadOptions, DurabilityMode,
};

let config = EngineConfig::default()
    .with_durability(DurabilityMode::WalSync)
    .with_wal_path("data.wal");

let engine = NoSqlEngine::open_memory(config)?;
engine.create_raw_collection("users")?;

let key = RawKey::from(&b"user:1"[..]);
let value = RawValue::from(&b"ada"[..]);

engine.put_raw("users", key.clone(), value, WriteOptions::default())?;
let read = engine.get_raw("users", &key, ReadOptions::default())?;
```

## Cross-links

- [Raw KV API](./raw-kv.md) -- raw key-value operations, write/read options, conflict handling
- [Schema Mode](./schema-mode.md) -- schema validation, document CRUD, secondary indexes
- [Values](./values.md) -- `MfsValue` enum, `ValueTag`, encode/decode
- [WAL](./wal.md) -- `RawWalSegmentWriter`, `RawWalSegmentReader`, replay
- [Checkpoint](./checkpoint.md) -- checkpoint write, recovery, checkpoint-then-WAL
