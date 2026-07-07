# Examples

Runnable examples are registered through workspace crates. Each
demonstrates one concept. Build with `--release` for representative
performance numbers.

## Example Index

| Example | Crate | What It Demonstrates | Run Command |
|---|---|---|---|
| `read_through_cache` | `mfs-core` | Redis-replacement pattern: cache-first reads with write-behind flush to a DB backend | `cargo run -p mfs-core --release --example read_through_cache` |
| `wal_recovery` | `mfs-core` | Crash recovery via append-only WAL with CRC32C integrity and torn-write tolerance | `cargo run -p mfs-core --release --example wal_recovery` |
| `dense_counters` | `mfs-core` | Atomic counters at L1 latency using `DenseU64Lane` with dirty-bit packing | `cargo run -p mfs-core --release --example dense_counters` |
| `inline_u64_map` | `mfs-core` | Seqlock-based `u64 -> u64` map with no per-write allocation | `cargo run -p mfs-core --release --example inline_u64_map` |
| `nosql_raw_kv` | `mfs-db` | Raw bytes, versions, conflicts, and delete against the NoSQL engine | `cargo run -p mfs-db --release --example nosql_raw_kv` |
| `nosql_schema_mode` | `mfs-db` | Schema validation plus put/get for typed documents | `cargo run -p mfs-db --release --example nosql_schema_mode` |
| `nosql_wal_recovery` | `mfs-db` | Raw WAL sync and replay for the NoSQL engine | `cargo run -p mfs-db --release --example nosql_wal_recovery` |
| `nosql_checkpoint_recovery` | `mfs-db` | Checkpoint plus WAL suffix replay for fast recovery | `cargo run -p mfs-db --release --example nosql_checkpoint_recovery` |
| `schema_sqlite_flush` | `mfs-compat` | Schema flush SQL generation: translates `SchemaStore` mutations into upsert/delete SQL | `cargo run -p mfs-compat --release --example schema_sqlite_flush` |
| `mfs_database` | `mfs-compat` | SQLite-backed schema store demo: MfS owns the hot path, SQLite is the swappable persistence layer | `cargo run -p mfs-compat --release --example mfs_database` |
| `object_store_wal` | `mfs-compat` | Crash recovery for Redis-like object-store values via WAL replay into `MfsObjectStore` | `cargo run -p mfs-compat --release --example object_store_wal` |
| `dense_kv_map` | `mfs-neural` | Inline 8-byte values at L1 latency: generation-checked slot reuse, `read_with` zero-copy reads | `cargo run -p mfs-neural --release --example dense_kv_map` |
| `dense_write_behind` | `mfs-neural` | 8-byte values with write-behind durability: dirty queue, version-checked flush, `load_clean` bypass | `cargo run -p mfs-neural --release --example dense_write_behind` |

## Standalone Crate

There is also a standalone SQLite VFS crate under `examples/sqlite_vfs/`
that is not part of the workspace:

```bash
(cd examples/sqlite_vfs && cargo run --release)
```

This demonstrates a page-store VFS adapter for SQLite, letting SQLite
use MfS as its storage backend.

## Where to Start

- **New to MfS?** Start with `read_through_cache` (core) or `nosql_raw_kv` (db).
- **Need durability?** Run `wal_recovery` (core) or `nosql_wal_recovery` (db).
- **Numeric workloads?** Try `dense_counters` (core) or `dense_kv_map` (neural).
- **SQLite integration?** Run `mfs_database` (compat).
