# MfS Documentation

MfS (Memory-first Store) is a high-throughput in-process Rust hot storage library.
It provides lock-free and write-behind caching primitives, an optional
write-ahead log, a schema layer with secondary indexes, and a Redis-like
object store — all as library crates with no external service dependency.

## Getting Started

Start with the [architecture overview](architecture.md) to understand how
the crates fit together, then use the [cache selection guide](choosing-a-cache.md)
to pick the right primitive for your workload.

For a quick introduction, run the examples under `examples/` — each demonstrates
one concept. See the [examples index](examples/index.md) for the full list.

## Crate Documentation

- **[mfs-core](core/overview.md)** — Foundation layer: concurrent maps,
  caches, write-behind, WAL durability, S3-FIFO policy cache, and inline
  numeric storage. This is where you start.
- **[mfs-neural](neural/overview.md)** — Dense numeric layers for 8-byte
  value types: `DenseKvMap`, `DenseWriteBehindMap`, `BucketedIndex`.
  Built on top of `mfs-core`.
- **[mfs-store](store/overview.md)** — Hot storage layer: raw key-value mode,
  schema-validated documents, WAL + checkpoint recovery, Redis-like
  value types. Built on top of `mfs-core`.
- **[mfs-compat](compat/overview.md)** — Compatibility layer: Redis-like
  object store, schema-aware document store, SQLite VFS adapter,
  SQL flush planning. Built on top of `mfs-core` and `mfs-store`.

## Cross-Cutting Guides

- **[Choosing a Cache](choosing-a-cache.md)** — Decision tree for selecting
  the right storage primitive for your workload.
- **[Examples](examples/index.md)** — All runnable examples with descriptions
  and run commands.
- **[Contributing](contributing.md)** — Build, test, and contribution guidelines.

## External Resources

- **[README](../README.md)** — Project overview, quickstart, and performance notes.
- **[Benchmarks](../BENCHMARKS.md)** — Full competitor comparison matrix
  (not included in the public release; available in the development repository).
