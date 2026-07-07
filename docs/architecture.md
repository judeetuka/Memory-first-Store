# Architecture

## Crate Dependency Graph

```
                         memory-first-store (facade)
                        /        |        \
                       /         |         \
              mfs-compat      mfs-db    mfs-neural
                  /    \         |         /
                 /      \        |        /
            mfs-core (foundation — not a workspace member dependency of itself)
```

## Layering

| Layer | Crate | Role |
|-------|-------|------|
| Foundation | `mfs-core` | `ConcurrentMap`, `MemoryFirstStore`, `LockFreeCache`, `WriteBehindCache`, `DenseU64Lane`, `InlineU64Map`, WAL (`durability`), S3-FIFO policy cache |
| Domain | `mfs-neural` | Dense numeric storage over mfs-core: `DenseKvMap`, `DenseWriteBehindMap`, `BucketedIndex` |
| Domain | `mfs-db` | NoSQL engine over mfs-core: `NoSqlEngine`, `Schema`, `MfsValue`, checkpoint + WAL recovery |
| Integration | `mfs-compat` | Compatibility adapters over mfs-core + mfs-db: `MfsObjectStore`, `SchemaStore`, SQLite VFS |
| Facade | `memory-first-store` | Re-exports all public types from underlying crates |

## Feature Flags

| Crate | Flag | What It Enables |
|-------|------|-----------------|
| mfs-core | `ahash` | `ahash::RandomState` as `AHashState` (high-performance, non-DoS-safe hashing) |
| mfs-core | `experimental` | `AtomicWriteBehindCache`, `SlotWriteBehindCache` (unstable backends) |
| mfs-neural | `experimental` | `ConcurrentDenseWriteBehindMap` (experimental write-behind index) |
| mfs-db | `json` | JSON support for schema definitions |
| memory-first-store | `ahash` | Pass-through to `mfs-core/ahash` |
| memory-first-store | `json` | Pass-through to `mfs-db/json` |

## Design Philosophy

MfS is memory-first: the hot path is in-process RAM. Persistence is exposed
as a write-behind API through the [`FlushBackend`] trait. The durability
module provides a reference WAL implementation, but the library doesn't
prescribe a specific backend.

**Key design decisions**:

- **No external service dependency.** Everything runs in-process. No Redis
  server, no external database, no network round-trip.

- **Fixed-capacity maps.** `ConcurrentMap` and its dependents are
  fixed-capacity. Pre-size for your expected working set. The opt-in
  `MfsMutableObjectStore` path provides growable maps for Redis-like
  workloads.

- **Write-behind, not write-through.** The `FlushBackend` contract is
  write-behind: mutations land in memory immediately, and a background
  flusher persists them in batches. This keeps the hot path at L1/L2
  cache latency.

- **Tiered defaults.** Simple workloads get `LockFreeCache`. Workloads
  needing durability get `WriteBehindCache`. Redis-like workloads get
  `MfsObjectStore`. Each tier adds a measurable overhead for a
  measurable benefit — pick the lightest tier that meets your needs.
