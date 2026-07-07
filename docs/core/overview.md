# mfs-core Overview

High-throughput memory-first storage primitives.

The hot path is in-process RAM only. Nanosecond timings apply to cached memory operations such as `DenseU64Lane::load`, not to database, disk, network, serialization, or fsync work. Persistence is intentionally exposed as a write-behind API through `FlushBackend`; the `durability` module ships a reference WAL implementation. Backend implementations must be idempotent because failed flushes can be retried with the same records.

## What is mfs-core

`mfs-core` is the foundation crate of the memory-first-store workspace. It provides:

- **`MemoryFirstStore`** -- sharded `parking_lot::RwLock` over `hashbrown::HashTable` with per-slot versioning, dirty tracking, and sampled access timestamps. The safe default for general-purpose keyed storage.
- **`DenseU64Lane`** -- a dense atomic numeric lane for ultra-hot `u64` state. No hashing, no locks. Values are 63-bit (bit 63 is the dirty flag).
- **`ConcurrentMap`** -- an in-house lock-free open-addressed hash map with `seize` hyaline reclamation. Fixed capacity, boxed entries, 7-bit metadata tags for fast probe skipping.
- **`FlushBackend` trait** -- the write-behind persistence interface. Implement it to plug in any durable target (WAL, database, object store).
- **`durability::WalBackend`** -- a reference append-only WAL with CRC32C integrity and configurable fsync thresholds.
- **Admission and eviction policies** -- `s3fifo`, `tiny_lfu`, `bounded_reclaim`, and `writeback` modules for cache admission and eviction.

## When to use mfs-core

Use `mfs-core` when you need an embedded, in-process cache that lives in the same process as your application. It replaces the Redis round-trip for hot data, bringing latencies from microseconds down to tens of nanoseconds.

Start with `mfs-core` as the first workspace dependency. Add `mfs-neural` for dense numeric layers, `mfs-db` for the optional durable NoSQL engine, and `mfs-compat` only for legacy adapters.

## Key design decisions

### DenseU64Lane: packed dirty bit

`DenseU64Lane` packs the dirty flag into bit 63 of the value itself. Stores complete in a single atomic write and the parallel dirty array is gone, so writes touch one cache line and are immune to cross-index false sharing on the dirty side. Values are restricted to 63 bits.

### MemoryFirstStore: sharded RwLock + HashTable

`MemoryFirstStore` is sharded with `parking_lot::RwLock` (single-word uncontended fast path) over `hashbrown::HashTable` (SIMD-tagged probing, no double-hashing because the key hash is pre-computed for shard selection and reused for the bucket lookup). Each shard sits in a `CachePadded` cell so adjacent shards never share a cache line, and the global logical clock sits in its own `CachePadded` cell.

### Sampled access tracking

Only ~1/64 of `get` calls advance the logical clock and update `last_touch`. The sample rate keys off the hash of the key itself (a value already needed to pick a shard) and so adds zero additional work. The idle heuristic is coarse but free. W-TinyLFU and Caffeine-style caches use the same trick.

### read_with: zero-clone reads

`MemoryFirstStore::read_with` avoids the `Arc::clone` on reads when the caller can express the read as a closure scoped to the read guard.

## Public modules

| Module | Description |
|---|---|
| `concurrent_map` | Lock-free open-addressed hash map |
| `inline_map` | Seqlock-based inline-value map |
| `lockfree` | `LockFreeCache` wrapping `ConcurrentMap` |
| `partitioned_lockfree` | Partitioned variant of `LockFreeCache` |
| `writeback` | `WriteBehindCache` with dirty queues |
| `durability` | WAL backend for crash recovery |
| `s3fifo` | S3-FIFO admission policy |
| `tiny_lfu` | TinyLFU admission filter |
| `bounded_reclaim` | Bounded reclamation strategies |
| `atomic_writeback` | (experimental) Atomic write-behind cache |
| `slot_writeback` | (experimental) Slot-based write-behind |

## Public types and functions

| Export | Description |
|---|---|
| `MemoryFirstStore<K, V, S>` | Sharded store with versioning and dirty tracking |
| `DenseU64Lane` | Dense atomic `u64` lane with packed dirty bit |
| `FlushBackend<K, V>` | Write-behind persistence trait |
| `FlushRecord<K, V>` | A single record emitted during flush |
| `Operation` | `Put` or `Delete` |
| `StoreConfig` | Shard count and initial capacity |
| `StoreStats` | Live statistics snapshot |
| `FastBuildHasher` / `FastHasher` | Default fast hasher |
| `auto_thread_count(requested)` | Pick a sensible worker thread count |
| `DENSE_VALUE_MAX` | Maximum value in a `DenseU64Lane` slot (2^63 - 1) |

## Code example

```rust
use mfs_core::{MemoryFirstStore, FlushBackend, FlushRecord, Operation};

// Create a store with default config.
let store = MemoryFirstStore::<u64, String>::new();

// Put and get.
store.put(1, "hello".to_string());
let val = store.get(&1).unwrap();
assert_eq!(val.as_ref(), "hello");

// Closure-based read avoids Arc::clone.
let len = store.read_with(&1, |v| v.len());
assert_eq!(len, Some(5));

// Flush idle dirty records through a backend.
struct MyBackend;
impl FlushBackend<u64, String> for MyBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, String>]) -> Result<(), ()> {
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put)    => { /* persist v */ },
                (None,    Operation::Delete) => { /* remove key */ },
                _ => {}
            }
        }
        Ok(())
    }
}

let mut backend = MyBackend;
store.flush_idle(&mut backend, 64, 10_000).unwrap();
```

## Related pages

- [ConcurrentMap](./concurrent-map.md) -- the lock-free hash map underneath `LockFreeCache` and `WriteBehindCache`.
- [MemoryFirstStore](./memory-first-store.md) -- full API reference and design notes for the sharded store.
