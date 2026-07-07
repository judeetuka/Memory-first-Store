# DenseWriteBehindMap

Concurrent keyed map with write-behind flush to a durable backend. Same 8-byte value constraint as `DenseKvMap`, plus dirty tracking and automatic or manual persistence.

## What it is

`DenseWriteBehindMap<K, V>` extends the dense-storage pattern with write-behind semantics. Like `DenseKvMap`, it stores values in pre-allocated `AtomicU64` slots and uses a sparse index for key-to-slot mapping. The difference: every mutation can enqueue a dirty record, and a flusher (background thread or manual caller) drains those records and persists them via your `FlushBackend`.

This is the generalization of `mfs-core::WriteBehindCache` for 8-byte values. It trades arbitrary value types for the same L1-speed update path that `DenseKvMap` provides.

## When to use

- Hot keyed workload with 8-byte values
- You need to persist mutations to a database, file, or other backend
- You want lock-free reads plus write-behind (not synchronous flush on every write)
- Working set fits in memory, fixed capacity is acceptable

**Don't use when:**

- You don't need persistence. Use [`DenseKvMap`](./dense-kv-map.md) instead (simpler, no dirty-tracking overhead).
- Values are arbitrary types. Use `mfs-core::WriteBehindCache`.
- You need queued/eventual writes with explicit visibility control. Use [`QueuedDenseWriteBehindMap`](./queued-write.md).

## Key design

**Index layer:**

Uses [`BucketedIndex`](./bucketed-index.md) instead of `ConcurrentMap`. The bucketed index stores entries inline in fixed-size buckets (32 entries per bucket, each bucket has its own `RwLock`). This avoids per-key `Box<Entry>` allocations on the write path, which matters when you're already doing dirty-queue bookkeeping.

**Value and version tracking:**

Each slot has three atomic fields:

- `values[slot]`: the actual `AtomicU64` value
- `generations[slot]`: generation counter for slot reuse safety (bit 0 is the write lock)
- `versions[slot]`: packed `(version << 1) | dirty_bit`. The version increments on every write; the dirty bit marks whether the slot needs flushing.

**Dirty queue:**

Mutations enqueue `DirtyEntry<K>` records into per-shard `ArrayQueue`s. The shard is chosen by hashing the key. Each dirty entry tracks the key, version, slot, logical clock timestamp, and operation type (`Put` or `Delete`).

**Flush logic:**

`flush_idle` drains dirty entries that have been idle for at least `idle_ticks` (based on a logical clock, not wall-clock time). It version-checks each entry against the current slot state, builds `FlushRecord`s, and calls your backend. On success, it clears the dirty bit. On failure, it requeues the entries for retry.

**Auto-flusher:**

`DenseMapAutoFlusher::spawn` starts one background thread per dirty shard. Each thread adapts its sleep tick based on queue depth: if the queue is growing, it flushes more aggressively; if it's shrinking, it backs off. The flusher wakes on `Condvar` notification when new dirty entries arrive, so latency is low even with long sleep ticks.

## Public API

### Construction

```rust
impl<K, V> DenseWriteBehindMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
{
    /// Construct with expected entry count.
    pub fn with_capacity(expected_entries: usize) -> Self;

    /// Construct with full configuration.
    pub fn with_config(config: WriteBehindConfig) -> Self;
}

impl<K, V, S> DenseWriteBehindMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone,
{
    /// Construct with custom hasher and configuration.
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self;
}
```

### Pin guard

```rust
impl<K, V, S> DenseWriteBehindMap<K, V, S> {
    /// Pin the index epoch. Hold across many ops.
    pub fn pin(&self) -> Pinned<'_, K, V, S>;
}

impl<'g, K, V, S> Pinned<'g, K, V, S> {
    /// Lookup a value.
    pub fn get(&self, key: &K) -> Option<V>;

    /// Insert or update. Marks the slot dirty and enqueues a flush record.
    /// Returns the new version.
    pub fn put(&self, key: K, value: V) -> u64;

    /// Insert or update without marking dirty. Use for loading from backend.
    /// Returns the new version.
    pub fn load_clean(&self, key: K, value: V) -> u64;

    /// Delete a key. Enqueues a delete record for the backend.
    /// Returns the new version.
    pub fn delete(&self, key: K) -> u64;
}
```

### One-shot helpers

```rust
impl<K, V, S> DenseWriteBehindMap<K, V, S> {
    pub fn get(&self, key: &K) -> Option<V>;
    pub fn put(&self, key: K, value: V) -> u64;
    pub fn load_clean(&self, key: K, value: V) -> u64;
    pub fn delete(&self, key: K) -> u64;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn stats(&self) -> DenseWriteBehindStats;
}
```

### Manual flush

```rust
impl<K, V, S> DenseWriteBehindMap<K, V, S> {
    /// Flush idle dirty records to the backend.
    /// Returns the number of records flushed.
    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>;

    /// Flush a specific shard (for parallel flushers).
    pub fn flush_shard_idle<B>(
        &self,
        shard_idx: usize,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>;

    /// Number of dirty shards.
    pub fn shard_count(&self) -> usize;

    /// Current depth of a specific shard's dirty queue.
    pub fn shard_dirty_depth(&self, shard_idx: usize) -> usize;

    /// Capacity of a specific shard's dirty queue.
    pub fn shard_dirty_capacity(&self, shard_idx: usize) -> usize;
}
```

### Auto-flusher

```rust
pub struct DenseMapAutoFlusher { /* ... */ }

impl DenseMapAutoFlusher {
    /// Spawn background flusher threads (one per dirty shard).
    pub fn spawn<K, V, S, B, F>(
        cache: Arc<DenseWriteBehindMap<K, V, S>>,
        backend_factory: F,
        config: AutoFlusherConfig,
    ) -> Self
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
        V: DenseValue,
        S: BuildHasher + Clone + Send + Sync + 'static,
        B: FlushBackend<K, V> + Send + 'static,
        F: FnMut(usize) -> B;

    /// Stop all flusher threads and wait for them to finish.
    pub fn stop(self);
}
```

### Stats

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DenseWriteBehindStats {
    pub len: usize,          // Number of live keys
    pub dirty: usize,        // Number of pending dirty records
    pub logical_clock: u64,  // Current logical clock value
}
```

## Code example

```rust
use mfs_neural::dense_writeback_map::{DenseWriteBehindMap, DenseMapAutoFlusher};
use mfs_core::writeback::{WriteBehindConfig, AutoFlusherConfig};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use std::sync::Arc;

// Define your backend.
struct DbBackend { /* your database connection */ }

impl FlushBackend<String, u64> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<String, u64>]) -> Result<(), ()> {
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put) => {
                    // Upsert to database: r.key => **v
                }
                (None, Operation::Delete) => {
                    // Delete from database: r.key
                }
                _ => {}
            }
        }
        Ok(())
    }
}

// Create the map.
let cache = Arc::new(DenseWriteBehindMap::<String, u64>::with_config(
    WriteBehindConfig {
        initial_capacity: 100_000,
        dirty_shards: 4,
        dirty_queue_capacity: 10_000,
    },
));

// Spawn auto-flusher.
let auto = DenseMapAutoFlusher::spawn(
    Arc::clone(&cache),
    |_shard_idx| DbBackend { /* ... */ },
    AutoFlusherConfig {
        min_tick_ms: 10,
        max_tick_ms: 1000,
        target_depth: 100,
        max_records_per_drain: 1000,
        idle_ticks_threshold: 10,
        final_drain_passes: 4,
    },
);

// Use the map.
cache.put("user:1".to_string(), 42);
cache.put("user:2".to_string(), 99);
cache.delete("user:1".to_string());

// Load from backend without marking dirty (e.g., on cache miss).
cache.load_clean("user:3".to_string(), 100);

// Later, stop the flusher (drains remaining records).
auto.stop();
```

## Configuration

`WriteBehindConfig` controls:

- `initial_capacity`: max number of live keys
- `dirty_shards`: number of dirty queue shards (rounded up to power of two)
- `dirty_queue_capacity`: capacity per shard queue

`AutoFlusherConfig` controls:

- `min_tick_ms` / `max_tick_ms`: sleep tick bounds
- `target_depth`: target queue depth for adaptive tick
- `max_records_per_drain`: max records per flush call
- `idle_ticks_threshold`: minimum idle ticks before a record is eligible
- `final_drain_passes`: how many drain passes on shutdown

## Thread safety

`DenseWriteBehindMap` is `Send + Sync`. All operations are safe for concurrent access. The dirty queue uses lock-free `ArrayQueue`s with backoff retry on full. The auto-flusher spawns one thread per shard, so flush parallelism matches shard count.

## Cross-references

- [Overview](./overview.md): crate-level design and `DenseValue` trait
- [DenseKvMap](./dense-kv-map.md): same pattern without write-behind
- [BucketedIndex](./bucketed-index.md): the index layer used here
- [QueuedDenseWriteBehindMap](./queued-write.md): adds queued/eventual writes on top
- `mfs-core::FlushBackend`: the trait your backend must implement
- `mfs-core::WriteBehindCache`: the arbitrary-value-type equivalent
