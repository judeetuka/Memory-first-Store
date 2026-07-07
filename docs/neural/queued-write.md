# QueuedDenseWriteBehindMap

Eventual-write wrapper over [`DenseWriteBehindMap`](./dense-writebehind-map.md). Writes are queued and applied asynchronously; the caller receives a `WriteTicket` that tracks when the write becomes visible.

## What it is

`QueuedDenseWriteBehindMap<K, V>` wraps a `DenseWriteBehindMap` and adds a queued write lane. Instead of applying writes immediately, `put_async` and `delete_async` enqueue commands into per-shard bounded queues. Background threads drain the queues and apply the commands to the underlying map.

The caller receives a `WriteTicket` for each queued operation. The ticket tracks when the operation has been applied, and the caller can poll or block until the write is visible.

This is useful when you want to decouple write submission from write application, for example to batch writes, smooth out latency spikes, or apply backpressure via bounded queues.

## When to use

- You want eventual writes with explicit visibility control
- You need to queue writes and apply them asynchronously
- You want backpressure via bounded queues (return `Full` error when queue is full)
- You're willing to trade immediate visibility for write-path decoupling

**Don't use when:**

- You need immediate write visibility. Use [`DenseWriteBehindMap`](./dense-writebehind-map.md) directly (eager writes).
- You don't need write-behind at all. Use [`DenseKvMap`](./dense-kv-map.md).
- You need synchronous flush on every write. Use `mfs-core::WriteBehindCache` with synchronous flush.

## Key design

**Per-shard queues:**

The map creates one `mpsc::sync_channel` per dirty shard. Each shard has a dedicated background thread that drains its queue and applies commands to the underlying `DenseWriteBehindMap`.

**WriteTicket:**

Each queued operation returns a `WriteTicket` containing a sequence number and a shared `AtomicU64` that tracks the last applied sequence. The ticket's `is_applied()` method checks if the sequence has been reached; `wait_applied()` spins until it is.

**Shard routing:**

Writes are routed to shards by hashing the key and rotating the hash to distribute across shards. This ensures that writes to the same key go to the same shard, preserving FIFO order for that key.

**Barrier:**

`barrier_all()` sends a barrier command to each shard and waits for all shards to acknowledge. This ensures all pending writes have been applied before returning.

**Flush integration:**

`flush_idle()` calls `barrier_all()` first to ensure all queued writes are applied, then delegates to the underlying map's `flush_idle()`. This ensures that flush sees the latest state.

**Shutdown:**

`shutdown()` sends a shutdown command to each shard and waits for all threads to exit. The `Drop` implementation also sends shutdown commands, so the threads are cleaned up even if you don't call `shutdown()` explicitly.

## Public API

### Construction

```rust
impl<K, V> QueuedDenseWriteBehindMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
{
    /// Construct with expected entry count.
    pub fn with_capacity(expected_entries: usize) -> Self;

    /// Construct with full configuration.
    pub fn with_config(config: WriteBehindConfig) -> Self;
}

impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    /// Construct with custom hasher and configuration.
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self;
}
```

### Queued writes

```rust
impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S> {
    /// Queue a put operation. Returns a ticket that tracks when the write is applied.
    /// Blocks if the shard queue is full.
    pub fn put_async(&self, key: K, value: V) -> Result<WriteTicket, QueuedWriteError>;

    /// Queue a put operation. Returns immediately with `Full` error if the shard queue is full.
    pub fn try_put_async(&self, key: K, value: V) -> Result<WriteTicket, QueuedWriteError>;

    /// Queue a delete operation. Returns a ticket that tracks when the delete is applied.
    /// Blocks if the shard queue is full.
    pub fn delete_async(&self, key: K) -> Result<WriteTicket, QueuedWriteError>;

    /// Queue a delete operation. Returns immediately with `Full` error if the shard queue is full.
    pub fn try_delete_async(&self, key: K) -> Result<WriteTicket, QueuedWriteError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedWriteError {
    Closed,  // Shard thread has exited
    Full,    // Shard queue is full (try_* variants only)
}
```

### WriteTicket

```rust
pub struct WriteTicket { /* ... */ }

impl WriteTicket {
    /// Check if the operation has been applied.
    pub fn is_applied(&self) -> bool;

    /// Block until the operation has been applied.
    pub fn wait_applied(&self);
}
```

### Barrier and flush

```rust
impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S> {
    /// Wait for all pending writes to be applied.
    pub fn barrier_all(&self) -> Result<(), QueuedWriteError>;

    /// Flush idle dirty records to the backend.
    /// Calls `barrier_all()` first to ensure all queued writes are applied.
    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>;
}
```

### Read and metadata

```rust
impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S> {
    /// Lookup a value. Note: may not see unapplied queued writes.
    pub fn get(&self, key: &K) -> Option<V>;

    /// Number of live keys.
    pub fn len(&self) -> usize;

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool;

    /// Get stats from the underlying map.
    pub fn stats(&self) -> DenseWriteBehindStats;
}
```

### Shutdown

```rust
impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S> {
    /// Stop all shard threads and wait for them to exit.
    pub fn shutdown(mut self) -> Result<(), QueuedWriteError>;
}
```

## Code example

```rust
use mfs_neural::queued_dense_writeback::QueuedDenseWriteBehindMap;
use mfs_core::writeback::WriteBehindConfig;
use mfs_core::{FlushBackend, FlushRecord, Operation};

// Define your backend.
struct DbBackend { /* ... */ }

impl FlushBackend<u64, u64> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, u64>]) -> Result<(), ()> {
        // Persist records to database...
        Ok(())
    }
}

// Create the map.
let map = QueuedDenseWriteBehindMap::<u64, u64>::with_config(WriteBehindConfig {
    initial_capacity: 10_000,
    dirty_shards: 4,
    dirty_queue_capacity: 1000,
});

// Queue some writes.
let ticket1 = map.put_async(1, 100).unwrap();
let ticket2 = map.put_async(2, 200).unwrap();
let ticket3 = map.delete_async(1).unwrap();

// Wait for specific writes to be applied.
ticket1.wait_applied();
assert!(ticket1.is_applied());

// Or wait for all pending writes.
map.barrier_all().unwrap();

// Now reads see the applied state.
assert_eq!(map.get(&2), Some(200));
assert_eq!(map.get(&1), None); // Deleted

// Flush to backend (barrier_all is called internally).
let mut backend = DbBackend { /* ... */ };
map.flush_idle(&mut backend, 0, 1000).unwrap();

// Shutdown (optional, Drop does this automatically).
map.shutdown().unwrap();
```

## Visibility semantics

**Important:** `get()` reads from the underlying `DenseWriteBehindMap`, which only sees writes that have been applied by the shard threads. A queued write is not visible until its ticket reports `is_applied() == true`.

If you need to read your own writes immediately, either:

1. Call `ticket.wait_applied()` before reading
2. Call `barrier_all()` to wait for all pending writes
3. Use the eager-write `DenseWriteBehindMap` directly

## FIFO ordering

Writes to the same key are routed to the same shard, so they're applied in FIFO order. If you queue `put(k, 1)` then `put(k, 2)`, the final value will be `2`, not `1`.

Writes to different keys may be applied in different orders across shards, since each shard has its own thread.

## Backpressure

The shard queues are bounded (capacity from `WriteBehindConfig::dirty_queue_capacity`). When a queue is full:

- `put_async` / `delete_async` block until space is available
- `try_put_async` / `try_delete_async` return `Err(QueuedWriteError::Full)` immediately

This provides natural backpressure: if the shard thread can't keep up, writers slow down or get rejected.

## Thread safety

`QueuedDenseWriteBehindMap` is `Send + Sync`. The shard threads are spawned on construction and cleaned up on drop or explicit `shutdown()`. All operations are safe for concurrent access from multiple threads.

## Cross-references

- [Overview](./overview.md): crate-level design and `DenseValue` trait
- [DenseWriteBehindMap](./dense-writebehind-map.md): the underlying map
- [DenseKvMap](./dense-kv-map.md): no write-behind variant
- `mfs-core::FlushBackend`: the trait your backend must implement
