# MemoryFirstStore

Sharded, versioned, write-behind-capable key-value store.

## What it is

`MemoryFirstStore<K, V, S>` is a concurrent key-value store built on sharded `parking_lot::RwLock<hashbrown::HashTable<(K, Slot<V>)>>`. Each slot carries a per-key version counter, a sampled `last_touch` tick, and a dirty flag. The store supports write-behind persistence through the `FlushBackend` trait: `flush_idle` drains records that have been idle for at least N ticks AND have no live `Arc` references outside the store, then calls your backend.

## When to use it

**Use `MemoryFirstStore` when:**

- You want straightforward semantics with modest concurrency requirements.
- You need per-key versioning for write-behind flush correctness.
- You need dirty tracking to know which entries have been modified since last flush.
- You want growable capacity (unlike `ConcurrentMap`, shards can grow).
- You're building a read-through cache backed by a database.

**Use something else when:**

- You need the absolute fastest lock-free reads and don't need dirty tracking. Use `LockFreeCache` or `WriteBehindCache` (both wrap `ConcurrentMap`).
- You need ultra-hot numeric state with no hashing overhead. Use `DenseU64Lane`.
- You need hot keyed 8-byte values with fast writes. Use `DenseKvMap`.

**Compared to alternatives:**

| Feature | `MemoryFirstStore` | `WriteBehindCache` | `LockFreeCache` |
|---|---|---|---|
| Read path | `RwLock` read guard | Lock-free (epoch pin) | Lock-free (epoch pin) |
| Dirty tracking | Yes (per-slot `AtomicBool`) | Yes (per-shard FIFO queue) | No |
| Versioning | Yes (per-slot counter) | Yes (per-entry version) | No |
| Growable | Yes (shards grow) | No (fixed `ConcurrentMap`) | No (fixed `ConcurrentMap`) |
| `Arc::clone` on read | Yes (or use `read_with`) | Yes (or use `read_with`) | No (returns `&V`) |
| Write-behind | Via `flush_idle` | Via dirty queue drain | Not supported |

## Key design decisions

### Sharded RwLock over HashTable

The store is sharded with `parking_lot::RwLock` (single-word uncontended fast path) over `hashbrown::HashTable` (SIMD-tagged probing, no double-hashing because the key hash is pre-computed for shard selection and reused for the bucket lookup).

### CachePadded shards

Each shard sits in a `CachePadded` cell so adjacent shards never share a cache line. The global logical clock sits in its own `CachePadded` cell. This prevents false sharing between shards under concurrent access.

### Power-of-two shard count

Shard count is always rounded up to the next power of two. Shard selection uses a bitmask (`hash & (shard_count - 1)`) instead of a modulo, which is faster.

### Sampled access tracking

Only ~1/64 of `get` calls advance the logical clock and update `last_touch`. The sample rate keys off the hash of the key itself (a value already needed to pick a shard) and so adds zero additional work. The idle heuristic is coarse but free. W-TinyLFU and Caffeine-style caches use the same trick.

The sample mask is 6 bits (`SAMPLE_BITS = 6`, `SAMPLE_MASK = 0x3F`). A get call records a touch only when `hash & SAMPLE_MASK == 0`.

### read_with: zero-clone reads

`read_with` holds the shard read guard for the duration of the caller's closure and returns whatever the closure produces. This skips `Arc::clone` on the hot path entirely.

### Version-checked flush

Each slot has a monotonically increasing version counter. `put`, `load_clean`, and `delete` all increment the version. `mark_flushed_and_evict` checks the version before clearing the dirty flag or evicting, so a concurrent writer that bumps the version between collect and flush prevents stale eviction.

### Arc reference counting for safe eviction

`collect_idle_dirty` skips entries where `Arc::strong_count(value) != 1`, meaning another thread holds a reference. `mark_flushed_and_evict` only evicts when `Arc::strong_count(value) <= 2` (the store's own reference plus the flush record's clone). This prevents evicting data that a reader is actively using.

## Public API

### Types

```rust
pub struct MemoryFirstStore<K, V, S = FastBuildHasher> { ... }

pub struct StoreConfig {
    pub shards: usize,
    pub initial_capacity_per_shard: usize,
}

pub struct StoreStats {
    pub shards: usize,
    pub len: usize,
    pub dirty: usize,
    pub logical_clock: u64,
}

pub struct FlushRecord<K, V> {
    pub key: K,
    pub value: Option<Arc<V>>,
    pub version: u64,
    pub op: Operation,
}

pub enum Operation {
    Put,
    Delete,
}

pub trait FlushBackend<K, V> {
    type Error;
    fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error>;
}
```

### Constructors

```rust
// Default config: shards = next_power_of_two(nproc * 2),
// initial_capacity_per_shard = 1024.
MemoryFirstStore::<K, V>::new() -> Self

// Custom config.
MemoryFirstStore::<K, V>::with_config(config: StoreConfig) -> Self

// Custom hasher + config.
MemoryFirstStore::<K, V, S>::with_hasher_and_config(
    hash_builder: S,
    config: StoreConfig,
) -> Self
```

### Read operations

| Method | Signature | Description |
|---|---|---|
| `get` | `(&self, key: &K) -> Option<Arc<V>>` | Lookup. Returns `Arc<V>` (pays `Arc::clone`). Records sampled touch. |
| `read_with` | `(&self, key: &K, f: F) -> Option<R>` where `F: FnOnce(&V) -> R` | Closure-based read. Holds read guard, skips `Arc::clone`. Records sampled touch. |
| `peek` | `(&self, key: &K) -> Option<Arc<V>>` | Lookup without recording a touch. |
| `get_batch` | `(&self, keys: &[K]) -> Vec<Option<Arc<V>>>` | Pipelined batch get. Pre-hashes all keys, walks contiguous runs of same-shard keys under a single read lock. |

### Write operations

| Method | Signature | Description |
|---|---|---|
| `put` | `(&self, key: K, value: V) -> u64` | Insert or replace. Marks slot dirty. Returns new version. |
| `load_clean` | `(&self, key: K, value: V) -> u64` | Insert or replace. Marks slot clean (won't flush back). Returns new version. |
| `delete` | `(&self, key: K) -> u64` | Tombstone the key. Marks slot dirty. Returns new version. |

### Flush operations

| Method | Signature | Description |
|---|---|---|
| `collect_idle_dirty` | `(&self, idle_ticks: u64, max_records: usize) -> Vec<FlushRecord<K, V>>` | Collect dirty records idle for at least `idle_ticks` with no external `Arc` references. |
| `mark_flushed_and_evict` | `(&self, records: &[FlushRecord<K, V>]) -> usize` | Version-check records, clear dirty flags, evict unreferenced entries. Returns eviction count. |
| `flush_idle` | `(&self, backend: &mut B, idle_ticks: u64, max_records: usize) -> Result<usize, B::Error>` | Combined: collect, flush through backend, mark flushed and evict. Returns eviction count. |

### Stats

| Method | Signature | Description |
|---|---|---|
| `stats` | `(&self) -> StoreStats` | Snapshot of shard count, live entries, dirty count, logical clock. |

## Code example

### Basic usage

```rust
use mfs_core::MemoryFirstStore;

let store = MemoryFirstStore::<u64, String>::new();

// Put returns the version (starts at 1).
let v1 = store.put(1, "hello".to_string());
assert_eq!(v1, 1);

// Get returns Arc<V>.
let val = store.get(&1).unwrap();
assert_eq!(val.as_ref(), "hello");

// Replace increments version.
let v2 = store.put(1, "world".to_string());
assert_eq!(v2, 2);

// read_with avoids Arc::clone.
let len = store.read_with(&1, |v| v.len());
assert_eq!(len, Some(5));

// Delete returns the new version.
let v3 = store.delete(1);
assert_eq!(v3, 3);
assert!(store.get(&1).is_none());
```

### Write-behind with a custom backend

```rust
use mfs_core::{MemoryFirstStore, FlushBackend, FlushRecord, Operation};
use std::sync::Arc;

struct DbBackend { /* your database connection */ }

impl FlushBackend<u64, String> for DbBackend {
    type Error = std::io::Error;

    fn flush(&mut self, records: &[FlushRecord<u64, String>]) -> Result<(), std::io::Error> {
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put)    => { /* upsert r.key => v.as_ref() */ },
                (None,    Operation::Delete) => { /* delete r.key */ },
                _ => {}
            }
        }
        Ok(())
    }
}

let store = MemoryFirstStore::<u64, String>::new();
store.put(1, "hello".to_string());
store.put(2, "world".to_string());
store.delete(3);

// Flush records idle for at least 64 ticks, up to 10,000 at a time.
let mut backend = DbBackend { /* ... */ };
let evicted = store.flush_idle(&mut backend, 64, 10_000)?;
println!("evicted {evicted} entries");
```

### Read-through cache pattern

```rust
use mfs_core::MemoryFirstStore;
use std::sync::Mutex;

struct UserRepo {
    cache: MemoryFirstStore<u64, UserRecord>,
    db: Mutex<Database>,
}

impl UserRepo {
    fn get(&self, id: u64) -> Option<UserRecord> {
        // Cache hit: fast path.
        if let Some(rec) = self.cache.get(&id) {
            return Some(rec.as_ref().clone());
        }
        // Cache miss: fall through to DB.
        let row = self.db.lock().unwrap().select(id)?;
        self.cache.load_clean(id, row.clone());  // clean: won't flush back
        Some(row)
    }

    fn put(&self, id: u64, record: UserRecord) {
        self.cache.put(id, record);  // dirty: will flush to DB
    }
}
```

### Batch reads

```rust
let store = MemoryFirstStore::<u64, u64>::new();
for i in 0..1000u64 {
    store.put(i, i * 10);
}

let keys: Vec<u64> = (0..1000).collect();
let results = store.get_batch(&keys);
// results[i] == Some(Arc<u64>) for each key, in order.
```

## StoreConfig defaults

```rust
impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            shards: next_power_of_two(available_parallelism() * 2),
            initial_capacity_per_shard: 1024,
        }
    }
}
```

The shard count defaults to `nproc * 2` rounded up to the next power of two. On a 4-core machine that's 8 shards. On a 16-core machine that's 32 shards. Each shard starts with capacity for 1024 entries and grows as needed.

## Performance notes

- **`read_with` over `get`.** `get` returns `Arc<V>` and pays an `Arc::clone` (atomic refcount RMW). `read_with` runs your closure inside the guard's lifetime and skips the clone. On Skylake the difference is 36 ns vs 148 ns.
- **`peek` for non-touching reads.** If you don't want to record an access timestamp (for example, background monitoring), use `peek` instead of `get`.
- **`get_batch` for large working sets.** `get_batch` pre-hashes all keys and prefetches shard headers. Useful for DRAM-bound random access on big working sets. On small working sets that fit in L3, scalar `get` in a loop is faster because the prefetch overhead dominates.
- **Failed flushes keep data hot.** If the backend returns an error, `flush_idle` propagates the error and the dirty records remain in the store for retry. The store never loses data on flush failure.

## Related pages

- [mfs-core Overview](./overview.md) -- the crate this type belongs to.
- [ConcurrentMap](./concurrent-map.md) -- the lock-free hash map used by `LockFreeCache` and `WriteBehindCache` instead of `MemoryFirstStore`.
