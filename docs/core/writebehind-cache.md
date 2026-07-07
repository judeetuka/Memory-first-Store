# WriteBehindCache

Lock-free write-behind cache. The headline store.

Source: [`crates/mfs-core/src/writeback.rs`](../../crates/mfs-core/src/writeback.rs)

## What it is

`WriteBehindCache<K, V>` gives you the read-path speed of
[`ConcurrentMap`](../../crates/mfs-core/src/concurrent_map.rs)
(epoch-protected lookups, no `RwLock`, no `Arc::clone` on the
`read_with` path) plus the dirty-tracking and write-behind flush
semantics of [`MemoryFirstStore`](./memory-first-store.md) (per-key
version, idle detection, `FlushBackend` integration).

## When to use it

Choose `WriteBehindCache` when you want lock-free reads and the
original "save when no longer in use" write-behind semantics. This is
the right choice for:

- Read-heavy workloads that also need persistence
- Scenarios where mutations should batch-flush to a backend rather than
  writing synchronously
- Cases where you need per-key version tracking for stale-entry
  detection during flush

Choose [`LockFreeCache`](./lockfree-cache.md) when you don't need dirty
tracking or write-behind. Choose
[`MemoryFirstStore`](./memory-first-store.md) when you want
`RwLock`-based semantics with modest concurrency.

## Key design

### Storage

`ConcurrentMap<K, ValueRecord<V>>`. Each value record carries:

- `value: Option<Arc<V>>` (live value, or `None` for a tombstone
  awaiting flush)
- `version: u64` (per-write monotonic version from the global logical
  clock)
- `last_touch: AtomicU64` (sampled tick for idle detection)

### Dirty tracking

Per-shard lock-free bounded MPMC ring buffers
(`crossbeam_queue::ArrayQueue`) of `(key, version, op)` triples. Each
shard sits in its own `CachePadded` cell so writers on different shards
never bounce a cache line. Concurrent writers on the same shard never
serialise on a mutex. Pushes are CAS-based.

### Versioning

Every mutation tags the record with the current logical clock tick. The
flusher uses this tick as a version to detect stale dirty entries. If a
queued entry's version no longer matches the entry currently in the
map, a later mutation already superseded it and the flusher skips that
entry entirely.

### Pinned writes

`Pinned::put` / `Pinned::delete` reuse the existing epoch pin instead of
constructing a fresh one inside `cache.put()`. In tight write loops the
saved pin construction is measurable.

### Read path cost

```text
get          : pin epoch + map lookup + sampled tick update + Arc::clone
read_with    : pin epoch + map lookup + sampled tick update + closure
```

No `RwLock` acquire. No global atomic on the hot path (the clock is
sampled at ~1/64 of `get`s). The `Arc::clone` in `get` is the only
refcount RMW. Use `read_with` or `get_ref` to skip it.

### Failure semantics

`flush_idle` drains eligible dirty entries from each shard, builds
`FlushRecord`s, calls the backend, and on success drops the drained
entries. On backend error the drained entries are pushed back to the
shard tail and the error is propagated. The map state is unchanged
either way. Data remains hot in RAM until the next successful flush.
Backends should be idempotent because retried records may be visible to
the backend before the error is observed.

## Public API reference

### `WriteBehindConfig`

| Field | Type | Default | Notes |
|---|---|---|---|
| `dirty_shards` | `usize` | `2 * available_parallelism` (power of 2) | More shards = better write scaling |
| `initial_capacity` | `usize` | 1,000,000 | Fixed-capacity map. Pre-size. |
| `dirty_queue_capacity` | `usize` | 16,384 | Per-shard ring buffer bound. Full queue = writer backoff. |

### `WriteBehindCache<K, V, S>`

| Method | Signature | Notes |
|---|---|---|
| `new` | `() -> Self` | Default config |
| `with_config` | `(WriteBehindConfig) -> Self` | |
| `with_capacity` | `(usize) -> Self` | Pre-size the map. Strongly preferred. |
| `with_hasher_and_config` | `(S, WriteBehindConfig) -> Self` | Custom hasher |
| `pin` | `(&self) -> Pinned<'_, K, V, S>` | Epoch-pin guard |
| `get` | `(&self, &K) -> Option<Arc<V>>` | One-shot read (pins internally) |
| `read_with` | `(&self, &K, F) -> Option<R>` | Closure-based, skips `Arc::clone` |
| `peek` | `(&self, &K) -> Option<Arc<V>>` | Read without updating `last_touch` |
| `put` | `(&self, K, V) -> u64` | Insert/replace, marks dirty. Returns version. |
| `try_put` | `(&self, K, V) -> Result<u64, WriteBehindError>` | Fallible variant |
| `put_arc` | `(&self, K, Arc<V>) -> u64` | Insert with existing Arc allocation |
| `load_clean` | `(&self, K, V) -> u64` | Insert without marking dirty (for rehydration) |
| `load_clean_arc` | `(&self, K, Arc<V>) -> u64` | Same, with existing Arc |
| `delete` | `(&self, K) -> u64` | Tombstone. Returns version. |
| `try_delete` | `(&self, K) -> Result<u64, WriteBehindError>` | Fallible variant |
| `stats` | `(&self) -> WriteBehindStats` | O(N). Out of hot path. |
| `len` | `(&self) -> usize` | Live entry count (excludes tombstones) |
| `is_empty` | `(&self) -> bool` | |
| `flush_idle` | `(&self, &mut B, u64, usize) -> Result<usize, B::Error>` | Drain + flush + cleanup |
| `flush_shard_idle` | `(&self, usize, &mut B, u64, usize) -> Result<usize, B::Error>` | Single-shard flush |
| `shard_count` | `(&self) -> usize` | |
| `shard_dirty_depth` | `(&self, usize) -> usize` | Per-shard dirty queue depth |
| `shard_dirty_capacity` | `(&self, usize) -> usize` | Per-shard ring buffer capacity |
| `evict_idle` | `(&self, u64) -> usize` | Remove entries idle for N+ ticks |
| `compact` | `(&self) -> Self` | Maintenance rebuild (requires `V: Clone`) |

### `Pinned<'g, K, V, S>`

| Method | Signature | Notes |
|---|---|---|
| `get` | `(&self, &K) -> Option<Arc<V>>` | |
| `read_with` | `(&self, &K, F) -> Option<R>` | Skips `Arc::clone` |
| `get_ref` | `(&self, &K) -> Option<&V>` | Reference bound to guard lifetime. ~4x faster than `get`. |
| `peek` | `(&self, &K) -> Option<Arc<V>>` | No touch update |
| `contains_key` | `(&self, &K) -> bool` | |
| `put` | `(&self, K, V) -> u64` | Reuses held pin |
| `try_put` | `(&self, K, V) -> Result<u64, WriteBehindError>` | |
| `put_arc` | `(&self, K, Arc<V>) -> u64` | |
| `load_clean` | `(&self, K, V) -> u64` | |
| `load_clean_arc` | `(&self, K, Arc<V>) -> u64` | |
| `delete` | `(&self, K) -> u64` | |
| `try_delete` | `(&self, K) -> Result<u64, WriteBehindError>` | |

### `AutoFlusher`

| Method | Signature | Notes |
|---|---|---|
| `spawn` | `(Arc<WriteBehindCache>, F, AutoFlusherConfig) -> Self` | One flusher thread per shard |
| `stop` | `(self)` | Signal, wake, final drain, join |

`AutoFlusherConfig` fields: `min_tick_ms` (1), `max_tick_ms` (50),
`target_depth` (1024), `max_records_per_drain` (8192),
`idle_ticks_threshold` (32), `final_drain_passes` (16).

## Code example

```rust
use mfs_core::writeback::{WriteBehindCache, AutoFlusher, AutoFlusherConfig};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use std::sync::{Arc, Mutex};

// A simple backend that collects flushed records.
struct DbBackend {
    db: Arc<Mutex<Vec<(u64, String)>>>,
}

impl FlushBackend<u64, String> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, String>]) -> Result<(), ()> {
        let mut db = self.db.lock().unwrap();
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put)    => db.push((r.key, v.as_ref().clone())),
                (None,    Operation::Delete) => { /* delete from db */ }
                _ => {}
            }
        }
        Ok(())
    }
}

let cache = Arc::new(WriteBehindCache::<u64, String>::with_capacity(100_000));

// Write path: mutations mark entries dirty.
cache.put(1, "alice".into());
cache.put(2, "bob".into());
cache.delete(3);

// Read path: use read_with to skip Arc::clone.
let name = cache.read_with(&1, |v| v.clone());
assert_eq!(name, Some("alice".to_string()));

// Or hold a pin for batched reads.
{
    let pinned = cache.pin();
    let a = pinned.get_ref(&1).map(|s| s.as_str());
    let b = pinned.get_ref(&2).map(|s| s.as_str());
    assert_eq!(a, Some("alice"));
    assert_eq!(b, Some("bob"));
}

// Background flusher: one thread per shard, adaptive tick.
let db = Arc::new(Mutex::new(Vec::new()));
let flusher = AutoFlusher::spawn(
    Arc::clone(&cache),
    |_shard_idx| DbBackend { db: Arc::clone(&db) },
    AutoFlusherConfig::default(),
);

// ... later, on shutdown:
flusher.stop();  // final drain + join
```

## Performance notes

- **Pre-size your cache.** `ConcurrentMap` is fixed-capacity. Under-sized
  maps scatter value allocations across the heap and cause read latency
  to fall off a cliff (10x+ regression measured at 1M entries).
- **Hold a pin guard.** `cache.pin()` returns a `Pinned` guard.
  Constructing the guard is the dominant per-call cost. In tight loops,
  hold one guard and run many reads against it.
- **Use `read_with` or `get_ref` over `get`.** `get` returns `Arc<V>`
  and pays an `Arc::clone` (atomic refcount RMW). `read_with` runs your
  closure inside the guard's lifetime. `get_ref` returns `&V` bound to
  the guard. On Skylake the difference is 36 ns vs 148 ns. On Zen 3
  it's 18 ns vs 25 ns.
- **Pinned writes save a pin construction.** In tight write loops, use
  `Pinned::put` / `Pinned::delete` instead of `cache.put` /
  `cache.delete`.

## A note on V3

Today's write path allocates one `Box<Entry<K, ValueRecord<V>>>` per
mutation (matching papaya's cost). A planned V3 refactor splits storage
into `ConcurrentMap<K, u32>` (key to slot) plus a pre-allocated slot
array of `ValueRecord<V>`, mirroring `DenseKvMap`. Updates of existing
keys would then drop from ~150 ns to ~10 ns per write. See
`docs/DESIGN-v3.md` for the architectural sketch.

## Cross-links

- [`LockFreeCache`](./lockfree-cache.md) for pure speed without dirty
  tracking.
- [`MemoryFirstStore`](./memory-first-store.md) for `RwLock`-based
  semantics with full version tracking.
- [`DenseU64Lane`](./dense-u64-lane.md) for ultra-hot numeric state.
- [`FlushBackend`](../../crates/mfs-core/src/lib.rs) trait for
  implementing custom flush targets.
- [`durability::WalBackend`](../../crates/mfs-core/src/durability.rs)
  for a reference WAL implementation.
