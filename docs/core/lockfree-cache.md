# LockFreeCache

Lock-free read-heavy cache built on the in-house
[`ConcurrentMap`](../../crates/mfs-core/src/concurrent_map.rs), an
open-addressed hash table with seize hyaline reclamation and 7-bit h2
metadata tags for fast probe skipping.

Source: [`crates/mfs-core/src/lockfree.rs`](../../crates/mfs-core/src/lockfree.rs)

## What it is

`LockFreeCache<K, V>` is a thin facade over `ConcurrentMap`. Reads pin
an epoch, do an acquire load, and return `&V` bound to the guard. No
`RwLock` CAS, no `Arc::clone`, no global atomic on the read path.

The previous version wrapped `papaya::HashMap`. The migration off papaya
kept the API shape identical so existing call sites don't need changes.

## When to use it

Use `LockFreeCache` when you want the fastest possible concurrent
get/insert path and you do **not** need:

- Per-slot dirty tracking + idle-driven write-behind
- Per-slot version numbers for safe flush-and-evict
- Sampled `last_touch` LRU hints
- The [`FlushBackend`](../../crates/mfs-core/src/lib.rs) trait integration

Use [`MemoryFirstStore`](./memory-first-store.md) or
[`WriteBehindCache`](./writebehind-cache.md) when you need any of the
above.

## Key design

- **Epoch-pinned reads.** `pin()` returns a `Pinned` guard that owns a
  `seize::LocalGuard`. Values returned via `&V` cannot be reclaimed
  underfoot. Drop the guard quickly. Long-lived guards delay
  reclamation.
- **Fixed capacity.** The underlying `ConcurrentMap` is fixed-capacity
  with no live resize. Default is 1,000,000 slots. Pre-size to your
  working set.
- **Insert outcome.** `insert` returns `bool` (true if landed, false if
  the table is full). `try_insert` exposes the underlying
  `InsertOutcome` enum (`Inserted`, `Replaced`, `Full`).
- **No metadata overhead.** No per-slot version, no dirty flag, no
  `last_touch` tick. Just key, value, and the hash table probe.

## Public API reference

### `LockFreeCache<K, V, S>`

| Method | Signature | Notes |
|---|---|---|
| `new` | `() -> Self` | Default capacity (1M slots) |
| `with_capacity` | `(usize) -> Self` | Explicit initial capacity |
| `with_hasher_and_capacity` | `(S, usize) -> Self` | Custom hasher + capacity |
| `pin` | `(&self) -> Pinned<'_, K, V, S>` | Epoch-pin guard for batched ops |
| `len` | `(&self) -> usize` | Live entry count |
| `is_empty` | `(&self) -> bool` | |

### `Pinned<'g, K, V, S>`

| Method | Signature | Notes |
|---|---|---|
| `get` | `(&self, &K) -> Option<&V>` | Lookup, reference valid for guard lifetime |
| `contains_key` | `(&self, &K) -> bool` | |
| `insert` | `(&self, K, V) -> bool` | False if table full |
| `try_insert` | `(&self, K, V) -> InsertOutcome` | Exposes capacity outcome |
| `remove` | `(&self, &K) -> bool` | |
| `len` | `(&self) -> usize` | |
| `is_empty` | `(&self) -> bool` | |

## Code example

```rust
use mfs_core::lockfree::LockFreeCache;

let cache = LockFreeCache::<u64, String>::with_capacity(100_000);

// Hold one pin across many reads to amortize the epoch-pin cost.
{
    let pinned = cache.pin();
    pinned.insert(1, "hello".into());
    pinned.insert(2, "world".into());

    assert_eq!(pinned.get(&1).map(|s| s.as_str()), Some("hello"));
    assert_eq!(pinned.get(&2).map(|s| s.as_str()), Some("world"));
    assert!(pinned.get(&3).is_none());

    pinned.remove(&1);
    assert!(pinned.get(&1).is_none());
}

// Check capacity outcome when the table might be full.
{
    let small = LockFreeCache::<u64, u64>::with_capacity(1);
    let pinned = small.pin();
    assert!(pinned.insert(0, 42));
    // Subsequent inserts may return false once the load factor is hit.
}
```

## Performance

On Skylake (T460, i5-6300U):

| Operation | ns/op | M ops/s |
|---|---|---|
| Read (`LockFreeCache`) | 4.09 | 245 |
| Read (`ConcurrentMap` direct) | 4.01 | 249 |

The `LockFreeCache` facade adds negligible overhead over the raw
`ConcurrentMap`.

## Cross-links

- [`WriteBehindCache`](./writebehind-cache.md) adds dirty tracking and
  write-behind flush on top of the same `ConcurrentMap` read path.
- [`DenseU64Lane`](./dense-u64-lane.md) for ultra-hot numeric state
  where hashing is unnecessary.
- [`MemoryFirstStore`](./memory-first-store.md) for the `RwLock`-based
  store with full version tracking and eviction.
- [`ConcurrentMap`](../../crates/mfs-core/src/concurrent_map.rs) is the
  underlying hash table.
