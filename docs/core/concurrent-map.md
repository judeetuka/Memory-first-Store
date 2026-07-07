# ConcurrentMap

A lock-free open-addressed concurrent hash map with fixed capacity.

Our in-house replacement for `papaya::HashMap` in the cache hot path. The design takes architectural inspiration from papaya (Ibraheem Ahmed, MIT-licensed; we read but did not copy the source) but is materially simpler.

## What it is

`ConcurrentMap<K, V, S>` is a fixed-capacity, lock-free, open-addressed hash map. It uses quadratic probing with 7-bit per-bucket metadata tags for fast probe skipping, boxed entries, and `seize`-based hyaline reclamation for safe memory deallocation.

## When to use it

**Use `ConcurrentMap` when:**

- You need the fastest possible lock-free reads for keyed data.
- Your working set size is known up front (the capacity is fixed at construction).
- You don't need dirty tracking, versioning, or write-behind semantics.
- You want to avoid external concurrent-map dependencies.

**Use something else when:**

- You need growable capacity. `ConcurrentMap` does not resize. Pre-size or use `rebuild_with_capacity` for maintenance-window growth.
- You need write-behind persistence. Use `WriteBehindCache` (wraps `ConcurrentMap` with dirty queues) or `MemoryFirstStore` (sharded `RwLock` with full flush support).
- You need arbitrary value types with version-checked flush semantics. Use `MemoryFirstStore`.

**Compared to alternatives:**

| Type | Lock-free reads | Dirty tracking | Write-behind | Fixed capacity |
|---|---|---|---|---|
| `ConcurrentMap` | Yes | No | No | Yes |
| `LockFreeCache` | Yes (wraps ConcurrentMap) | No | No | Yes |
| `WriteBehindCache` | Yes (wraps ConcurrentMap) | Yes | Yes | Yes |
| `MemoryFirstStore` | No (RwLock) | Yes | Yes | No (growable) |

## Key design decisions

### Fixed capacity

No incremental resize, no stop-the-world resize. If the table fills, `insert` returns `InsertOutcome::Full`. Callers who need growth should over-size up front or layer their own rebuild on top via `rebuild_with_capacity`.

### Open-addressed with quadratic probing

Probing sequence: `index += stride; stride++` each step (Robin-Hood-style). Per-bucket metadata byte stores the high 7 bits of the key hash so most non-matching probes terminate without touching the entry pointer.

### Boxed entries

Each insert allocates a `Box<Entry<K, V>>` containing the key and value inline. The bucket holds an `AtomicPtr<Entry<K, V>>`. This is the same approach papaya uses. The cost is ~600 ns per first insert of a key on Skylake, ~300 ns on Zen 3.

### Hyaline reclamation via seize

Readers acquire a lightweight `LocalGuard` (a single atomic store, no `SeqCst` fence) that keeps retired entries alive until the guard drops. The thread that holds the last reference to a retire batch is the one that frees it. This gives bounded memory, predictable tail latency, and no unbounded reclamation stalls when a reader is preempted.

### Read hot path

1. Compute `(h1, h2)` from the hash. `h1` indexes the table; `h2` is the 7-bit metadata signature.
2. Probe quadratically.
3. At each slot: load the `AtomicU8` metadata (one acquire load).
4. If `meta == h2`: `guard.protect(&entries[i], Acquire)` to obtain a hyaline-protected pointer and compare keys.
5. If `meta == EMPTY`: the key is absent, return early.
6. Otherwise advance the probe.

On a hit: 1 acquire load on meta + 1 protected load on entry + 1 key comparison + 1 value access. Hot key, all in L1: ~5 ns before guard overhead.

### Reclamation on remove

On `remove`, we `compare_exchange` the bucket to null, then `guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>)`. `seize` batches retired entries and frees them when no guard could still observe them. The local-to-global retire-list batch size is tuned via `DEFAULT_RETIRE_BATCH` (raise it for write-heavy workloads; lower it for memory-tight environments).

## Public API

### Constructors

```rust
// Default hasher, fixed capacity.
ConcurrentMap::<K, V>::with_capacity(capacity: usize) -> Self

// Custom hasher + capacity.
ConcurrentMap::<K, V, S>::with_hasher_and_capacity(
    hash_builder: S,
    capacity: usize,
) -> Self

// Custom hasher + capacity + retire batch size.
ConcurrentMap::<K, V, S>::with_hasher_capacity_and_batch(
    hash_builder: S,
    capacity: usize,
    batch_size: usize,
) -> Self
```

### Convenience methods (one-shot guard per call)

| Method | Signature | Description |
|---|---|---|
| `get_owned` | `(&self, key: &K) -> Option<V>` where `V: Clone` | Lookup and clone the value |
| `read_with` | `(&self, key: &K, f: F) -> Option<R>` where `F: FnOnce(&V) -> R` | Closure-based read, avoids `Clone` |
| `contains_key` | `(&self, key: &K) -> bool` | Check if key exists |
| `insert` | `(&self, key: K, value: V) -> InsertOutcome` | Insert or replace |
| `remove` | `(&self, key: &K) -> Option<V>` where `V: Clone` | Remove and return old value |
| `update_with` | `(&self, key: &K, f: F) -> bool` where `F: FnOnce(&V)` | In-place mutation (not atomic) |
| `for_each` | `(&self, f: F)` where `F: FnMut(&K, &V)` | Iterate all live entries |
| `can_insert_or_replace` | `(&self, key: &K) -> bool` | Check if insert/replace fits in probe window |
| `capacity` | `(&self) -> usize` | Table capacity |
| `len` | `(&self) -> usize` | Live entry count |
| `is_empty` | `(&self) -> bool` | Whether map has no entries |

### Pinned guard (hold across many ops)

```rust
let pinned = map.pin();  // returns Pinned<'_, K, V, S>
```

| Method | Signature | Description |
|---|---|---|
| `get` | `(&self, key: &K) -> Option<&V>` | Lookup, returns reference bound to guard |
| `get_with_hash` | `(&self, key: &K) -> Option<(&V, u64)>` | Lookup + return the hash for reuse |
| `read_with` | `(&self, key: &K, f: F) -> Option<R>` | Closure-based read |
| `contains_key` | `(&self, key: &K) -> bool` | Check existence |
| `insert` | `(&self, key: K, value: V) -> InsertOutcome` | Insert or replace |
| `insert_returning_old` | `(&self, key: K, value: V) -> (InsertOutcome, Option<V>)` | Insert/replace, clone old value |
| `remove` | `(&self, key: &K) -> bool` | Remove without returning value |
| `remove_owned` | `(&self, key: &K) -> Option<V>` | Remove and clone old value |
| `remove_if_value` | `(&self, key: &K, expected: &V) -> bool` | Conditional remove |
| `can_insert_or_replace` | `(&self, key: &K) -> bool` | Probe-window check |

### Maintenance

```rust
// Build a new map with larger capacity, copying live entries.
// Not live resize -- callers swap at an application boundary.
map.rebuild_with_capacity(new_capacity: usize) -> Result<Self, RebuildCapacityError>
```

### InsertOutcome

```rust
enum InsertOutcome {
    Inserted,   // New key added
    Replaced,   // Existing key updated
    Full,       // Table at capacity, probe limit exceeded
}
```

## Code example

```rust
use mfs_core::concurrent_map::{ConcurrentMap, InsertOutcome};

let map = ConcurrentMap::<String, u64>::with_capacity(10_000);

// Insert.
assert_eq!(map.insert("key".into(), 42), InsertOutcome::Inserted);

// One-shot read (pins internally).
assert_eq!(map.get_owned(&"key".into()), Some(42));

// Hold a pin guard across many reads to amortize guard cost.
let pinned = map.pin();
assert_eq!(pinned.get(&"key".into()).copied(), Some(42));
assert!(pinned.contains_key(&"key".into()));

// Closure-based read avoids Clone.
let doubled = map.read_with(&"key".into(), |v| v * 2);
assert_eq!(doubled, Some(84));

// Replace.
assert_eq!(map.insert("key".into(), 99), InsertOutcome::Replaced);

// Remove.
assert_eq!(map.remove(&"key".into()), Some(99));
assert!(map.get_owned(&"key".into()).is_none());

// Maintenance: rebuild with larger capacity.
let bigger = map.rebuild_with_capacity(20_000).unwrap();
```

## Performance notes

- **Pre-size your map.** `ConcurrentMap` is fixed-capacity with no live resize. Size for your steady-state working set.
- **Hold a pin guard in tight loops.** `map.pin()` returns a `Pinned` guard that owns a `seize::LocalGuard`. Constructing the guard is the dominant per-call cost. In tight read loops, hold one guard and run many reads against it.
- **`read_with` over `get_owned`.** `get_owned` pays a `Clone`. `read_with` runs your closure inside the guard's lifetime and skips the clone entirely.
- **Retire batch tuning.** `DEFAULT_RETIRE_BATCH` is 4096. Raise it for write-heavy replace loops to lower per-write overhead. Lower it for memory-tight environments.

## Related pages

- [mfs-core Overview](./overview.md) -- the crate this type belongs to.
- [MemoryFirstStore](./memory-first-store.md) -- the sharded store that uses `RwLock<HashTable>` instead of `ConcurrentMap`.
