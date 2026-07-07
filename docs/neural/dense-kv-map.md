# DenseKvMap

Concurrent keyed map for 8-byte values. No write-behind, no dirty tracking. Pure speed.

## What it is

`DenseKvMap<K, V>` is a lock-free concurrent hash map where values are stored in pre-allocated `AtomicU64` slots instead of boxed heap allocations. The index layer maps keys to `(slot, generation)` handles; the value layer holds the actual data.

First insert of a key allocates one `Box<Entry<K, u64>>` in the index (matching the floor of any boxed concurrent hash table). After that, updates touch only the atomic value layer. No allocation, no lock, no refcount on the hot update path.

## When to use

- Hot keyed workload where values fit in 8 bytes (`u64`, `i64`, `f64`, `[u8; 8]`)
- Reads and writes dominate, no persistence or flush-to-backend needed
- You want ~17 ns existing-key updates instead of ~114 ns (papaya baseline)
- Working set fits in memory, fixed capacity is acceptable

**Don't use when:**

- You need write-behind to a database or file. Use [`DenseWriteBehindMap`](./dense-writebehind-map.md) instead.
- Values are arbitrary types (strings, structs, etc.). Use `mfs-core::LockFreeCache` or `MemoryFirstStore`.
- Keys are small contiguous integers. Use `mfs-core::DenseU64Lane` (no hash, indexed access).
- `K = u64, V = u64` specifically. Use `mfs-core::InlineU64Map` (seqlock, no Box allocation ever).

## Key design

**Two-layer storage:**

1. **Index**: `mfs_core::concurrent_map::ConcurrentMap<K, u64>` mapping each key to a packed `(slot, generation)` handle.
2. **Values**: `Box<[AtomicU64]>` of `capacity` slots. Reads are one atomic load; writes are one atomic store.

**Generation-checked slot reuse:**

Each slot has a generation counter. When a key is deleted, the slot is recycled and its generation increments. Readers verify the generation before and after loading the value, so a recycled slot can't be mistaken for the old key.

**Slot locking:**

Updates acquire a per-slot write lock (bit 0 of the generation counter) via CAS. This prevents concurrent writers from corrupting the value mid-update. The lock is held only for the duration of the atomic store.

## Public API

### Construction

```rust
impl<K, V> DenseKvMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
{
    /// Construct with the given capacity.
    pub fn with_capacity(capacity: u32) -> Self;
}

impl<K, V, S> DenseKvMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    /// Construct with a custom hasher and capacity.
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: u32) -> Self;
}
```

### Pin guard (for tight loops)

```rust
impl<K, V, S> DenseKvMap<K, V, S> {
    /// Pin the underlying index epoch. Hold across many ops to amortise the pin cost.
    pub fn pin(&self) -> Pinned<'_, K, V, S>;
}

impl<'g, K, V, S> Pinned<'g, K, V, S> {
    /// Lookup. ~5 ns on Skylake when used inside a tight loop.
    pub fn get(&self, key: &K) -> Option<V>;

    /// Closure-based lookup. Avoids copying the value out.
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R;

    /// Update or insert. Hot path (existing key) is ~17 ns on T460.
    pub fn put(&self, key: K, value: V) -> Result<(), V>;

    /// Remove a key and return its value.
    pub fn remove(&self, key: &K) -> Option<V>;

    /// Check if a key exists.
    pub fn contains_key(&self, key: &K) -> bool;
}
```

### One-shot helpers (convenience, slower in tight loops)

```rust
impl<K, V, S> DenseKvMap<K, V, S> {
    /// One-shot lookup. Prefer `Pinned::get` in tight loops.
    pub fn get(&self, key: &K) -> Option<V>;

    /// One-shot closure-based lookup.
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R;

    /// One-shot insert/update.
    pub fn put(&self, key: K, value: V) -> Result<(), V>;

    /// One-shot remove.
    pub fn remove(&self, key: &K) -> Option<V>;

    /// Check if a key exists.
    pub fn contains_key(&self, key: &K) -> bool;

    /// Number of live keys.
    pub fn len(&self) -> usize;

    /// Check if the map is empty.
    pub fn is_empty(&self) -> bool;

    /// Fixed capacity (not current size).
    pub fn capacity(&self) -> u32;
}
```

## Code example

```rust
use mfs_neural::DenseKvMap;

// Create a map with capacity for 100k keys.
let map = DenseKvMap::<String, u64>::with_capacity(100_000);

// Insert some keys.
map.put("counter_a".to_string(), 42).unwrap();
map.put("counter_b".to_string(), 99).unwrap();

// Read a value.
assert_eq!(map.get(&"counter_a".to_string()), Some(42));

// Update an existing key (hot path, ~17 ns).
map.put("counter_a".to_string(), 100).unwrap();
assert_eq!(map.get(&"counter_a".to_string()), Some(100));

// Remove a key.
assert_eq!(map.remove(&"counter_b".to_string()), Some(99));
assert_eq!(map.get(&"counter_b".to_string()), None);

// In a tight loop, hold a pin guard:
let pinned = map.pin();
for i in 0..1000 {
    if let Some(value) = pinned.get(&"counter_a".to_string()) {
        // Use value...
    }
}
```

## Capacity and errors

The map has fixed capacity. If you try to insert a new key when all slots are in use, `put` returns `Err(value)`. Deleted keys recycle their slots, so capacity is the maximum number of *simultaneously live* keys, not the total number of inserts over the map's lifetime.

```rust
let map = DenseKvMap::<u64, u64>::with_capacity(2);
map.put(1, 10).unwrap();
map.put(2, 20).unwrap();
assert!(map.put(3, 30).is_err()); // Full

map.remove(&1); // Recycles slot
map.put(3, 30).unwrap(); // Now succeeds
```

## Thread safety

`DenseKvMap` is `Send + Sync` and safe for concurrent access from multiple threads. All operations are linearizable. The generation-check protocol ensures readers never observe a value from a different key, even when slots are recycled under concurrent delete/insert traffic.

## Cross-references

- [Overview](./overview.md): crate-level design and `DenseValue` trait
- [DenseWriteBehindMap](./dense-writebehind-map.md): adds dirty tracking and backend flush
- [BucketedIndex](./bucketed-index.md): alternative index layer used by write-behind variants
- `mfs-core::ConcurrentMap`: the underlying index implementation
- `mfs-core::InlineU64Map`: specialized `u64 -> u64` variant with seqlock
