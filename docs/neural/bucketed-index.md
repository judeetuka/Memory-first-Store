# BucketedIndex

Fixed-capacity `K -> u64` handle index with inline bucket storage. Used as the index layer for [`DenseWriteBehindMap`](./dense-writebehind-map.md) and other dense variants where write speed matters more than fully lock-free reads.

## What it is

`BucketedIndex<K, S>` is a concurrent hash table that stores `(K, handle)` pairs inline inside fixed-size buckets. Each bucket holds up to 32 entries and has its own `parking_lot::RwLock`. This avoids per-key `Box<Entry>` allocations on the write path, which is the bottleneck in boxed concurrent hash tables like `papaya` or `ConcurrentMap`.

The tradeoff: reads acquire a read lock on the bucket, so they're not fully lock-free. For write-heavy workloads where you're already doing dirty-queue bookkeeping, the write-path win outweighs the read-path cost.

## When to use

- You're building a dense map variant and need an index layer
- Write speed matters more than lock-free reads
- You want to avoid per-key allocations on insert
- Fixed capacity is acceptable

**Don't use when:**

- You need fully lock-free reads. Use `mfs-core::ConcurrentMap` instead.
- You're building a general-purpose map. `BucketedIndex` is an internal primitive, not a user-facing API.
- You need growable capacity. This is fixed-size.

## Key design

**Bucket structure:**

Each bucket is a `RwLock<BucketInner<K>>` containing an array of 32 `BucketSlot<K>` entries. Each slot is one of:

- `Empty`: unused
- `Tombstone`: deleted entry (probe chain continues)
- `Occupied(BucketEntry<K>)`: live entry with key, h2 tag, and handle

**Hashing:**

Uses the standard h1/h2 split:

- `h1`: lower bits, selects the starting bucket
- `h2`: upper 7 bits, stored inline in each entry for fast rejection during probe

**Open addressing with linear probing:**

When a bucket is full, the index probes subsequent buckets (linear probing). The probe limit is `5 * log2(bucket_count)`, so it scales with table size.

**Tombstone handling:**

Deletes mark slots as `Tombstone` instead of `Empty`, so probe chains aren't broken. On insert, tombstones are reused: the index tracks the first tombstone seen during probe and uses it if the probe reaches the limit without finding an empty slot or existing key.

**Tombstone reuse locks:**

To avoid races when multiple threads try to reuse the same tombstone, each bucket has a corresponding `Mutex<()>` for tombstone-reuse coordination. The lock is only held during the slow-path tombstone reuse, not during normal insert/lookup.

## Public API

### Construction

```rust
impl<K> BucketedIndex<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    /// Construct with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self;
}

impl<K, S> BucketedIndex<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    /// Construct with a custom hasher and capacity.
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self;
}
```

### Lookup

```rust
impl<K, S> BucketedIndex<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    /// Lookup a key and return its handle.
    pub fn get(&self, key: &K) -> Option<u64>;

    /// Check if a key exists.
    pub fn contains_key(&self, key: &K) -> bool;
}
```

### Insert

```rust
impl<K, S> BucketedIndex<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    /// Insert a key-handle pair.
    /// Returns (outcome, old_handle).
    /// - Inserted: new key, old_handle is None
    /// - Replaced: existing key updated, old_handle is Some
    /// - Full: table is full, old_handle is None
    pub fn insert_returning_old(
        &self,
        key: K,
        handle: u64,
    ) -> (BucketedInsertOutcome, Option<u64>);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketedInsertOutcome {
    Inserted,
    Replaced,
    Full,
}
```

### Remove

```rust
impl<K, S> BucketedIndex<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    /// Remove a key and return its handle.
    pub fn remove(&self, key: &K) -> Option<u64>;

    /// Remove a key only if its handle matches the expected value.
    /// Returns true if removed, false if key not found or handle mismatch.
    pub fn remove_if_value(&self, key: &K, expected: &u64) -> bool;
}
```

### Metadata

```rust
impl<K, S> BucketedIndex<K, S> {
    /// Number of live entries.
    pub fn len(&self) -> usize;

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool;

    /// Total capacity (buckets * 32).
    pub fn capacity(&self) -> usize;
}
```

## Code example

```rust
use mfs_neural::bucketed_index::{BucketedIndex, BucketedInsertOutcome};

// Create an index with capacity for ~1000 entries.
let index = BucketedIndex::<String>::with_capacity(1000);

// Insert some keys.
let (outcome, old) = index.insert_returning_old("key1".to_string(), 100);
assert_eq!(outcome, BucketedInsertOutcome::Inserted);
assert_eq!(old, None);

// Lookup.
assert_eq!(index.get(&"key1".to_string()), Some(100));

// Update an existing key.
let (outcome, old) = index.insert_returning_old("key1".to_string(), 200);
assert_eq!(outcome, BucketedInsertOutcome::Replaced);
assert_eq!(old, Some(100));

// Conditional remove.
assert!(!index.remove_if_value(&"key1".to_string(), &999)); // Wrong handle
assert!(index.remove_if_value(&"key1".to_string(), &200));  // Correct handle
assert_eq!(index.get(&"key1".to_string()), None);
```

## Capacity and probing

The index sizes itself to maintain a load factor of ~0.75. When you request capacity `N`, it allocates `ceil(N * 4/3 / 32)` buckets (rounded up to power of two), giving a total capacity of `buckets * 32`.

If the probe chain exceeds `5 * log2(bucket_count)` without finding an empty slot or the target key, the insert returns `Full`. This can happen when the table is heavily loaded or when tombstones fragment the probe chains.

**Tombstone saturation:**

Under heavy delete/insert traffic, tombstones can accumulate and degrade performance. The tombstone-reuse logic mitigates this by reusing tombstones on insert, but if the table is near capacity, you may hit `Full` even though `len() < capacity()`. In practice, this is rare for typical workloads.

## Thread safety

`BucketedIndex` is `Send + Sync`. Reads acquire a read lock on the bucket; writes acquire a write lock. Concurrent reads to the same bucket are lock-free (via `RwLock` read mode). Concurrent writes to the same bucket serialize on the write lock.

The tombstone-reuse path acquires an additional `Mutex` to coordinate reuse across threads, but this is only hit on the slow path when the probe chain is saturated.

## Performance characteristics

- **Reads**: acquire read lock, scan up to 32 entries, check h2 tag for fast rejection. Typical case: 1-2 cache lines.
- **Writes (new key)**: acquire write lock, scan for empty slot or tombstone, insert. If bucket is full, probe next bucket.
- **Writes (existing key)**: acquire write lock, scan for key, update handle in place.
- **Deletes**: acquire write lock, scan for key, mark as tombstone.

Compared to `ConcurrentMap`:

- **Write path**: faster (no `Box<Entry>` allocation)
- **Read path**: slower (read lock vs. lock-free epoch pin)

For write-heavy workloads with dirty-queue bookkeeping, the write-path win dominates.

## Cross-references

- [Overview](./overview.md): crate-level design
- [DenseWriteBehindMap](./dense-writebehind-map.md): primary user of this index
- `mfs-core::ConcurrentMap`: the lock-free alternative for read-heavy workloads
