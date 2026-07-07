# InlineU64Map

Lock-free hash map specialized for `u64` keys and `u64` values with inline storage.

## What It Is

`InlineU64Map` stores key-value pairs directly in the bucket as a pair of `AtomicU64`s. No `Box`, no allocation on the hot path, no reclamation overhead. The bucket is the storage.

Where [`ConcurrentMap`](concurrent-map.md) allocates a `Box<Entry<K, V>>` per insert, `InlineU64Map` eliminates that allocation entirely by restricting the type to `(u64, u64)`.

## When to Use

- You need the absolute fastest `u64 -> u64` map for hot-path counters, generation tracking, or slot indices.
- Your keys and values fit in 8 bytes each (or you can transmute them).
- You want lock-free reads without epoch-based reclamation overhead.
- Fixed capacity is acceptable (no resize).

**Don't use when:**
- You need arbitrary key/value types (use `ConcurrentMap` or `LockFreeCache`).
- You need growable capacity.
- Your values are larger than 8 bytes.

## Key Design

### Seqlock Per Slot

Without a `Box` per insert, epoch-based reclamation can't give readers a stable view. Instead, each slot uses a classical **seqlock** pattern:

- Each slot has a 16-bit `meta` atomic:
  - Bit 0: "writing in progress" flag
  - Bits 1-7: 7-bit h2 hash signature (for fast probe skipping)
  - Bits 8-15: version counter (8 bits, wraps; readers re-check)

- **Read path**: load meta v1, load key, load value, load meta v2. If v1 == v2 and bit-0 is clear, the read is consistent. Retry on mismatch.

- **Write path**: CAS meta to set the writing bit, store key + value, then clear the writing bit and increment the version.

Readers spin only on a slot actively being written. Under light contention this is effectively wait-free. Under heavy contention readers retry at most a handful of times.

### Open Addressing with Linear Probing

The map uses open addressing with quadratic probing (step increases by 1, 2, 3...). The h2 signature in the meta field lets probes skip non-matching slots without loading the full key.

### Fixed Capacity

Pre-size at construction. The map rounds up to the next power of two and targets ~67% load factor (capacity * 4/3). No resize is supported.

## Performance

On Skylake T460:

| Operation | Latency | Notes |
|-----------|---------|-------|
| `get(k)` | 5-7 ns | 4 acquire loads + key compare + version check |
| `put(k, v)` | ~14 ns | Insert or update, ~6x faster than papaya on T460 |

On Zen 3 expect each number to drop by ~2x thanks to faster atomic CAS and lower load latency.

## Public API

```rust
use mfs_core::InlineU64Map;

// Construction
let map = InlineU64Map::with_capacity(1024);

// Basic operations
map.insert(key: u64, value: u64) -> InsertOutcome;
map.get(key: u64) -> Option<u64>;
map.remove(key: u64) -> Option<u64>;

// Variants
map.insert_returning_old(key: u64, value: u64) -> (InsertOutcome, Option<u64>);
map.update(key: u64, value: u64) -> Option<u64>;  // update-only, no insert
map.remove_if_value(key: u64, expected: u64) -> bool;  // conditional remove

// Metadata
map.capacity() -> usize;
map.len() -> usize;
map.is_empty() -> bool;
```

### InsertOutcome

```rust
enum InsertOutcome {
    Inserted,   // new entry
    Replaced,   // existing key updated
    Full,       // map at capacity, probe limit exceeded
}
```

### Constraints

- **Sentinel key**: `u64::MAX` is reserved to encode empty slots. Attempting to insert this key panics.
- **Fixed capacity**: no resize. Pre-size or fail loudly.
- **Type restriction**: K and V must be `u64`. Wrap or transmute for other 8-byte payloads.

## Code Example

```rust
use mfs_core::InlineU64Map;
use std::sync::Arc;
use std::thread;

let map = Arc::new(InlineU64Map::with_capacity(16384));

// Concurrent inserts from multiple threads
let mut handles = vec![];
for tid in 0..4 {
    let map = Arc::clone(&map);
    handles.push(thread::spawn(move || {
        for i in 0..1000u64 {
            let key = tid as u64 * 1000 + i + 1;
            map.insert(key, key * 7);
        }
    }));
}
for h in handles {
    h.join().unwrap();
}

// Reads are lock-free and consistent
for tid in 0..4u64 {
    for i in 0..1000u64 {
        let key = tid * 1000 + i + 1;
        assert_eq!(map.get(key), Some(key * 7));
    }
}
```

## Cross-Links

- [ConcurrentMap](concurrent-map.md) — the general-purpose lock-free map with boxed entries
- [LockFreeCache](lockfree-cache.md) — cache built on `ConcurrentMap` with epoch-based reclamation
- [DenseU64Lane](dense-u64-lane.md) — indexed (non-hashed) atomic u64 storage at L1 latency
- [Architecture](../architecture.md) — how `InlineU64Map` fits in the crate layering
