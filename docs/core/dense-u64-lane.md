# DenseU64Lane

Dense atomic numeric lane for ultra-hot SNN/GNN-style `u64` state.
L1 cache latency floor.

Source: [`crates/mfs-core/src/lib.rs`](../../crates/mfs-core/src/lib.rs)

## What it is

`DenseU64Lane` is a `Box<[AtomicU64]>` with the dirty flag packed into
bit 63 of the value itself. Stores complete in a single atomic write.
Reads are `Relaxed` loads at the L1 latency floor. No locks, no
hashing, no reclamation overhead.

## When to use it

Use `DenseU64Lane` when:

- Your keys are small contiguous integers (indices, not arbitrary keys)
- Values fit in 63 bits (bit 63 is reserved for the dirty flag)
- You need the absolute lowest read/write latency
- Persistence is a best-effort write-behind snapshot, not a
  version-checked flush

Do **not** use it when:

- You need arbitrary key types (use
  [`LockFreeCache`](./lockfree-cache.md) or
  [`WriteBehindCache`](./writebehind-cache.md))
- You need version-checked write-behind semantics (use
  [`MemoryFirstStore`](./memory-first-store.md))
- Your values exceed 63 bits

## Key design

### Packed dirty bit

Each slot is a single `AtomicU64` whose bit 63 is the dirty flag and
whose low 63 bits are the value payload. Stores set the dirty bit in
the same atomic write that updates the value. There is no parallel dirty
array, so writes touch exactly one cache line and adjacent indices
cannot ping-pong an unrelated dirty line.

```text
  63        62                                          0
+----+-----------------------------------------------+
| D  |              63-bit value payload              |
+----+-----------------------------------------------+
```

### L1 latency

`load` is a single `Relaxed` atomic load plus a bitmask. On Zen 3 this
runs at ~0.99 ns/op (1,005 M ops/s), which is the L1 cache latency
floor for this hardware.

### No hashing, no locks

Access is by index. No hash computation, no shard selection, no epoch
pinning. The entire read path is one instruction plus a mask.

### CAS loop for fetch_add

`fetch_add` uses a compare-and-swap loop because the dirty bit must
remain pinned across the update. The loop preserves the dirty bit while
adding to the value payload.

### Dirty scan with prefetch

`dirty_values` walks the slot array with an 8-element software prefetch
lookahead. `load_many` does the same for batch reads with irregular
access patterns. For sequential reads, the hardware streamer handles
prefetching automatically and scalar `load` in a loop is faster.

### Tradeoffs

- Values are 63-bit. `DENSE_VALUE_MAX` is exposed as a constant
  (`(1 << 63) - 1`).
- Indexed access only. No key hashing.
- `dirty_values` + `mark_clean` do not carry per-slot versions. A
  concurrent writer can race a cleaner. Use `MemoryFirstStore` if you
  need version-checked write-behind.

## Public API reference

### `DenseU64Lane`

| Method | Signature | Notes |
|---|---|---|
| `with_len` | `(usize) -> Self` | Allocate `len` slots, all zeroed |
| `len` | `(&self) -> usize` | Number of slots |
| `is_empty` | `(&self) -> bool` | |
| `load` | `(&self, usize) -> u64` | Read value (dirty bit masked off). ~1 ns. |
| `load_raw` | `(&self, usize) -> u64` | Read packed word including dirty bit |
| `store` | `(&self, usize, u64)` | Set value + mark dirty in one atomic write |
| `fetch_add` | `(&self, usize, u64) -> u64` | Atomic add, returns previous value. CAS loop. |
| `mark_dirty` | `(&self, usize)` | Set dirty bit without changing value |
| `mark_clean` | `(&self, usize)` | Clear dirty bit, preserve value |
| `mark_many_clean` | `(&self, impl IntoIterator<Item = usize>)` | Batch mark_clean |
| `is_dirty` | `(&self, usize) -> bool` | Check dirty bit |
| `dirty_values` | `(&self, usize) -> Vec<(usize, u64)>` | Collect up to `max` dirty (index, value) pairs |
| `load_many` | `(&self, &[usize], &mut [u64])` | Pipelined batch load with prefetch |

### Constants

| Constant | Value | Notes |
|---|---|---|
| `DENSE_VALUE_MAX` | `(1 << 63) - 1` | Maximum representable value |

## Code example

```rust
use mfs_core::DenseU64Lane;
use std::sync::Arc;

// Allocate a lane for 1M counters.
let lane = Arc::new(DenseU64Lane::with_len(1_000_000));

// Store a value (sets dirty bit automatically).
lane.store(42, 99);
assert_eq!(lane.load(42), 99);
assert!(lane.is_dirty(42));

// Atomic increment (CAS loop, preserves dirty bit).
let prev = lane.fetch_add(42, 1);
assert_eq!(prev, 99);
assert_eq!(lane.load(42), 100);

// Batch load with software prefetch (for irregular access patterns).
let indices = vec![0, 42, 1000, 999_999];
let mut out = vec![0u64; indices.len()];
lane.load_many(&indices, &mut out);

// Periodically scan and persist dirty entries.
for (idx, val) in lane.dirty_values(usize::MAX) {
    persist_to_disk(idx, val)?;
}
// Mark persisted slots clean.
lane.mark_many_clean(0..lane.len());

// Maximum value check.
assert_eq!(mfs_core::DENSE_VALUE_MAX, (1u64 << 63) - 1);
lane.store(0, mfs_core::DENSE_VALUE_MAX);
assert_eq!(lane.load(0), mfs_core::DENSE_VALUE_MAX);
```

## Performance

| Operation | Hardware | ns/op | M ops/s |
|---|---|---|---|
| `load` | Zen 3 (5800H) | 0.99 | 1,005 |
| `load` | Zen 3 (5800H) | 2.1-2.2 | ~1,000 (atomic load floor) |
| `store` | Skylake (T460) | ~2 | ~500 |
| `fetch_add` | Skylake (T460) | ~8 | ~125 |

`load` sits at the L1 cache latency floor. There is nothing faster for
an indexed atomic read on this hardware.

## Cross-links

- [`LockFreeCache`](./lockfree-cache.md) for keyed lookups at lock-free
  speed.
- [`WriteBehindCache`](./writebehind-cache.md) for keyed lookups with
  dirty tracking and write-behind.
- [`MemoryFirstStore`](./memory-first-store.md) for version-checked
  write-behind with arbitrary value types.
- [`DenseKvMap`](../../crates/mfs-core/src/dense_kv.rs) (in `mfs-neural`)
  for hot keyed 8-byte values with the same inline-slot design.
