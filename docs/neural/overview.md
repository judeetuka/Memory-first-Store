# mfs-neural: Dense Numeric Storage Primitives

High-throughput, in-process storage for hot keyed data where values fit in 8 bytes. Built on `mfs-core`, this crate trades arbitrary value types for L1-cache-speed reads and writes.

## What is it

`mfs-neural` provides concurrent maps optimized for a specific workload: keyed data where each value is exactly 8 bytes (`u64`, `i64`, `f64`, or `[u8; 8]`). Instead of boxing values on the heap, these maps pack them into pre-allocated `AtomicU64` slots. The result is single-atomic reads and writes with no allocation on the hot path.

The crate sits between `mfs-core` (general-purpose caches and write-behind infrastructure) and your application. Use it when you need keyed access to counters, small numeric state, or packed byte arrays, and the working set fits in memory.

## The DenseValue constraint

Every type in this crate requires values to implement `DenseValue`:

```rust
pub trait DenseValue: Copy + Send + Sync + 'static {
    fn into_u64(self) -> u64;
    fn from_u64(raw: u64) -> Self;
}
```

This trait is sealed. Only four types implement it:

| Type | Encoding |
|------|----------|
| `u64` | Identity |
| `i64` | Bit-cast via `as u64` |
| `f64` | `to_bits()` / `from_bits()` |
| `[u8; 8]` | `u64::from_ne_bytes()` / `to_ne_bytes()` |

The seal exists because raw `Copy + size_of::<T>() == 8` isn't safe enough. References, pointers, and structs with padding can carry invalid bit patterns. Only primitive types with well-defined 64-bit representations are allowed.

## What's in the crate

| Type | Purpose | When to use |
|------|---------|-------------|
| [`DenseKvMap`](./dense-kv-map.md) | Concurrent keyed map, no write-behind | Hot reads and writes, no persistence needed |
| [`DenseWriteBehindMap`](./dense-writebehind-map.md) | Concurrent keyed map with write-behind flush | Same as above, plus dirty tracking and backend flush |
| [`BucketedIndex`](./bucketed-index.md) | Fixed-capacity `K -> u64` handle index | Internal index layer for write-behind variants |
| [`QueuedDenseWriteBehindMap`](./queued-write.md) | Eventual-write wrapper over `DenseWriteBehindMap` | When writes should be queued and applied asynchronously |

Supporting types:

- `DenseU64Lane` (re-exported from `mfs-core`): positional `Box<[AtomicU64]>` for integer-indexed state.
- `InlineHandleIndex`: inline handle storage for single-slot variants.

## Design principles

**Two-layer architecture.** Every keyed map splits storage into an index layer (`K -> slot handle`) and a value layer (`Box<[AtomicU64]>`). The index maps keys to packed `(slot, generation)` handles. The value layer holds the actual data in pre-allocated atomic slots.

**Generation-checked slot reuse.** When a key is deleted, its slot goes back to a free list. A per-slot generation counter prevents stale reads: a reader that cached an old handle will see a generation mismatch and retry. This keeps slot recycling safe under concurrent delete/insert traffic.

**Fixed capacity, no resize.** All maps are pre-sized at construction. This avoids the complexity and latency spikes of live resizing. Size for your steady-state working set.

**Pin guards for tight loops.** `map.pin()` returns a guard that holds an epoch pin into the underlying index. Constructing the guard is the dominant per-call cost. In hot loops, hold one guard across many operations instead of calling `map.get()` repeatedly.

## Performance targets

On Skylake T460 (i5-6300U):

| Operation | `DenseKvMap` | Notes |
|-----------|-------------|-------|
| `get(k)` | ~7 ns | Index lookup, generation check, atomic load, re-check |
| `put(k, v)` existing key | ~17 ns | Index lookup, generation lock, value store, unlock |
| `put(k, v)` new key | ~230-300 ns | One index allocation, one slot fetch |
| `remove(k)` | ~230-300 ns | Plus slot recycle |

Steady-state cache workloads amortize the insert cost. Once a key exists, every subsequent write hits the 17 ns path.

## Choosing the right type

```
Need write-behind to a durable backend?
├── Yes → DenseWriteBehindMap
│         Need queued/eventual writes?
│         ├── Yes → QueuedDenseWriteBehindMap
│         └── No  → DenseWriteBehindMap
└── No  → DenseKvMap

Keys are small contiguous integers (0..N)?
→ Use DenseU64Lane from mfs-core instead (no hash, indexed access)

K = u64, V = u64 specifically?
→ Consider InlineU64Map from mfs-core (seqlock, no Box allocation ever)
```

## Cross-references

- [DenseKvMap](./dense-kv-map.md): the base concurrent map
- [DenseWriteBehindMap](./dense-writebehind-map.md): adds dirty tracking and flush
- [BucketedIndex](./bucketed-index.md): the inline-bucket index layer
- [QueuedDenseWriteBehindMap](./queued-write.md): eventual-write wrapper
- `mfs-core` documentation: `FlushBackend`, `ConcurrentMap`, `WriteBehindConfig`
