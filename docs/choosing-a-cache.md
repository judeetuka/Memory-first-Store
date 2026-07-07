# Choosing a Cache

MfS ships several cache and map types. They share a common
[`FlushBackend`](../crates/mfs-core/src/lib.rs) trait but differ in
concurrency model, value constraints, and durability semantics.

Use this decision tree to narrow down the right primitive for your workload.

## Decision Tree

```text
                        ┌── ultra-hot u64 numeric lanes? ─────────────┐
                        │                                              ▼
                        │                              ┌─ DenseU64Lane ─┐
                        │                                              ▲
need dirty tracking ────┤                                              │
+ flush_idle?           │                                              │
                        │   yes ──── lock-free reads acceptable? ──┐   │
                        │                                          ├─→ WriteBehindCache
                        │                                          │
                        │                                          └─→ MemoryFirstStore
                        │
                        └── no ─── hot keyed 8-byte values? ── yes ─→ DenseKvMap
                                      │
                                      no ───────────────────────────→ LockFreeCache
```

## Branch Explanations

### Do you need dirty tracking and `flush_idle`?

If your workload requires knowing which entries have been mutated since
the last persist, and you want to flush only idle entries (no live
references outside the store), you need one of the dirty-tracking
stores. These are the "safe defaults" for write-behind durability.

**Yes** leads to a second question:

#### Are lock-free reads acceptable?

If you can tolerate the `ConcurrentMap` read path (epoch-pinned acquire
loads, no `RwLock`, no `Arc::clone` on the hot read path), choose
[`WriteBehindCache`](core/writebehind-cache.md). This is the headline
store: lock-free reads plus per-shard FIFO dirty queues that a flusher
drains with version-checked `FlushRecord` emission. Backend errors push
entries back for retry.

If you need straightforward `RwLock` semantics with per-key version
counters, sampled `last_touch` ticks, and explicit dirty flags, choose
[`MemoryFirstStore`](core/memory-first-store.md). It's simpler to reason
about but pays `RwLock` CAS cost on reads.

**No** (you don't need dirty tracking) leads to the next branch.

### Are your values hot keyed 8-byte types?

If your workload is keyed, write-heavy, and each value fits in 8 bytes
(`u64`, `i64`, `f64`, `[u8; 8]`), choose
[`DenseKvMap`](neural/dense-kv-map.md). The index maps `K -> (slot,
generation)` once; existing-key writes update a pre-allocated `AtomicU64`
slot in place, avoiding the boxed-entry replacement and reclamation tail
that hurts hot-key writes on the general-purpose maps.

**No** (arbitrary value types, or reads dominate with no write-behind
need) leads to the default.

### Default: `LockFreeCache`

[`LockFreeCache`](core/lockfree-cache.md) wraps the in-house
[`ConcurrentMap`](core/concurrent-map.md), a lock-free open-addressed
hash table with `seize` hyaline reclamation and 7-bit h2 metadata tags.
Reads pin an epoch, do an acquire load, and return `&V` bound to the
guard. No dirty tracking, no versions, no write-behind. Choose this when
reads dominate and you don't need persistence.

## Sub-Variants

These types sit alongside the main branches for specialised workloads.

### `InlineU64Map`

A seqlock-based `u64 -> u64` map with no per-write allocation. Use this
when you need the absolute floor for `u64`-to-`u64` lookups and can
tolerate seqlock retry on write contention. It's a specialised
alternative to `DenseKvMap` when both key and value are `u64`.

See [`InlineU64Map`](core/inline-u64-map.md).

### `DenseWriteBehindMap`

Same 8-byte value class as `DenseKvMap`, but with write-behind
durability. Mutations enqueue dirty entries; a flusher drains them into
your `FlushBackend`. Use this when you need the dense hot-write path
**and** write-behind persistence, but note that the write-behind
bookkeeping is not the lowest-tail hot-write path.

See [`DenseWriteBehindMap`](neural/dense-writebehind-map.md).

### `S3FifoCache`

A bounded policy-bearing cache implementing the S3-FIFO eviction
algorithm. It intentionally does not modify `LockFreeCache` or
`WriteBehindCache`, because every admission policy has read/write
overhead and the raw hot path must stay policy-free. Use this when you
need better hit ratios than simple LRU or FIFO under scan-heavy
workloads, and you're willing to trade some raw throughput for better
cache efficiency.

See [`S3FifoCache`](core/s3fifo.md).

### `DenseU64Lane`

`Box<[AtomicU64]>` with the dirty flag packed into bit 63 of the value.
Stores complete in a single atomic write; reads are `Relaxed` loads at
the L1 latency floor. No locks, no hashing, no reclamation overhead.
Tradeoff: 63-bit values, indexed access only. Use this for SNN/GNN-class
numeric state where each key is a small contiguous integer.

See [`DenseU64Lane`](core/dense-u64-lane.md).

## Quick Reference

| Type | Dirty Tracking | Lock-Free Reads | Value Constraint | Write-Behind |
|---|---|---|---|---|
| `MemoryFirstStore` | Yes | No (`RwLock`) | Any `Clone` | Yes |
| `LockFreeCache` | No | Yes | Any `Clone` | No |
| `WriteBehindCache` | Yes | Yes | Any `Clone` | Yes |
| `DenseKvMap` | No | Yes | 8-byte (`DenseValue`) | No |
| `InlineU64Map` | No | Yes (seqlock) | `u64 -> u64` | No |
| `DenseWriteBehindMap` | Yes | Yes | 8-byte (`DenseValue`) | Yes |
| `S3FifoCache` | No | No (policy overhead) | Any `Clone + Hash` | No |
| `DenseU64Lane` | Yes (bit 63) | Yes (atomic load) | 63-bit `u64`, indexed | Manual scan |
