# memory-first-store

> ⚠️ **Purpose-built**: This project was created as the data engine for a
> Spiking Neural Network / GNN engine and as an embedded SQLite replacement
> for specific workloads. It is opinionated, fixed-capacity, and memory-first
> by design — it may not be the right fit for every use case. See
> [Known Limitations](docs/architecture.md) for details.

High-throughput, in-process Rust storage primitives for hot data: lock-free
concurrent caches with optional write-behind to a durable backend, plus a
dense numeric lane that runs at L1 cache latency.

Built as an embedded alternative to Redis for cases where the cache lives in
the same process as the application and the network round-trip is the
bottleneck. Latencies are measured in tens of nanoseconds, not microseconds.

> Looking for the full benchmark catalogue with raw numbers, hardware
> specs, and the data each benchmark operates on?
> See [`BENCHMARKS.md`](./BENCHMARKS.md).

## At a glance

> **No third-party concurrent-map dependency.** The
> `feat/write-perf-and-auto-threads` branch ships an in-house
> open-addressed lock-free hash table
> ([`ConcurrentMap`](crates/mfs-core/src/concurrent_map.rs)) and a seqlock-based
> inline-value variant ([`InlineU64Map`](crates/mfs-core/src/inline_map.rs)) that
> replaced `papaya::HashMap` throughout. Papaya remains only as a
> `dev-dependency` so the competitor bench can still measure
> against it directly.

Microbenchmarks (T460 Skylake, criterion spot-check after the
slot-generation fix):

| Operation | Backend | ns/op | M ops/s |
|---|---|---|---|
| **Read** | `papaya::HashMap` (reference, dev-dep only) | 5.14 | 195 |
| Read | **`ConcurrentMap`** (ours) | **4.01** | **249** |
| Read | `LockFreeCache` (now ours) | 4.09 | 245 |
| Read | `WriteBehindCache::read_with` (now ours) | 5.11 | 196 |
| Read | `DenseKvMap::Pinned::get` (generation-checked) | 7.25 | 138 |
| Read | `InlineU64Map::get` (seqlock) | 5.45 | 184 |
| Read | `leapfrog::LeapMap` (dev-dep competitor) | 6.94–7.89 | 127–144 |
| Read | `DenseU64Lane::load` (no hash, indexed) | 0.99 | 1,005 |
| **Replace** existing key | `papaya::HashMap` (reference) | 114 | 8.8 |
| Replace | **`DenseKvMap::Pinned::put`** (generation-checked) | **17.4** | **57** |

**Read takeaway**: `ConcurrentMap`/`LockFreeCache` sit at the hot-key
floor for this machine and beat raw papaya in the latest local
spot-check. `DenseKvMap` now pays a small generation-check cost on
reads so it can safely recycle slots under concurrent delete/insert
traffic.

**Write takeaway**: after the slot-generation correctness fix,
`DenseKvMap` still updates existing keys in **~17 ns on Skylake —
~6.5× faster than `papaya::insert`** in the latest local spot-check.
The trick is inline value storage in a pre-allocated
`Box<[AtomicU64]>` lane; the index map stores `K → (slot,
generation)`, so updates skip the per-call `Box<Entry>` allocation
that bottlenecks papaya while preventing stale-slot reads.

Realistic mixed workload (95/4/1 read/write/delete, 80/20 hot-key skew,
100 k user-profile records ~200 B each, background flusher to a counting
backend). T460 is the reference platform; Beelink is a spot-check until
the full Zen 3 suite can run without host SIGKILLs. CV is the
coefficient-of-variation across runs; ≤ 5% means stable.

| Hardware | Threads | Throughput (M ops/s) | CV | Read p50 / p99 | Flush rate |
|---|---|---|---|---|---|
| Skylake T460 (i5-6300U, 2c/4t) | 2 | **8.96** | 1.8% | 159 / 656 ns | 0.40 M rec/s |
| Beelink Zen 3 (5800H, 8c/16t) | 4 | **49.40** | 1.6% | 50 / 260 ns | 0.80 M rec/s |

> **Note on latency reporting**: earlier Beelink runs used `hpet`,
> which quantised `Instant::now()` to ~1.4 µs per call. The benchmark
> catalogue now records the Zen 3 machine as `tsc` after the 2026-05-11
> re-check, so new Zen 3 latency runs should report honest ns-class
> timings. Throughput numbers are wall-clock based on both machines.

## Headline Performance

| Operation | Metric | Hardware |
|---|---:|---|
| `DenseU64Lane::load` | 2.1-2.2 G ops/s (atomic load floor) | Zen 3 |
| `LockFreeCache` read (T=8) | ~315 M ops/s | Zen 3 |
| `mfs_realistic` mixed workload (T=8) | ~60 M ops/s, read p50 ~60 ns | Zen 3 |
| `mfs_s3fifo` vs quick_cache mixed (T=8, 80/20) | mfs leads quick_cache | Skylake T460 |
| `DenseKvMap` existing-key update | ~17 ns, ~6.5× faster than papaya | Skylake |
| `ConcurrentMap` read | ~4 ns, ~249 M ops/s | Skylake |
| Custom-harness local DB comparison | MfS 4.67 M vs SQLite 0.36 M reads/s | Skylake |

## What's in the box

Three cache types, dense keyed maps, and one numeric lane, sharing a single
[`FlushBackend`](crates/mfs-core/src/lib.rs) trait so you can plug in any
write-behind target. Plus a reference Write-Ahead Log for crash recovery.

### `MemoryFirstStore` — the safe default

Sharded `parking_lot::RwLock<hashbrown::HashTable<(K, Slot<V>)>>`. Each
slot carries a per-key version counter, a sampled `last_touch` tick, and
a dirty flag. `flush_idle` drains records that have been idle for at
least N ticks AND have no live `Arc` references outside the store, then
calls your backend.

Choose this when you want straightforward semantics and modest concurrency.

### `LockFreeCache` — pure speed, no metadata

Wraps our in-house [`ConcurrentMap`](crates/mfs-core/src/concurrent_map.rs),
a lock-free open-addressed hash table with `seize` hyaline reclamation
and 7-bit h2 metadata tags for fast probe skipping. Reads pin an
epoch, do an acquire load, and return `&V` bound to the guard — no
`RwLock` CAS, no `Arc::clone`, no global atomic on the read path.

Choose this when reads dominate and you don't need dirty tracking,
versions, or write-behind.

### `WriteBehindCache` — the lock-free path with write-behind

Read path identical to `LockFreeCache`. Mutations additionally enqueue
`(key, version, op)` triples into a per-shard FIFO dirty queue. A
flusher (background thread or any caller) drains queues, version-checks
each entry against the current `ConcurrentMap` state, and emits
`FlushRecord`s for entries whose version still matches. Backend errors
push the entries back to the queue for retry.

Choose this when you want lock-free reads and the original "save when
no longer in use" semantics. **This is the headline store.**

### `DenseU64Lane` — for SNN/GNN-class numeric state

`Box<[AtomicU64]>` with the dirty flag packed into bit 63 of the value.
Stores complete in a single atomic write; reads are `Relaxed` loads at
the L1 latency floor. No locks, no hashing, no reclamation overhead.
Tradeoff: 63-bit values, indexed access only.

### `DenseKvMap` — for hot keyed 8-byte state

Use `mfs_neural::DenseKvMap<K, V>` when the workload is keyed, write-heavy,
and each value implements `DenseValue` (`u64`, `i64`, `f64`, `[u8; 8]`). The
index maps `K -> (slot, generation)` once;
existing-key writes update a pre-allocated `AtomicU64` slot in place. That avoids
the boxed-entry replacement and reclamation tail that hurts hot-key writes.

Choose this over `LockFreeCache` when hot writes matter more than arbitrary value
types. Use `InlineU64Map` for the specialised `u64 -> u64` floor. Use
`DenseWriteBehindMap<K, V>` when you need write-behind for the same 8-byte value
class, but note that write-behind bookkeeping is not the lowest-tail hot-write
path.

### `durability::WalBackend` — crash recovery

Append-only log with length-prefixed records, hardware-accelerated
**CRC32C** integrity, and `File::sync_data()` (`fdatasync`) at
configurable thresholds. `replay()` walks the log, stops cleanly at
truncation/torn writes, and emits a callback per recovered record.

## Choosing between them

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

## Usage

Runnable examples are registered through workspace crates:

MfS is Core-first. Start with `mfs-core`, add `mfs-neural` for hot-path
dense numeric layers, and add `mfs-db` when you need the optional
durable NoSQL layer. Use `mfs-compat` only for compatibility and legacy
adapters, including SQLite-facing pieces.

Use workspace crates in this order:

1. `mfs-core`: cache/store primitives, write-behind, and WAL.
2. `mfs-neural`: dense numeric layers built on core.
3. `mfs-db`: optional durable NoSQL engine built on core.
4. `mfs-compat`: compatibility and legacy adapters.

NoSQL engine examples:

```bash
cargo run -p mfs-db --release --example nosql_raw_kv        # raw bytes, versions, conflicts, delete
cargo run -p mfs-db --release --example nosql_schema_mode   # schema validation plus put/get
cargo run -p mfs-db --release --example nosql_wal_recovery  # raw WAL sync and replay
cargo run -p mfs-db --release --example nosql_checkpoint_recovery  # checkpoint plus WAL suffix replay
```

SQLite remains as a compatibility path:

```bash
cargo run -p mfs-compat --release --example schema_sqlite_flush  # schema flush SQL generation
cargo run -p mfs-compat --release --example mfs_database         # SQLite-backed schema store demo
cargo run -p mfs-compat --release --example sqlite_vfs_page_adapter  # page-store VFS adapter demo
(cd examples/sqlite_vfs && cargo run --release)                  # standalone SQLite VFS crate
```

Other cache and lane examples:

```bash
cargo run -p mfs-core --release --example read_through_cache  # Redis-replacement pattern
cargo run -p mfs-core --release --example wal_recovery        # crash recovery
cargo run -p mfs-core --release --example dense_counters      # atomic counters at L1 latency
```

Start with `nosql_raw_kv` for the raw engine path or
`nosql_schema_mode` for schema-aware documents. Use the SQLite examples
only when checking SQL persistence or the VFS compatibility layer.

### 1. Read-through cache backed by a database

The pattern: app talks to a `UserRepo`, which checks the cache first and
only falls through to the slow DB on miss. Mutations write to the cache
and a background flusher persists them in batches. Full code at
`examples/read_through_cache.rs`. The skeleton:

```rust
use memory_first_store::writeback::WriteBehindCache;
use memory_first_store::{FlushBackend, FlushRecord, Operation};
use std::sync::{Arc, Mutex};

struct UserRecord { /* … your record type … */ }

struct DbBackend { db: Arc<Mutex<MyDb>> }

impl FlushBackend<u64, UserRecord> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, UserRecord>]) -> Result<(), ()> {
        // Idempotent batch upsert / delete against your real database.
        let mut db = self.db.lock().unwrap();
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put)    => db.upsert(r.key, v.as_ref().clone()),
                (None,    Operation::Delete) => db.delete(r.key),
                _ => {}
            }
        }
        Ok(())
    }
}

let cache = Arc::new(WriteBehindCache::<u64, UserRecord>::with_capacity(100_000));

// Read path: cache hit ⇒ fast; miss ⇒ DB load + load_clean.
fn get(cache: &WriteBehindCache<u64, UserRecord>, db: &Mutex<MyDb>, id: u64)
    -> Option<UserRecord>
{
    if let Some(rec) = cache.get(&id) { return Some(rec.as_ref().clone()); }
    let row = db.lock().unwrap().select(id)?;
    cache.load_clean(id, row.clone());     // populated as CLEAN — won't flush back
    Some(row)
}

// Write path: cache.put / cache.delete; the flusher persists later.
cache.put(user_id, UserRecord { /* … */ });
cache.delete(other_id);

// Background flusher (run on its own thread):
loop {
    cache.flush_idle(&mut backend, /*idle_ticks=*/ 32, /*max=*/ 10_000)?;
    std::thread::sleep(std::time::Duration::from_millis(100));
}
```

> **Caveat: read-after-delete.** A `cache.get(&id)` immediately after a
> `cache.delete(&id)` returns `None` (the entry is a tombstone), so a
> naïve read-through falls through to the DB, finds the row still
> there (delete hasn't flushed yet), and `load_clean`s it back —
> silently undoing the delete. In a real app, either flush
> synchronously after a delete, keep an application-level
> "delete-pending" set, or change the read-through to bypass the cache
> for `id`s known to be in flight. The example demonstrates the safe
> wait-for-flush variant.

### 2. WAL-based crash recovery

```rust
use memory_first_store::durability::{WalBackend, WalConfig, U64Codec, replay_into_u64_store};
use memory_first_store::MemoryFirstStore;

// On startup: rebuild the in-memory state from disk.
let store = MemoryFirstStore::<u64, u64>::new();
let recovered = replay_into_u64_store("data.wal", &store)?;
println!("recovered {recovered} records");

// During operation: the WAL is your FlushBackend.
let mut wal = WalBackend::open("data.wal", U64Codec, WalConfig::default())?;
store.flush_idle(&mut wal, /*idle_ticks=*/ 1, /*max=*/ 10_000)?;
wal.sync_now()?;   // fsync — data now survives kill -9 / power loss
```

For non-`u64` value types implement `WalCodec<K, V>` (encode/decode to
bytes); the rest of the WAL machinery (length-prefixed records,
hardware CRC32C, torn-write-tolerant replay) is type-agnostic.

### 3. Dense numeric lane

For per-key atomic counters / SNN / GNN state where each key is a
small contiguous integer:

```rust
use memory_first_store::DenseU64Lane;
use std::sync::Arc;

let lane = Arc::new(DenseU64Lane::with_len(1_000_000));
lane.store(42, 99);              // single atomic write, dirty bit set in MSB
let v = lane.load(42);           // ~0.5 ns on Zen 3 — at the L1 floor
lane.fetch_add(42, 1);           // CAS loop over the packed word

// Periodically scan and persist dirty entries:
for (idx, val) in lane.dirty_values(usize::MAX) {
    persist(idx, val)?;
    lane.mark_clean(idx);
}
```

Tradeoff: values are 63-bit (bit 63 is reserved as the dirty flag).
`DENSE_VALUE_MAX` is exposed as a constant.

## Performance notes

### Pre-size your cache

`ConcurrentMap` is **fixed-capacity** — no live resize. The default
`with_capacity(1_000_000)` is sized for the steady-state working set; if
you need substantially more, pass an explicit capacity at construction.
Historical regressions when the underlying map (then `papaya`) was
under-sized still apply: the diagnostic in `benches/probe.rs` attempts a
large preload into a cache built for 1024 entries and measures the
miss-heavy saturated-table probe cost. Use `LockFreeCache::with_capacity` and
`WriteBehindCache::with_capacity` whenever you have any estimate of the
working set. `ConcurrentMap::rebuild_with_capacity` can build a larger
snapshot for caller-controlled maintenance swaps, but it is not live resize;
writers should be quiesced if you need an exact handoff.

For Redis-like object workloads that need growable key capacity and mutable
list/hash/set/zset operations, use the opt-in `mfs_compat::object_store::
MfsMutableObjectStore` path. It avoids the fixed-capacity `ConcurrentMap` choke
point by using sharded growable maps, while the core lock-free cache lanes stay
fixed-capacity for predictable hot-path probes. Papaya-style incremental live
resizing for `ConcurrentMap` itself remains a separate design problem.

### Hold a pin guard

`cache.pin()` returns a `Pinned<'_, ...>` guard that owns a
`seize::LocalGuard`. Constructing the guard is the dominant per-call
cost; in tight read loops, hold one guard and run many reads against
it, then drop. The convenience `cache.get(&key)` exists but pins
internally on every call.

### Use `read_with` over `get` when possible

`get` returns `Arc<V>` and pays an `Arc::clone` (atomic refcount RMW).
`read_with(&key, |&V| -> R)` runs your closure inside the guard's
lifetime and skips the clone. On Skylake the difference is 36 ns vs
148 ns; on Zen 3 it's 18 ns vs 25 ns.

### Don't `get_batch` on small working sets

`get_batch` pre-hashes all keys and prefetches shard headers. On
Zen 3 with 1 M u64 keys it's slower than scalar `get` because the
working set fits in L3 and the prefetch overhead dominates. Useful for
DRAM-bound random access on bigger working sets.

## Durability

`durability::WalBackend` is a reference WAL implementation:

- Append-only file with `BufWriter`, length-prefixed records, hardware
  CRC32C (SSE4.2 / NEON CRC32 instructions, multi-GB/s).
- Configurable sync thresholds: `sync_data()` after N bytes or N
  records, with explicit `sync_now()` for synchronous durability.
- `replay()` walks the log on startup, stops cleanly at the first
  invalid record (truncation, torn write, checksum mismatch).
- `U64Codec` for `(u64, u64)` works out of the box; arbitrary types
  implement `WalCodec`.

```rust
use memory_first_store::durability::{WalBackend, WalConfig, U64Codec, replay_into_u64_store};
use memory_first_store::MemoryFirstStore;

// Open / create the WAL.
let mut wal = WalBackend::open("data.wal", U64Codec, WalConfig::default())?;

// Recovery on startup.
let store = MemoryFirstStore::<u64, u64>::new();
let recovered = replay_into_u64_store("data.wal", &store)?;
println!("recovered {recovered} records");

// During operation, flush dirty data through the WAL.
store.flush_idle(&mut wal, 64, 10_000)?;
wal.sync_now()?;  // fsync
```

For production: pair with periodic snapshots (e.g. `fork()` BGSAVE-style)
to bound recovery time. See
`crates/mfs-core/src/durability.rs` for what this module is
**not** (no segment rotation, no group commit across threads, no NVRAM
support — those are layer-on-top concerns).

## Known Limitations

See [BENCHMARKS.md](./BENCHMARKS.md#known-limitations) for the full list. Key points:

- **S3-FIFO admission filter**: Off by default. Four experiment variants plus TwoCounterDecay are opt-in via `S3FifoAdmissionExperiment`. The default ghost25 configuration provides the best hit-ratio-vs-speed tradeoff.
- **Fixed capacity**: `ConcurrentMap` and its dependents are fixed-capacity. Pre-size for your working set. Use `rebuild_with_capacity` for maintenance-window growth. `MfsMutableObjectStore` (opt-in, `experimental` feature) provides growable sharded maps.
- **No automatic disk tiering**: Core cache lanes are memory-first. `MfsMutableObjectStore` supports manifest-based cold generations with explicit read-through promotion; foyer-style automatic hybrid tiering is not yet supported.

## Experimental Types

Types behind the `experimental` feature flag are unstable and may change or be removed without a major version bump:

```toml
[dependencies]
mfs-core = { version = "0.2", features = ["experimental"] }
mfs-neural = { version = "0.2", features = ["experimental"] }
```

This gates `AtomicWriteBehindCache`, `SlotWriteBehindCache` (mfs-core) and `ConcurrentDenseWriteBehindMap` (mfs-neural). These types were benchmarked but did not beat the default boxed `WriteBehindCache`/`ConcurrentMap` backends in the full matrix.

## Building, testing, benchmarking

```bash
make ci                       # fmt-check, clippy, and workspace tests
make test                     # cargo test --workspace --all-features
make bench                    # registered custom-harness benches
make bench-hot                # raw read/write hot-path microbenches (min/median/max over 7 trials)
make bench-nosql-engine       # NoSQL engine lane harness
make bench-schema-store       # schema CRUD/index/include/WAL/SQL flush bench
make bench-realistic          # mixed workload (Redis-replacement profile), single run
make bench-realistic-stable   # same with MFS_RUNS=10 — prints distribution + CV
make bench-criterion          # criterion-driven microbenches with confidence intervals
make bench-competitors        # criterion head-to-head vs Rust competitors
make bench-criterion-report   # open the criterion HTML report in target/criterion/
make bench-probe              # capacity-fragmentation diagnostic
make doc                      # cargo doc --open
```

### Reading the benchmark output

There are two harness flavors, used for different things:

**Custom harness (`hot_path`, `schema_store`, `realistic`, `probe`)** — fast, targeted,
prints plain text. Each test runs a fixed number of trials and reports
`min / median / max` directly. The realistic bench additionally
supports `MFS_RUNS=N` to print a cross-run distribution
(`min / median / p95 / max / stddev / cv%`). Good for quick checks
during development. CV (coefficient of variation = stddev / mean as %)
under 5% means the numbers are stable; over 15% means thermals or
scheduler noise dominates.

**Criterion harness (`criterion_bench`)** — adaptive iteration counts,
proper statistical reporting (mean, median, MAD, regression detection),
HTML report under `target/criterion/`. Each `c.bench_function` line
prints `time: [low mid high]` representing the 99% confidence interval
plus throughput in `Melem/s` (M elements per second). Slower to run
(every group takes ~30 s minimum) but the right tool for tracking
performance regressions over time.

### Realistic-bench knobs

```bash
MFS_DURATION_SECS=10 MFS_RUNS=10 MFS_THREADS=8 MFS_KEYS=500000 \
  MFS_READ_PCT=99 MFS_WRITE_PCT=1 \
  make bench-realistic
```

Full list of environment overrides in `make help` and inside
`benches/realistic.rs`.

## Status

Experimental. APIs may change. The architecture is settled; the
performance numbers are reproducible. Tests pass on Linux x86_64 (Zen 3
and Skylake confirmed) and aarch64 (M-series should work, untested in
this iteration).

## License

MIT or Apache-2.0, at your option.
