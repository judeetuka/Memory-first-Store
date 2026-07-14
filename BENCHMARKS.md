# Benchmarks

Memory-first-store (MfS) is a set of high-throughput, in-process Rust storage
primitives for hot data: lock-free concurrent caches with optional write-behind
to a durable backend, dense numeric lanes at L1 cache latency, and an embedded
alternative to Redis for cases where the cache lives in the same process as the
application. This file is the benchmark catalogue. Each competitor in
`benches/<competitor>/` is exercised on workloads suited to that competitor's
design. Charts are HDR-histogram CDFs (log-x latency axis, linear-y cumulative
probability) rendered by `plotters` straight into each bench folder so they
preview in any IDE.

*Benchmarks should be taken with a grain of salt. Always measure for your workload.*

Hardware: Intel Core i5-6300U Skylake (Lenovo T460, 2c/4t, `tsc`
clocksource, governor=`powersave`) unless otherwise noted. The Beelink
Zen 3 (5800H, 8c/16t) numbers are appended once captured.

Thread-count interpretation on the T460: 1 thread shows the single-lane
latency floor, 2 threads maps to the two physical cores, and 4 threads
maps to full SMT occupancy. Any 8-thread row on this machine is an
oversubscription/scheduler-stress result, not the main steady-state score
for a 2c/4t laptop.

Generate everything with:

```bash
cargo bench --bench papaya_latency
cargo bench --bench papaya_single_thread
cargo bench --bench dashmap_contention
cargo bench --bench moka_zipfian
cargo bench --bench scc_hash_map
cargo bench --bench scc_hash_index
cargo bench --bench scc_hash_cache
cargo bench --bench scc_tree_index
cargo bench --bench quick_cache_benchmarks
cargo bench --bench tinyufo_bench_perf
cargo bench --bench tinyufo_bench_hit_ratio
cargo bench --bench foyer_bench_hit_ratio
cargo bench --bench foyer_bench_dynamic_dispatch
cargo bench --bench mfs_hot_path
cargo bench --bench mfs_wal_async
cargo bench --bench mfs_slot_writeback
cargo bench --bench mfs_queued_write
cargo bench --bench mfs_schema_store
cargo bench -p mfs-compat --bench local_db_sqlite_kv
cargo bench --bench mfs_realistic
cargo bench --bench mfs_criterion
cargo bench --bench mfs_probe
```

## Known Limitations

- **Admission-heavy eviction hit ratio**: MfS's S3-FIFO policy cache trails TinyUFO/moka/quick_cache by 0.1-2.5pp on Zipfian hit ratio at low capacity ratios. TwoCounterDecay admission experiment implemented but does not close the gap. Use those libraries when hit ratio matters most.
- **Core map growth**: `ConcurrentMap` is fixed-capacity; pre-size for your working set. Use `rebuild_with_capacity` for maintenance-window resizing. `MfsMutableObjectStore` (opt-in, mfs-compat) provides growable sharded maps for Redis-like object workloads.
- **Automatic hybrid memory+disk**: The opt-in `MfsMutableObjectStore` path supports manifest-based cold generations, compacting GC, and policy-driven auto-demotion, but bare `get` calls are memory-only. Foyer-style automatic tiering requires explicit persistence wrapper calls.
- **Single-operation new-key insert cost**: `scc::HashMap`'s single-insert median (~93-118ns) is faster than MfS's boxed path (~244-456ns). Existing-key updates via `DenseKvMap`/`InlineU64Map`/`BucketedIndex` close this to 93-182ns and are the recommended hot-write primitives.
- **Experimental types**: `AtomicWriteBehindCache`, `SlotWriteBehindCache`, and `ConcurrentDenseWriteBehindMap` are gated behind the `experimental` feature flag and are not recommended for production use.

## Headline Performance

| Operation | Metric | Value | Hardware |
|---|---:|---:|---|
| `DenseU64Lane::load` | ~0.5 ns | 2.1-2.2 G ops/s | Zen 3 |
| `LockFreeCache` read T=8 | ~315 M ops/s | ~315 M ops/s | Zen 3 |
| `mfs_realistic` mixed T=8 | p50 ~60 ns | ~60 M ops/s | Zen 3 |
| Custom-harness Local DB | Read 4.67 M vs SQLite 0.36 M | — | Skylake |
| `DenseKvMap` existing-key update | ~17 ns (6.5x faster than papaya) | ~57 M ops/s | Skylake |
| `ConcurrentMap` read | ~4 ns | ~249 M ops/s | Skylake |
| S3FIFO cache 8-thread mixed (tinyufo harness) | 7.64 M ops/s (leads quick_cache) | Skylake T460 |
| ghost25 hit ratio | 72.5-98.8% (3 Zipf workloads) | — | — |

---

## papaya — concurrent insert tail latency

Papaya's home turf, verbatim from the upstream `latency.rs`: 8-thread
concurrent insert of 1 M items per thread into a growing map, recorded
with HDR histograms. Contestants are papaya (incremental resize, the
papaya default), papaya (blocking resize), and dashmap.

| papaya (incremental) | papaya (blocking) | dashmap |
|:---:|:---:|:---:|
| ![](benches/papaya/charts/papaya-cdf.png) | ![](benches/papaya/charts/papaya-blocking-cdf.png) | ![](benches/papaya/charts/dashmap-cdf.png) |

---

## dashmap — 8-thread sharded-RwLock contention

Dashmap's strength is moderate-to-high contention with sharded
`RwLock`. Hand-rolled (dashmap has no upstream bench suite; the
canonical comparison lives in the external `conc-map-bench` harness).
8 threads × 200 k ops, 50/50 read/write on 64 hot keys.

![](benches/dashmap/charts/contention-cdf.png)

---

## moka — TinyLFU hit ratio under Zipfian access

Moka's TinyLFU admission policy is engineered for high hit ratios
when working set > capacity. Hand-rolled (moka has no upstream bench
suite). Zipfian(α=1.1) access pattern over 100 k working set into a
10 k-slot cache, 1 M ops.

![](benches/moka/charts/zipfian-cdf.png)

---

## scc — upstream bench suite (criterion HTML reports)

scc ships its own benchmark suite. After running, criterion writes a
report tree to `target/criterion/`. Open `target/criterion/report/index.html`
in a browser for interactive comparison.

| Bench | Coverage |
|---|---|
| `scc_hash_map` | `scc::HashMap` insert/read/upsert/remove |
| `scc_hash_index` | `scc::HashIndex` (read-optimised) |
| `scc_hash_cache` | `scc::HashCache` (capacity-bounded) |
| `scc_tree_index` | `scc::TreeIndex` (ordered) |

---

## quick_cache — upstream bench suite

S3-FIFO eviction. Run `cargo bench --bench quick_cache_benchmarks` and
inspect `target/criterion/`.

---

## tinyufo — upstream bench suite

S3-FIFO + TinyLFU admission. Two registered benches:

- `tinyufo_bench_perf` — single-thread and multi-thread throughput.
- `tinyufo_bench_hit_ratio` — hit ratio vs LRU under skewed access.

`tinyufo_bench_memory` is present in `benches/tinyufo/` but unregistered
because it requires the `dhat` heap profiler.

---

## foyer — upstream bench suite

foyer's hybrid memory + disk cache. We only register the memory-tier
benches:

- `foyer_bench_hit_ratio` — LFU/TinyLFU / S3-FIFO / LRU / FIFO comparison.
- `foyer_bench_dynamic_dispatch` — overhead of dynamic eviction policy
  dispatch.

The full hybrid (memory + disk) bench lives in foyer's own
`foyer-bench` binary and is out of scope for this repo.

---

## MfS — internal benches

Workloads designed for MfS's own access shapes:

- `mfs_hot_path` — raw read/write microbenches per primitive
  (`ConcurrentMap`, `LockFreeCache`, `WriteBehindCache`,
  `DenseU64Lane`, `DenseKvMap`, `InlineU64Map`).
- `mfs_realistic` — multi-threaded UserProfile-sized workload with the
  background auto-flusher.
- `mfs_schema_store` — schema-layer CRUD, indexed lookup, relationship
  include, WAL replay, SQL flush planning/execution, and thread scaling.
- `mfs_probe` — capacity-fragmentation diagnostic.
- `mfs_criterion` — criterion-based microbenches with confidence
  intervals.

Criterion outputs land in `target/criterion/`. Custom-harness outputs
go to stdout (see `MFS_RUNS`, `MFS_THREADS`, `MFS_READ_PCT` etc. env
overrides documented in `Makefile`).

---

## Discussion

MfS is optimised for read-heavy and moderate-write workloads on the
in-memory hot path. The headline numbers MfS competes for:

- **Read floor** — `ConcurrentMap`/`LockFreeCache` sits at or below
  raw `papaya::HashMap::get` on this hardware for hot single-key reads.
- **Update existing key** — `DenseKvMap::put` and `InlineU64Map::update`
  use inline storage with no per-call `Box<Entry>` allocation, which
  is the per-write overhead papaya inherits from its design. MfS wins
  this lane by ~5-15× depending on hardware.
- **Mixed 95/4/1 with deletes** — competitive with `dashmap` once the
  inline path is used.
- **Mixed 50/45/5** — `dashmap` still leads raw-map throughput because
  it tracks no dirty state; MfS's writeback variants pay ~17 ns/write
  for dirty bookkeeping in exchange for the write-behind contract.

---

## T460 results, 2026-05-14 run

All numbers below are from a fresh run on this Arch box (i5-6300U,
2c/4t, `tsc`, governor=`powersave`). Brief cooldown between
competitors.

### HDR-based benches (PNGs in `benches/<competitor>/charts/`)

| Bench | Contestant | p50 | p99 | p99.9 | max |
|---|---|---:|---:|---:|---:|
| dashmap_contention (8t × 200k, 50% reads, 64 hot keys) | dashmap | 189 ns | 654 ns | 1.27 µs | 45.8 ms |
| dashmap_contention | mfs_lockfree_shadow | **155 ns** | 1.11 µs | 82.8 µs | 28.6 ms |
| moka_zipfian (α=1.1, ws=100k, cap=10k, 1M ops) | moka_sync | 354 ns | 18.6 µs | 61.0 µs | 268 µs |
| moka_zipfian | mfs_lockfree_shadow (uncapped) | **89 ns** | 673 ns | 1.97 µs | 72.8 µs |
| papaya_latency (papaya incremental, 8t × 1M concurrent insert) | per-thread p99 | — | up to 69 ms | — | — |
| papaya_latency (papaya blocking) | per-thread p99 | — | up to 386 ms | — | — |
| papaya_latency (dashmap) | per-thread p99 | — | up to 23 ms | — | — |

Note on the moka comparison: MfS `LockFreeCache` is uncapped, so its
hit ratio (93.5%) is unfairly higher than moka's (86.9%) which is
constrained to 10 k entries. The relevant comparison is latency — MfS
is ~4× faster per op because it does no eviction work.

### Criterion microbenches (full reports in `target/criterion/`)

| Group | Best | Notes |
|---|---|---|
| `papaya_single_thread` read 10k loop | std=368 µs, dashmap=562 µs, papaya=673 µs | std HashMap wins single-thread reads |
| `scc_hash_map` reads | scc::read_sync ≈ 195 ns | + many async variants |
| `scc_hash_index` peek | 142 ns | read-optimised variant |
| `scc_hash_cache` get | 146 ns | capacity-bounded |
| `scc_tree_index` insert | 148-159 ns | ordered |
| `tinyufo_bench_perf` single-thread reads | lru=131 ns, quick_cache=132 ns, tinyufo=177 ns, moka=318 ns | quick_cache ties LRU |
| `foyer_bench_dynamic_dispatch` | static=dyn box=dyn arc ≈ 22 ns | dyn dispatch overhead is negligible |
| `mfs_criterion` dense_load_scalar | **850 ps** | sub-nanosecond, hardware floor |
| `mfs_criterion` mfs_get hot | 33.9 ns | post-optimisation baseline |

### MfS internal hot-path bench (mfs_hot_path, T460)

| Path | min (ns/op) | peak ops/s |
|---|---:|---:|
| dense_load_scalar | 1.07 | 937 M |
| dense_load_many_seq | 2.69 | 372 M |
| dense_load_many_stride17 | 9.37 | 107 M |
| lockfree_get | 27.07 | 36.9 M |
| writeback_read_with_pinned | 36.51 | 27.4 M |
| writeback_get_pinned | 151.47 | 6.6 M |
| writeback_get_oneshot | 163.93 | 6.1 M |
| generic_read_with | 106.93 | 9.4 M |
| generic_get | 208.74 | 4.8 M |
| generic_get_batch | 211.57 | 4.7 M |

Multi-thread read scaling (mfs_hot_path):

| Threads | mfs_get | mfs_read_with | lockfree_get | writeback_read_with |
|---:|---:|---:|---:|---:|
| 1 | 210.69 ns / 4.7 M | 111.99 ns / 8.9 M | 28.36 ns / 35.3 M | 36.73 ns / 27.2 M |
| 2 | 132.55 ns / 7.5 M | 86.00 ns / 11.6 M | 12.27 ns / **81.5 M** | 17.63 ns / 56.7 M |
| 4 | 104.02 ns / 9.6 M | 68.83 ns / 14.5 M | 14.14 ns / 70.7 M | 17.94 ns / 55.8 M |
| 8 | 93.72 ns / 10.7 M | 71.48 ns / 14.0 M | 17.26 ns / 57.9 M | 18.29 ns / 54.7 M |

### MfS realistic workload (mfs_realistic, T460, 2 threads)

5 s × 2 threads, 100 k keys, ~128 B values, 95/4/1 read/write/delete,
80% hot, flush every 10 ms, 1/64 latency sampling:

| Metric | Value |
|---|---:|
| Aggregate throughput | **7.92 M ops/sec** |
| Read p50 / p99 / p99.9 / max | 167 ns / 730 ns / 1.48 µs / 95.4 µs |
| Write p50 / p99 | 1.05 µs / 14.5 µs |
| Delete p50 | 388 ns |
| Flush rate | 0.36 M rec/s |
| Cache state at end | 80,318 entries, 0 dirty |

### MfS probe diagnostic (mfs_probe)

Confirms the under-sized capacity foot-gun penalty. The under-sized
variant attempts a larger preload than the fixed-capacity table can hold,
then measures the miss-heavy saturated-table probe cost:

| Configuration | min ns/op |
|---|---:|
| papaya<u64,u64> Fx insert-populated | 38.79 |
| papaya<u64,u64> Fx compute-populated | 36.75 |
| papaya<u64,Probe> Fx compute-populated | 31.67 |
| WriteBehindCache (under-sized to 1024, attempted 100 k preload) | 146.56 |
| WriteBehindCache (pre-sized to working set) | 146.21 |
| WriteBehindCache pre-sized `read_with` | 41.38 |

Lesson: under-sizing the cache vs pre-sizing barely changes raw `get`
cost here (146 ns either way) but `read_with` on the pre-sized variant
is 3.5× faster — pre-size whenever you can estimate working set.

---

## Appendix A — Beelink Zen 3 results, 2026-05-14 run

Same 17 binaries, copied from this Skylake box (binaries are generic
x86_64, no `target-cpu=native`). Beelink runs Ubuntu 24.04, AMD Ryzen
7 5800H (8 cores / 16 threads), governor=`powersave`. Sequential
execution with cooldowns between competitors, matching the T460 run.

### HDR-based benches (PNGs in `benches/<competitor>/charts-beelink/`)

| dashmap-contention (Beelink) | moka-zipfian (Beelink) |
|:---:|:---:|
| ![](benches/dashmap/charts-beelink/contention-cdf.png) | ![](benches/moka/charts-beelink/zipfian-cdf.png) |

| papaya (Beelink) | papaya-blocking (Beelink) | dashmap (Beelink) |
|:---:|:---:|:---:|
| ![](benches/papaya/charts-beelink/papaya-cdf.png) | ![](benches/papaya/charts-beelink/papaya-blocking-cdf.png) | ![](benches/papaya/charts-beelink/dashmap-cdf.png) |

### Key Beelink vs T460 numbers

| Bench | Metric | T460 | Beelink | Ratio |
|---|---|---:|---:|---:|
| `dashmap_contention` (8 threads) | dashmap p50 / p99 | 189 ns / 654 ns | 101 ns / 260 ns | **1.9× / 2.5×** |
| `dashmap_contention` | mfs_lockfree p50 / p99 | 155 ns / 1.11 µs | 80 ns / 882 ns | 1.9× / 1.3× |
| `moka_zipfian` | moka p50 | 354 ns | 100 ns | **3.5×** |
| `moka_zipfian` | mfs_lockfree (uncapped) p50 | 89 ns | 30 ns | **3.0×** |
| `mfs_hot_path` | dense_load_scalar | 1.07 ns | 0.46 ns | 2.3× |
| `mfs_hot_path` | lockfree_get T=8 aggregate | 57.9 M ops/s | **315.9 M ops/s** | **5.5×** |
| `mfs_hot_path` | writeback_read_with T=8 aggregate | 54.7 M ops/s | 232 M ops/s | 4.2× |
| `mfs_realistic` (T=8 on Beelink, T=2 on T460) | aggregate ops/sec | 7.92 M | **60.16 M** | **7.6×** |
| `mfs_realistic` | read p50 / p99 / p99.9 | 167 / 730 / 1480 ns | 60 / 270 / 702 ns | 2.8× / 2.7× / 2.1× |
| `mfs_criterion` | mfs_get hot | 33.9 ns | 7.96 ns | **4.3×** |
| `papaya_single_thread` (10k loop) | papaya read | 673 µs | 133 µs | 5.1× |
| `papaya_single_thread` | std read | 368 µs | 80 µs | 4.6× |
| `papaya_single_thread` | dashmap read | 562 µs | 106 µs | 5.3× |
| `scc_hash_map` | insert_sync | 60.7 ns | 23.9 ns | 2.5× |
| `scc_hash_index` peek | 142 ns | 59.4 ns | 2.4× |
| `tinyufo perf` single-thread reads | lru / quick_cache / tinyufo / moka | 131 / 132 / 177 / 318 ns | 62 / 65 / 81 / 137 ns | ~2× across the board |

### Observations

- **Multi-thread scaling is where Zen 3 dominates.** Single-thread
  speedup is ~2-3×; 8-thread aggregate throughput is 5-7× because
  Beelink has 4× the logical cores (16 vs 4).
- **`mfs_realistic` at T=8 hits 60 M ops/sec** on Beelink — 7.6× the
  T460 number. Read p50 of 60 ns under load is the headline number.
- **`papaya` incremental resize tail latency is much better on Beelink**
  (per-thread p99 down from 43-69 ms to 0-10 ms). The blocking-resize
  mode still has bad spikes (64-160 ms) even with more cores.
- **`dashmap` p99 under contention got worse on more cores** (T460
  20-23 ms → Beelink 64-104 ms). Sharded `RwLock` doesn't scale
  uniformly past a certain core count for this hot-key workload.
- **`dense_load_scalar` peaks at 2.16 G ops/s on Beelink** — that's
  `AtomicU64::load` at hardware speed, no hashing.

### Beelink-specific tinyufo single-thread reads

| Cache | T460 ns/op (ops/s) | Beelink ns/op (ops/s) |
|---|---:|---:|
| lru | 131 (7.6 M) | **62 (15.9 M)** |
| quick_cache | 132 (7.6 M) | **65 (15.3 M)** |
| tinyufo | 177 (5.6 M) | 81 (12.3 M) |
| tinyufo compact | 205 (4.9 M) | 95 (10.5 M) |
| moka | 318 (3.1 M) | 137 (7.3 M) |

---

## Local refresh, 2026-05-28

Full refresh command ran every registered bench from `Cargo.toml` and wrote
logs to `target/bench-refresh-20260528-152110/`. All benches completed except
`quick_cache_benchmarks`, which timed out after 20 minutes but produced partial
Criterion output through the large Zipfian cases.

### MfS hot paths (`mfs_hot_path`)

| Path | min ns/op | median ns/op | peak ops/s |
|---|---:|---:|---:|
| generic_get | 314.85 | 375.56 | 3.18 M |
| generic_read_with | 193.66 | 227.19 | 5.16 M |
| lockfree_get | **47.51** | **58.59** | **21.05 M** |
| lockfree_insert | 441.23 | 455.88 | 2.27 M |
| lockfree_replace | **324.18** | **365.24** | **3.08 M** |
| bucketed_index_insert | **244.40** | **252.55** | **4.09 M** |
| bucketed_index_replace | **93.59** | **111.10** | **10.68 M** |
| writeback_get_pinned | 186.24 | 188.04 | 5.37 M |
| writeback_read_with_pinned | 68.48 | 90.79 | 14.60 M |
| writeback_get_ref_pinned | **64.64** | **74.73** | **15.47 M** |
| writeback_get_oneshot | 192.50 | 209.76 | 5.19 M |
| dense_writeback_get | **45.07** | **54.01** | **22.19 M** |
| dense_writeback_put | **156.47** | **160.29** | **6.39 M** |
| dense_writeback_put_dirty | **145.38** | **157.77** | **6.88 M** |
| dense_writeback_map_get (`u64 -> [u8; 8]`) | **92.72** | **128.19** | **10.78 M** |
| dense_writeback_map_put (`u64 -> [u8; 8]`) | **199.68** | **235.38** | **5.01 M** |
| dense_load_scalar | **2.71** | 4.09 | 369 M |
| dense_load_many_seq | 7.00 | 7.64 | 143 M |
| dense_load_many_stride17 | 12.08 | 13.64 | 82.8 M |

Multi-thread read scaling (`mfs_hot_path`):

| Threads | mfs_get | mfs_read_with | lockfree_get | writeback_read_with |
|---:|---:|---:|---:|---:|
| 1 | 287.13 ns / 3.48 M | 170.76 ns / 5.86 M | 41.58 ns / 24.1 M | 54.76 ns / 18.3 M |
| 2 | 167.75 ns / 5.96 M | 124.78 ns / 8.01 M | 27.35 ns / 36.6 M | 32.17 ns / 31.1 M |
| 4 | 186.91 ns / 5.35 M | 109.84 ns / 9.10 M | 28.87 ns / 34.6 M | 32.52 ns / 30.8 M |
| 8 | 155.78 ns / 6.42 M | 118.64 ns / 8.43 M | **25.74 ns / 38.8 M** | 38.98 ns / 25.7 M |

### Direct foyer-memory comparison (`foyer_memory_vs_mfs`)

8 threads, 64 hot keys, 50/50 read/write, 50k ops/thread. Like the dashmap
row below, this oversubscribes the T460 and is best treated as scheduler-stress
data:

| Contestant | p50 | p99 | p99.9 | max | Throughput |
|---|---:|---:|---:|---:|---:|
| foyer_memory_fifo | 752 ns | 1.95 µs | 75.0 µs | 6.80 ms | 3.26 Mops/s |
| **mfs_lockfree** | **150 ns** | **1.15 µs** | **10.8 µs** | 58.4 ms | **6.22 Mops/s** |

MfS wins every measured percentile and throughput against foyer's cheapest
memory-tier FIFO path in this workload. The max outlier remains scheduler/noise
sensitive and should not be used as a headline.

### Dashmap hot-key contention (`dashmap_contention`)

8 threads, 100k ops/thread. The expanded harness separates read-only,
write-only, and 50/50 mixed lanes across single-key, 64-key, and 1024-key hot
sets. It also separates MfS one-shot pinning from the realistic thread-pinned
loop.

| Scenario | Contestant | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|
| single key read | dashmap | 127 ns | 1.10 µs | 6.82 µs |
| single key read | mfs_lockfree_thread_pin | **54 ns** | **90 ns** | 404 ns |
| single key read | mfs_inline_u64 | 61 ns | 96 ns | **387 ns** |
| single key write | dashmap | 126 ns | 1.16 µs | 70.1 µs |
| single key write | mfs_lockfree_thread_pin | 225 ns | 5.03 µs | 21.2 µs |
| single key write | mfs_inline_u64 | **106 ns** | 1.67 µs | 4.82 µs |
| single key write | mfs_partitioned_thread_pin | 261 ns | **908 ns** | **4.50 µs** |
| single key mixed | dashmap | 137 ns | 1.70 µs | 81.7 µs |
| single key mixed | mfs_lockfree_thread_pin | 166 ns | 668 ns | 6.67 µs |
| single key mixed | mfs_inline_u64 | **109 ns** | 880 ns | **1.63 µs** |
| single key mixed | mfs_partitioned_thread_pin | 171 ns | **631 ns** | 1.69 µs |
| 64-key read | dashmap | 123 ns | 452 ns | 869 ns |
| 64-key read | mfs_lockfree_thread_pin | **52 ns** | **121 ns** | **412 ns** |
| 64-key read | mfs_inline_u64 | 61 ns | 238 ns | 520 ns |
| 64-key write | dashmap | 129 ns | 569 ns | 1.31 µs |
| 64-key write | mfs_lockfree_thread_pin | 168 ns | 564 ns | 8.54 µs |
| 64-key write | mfs_inline_u64 | **80 ns** | **348 ns** | **666 ns** |
| 64-key mixed | dashmap | 166 ns | 551 ns | 1.18 µs |
| 64-key mixed | mfs_lockfree_thread_pin | 147 ns | 443 ns | 1.18 µs |
| 64-key mixed | mfs_inline_u64 | **104 ns** | **328 ns** | **663 ns** |
| 1024-key mixed | dashmap | 152 ns | 548 ns | 1.22 µs |
| 1024-key mixed | mfs_lockfree_thread_pin | 155 ns | 579 ns | 5.35 µs |
| 1024-key mixed | mfs_inline_u64 | **93 ns** | **358 ns** | **829 ns** |

Interpretation: DashMap is not generally faster. MfS thread-pinned reads are much
faster and have tighter read tails. The important hot-write result is
`InlineU64Map`: once MfS uses inline stable storage with per-slot seqlock writes,
it beats DashMap on 64-key write and 64/1024-key mixed rows. A naive striped mutex
around `LockFreeCache` does not solve the tail because the underlying operation
still performs boxed replacement and reclamation. The useful lesson is that MfS can
win hot writes, but the winning shape is stable-entry/in-place mutation, not
locking around the boxed `ConcurrentMap` replace path.

### Zipfian latency and hit ratio (`moka_zipfian`)

Zipfian(α=1.1), 100k working set, moka capped at 10k, MfS uncapped:

| Contestant | hit ratio | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| moka_sync | 86.889% | 475 ns | 27.1 µs | 89.0 µs | 3.65 ms |
| mfs_lockfree_shadow (uncapped) | **93.544%** | **109 ns** | **924 ns** | **2.40 µs** | 3.06 ms |
| mfs_s3fifo (cap=10k) | 86.711% | **127 ns** | **1.92 µs** | **5.84 µs** | 3.14 ms |

Latency takeaway remains strong for MfS, but the hit-ratio comparison is still
not apples-to-apples for `mfs_lockfree_shadow` because that path is uncapped.
The `mfs_s3fifo` row is capacity-bounded and already nearly matches moka's hit
ratio while being far faster on latency. A follow-up 2026-05-29 policy refresh
added `mfs_s3fifo` to the TinyUFO throughput harness and the foyer hit-ratio
matrix so the bounded-cache story is now apples-to-apples.

### Policy cache hit-ratio edge (`tinyufo_bench_hit_ratio`, `foyer_bench_hit_ratio`)

Representative rows:

| Workload | LRU | moka | quick_cache | TinyUFO |
|---|---:|---:|---:|---:|
| Zipf 1.00, cap 5% | 58.65% | 66.86% | 67.71% | **68.78%** |
| Zipf 1.10, cap 10% | 78.04% | 82.97% | 83.12% | **83.99%** |
| Zipf 1.50, cap 25% | 98.81% | 99.09% | 99.08% | **99.10%** |

Foyer-memory's policy matrix shows the same pattern: S3FIFO/LFU/moka trade the
lead depending on skew and capacity. Representative foyer rows:

| Workload | foyer FIFO | foyer LRU | foyer LFU | foyer S3FIFO | moka | mfs_s3fifo | mfs_s3fifo_1s |
|---|---:|---:|---:|---:|---:|---:|---:|
| Zipf 1.00, cap 5% | 53.95% | 58.25% | 66.70% | **67.85%** | 66.84% | 66.00% | 66.38% |
| Zipf 1.10, cap 10% | 74.16% | 77.70% | 82.50% | **83.23%** | 82.95% | 82.11% | 82.19% |
| Zipf 1.50, cap 25% | 98.34% | 98.77% | 99.03% | 99.01% | **99.08%** | 98.95% | 98.99% |

Remaining competitor edge: MfS now has a real bounded policy cache, but it still
trails the best admission-heavy policies by roughly 0.1-2.5 percentage points in
representative foyer rows. `S3FifoConfig::with_shards(1)` improves hit ratio a
little by removing per-shard policy partitioning, but the remaining policy work
is TinyLFU-style admission quality, not raw speed.

A 2026-06-23 MfS-only tuning sweep keeps production defaults unchanged and checks
current public knobs. Command:
`cargo bench --all-features -p mfs-core --bench mfs_s3fifo_tuning`.
Representative rows:

| Workload | default | single shard | threshold2 | hot95 | ghost25 | Best note |
|---|---:|---:|---:|---:|---:|---|
| Zipf 1.00, cap 5% | 72.285% | 72.296% | 71.617% | 72.529% | **72.545%** | smaller ghost wins by +0.260 pp |
| Zipf 1.10, cap 10% | 86.751% | 86.752% | 86.662% | **86.872%** | 86.838% | larger hot queue wins by +0.121 pp |
| Zipf 1.50, cap 25% | **98.826%** | **98.826%** | **98.826%** | **98.826%** | **98.826%** | knobs do not matter once skew is high |

The sweep reinforces the earlier admission lesson: `small_to_main_threshold=2`
allocates the frequency sketch and still loses hit ratio here, so it is not a
TinyLFU substitute.

A 2026-06-25 opt-in TinyLFU admission slice adds
`S3FifoConfig::with_admission_filter(true)` and keeps defaults unchanged. The
first MfS-only tuning run did **not** justify a default change:

| Workload | default | ghost25 | admission | admission_ghost0 | admission_ghost25 | Best note |
|---|---:|---:|---:|---:|---:|---|
| Zipf 1.00, cap 5% | 72.285% | **72.545%** | 69.666% | 71.498% | 72.116% | admission variants trail `ghost25` |
| Zipf 1.10, cap 10% | 86.751% | 86.838% | 86.825% | 86.583% | **86.900%** | `admission_ghost25` gains +0.149 pp over default |
| Zipf 1.50, cap 25% | **98.826%** | **98.826%** | **98.826%** | **98.826%** | **98.826%** | all variants tie under high skew |

Interpretation: the new admission filter is useful as an opt-in experiment and
now has benchmark rows in the tuning and competitor matrices, but the first run
does not close the admission-heavy competitor gap.

The full-matrix follow-up used the competitor harnesses rather than the
deterministic MfS-only bench:

```bash
cargo bench --all-features -p mfs-core --bench foyer_bench_hit_ratio
cargo bench --all-features -p mfs-core --bench tinyufo_bench_hit_ratio
```

Representative full-matrix rows:

| Matrix | Workload | default | hot95 | ghost25 |
|---|---|---:|---:|---:|
| Foyer | Zipf 1.00, cap 5% | 66.00% | 66.40% (+0.40 pp) | **66.52% (+0.52 pp)** |
| Foyer | Zipf 1.10, cap 10% | 82.13% | 82.44% (+0.31 pp) | **82.55% (+0.42 pp)** |
| Foyer | Zipf 1.50, cap 25% | 98.96% | 98.98% (+0.02 pp) | **98.99% (+0.03 pp)** |
| TinyUFO | Zipf 1.00, cap 5% | 66.32% | 66.70% (+0.38 pp) | **66.85% (+0.53 pp)** |
| TinyUFO | Zipf 1.10, cap 10% | 82.20% | 82.47% (+0.27 pp) | **82.60% (+0.40 pp)** |
| TinyUFO | Zipf 1.50, cap 25% | 99.00% | 99.02% (+0.02 pp) | **99.03% (+0.03 pp)** |

Across the full matrix, `ghost25` was the best MfS variant on 24/25 Foyer rows
and 25/25 TinyUFO rows. `hot95` improved over default on every row, but it
almost never beat `ghost25`. The gains are small, and `ghost25` still trails the
strongest competitor rows, so this is a follow-up validation candidate rather
than a default change. `threshold2` stays excluded because prior evidence
regressed the meaningful rows and this matrix does not revive it.

### Policy cache throughput (`tinyufo_bench_perf`)

Single-thread reads (T460 oversubscribed 8-thread run):

| Cache | avg ns/op | ops/s |
|---|---:|---:|
| lru | **138 ns** | **7.23 M** |
| quick_cache | 143 ns | 6.98 M |
| mfs_s3fifo | 160 ns | 6.24 M |
| tinyufo | 178 ns | 5.59 M |
| tinyufo compact | 224 ns | 4.46 M |
| moka | 321 ns | 3.11 M |

8-thread aggregate reads are essentially tied between `mfs_s3fifo` (**10.56 Mops/s**)
and quick_cache (**10.59 Mops/s**), with TinyUFO (10.29 Mops/s), TinyUFO compact
(6.70 Mops/s), moka (5.29 Mops/s), and LRU (2.64 Mops/s) behind. After the S3FIFO
contention fix (shard count doubled from 2× to 4×, ghost25 as default), the mixed
read/write row now favors `mfs_s3fifo`:

| Cache | 8-thread mixed ops/s |
|---|---:|
| **mfs_s3fifo** | **7.64 M** |
| tinyufo compact | 6.69 M |
| tinyufo | 5.88 M |
| quick_cache | 5.23 M |
| moka | 2.62 M |
| lru | 3.99 M |

MfS previously trailed quick_cache on 8-thread mixed throughput; the shard-count
increase and ghost25 default closed that gap.

### SCC benches

Representative Criterion medians:

| Bench | Median |
|---|---:|
| scc::HashMap insert_single_async | 93.13 ns |
| scc::HashMap insert_single_sync | 106.22 ns |
| scc::HashMap insert_remove_single_sync | 201.81 ns |
| scc::HashMap read_sync | 223.15 ns |
| scc::HashIndex peek | 200.28 ns |
| scc::HashCache get | 381.42 ns |
| scc::TreeIndex peek | 237.99 ns |
| scc::TreeIndex insert_sync | 362.00 ns |

Remaining competitor edge: `scc::HashMap` reports much faster single-operation
insert medians than `mfs_hot_path`'s `lockfree_insert` (441/456 ns). This is
not the same workload as MfS's boxed-entry million-key insertion loop, but it is
a real signal: write-path allocation and insertion remain the biggest area to
attack. The existing inline-slot designs (`DenseKvMap`, `InlineU64Map`, planned
WriteBehindCache v3) are the right response.

After adding `DenseWriteBehindU64` to `mfs_hot_path`, the slot-index direction is
confirmed with current numbers: `dense_writeback_put` is about **2.5× faster**
than Criterion's `writeback_put` batch median (~160 ns/op vs ~485 ns/op) and
beats the boxed `lockfree_replace` in this run (~160 ns/op vs ~198 ns/op). It is
still above the 17 ns `DenseKvMap` floor because it includes slot locking,
versioning, and dirty-queue bookkeeping.

The first generic v3 slice (`DenseWriteBehindMap<K, V>`, currently for
`V: DenseValue`) also beats the boxed path: `dense_writeback_map_put`
measured **199.68 / 235.38 ns** for `u64 -> [u8; 8]`, versus the same-run
`lockfree_replace` at **296.62 / 321.67 ns**. This proves the slot-index design
extends beyond `u64 -> u64` without losing the write-speed advantage.

The direct in-memory `DenseKvMap<K, V>` lane is now tracked in `mfs_neural_hot_path`.
Fresh 1M-key rows on this run:

| Path | min ns/op | median ns/op |
|---|---:|---:|
| dense_kv_get | 62.87 | 79.04 |
| dense_kv_read_with | 59.98 | 92.48 |
| dense_kv_put | **156.38** | **182.16** |
| dense_writeback_map_get | 195.95 | 228.62 |
| dense_writeback_map_put | 298.95 | 310.72 |

Interpretation: this million-key harness is slower than the earlier 17 ns
existing-key spot-check, but the ordering still holds: the in-memory dense slot
map is materially faster than the write-behind dense map. That matches the
contention result: `DenseKvMap` is the production hot-write lane for 8-byte state,
while write-behind remains a higher-overhead durability lane.

After moving `DenseWriteBehindMap`'s logical-clock tick to the actual dirty-enqueue
branch, the contention tail improves but still does not reach `DenseKvMap`. Adding
`DenseWriteBehindU64` and the experimental `ConcurrentDenseWriteBehindMap` to the
same bench isolates the index-layer cost: the faster write-behind lanes use the
lock-free `ConcurrentMap` handle index, while the existing generic map uses
`BucketedIndex`.

A follow-up thread sweep changed `dense_dashmap_contention` to run every scenario
at 1, 2, 4, and 8 threads in one invocation. Command:
`cargo bench --all-features -p mfs-neural --bench dense_dashmap_contention`.
Representative 8-thread rows from the 2026-06-23 run:

| Scenario | DashMap p99 / p99.9 | InlineU64 p99 / p99.9 | DenseKvMap p99 / p99.9 | DenseWriteBehindU64 p99 / p99.9 | ConcurrentDenseWB p99 / p99.9 | DenseWriteBehindMap p99 / p99.9 |
|---|---:|---:|---:|---:|---:|---:|
| single key write | **801 ns** / 29.4 µs | 1.25 µs / 3.10 µs | 831 ns / **1.47 µs** | 1.62 µs / 3.64 µs | 1.12 µs / 2.00 µs | 3.62 µs / 37.3 µs |
| single key mixed | **433 ns** / 1.38 µs | 520 ns / 957 ns | 456 ns / **816 ns** | 641 ns / 1.24 µs | 684 ns / 1.24 µs | 2.65 µs / 15.8 µs |
| 64-key write | 384 ns / 849 ns | 257 ns / 489 ns | **220 ns** / **469 ns** | 348 ns / 626 ns | 292 ns / 578 ns | 1.05 µs / 4.73 µs |
| 64-key mixed | 356 ns / 698 ns | **193 ns** / **382 ns** | 209 ns / 460 ns | 333 ns / 733 ns | 279 ns / 605 ns | 816 ns / 1.93 µs |
| 1024-key mixed | 347 ns / 769 ns | 210 ns / 526 ns | 271 ns / 707 ns | 372 ns / 907 ns | **246 ns** / **571 ns** | 489 ns / 1.05 µs |

The sweep makes the DashMap edge narrower than the older `mfs_lockfree_shadow`
stress row suggested. DashMap still keeps a few p99 wins, especially 8-thread
single-key pure write and mixed rows, but MfS dense/inline lanes won every p99.9
row in this run. `ConcurrentDenseWriteBehindMap` consistently beats the bucketed
`DenseWriteBehindMap` tail and is the better generic write-behind hot-contention
index experiment, but it does not replace non-durable `DenseKvMap` or the
specialized `DenseWriteBehindU64` floor.

A later diagnostic mode for `dense_dashmap_contention` repeats selected rows with
run-to-run p99/p99.9 summaries:

```bash
MFS_DENSE_CONTENTION_THREADS=8 \
MFS_DENSE_CONTENTION_SCENARIOS=single_key_write,single_key_mixed \
MFS_DENSE_CONTENTION_REPEATS=10 \
cargo bench --all-features -p mfs-neural --bench dense_dashmap_contention
```

Representative median p99/p99.9 from that diagnostic run:

| Scenario | DashMap | DenseKvMap | InlineU64 | ConcurrentDenseWB |
|---|---:|---:|---:|---:|
| single key write | 1.17 µs / 81.9 µs | **727 ns / 1.25 µs** | 1.26 µs / 3.19 µs | 1.19 µs / 2.24 µs |
| single key mixed | 747 ns / 19.3 µs | **479 ns / 803 ns** | 537 ns / 935 ns | 589 ns / 1.06 µs |

Interpretation: the earlier DashMap p99 edge is not stable enough to justify
production surgery. MfS wins the repeated-run median p99 on the two narrow
single-key rows here, and still wins p99.9 by a large margin. Treat DashMap as a
tail-pressure benchmark, not a code-change trigger.

The safe clock fix removes one global atomic from repeated dirty writes. The dense
generic value lanes now use a sealed `DenseValue` trait (`u64`, `i64`, `f64`,
`[u8; 8]`) instead of accepting arbitrary `Copy` 8-byte types. That avoids invalid
bit-pattern and padding hazards in the dense `AtomicU64` storage. Public
write-behind reads now retry if they observe a slot's write bit, so concurrent
readers do not transiently see `None` for existing keys. `BucketedIndex` helps
insert-heavy/no-allocation lanes but is not the right default for hot existing-key
contention.

The larger-value v3 slice (`SlotWriteBehindCache<K, V>`) avoids replacing the
hash-table entry on existing-key writes but still allocates a fresh `Arc<V>`
payload per write. On `u64 -> [u8; 128]` it measured:

| Path | min ns/op | median ns/op |
|---|---:|---:|
| boxed_writeback_read_with | **46.42** | **67.80** |
| boxed_writeback_put | 683.12 | 792.65 |
| slot_writeback_read_with | 68.01 | 103.58 |
| slot_writeback_put | **538.95** | **654.16** |

This gives a **~17% write improvement** for larger owned values, but reads are
slower because the slot variant performs two protected lookups (`index -> slot`,
then `slot -> Arc<V>`). Therefore this type is useful as a write-optimized v3
prototype, not yet a replacement for the read-optimized boxed `WriteBehindCache`.

The stronger arbitrary-value v3 slice is `AtomicWriteBehindCache<K, V>`, which
keeps the map entry stable and atomically swaps only the inner `Arc<V>` pointer.
On the same `u64 -> [u8; 128]` bench:

| Path | min ns/op | median ns/op |
|---|---:|---:|
| boxed_writeback_read_with | 45.06 | 61.90 |
| boxed_writeback_put | 563.93 | 698.31 |
| slot_writeback_read_with | 89.23 | 112.92 |
| slot_writeback_put | 310.03 | 404.56 |
| atomic_writeback_read_with | **40.86** | **54.29** |
| atomic_writeback_put | **377.73** | **417.76** |

After completing the `AtomicWriteBehindCache` flushing system and re-running the
same bench, the result was less decisive:

| Path | min ns/op | median ns/op |
|---|---:|---:|
| boxed_writeback_read_with | **28.92** | **54.86** |
| boxed_writeback_put | 549.31 | 684.67 |
| slot_writeback_read_with | 72.70 | 100.57 |
| slot_writeback_put | **435.44** | **459.68** |
| atomic_writeback_read_with | 86.59 | 110.15 |
| atomic_writeback_put | 541.14 | 679.01 |

Current interpretation: `AtomicWriteBehindCache` has the right flushing
semantics and avoids map-entry churn. `SlotWriteBehindCache` is the
stronger write-optimized lane in the latest run, while boxed `WriteBehindCache`
still wins reads for larger values. Neither slot nor atomic v3 removes the
fixed-capacity limit; papaya-style incremental growth remains a separate axis.

The first Redis-like `MfsValue` writer comparison (`mfs_object_store`, 10k keys,
128-byte byte values plus small hashes) keeps the boxed writer as the object-store
baseline for now:

| Path | min ns/op | median ns/op |
|---|---:|---:|
| boxed_value_read_with | **26.65** | **48.58** |
| boxed_value_put_bytes | **649.75** | **863.50** |
| boxed_value_put_hash | 1643.39 | **1720.23** |
| atomic_value_read_with | 182.20 | 198.82 |
| atomic_value_put_bytes | 883.09 | 1121.37 |
| atomic_value_put_hash | **1392.23** | 2018.27 |
| slot_value_read_with | 108.33 | 182.84 |
| slot_value_put_bytes | 912.43 | 1020.27 |
| slot_value_put_hash | 1605.36 | 2173.77 |

Interpretation: the theory that stable-entry atomic swaps would immediately win
for arbitrary Redis-like values did not hold on this run. `MfsObjectStore` should
remain backed by boxed `WriteBehindCache<Vec<u8>, MfsValue>` until a future
object-specific slot/atomic writer beats it on both reads and writes.

The helper-mutation rows in `mfs_object_store` measure public Redis-like
commands on one growing key, reported as amortized per-item latency over a
1k-item batch. Batch APIs amortize the full-container clone/rebuild cost into one
write. The 2026-06-25 refresh also includes scalar helpers, reads, deletes,
write-behind flushes, WAL flushes, and mutable checkpoint/recover rows:

| Path | median ns/op |
|---|---:|
| object_append_bytes | 2,313.28 |
| mutable_object_append_bytes | **1,532.34** |
| object_incr_by | 2,370.77 |
| mutable_object_incr_by | **1,988.48** |
| object_get_string | **2,176.31** |
| mutable_object_get_string | 2,504.81 |
| object_delete_existing | **2,635.57** |
| mutable_object_delete_existing | 3,434.28 |
| mutable_object_grow_strings | 2,628.64 |
| object_list_push | 79,799.64 |
| mutable_object_list_push | **1,747.42** |
| object_list_extend_1k | 955.94 |
| mutable_object_list_extend_1k | **899.61** |
| object_hash_set | 142,807.78 |
| mutable_object_hash_set | **2,613.08** |
| object_hash_set_many_1k | **1,732.91** |
| mutable_object_hash_set_many_1k | 1,865.95 |
| object_set_add | 78,529.96 |
| mutable_object_set_add | **1,558.43** |
| object_set_add_many_1k | 1,521.31 |
| mutable_object_set_add_many_1k | **1,188.48** |
| object_zadd | 142,388.64 |
| mutable_object_zadd | **4,601.10** |
| object_zadd_many_1k | **2,983.90** |
| mutable_object_zadd_many_1k | 4,345.99 |
| object_flush_counting_1k | **4,457.57** |
| mutable_object_flush_counting_1k | 7,253.86 |
| object_flush_wal_1k | **5,714.56** |
| mutable_object_flush_wal_1k | 7,335.68 |
| mutable_object_checkpoint_recover_1k | 16,485.39 |

Interpretation: single-command list/hash/set/zset mutations remain O(n) on the
default boxed `MfsObjectStore` because the current `MfsValue` representation stores
whole containers. The batch helpers do not change that representation, but they
collapse 1,000 read-clone-write cycles into one read-clone-write cycle for
workloads that can batch. The opt-in `MfsMutableObjectStore` prototype keeps
list/hash/set containers mutable behind sharded locks and stores sorted sets as a
member-to-score map plus score/member ordered index. On the latest run it cuts
list push from **79.8 µs** to **1.75 µs**, hash set from **143 µs** to **2.61 µs**,
set add from **78.5 µs** to **1.56 µs**, and zadd from **142 µs** to **4.60 µs**.
Scalar reads/deletes are closer, but boxed won this fresh string-read and delete
run; boxed also wins the counting-flush and WAL-flush rows.
`mutable_object_grow_strings`
uses an initial capacity hint of 1 and still inserts 10k strings, confirming the
mutable candidate is not fixed-capacity like the boxed `ConcurrentMap` path. The mutable backend now has
write-behind/WAL parity plus a library-only `<name>.mfs/` bundle with `MANIFEST`,
`wal/`, `checkpoints/`, and checkpoint+WAL recovery.

The `mfs_object_realistic` custom harness now runs both boxed `MfsObjectStore`
and opt-in `MfsMutableObjectStore` on string-heavy, hash-heavy, list-heavy, and
mixed Redis-like workloads. It reports aggregate throughput, sampled read/mutation
p50 and p99 latency plus sample counts, flush records/s, approximate flushed
bytes, and optional WAL replay validation when `MFS_OBJ_WAL_PATH` is set. Useful
knobs include `MFS_OBJ_OPS`, `MFS_OBJ_RUNS`, `MFS_OBJ_THREADS`, `MFS_OBJ_KEYS`,
`MFS_OBJ_SAMPLE_RATE`, and shape controls such as `MFS_OBJ_VALUE_BYTES`,
`MFS_OBJ_HASH_FIELDS`, and `MFS_OBJ_LIST_ITEMS`.
Bounded evidence run: `MFS_OBJ_OPS=5000 MFS_OBJ_RUNS=1 MFS_OBJ_THREADS=4
MFS_OBJ_KEYS=1000 cargo bench --all-features -p mfs-compat --bench
mfs_object_realistic`.

| Store/workload | Throughput | Read p50 / p99 | Mutate p50 / p99 | Flush rate |
|---|---:|---:|---:|---:|
| boxed:string-heavy | 0.43 Mops/s | 357 ns / 4.70 µs | 1.06 / 1.36 µs | 0.48 Mrec/s |
| mutable:string-heavy | **0.53 Mops/s** | 439 ns / 4.84 ms | 1.33 / 1.69 µs | **0.65 Mrec/s** |
| boxed:hash-heavy | 0.11 Mops/s | **702 ns** / **6.18 µs** | 2.29 / 7.10 µs | **0.82 Mrec/s** |
| mutable:hash-heavy | **0.35 Mops/s** | 1.07 µs / 41.3 µs | **1.45 / 2.85 µs** | 0.17 Mrec/s |
| boxed:list-heavy | **0.49 Mops/s** | **540 ns / 2.59 µs** | 1.48 / 6.79 µs | **0.96 Mrec/s** |
| mutable:list-heavy | 0.46 Mops/s | 954 ns / 3.11 ms | **1.42 / 4.41 µs** | 0.34 Mrec/s |
| boxed:mixed | 0.16 Mops/s | **707 ns** / 10.3 µs | **1.40 µs** / 3.28 µs | **0.83 Mrec/s** |
| mutable:mixed | **0.28 Mops/s** | 1.49 µs / **8.38 µs** | 1.50 / **3.15 µs** | 0.25 Mrec/s |

Interpretation: mutable improves throughput on string-heavy, hash-heavy, and
mixed workloads, and lowers mutation tails in hash/list/mixed rows. The old
string/list millisecond p99 rows above came from the default `MFS_OBJ_SAMPLE_RATE=64`
and only a small number of read samples, so they are sparse-sampling-sensitive
outliers rather than confirmed stable production tails. A follow-up full-sampling
run (`MFS_OBJ_OPS=20000 MFS_OBJ_RUNS=3 MFS_OBJ_THREADS=4 MFS_OBJ_KEYS=1000
MFS_OBJ_SAMPLE_RATE=1`) kept mutable reads mostly in the microsecond range:
string-heavy p99 **3.23-6.67 µs**, hash-heavy **6.48-28.0 µs**, list-heavy
**4.76-5.60 µs**, and mixed **14.0-36.1 µs**. The mutable read path has
real lock/materialization overhead versus boxed, so it remains opt-in.

Clean-room SCC study led to a fixed-capacity `BucketedIndex<K>` prototype with
32 inline `(K, handle)` entries per bucket and per-bucket `RwLock`s. Initial
hot-path numbers are promising: `bucketed_index_insert` measured
**355.46 / 445.79 ns** and `bucketed_index_replace` measured
**202.90 / 219.55 ns**, versus same-run boxed `lockfree_insert` at
**535.09 / 783.70 ns** and `lockfree_replace` at **230.50 / 270.77 ns**. This
beats the boxed index path but is not yet integrated under the dense/slot
write-behind variants.

After wiring `BucketedIndex<K>` under `DenseWriteBehindMap<K, V>`, the index
prototype improved further in the latest run: `bucketed_index_insert` measured
**244.40 / 252.55 ns** and `bucketed_index_replace` measured
**93.59 / 111.10 ns**, versus same-run boxed `lockfree_insert` at
**296.67 / 308.63 ns** and `lockfree_replace` at **177.60 / 197.93 ns**.
`dense_writeback_map_put` improved to **183.38 / 215.14 ns** for
`u64 -> [u8; 8]`. The bucketed index is now near the scc single-insert lane;
the remaining cost is slot locking, versioning, and dirty-queue bookkeeping.

The queued write lane for `DenseWriteBehindMap` is explicit about semantics:
`put_async` only means accepted into an in-memory queue; visibility is guaranteed
after `WriteTicket::wait_applied` or `barrier_all`. On the queued-write bench
(100k writes, 5 trials):

| Path | min ns/op | median ns/op |
|---|---:|---:|
| eager_dense_put | 174.11 | 185.89 |
| eager_dense_replace | 169.08 | 203.15 |
| queued_put_enqueue | **68.34** | 209.06 |
| queued_replace_enqueue | **97.96** | **132.27** |
| queued_barrier_all | **140.61** | 221.23 |

Queued writes improve caller-visible enqueue latency, especially same-key
replace bursts, but `barrier_all` is the honest apply-visible metric. Do not
compare enqueue-only numbers to eager-write durability/visibility semantics.

### MfS realistic workload (`mfs_realistic`)

5 s, 2 threads, 100k keys, ~128 B values, 95/4/1 read/write/delete,
80% hot, flusher tick 10 ms, 1/64 latency sampling:

| Metric | Value |
|---|---:|
| Aggregate throughput | 3.75 M ops/sec |
| Reads / misses | 14.56 M / 3.24 M |
| Writes / deletes | 748,690 / 187,920 |
| Read p50 / p99 / p99.9 / max | 218 ns / 945 ns / 1.93 µs / 5.02 ms |
| Write p50 / p99 | 1.65 µs / 14.9 µs |
| Delete p50 | 574 ns |
| Flush rate | 0.18 M rec/s |
| Cache state at end | 82,268 live, 0 dirty |

The 2026-05-29 branch adds WAL-backed realistic modes controlled by
`MFS_WAL_PATH` and `MFS_WAL_MODE=direct|async|group`. A short smoke run
(`MFS_DURATION_SECS=1 MFS_KEYS=1000 MFS_THREADS=2 MFS_VALUE_BYTES=32
MFS_AUTO_FLUSHER=1`) proved the full path: memory writes -> dirty queues ->
AutoFlusher -> WAL files -> durable shutdown -> replay validation.

| Backend | Throughput | Read p50 / p99 | Write p50 / p99 | Flushed | Durable sync | WAL files / replayed |
|---|---:|---:|---:|---:|---:|---:|
| counting | 10.72 M ops/s | 38 ns / 271 ns | 872 ns / 9.81 µs | 60,309 | 0.00 ms | 0 / 0 |
| wal_async | 9.34 M ops/s | 41 ns / 323 ns | 955 ns / 9.33 µs | 61,164 | 4.41 ms | 8 / 61,164 |
| wal_group | 9.24 M ops/s | 41 ns / 324 ns | 954 ns / 9.69 µs | 60,377 | 3.05 ms | 8 / 60,377 |

These are smoke numbers, not stable headline results. Their value is semantic:
the realistic bench now verifies replayable WAL output for flushed records.

### WAL async-vs-direct (`mfs_wal_async`)

128 batches × 64 records = 8192 records. `flush_*` is caller-visible
`FlushBackend::flush` latency; durable total includes the final sync/shutdown.
The 2026-05-29 refresh adds two important cases: 4-producer WAL pressure and
`GroupCommitWalBackend`, whose `flush()` is a durable ack rather than an enqueue.

| Case | flush p50 | flush p99 | enqueue total | durable total | durable rate |
|---|---:|---:|---:|---:|---:|
| direct_default | 8.76 µs | 39.4 µs | 1.49 ms | **1.49 ms** | **5.50 M rec/s** |
| async_default | **2.04 µs** | 34.2 µs | **0.46 ms** | 9.47 ms | 0.87 M rec/s |
| group_commit_default | 37.4 µs | 196 µs | 5.64 ms | 5.74 ms | 1.43 M rec/s |
| direct_sync_at_end | 9.24 µs | 63.9 µs | 1.34 ms | **1.37 ms** | **5.97 M rec/s** |
| async_sync_at_end | **1.31 µs** | **6.06 µs** | **0.23 ms** | 4.09 ms | 2.00 M rec/s |
| group_commit_sync_each | 43.9 µs | 83.5 µs | 7.31 ms | 7.44 ms | 1.10 M rec/s |
| direct_4p_sync_at_end | 9.09 µs | 1.43 ms | 9.05 ms | 9.11 ms | 3.60 M rec/s |
| async_4p_sync_at_end | **1.21 µs** | **15.0 µs** | **6.46 ms** | 23.54 ms | 1.39 M rec/s |
| group_commit_4p | 64.7 µs | 4.21 ms | 36.72 ms | 36.82 ms | 0.89 M rec/s |

Interpretation: async WAL remains the fastest caller-visible path, but it is an
enqueue contract; use `sync_barrier`/`shutdown` for the durable boundary. The new
durable group-commit backend is semantically stronger because `flush()` only
returns after `sync_data`, but on this filesystem that strength is expensive and
does not beat direct buffered WAL. The product path is therefore explicit:
memory writes stay fast, async WAL gives low caller latency with explicit
barriers, and group-commit handles are for callers who want each flush call to be
power-loss durable before dirty state is cleaned.

### Probe diagnostic (`mfs_probe`)

| Configuration | min ns/op |
|---|---:|
| papaya<u64,u64> Fx insert-populated | 56.37 |
| papaya<u64,u64> Fx compute-populated | 63.96 |
| papaya<u64,Probe> Fx compute-populated | 70.59 |
| WriteBehindCache under-sized to 1024, attempted large preload | 276.46 |
| WriteBehindCache pre-sized cap | 215.45 |
| WriteBehindCache pre-sized read_with | 63.35 |

---

## Local refresh, 2026-06-19

Fresh T460 run on the same laptop class as the earlier tables: i5-6300U,
2c/4t, `tsc`, governor=`powersave`. This refresh reran the core MfS hot
paths, hot storage lanes, the new local library-only DB comparison, the
probe diagnostic, and the full `bench-competitors` target.

Commands run:

```bash
make bench-hot
MFS_NOSQL_TRIALS=5 MFS_NOSQL_KEYS=1024 MFS_NOSQL_VALUE_BYTES=128 make bench-nosql-engine
MFS_LOCAL_DB_KEYS=10000 MFS_LOCAL_DB_TRIALS=3 make bench-local-db
make bench-probe
make bench-competitors
```

### MfS hot paths (`mfs_hot_path`)

| Path | min ns/op | median ns/op | peak ops/s |
|---|---:|---:|---:|
| generic_get | 313.53 | 331.33 | 3.19 M |
| generic_read_with | 197.17 | 203.28 | 5.07 M |
| lockfree_get | **50.38** | **65.15** | **19.85 M** |
| lockfree_insert | 510.04 | 552.13 | 1.96 M |
| lockfree_replace | 334.46 | 358.12 | 2.99 M |
| writeback_get_pinned | 178.54 | 185.65 | 5.60 M |
| writeback_read_with_pinned | 63.25 | 75.81 | 15.81 M |
| writeback_get_ref_pinned | 66.16 | 69.85 | 15.11 M |
| writeback_get_oneshot | 197.46 | 207.82 | 5.06 M |
| generic_get_batch | 402.15 | 434.90 | 2.49 M |
| dense_load_scalar | **3.07** | **4.42** | **325 M** |
| dense_load_many_seq | 5.65 | 7.38 | 177 M |
| dense_load_many_stride17 | 15.58 | 19.63 | 64.2 M |

Multi-thread read scaling:

On this T460, treat 1/2/4 threads as the primary rows. The 8-thread row
oversubscribes the 2c/4t CPU and is best read as scheduler-stress data.

| Threads | mfs_get | mfs_read_with | lockfree_get | writeback_read_with |
|---:|---:|---:|---:|---:|
| 1 | 333.10 ns / 3.00 M | 191.56 ns / 5.22 M | 46.11 ns / 21.7 M | 47.55 ns / 21.0 M |
| 2 | 198.32 ns / 5.04 M | 133.60 ns / 7.48 M | 24.96 ns / **40.1 M** | 37.44 ns / 26.7 M |
| 4 | 162.67 ns / 6.15 M | 93.90 ns / 10.7 M | **23.41 ns / 42.7 M** | 35.29 ns / 28.3 M |
| 8 | 152.36 ns / 6.56 M | 103.46 ns / 9.67 M | 26.55 ns / 37.7 M | **27.94 ns / 35.8 M** |

### MfS hot store (`mfs_store_bench`)

Single-threaded, 1024 keys, 128-byte values, 5 trials.

| Lane | Durability | min ns/op | median ns/op | max ns/op | peak ops/s |
|---|---|---:|---:|---:|---:|
| raw_hot_get | MemoryOnly | **151.22** | **215.69** | 521.01 | **6.61 M** |
| raw_memory_put | MemoryOnly | 410.70 | 477.59 | 820.88 | 2.43 M |
| expected_version_conflict_put | MemoryOnly | 334.83 | 445.11 | 845.29 | 2.99 M |
| schema_put_one_secondary_index | MemoryOnly | 6,727.74 | 9,101.62 | 11,147.88 | 0.15 M |
| schema_update_one_secondary_index | MemoryOnly | 4,528.70 | 6,436.71 | 9,759.32 | 0.22 M |
| wal_enqueue | WalAsync | 1,072.37 | 1,165.74 | 1,587.19 | 0.93 M |
| wal_sync | WalSync | 6,507.17 | 9,419.57 | 13,415.62 | 0.15 M |
| checkpoint_write | SnapshotOnly | 940.10 | 1,061.51 | 1,483.17 | 1.06 M |
| replay | WalSync | 4,628.74 | 5,283.78 | 7,250.38 | 0.22 M |

The schema update lane preloads the same keys, then updates payload plus the
single secondary index field. It exercises the old-document decode/index-diff path
that pure insert does not. In this run update is faster than insert, so old-doc
decode is real work but not the only schema cost; validation, encoding, and index
planning remain the larger combined target.

### Local library-only DB comparison (`local_db_sqlite_kv`)

No Redis/Memcached/Dragonfly services or extra binaries. All competitors are
linked libraries. 10k keys, 128-byte values, 3 trials. Read lanes distinguish
owned/materialized reads from borrowed/view reads where the competitor API allows
it. `*_tx` write lanes commit once per full key batch; `*_autocommit` lanes
commit once per key. MfS `wal_enqueue_buffered` is not fsync-durable;
`mfs_wal_sync_per_put` is per-key sync.

| Lane | min ns/op | median ns/op | max ns/op | peak ops/s |
|---|---:|---:|---:|---:|
| mfs_raw_read_owned | **214.34** | **246.08** | 377.37 | **4.67 M** |
| sqlite_memory_read_owned | 2,807.66 | 3,107.40 | 3,261.44 | 0.36 M |
| redb_read_view | 939.26 | 1,200.60 | 2,071.34 | 1.06 M |
| fjall_read_view | 1,683.39 | 1,817.31 | 1,900.65 | 0.59 M |
| redb_read_owned | 774.08 | 2,108.08 | 2,646.54 | 1.29 M |
| fjall_read_owned | 1,702.59 | 1,799.66 | 2,451.90 | 0.59 M |
| mfs_raw_memory_put | **411.98** | **545.61** | 718.34 | **2.43 M** |
| sqlite_memory_put_tx | 3,421.56 | 4,367.27 | 6,013.77 | 0.29 M |
| redb_none_put_tx | 2,122.68 | 2,436.39 | 4,770.84 | 0.47 M |
| fjall_buffer_put | 3,819.79 | 4,504.61 | 4,819.09 | 0.26 M |
| sqlite_memory_put_autocommit | 7,399.05 | 8,342.62 | 8,926.16 | 0.14 M |
| mfs_wal_enqueue_buffered | **1,255.71** | **1,748.28** | 1,982.25 | **0.80 M** |
| sqlite_wal_normal_put_tx | 3,085.09 | 3,552.57 | 3,705.94 | 0.32 M |
| fjall_sync_data_put_tx | 4,723.83 | 4,759.72 | 5,285.88 | 0.21 M |
| mfs_wal_sync_per_put | **4,539.75** | **5,372.40** | 5,461.47 | **0.22 M** |
| sqlite_wal_full_put_autocommit | 34,998.46 | 35,825.82 | 36,884.78 | 28.6 k |
| redb_immediate_put_autocommit | 65,821.46 | 69,112.55 | 70,220.76 | 15.2 k |
| fjall_sync_all_put_autocommit | 5,502.50 | 6,156.89 | 6,237.82 | 0.18 M |

Takeaway: on this machine and this 128-byte KV workload, MfS is ahead on every
local library-only lane. `redb` is the closest read/non-durable write competitor;
`fjall` is the closest per-put sync competitor.

### Direct foyer-memory comparison (`foyer_memory_vs_mfs`)

8 threads, 64 hot keys, 50/50 read/write, 50k ops/thread. Like the dashmap
row below, this oversubscribes the T460 and is best treated as scheduler-stress
data:

| Contestant | p50 | p99 | p99.9 | max | Throughput |
|---|---:|---:|---:|---:|---:|
| foyer_memory_fifo | 361 ns | 1.07 µs | 76.5 µs | 12.8 ms | 4.99 Mops/s |
| **mfs_lockfree** | **91 ns** | **567 ns** | **8.18 µs** | 41.1 ms | **6.22 Mops/s** |

### Dashmap hot-key contention (`dashmap_contention`)

8 threads, 64 hot keys, 50/50 read/write, 200k ops/thread. This
oversubscribes the T460's 2c/4t CPU, so use it as a tail-latency stress
test rather than the main laptop throughput score.

| Contestant | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|
| dashmap | **154 ns** | **557 ns** | **1.12 µs** | 22.0 ms |
| mfs_lockfree_shadow | 158 ns | 1.19 µs | 9.15 µs | 42.4 ms |

Dashmap keeps the short-tail edge on this extreme hot-key contention workload.
MfS is close at p50 but trails p99/p99.9.

### Zipfian cache latency and hit ratio (`moka_zipfian`)

Zipfian(α=1.1), 100k working set, 10k cap for moka/MfS S3FIFO, 1M ops:

| Contestant | hit ratio | p50 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|
| moka_sync | 86.900% | 446 ns | 24.3 µs | 84.2 µs | 5.16 ms |
| mfs_lockfree_shadow (uncapped) | **93.544%** | **110 ns** | **719 ns** | **1.97 µs** | 1.82 ms |
| mfs_s3fifo (cap=10k) | 86.711% | 187 ns | 2.22 µs | 7.76 µs | **1.73 ms** |

The bounded MfS S3FIFO lane nearly matches moka's hit ratio and is much faster
on latency. The uncapped `mfs_lockfree_shadow` row remains a latency reference,
not a fair hit-ratio comparison.

### Papaya diagnostics

Papaya's 8-thread concurrent insert tail-latency bench:

| Contestant | Single p99 insert | Concurrent p99 range | Worst observed concurrent p99 |
|---|---:|---:|---:|
| papaya incremental | 35 ms | 32-87 ms | 87 ms |
| papaya blocking | 28 ms | 172-477 ms | 477 ms |
| dashmap | 26 ms | **16-31 ms** | **31 ms** |

Single-thread read Criterion loop (`papaya_single_thread`, 10k-loop time):

| Map | Criterion mid |
|---|---:|
| std::HashMap | **573.37 µs** |
| dashmap | 899.11 µs |
| papaya | 924.45 µs |

### SCC benches

Representative Criterion medians:

| Bench | Median |
|---|---:|
| scc::HashMap insert_single_async | 150.19 ns |
| scc::HashMap insert_single_sync | **118.07 ns** |
| scc::HashMap insert_remove_single_sync | 311.30 ns |
| scc::HashMap read_sync | 288.15 ns |
| scc::HashIndex iter_with | **13.69 ns** |
| scc::HashIndex peek | 270.69 ns |
| scc::HashCache get | 288.65 ns |
| scc::HashCache put, saturated | 127.73 ns |
| scc::TreeIndex iter_with | **13.67 ns** |
| scc::TreeIndex peek | 242.84 ns |
| scc::TreeIndex insert_sync | 346.13 ns |

SCC still shows strong single-operation insert/cache lanes. These are not the
same workload as MfS million-key writeback loops, but they remain useful pressure
targets for the write path.

### Quick-cache representative Criterion rows

| Workload | Criterion mid | Throughput |
|---|---:|---:|
| Reads N=10k S=0.5 cap=10k | 36.40 µs | 27.48 Melem/s |
| Reads N=10k S=0.75 cap=10k | 44.04 µs | 22.71 Melem/s |
| Reads N=1M S=0.5 cap=1M | 61.47 µs | 16.27 Melem/s |
| Reads N=1M S=0.75 cap=1M | 75.84 µs | 13.19 Melem/s |
| Zipf N=1M S=0.5 cap=50k | 500.10 µs | 2.00 Melem/s |
| Zipf N=1M S=0.75 cap=100k | 602.32 µs | 1.66 Melem/s |

### Policy cache hit-ratio edge (`tinyufo_bench_hit_ratio`, `foyer_bench_hit_ratio`)

Representative TinyUFO rows:

| Workload | LRU | moka | quick_cache | TinyUFO |
|---|---:|---:|---:|---:|
| Zipf 1.00, cap 5% | 58.69% | 66.94% | 67.74% | **68.81%** |
| Zipf 1.10, cap 10% | 78.02% | 82.86% | 83.12% | **83.96%** |
| Zipf 1.50, cap 25% | 98.81% | 99.09% | 99.08% | **99.10%** |

Representative foyer rows:

| Workload | foyer FIFO | foyer LRU | foyer LFU | foyer S3FIFO | moka | mfs_s3fifo | mfs_s3fifo_1s |
|---|---:|---:|---:|---:|---:|---:|---:|
| Zipf 1.00, cap 5% | 53.97% | 58.27% | 66.73% | **67.85%** | 66.94% | 66.02% | 66.39% |
| Zipf 1.10, cap 10% | 74.17% | 77.69% | 82.51% | **83.23%** | 82.96% | 82.12% | 82.20% |
| Zipf 1.50, cap 25% | 98.35% | 98.78% | 99.03% | 99.01% | **99.08%** | 98.96% | 99.00% |

MfS S3FIFO is fast, but the best admission-heavy policies still edge it on hit
ratio by small margins.

### Policy cache throughput (`tinyufo_bench_perf`)

Single-thread reads:

| Cache | avg ns/op | ops/s |
|---|---:|---:|
| quick_cache | **171 ns** | **5.84 M** |
| mfs_s3fifo | 173 ns | 5.75 M |
| lru | 178 ns | 5.61 M |
| tinyufo | 243 ns | 4.11 M |
| tinyufo compact | 287 ns | 3.47 M |
| moka | 429 ns | 2.33 M |

8-thread aggregate reads:

| Cache | Aggregate ops/s |
|---|---:|
| tinyufo | **12.86 M** |
| quick_cache | 12.34 M |
| mfs_s3fifo | 11.85 M |
| tinyufo compact | 10.14 M |
| moka | 6.39 M |
| lru | 2.55 M |

8-thread mixed read/write:

| Cache | Aggregate ops/s | Misses |
|---|---:|---:|
| quick_cache | **8.63 M** | 307k |
| mfs_s3fifo | 7.62 M | 327k |
| tinyufo | 7.16 M | 295k |
| tinyufo compact | 6.82 M | 294k |
| lru | 3.50 M | 410k |
| moka | 3.43 M | 296k |

### Foyer dynamic dispatch (`foyer_bench_dynamic_dispatch`)

| Loops | static | box dynamic | arc dynamic |
|---:|---:|---:|---:|
| 100k | 30 ns | 30 ns | 38 ns |
| 1M | 30 ns | 28 ns | 32 ns |
| 10M | 29 ns | 29 ns | 29 ns |

Dynamic-dispatch overhead is negligible in this microbench compared with cache
policy and storage costs.

### Probe diagnostic (`mfs_probe`)

| Configuration | min ns/op |
|---|---:|
| papaya<u64,u64> Fx insert-populated | 53.01 |
| papaya<u64,u64> Fx compute-populated | 48.03 |
| papaya<u64,Probe> Fx compute-populated | 52.48 |
| WriteBehindCache under-sized to 1024, attempted large preload | 115.59 |
| WriteBehindCache pre-sized cap | 162.02 |
| WriteBehindCache pre-sized read_with | 62.22 |

---

## Reproducibility notes

- `cargo bench --bench <name>` runs each contestant. HDR-based benches
  (`papaya_latency`, `dashmap_contention`, `moka_zipfian`) write PNG
  charts into `benches/<competitor>/charts/` and `.hist` files into
  `benches/<competitor>/` so both are previewable from the IDE and
  re-renderable offline.
- Criterion benches write to `target/criterion/`.
- The PNG charts and `.hist` files are gitignored
  (`benches/*/charts/`, `benches/*/*.hist`).
- For thermal stability on the T460, give the laptop ~30 s of idle
  between consecutive long bench runs.
