//! Criterion-based microbenches with adaptive iteration counts and
//! proper statistical reporting (mean, stddev, MAD, regression analysis).
//!
//! Each bench function runs in a tight loop where criterion times many
//! iterations and reports a confidence interval rather than a single
//! number. Per-op nanoseconds reported in the stdout summary; full
//! distributions and HTML reports land under `target/criterion/`.
//!
//! Run with `make bench-criterion` (or `cargo bench --bench criterion_bench`).

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mfs_core::lockfree::LockFreeCache;
use mfs_core::writeback::WriteBehindCache;
use mfs_core::{DenseU64Lane, MemoryFirstStore};
use std::hint::black_box;

const N: usize = 1_000_000;

/// Criterion measurement config tuned for this crate's mix of nanosecond
/// and microsecond ops. Slightly longer than default measurement so the
/// distribution stabilizes; sample size kept moderate so total wall time
/// is reasonable.
fn configured() -> Criterion {
    Criterion::default()
        .warm_up_time(std::time::Duration::from_secs(1))
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(60)
        .significance_level(0.01)
        .noise_threshold(0.02)
}

fn populate_mfs() -> MemoryFirstStore<u64, u64> {
    let s = MemoryFirstStore::<u64, u64>::new();
    for i in 0..N as u64 {
        s.load_clean(i, i.wrapping_mul(2));
    }
    s
}

fn populate_lockfree() -> LockFreeCache<u64, u64> {
    let c = LockFreeCache::<u64, u64>::with_capacity(N);
    {
        let p = c.pin();
        for i in 0..N as u64 {
            p.insert(i, i.wrapping_mul(2));
        }
    }
    c
}

fn populate_writeback() -> WriteBehindCache<u64, u64> {
    let c = WriteBehindCache::<u64, u64>::with_capacity(N);
    for i in 0..N as u64 {
        c.load_clean(i, i.wrapping_mul(2));
    }
    c
}

fn populate_dense() -> DenseU64Lane {
    let lane = DenseU64Lane::with_len(N);
    for i in 0..N {
        lane.store(i, (i as u64).wrapping_mul(2));
        lane.mark_clean(i);
    }
    lane
}

/// Reads on a single key (cache-resident, hottest possible path). Use to
/// expose the per-call function-call overhead independent of the random
/// access pattern.
fn bench_hot_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("hot_single_key");
    group.throughput(Throughput::Elements(1));

    let mfs = populate_mfs();
    group.bench_function("mfs_get", |b| {
        b.iter(|| {
            let v = mfs.get(black_box(&42));
            black_box(v);
        });
    });
    group.bench_function("mfs_read_with", |b| {
        b.iter(|| {
            let v = mfs.read_with(black_box(&42), |&v| v);
            black_box(v);
        });
    });

    let lockfree = populate_lockfree();
    group.bench_function("lockfree_get", |b| {
        let p = lockfree.pin();
        b.iter(|| {
            let v = p.get(black_box(&42)).copied();
            black_box(v);
        });
    });

    let wb = populate_writeback();
    group.bench_function("writeback_get", |b| {
        let p = wb.pin();
        b.iter(|| {
            let v = p.get(black_box(&42));
            black_box(v);
        });
    });
    group.bench_function("writeback_read_with", |b| {
        let p = wb.pin();
        b.iter(|| {
            let v = p.read_with(black_box(&42), |&v| v);
            black_box(v);
        });
    });

    let dense = populate_dense();
    group.bench_function("dense_load", |b| {
        b.iter(|| {
            let v = dense.load(black_box(0));
            black_box(v);
        });
    });

    group.finish();
}

/// Reads spread over the whole 1M-key universe. This is the
/// DRAM/L3-bound case — each lookup is a fresh cache miss on hardware
/// where the working set doesn't fit in cache. Measures how the table
/// layout and seize/RwLock overhead actually behave in production.
fn bench_random_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_reads_1M_keys");
    group.throughput(Throughput::Elements(1));

    let keys: Vec<u64> = (0..N as u64).collect();
    let probe_len = 16;
    let probe = &keys[..probe_len];

    let mfs = populate_mfs();
    group.bench_function("mfs_get", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = probe[i & (probe_len - 1)];
            i = i.wrapping_add(1);
            black_box(mfs.get(&k));
        });
    });
    group.bench_function("mfs_read_with", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = probe[i & (probe_len - 1)];
            i = i.wrapping_add(1);
            black_box(mfs.read_with(&k, |&v| v));
        });
    });

    let lockfree = populate_lockfree();
    group.bench_function("lockfree_get", |b| {
        let p = lockfree.pin();
        let mut i = 0usize;
        b.iter(|| {
            let k = probe[i & (probe_len - 1)];
            i = i.wrapping_add(1);
            black_box(p.get(&k).copied());
        });
    });

    let wb = populate_writeback();
    group.bench_function("writeback_read_with", |b| {
        let p = wb.pin();
        let mut i = 0usize;
        b.iter(|| {
            let k = probe[i & (probe_len - 1)];
            i = i.wrapping_add(1);
            black_box(p.read_with(&k, |&v| v));
        });
    });

    group.finish();
}

/// Sweeps the dense lane across a few sizes so the distinction between
/// L1, L2, L3, and DRAM-resident reads becomes visible.
fn bench_dense_size_sweep(c: &mut Criterion) {
    let mut group = c.benchmark_group("dense_lane_size_sweep");
    group.throughput(Throughput::Elements(1));

    for &len_log2 in &[10u32, 14, 18, 20, 22] {
        let len = 1usize << len_log2;
        let lane = DenseU64Lane::with_len(len);
        for i in 0..len {
            lane.store(i, (i as u64).wrapping_mul(2));
            lane.mark_clean(i);
        }
        let mask = len - 1;

        group.bench_with_input(
            BenchmarkId::new("scalar_load", format!("{}KiB", len * 8 / 1024)),
            &lane,
            |b, lane| {
                let mut i = 0usize;
                b.iter(|| {
                    i = (i.wrapping_add(1)) & mask;
                    black_box(lane.load(i));
                });
            },
        );
    }

    group.finish();
}

/// Mutation paths. Population is per-iteration via iter_batched so each
/// timed step starts from a known fresh state.
fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes");
    group.throughput(Throughput::Elements(1));

    group.bench_function("mfs_put", |b| {
        b.iter_batched(
            populate_mfs,
            |s| {
                s.put(black_box(123), black_box(999));
                black_box(s);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("lockfree_insert", |b| {
        b.iter_batched(
            populate_lockfree,
            |c| {
                let p = c.pin();
                p.insert(black_box(123), black_box(999));
                drop(p);
                black_box(c);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("writeback_put", |b| {
        b.iter_batched(
            populate_writeback,
            |c| {
                c.put(black_box(123), black_box(999));
                black_box(c);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("dense_store", |b| {
        b.iter_batched(
            populate_dense,
            |lane| {
                lane.store(black_box(0), black_box(999));
                black_box(lane);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = configured();
    targets = bench_hot_single, bench_random_reads, bench_dense_size_sweep, bench_writes
}
criterion_main!(benches);
