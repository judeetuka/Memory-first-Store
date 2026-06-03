use mfs_core::lockfree::LockFreeCache;
use mfs_core::writeback::WriteBehindCache;
use mfs_core::{DenseU64Lane, MemoryFirstStore};
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const TRIALS: usize = 7;

struct Stats {
    label: &'static str,
    count: u64,
    min: Duration,
    median: Duration,
    max: Duration,
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "{:<28} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op (peak ops/s={:.0})",
            self.label,
            self.count,
            TRIALS,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min),
        );
    }
}

fn measure<F>(label: &'static str, count: u64, mut body: F) -> Stats
where
    F: FnMut() -> u64,
{
    let mut samples: Vec<Duration> = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let acc = body();
        let elapsed = start.elapsed();
        black_box(acc);
        samples.push(elapsed);
    }
    samples.sort();
    Stats {
        label,
        count,
        min: samples[0],
        median: samples[TRIALS / 2],
        max: samples[TRIALS - 1],
    }
}

fn populate_store(count: u64) -> MemoryFirstStore<u64, u64> {
    let store = MemoryFirstStore::<u64, u64>::new();
    for i in 0..count {
        store.load_clean(i, i.wrapping_mul(2));
    }
    store
}

fn populate_lockfree(count: u64) -> LockFreeCache<u64, u64> {
    let cache = LockFreeCache::<u64, u64>::with_capacity(count as usize);
    let p = cache.pin();
    for i in 0..count {
        p.insert(i, i.wrapping_mul(2));
    }
    drop(p);
    cache
}

fn populate_writeback(count: u64) -> WriteBehindCache<u64, u64> {
    let cache = WriteBehindCache::<u64, u64>::with_capacity(count as usize);
    for i in 0..count {
        cache.load_clean(i, i.wrapping_mul(2));
    }
    cache
}

fn populate_lane(count: usize) -> DenseU64Lane {
    let lane = DenseU64Lane::with_len(count);
    for i in 0..count {
        lane.store(i, (i as u64).wrapping_mul(2));
        lane.mark_clean(i);
    }
    lane
}

fn main() {
    let count = 1_000_000u64;
    println!("=== core single-threaded hot paths (count={count}, trials={TRIALS}) ===");

    let store = populate_store(count);

    measure("generic_get", count, || {
        let mut checksum = 0u64;
        for i in 0..count {
            checksum ^= *store.get(black_box(&i)).expect("loaded key");
        }
        checksum
    })
    .print();

    measure("generic_read_with", count, || {
        let mut checksum = 0u64;
        for i in 0..count {
            checksum ^= store.read_with(black_box(&i), |v| *v).expect("loaded key");
        }
        checksum
    })
    .print();

    let lockfree = populate_lockfree(count);
    measure("lockfree_get", count, || {
        let mut checksum = 0u64;
        let p = lockfree.pin();
        for i in 0..count {
            checksum ^= *p.get(black_box(&i)).expect("loaded key");
        }
        checksum
    })
    .print();

    measure("lockfree_insert", count, || {
        let cache = LockFreeCache::<u64, u64>::with_capacity(count as usize);
        let p = cache.pin();
        for i in 0..count {
            p.insert(black_box(i), black_box(i.wrapping_mul(3)));
        }
        cache.len() as u64
    })
    .print();

    measure("lockfree_replace", count, || {
        let p = lockfree.pin();
        for i in 0..count {
            p.insert(black_box(i), black_box(i.wrapping_mul(11)));
        }
        lockfree.len() as u64
    })
    .print();

    let writeback = populate_writeback(count);
    measure("writeback_get_pinned", count, || {
        let mut checksum = 0u64;
        let p = writeback.pin();
        for i in 0..count {
            checksum ^= *p.get(black_box(&i)).expect("loaded key");
        }
        checksum
    })
    .print();

    measure("writeback_read_with_pinned", count, || {
        let mut checksum = 0u64;
        let p = writeback.pin();
        for i in 0..count {
            checksum ^= p.read_with(black_box(&i), |v| *v).expect("loaded key");
        }
        checksum
    })
    .print();

    measure("writeback_get_ref_pinned", count, || {
        let mut checksum = 0u64;
        let p = writeback.pin();
        for i in 0..count {
            checksum ^= *p.get_ref(black_box(&i)).expect("loaded key");
        }
        checksum
    })
    .print();

    measure("writeback_get_oneshot", count, || {
        let mut checksum = 0u64;
        for i in 0..count {
            checksum ^= *writeback.get(black_box(&i)).expect("loaded key");
        }
        checksum
    })
    .print();

    let batch_size = 64usize;
    let batches = (count as usize) / batch_size;
    let total_batched = (batches * batch_size) as u64;
    measure("generic_get_batch", total_batched, || {
        let mut keys_buf: Vec<u64> = Vec::with_capacity(batch_size);
        let mut checksum = 0u64;
        for b in 0..batches {
            keys_buf.clear();
            let base = (b * batch_size) as u64;
            for k in 0..batch_size as u64 {
                keys_buf.push(base + k);
            }
            for r in store.get_batch(black_box(&keys_buf)) {
                checksum ^= *r.expect("loaded key");
            }
        }
        checksum
    })
    .print();

    let lane = populate_lane(count as usize);

    measure("dense_load_scalar", count, || {
        let mut checksum = 0u64;
        for i in 0..count as usize {
            checksum ^= lane.load(black_box(i));
        }
        checksum
    })
    .print();

    let indices_seq: Vec<usize> = (0..count as usize).collect();
    let mut out = vec![0u64; indices_seq.len()];
    measure("dense_load_many_seq", count, || {
        lane.load_many(black_box(&indices_seq), &mut out);
        let mut checksum = 0u64;
        for v in &out {
            checksum ^= *v;
        }
        checksum
    })
    .print();

    let stride = 17usize;
    let cap = count as usize;
    let indices_stride: Vec<usize> = (0..cap).map(|k| (k * stride) % cap).collect();
    measure("dense_load_many_stride17", count, || {
        lane.load_many(black_box(&indices_stride), &mut out);
        let mut checksum = 0u64;
        for v in &out {
            checksum ^= *v;
        }
        checksum
    })
    .print();

    println!();
    multithread_bench(count);
}

fn multithread_bench(count: u64) {
    let threads_to_try: &[usize] = &[1, 2, 4, 8];
    let store = Arc::new(populate_store(count));
    let lockfree = Arc::new(populate_lockfree(count));
    let writeback = Arc::new(populate_writeback(count));

    println!("=== core multi-threaded read scaling ===");
    println!("Each thread reads its disjoint slice of the key range.");

    for &t in threads_to_try {
        let per_thread = count / t as u64;
        if per_thread == 0 {
            continue;
        }

        let mut samples_get: Vec<Duration> = Vec::with_capacity(TRIALS);
        let mut samples_read_with: Vec<Duration> = Vec::with_capacity(TRIALS);
        let mut samples_lockfree: Vec<Duration> = Vec::with_capacity(TRIALS);
        let mut samples_writeback: Vec<Duration> = Vec::with_capacity(TRIALS);

        for _ in 0..TRIALS {
            let start = Instant::now();
            thread::scope(|s| {
                for tid in 0..t {
                    let store = Arc::clone(&store);
                    s.spawn(move || {
                        let base = tid as u64 * per_thread;
                        let acc = AtomicU64::new(0);
                        let mut local = 0u64;
                        for i in base..(base + per_thread) {
                            local ^= *store.get(&i).expect("loaded key");
                        }
                        acc.fetch_xor(local, Ordering::Relaxed);
                        black_box(acc.load(Ordering::Relaxed));
                    });
                }
            });
            samples_get.push(start.elapsed());

            let start = Instant::now();
            thread::scope(|s| {
                for tid in 0..t {
                    let store = Arc::clone(&store);
                    s.spawn(move || {
                        let base = tid as u64 * per_thread;
                        let acc = AtomicU64::new(0);
                        let mut local = 0u64;
                        for i in base..(base + per_thread) {
                            local ^= store.read_with(&i, |v| *v).expect("loaded key");
                        }
                        acc.fetch_xor(local, Ordering::Relaxed);
                        black_box(acc.load(Ordering::Relaxed));
                    });
                }
            });
            samples_read_with.push(start.elapsed());

            let start = Instant::now();
            thread::scope(|s| {
                for tid in 0..t {
                    let lockfree = Arc::clone(&lockfree);
                    s.spawn(move || {
                        let base = tid as u64 * per_thread;
                        let p = lockfree.pin();
                        let acc = AtomicU64::new(0);
                        let mut local = 0u64;
                        for i in base..(base + per_thread) {
                            local ^= *p.get(&i).expect("loaded key");
                        }
                        acc.fetch_xor(local, Ordering::Relaxed);
                        black_box(acc.load(Ordering::Relaxed));
                    });
                }
            });
            samples_lockfree.push(start.elapsed());

            let start = Instant::now();
            thread::scope(|s| {
                for tid in 0..t {
                    let writeback = Arc::clone(&writeback);
                    s.spawn(move || {
                        let base = tid as u64 * per_thread;
                        let p = writeback.pin();
                        let acc = AtomicU64::new(0);
                        let mut local = 0u64;
                        for i in base..(base + per_thread) {
                            local ^= p.read_with(&i, |v| *v).expect("loaded key");
                        }
                        acc.fetch_xor(local, Ordering::Relaxed);
                        black_box(acc.load(Ordering::Relaxed));
                    });
                }
            });
            samples_writeback.push(start.elapsed());
        }

        samples_get.sort();
        samples_read_with.sort();
        samples_lockfree.sort();
        samples_writeback.sort();
        let total = per_thread * t as u64;
        let report = |label: &str, dur: Duration| {
            let ns = dur.as_nanos() as f64 / total as f64;
            let ops = total as f64 / dur.as_secs_f64();
            println!("  threads={t:<2} {label:<18} min={ns:.2} ns/op (aggregate {ops:.0} ops/s)");
        };
        report("mfs_get", samples_get[0]);
        report("mfs_read_with", samples_read_with[0]);
        report("lockfree_get", samples_lockfree[0]);
        report("writeback_read_with", samples_writeback[0]);
    }
}
