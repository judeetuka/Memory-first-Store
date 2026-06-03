// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use mfs_core::s3fifo::{S3FifoCache, S3FifoOpDiagnostics};
use rand::prelude::*;
use std::num::NonZeroUsize;
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const ITEMS: usize = 100;

const ITERATIONS: usize = 5_000_000;
const THREADS: usize = 8;
const MFS_S3FIFO_DIAG_ENV: &str = "MFS_S3FIFO_DIAG";
const MFS_S3FIFO_DIAG_SAMPLE_EVERY_ENV: &str = "MFS_S3FIFO_DIAG_SAMPLE_EVERY";
const DEFAULT_MFS_S3FIFO_DIAG_SAMPLE_EVERY: usize = 1024;

/*
cargo bench  --bench bench_perf

Note: the performance number vary a lot on different planform, CPU and CPU arch
Below is from Linux + Ryzen 5 7600 CPU

lru read total 150.423567ms, 30ns avg per operation, 33239472 ops per second
moka read total 462.133322ms, 92ns avg per operation, 10819389 ops per second
quick_cache read total 125.618216ms, 25ns avg per operation, 39803144 ops per second
tinyufo read total 199.007359ms, 39ns avg per operation, 25124698 ops per second
tinyufo compact read total 331.145859ms, 66ns avg per operation, 15099087 ops per second

lru read total 5.402631847s, 1.08µs avg per operation, 925474 ops per second
...
total 6960329 ops per second

moka read total 2.742258211s, 548ns avg per operation, 1823314 ops per second
...
total 14072430 ops per second

quick_cache read total 1.186566627s, 237ns avg per operation, 4213838 ops per second
...
total 33694776 ops per second

tinyufo read total 208.346855ms, 41ns avg per operation, 23998444 ops per second
...
total 148691408 ops per second

tinyufo compact read total 539.403037ms, 107ns avg per operation, 9269507 ops per second
...
total 74130632 ops per second

lru mixed read/write 5.500309876s, 1.1µs avg per operation, 909039 ops per second, 407431 misses
...
total 6846743 ops per second

moka mixed read/write 2.368500882s, 473ns avg per operation, 2111040 ops per second 279324 misses
...
total 16557962 ops per second

quick_cache mixed read/write 838.072588ms, 167ns avg per operation, 5966070 ops per second 315051 misses
...
total 47698472 ops per second

tinyufo mixed read/write 456.134531ms, 91ns avg per operation, 10961678 ops per second, 294977 misses
...
total 80865792 ops per second

tinyufo compact mixed read/write 638.770053ms, 127ns avg per operation, 7827543 ops per second, 294641 misses
...
total 62600844 ops per second
*/

fn main() {
    println!("Note: these performance numbers vary a lot across different CPUs and OSes.");
    // we don't bench eviction here so make the caches large enough to hold all
    let lru = Mutex::new(lru::LruCache::<u64, ()>::unbounded());
    let moka = moka::sync::Cache::new(ITEMS as u64 + 10);
    let quick_cache = quick_cache::sync::Cache::new(ITEMS + 10);
    let tinyufo = tinyufo::TinyUfo::new(ITEMS + 10, 10);
    let tinyufo_compact = tinyufo::TinyUfo::new_compact(ITEMS + 10, 10);
    let mfs_s3fifo = mfs_core::s3fifo::S3FifoCache::<u64, ()>::with_capacity(ITEMS + 10);

    // populate first, then we bench access/promotion
    for i in 0..ITEMS {
        lru.lock().unwrap().put(i as u64, ());
        moka.insert(i as u64, ());
        quick_cache.insert(i as u64, ());
        tinyufo.put(i as u64, (), 1);
        tinyufo_compact.put(i as u64, (), 1);
        mfs_s3fifo.insert(i as u64, ());
    }

    // single thread
    let mut rng = rand::rng();
    let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        lru.lock().unwrap().get(&(zipf.sample(&mut rng) as u64));
    }
    let elapsed = before.elapsed();
    println!(
        "lru read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        moka.get(&(zipf.sample(&mut rng) as u64));
    }
    let elapsed = before.elapsed();
    println!(
        "moka read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        quick_cache.get(&(zipf.sample(&mut rng) as u64));
    }
    let elapsed = before.elapsed();
    println!(
        "quick_cache read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        tinyufo.get(&(zipf.sample(&mut rng) as u64));
    }
    let elapsed = before.elapsed();
    println!(
        "tinyufo read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        tinyufo_compact.get(&(zipf.sample(&mut rng) as u64));
    }
    let elapsed = before.elapsed();
    println!(
        "tinyufo compact read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    let before = Instant::now();
    for _ in 0..ITERATIONS {
        mfs_s3fifo.read_with(&(zipf.sample(&mut rng) as u64), |_| ());
    }
    let elapsed = before.elapsed();
    println!(
        "mfs_s3fifo read total {elapsed:?}, {:?} avg per operation, {} ops per second",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
    );

    // concurrent
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    lru.lock().unwrap().get(&(zipf.sample(&mut rng) as u64));
                }
                let elapsed = before.elapsed();
                println!(
                    "lru read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    mfs_s3fifo.read_with(&(zipf.sample(&mut rng) as u64), |_| ());
                }
                let elapsed = before.elapsed();
                println!(
                    "mfs_s3fifo read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    moka.get(&(zipf.sample(&mut rng) as u64));
                }
                let elapsed = before.elapsed();
                println!(
                    "moka read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    quick_cache.get(&(zipf.sample(&mut rng) as u64));
                }
                let elapsed = before.elapsed();
                println!(
                    "quick_cache read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    tinyufo.get(&(zipf.sample(&mut rng) as u64));
                }
                let elapsed = before.elapsed();
                println!(
                    "tinyufo read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    tinyufo_compact.get(&(zipf.sample(&mut rng) as u64));
                }
                let elapsed = before.elapsed();
                println!(
                    "tinyufo compact read total {elapsed:?}, {:?} avg per operation, {} ops per second",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    ///// bench mixed read and write /////
    const CACHE_SIZE: usize = 1000;
    let items: usize = 10000;
    const ZIPF_EXP: f64 = 1.3;

    let lru = Mutex::new(lru::LruCache::<u64, ()>::new(
        NonZeroUsize::new(CACHE_SIZE).unwrap(),
    ));
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    let mut lru = lru.lock().unwrap();
                    if lru.get(&key).is_none() {
                        lru.put(key, ());
                        miss_count += 1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "lru mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second, {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let moka = moka::sync::Cache::new(CACHE_SIZE as u64);
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if moka.get(&key).is_none() {
                        moka.insert(key, ());
                        miss_count += 1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "moka mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let quick_cache = quick_cache::sync::Cache::new(CACHE_SIZE);
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if quick_cache.get(&key).is_none() {
                        quick_cache.insert(key, ());
                        miss_count += 1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "quick_cache mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let tinyufo = tinyufo::TinyUfo::new(CACHE_SIZE, CACHE_SIZE);
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if tinyufo.get(&key).is_none() {
                        tinyufo.put(key, (), 1);
                        miss_count +=1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "tinyufo mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second, {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32,
                );
            });
        }
    });

    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let tinyufo_compact = tinyufo::TinyUfo::new(CACHE_SIZE, CACHE_SIZE);
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if tinyufo_compact.get(&key).is_none() {
                        tinyufo_compact.put(key, (), 1);
                        miss_count +=1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "tinyufo compact mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second, {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32,
                );
            });
        }
    });

    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    let mfs_s3fifo = mfs_core::s3fifo::S3FifoCache::<u64, ()>::with_capacity(CACHE_SIZE);
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut miss_count = 0;
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                let before = Instant::now();
                for _ in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if mfs_s3fifo.read_with(&key, |_| ()).is_none() {
                        mfs_s3fifo.insert(key, ());
                        miss_count += 1;
                    }
                }
                let elapsed = before.elapsed();
                println!(
                    "mfs_s3fifo mixed read/write {elapsed:?}, {:?} avg per operation, {} ops per second, {miss_count} misses",
                    elapsed / ITERATIONS as u32,
                    (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32,
                );
            });
        }
    });
    let elapsed = before.elapsed();
    println!(
        "total {} ops per second",
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32
    );

    if s3fifo_diag_enabled() {
        run_s3fifo_diagnostics();
    }
}

#[derive(Clone, Copy, Default)]
struct S3FifoBenchDiagnostics {
    read_samples: usize,
    mixed_samples: usize,
    insert_miss_samples: usize,
    miss_count: usize,
    read_miss_samples: usize,
    total_read_op: Duration,
    total_mixed_iteration: Duration,
    public_read_with: Duration,
    public_insert_on_miss: Duration,
    internal: S3FifoOpDiagnostics,
}

impl S3FifoBenchDiagnostics {
    fn add_assign(&mut self, other: Self) {
        self.read_samples += other.read_samples;
        self.mixed_samples += other.mixed_samples;
        self.insert_miss_samples += other.insert_miss_samples;
        self.miss_count += other.miss_count;
        self.read_miss_samples += other.read_miss_samples;
        self.total_read_op += other.total_read_op;
        self.total_mixed_iteration += other.total_mixed_iteration;
        self.public_read_with += other.public_read_with;
        self.public_insert_on_miss += other.public_insert_on_miss;
        self.internal.add_assign(other.internal);
    }
}

fn s3fifo_diag_enabled() -> bool {
    matches!(std::env::var(MFS_S3FIFO_DIAG_ENV).as_deref(), Ok("1"))
}

fn s3fifo_diag_sample_every() -> usize {
    std::env::var(MFS_S3FIFO_DIAG_SAMPLE_EVERY_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MFS_S3FIFO_DIAG_SAMPLE_EVERY)
}

fn should_sample(iteration: usize, sample_every: usize) -> bool {
    iteration.is_multiple_of(sample_every)
}

fn run_s3fifo_diagnostics() {
    let sample_every = s3fifo_diag_sample_every();
    run_s3fifo_read_diagnostic(sample_every);
    run_s3fifo_mixed_diagnostic(sample_every);
}

fn run_s3fifo_read_diagnostic(sample_every: usize) {
    let mfs_s3fifo = S3FifoCache::<u64, ()>::with_capacity(ITEMS + 10);
    for i in 0..ITEMS {
        mfs_s3fifo.insert(i as u64, ());
    }

    let mut rng = rand::rng();
    let zipf = rand_distr::Zipf::new(ITEMS as f64, 1.03).unwrap();
    let mut totals = S3FifoBenchDiagnostics::default();
    let before = Instant::now();
    for iteration in 0..ITERATIONS {
        let key = zipf.sample(&mut rng) as u64;
        if should_sample(iteration, sample_every) {
            let op_start = Instant::now();
            let public_start = Instant::now();
            let read = mfs_s3fifo.read_with_diagnostics(&key, |_| ());
            totals.public_read_with += public_start.elapsed();
            totals.total_read_op += op_start.elapsed();
            totals.read_samples += 1;
            if read.result.is_none() {
                totals.read_miss_samples += 1;
            }
            totals.internal.add_assign(read.metrics);
        } else {
            mfs_s3fifo.read_with(&key, |_| ());
        }
    }
    let elapsed = before.elapsed();
    println!(
        "diag_mfs_s3fifo_default_read_instrumented total {elapsed:?}, {:?} avg per operation, {} ops per second, sample_every {}, read_samples {}, read_miss_samples {}, total_read_op_ns {}, public_read_with_ns {}, rwlock_read_acquire_ns {}, rwlock_write_acquire_ns {}, map_lookup_read_closure_ns {}, fifo_maintenance_ns {}, ghost_bookkeeping_ns {}, admission_bookkeeping_ns {}, top_cost={}",
        elapsed / ITERATIONS as u32,
        (ITERATIONS as f32 / elapsed.as_secs_f32()) as u32,
        sample_every,
        totals.read_samples,
        totals.read_miss_samples,
        nanos(totals.total_read_op),
        nanos(totals.public_read_with),
        nanos(totals.internal.rwlock_read_acquire),
        nanos(totals.internal.rwlock_write_acquire),
        nanos(totals.internal.map_lookup_read_closure),
        nanos(totals.internal.fifo_maintenance),
        nanos(totals.internal.ghost_bookkeeping),
        nanos(totals.internal.admission_bookkeeping),
        top_cost(&totals, totals.public_read_with),
    );
}

fn run_s3fifo_mixed_diagnostic(sample_every: usize) {
    const CACHE_SIZE: usize = 1000;
    let items: usize = 10000;
    const ZIPF_EXP: f64 = 1.3;

    let mfs_s3fifo = S3FifoCache::<u64, ()>::with_capacity(CACHE_SIZE);
    let totals = Mutex::new(S3FifoBenchDiagnostics::default());
    let wg = Barrier::new(THREADS);
    let before = Instant::now();
    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                let mut local = S3FifoBenchDiagnostics::default();
                let mut rng = rand::rng();
                let zipf = rand_distr::Zipf::new(items as f64, ZIPF_EXP).unwrap();
                wg.wait();
                for iteration in 0..ITERATIONS {
                    let key = zipf.sample(&mut rng) as u64;
                    if should_sample(iteration, sample_every) {
                        let iteration_start = Instant::now();
                        let public_read_start = Instant::now();
                        let read = mfs_s3fifo.read_with_diagnostics(&key, |_| ());
                        local.public_read_with += public_read_start.elapsed();
                        local.read_samples += 1;
                        local.internal.add_assign(read.metrics);

                        if read.result.is_none() {
                            let public_insert_start = Instant::now();
                            let insert = mfs_s3fifo.insert_diagnostics(key, ());
                            local.public_insert_on_miss += public_insert_start.elapsed();
                            local.insert_miss_samples += 1;
                            local.miss_count += 1;
                            local.internal.add_assign(insert.metrics);
                        }

                        local.total_mixed_iteration += iteration_start.elapsed();
                        local.mixed_samples += 1;
                    } else if mfs_s3fifo.read_with(&key, |_| ()).is_none() {
                        mfs_s3fifo.insert(key, ());
                        local.miss_count += 1;
                    }
                }
                totals.lock().unwrap().add_assign(local);
            });
        }
    });
    let elapsed = before.elapsed();
    let totals = *totals.lock().unwrap();
    println!(
        "diag_mfs_s3fifo_default_mixed_instrumented total {elapsed:?}, {:?} avg per operation, {} ops per second, {} misses, sample_every {}, mixed_samples {}, read_samples {}, insert_miss_samples {}, total_mixed_iteration_ns {}, public_read_with_ns {}, public_insert_on_miss_ns {}, rwlock_read_acquire_ns {}, rwlock_write_acquire_ns {}, map_lookup_read_closure_ns {}, fifo_maintenance_ns {}, ghost_bookkeeping_ns {}, admission_bookkeeping_ns {}, top_cost={}",
        elapsed / (ITERATIONS * THREADS) as u32,
        (ITERATIONS as f32 * THREADS as f32 / elapsed.as_secs_f32()) as u32,
        totals.miss_count,
        sample_every,
        totals.mixed_samples,
        totals.read_samples,
        totals.insert_miss_samples,
        nanos(totals.total_mixed_iteration),
        nanos(totals.public_read_with),
        nanos(totals.public_insert_on_miss),
        nanos(totals.internal.rwlock_read_acquire),
        nanos(totals.internal.rwlock_write_acquire),
        nanos(totals.internal.map_lookup_read_closure),
        nanos(totals.internal.fifo_maintenance),
        nanos(totals.internal.ghost_bookkeeping),
        nanos(totals.internal.admission_bookkeeping),
        top_cost(
            &totals,
            totals.public_read_with + totals.public_insert_on_miss
        ),
    );
}

fn nanos(duration: Duration) -> u128 {
    duration.as_nanos()
}

fn top_cost(totals: &S3FifoBenchDiagnostics, public_total: Duration) -> &'static str {
    let buckets = [
        (
            "rwlock_read_acquire_ns",
            totals.internal.rwlock_read_acquire,
        ),
        (
            "rwlock_write_acquire_ns",
            totals.internal.rwlock_write_acquire,
        ),
        (
            "map_lookup_read_closure_ns",
            totals.internal.map_lookup_read_closure,
        ),
        ("fifo_maintenance_ns", totals.internal.fifo_maintenance),
        ("ghost_bookkeeping_ns", totals.internal.ghost_bookkeeping),
        (
            "admission_bookkeeping_ns",
            totals.internal.admission_bookkeeping,
        ),
    ];
    let bucket_total = buckets
        .iter()
        .map(|(_, duration)| duration.as_nanos())
        .sum::<u128>();
    let public_total = public_total.as_nanos();
    let Some((name, duration)) = buckets
        .iter()
        .max_by_key(|(_, duration)| duration.as_nanos())
    else {
        return "inconclusive";
    };
    let top = duration.as_nanos();
    if public_total == 0
        || bucket_total == 0
        || bucket_total > public_total.saturating_mul(2)
        || top.saturating_mul(100) < bucket_total.saturating_mul(35)
    {
        "inconclusive"
    } else {
        name
    }
}
