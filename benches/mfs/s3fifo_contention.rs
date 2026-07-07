use base64::engine::general_purpose::STANDARD;
use base64::write::EncoderWriter;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::{Histogram, SyncHistogram};
use rand::prelude::*;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

fn main() {
    // ── env-config knobs ──────────────────────────────────────────
    let threads: usize = std::env::var("MFS_CONTENTION_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let read_pct: u32 = std::env::var("MFS_CONTENTION_READ_PCT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80);
    let ops: usize = std::env::var("MFS_CONTENTION_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let keys: usize = std::env::var("MFS_CONTENTION_KEYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let zipf_alpha: f64 = std::env::var("MFS_CONTENTION_ZIPF")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1.10);

    let capacity = keys;
    let pre_populate = (capacity as f64 * 0.8) as usize;
    let ops_per_thread = ops / threads;

    println!(
        "=== MfS S3FIFO Contention Benchmark ===\n\
         threads={threads}  read_pct={read_pct}%  ops={ops}  keys={keys}  zipf_α={zipf_alpha}\n\
         capacity={capacity}  pre_populate={pre_populate}  ops_per_thread={ops_per_thread}\n"
    );

    // ── mfs_s3fifo ────────────────────────────────────────────────
    let (mfs_stats, mfs_hist) = {
        let cache =
            mfs_core::s3fifo::S3FifoCache::<u64, ()>::with_capacity(capacity);
        for i in 0..pre_populate as u64 {
            cache.insert(i, ());
        }
        run_contention(
            "mfs_s3fifo",
            &cache,
            threads,
            read_pct,
            ops_per_thread,
            keys as u64,
            zipf_alpha,
        )
    };

    // ── quick_cache ───────────────────────────────────────────────
    let (qc_stats, qc_hist) = {
        let cache = quick_cache::sync::Cache::new(capacity);
        for i in 0..pre_populate as u64 {
            cache.insert(i, ());
        }
        run_contention(
            "quick_cache",
            &cache,
            threads,
            read_pct,
            ops_per_thread,
            keys as u64,
            zipf_alpha,
        )
    };

    // ── comparison ────────────────────────────────────────────────
    println!(
        "{:<18} {:>6} {:>12} {:>10} {:>10} {:>10} {:>10}",
        "cache", "threads", "M ops/s", "reads", "writes", "misses", "hit_rate%"
    );
    mfs_stats.print_aggregate("mfs_s3fifo");
    qc_stats.print_aggregate("quick_cache");
    println!();

    println!("latency        p50        p99        p999       max");
    print_latency_row("mfs_s3fifo", &mfs_hist);
    print_latency_row("quick_cache", &qc_hist);

    // ── write .hist files ─────────────────────────────────────────
    std::fs::create_dir_all("benches/mfs").ok();
    write_hist("benches/mfs/s3fifo_contention_mfs.hist", &mfs_hist);
    write_hist("benches/mfs/s3fifo_contention_quick_cache.hist", &qc_hist);
}

// ── helpers ────────────────────────────────────────────────────────

#[inline]
fn should_read(rng: &mut u64, read_pct: u32) -> bool {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    (*rng as u32 % 100) < read_pct
}

#[derive(Default)]
struct AggregatedStats {
    reads: AtomicU64,
    writes: AtomicU64,
    misses: AtomicU64,
}

struct RunResult {
    stats: AggregatedStats,
    elapsed: std::time::Duration,
    threads: usize,
    ops_per_thread: usize,
}

impl RunResult {
    fn print_aggregate(&self, label: &str) {
        let total_ops = self.threads * self.ops_per_thread;
        let reads = self.stats.reads.load(Ordering::Relaxed);
        let writes = self.stats.writes.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let hits = reads.saturating_sub(misses);
        let hit_rate = if reads > 0 {
            100.0 * hits as f64 / reads as f64
        } else {
            0.0
        };
        let mops = total_ops as f64 / self.elapsed.as_secs_f64() / 1_000_000.0;
        println!(
            "{:<18} {:>6} {:>12.2} {:>10} {:>10} {:>10} {:>9.2}",
            label, self.threads, mops, reads, writes, misses, hit_rate
        );
    }
}

fn print_latency_row(label: &str, hist: &Histogram<u64>) {
    println!(
        "{:<14} {:>8}ns {:>8}ns {:>8}ns {:>8}ns",
        label,
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.max(),
    );
}

fn write_hist(path: &str, hist: &Histogram<u64>) {
    if let Ok(mut f) = File::create(path) {
        let mut s = V2DeflateSerializer::new();
        let _ = s.serialize(hist, &mut EncoderWriter::new(&mut f, &STANDARD));
    }
}

// ── multi-threaded runner ──────────────────────────────────────────
//
// Uses Barrier + thread::scope (matching bench_perf.rs:511-542).
// Latency is sampled every 1024th op to keep HDR overhead low.

fn run_contention<C>(
    label: &str,
    cache: &C,
    threads: usize,
    read_pct: u32,
    ops_per_thread: usize,
    key_support: u64,
    zipf_alpha: f64,
) -> (RunResult, Histogram<u64>)
where
    C: ContentionCache + Sync,
{
    let stats = AggregatedStats::default();
    let mut sync_hist =
        SyncHistogram::<u64>::from(Histogram::new(2).unwrap());
    let barrier = Barrier::new(threads);

    let before = Instant::now();
    let reads = &stats.reads;
    let writes = &stats.writes;
    let misses = &stats.misses;

    thread::scope(|s| {
        for thread_idx in 0..threads {
            let barrier = &barrier;
            let mut recorder = sync_hist.recorder();
            let mut seed = fast_seed(thread_idx);
            s.spawn(move || {
                let mut rng = rand::rng();
                let zipf =
                    rand_distr::Zipf::new(key_support as f64, zipf_alpha)
                        .unwrap();
                barrier.wait();

                let mut local_reads = 0u64;
                let mut local_writes = 0u64;
                let mut local_misses = 0u64;

                for op_idx in 0..ops_per_thread {
                    let key = zipf.sample(&mut rng) as u64;
                    let is_read = should_read(&mut seed, read_pct);

                    if is_read {
                        local_reads += 1;
                        let sample = op_idx & 1023 == 0;
                        let start = if sample {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        let hit = cache.cache_read(&key);
                        if hit.is_none() {
                            local_misses += 1;
                        }
                        if let Some(t0) = start {
                            recorder
                                .record(t0.elapsed().as_nanos() as u64)
                                .ok();
                        }
                    } else {
                        local_writes += 1;
                        let sample = op_idx & 1023 == 0;
                        let start = if sample {
                            Some(Instant::now())
                        } else {
                            None
                        };
                        cache.cache_write(key, ());
                        if let Some(t0) = start {
                            recorder
                                .record(t0.elapsed().as_nanos() as u64)
                                .ok();
                        }
                    }
                }

                reads.fetch_add(local_reads, Ordering::Relaxed);
                writes.fetch_add(local_writes, Ordering::Relaxed);
                misses.fetch_add(local_misses, Ordering::Relaxed);

                let elapsed = before.elapsed();
                let mops = (local_reads + local_writes) as f64
                    / elapsed.as_secs_f64()
                    / 1_000_000.0;
                println!(
                    "  {label:>14} thread={thread_idx:>2}  \
                     {mops:>8.2} M ops/s  \
                     (r={local_reads} w={local_writes} m={local_misses})"
                );
            });
        }
    });
    let elapsed = before.elapsed();

    sync_hist.refresh();
    let hist = (*sync_hist).clone();

    (
        RunResult {
            stats,
            elapsed,
            threads,
            ops_per_thread,
        },
        hist,
    )
}

#[inline]
fn fast_seed(thread_idx: usize) -> u64 {
    (thread_idx as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xDEAD_BEEF
}

// ── trait to abstract over S3FIFO and quick_cache for the same
//    harness loop ───────────────────────────────────────────────────

trait ContentionCache {
    fn cache_read(&self, key: &u64) -> Option<()>;
    fn cache_write(&self, key: u64, value: ());
}

impl ContentionCache for mfs_core::s3fifo::S3FifoCache<u64, ()> {
    #[inline]
    fn cache_read(&self, key: &u64) -> Option<()> {
        self.read_with(key, |_| ())
    }

    #[inline]
    fn cache_write(&self, key: u64, value: ()) {
        self.insert(key, value);
    }
}

impl ContentionCache for quick_cache::sync::Cache<u64, ()> {
    #[inline]
    fn cache_read(&self, key: &u64) -> Option<()> {
        self.get(key).map(|_| ())
    }

    #[inline]
    fn cache_write(&self, key: u64, value: ()) {
        self.insert(key, value);
    }
}
