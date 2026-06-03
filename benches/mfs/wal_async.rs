use mfs_core::durability::{
    AsyncWalBackend, AsyncWalConfig, GroupCommitWalBackend, GroupCommitWalConfig, U64Codec,
    WalBackend, WalConfig,
};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BATCHES: usize = 128;
const RECORDS_PER_BATCH: usize = 64;
const PRODUCERS: usize = 4;

struct CaseStats {
    label: &'static str,
    flush_p50: Duration,
    flush_p99: Duration,
    flush_max: Duration,
    enqueue_total: Duration,
    durable_total: Duration,
    records: usize,
}

impl CaseStats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64;
        let mrec_s = self.records as f64 / self.durable_total.as_secs_f64() / 1_000_000.0;
        println!(
            "{:<28} flush_p50={:>9.0}ns flush_p99={:>9.0}ns flush_max={:>9.0}ns enqueue_total={:>7.2}ms durable_total={:>7.2}ms durable_rate={:>7.2} Mrec/s",
            self.label,
            ns(self.flush_p50),
            ns(self.flush_p99),
            ns(self.flush_max),
            self.enqueue_total.as_secs_f64() * 1e3,
            self.durable_total.as_secs_f64() * 1e3,
            mrec_s,
        );
    }
}

fn main() {
    let batches = build_batches();
    let total_records = BATCHES * RECORDS_PER_BATCH;
    println!(
        "=== WAL async-vs-direct (batches={BATCHES}, records/batch={RECORDS_PER_BATCH}, total_records={total_records}) ==="
    );
    println!(
        "flush_* is caller-visible FlushBackend::flush latency. durable_total includes final sync/shutdown."
    );

    let default_cfg = WalConfig::default();
    let sync_at_end_cfg = WalConfig {
        sync_threshold_bytes: usize::MAX / 4,
        sync_threshold_records: usize::MAX / 4,
        buffer_capacity_bytes: 256 * 1024,
    };

    bench_direct("direct_default", &batches, default_cfg).print();
    bench_async("async_default", &batches, default_cfg).print();
    bench_group_commit("group_commit_default", &batches, default_cfg).print();
    bench_direct("direct_sync_at_end", &batches, sync_at_end_cfg).print();
    bench_async("async_sync_at_end", &batches, sync_at_end_cfg).print();
    bench_group_commit("group_commit_sync_each", &batches, sync_at_end_cfg).print();
    bench_direct_multi_producer("direct_4p_sync_at_end", sync_at_end_cfg).print();
    bench_async_multi_producer("async_4p_sync_at_end", sync_at_end_cfg).print();
    bench_group_commit_multi_producer("group_commit_4p", sync_at_end_cfg).print();
}

fn build_batches() -> Vec<Vec<FlushRecord<u64, u64>>> {
    let mut batches = Vec::with_capacity(BATCHES);
    let mut version = 1u64;
    for b in 0..BATCHES {
        let mut records = Vec::with_capacity(RECORDS_PER_BATCH);
        for r in 0..RECORDS_PER_BATCH {
            let key = (b * RECORDS_PER_BATCH + r) as u64;
            records.push(FlushRecord {
                key,
                value: Some(Arc::new(key.wrapping_mul(7))),
                version,
                op: Operation::Put,
            });
            version += 1;
        }
        batches.push(records);
    }
    batches
}

fn build_batch(global_batch: usize) -> Vec<FlushRecord<u64, u64>> {
    let mut records = Vec::with_capacity(RECORDS_PER_BATCH);
    let base = global_batch * RECORDS_PER_BATCH;
    for r in 0..RECORDS_PER_BATCH {
        let key = (base + r) as u64;
        records.push(FlushRecord {
            key,
            value: Some(Arc::new(key.wrapping_mul(7))),
            version: key + 1,
            op: Operation::Put,
        });
    }
    records
}

fn bench_direct(
    label: &'static str,
    batches: &[Vec<FlushRecord<u64, u64>>],
    cfg: WalConfig,
) -> CaseStats {
    let path = tmp_path(label);
    let mut wal = WalBackend::open(&path, U64Codec, cfg).expect("open direct wal");
    let start_all = Instant::now();
    let mut samples = Vec::with_capacity(BATCHES);
    for batch in batches {
        let start = Instant::now();
        wal.flush(batch).expect("direct flush");
        samples.push(start.elapsed());
    }
    let enqueue_total = start_all.elapsed();
    wal.sync_now().expect("direct final sync");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        BATCHES * RECORDS_PER_BATCH,
    )
}

fn bench_async(
    label: &'static str,
    batches: &[Vec<FlushRecord<u64, u64>>],
    cfg: WalConfig,
) -> CaseStats {
    let path = tmp_path(label);
    let mut wal = AsyncWalBackend::open(
        &path,
        U64Codec,
        cfg,
        AsyncWalConfig {
            queue_capacity: BATCHES,
        },
    )
    .expect("open async wal");
    let start_all = Instant::now();
    let mut samples = Vec::with_capacity(BATCHES);
    for batch in batches {
        let start = Instant::now();
        wal.flush(batch).expect("async enqueue");
        samples.push(start.elapsed());
    }
    let enqueue_total = start_all.elapsed();
    wal.sync_barrier().expect("async sync barrier");
    wal.shutdown().expect("async shutdown");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        BATCHES * RECORDS_PER_BATCH,
    )
}

fn bench_group_commit(
    label: &'static str,
    batches: &[Vec<FlushRecord<u64, u64>>],
    cfg: WalConfig,
) -> CaseStats {
    let path = tmp_path(label);
    let wal = GroupCommitWalBackend::open(
        &path,
        U64Codec,
        cfg,
        GroupCommitWalConfig {
            queue_capacity: BATCHES,
            max_group_records: BATCHES * RECORDS_PER_BATCH,
        },
    )
    .expect("open group commit wal");
    let mut handle = wal.handle();
    let start_all = Instant::now();
    let mut samples = Vec::with_capacity(BATCHES);
    for batch in batches {
        let start = Instant::now();
        handle.flush(batch).expect("group commit flush");
        samples.push(start.elapsed());
    }
    let enqueue_total = start_all.elapsed();
    wal.sync_barrier().expect("group sync barrier");
    wal.shutdown().expect("group shutdown");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        BATCHES * RECORDS_PER_BATCH,
    )
}

fn bench_direct_multi_producer(label: &'static str, cfg: WalConfig) -> CaseStats {
    let path = tmp_path(label);
    let wal = Arc::new(Mutex::new(
        WalBackend::open(&path, U64Codec, cfg).expect("open direct wal"),
    ));
    let samples = Arc::new(Mutex::new(Vec::with_capacity(PRODUCERS * BATCHES)));
    let start_all = Instant::now();
    thread::scope(|s| {
        for producer in 0..PRODUCERS {
            let wal = Arc::clone(&wal);
            let samples = Arc::clone(&samples);
            s.spawn(move || {
                for b in 0..BATCHES {
                    let batch = build_batch(producer * BATCHES + b);
                    let start = Instant::now();
                    wal.lock()
                        .expect("direct wal mutex")
                        .flush(&batch)
                        .expect("direct flush");
                    samples.lock().expect("samples mutex").push(start.elapsed());
                }
            });
        }
    });
    let enqueue_total = start_all.elapsed();
    wal.lock()
        .expect("direct wal mutex")
        .sync_now()
        .expect("direct final sync");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    let samples = Arc::try_unwrap(samples)
        .expect("samples still shared")
        .into_inner()
        .expect("samples mutex");
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        PRODUCERS * BATCHES * RECORDS_PER_BATCH,
    )
}

fn bench_async_multi_producer(label: &'static str, cfg: WalConfig) -> CaseStats {
    let path = tmp_path(label);
    let wal = Arc::new(
        AsyncWalBackend::open(
            &path,
            U64Codec,
            cfg,
            AsyncWalConfig {
                queue_capacity: PRODUCERS * BATCHES,
            },
        )
        .expect("open async wal"),
    );
    let samples = Arc::new(Mutex::new(Vec::with_capacity(PRODUCERS * BATCHES)));
    let start_all = Instant::now();
    thread::scope(|s| {
        for producer in 0..PRODUCERS {
            let wal = Arc::clone(&wal);
            let samples = Arc::clone(&samples);
            s.spawn(move || {
                for b in 0..BATCHES {
                    let batch = build_batch(producer * BATCHES + b);
                    let start = Instant::now();
                    wal.enqueue(&batch).expect("async enqueue");
                    samples.lock().expect("samples mutex").push(start.elapsed());
                }
            });
        }
    });
    let enqueue_total = start_all.elapsed();
    wal.sync_barrier().expect("async sync barrier");
    let wal = match Arc::try_unwrap(wal) {
        Ok(wal) => wal,
        Err(_) => panic!("async wal still shared"),
    };
    wal.shutdown().expect("async shutdown");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    let samples = Arc::try_unwrap(samples)
        .expect("samples still shared")
        .into_inner()
        .expect("samples mutex");
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        PRODUCERS * BATCHES * RECORDS_PER_BATCH,
    )
}

fn bench_group_commit_multi_producer(label: &'static str, cfg: WalConfig) -> CaseStats {
    let path = tmp_path(label);
    let wal = GroupCommitWalBackend::open(
        &path,
        U64Codec,
        cfg,
        GroupCommitWalConfig {
            queue_capacity: PRODUCERS * BATCHES,
            max_group_records: PRODUCERS * RECORDS_PER_BATCH,
        },
    )
    .expect("open group commit wal");
    let handle = wal.handle();
    let samples = Arc::new(Mutex::new(Vec::with_capacity(PRODUCERS * BATCHES)));
    let start_all = Instant::now();
    thread::scope(|s| {
        for producer in 0..PRODUCERS {
            let mut handle = handle.clone();
            let samples = Arc::clone(&samples);
            s.spawn(move || {
                for b in 0..BATCHES {
                    let batch = build_batch(producer * BATCHES + b);
                    let start = Instant::now();
                    handle.flush(&batch).expect("group commit flush");
                    samples.lock().expect("samples mutex").push(start.elapsed());
                }
            });
        }
    });
    let enqueue_total = start_all.elapsed();
    wal.sync_barrier().expect("group sync barrier");
    wal.shutdown().expect("group shutdown");
    let durable_total = start_all.elapsed();
    std::fs::remove_file(&path).ok();
    let samples = Arc::try_unwrap(samples)
        .expect("samples still shared")
        .into_inner()
        .expect("samples mutex");
    stats(
        label,
        samples,
        enqueue_total,
        durable_total,
        PRODUCERS * BATCHES * RECORDS_PER_BATCH,
    )
}

fn stats(
    label: &'static str,
    mut samples: Vec<Duration>,
    enqueue_total: Duration,
    durable_total: Duration,
    records: usize,
) -> CaseStats {
    samples.sort_unstable();
    let p = |q: f64| -> Duration {
        let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };
    CaseStats {
        label,
        flush_p50: p(0.50),
        flush_p99: p(0.99),
        flush_max: *samples.last().unwrap(),
        enqueue_total,
        durable_total,
        records,
    }
}

fn tmp_path(label: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("mfs_wal_async_bench_{label}_{pid}_{ts}.log"));
    p
}
