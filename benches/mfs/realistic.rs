//! Realistic-workload benchmark.
//!
//! Simulates a typical Redis-replacement use case: structured records,
//! a mixed read/write/delete operation mix with skewed key access,
//! multiple concurrent worker threads, and a background flusher
//! draining dirty records to a mock "database" backend at a fixed
//! cadence.
//!
//! ## Workload knobs (env vars)
//!
//! - `MFS_DURATION_SECS`     wall-clock seconds per run (default 5)
//! - `MFS_RUNS`              number of independent timed runs (default 1).
//!   When >1, each run rewarms the cache, measures independently, and
//!   a distribution summary (min/median/p95/max/stddev) is printed.
//! - `MFS_THREADS`           worker threads (default = num CPUs, capped at 8)
//! - `MFS_KEYS`              total key universe size (default 100_000)
//! - `MFS_VALUE_BYTES`       size of the metadata blob in each value (default 128)
//! - `MFS_READ_PCT`          read percentage (default 95)
//! - `MFS_WRITE_PCT`         write percentage (default 4) — delete = 100 - read - write
//! - `MFS_HOT_PCT`           % of accesses that target the "hot" 20% of keys (default 80)
//! - `MFS_FLUSH_INTERVAL_MS` flusher tick interval in ms (default 10)
//! - `MFS_SAMPLE_RATE`       1-in-N sampling rate for latency timing (default 64)
//! - `MFS_WAL_PATH`          base path for WAL-backed flushes (unset = counting backend)
//! - `MFS_WAL_MODE`          `direct`, `async`, or `group` (default async when WAL is enabled)
//! - `MFS_WAL_KEEP`          keep WAL files after replay validation (default 0)

use mfs_core::durability::{
    AsyncWalBackend, AsyncWalConfig, GroupCommitWalBackend, GroupCommitWalConfig, WalBackend,
    WalCodec, WalConfig,
};
use mfs_core::writeback::{AutoFlusher, AutoFlusherConfig, WriteBehindCache};
use mfs_core::{FlushBackend, FlushRecord, auto_thread_count};
use std::env;
use std::hint::black_box;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Two flusher backends are available in this bench:
///
/// - [`FlusherMode::Single`] (default) — one background thread on a
///   fixed tick, the v2 baseline. Best on CPU-constrained machines
///   (T460 has only 4 logical cores, so adding 8+ flusher threads
///   oversubscribes the scheduler and regresses overall throughput).
/// - [`FlusherMode::Auto`] — [`AutoFlusher`] with per-shard threads
///   and adaptive ticks. Best on machines where worker + flusher
///   threads fit comfortably (Beelink Ser 5 Pro: 16 logical / 4
///   workers / N flushers).
///
/// Switch with `MFS_AUTO_FLUSHER=1`.
#[derive(Debug, Clone, Copy)]
enum FlusherMode {
    Single,
    Auto,
}

#[allow(dead_code)]
#[derive(Clone)]
struct UserProfile {
    id: u64,
    name: String,
    email: String,
    balance_cents: u64,
    last_login_unix: u64,
    metadata: Vec<u8>,
}

fn make_profile(id: u64, value_bytes: usize) -> UserProfile {
    UserProfile {
        id,
        name: format!("user_{:08x}", id),
        email: format!("user_{:08x}@example.com", id),
        balance_cents: (id.wrapping_mul(13)) % 1_000_000,
        last_login_unix: 1_700_000_000 + (id % 86400),
        metadata: (0..value_bytes)
            .map(|i| ((id ^ i as u64) & 0xff) as u8)
            .collect(),
    }
}

fn approx_size(p: &UserProfile) -> usize {
    32 + p.name.len() + p.email.len() + 16 + p.metadata.len()
}

#[derive(Clone, Copy)]
struct UserProfileCodec;

impl WalCodec<u64, UserProfile> for UserProfileCodec {
    fn encode_key(&self, key: &u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&key.to_le_bytes());
    }

    fn encode_value(&self, value: &UserProfile, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.id.to_le_bytes());
        encode_bytes(value.name.as_bytes(), out);
        encode_bytes(value.email.as_bytes(), out);
        out.extend_from_slice(&value.balance_cents.to_le_bytes());
        out.extend_from_slice(&value.last_login_unix.to_le_bytes());
        encode_bytes(&value.metadata, out);
    }

    fn decode_key(&self, bytes: &[u8]) -> io::Result<u64> {
        if bytes.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "u64 key must be 8 bytes",
            ));
        }
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }

    fn decode_value(&self, bytes: &[u8]) -> io::Result<UserProfile> {
        let mut cursor = 0usize;
        let id = read_u64(bytes, &mut cursor)?;
        let name = String::from_utf8(read_bytes(bytes, &mut cursor)?.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let email = String::from_utf8(read_bytes(bytes, &mut cursor)?.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let balance_cents = read_u64(bytes, &mut cursor)?;
        let last_login_unix = read_u64(bytes, &mut cursor)?;
        let metadata = read_bytes(bytes, &mut cursor)?.to_vec();
        if cursor != bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing UserProfile bytes",
            ));
        }
        Ok(UserProfile {
            id,
            name,
            email,
            balance_cents,
            last_login_unix,
            metadata,
        })
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    let len = u32::try_from(bytes.len()).expect("profile field too large");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if bytes.len().saturating_sub(*cursor) < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "u64 field truncated",
        ));
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().expect("8 bytes"));
    *cursor += 8;
    Ok(value)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> io::Result<&'a [u8]> {
    if bytes.len().saturating_sub(*cursor) < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "length field truncated",
        ));
    }
    let len = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().expect("4 bytes")) as usize;
    *cursor += 4;
    if bytes.len().saturating_sub(*cursor) < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "byte field truncated",
        ));
    }
    let out = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

struct XorShift(u64);
impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    #[inline]
    fn next_pct(&mut self) -> u32 {
        (self.next_u64() % 100) as u32
    }
}

#[inline]
fn pick_key(rng: &mut XorShift, total: u64, hot_pct: u32) -> u64 {
    if rng.next_pct() < hot_pct {
        let hot_count = (total / 5).max(1);
        rng.next_u64() % hot_count
    } else {
        rng.next_u64() % total
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalMode {
    Counting,
    Direct,
    Async,
    Group,
}

impl WalMode {
    fn from_env(has_path: bool) -> Self {
        if !has_path {
            return Self::Counting;
        }
        match env::var("MFS_WAL_MODE")
            .unwrap_or_else(|_| "async".to_string())
            .as_str()
        {
            "direct" => Self::Direct,
            "group" => Self::Group,
            "async" => Self::Async,
            _ => Self::Async,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Counting => "counting",
            Self::Direct => "wal_direct",
            Self::Async => "wal_async",
            Self::Group => "wal_group",
        }
    }
}

#[derive(Clone)]
struct WalSettings {
    mode: WalMode,
    base_path: Option<PathBuf>,
    wal_config: WalConfig,
    async_config: AsyncWalConfig,
    group_config: GroupCommitWalConfig,
    keep_files: bool,
    paths: Arc<std::sync::Mutex<Vec<PathBuf>>>,
}

impl WalSettings {
    fn from_env() -> Self {
        let base_path = env::var("MFS_WAL_PATH").ok().map(PathBuf::from);
        let mode = WalMode::from_env(base_path.is_some());
        let wal_config = WalConfig {
            sync_threshold_bytes: env_u64(
                "MFS_WAL_SYNC_BYTES",
                WalConfig::default().sync_threshold_bytes as u64,
            ) as usize,
            sync_threshold_records: env_u64(
                "MFS_WAL_SYNC_RECORDS",
                WalConfig::default().sync_threshold_records as u64,
            ) as usize,
            buffer_capacity_bytes: env_u64(
                "MFS_WAL_BUFFER_BYTES",
                WalConfig::default().buffer_capacity_bytes as u64,
            ) as usize,
        };
        let async_config = AsyncWalConfig {
            queue_capacity: env_u64(
                "MFS_WAL_QUEUE_CAPACITY",
                AsyncWalConfig::default().queue_capacity as u64,
            ) as usize,
        };
        let group_config = GroupCommitWalConfig {
            queue_capacity: env_u64(
                "MFS_WAL_QUEUE_CAPACITY",
                GroupCommitWalConfig::default().queue_capacity as u64,
            ) as usize,
            max_group_records: env_u64(
                "MFS_WAL_MAX_GROUP_RECORDS",
                GroupCommitWalConfig::default().max_group_records as u64,
            ) as usize,
        };
        Self {
            mode,
            base_path,
            wal_config,
            async_config,
            group_config,
            keep_files: env_u64("MFS_WAL_KEEP", 0) != 0,
            paths: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn path_for(&self, run_idx: usize, shard_idx: Option<usize>) -> io::Result<Option<PathBuf>> {
        let Some(base) = &self.base_path else {
            return Ok(None);
        };
        let suffix = match shard_idx {
            Some(shard) => format!("run{run_idx}.shard{shard}.wal"),
            None => format!("run{run_idx}.wal"),
        };
        let path = PathBuf::from(format!("{}.{}", base.display(), suffix));
        std::fs::remove_file(&path).ok();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        self.paths.lock().expect("wal path list").push(path.clone());
        Ok(Some(path))
    }

    fn replay_and_cleanup(&self) -> io::Result<(usize, usize)> {
        let paths = self.paths.lock().expect("wal path list").clone();
        let mut records = 0usize;
        for path in &paths {
            let codec = UserProfileCodec;
            records +=
                WalBackend::<u64, UserProfile, UserProfileCodec>::replay(path, &codec, |_| {})?;
            if !self.keep_files {
                std::fs::remove_file(path).ok();
            }
        }
        Ok((paths.len(), records))
    }
}

enum BenchBackendKind {
    Counting,
    Direct(Option<WalBackend<u64, UserProfile, UserProfileCodec>>),
    Async(Option<AsyncWalBackend<u64, UserProfile, UserProfileCodec>>),
    Group {
        wal: Option<GroupCommitWalBackend<u64, UserProfile, UserProfileCodec>>,
        handle: mfs_core::durability::GroupCommitWalHandle<u64, UserProfile>,
    },
}

struct BenchBackend {
    kind: BenchBackendKind,
    flushed: Arc<AtomicUsize>,
    bytes_flushed: Arc<AtomicUsize>,
    durable_nanos: Arc<AtomicU64>,
}

impl BenchBackend {
    fn new(
        settings: &WalSettings,
        run_idx: usize,
        shard_idx: Option<usize>,
        flushed: Arc<AtomicUsize>,
        bytes_flushed: Arc<AtomicUsize>,
        durable_nanos: Arc<AtomicU64>,
    ) -> io::Result<Self> {
        let kind = match settings.mode {
            WalMode::Counting => BenchBackendKind::Counting,
            WalMode::Direct => {
                let path = settings.path_for(run_idx, shard_idx)?.expect("wal path");
                BenchBackendKind::Direct(Some(WalBackend::open(
                    path,
                    UserProfileCodec,
                    settings.wal_config,
                )?))
            }
            WalMode::Async => {
                let path = settings.path_for(run_idx, shard_idx)?.expect("wal path");
                BenchBackendKind::Async(Some(AsyncWalBackend::open(
                    path,
                    UserProfileCodec,
                    settings.wal_config,
                    settings.async_config,
                )?))
            }
            WalMode::Group => {
                let path = settings.path_for(run_idx, shard_idx)?.expect("wal path");
                let wal = GroupCommitWalBackend::open(
                    path,
                    UserProfileCodec,
                    settings.wal_config,
                    settings.group_config,
                )?;
                let handle = wal.handle();
                BenchBackendKind::Group {
                    wal: Some(wal),
                    handle,
                }
            }
        };
        Ok(Self {
            kind,
            flushed,
            bytes_flushed,
            durable_nanos,
        })
    }

    fn finalize(&mut self) -> io::Result<()> {
        let start = Instant::now();
        let result = match &mut self.kind {
            BenchBackendKind::Counting => Ok(()),
            BenchBackendKind::Direct(wal) => match wal.as_mut() {
                Some(wal) => wal.sync_now(),
                None => Ok(()),
            },
            BenchBackendKind::Async(wal) => match wal.take() {
                Some(wal) => wal.shutdown(),
                None => Ok(()),
            },
            BenchBackendKind::Group { wal, .. } => match wal.take() {
                Some(wal) => wal.shutdown(),
                None => Ok(()),
            },
        };
        self.durable_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }
}

impl Drop for BenchBackend {
    fn drop(&mut self) {
        let _ = self.finalize();
    }
}

impl FlushBackend<u64, UserProfile> for BenchBackend {
    type Error = io::Error;

    fn flush(&mut self, records: &[FlushRecord<u64, UserProfile>]) -> io::Result<()> {
        let mut total_bytes = 0usize;
        for r in records {
            if let Some(v) = &r.value {
                total_bytes += approx_size(v.as_ref());
                black_box(v.metadata.first());
            }
        }
        match &mut self.kind {
            BenchBackendKind::Counting => {}
            BenchBackendKind::Direct(wal) => {
                wal.as_mut().expect("direct WAL live").flush(records)?
            }
            BenchBackendKind::Async(wal) => {
                wal.as_ref().expect("async WAL live").enqueue(records)?
            }
            BenchBackendKind::Group { handle, .. } => handle.flush(records)?,
        }
        self.flushed.fetch_add(records.len(), Ordering::Relaxed);
        self.bytes_flushed.fetch_add(total_bytes, Ordering::Relaxed);
        Ok(())
    }
}

struct WorkerStats {
    reads: u64,
    writes: u64,
    deletes: u64,
    misses: u64,
    samples: Vec<(u8, u64)>,
}

#[derive(Clone)]
struct RunResult {
    elapsed_secs: f64,
    total_ops: u64,
    reads: u64,
    writes: u64,
    deletes: u64,
    misses: u64,
    flushed: usize,
    flushed_bytes: usize,
    flush_loops: u64,
    read_p50: u64,
    read_p99: u64,
    read_p999: u64,
    read_max: u64,
    write_p50: u64,
    write_p99: u64,
    delete_p50: u64,
    cache_len: usize,
    cache_dirty: usize,
    wal_mode: &'static str,
    wal_durable_ms: f64,
    wal_files: usize,
    wal_replayed: usize,
}

#[derive(Clone, Copy)]
struct Config {
    flusher_mode: FlusherMode,
    duration_secs: u64,
    threads: usize,
    keys: u64,
    value_bytes: usize,
    read_pct: u32,
    write_pct: u32,
    hot_pct: u32,
    flush_ms: u64,
    sample_rate: u64,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fmt_int(n: u128) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn run_once(cfg: Config, run_idx: usize, total_runs: usize) -> RunResult {
    if total_runs > 1 {
        println!("--- run {}/{} ---", run_idx + 1, total_runs);
    }

    let cache: Arc<WriteBehindCache<u64, UserProfile>> =
        Arc::new(WriteBehindCache::with_capacity(cfg.keys as usize));

    let warm_start = Instant::now();
    for i in 0..cfg.keys {
        cache.load_clean(i, make_profile(i, cfg.value_bytes));
    }
    println!(
        "warmed {} records in {:.2}s",
        fmt_int(cfg.keys as u128),
        warm_start.elapsed().as_secs_f64(),
    );

    let flusher_flushed = Arc::new(AtomicUsize::new(0));
    let flusher_bytes = Arc::new(AtomicUsize::new(0));
    let flusher_loops = Arc::new(AtomicU64::new(0));
    let durable_nanos = Arc::new(AtomicU64::new(0));
    let wal_settings = WalSettings::from_env();

    // Either start a single background flusher (the v2 default), or
    // hand control to AutoFlusher which spawns one thread per shard
    // with adaptive ticks. Single mode is recommended on
    // CPU-constrained machines (≤4 logical cores) where the per-shard
    // threads would oversubscribe the scheduler.
    enum FlusherHandle {
        Single {
            stop: Arc<AtomicBool>,
            handle: thread::JoinHandle<()>,
        },
        Auto(AutoFlusher),
    }
    let flusher = match cfg.flusher_mode {
        FlusherMode::Single => {
            let stop = Arc::new(AtomicBool::new(false));
            let cache = Arc::clone(&cache);
            let stop_handle = Arc::clone(&stop);
            let flushed = Arc::clone(&flusher_flushed);
            let bytes = Arc::clone(&flusher_bytes);
            let loops = Arc::clone(&flusher_loops);
            let durable_nanos = Arc::clone(&durable_nanos);
            let wal_settings = wal_settings.clone();
            let flush_ms = cfg.flush_ms;
            let handle = thread::spawn(move || {
                let mut backend = BenchBackend::new(
                    &wal_settings,
                    run_idx,
                    None,
                    Arc::clone(&flushed),
                    Arc::clone(&bytes),
                    durable_nanos,
                )
                .expect("open realistic backend");
                while !stop_handle.load(Ordering::Relaxed) {
                    let _ = cache.flush_idle(&mut backend, 32, 10_000);
                    loops.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(flush_ms));
                }
                for _ in 0..16 {
                    let n = cache.flush_idle(&mut backend, 0, 50_000).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                }
            });
            FlusherHandle::Single { stop, handle }
        }
        FlusherMode::Auto => {
            let flushed_for_factory = Arc::clone(&flusher_flushed);
            let bytes_for_factory = Arc::clone(&flusher_bytes);
            let durable_for_factory = Arc::clone(&durable_nanos);
            let wal_settings_for_factory = wal_settings.clone();
            let auto = AutoFlusher::spawn(
                Arc::clone(&cache),
                move |shard_idx| {
                    BenchBackend::new(
                        &wal_settings_for_factory,
                        run_idx,
                        Some(shard_idx),
                        Arc::clone(&flushed_for_factory),
                        Arc::clone(&bytes_for_factory),
                        Arc::clone(&durable_for_factory),
                    )
                    .expect("open realistic backend")
                },
                AutoFlusherConfig {
                    min_tick_ms: 1,
                    max_tick_ms: cfg.flush_ms,
                    target_depth: 1024,
                    max_records_per_drain: 8192,
                    idle_ticks_threshold: 32,
                    final_drain_passes: 32,
                },
            );
            FlusherHandle::Auto(auto)
        }
    };

    let total_ops = Arc::new(AtomicU64::new(0));
    let bench_start = Instant::now();
    let deadline = bench_start + Duration::from_secs(cfg.duration_secs);
    let mut handles = Vec::new();
    for tid in 0..cfg.threads {
        let cache = Arc::clone(&cache);
        let total_ops = Arc::clone(&total_ops);
        handles.push(thread::spawn(move || {
            let mut rng = XorShift::new(
                0xa5a5_a5a5_0000_0001
                    ^ (tid as u64).wrapping_mul(0x9e37_79b9)
                    ^ (run_idx as u64).wrapping_mul(0x517c_c1b7_2722_0a95),
            );
            let mut stats = WorkerStats {
                reads: 0,
                writes: 0,
                deletes: 0,
                misses: 0,
                samples: Vec::with_capacity(1 << 16),
            };
            let mut local_ops: u64 = 0;
            const BATCH: u64 = 8192;
            'outer: loop {
                let p = cache.pin();
                for _ in 0..BATCH {
                    let key = pick_key(&mut rng, cfg.keys, cfg.hot_pct);
                    let roll = rng.next_pct();
                    let sampled = local_ops.is_multiple_of(cfg.sample_rate);
                    let t0 = if sampled { Some(Instant::now()) } else { None };

                    if roll < cfg.read_pct {
                        let hit = p.read_with(&key, |v| v.balance_cents).is_some();
                        if hit {
                            stats.reads += 1;
                        } else {
                            stats.misses += 1;
                        }
                    } else if roll < cfg.read_pct + cfg.write_pct {
                        cache.put(key, make_profile(key, cfg.value_bytes));
                        stats.writes += 1;
                        if let Some(t) = t0 {
                            stats.samples.push((1, t.elapsed().as_nanos() as u64));
                        }
                        local_ops += 1;
                        continue;
                    } else {
                        cache.delete(key);
                        stats.deletes += 1;
                        if let Some(t) = t0 {
                            stats.samples.push((2, t.elapsed().as_nanos() as u64));
                        }
                        local_ops += 1;
                        continue;
                    }

                    if let Some(t) = t0 {
                        stats.samples.push((0, t.elapsed().as_nanos() as u64));
                    }
                    local_ops += 1;
                }
                drop(p);
                if Instant::now() >= deadline {
                    break 'outer;
                }
            }
            total_ops.fetch_add(local_ops, Ordering::Relaxed);
            stats
        }));
    }

    let mut all_stats: Vec<WorkerStats> = Vec::with_capacity(cfg.threads);
    for h in handles {
        all_stats.push(h.join().expect("worker join"));
    }
    let elapsed = bench_start.elapsed();
    match flusher {
        FlusherHandle::Single { stop, handle } => {
            stop.store(true, Ordering::Relaxed);
            handle.join().expect("flusher join");
        }
        FlusherHandle::Auto(auto) => auto.stop(),
    };

    let (wal_files, wal_replayed) = wal_settings
        .replay_and_cleanup()
        .expect("WAL replay validation");

    let mut totals = WorkerStats {
        reads: 0,
        writes: 0,
        deletes: 0,
        misses: 0,
        samples: Vec::new(),
    };
    for s in &all_stats {
        totals.reads += s.reads;
        totals.writes += s.writes;
        totals.deletes += s.deletes;
        totals.misses += s.misses;
        totals.samples.extend_from_slice(&s.samples);
    }
    let total_ops_v = total_ops.load(Ordering::Relaxed);

    let mut read_lat: Vec<u64> = totals
        .samples
        .iter()
        .filter(|(k, _)| *k == 0)
        .map(|(_, n)| *n)
        .collect();
    let mut write_lat: Vec<u64> = totals
        .samples
        .iter()
        .filter(|(k, _)| *k == 1)
        .map(|(_, n)| *n)
        .collect();
    let mut delete_lat: Vec<u64> = totals
        .samples
        .iter()
        .filter(|(k, _)| *k == 2)
        .map(|(_, n)| *n)
        .collect();
    read_lat.sort_unstable();
    write_lat.sort_unstable();
    delete_lat.sort_unstable();

    let stats_view = cache.stats();

    RunResult {
        elapsed_secs: elapsed.as_secs_f64(),
        total_ops: total_ops_v,
        reads: totals.reads,
        writes: totals.writes,
        deletes: totals.deletes,
        misses: totals.misses,
        flushed: flusher_flushed.load(Ordering::Relaxed),
        flushed_bytes: flusher_bytes.load(Ordering::Relaxed),
        flush_loops: flusher_loops.load(Ordering::Relaxed),
        read_p50: percentile(&read_lat, 0.50),
        read_p99: percentile(&read_lat, 0.99),
        read_p999: percentile(&read_lat, 0.999),
        read_max: read_lat.last().copied().unwrap_or(0),
        write_p50: percentile(&write_lat, 0.50),
        write_p99: percentile(&write_lat, 0.99),
        delete_p50: percentile(&delete_lat, 0.50),
        cache_len: stats_view.len,
        cache_dirty: stats_view.dirty,
        wal_mode: wal_settings.mode.label(),
        wal_durable_ms: durable_nanos.load(Ordering::Relaxed) as f64 / 1e6,
        wal_files,
        wal_replayed,
    }
}

fn print_single(r: &RunResult) {
    let throughput = r.total_ops as f64 / r.elapsed_secs;
    println!();
    println!("=== results ===");
    println!("elapsed       = {:.2}s", r.elapsed_secs);
    println!(
        "throughput    = {:>10.2} M ops/sec aggregate ({})",
        throughput / 1e6,
        fmt_int(r.total_ops as u128),
    );
    println!(
        "  reads (hit) = {:>10}      misses = {:>10}",
        fmt_int(r.reads as u128),
        fmt_int(r.misses as u128),
    );
    println!("  writes      = {:>10}", fmt_int(r.writes as u128));
    println!("  deletes     = {:>10}", fmt_int(r.deletes as u128));
    println!();
    println!(
        "read latency  p50={:>5}ns  p99={:>6}ns  p99.9={:>7}ns  max={:>9}ns",
        r.read_p50, r.read_p99, r.read_p999, r.read_max,
    );
    println!(
        "write latency p50={:>5}ns  p99={:>6}ns",
        r.write_p50, r.write_p99,
    );
    println!("delete p50    = {}ns", r.delete_p50);
    println!();
    println!(
        "flush         = {:>10} records ({:.2} MiB) over {} loops -> {:.2}M rec/s",
        fmt_int(r.flushed as u128),
        r.flushed_bytes as f64 / (1024.0 * 1024.0),
        fmt_int(r.flush_loops as u128),
        r.flushed as f64 / r.elapsed_secs / 1e6,
    );
    println!(
        "cache_state   = len={}  dirty={}",
        fmt_int(r.cache_len as u128),
        fmt_int(r.cache_dirty as u128),
    );
    println!(
        "wal           = mode={} durable_sync={:.2}ms files={} replayed={}",
        r.wal_mode,
        r.wal_durable_ms,
        r.wal_files,
        fmt_int(r.wal_replayed as u128),
    );
}

fn distribution(label: &str, values: &mut [f64]) -> String {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n;
    let stddev = var.sqrt();
    let min = values.first().copied().unwrap_or(0.0);
    let max = values.last().copied().unwrap_or(0.0);
    let med = values[values.len() / 2];
    let p95 = values[((values.len() as f64 - 1.0) * 0.95).round() as usize];
    let cv = if mean.abs() > f64::EPSILON {
        (stddev / mean) * 100.0
    } else {
        0.0
    };
    format!(
        "  {label:<24} min={min:>9.2}  median={med:>9.2}  p95={p95:>9.2}  max={max:>9.2}  stddev={stddev:>7.2}  cv={cv:>5.1}%",
    )
}

fn print_distribution(runs: &[RunResult]) {
    if runs.len() < 2 {
        return;
    }
    println!();
    println!("=== distribution across {} runs ===", runs.len());

    let mut throughput: Vec<f64> = runs
        .iter()
        .map(|r| r.total_ops as f64 / r.elapsed_secs / 1e6)
        .collect();
    let mut read_p50: Vec<f64> = runs.iter().map(|r| r.read_p50 as f64).collect();
    let mut read_p99: Vec<f64> = runs.iter().map(|r| r.read_p99 as f64).collect();
    let mut write_p50: Vec<f64> = runs.iter().map(|r| r.write_p50 as f64).collect();
    let mut flush_rate: Vec<f64> = runs
        .iter()
        .map(|r| r.flushed as f64 / r.elapsed_secs / 1e6)
        .collect();

    println!("{}", distribution("throughput (M ops/s)", &mut throughput));
    println!("{}", distribution("read p50 (ns)", &mut read_p50));
    println!("{}", distribution("read p99 (ns)", &mut read_p99));
    println!("{}", distribution("write p50 (ns)", &mut write_p50));
    println!("{}", distribution("flush (M rec/s)", &mut flush_rate));
}

fn main() {
    // MFS_THREADS overrides the worker count via auto_thread_count: the
    // env value is the explicit request, None falls back to nproc/2,
    // and any value above nproc collapses back to nproc/2.
    let requested_threads = std::env::var("MFS_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let cfg = Config {
        duration_secs: env_u64("MFS_DURATION_SECS", 5),
        threads: auto_thread_count(requested_threads),
        keys: env_u64("MFS_KEYS", 100_000),
        value_bytes: env_u64("MFS_VALUE_BYTES", 128) as usize,
        read_pct: env_u64("MFS_READ_PCT", 95) as u32,
        write_pct: env_u64("MFS_WRITE_PCT", 4) as u32,
        hot_pct: env_u64("MFS_HOT_PCT", 80) as u32,
        flush_ms: env_u64("MFS_FLUSH_INTERVAL_MS", 10),
        sample_rate: env_u64("MFS_SAMPLE_RATE", 64).max(1),
        flusher_mode: if env_u64("MFS_AUTO_FLUSHER", 0) != 0 {
            FlusherMode::Auto
        } else {
            FlusherMode::Single
        },
    };
    let runs = env_u64("MFS_RUNS", 1).max(1) as usize;
    let wal_preview = WalSettings::from_env();

    println!("=== realistic workload bench ===");
    println!(
        "duration={}s runs={} threads={} keys={} value~{}B mix={}/{}/{}% hot={}% flush={}ms sample=1/{}",
        cfg.duration_secs,
        runs,
        cfg.threads,
        fmt_int(cfg.keys as u128),
        cfg.value_bytes,
        cfg.read_pct,
        cfg.write_pct,
        100 - cfg.read_pct - cfg.write_pct,
        cfg.hot_pct,
        cfg.flush_ms,
        cfg.sample_rate,
    );
    println!(
        "backend={} wal_path={}",
        wal_preview.mode.label(),
        wal_preview
            .base_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<none>".to_string())
    );

    let mut results: Vec<RunResult> = Vec::with_capacity(runs);
    for i in 0..runs {
        let r = run_once(cfg, i, runs);
        if runs == 1 {
            print_single(&r);
        } else {
            println!(
                "  throughput={:.2} M ops/s   read p50={}ns   p99={}ns",
                r.total_ops as f64 / r.elapsed_secs / 1e6,
                r.read_p50,
                r.read_p99,
            );
        }
        results.push(r);
    }
    print_distribution(&results);
}
