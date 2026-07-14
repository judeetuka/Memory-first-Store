//! Realistic Redis-like object-store workload benchmark.
//!
//! Workloads:
//! - `string-heavy`: string reads, replacements, deletes.
//! - `hash-heavy`: field reads, field writes, field deletes.
//! - `list-heavy`: range/index reads, pushes, pops.
//! - `mixed`: strings, integers, bytes, hashes, lists, sets, sorted sets, JSON.
//!
//! ## Workload knobs (env vars)
//!
//! - `MFS_OBJ_OPS`          total operations per workload/run (default 20,000)
//! - `MFS_OBJ_RUNS`         independent runs per workload (default 3)
//! - `MFS_OBJ_THREADS`      worker threads (default = available CPUs, capped at 8)
//! - `MFS_OBJ_KEYS`         key universe size per type (default 5,000)
//! - `MFS_OBJ_READ_PCT`     read percentage (default 80)
//! - `MFS_OBJ_WRITE_PCT`    write percentage (default 15), delete/pop/remove gets the rest
//! - `MFS_OBJ_HOT_PCT`      % accesses targeting the hot 20% of keys (default 80)
//! - `MFS_OBJ_VALUE_BYTES`  bytes payload size (default 128)
//! - `MFS_OBJ_HASH_FIELDS`  fields per hash value (default 8)
//! - `MFS_OBJ_LIST_ITEMS`   items per initial list value (default 8)
//! - `MFS_OBJ_SAMPLE_RATE`  1-in-N latency sampling rate (default 64)
//! - `MFS_OBJ_WAL_PATH`     optional WAL path prefix for flush/replay validation
//! - `MFS_OBJ_WAL_KEEP`     keep WAL files after validation (default 0)
//! - `MFS_OBJ_TIERED`       add opt-in `mutable-tiered` rows (default 0)
//! - `MFS_OBJ_COLD_READ_PCT` percent of tiered reads routed to cold keys (default 10)
//! - `MFS_OBJ_TIER_MAX_RECORDS` maximum records demoted before timed tiered work (default 128)
//! - `MFS_OBJ_TIER_IDLE_TICKS` minimum idle ticks before demotion (default 1024)
//! - `MFS_OBJ_TIER_MIN_CLEAN_AGE` minimum clean age ticks before demotion (default 1)
//! - `MFS_OBJ_TIER_HOT_CAPACITY` optional hot capacity soft limit for tiering
//!
//! The benchmark flushes after workers finish, so it sizes dirty queues for the
//! configured operation budget instead of relying on background drain progress.

use mfs_compat::object_store::{MfsMutableObjectStore, MfsObjectStore, ObjectStoreError};
use mfs_compat::object_store_durability::{
    MutableObjectStoreBundle, MutableObjectStorePersistence, MutableObjectTieringReport,
    TieringPolicy,
};
use mfs_core::durability::{WalBackend, WalConfig};
use mfs_core::writeback::{WriteBehindConfig, WriteBehindStats};
use mfs_core::{FlushBackend, FlushRecord};
use mfs_store::value::{MfsValue, MfsValueCodec, SortedSetEntry};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
struct Config {
    ops: u64,
    runs: usize,
    threads: usize,
    keys: u64,
    read_pct: u32,
    write_pct: u32,
    hot_pct: u32,
    value_bytes: usize,
    hash_fields: usize,
    list_items: usize,
    sample_rate: u64,
    wal_path: Option<PathBuf>,
    keep_wal: bool,
    tiered: bool,
    cold_read_pct: u32,
    tier_policy: TieringPolicy,
}

impl Config {
    fn from_env() -> Self {
        let requested_threads = env_usize("MFS_OBJ_THREADS", default_threads());
        let read_pct = env_u32("MFS_OBJ_READ_PCT", 80).min(100);
        let write_pct = env_u32("MFS_OBJ_WRITE_PCT", 15).min(100 - read_pct);
        let default_tier_policy = TieringPolicy::default();
        Self {
            ops: env_u64("MFS_OBJ_OPS", 20_000),
            runs: env_usize("MFS_OBJ_RUNS", 3).max(1),
            threads: requested_threads.max(1),
            keys: env_u64("MFS_OBJ_KEYS", 5_000).max(1),
            read_pct,
            write_pct,
            hot_pct: env_u32("MFS_OBJ_HOT_PCT", 80).min(100),
            value_bytes: env_usize("MFS_OBJ_VALUE_BYTES", 128).max(1),
            hash_fields: env_usize("MFS_OBJ_HASH_FIELDS", 8).max(1),
            list_items: env_usize("MFS_OBJ_LIST_ITEMS", 8).max(1),
            sample_rate: env_u64("MFS_OBJ_SAMPLE_RATE", 64).max(1),
            wal_path: env::var("MFS_OBJ_WAL_PATH").ok().map(PathBuf::from),
            keep_wal: env_bool("MFS_OBJ_WAL_KEEP"),
            tiered: env_bool("MFS_OBJ_TIERED"),
            cold_read_pct: env_u32("MFS_OBJ_COLD_READ_PCT", 10).min(100),
            tier_policy: TieringPolicy {
                idle_threshold_ticks: env_u64(
                    "MFS_OBJ_TIER_IDLE_TICKS",
                    default_tier_policy.idle_threshold_ticks,
                ),
                max_records: env_usize("MFS_OBJ_TIER_MAX_RECORDS", default_tier_policy.max_records),
                hot_capacity_soft_limit: env_optional_usize("MFS_OBJ_TIER_HOT_CAPACITY"),
                min_clean_age_ticks: env_u64(
                    "MFS_OBJ_TIER_MIN_CLEAN_AGE",
                    default_tier_policy.min_clean_age_ticks,
                ),
            },
        }
    }

    fn write_cutoff(&self) -> u32 {
        self.read_pct + self.write_pct
    }
}

#[derive(Debug, Clone, Copy)]
enum Workload {
    StringHeavy,
    HashHeavy,
    ListHeavy,
    Mixed,
}

impl Workload {
    const ALL: [Self; 4] = [
        Self::StringHeavy,
        Self::HashHeavy,
        Self::ListHeavy,
        Self::Mixed,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::StringHeavy => "string-heavy",
            Self::HashHeavy => "hash-heavy",
            Self::ListHeavy => "list-heavy",
            Self::Mixed => "mixed",
        }
    }

    fn key_families(self) -> usize {
        match self {
            Self::Mixed => 8,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleKind {
    Read,
    Mutate,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    kind: SampleKind,
    nanos: u128,
}

#[derive(Default)]
struct WorkerStats {
    ops: u64,
    reads: u64,
    mutations: u64,
    misses: u64,
    checksum: u64,
    samples: Vec<Sample>,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.ops += other.ops;
        self.reads += other.reads;
        self.mutations += other.mutations;
        self.misses += other.misses;
        self.checksum ^= other.checksum;
        self.samples.extend(other.samples);
    }
}

struct RunStats {
    store: StoreKind,
    workload: Workload,
    run: usize,
    threads: usize,
    elapsed: Duration,
    worker: WorkerStats,
    flush_records: u64,
    flush_bytes: u64,
    flush_elapsed: Duration,
    replayed: usize,
    cache_len: usize,
    dirty: usize,
    tier: Option<TierRunStats>,
}

#[derive(Debug, Clone, Copy)]
struct TierRunStats {
    cold_read_pct: u32,
    cold_read_candidates: usize,
    cold_read_attempts: u64,
    cold_promotions: u64,
    report: MutableObjectTieringReport,
}

impl RunStats {
    fn print(&self) {
        let throughput = self.worker.ops as f64 / self.elapsed.as_secs_f64();
        let flush_rate = if self.flush_elapsed.is_zero() {
            0.0
        } else {
            self.flush_records as f64 / self.flush_elapsed.as_secs_f64()
        };
        let (read_p50, read_p99) = percentile_pair(&self.worker.samples, SampleKind::Read);
        let (mut_p50, mut_p99) = percentile_pair(&self.worker.samples, SampleKind::Mutate);
        let read_samples = sample_count(&self.worker.samples, SampleKind::Read);
        let mut_samples = sample_count(&self.worker.samples, SampleKind::Mutate);
        match self.tier {
            Some(tier) => println!(
                "{:<14} run={} ops={} threads={} throughput={:.2} Mops/s reads={} mutations={} misses={} read_samples={} mut_samples={} read_p50={}ns read_p99={}ns mut_p50={}ns mut_p99={}ns flush_records={} flush_rate={:.2} Mrec/s replayed={} cache_len={} dirty={} bytes={:.2} MiB cold_read_pct={} cold_read_candidates={} cold_read_attempts={} cold_promotions={} tier_attempted={} tier_demoted={} tier_skipped_dirty={} tier_skipped_recent={} tier_skipped_capacity={} tier_skipped_empty={} tier_flush_records={}",
                format!("{}:{}", self.store.label(), self.workload.label()),
                self.run,
                self.worker.ops,
                self.threads,
                throughput / 1_000_000.0,
                self.worker.reads,
                self.worker.mutations,
                self.worker.misses,
                read_samples,
                mut_samples,
                read_p50,
                read_p99,
                mut_p50,
                mut_p99,
                self.flush_records,
                flush_rate / 1_000_000.0,
                self.replayed,
                self.cache_len,
                self.dirty,
                self.flush_bytes as f64 / (1024.0 * 1024.0),
                tier.cold_read_pct,
                tier.cold_read_candidates,
                tier.cold_read_attempts,
                tier.cold_promotions,
                tier.report.attempted,
                tier.report.demoted,
                tier.report.skipped_dirty,
                tier.report.skipped_recent,
                tier.report.skipped_capacity,
                tier.report.skipped_empty,
                tier.report.flush_records,
            ),
            None => println!(
                "{:<14} run={} ops={} threads={} throughput={:.2} Mops/s reads={} mutations={} misses={} read_samples={} mut_samples={} read_p50={}ns read_p99={}ns mut_p50={}ns mut_p99={}ns flush_records={} flush_rate={:.2} Mrec/s replayed={} cache_len={} dirty={} bytes={:.2} MiB",
                format!("{}:{}", self.store.label(), self.workload.label()),
                self.run,
                self.worker.ops,
                self.threads,
                throughput / 1_000_000.0,
                self.worker.reads,
                self.worker.mutations,
                self.worker.misses,
                read_samples,
                mut_samples,
                read_p50,
                read_p99,
                mut_p50,
                mut_p99,
                self.flush_records,
                flush_rate / 1_000_000.0,
                self.replayed,
                self.cache_len,
                self.dirty,
                self.flush_bytes as f64 / (1024.0 * 1024.0),
            ),
        }
        black_box(self.worker.checksum);
    }
}

#[derive(Debug, Clone, Copy)]
enum StoreKind {
    Boxed,
    Mutable,
    MutableTiered,
}

impl StoreKind {
    fn enabled(cfg: &Config) -> Vec<Self> {
        let mut stores = vec![Self::Boxed, Self::Mutable];
        if cfg.tiered {
            stores.push(Self::MutableTiered);
        }
        stores
    }

    fn label(self) -> &'static str {
        match self {
            Self::Boxed => "boxed",
            Self::Mutable => "mutable",
            Self::MutableTiered => "mutable-tiered",
        }
    }
}

trait ObjectBenchStore: Send + Sync + 'static {
    fn load_clean(&self, key: Vec<u8>, value: MfsValue) -> u64;
    fn get_string(&self, key: &[u8]) -> Result<Option<String>, ObjectStoreError>;
    fn set_string(&self, key: Vec<u8>, value: String) -> u64;
    fn delete(&self, key: Vec<u8>) -> u64;
    fn hash_get(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn hash_set(
        &self,
        key: Vec<u8>,
        field: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<u64, ObjectStoreError>;
    fn hash_del(&self, key: Vec<u8>, field: &[u8]) -> Result<u64, ObjectStoreError>;
    fn list_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError>;
    fn list_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError>;
    fn list_push(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, ObjectStoreError>;
    fn list_pop_front(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn list_pop_back(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn get_integer(&self, key: &[u8]) -> Result<Option<i64>, ObjectStoreError>;
    fn incr_by(&self, key: Vec<u8>, delta: i64) -> Result<i64, ObjectStoreError>;
    fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn set_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64;
    fn set_contains(&self, key: &[u8], member: &[u8]) -> Result<bool, ObjectStoreError>;
    fn set_add(&self, key: Vec<u8>, member: Vec<u8>) -> Result<u64, ObjectStoreError>;
    fn set_remove(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError>;
    fn zlen(&self, key: &[u8]) -> Result<usize, ObjectStoreError>;
    fn zrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, ObjectStoreError>;
    fn zadd(&self, key: Vec<u8>, score: f64, member: Vec<u8>) -> Result<u64, ObjectStoreError>;
    fn zrem(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError>;
    fn read_with<R, F>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&MfsValue) -> R;
    fn set_json_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64;
    fn flush_idle<B: FlushBackend<Vec<u8>, MfsValue>>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>;
    fn stats(&self) -> WriteBehindStats;
    fn cold_read_pct(&self) -> u32 {
        0
    }
    fn cold_read_key(&self, _prefix: u8, _rng: &mut XorShift) -> Option<Vec<u8>> {
        None
    }
    fn promote_for_cold_read(&self, _key: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

macro_rules! impl_object_bench_store {
    ($ty:ty) => {
        impl ObjectBenchStore for $ty {
            fn load_clean(&self, key: Vec<u8>, value: MfsValue) -> u64 {
                self.load_clean(key, value)
            }
            fn get_string(&self, key: &[u8]) -> Result<Option<String>, ObjectStoreError> {
                self.get_string(key)
            }
            fn set_string(&self, key: Vec<u8>, value: String) -> u64 {
                self.set_string(key, value)
            }
            fn delete(&self, key: Vec<u8>) -> u64 {
                self.delete(key)
            }
            fn hash_get(
                &self,
                key: &[u8],
                field: &[u8],
            ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
                self.hash_get(key, field)
            }
            fn hash_set(
                &self,
                key: Vec<u8>,
                field: Vec<u8>,
                value: Vec<u8>,
            ) -> Result<u64, ObjectStoreError> {
                self.hash_set(key, field, value)
            }
            fn hash_del(&self, key: Vec<u8>, field: &[u8]) -> Result<u64, ObjectStoreError> {
                self.hash_del(key, field)
            }
            fn list_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
                self.list_len(key)
            }
            fn list_range(
                &self,
                key: &[u8],
                start: i64,
                stop: i64,
            ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
                self.list_range(key, start, stop)
            }
            fn list_push(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, ObjectStoreError> {
                self.list_push(key, value)
            }
            fn list_pop_front(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError> {
                self.list_pop_front(key)
            }
            fn list_pop_back(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError> {
                self.list_pop_back(key)
            }
            fn get_integer(&self, key: &[u8]) -> Result<Option<i64>, ObjectStoreError> {
                self.get_integer(key)
            }
            fn incr_by(&self, key: Vec<u8>, delta: i64) -> Result<i64, ObjectStoreError> {
                self.incr_by(key, delta)
            }
            fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError> {
                self.get_bytes(key)
            }
            fn set_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64 {
                self.set_bytes(key, value)
            }
            fn set_contains(&self, key: &[u8], member: &[u8]) -> Result<bool, ObjectStoreError> {
                self.set_contains(key, member)
            }
            fn set_add(&self, key: Vec<u8>, member: Vec<u8>) -> Result<u64, ObjectStoreError> {
                self.set_add(key, member)
            }
            fn set_remove(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError> {
                self.set_remove(key, member)
            }
            fn zlen(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
                self.zlen(key)
            }
            fn zrange(
                &self,
                key: &[u8],
                start: i64,
                stop: i64,
            ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
                self.zrange(key, start, stop)
            }
            fn zadd(
                &self,
                key: Vec<u8>,
                score: f64,
                member: Vec<u8>,
            ) -> Result<u64, ObjectStoreError> {
                self.zadd(key, score, member)
            }
            fn zrem(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError> {
                self.zrem(key, member)
            }
            fn read_with<R, F>(&self, key: &[u8], f: F) -> Option<R>
            where
                F: FnOnce(&MfsValue) -> R,
            {
                self.read_with(key, f)
            }
            fn set_json_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64 {
                self.set_json_bytes(key, value)
            }
            fn flush_idle<B: FlushBackend<Vec<u8>, MfsValue>>(
                &self,
                backend: &mut B,
                idle_ticks: u64,
                max_records: usize,
            ) -> Result<usize, B::Error> {
                self.flush_idle(backend, idle_ticks, max_records)
            }
            fn stats(&self) -> WriteBehindStats {
                self.stats()
            }
        }
    };
}

impl_object_bench_store!(MfsObjectStore);
impl_object_bench_store!(MfsMutableObjectStore);

struct TieredBenchStore {
    hot: Arc<MfsMutableObjectStore>,
    bundle: Arc<MutableObjectStoreBundle>,
    cold_keys: Arc<BTreeMap<u8, Vec<Vec<u8>>>>,
    cold_read_pct: u32,
    cold_read_attempts: AtomicU64,
    cold_promotions: AtomicU64,
}

impl TieredBenchStore {
    fn new(
        hot: Arc<MfsMutableObjectStore>,
        bundle: MutableObjectStoreBundle,
        cold_keys: BTreeMap<u8, Vec<Vec<u8>>>,
        cold_read_pct: u32,
    ) -> Self {
        Self {
            hot,
            bundle: Arc::new(bundle),
            cold_keys: Arc::new(cold_keys),
            cold_read_pct,
            cold_read_attempts: AtomicU64::new(0),
            cold_promotions: AtomicU64::new(0),
        }
    }

    fn cold_read_candidates(&self) -> usize {
        self.cold_keys.values().map(Vec::len).sum()
    }

    fn cold_read_attempts(&self) -> u64 {
        self.cold_read_attempts.load(Ordering::Relaxed)
    }

    fn cold_promotions(&self) -> u64 {
        self.cold_promotions.load(Ordering::Relaxed)
    }
}

impl ObjectBenchStore for TieredBenchStore {
    fn load_clean(&self, key: Vec<u8>, value: MfsValue) -> u64 {
        self.hot.load_clean(key, value)
    }

    fn get_string(&self, key: &[u8]) -> Result<Option<String>, ObjectStoreError> {
        self.hot.get_string(key)
    }

    fn set_string(&self, key: Vec<u8>, value: String) -> u64 {
        self.hot.set_string(key, value)
    }

    fn delete(&self, key: Vec<u8>) -> u64 {
        self.hot.delete(key)
    }

    fn hash_get(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.hot.hash_get(key, field)
    }

    fn hash_set(
        &self,
        key: Vec<u8>,
        field: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<u64, ObjectStoreError> {
        self.hot.hash_set(key, field, value)
    }

    fn hash_del(&self, key: Vec<u8>, field: &[u8]) -> Result<u64, ObjectStoreError> {
        self.hot.hash_del(key, field)
    }

    fn list_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        self.hot.list_len(key)
    }

    fn list_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        self.hot.list_range(key, start, stop)
    }

    fn list_push(&self, key: Vec<u8>, value: Vec<u8>) -> Result<u64, ObjectStoreError> {
        self.hot.list_push(key, value)
    }

    fn list_pop_front(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.hot.list_pop_front(key)
    }

    fn list_pop_back(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.hot.list_pop_back(key)
    }

    fn get_integer(&self, key: &[u8]) -> Result<Option<i64>, ObjectStoreError> {
        self.hot.get_integer(key)
    }

    fn incr_by(&self, key: Vec<u8>, delta: i64) -> Result<i64, ObjectStoreError> {
        self.hot.incr_by(key, delta)
    }

    fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.hot.get_bytes(key)
    }

    fn set_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64 {
        self.hot.set_bytes(key, value)
    }

    fn set_contains(&self, key: &[u8], member: &[u8]) -> Result<bool, ObjectStoreError> {
        self.hot.set_contains(key, member)
    }

    fn set_add(&self, key: Vec<u8>, member: Vec<u8>) -> Result<u64, ObjectStoreError> {
        self.hot.set_add(key, member)
    }

    fn set_remove(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError> {
        self.hot.set_remove(key, member)
    }

    fn zlen(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        self.hot.zlen(key)
    }

    fn zrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        self.hot.zrange(key, start, stop)
    }

    fn zadd(&self, key: Vec<u8>, score: f64, member: Vec<u8>) -> Result<u64, ObjectStoreError> {
        self.hot.zadd(key, score, member)
    }

    fn zrem(&self, key: Vec<u8>, member: &[u8]) -> Result<u64, ObjectStoreError> {
        self.hot.zrem(key, member)
    }

    fn read_with<R, F>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&MfsValue) -> R,
    {
        self.hot.read_with(key, f)
    }

    fn set_json_bytes(&self, key: Vec<u8>, value: Vec<u8>) -> u64 {
        self.hot.set_json_bytes(key, value)
    }

    fn flush_idle<B: FlushBackend<Vec<u8>, MfsValue>>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error> {
        self.hot.flush_idle(backend, idle_ticks, max_records)
    }

    fn stats(&self) -> WriteBehindStats {
        self.hot.stats()
    }

    fn cold_read_pct(&self) -> u32 {
        if self.cold_read_candidates() == 0 {
            0
        } else {
            self.cold_read_pct
        }
    }

    fn cold_read_key(&self, prefix: u8, rng: &mut XorShift) -> Option<Vec<u8>> {
        let keys = self.cold_keys.get(&prefix)?;
        if keys.is_empty() {
            return None;
        }
        let idx = (rng.next_u64() as usize) % keys.len();
        Some(keys[idx].clone())
    }

    fn promote_for_cold_read(&self, key: &[u8]) -> io::Result<()> {
        self.cold_read_attempts.fetch_add(1, Ordering::Relaxed);
        if self.hot.get(key).is_none() && self.bundle.promote_cold_key(self.hot.as_ref(), key)? {
            self.cold_promotions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

struct BenchBackend {
    kind: BackendKind,
    records: u64,
    bytes: u64,
}

enum BackendKind {
    Counting,
    Wal(WalBackend<Vec<u8>, MfsValue, MfsValueCodec>),
}

impl BenchBackend {
    fn counting() -> Self {
        Self {
            kind: BackendKind::Counting,
            records: 0,
            bytes: 0,
        }
    }

    fn wal(path: &Path) -> io::Result<Self> {
        Ok(Self {
            kind: BackendKind::Wal(WalBackend::open(path, MfsValueCodec, WalConfig::default())?),
            records: 0,
            bytes: 0,
        })
    }

    fn sync_now(&mut self) -> io::Result<()> {
        match &mut self.kind {
            BackendKind::Counting => Ok(()),
            BackendKind::Wal(wal) => wal.sync_now(),
        }
    }
}

impl FlushBackend<Vec<u8>, MfsValue> for BenchBackend {
    type Error = io::Error;

    fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> io::Result<()> {
        self.records += records.len() as u64;
        self.bytes += records.iter().map(approx_record_bytes).sum::<usize>() as u64;
        match &mut self.kind {
            BackendKind::Counting => Ok(()),
            BackendKind::Wal(wal) => wal.flush(records),
        }
    }
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

fn main() -> io::Result<()> {
    let cfg = Config::from_env();
    println!("=== Redis-like object-store realistic workloads ===");
    println!(
        "ops={} runs={} threads={} keys={} read_pct={} write_pct={} hot_pct={} value_bytes={} hash_fields={} list_items={} sample_rate={} wal={}",
        cfg.ops,
        cfg.runs,
        cfg.threads,
        cfg.keys,
        cfg.read_pct,
        cfg.write_pct,
        cfg.hot_pct,
        cfg.value_bytes,
        cfg.hash_fields,
        cfg.list_items,
        cfg.sample_rate,
        cfg.wal_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "counting".to_string()),
    );
    if cfg.tiered {
        println!(
            "tiered=1 cold_read_pct={} tier_idle_ticks={} tier_min_clean_age={} tier_max_records={} tier_hot_capacity={}",
            cfg.cold_read_pct,
            cfg.tier_policy.idle_threshold_ticks,
            cfg.tier_policy.min_clean_age_ticks,
            cfg.tier_policy.max_records,
            cfg.tier_policy
                .hot_capacity_soft_limit
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        );
    }

    for store in StoreKind::enabled(&cfg) {
        for workload in Workload::ALL {
            for run in 0..cfg.runs {
                let stats = run_workload(&cfg, store, workload, run)?;
                stats.print();
            }
        }
    }

    Ok(())
}

fn run_workload(
    cfg: &Config,
    store_kind: StoreKind,
    workload: Workload,
    run: usize,
) -> io::Result<RunStats> {
    match store_kind {
        StoreKind::Boxed => {
            let store = Arc::new(MfsObjectStore::with_config(store_config(
                cfg,
                workload_capacity(cfg, workload),
            )));
            run_workload_for_store(cfg, store_kind, workload, run, store)
        }
        StoreKind::Mutable => {
            let store = Arc::new(MfsMutableObjectStore::with_capacity(workload_capacity(
                cfg, workload,
            )));
            run_workload_for_store(cfg, store_kind, workload, run, store)
        }
        StoreKind::MutableTiered => run_tiered_workload(cfg, workload, run),
    }
}

fn run_workload_for_store<S>(
    cfg: &Config,
    store_kind: StoreKind,
    workload: Workload,
    run: usize,
    store: Arc<S>,
) -> io::Result<RunStats>
where
    S: ObjectBenchStore,
{
    prepopulate(store.as_ref(), cfg, workload);

    let (worker, elapsed) = run_workers(cfg, workload, run, Arc::clone(&store))?;

    let wal_path = cfg
        .wal_path
        .as_ref()
        .map(|base| workload_wal_path(base, store_kind, workload, run));
    let mut backend = match wal_path.as_deref() {
        Option::Some(path) => BenchBackend::wal(path)?,
        Option::None => BenchBackend::counting(),
    };
    let flush_start = Instant::now();
    store.flush_idle(&mut backend, /*idle_ticks=*/ 0, usize::MAX)?;
    backend.sync_now()?;
    let flush_elapsed = flush_start.elapsed();

    let replayed = match wal_path.as_deref() {
        Option::Some(path) => {
            let replayed = WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(
                path,
                &MfsValueCodec,
                |_| {},
            )?;
            if !cfg.keep_wal {
                std::fs::remove_file(path).ok();
            }
            replayed
        }
        Option::None => 0,
    };
    let cache = store.stats();

    Ok(RunStats {
        store: store_kind,
        workload,
        run,
        threads: cfg.threads,
        elapsed,
        worker,
        flush_records: backend.records,
        flush_bytes: backend.bytes,
        flush_elapsed,
        replayed,
        cache_len: cache.len,
        dirty: cache.dirty,
        tier: None,
    })
}

fn run_tiered_workload(cfg: &Config, workload: Workload, run: usize) -> io::Result<RunStats> {
    let store_kind = StoreKind::MutableTiered;
    let tier_root = tier_bundle_path(store_kind, workload, run);
    let _guard = TempTierBundle::new(tier_root.clone(), cfg.keep_wal);
    let hot = Arc::new(MfsMutableObjectStore::with_capacity(workload_capacity(
        cfg, workload,
    )));
    prepopulate(hot.as_ref(), cfg, workload);

    let mut persistence = MutableObjectStorePersistence::open(&tier_root)?;
    let tier_report = persistence.demote_by_policy(hot.as_ref(), cfg.tier_policy)?;
    let cold_keys = collect_cold_candidates(hot.as_ref(), cfg, workload);
    let cold_read_candidates = cold_key_count(&cold_keys);
    if cfg.cold_read_pct > 0 && cold_read_candidates == 0 {
        return Err(io::Error::other(
            "mutable-tiered requested cold reads but tiering demoted no readable keys; adjust MFS_OBJ_TIER_* knobs",
        ));
    }

    let tiered_store = Arc::new(TieredBenchStore::new(
        Arc::clone(&hot),
        persistence.bundle().clone(),
        cold_keys,
        cfg.cold_read_pct,
    ));
    let (worker, elapsed) = run_workers(cfg, workload, run, Arc::clone(&tiered_store))?;

    let flush_start = Instant::now();
    let flush_records = persistence.flush_idle(hot.as_ref(), /*idle_ticks=*/ 0, usize::MAX)? as u64;
    persistence.sync_now()?;
    let flush_elapsed = flush_start.elapsed();
    let flush_bytes = file_size_if_exists(&persistence.bundle().wal_path())?;
    let replayed = persistence
        .bundle()
        .recover(workload_capacity(cfg, workload))?
        .wal_records;
    let cache = hot.stats();

    Ok(RunStats {
        store: store_kind,
        workload,
        run,
        threads: cfg.threads,
        elapsed,
        worker,
        flush_records,
        flush_bytes,
        flush_elapsed,
        replayed,
        cache_len: cache.len,
        dirty: cache.dirty,
        tier: Some(TierRunStats {
            cold_read_pct: cfg.cold_read_pct,
            cold_read_candidates,
            cold_read_attempts: tiered_store.cold_read_attempts(),
            cold_promotions: tiered_store.cold_promotions(),
            report: tier_report,
        }),
    })
}

fn run_workers<S>(
    cfg: &Config,
    workload: Workload,
    run: usize,
    store: Arc<S>,
) -> io::Result<(WorkerStats, Duration)>
where
    S: ObjectBenchStore,
{
    let start = Instant::now();
    let mut handles = Vec::with_capacity(cfg.threads);
    for thread_idx in 0..cfg.threads {
        let store = Arc::clone(&store);
        let cfg = cfg.clone();
        handles.push(thread::spawn(move || {
            run_worker(store.as_ref(), &cfg, workload, run, thread_idx)
        }));
    }

    let mut worker = WorkerStats::default();
    for handle in handles {
        let stats = handle
            .join()
            .map_err(|_| io::Error::other("object workload worker panicked"))??;
        worker.merge(stats);
    }
    Ok((worker, start.elapsed()))
}

fn run_worker<S>(
    store: &S,
    cfg: &Config,
    workload: Workload,
    run: usize,
    thread_idx: usize,
) -> io::Result<WorkerStats>
where
    S: ObjectBenchStore,
{
    let mut stats = WorkerStats::default();
    let ops = ops_for_thread(cfg.ops, cfg.threads, thread_idx);
    let mut rng = XorShift::new(0x9e37_79b9_7f4a_7c15 ^ run as u64 ^ ((thread_idx as u64) << 32));

    for op_idx in 0..ops {
        let id = pick_key(&mut rng, cfg.keys, cfg.hot_pct);
        let roll = rng.next_pct();
        let sampled = op_idx % cfg.sample_rate == 0;
        let start = sampled.then(Instant::now);
        let is_read = roll < cfg.read_pct;
        let cold_read_pct = store.cold_read_pct();
        let cold_route = is_read && cold_read_pct > 0 && rng.next_pct() < cold_read_pct;
        let checksum = match workload {
            Workload::StringHeavy => run_string_op(store, cfg, &mut rng, id, roll, cold_route),
            Workload::HashHeavy => run_hash_op(store, cfg, &mut rng, id, roll, cold_route),
            Workload::ListHeavy => run_list_op(store, cfg, &mut rng, id, roll, cold_route),
            Workload::Mixed => run_mixed_op(store, cfg, &mut rng, id, roll, cold_route),
        }?;
        if let Some(start) = start {
            stats.samples.push(Sample {
                kind: if is_read {
                    SampleKind::Read
                } else {
                    SampleKind::Mutate
                },
                nanos: start.elapsed().as_nanos(),
            });
        }
        stats.ops += 1;
        if is_read {
            stats.reads += 1;
            if checksum == 0 {
                stats.misses += 1;
            }
        } else {
            stats.mutations += 1;
        }
        stats.checksum ^= checksum;
    }

    Ok(stats)
}

fn read_key_for<S>(
    store: &S,
    prefix: u8,
    id: u64,
    rng: &mut XorShift,
    cold_route: bool,
) -> io::Result<Vec<u8>>
where
    S: ObjectBenchStore,
{
    if cold_route && let Some(key) = store.cold_read_key(prefix, rng) {
        store.promote_for_cold_read(&key)?;
        return Ok(key);
    }
    Ok(key(prefix, id))
}

fn store_error(error: ObjectStoreError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}"))
}

fn run_string_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    if roll < cfg.read_pct {
        let key = read_key_for(store, b's', id, rng, cold_route)?;
        return Ok(store
            .get_string(&key)
            .map_err(store_error)?
            .map(|value| value.len() as u64)
            .unwrap_or(0));
    }
    let key = key(b's', id);
    if roll < cfg.write_cutoff() {
        store.set_string(key, string_payload(id ^ rng.next_u64(), cfg.value_bytes));
        return Ok(1);
    }
    store.delete(key);
    Ok(1)
}

fn run_hash_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    let field = field_key(rng.next_u64() as usize % cfg.hash_fields);
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'h', id, rng, cold_route)?;
        return Ok(store
            .hash_get(&key, &field)
            .map_err(store_error)?
            .map(|value| value.len() as u64)
            .unwrap_or(0));
    }
    let key = key(b'h', id);
    if roll < cfg.write_cutoff() {
        store
            .hash_set(key, field, bytes_payload(id ^ rng.next_u64(), 16))
            .map_err(store_error)?;
        return Ok(1);
    }
    store.hash_del(key, &field).map_err(store_error)
}

fn run_list_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'l', id, rng, cold_route)?;
        if rng.next_u64() & 1 == 0 {
            return Ok(store.list_len(&key).map_err(store_error)? as u64);
        }
        return Ok(store.list_range(&key, -4, -1).map_err(store_error)?.len() as u64);
    }
    let key = key(b'l', id);
    if roll < cfg.write_cutoff() {
        store
            .list_push(key, bytes_payload(id ^ rng.next_u64(), 16))
            .map_err(store_error)?;
        return Ok(1);
    }
    if rng.next_u64() & 1 == 0 {
        Ok(store
            .list_pop_front(key)
            .map_err(store_error)?
            .map(|value| value.len() as u64)
            .unwrap_or(0))
    } else {
        Ok(store
            .list_pop_back(key)
            .map_err(store_error)?
            .map(|value| value.len() as u64)
            .unwrap_or(0))
    }
}

fn run_mixed_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    match rng.next_u64() % 8 {
        0 => run_string_op(store, cfg, rng, id, roll, cold_route),
        1 => run_integer_op(store, cfg, rng, id, roll, cold_route),
        2 => run_bytes_op(store, cfg, rng, id, roll, cold_route),
        3 => run_hash_op(store, cfg, rng, id, roll, cold_route),
        4 => run_list_op(store, cfg, rng, id, roll, cold_route),
        5 => run_set_op(store, cfg, rng, id, roll, cold_route),
        6 => run_sorted_set_op(store, cfg, rng, id, roll, cold_route),
        _ => run_json_op(store, cfg, rng, id, roll, cold_route),
    }
}

fn run_integer_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'i', id, rng, cold_route)?;
        return Ok(store
            .get_integer(&key)
            .map_err(store_error)?
            .map(|value| value as u64)
            .unwrap_or(0));
    }
    let key = key(b'i', id);
    if roll < cfg.write_cutoff() {
        return Ok(store.incr_by(key, 1).map_err(store_error)? as u64);
    }
    store.delete(key);
    Ok(1)
}

fn run_bytes_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'b', id, rng, cold_route)?;
        return Ok(store
            .get_bytes(&key)
            .map_err(store_error)?
            .map(|value| value.len() as u64)
            .unwrap_or(0));
    }
    let key = key(b'b', id);
    if roll < cfg.write_cutoff() {
        store.set_bytes(key, bytes_payload(id ^ rng.next_u64(), cfg.value_bytes));
        return Ok(1);
    }
    store.delete(key);
    Ok(1)
}

fn run_set_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    let member = member_key(rng.next_u64() % 32);
    if roll < cfg.read_pct {
        let key = read_key_for(store, b't', id, rng, cold_route)?;
        return Ok(u64::from(
            store.set_contains(&key, &member).map_err(store_error)?,
        ));
    }
    let key = key(b't', id);
    if roll < cfg.write_cutoff() {
        store.set_add(key, member).map_err(store_error)?;
        return Ok(1);
    }
    store.set_remove(key, &member).map_err(store_error)
}

fn run_sorted_set_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    let member = member_key(rng.next_u64() % 32);
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'z', id, rng, cold_route)?;
        if rng.next_u64() & 1 == 0 {
            return Ok(store.zlen(&key).map_err(store_error)? as u64);
        }
        return Ok(store.zrange(&key, 0, 3).map_err(store_error)?.len() as u64);
    }
    let key = key(b'z', id);
    if roll < cfg.write_cutoff() {
        store
            .zadd(key, (rng.next_u64() % 1_000_000) as f64, member)
            .map_err(store_error)?;
        return Ok(1);
    }
    store.zrem(key, &member).map_err(store_error)
}

fn run_json_op<S>(
    store: &S,
    cfg: &Config,
    rng: &mut XorShift,
    id: u64,
    roll: u32,
    cold_route: bool,
) -> io::Result<u64>
where
    S: ObjectBenchStore,
{
    if roll < cfg.read_pct {
        let key = read_key_for(store, b'j', id, rng, cold_route)?;
        return Ok(store
            .read_with(&key, |value| match value {
                MfsValue::Json(bytes) => bytes.len() as u64,
                _ => 0,
            })
            .unwrap_or(0));
    }
    let key = key(b'j', id);
    if roll < cfg.write_cutoff() {
        store.set_json_bytes(key, json_payload(id ^ rng.next_u64()));
        return Ok(1);
    }
    store.delete(key);
    Ok(1)
}

fn prepopulate<S>(store: &S, cfg: &Config, workload: Workload)
where
    S: ObjectBenchStore,
{
    for id in 0..cfg.keys {
        match workload {
            Workload::StringHeavy => {
                store.load_clean(
                    key(b's', id),
                    MfsValue::String(string_payload(id, cfg.value_bytes)),
                );
            }
            Workload::HashHeavy => {
                store.load_clean(key(b'h', id), MfsValue::Hash(hash_value(id, cfg)));
            }
            Workload::ListHeavy => {
                store.load_clean(key(b'l', id), MfsValue::List(list_value(id, cfg)));
            }
            Workload::Mixed => {
                store.load_clean(
                    key(b's', id),
                    MfsValue::String(string_payload(id, cfg.value_bytes)),
                );
                store.load_clean(key(b'i', id), MfsValue::Integer(id as i64));
                store.load_clean(
                    key(b'b', id),
                    MfsValue::Bytes(bytes_payload(id, cfg.value_bytes)),
                );
                store.load_clean(key(b'h', id), MfsValue::Hash(hash_value(id, cfg)));
                store.load_clean(key(b'l', id), MfsValue::List(list_value(id, cfg)));
                store.load_clean(key(b't', id), MfsValue::Set(set_value(id)));
                store.load_clean(key(b'z', id), MfsValue::SortedSet(sorted_set_value(id)));
                store.load_clean(key(b'j', id), MfsValue::Json(json_payload(id)));
            }
        }
    }
}

fn pick_key(rng: &mut XorShift, total: u64, hot_pct: u32) -> u64 {
    if rng.next_pct() < hot_pct {
        let hot_count = (total / 5).max(1);
        rng.next_u64() % hot_count
    } else {
        rng.next_u64() % total
    }
}

fn ops_for_thread(total_ops: u64, threads: usize, thread_idx: usize) -> u64 {
    let threads = threads as u64;
    let base = total_ops / threads;
    let extra = total_ops % threads;
    base + u64::from((thread_idx as u64) < extra)
}

fn store_config(cfg: &Config, initial_capacity: usize) -> WriteBehindConfig {
    WriteBehindConfig {
        dirty_shards: cfg.threads.next_power_of_two().max(1),
        initial_capacity,
        dirty_queue_capacity: usize::try_from(cfg.ops)
            .unwrap_or(usize::MAX - 1024)
            .saturating_add(1024)
            .max(16 * 1024),
    }
}

fn workload_capacity(cfg: &Config, workload: Workload) -> usize {
    let preload_entries = (cfg.keys as usize).saturating_mul(workload.key_families());
    preload_entries.saturating_mul(8).max(128)
}

fn collect_cold_candidates(
    store: &MfsMutableObjectStore,
    cfg: &Config,
    workload: Workload,
) -> BTreeMap<u8, Vec<Vec<u8>>> {
    let mut cold = BTreeMap::<u8, Vec<Vec<u8>>>::new();
    for prefix in workload_prefixes(workload) {
        for id in 0..cfg.keys {
            let key = key(*prefix, id);
            if store.get(&key).is_none() {
                cold.entry(*prefix).or_default().push(key);
            }
        }
    }
    cold
}

fn workload_prefixes(workload: Workload) -> &'static [u8] {
    match workload {
        Workload::StringHeavy => b"s",
        Workload::HashHeavy => b"h",
        Workload::ListHeavy => b"l",
        Workload::Mixed => b"sibhltzj",
    }
}

fn cold_key_count(cold: &BTreeMap<u8, Vec<Vec<u8>>>) -> usize {
    cold.values().map(Vec::len).sum()
}

fn key(prefix: u8, id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    out.push(prefix);
    out.extend_from_slice(&id.to_le_bytes());
    out
}

fn field_key(idx: usize) -> Vec<u8> {
    format!("field_{idx}").into_bytes()
}

fn member_key(idx: u64) -> Vec<u8> {
    format!("member_{idx}").into_bytes()
}

fn string_payload(seed: u64, len: usize) -> String {
    let mut out = String::with_capacity(len.max(16));
    out.push_str("user_");
    out.push_str(&format!("{seed:016x}"));
    while out.len() < len {
        out.push(char::from(b'a' + (out.len() % 26) as u8));
    }
    out
}

fn bytes_payload(seed: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| seed.wrapping_add(idx as u64) as u8)
        .collect()
}

fn hash_value(seed: u64, cfg: &Config) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut fields = BTreeMap::new();
    for idx in 0..cfg.hash_fields {
        fields.insert(field_key(idx), bytes_payload(seed ^ idx as u64, 16));
    }
    fields
}

fn list_value(seed: u64, cfg: &Config) -> Vec<Vec<u8>> {
    (0..cfg.list_items)
        .map(|idx| bytes_payload(seed ^ idx as u64, 16))
        .collect()
}

fn set_value(seed: u64) -> BTreeSet<Vec<u8>> {
    (0..8)
        .map(|idx| member_key(seed.wrapping_add(idx)))
        .collect()
}

fn sorted_set_value(seed: u64) -> Vec<SortedSetEntry> {
    (0..8)
        .map(|idx| SortedSetEntry {
            score: (seed.wrapping_add(idx) % 1_000_000) as f64,
            member: member_key(seed.wrapping_add(idx)),
        })
        .collect()
}

fn json_payload(seed: u64) -> Vec<u8> {
    format!(r#"{{"id":{seed},"active":true}}"#).into_bytes()
}

fn approx_record_bytes(record: &FlushRecord<Vec<u8>, MfsValue>) -> usize {
    record.key.len() + record.value.as_deref().map(approx_value_bytes).unwrap_or(0)
}

fn approx_value_bytes(value: &MfsValue) -> usize {
    match value {
        MfsValue::Bytes(bytes) | MfsValue::Json(bytes) => bytes.len(),
        MfsValue::String(value) => value.len(),
        MfsValue::Integer(_) => 8,
        MfsValue::List(values) => values.iter().map(Vec::len).sum(),
        MfsValue::Hash(fields) => fields
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum(),
        MfsValue::Set(values) => values.iter().map(Vec::len).sum(),
        MfsValue::SortedSet(entries) => entries.iter().map(|entry| 8 + entry.member.len()).sum(),
        MfsValue::Stream(entries) => entries
            .iter()
            .map(|entry| {
                16 + entry
                    .fields
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
            })
            .sum(),
        MfsValue::Null => 0,
    }
}

fn percentile_pair(samples: &[Sample], kind: SampleKind) -> (u128, u128) {
    let mut values = samples
        .iter()
        .filter(|sample| sample.kind == kind)
        .map(|sample| sample.nanos)
        .collect::<Vec<_>>();
    if values.is_empty() {
        return (0, 0);
    }
    values.sort_unstable();
    (percentile(&values, 50), percentile(&values, 99))
}

fn sample_count(samples: &[Sample], kind: SampleKind) -> usize {
    samples.iter().filter(|sample| sample.kind == kind).count()
}

fn percentile(sorted: &[u128], pct: usize) -> u128 {
    let idx = sorted.len().saturating_sub(1).saturating_mul(pct) / 100;
    sorted[idx]
}

struct TempTierBundle {
    path: PathBuf,
    keep: bool,
}

impl TempTierBundle {
    fn new(path: PathBuf, keep: bool) -> Self {
        Self { path, keep }
    }
}

impl Drop for TempTierBundle {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

fn tier_bundle_path(store: StoreKind, workload: Workload, run: usize) -> PathBuf {
    let pid = std::process::id();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    env::temp_dir().join(format!(
        "mfs_object_realistic_{}_{}_run{}_{}_{}",
        store.label(),
        workload.label(),
        run,
        pid,
        stamp
    ))
}

fn file_size_if_exists(path: &Path) -> io::Result<u64> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(metadata.len()),
        Ok(_) => Ok(0),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error),
    }
}

fn workload_wal_path(base: &Path, store: StoreKind, workload: Workload, run: usize) -> PathBuf {
    let pid = std::process::id();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    PathBuf::from(format!(
        "{}_{}_{}_run{}_{}_{}.wal",
        base.display(),
        store.label(),
        workload.label(),
        run,
        pid,
        stamp
    ))
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_optional_usize(name: &str) -> Option<usize> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str) -> bool {
    match env::var(name) {
        Ok(value) => matches!(value.as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4)
}
