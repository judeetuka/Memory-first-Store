use mfs_store::store::{
    DocumentVersion, DurabilityMode, MfsStoreConfig, StoreError, Lsn, MfsStore, RawKey, RawValue,
    RawWalSegmentWriter, ReadOptions, WriteOptions, replay_raw_wal, write_raw_checkpoint_to_dir,
};
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const TRIALS: usize = 3;
const KEY_COUNT: usize = 1_024;
const VALUE_BYTES: usize = 128;
const THREADS: usize = 1;
const RAW_COLLECTION: &str = "raw";

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    key_count: usize,
    value_size: usize,
    threads: usize,
    trials: usize,
}

struct Stats {
    label: &'static str,
    durability: DurabilityMode,
    count: u64,
    value_size: usize,
    threads: usize,
    trials: usize,
    min: Duration,
    median: Duration,
    max: Duration,
}

struct RawReadState {
    engine: MfsStore,
    keys: Vec<RawKey>,
}

struct RawWriteState {
    engine: MfsStore,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
}

struct SchemaWriteState {
    engine: MfsStore,
    schema: Schema,
    documents: Vec<SchemaValue>,
}

struct WalWriteState {
    engine: MfsStore,
    path: PathBuf,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
}

struct CheckpointWriteState {
    engine: MfsStore,
    dir: PathBuf,
}

struct ReplayState {
    path: PathBuf,
    capacity: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            key_count: env_usize("MFS_NOSQL_KEYS", KEY_COUNT),
            value_size: env_usize("MFS_NOSQL_VALUE_BYTES", VALUE_BYTES),
            threads: THREADS,
            trials: env_usize("MFS_NOSQL_TRIALS", TRIALS),
        }
    }
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "lane={:<34} durability={:<12} key_count={} value_size={} threads={} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op peak_lane_ops_per_sec={:.0}",
            self.label,
            self.durability.name(),
            self.count,
            self.value_size,
            self.threads,
            self.count,
            self.trials,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min),
        );
    }
}

fn main() {
    let config = BenchConfig::from_env();
    println!(
        "=== MfsStore lane benchmark (key_count={}, value_size={}, threads={}, trials={}) ===",
        config.key_count, config.value_size, config.threads, config.trials,
    );
    println!(
        "Hot reads are memory-only. WAL enqueue/sync, checkpoint write, and replay are separate lanes."
    );

    bench_raw_hot_get(config).print();
    bench_raw_memory_put(config).print();
    bench_expected_version_conflict_put(config).print();
    bench_schema_put_one_secondary_index(config).print();
    bench_schema_update_one_secondary_index(config).print();
    bench_wal_enqueue(config).print();
    bench_wal_sync(config).print();
    bench_checkpoint_write(config).print();
    bench_replay(config).print();
}

fn bench_raw_hot_get(config: BenchConfig) -> Stats {
    measure_with_state(
        "raw_hot_get",
        DurabilityMode::MemoryOnly,
        config,
        config.key_count as u64,
        |trial| raw_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for key in &state.keys {
                let read = state
                    .engine
                    .get_raw(RAW_COLLECTION, black_box(key), ReadOptions::default())
                    .expect("raw hot get")
                    .expect("preloaded raw value");
                checksum ^= read.version.get();
                checksum ^= read.value.as_bytes()[0] as u64;
            }
            checksum
        },
        drop,
    )
}

fn bench_raw_memory_put(config: BenchConfig) -> Stats {
    measure_with_state(
        "raw_memory_put",
        DurabilityMode::MemoryOnly,
        config,
        config.key_count as u64,
        |trial| raw_write_state(config, trial, DurabilityMode::MemoryOnly, None),
        |state| put_raw_values(state, DurabilityMode::MemoryOnly, None),
        drop,
    )
}

fn bench_expected_version_conflict_put(config: BenchConfig) -> Stats {
    measure_with_state(
        "expected_version_conflict_put",
        DurabilityMode::MemoryOnly,
        config,
        config.key_count as u64,
        |trial| raw_conflict_state(config, trial),
        |state| {
            let mut conflicts = 0u64;
            for (key, value) in state.keys.iter().zip(state.values.iter()) {
                let err = state
                    .engine
                    .put_raw(
                        RAW_COLLECTION,
                        black_box(key.clone()),
                        black_box(value.clone()),
                        WriteOptions {
                            durability: Some(DurabilityMode::MemoryOnly),
                            expected_version: Some(DocumentVersion::ZERO),
                        },
                    )
                    .expect_err("stale expected version must conflict");
                match err {
                    StoreError::Conflict { .. } => conflicts += 1,
                    other => panic!("unexpected conflict-lane error: {other}"),
                }
            }
            conflicts
        },
        drop,
    )
}

fn bench_schema_put_one_secondary_index(config: BenchConfig) -> Stats {
    measure_with_state(
        "schema_put_one_secondary_index",
        DurabilityMode::MemoryOnly,
        config,
        config.key_count as u64,
        |trial| schema_write_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for document in &state.documents {
                let result = state
                    .engine
                    .put_schema(
                        &state.schema,
                        black_box(document.clone()),
                        WriteOptions::default(),
                    )
                    .expect("schema put with one secondary index");
                checksum ^= result.version.get();
            }
            checksum
        },
        drop,
    )
}

fn bench_schema_update_one_secondary_index(config: BenchConfig) -> Stats {
    measure_with_state(
        "schema_update_one_secondary_index",
        DurabilityMode::MemoryOnly,
        config,
        config.key_count as u64,
        |trial| schema_update_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for document in &state.documents {
                let result = state
                    .engine
                    .put_schema(
                        &state.schema,
                        black_box(document.clone()),
                        WriteOptions::default(),
                    )
                    .expect("schema update with one secondary index");
                checksum ^= result.version.get();
            }
            checksum
        },
        drop,
    )
}

fn bench_wal_enqueue(config: BenchConfig) -> Stats {
    measure_with_state(
        "wal_enqueue",
        DurabilityMode::WalAsync,
        config,
        config.key_count as u64,
        |trial| wal_write_state(config, trial, "wal_enqueue", DurabilityMode::WalAsync),
        |state| put_raw_values(state, DurabilityMode::WalAsync, None),
        cleanup_wal_write_state,
    )
}

fn bench_wal_sync(config: BenchConfig) -> Stats {
    measure_with_state(
        "wal_sync",
        DurabilityMode::WalSync,
        config,
        config.key_count as u64,
        |trial| wal_write_state(config, trial, "wal_sync", DurabilityMode::WalSync),
        |state| put_raw_values(state, DurabilityMode::WalSync, None),
        cleanup_wal_write_state,
    )
}

fn bench_checkpoint_write(config: BenchConfig) -> Stats {
    measure_with_state(
        "checkpoint_write",
        DurabilityMode::SnapshotOnly,
        config,
        config.key_count as u64,
        |trial| checkpoint_write_state(config, trial),
        |state| {
            let metadata = write_raw_checkpoint_to_dir(
                &state.dir,
                &state.engine,
                Lsn::new(config.key_count as u64),
            )
            .expect("write raw checkpoint");
            metadata.record_count as u64
        },
        cleanup_checkpoint_write_state,
    )
}

fn bench_replay(config: BenchConfig) -> Stats {
    measure_with_state(
        "replay",
        DurabilityMode::WalSync,
        config,
        config.key_count as u64,
        |trial| replay_state(config, trial),
        |state| {
            let engine = open_engine(state.capacity, DurabilityMode::MemoryOnly, None);
            let stats = replay_raw_wal(&state.path, &engine).expect("replay raw WAL");
            stats.records as u64 ^ stats.last_lsn.get()
        },
        cleanup_replay_state,
    )
}

fn measure_with_state<T, Setup, Body, Cleanup>(
    label: &'static str,
    durability: DurabilityMode,
    config: BenchConfig,
    count: u64,
    mut setup: Setup,
    mut body: Body,
    mut cleanup: Cleanup,
) -> Stats
where
    Setup: FnMut(usize) -> T,
    Body: FnMut(&mut T) -> u64,
    Cleanup: FnMut(T),
{
    let mut samples = Vec::with_capacity(config.trials);
    for trial in 0..config.trials {
        let mut state = setup(trial);
        let start = Instant::now();
        let acc = body(&mut state);
        let elapsed = start.elapsed();
        cleanup(state);
        black_box(acc);
        samples.push(elapsed);
    }
    samples.sort_unstable();
    Stats {
        label,
        durability,
        count,
        value_size: config.value_size,
        threads: config.threads,
        trials: config.trials,
        min: samples[0],
        median: samples[samples.len() / 2],
        max: *samples.last().expect("at least one sample"),
    }
}

fn raw_read_state(config: BenchConfig, trial: usize) -> RawReadState {
    let keys = raw_keys(config.key_count, trial);
    let values = raw_values(config.key_count, config.value_size, trial);
    let engine = open_engine(
        capacity_for(config.key_count),
        DurabilityMode::MemoryOnly,
        None,
    );
    engine
        .create_raw_collection(RAW_COLLECTION)
        .expect("create raw collection");
    preload_raw(&engine, &keys, &values, DurabilityMode::MemoryOnly);

    for key in &keys {
        let _ = engine
            .get_raw(RAW_COLLECTION, key, ReadOptions::default())
            .expect("warm raw hot get");
    }

    RawReadState { engine, keys }
}

fn raw_write_state(
    config: BenchConfig,
    trial: usize,
    durability: DurabilityMode,
    wal_path: Option<PathBuf>,
) -> RawWriteState {
    let engine = open_engine(capacity_for(config.key_count), durability, wal_path);
    engine
        .create_raw_collection(RAW_COLLECTION)
        .expect("create raw collection");
    RawWriteState {
        engine,
        keys: raw_keys(config.key_count, trial),
        values: raw_values(config.key_count, config.value_size, trial),
    }
}

fn raw_conflict_state(config: BenchConfig, trial: usize) -> RawWriteState {
    let state = raw_write_state(config, trial, DurabilityMode::MemoryOnly, None);
    preload_raw(
        &state.engine,
        &state.keys,
        &state.values,
        DurabilityMode::MemoryOnly,
    );
    state
}

fn schema_write_state(config: BenchConfig, trial: usize) -> SchemaWriteState {
    let schema = indexed_schema();
    let engine = open_engine(
        capacity_for(config.key_count),
        DurabilityMode::MemoryOnly,
        None,
    );
    engine
        .create_schema_collection(&schema)
        .expect("create schema collection");
    let documents = (0..config.key_count)
        .map(|i| schema_document(i as u64, config.value_size, trial))
        .collect();
    SchemaWriteState {
        engine,
        schema,
        documents,
    }
}

fn schema_update_state(config: BenchConfig, trial: usize) -> SchemaWriteState {
    let schema = indexed_schema();
    let engine = open_engine(
        capacity_for(config.key_count),
        DurabilityMode::MemoryOnly,
        None,
    );
    engine
        .create_schema_collection(&schema)
        .expect("create schema collection");
    for i in 0..config.key_count {
        engine
            .put_schema(
                &schema,
                schema_document(i as u64, config.value_size, trial),
                WriteOptions::default(),
            )
            .expect("preload schema document");
    }
    let documents = (0..config.key_count)
        .map(|i| schema_update_document(i as u64, config.value_size, trial))
        .collect();
    SchemaWriteState {
        engine,
        schema,
        documents,
    }
}

fn wal_write_state(
    config: BenchConfig,
    trial: usize,
    label: &str,
    durability: DurabilityMode,
) -> WalWriteState {
    let path = tmp_path(label, trial, "wal");
    let raw = raw_write_state(config, trial, durability, Some(path.clone()));
    WalWriteState {
        engine: raw.engine,
        path,
        keys: raw.keys,
        values: raw.values,
    }
}

fn checkpoint_write_state(config: BenchConfig, trial: usize) -> CheckpointWriteState {
    let keys = raw_keys(config.key_count, trial);
    let values = raw_values(config.key_count, config.value_size, trial);
    let engine = open_engine(
        capacity_for(config.key_count),
        DurabilityMode::MemoryOnly,
        None,
    );
    engine
        .create_raw_collection(RAW_COLLECTION)
        .expect("create raw collection");
    preload_raw(&engine, &keys, &values, DurabilityMode::MemoryOnly);
    CheckpointWriteState {
        engine,
        dir: tmp_path("checkpoint_write", trial, "dir"),
    }
}

fn replay_state(config: BenchConfig, trial: usize) -> ReplayState {
    let path = tmp_path("replay", trial, "wal");
    let keys = raw_keys(config.key_count, trial);
    let values = raw_values(config.key_count, config.value_size, trial);
    let mut wal = RawWalSegmentWriter::open(&path).expect("open replay seed WAL");
    for (key, value) in keys.iter().zip(values.iter()) {
        wal.append_put(RAW_COLLECTION, key, value)
            .expect("append replay seed record");
    }
    wal.sync_now().expect("sync replay seed WAL");
    drop(wal);
    ReplayState {
        path,
        capacity: capacity_for(config.key_count),
    }
}

fn put_raw_values(
    state: &mut impl RawWriteParts,
    durability: DurabilityMode,
    expected_version: Option<DocumentVersion>,
) -> u64 {
    let mut checksum = 0u64;
    for (key, value) in state.keys().iter().zip(state.values().iter()) {
        let result = state
            .engine()
            .put_raw(
                RAW_COLLECTION,
                black_box(key.clone()),
                black_box(value.clone()),
                WriteOptions {
                    durability: Some(durability),
                    expected_version,
                },
            )
            .expect("raw put");
        checksum ^= result.version.get();
        checksum ^= result.lsn.map_or(0, Lsn::get);
    }
    checksum
}

trait RawWriteParts {
    fn engine(&self) -> &MfsStore;
    fn keys(&self) -> &[RawKey];
    fn values(&self) -> &[RawValue];
}

impl RawWriteParts for RawWriteState {
    fn engine(&self) -> &MfsStore {
        &self.engine
    }

    fn keys(&self) -> &[RawKey] {
        &self.keys
    }

    fn values(&self) -> &[RawValue] {
        &self.values
    }
}

impl RawWriteParts for WalWriteState {
    fn engine(&self) -> &MfsStore {
        &self.engine
    }

    fn keys(&self) -> &[RawKey] {
        &self.keys
    }

    fn values(&self) -> &[RawValue] {
        &self.values
    }
}

fn preload_raw(
    engine: &MfsStore,
    keys: &[RawKey],
    values: &[RawValue],
    durability: DurabilityMode,
) {
    for (key, value) in keys.iter().zip(values.iter()) {
        engine
            .put_raw(
                RAW_COLLECTION,
                key.clone(),
                value.clone(),
                WriteOptions {
                    durability: Some(durability),
                    expected_version: None,
                },
            )
            .expect("preload raw value");
    }
}

fn open_engine(
    raw_initial_capacity: usize,
    durability: DurabilityMode,
    wal_path: Option<PathBuf>,
) -> MfsStore {
    MfsStore::open_memory(MfsStoreConfig {
        raw_initial_capacity,
        durability,
        wal_path,
        ..MfsStoreConfig::default()
    })
    .expect("open MfsStore")
}

fn raw_keys(count: usize, trial: usize) -> Vec<RawKey> {
    (0..count)
        .map(|i| RawKey::from(((trial as u64) << 32 | i as u64).to_le_bytes().to_vec()))
        .collect()
}

fn raw_values(count: usize, value_size: usize, trial: usize) -> Vec<RawValue> {
    (0..count)
        .map(|i| RawValue::from(value_bytes(i as u64, value_size, trial)))
        .collect()
}

fn value_bytes(seed: u64, value_size: usize, trial: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; value_size];
    for (idx, byte) in bytes.iter_mut().enumerate() {
        *byte = seed.wrapping_add(trial as u64).wrapping_add(idx as u64) as u8;
    }
    bytes
}

fn indexed_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::Int64);
    id.primary = true;

    let mut shard = SchemaField::new("shard", SchemaFieldType::Int32);
    shard.indexed = true;

    let payload = SchemaField::new("payload", SchemaFieldType::Bytes);
    Schema::new("bench_docs", vec![id, shard, payload])
}

fn schema_document(i: u64, value_size: usize, trial: usize) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::Int64(i as i64)),
        ("shard".to_string(), SchemaValue::Int32((i % 64) as i32)),
        (
            "payload".to_string(),
            SchemaValue::Bytes(value_bytes(i, value_size, trial)),
        ),
    ])
}

fn schema_update_document(i: u64, value_size: usize, trial: usize) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::Int64(i as i64)),
        (
            "shard".to_string(),
            SchemaValue::Int32(((i + 1) % 64) as i32),
        ),
        (
            "payload".to_string(),
            SchemaValue::Bytes(value_bytes(i.wrapping_add(17), value_size, trial + 1)),
        ),
    ])
}

fn cleanup_wal_write_state(state: WalWriteState) {
    let path = state.path.clone();
    drop(state);
    remove_file(&path);
}

fn cleanup_checkpoint_write_state(state: CheckpointWriteState) {
    let dir = state.dir.clone();
    drop(state);
    remove_dir(&dir);
}

fn cleanup_replay_state(state: ReplayState) {
    let path = state.path.clone();
    drop(state);
    remove_file(&path);
}

fn remove_file(path: &Path) {
    let _ = fs::remove_file(path);
}

fn remove_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

fn tmp_path(label: &str, trial: usize, suffix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let pid = std::process::id();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after unix epoch")
        .as_nanos();
    path.push(format!(
        "mfs_store_bench_{label}_{trial}_{pid}_{ts}.{suffix}"
    ));
    path
}

fn capacity_for(key_count: usize) -> usize {
    key_count.saturating_mul(4).max(16)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
