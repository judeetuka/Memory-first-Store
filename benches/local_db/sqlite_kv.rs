use mfs_db::engine::{
    DurabilityMode, EngineConfig, Lsn, NoSqlEngine, RawKey, RawValue, ReadOptions, WriteOptions,
};
use redb::{
    Database as RedbDatabase, Durability as RedbDurability, ReadableDatabase, TableDefinition,
};
use rusqlite::{Connection, params};
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const KEY_COUNT: usize = 1_024;
const VALUE_BYTES: usize = 128;
const TRIALS: usize = 5;
const RAW_COLLECTION: &str = "raw";
const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    key_count: usize,
    value_size: usize,
    trials: usize,
}

struct Stats {
    label: &'static str,
    count: u64,
    value_size: usize,
    trials: usize,
    min: Duration,
    median: Duration,
    max: Duration,
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            key_count: env_usize("MFS_LOCAL_DB_KEYS", KEY_COUNT),
            value_size: env_usize("MFS_LOCAL_DB_VALUE_BYTES", VALUE_BYTES),
            trials: env_usize("MFS_LOCAL_DB_TRIALS", TRIALS).max(1),
        }
    }
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "lane={:<34} key_count={} value_size={} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op peak_lane_ops_per_sec={:.0}",
            self.label,
            self.count,
            self.value_size,
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
        "=== Local DB KV benchmark (key_count={}, value_size={}, trials={}) ===",
        config.key_count, config.value_size, config.trials,
    );
    println!(
        "SQLite/redb/fjall run in-process as libraries; no services or external binaries are used."
    );
    println!(
        "Read lanes are split into borrowed/view first-byte reads and owned/materialized value reads where the API allows it."
    );
    println!(
        "`*_tx` write lanes commit once per full key batch; `*_autocommit` lanes commit once per key. MfS `wal_enqueue` is buffered and not fsync-durable; `wal_sync` is per-key sync."
    );

    bench_mfs_raw_read_owned(config).print();
    bench_sqlite_memory_read_owned(config).print();
    bench_redb_read_view(config).print();
    bench_fjall_read_view(config).print();
    bench_redb_read_owned(config).print();
    bench_fjall_read_owned(config).print();
    bench_mfs_raw_memory_put(config).print();
    bench_sqlite_memory_put_tx(config).print();
    bench_redb_none_put_tx(config).print();
    bench_fjall_buffer_put(config).print();
    bench_sqlite_memory_put_autocommit(config).print();
    bench_mfs_wal_enqueue_buffered(config).print();
    bench_sqlite_wal_normal_put_tx(config).print();
    bench_fjall_sync_data_put_tx(config).print();
    bench_mfs_wal_sync_per_put(config).print();
    bench_sqlite_wal_full_put_autocommit(config).print();
    bench_redb_immediate_put_autocommit(config).print();
    bench_fjall_sync_all_put_autocommit(config).print();
}

fn bench_mfs_raw_read_owned(config: BenchConfig) -> Stats {
    measure(
        "mfs_raw_read_owned",
        config,
        |trial| mfs_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for key in &state.keys {
                let read = state
                    .engine
                    .get_raw(RAW_COLLECTION, black_box(key), ReadOptions::default())
                    .expect("mfs raw get")
                    .expect("preloaded value");
                checksum ^= read.version.get();
                checksum ^= read.value.as_bytes()[0] as u64;
            }
            checksum
        },
        |_| {},
    )
}

fn bench_mfs_raw_memory_put(config: BenchConfig) -> Stats {
    measure(
        "mfs_raw_memory_put",
        config,
        |trial| mfs_write_state(config, trial, DurabilityMode::MemoryOnly, None),
        |state| put_mfs_values(state, DurabilityMode::MemoryOnly),
        |_| {},
    )
}

fn bench_mfs_wal_enqueue_buffered(config: BenchConfig) -> Stats {
    measure(
        "mfs_wal_enqueue_buffered",
        config,
        |trial| {
            let path = tmp_path("mfs_wal_enqueue", trial, "wal");
            mfs_write_state(config, trial, DurabilityMode::WalAsync, Some(path))
        },
        |state| put_mfs_values(state, DurabilityMode::WalAsync),
        cleanup_mfs_write_state,
    )
}

fn bench_mfs_wal_sync_per_put(config: BenchConfig) -> Stats {
    measure(
        "mfs_wal_sync_per_put",
        config,
        |trial| {
            let path = tmp_path("mfs_wal_sync", trial, "wal");
            mfs_write_state(config, trial, DurabilityMode::WalSync, Some(path))
        },
        |state| put_mfs_values(state, DurabilityMode::WalSync),
        cleanup_mfs_write_state,
    )
}

fn bench_sqlite_memory_read_owned(config: BenchConfig) -> Stats {
    measure(
        "sqlite_memory_read_owned",
        config,
        |trial| sqlite_memory_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            let mut statement = state
                .connection
                .prepare_cached("SELECT value FROM kv WHERE key = ?1")
                .expect("prepare sqlite read");
            for key in &state.keys {
                let value: Vec<u8> = statement
                    .query_row(params![black_box(key.as_bytes())], |row| row.get(0))
                    .expect("sqlite get");
                checksum ^= value[0] as u64;
            }
            checksum
        },
        |_| {},
    )
}

fn bench_redb_read_view(config: BenchConfig) -> Stats {
    measure(
        "redb_read_view",
        config,
        |trial| redb_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            let read_txn = state.db.begin_read().expect("begin redb read");
            let table = read_txn.open_table(REDB_TABLE).expect("open redb table");
            for key in &state.keys {
                let value = table
                    .get(black_box(key.as_bytes()))
                    .expect("redb get")
                    .expect("preloaded redb value");
                checksum ^= value.value()[0] as u64;
            }
            checksum
        },
        cleanup_redb_state,
    )
}

fn bench_fjall_read_view(config: BenchConfig) -> Stats {
    measure(
        "fjall_read_view",
        config,
        |trial| fjall_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for key in &state.keys {
                let value = state
                    .keyspace
                    .get(black_box(key.as_bytes()))
                    .expect("fjall get")
                    .expect("preloaded fjall value");
                checksum ^= value[0] as u64;
            }
            checksum
        },
        cleanup_fjall_state,
    )
}

fn bench_redb_read_owned(config: BenchConfig) -> Stats {
    measure(
        "redb_read_owned",
        config,
        |trial| redb_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            let read_txn = state.db.begin_read().expect("begin redb read");
            let table = read_txn.open_table(REDB_TABLE).expect("open redb table");
            for key in &state.keys {
                let value = table
                    .get(black_box(key.as_bytes()))
                    .expect("redb get")
                    .expect("preloaded redb value")
                    .value()
                    .to_vec();
                checksum ^= value[0] as u64;
            }
            checksum
        },
        cleanup_redb_state,
    )
}

fn bench_fjall_read_owned(config: BenchConfig) -> Stats {
    measure(
        "fjall_read_owned",
        config,
        |trial| fjall_read_state(config, trial),
        |state| {
            let mut checksum = 0u64;
            for key in &state.keys {
                let value = state
                    .keyspace
                    .get(black_box(key.as_bytes()))
                    .expect("fjall get")
                    .expect("preloaded fjall value")
                    .to_vec();
                checksum ^= value[0] as u64;
            }
            checksum
        },
        cleanup_fjall_state,
    )
}

fn bench_sqlite_memory_put_tx(config: BenchConfig) -> Stats {
    measure(
        "sqlite_memory_put_tx",
        config,
        |trial| sqlite_memory_write_state(config, trial),
        |state| {
            sqlite_put_transaction(
                state,
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?1, ?2)",
            )
        },
        |_| {},
    )
}

fn bench_redb_none_put_tx(config: BenchConfig) -> Stats {
    measure(
        "redb_none_put_tx",
        config,
        |trial| redb_write_state(config, trial, "redb_none"),
        |state| redb_put_transaction(state, RedbDurability::None),
        cleanup_redb_state,
    )
}

fn bench_fjall_buffer_put(config: BenchConfig) -> Stats {
    measure(
        "fjall_buffer_put",
        config,
        |trial| fjall_write_state(config, trial, "fjall_buffer"),
        |state| fjall_put_values(state, None),
        cleanup_fjall_state,
    )
}

fn bench_sqlite_memory_put_autocommit(config: BenchConfig) -> Stats {
    measure(
        "sqlite_memory_put_autocommit",
        config,
        |trial| sqlite_memory_write_state(config, trial),
        |state| {
            sqlite_put_autocommit(
                state,
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?1, ?2)",
            )
        },
        |_| {},
    )
}

fn bench_sqlite_wal_normal_put_tx(config: BenchConfig) -> Stats {
    measure(
        "sqlite_wal_normal_put_tx",
        config,
        |trial| {
            let path = tmp_path("sqlite_wal_normal", trial, "sqlite");
            sqlite_file_write_state(config, trial, path, "NORMAL")
        },
        |state| {
            sqlite_put_transaction(
                state,
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?1, ?2)",
            )
        },
        cleanup_sqlite_state,
    )
}

fn bench_fjall_sync_data_put_tx(config: BenchConfig) -> Stats {
    measure(
        "fjall_sync_data_put_tx",
        config,
        |trial| fjall_write_state(config, trial, "fjall_sync_data"),
        |state| fjall_put_values(state, Some(fjall::PersistMode::SyncData)),
        cleanup_fjall_state,
    )
}

fn bench_sqlite_wal_full_put_autocommit(config: BenchConfig) -> Stats {
    measure(
        "sqlite_wal_full_put_autocommit",
        config,
        |trial| {
            let path = tmp_path("sqlite_wal_full", trial, "sqlite");
            sqlite_file_write_state(config, trial, path, "FULL")
        },
        |state| {
            sqlite_put_autocommit(
                state,
                "INSERT OR REPLACE INTO kv(key, value) VALUES(?1, ?2)",
            )
        },
        cleanup_sqlite_state,
    )
}

fn bench_redb_immediate_put_autocommit(config: BenchConfig) -> Stats {
    measure(
        "redb_immediate_put_autocommit",
        config,
        |trial| redb_write_state(config, trial, "redb_immediate"),
        redb_put_immediate_autocommit,
        cleanup_redb_state,
    )
}

fn bench_fjall_sync_all_put_autocommit(config: BenchConfig) -> Stats {
    measure(
        "fjall_sync_all_put_autocommit",
        config,
        |trial| fjall_write_state(config, trial, "fjall_sync_all"),
        fjall_put_sync_all_autocommit,
        cleanup_fjall_state,
    )
}

fn measure<State>(
    label: &'static str,
    config: BenchConfig,
    mut setup: impl FnMut(usize) -> State,
    mut body: impl FnMut(&mut State) -> u64,
    mut cleanup: impl FnMut(State),
) -> Stats {
    let mut samples = Vec::with_capacity(config.trials);
    for trial in 0..config.trials {
        let mut state = setup(trial);
        let start = Instant::now();
        let checksum = body(&mut state);
        let elapsed = start.elapsed();
        black_box(checksum);
        cleanup(state);
        samples.push(elapsed);
    }
    samples.sort_unstable();
    Stats {
        label,
        count: config.key_count as u64,
        value_size: config.value_size,
        trials: config.trials,
        min: samples[0],
        median: samples[samples.len() / 2],
        max: samples[samples.len() - 1],
    }
}

struct MfsWriteState {
    engine: NoSqlEngine,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
    wal_path: Option<PathBuf>,
}

struct MfsReadState {
    engine: NoSqlEngine,
    keys: Vec<RawKey>,
}

fn mfs_read_state(config: BenchConfig, trial: usize) -> MfsReadState {
    let keys = raw_keys(config.key_count, trial);
    let values = raw_values(config.key_count, config.value_size, trial);
    let engine = open_mfs_engine(config.key_count, DurabilityMode::MemoryOnly, None);
    engine
        .create_raw_collection(RAW_COLLECTION)
        .expect("create mfs collection");
    preload_mfs(&engine, &keys, &values);
    MfsReadState { engine, keys }
}

fn mfs_write_state(
    config: BenchConfig,
    trial: usize,
    durability: DurabilityMode,
    wal_path: Option<PathBuf>,
) -> MfsWriteState {
    let keys = raw_keys(config.key_count, trial);
    let values = raw_values(config.key_count, config.value_size, trial);
    let engine = open_mfs_engine(config.key_count, durability, wal_path.clone());
    engine
        .create_raw_collection(RAW_COLLECTION)
        .expect("create mfs collection");
    MfsWriteState {
        engine,
        keys,
        values,
        wal_path,
    }
}

fn open_mfs_engine(
    key_count: usize,
    durability: DurabilityMode,
    wal_path: Option<PathBuf>,
) -> NoSqlEngine {
    NoSqlEngine::open_memory(EngineConfig {
        raw_initial_capacity: key_count.saturating_mul(4).max(64),
        durability,
        wal_path,
        ..EngineConfig::default()
    })
    .expect("open mfs engine")
}

fn preload_mfs(engine: &NoSqlEngine, keys: &[RawKey], values: &[RawValue]) {
    for (key, value) in keys.iter().zip(values.iter()) {
        engine
            .put_raw(
                RAW_COLLECTION,
                key.clone(),
                value.clone(),
                WriteOptions {
                    durability: Some(DurabilityMode::MemoryOnly),
                    expected_version: None,
                },
            )
            .expect("preload mfs raw value");
    }
}

fn put_mfs_values(state: &mut MfsWriteState, durability: DurabilityMode) -> u64 {
    let mut checksum = 0u64;
    for (key, value) in state.keys.iter().zip(state.values.iter()) {
        let result = state
            .engine
            .put_raw(
                RAW_COLLECTION,
                black_box(key.clone()),
                black_box(value.clone()),
                WriteOptions {
                    durability: Some(durability),
                    expected_version: None,
                },
            )
            .expect("mfs put");
        checksum ^= result.version.get();
        checksum ^= result.lsn.map_or(0, Lsn::get);
    }
    checksum
}

fn cleanup_mfs_write_state(state: MfsWriteState) {
    drop(state.engine);
    if let Some(path) = state.wal_path {
        remove_sqlite_family(&path);
        let _ = fs::remove_file(path);
    }
}

struct SqliteState {
    connection: Connection,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
    path: Option<PathBuf>,
}

fn sqlite_memory_read_state(config: BenchConfig, trial: usize) -> SqliteState {
    let mut state = sqlite_memory_write_state(config, trial);
    preload_sqlite(&mut state);
    state
}

fn sqlite_memory_write_state(config: BenchConfig, trial: usize) -> SqliteState {
    let connection = Connection::open_in_memory().expect("open sqlite memory db");
    configure_sqlite_memory(&connection);
    init_sqlite_schema(&connection);
    SqliteState {
        connection,
        keys: raw_keys(config.key_count, trial),
        values: raw_values(config.key_count, config.value_size, trial),
        path: None,
    }
}

fn sqlite_file_write_state(
    config: BenchConfig,
    trial: usize,
    path: PathBuf,
    synchronous: &str,
) -> SqliteState {
    let _ = fs::remove_file(&path);
    let connection = Connection::open(&path).expect("open sqlite file db");
    configure_sqlite_wal(&connection, synchronous);
    init_sqlite_schema(&connection);
    SqliteState {
        connection,
        keys: raw_keys(config.key_count, trial),
        values: raw_values(config.key_count, config.value_size, trial),
        path: Some(path),
    }
}

fn configure_sqlite_memory(connection: &Connection) {
    connection
        .pragma_update(None, "journal_mode", "MEMORY")
        .expect("set sqlite journal_mode memory");
    connection
        .pragma_update(None, "synchronous", "OFF")
        .expect("set sqlite synchronous off");
}

fn configure_sqlite_wal(connection: &Connection, synchronous: &str) {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .expect("set sqlite journal_mode wal");
    connection
        .pragma_update(None, "synchronous", synchronous)
        .expect("set sqlite synchronous mode");
}

fn init_sqlite_schema(connection: &Connection) {
    connection
        .execute_batch("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;")
        .expect("create sqlite kv table");
}

fn preload_sqlite(state: &mut SqliteState) {
    let _ = sqlite_put_transaction(state, "INSERT INTO kv(key, value) VALUES(?1, ?2)");
}

fn sqlite_put_transaction(state: &mut SqliteState, sql: &str) -> u64 {
    let transaction = state
        .connection
        .transaction()
        .expect("begin sqlite transaction");
    let mut checksum = 0u64;
    {
        let mut statement = transaction
            .prepare_cached(sql)
            .expect("prepare sqlite insert");
        for (key, value) in state.keys.iter().zip(state.values.iter()) {
            statement
                .execute(params![
                    black_box(key.as_bytes()),
                    black_box(value.as_bytes())
                ])
                .expect("sqlite insert transaction");
            checksum ^= value.as_bytes()[0] as u64;
        }
    }
    transaction.commit().expect("commit sqlite transaction");
    checksum
}

fn sqlite_put_autocommit(state: &mut SqliteState, sql: &str) -> u64 {
    let mut statement = state
        .connection
        .prepare_cached(sql)
        .expect("prepare sqlite insert");
    let mut checksum = 0u64;
    for (key, value) in state.keys.iter().zip(state.values.iter()) {
        statement
            .execute(params![
                black_box(key.as_bytes()),
                black_box(value.as_bytes())
            ])
            .expect("sqlite insert autocommit");
        checksum ^= value.as_bytes()[0] as u64;
    }
    checksum
}

fn cleanup_sqlite_state(state: SqliteState) {
    let path = state.path.clone();
    drop(state);
    if let Some(path) = path {
        remove_sqlite_family(&path);
    }
}

struct RedbState {
    db: RedbDatabase,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
    path: PathBuf,
}

fn redb_read_state(config: BenchConfig, trial: usize) -> RedbState {
    let mut state = redb_write_state(config, trial, "redb_read");
    let _ = redb_put_transaction(&mut state, RedbDurability::None);
    state
}

fn redb_write_state(config: BenchConfig, trial: usize, label: &str) -> RedbState {
    let path = tmp_path(label, trial, "redb");
    let _ = fs::remove_file(&path);
    let db = RedbDatabase::create(&path).expect("create redb database");
    init_redb_schema(&db);
    RedbState {
        db,
        keys: raw_keys(config.key_count, trial),
        values: raw_values(config.key_count, config.value_size, trial),
        path,
    }
}

fn init_redb_schema(db: &RedbDatabase) {
    let write_txn = db.begin_write().expect("begin redb schema transaction");
    {
        let _ = write_txn.open_table(REDB_TABLE).expect("open redb table");
    }
    write_txn.commit().expect("commit redb schema transaction");
}

fn redb_put_transaction(state: &mut RedbState, durability: RedbDurability) -> u64 {
    let mut write_txn = state.db.begin_write().expect("begin redb transaction");
    write_txn
        .set_durability(durability)
        .expect("set redb durability");
    let mut checksum = 0u64;
    {
        let mut table = write_txn.open_table(REDB_TABLE).expect("open redb table");
        for (key, value) in state.keys.iter().zip(state.values.iter()) {
            table
                .insert(black_box(key.as_bytes()), black_box(value.as_bytes()))
                .expect("redb insert");
            checksum ^= value.as_bytes()[0] as u64;
        }
    }
    write_txn.commit().expect("commit redb transaction");
    checksum
}

fn redb_put_immediate_autocommit(state: &mut RedbState) -> u64 {
    let mut checksum = 0u64;
    for (key, value) in state.keys.iter().zip(state.values.iter()) {
        let mut write_txn = state.db.begin_write().expect("begin redb transaction");
        write_txn
            .set_durability(RedbDurability::Immediate)
            .expect("set redb durability");
        {
            let mut table = write_txn.open_table(REDB_TABLE).expect("open redb table");
            table
                .insert(black_box(key.as_bytes()), black_box(value.as_bytes()))
                .expect("redb insert");
        }
        write_txn.commit().expect("commit redb transaction");
        checksum ^= value.as_bytes()[0] as u64;
    }
    checksum
}

fn cleanup_redb_state(state: RedbState) {
    let path = state.path.clone();
    drop(state);
    let _ = fs::remove_file(path);
}

struct FjallState {
    db: fjall::Database,
    keyspace: fjall::Keyspace,
    keys: Vec<RawKey>,
    values: Vec<RawValue>,
    path: PathBuf,
}

fn fjall_read_state(config: BenchConfig, trial: usize) -> FjallState {
    let mut state = fjall_write_state(config, trial, "fjall_read");
    let _ = fjall_put_values(&mut state, None);
    state
}

fn fjall_write_state(config: BenchConfig, trial: usize, label: &str) -> FjallState {
    let path = tmp_path(label, trial, "fjall");
    let _ = fs::remove_dir_all(&path);
    let db = fjall::Database::builder(&path)
        .worker_threads(1)
        .open()
        .expect("open fjall database");
    let keyspace = db
        .keyspace("kv", fjall::KeyspaceCreateOptions::default)
        .expect("open fjall keyspace");
    FjallState {
        db,
        keyspace,
        keys: raw_keys(config.key_count, trial),
        values: raw_values(config.key_count, config.value_size, trial),
        path,
    }
}

fn fjall_put_values(state: &mut FjallState, persist: Option<fjall::PersistMode>) -> u64 {
    let mut checksum = 0u64;
    for (key, value) in state.keys.iter().zip(state.values.iter()) {
        state
            .keyspace
            .insert(black_box(key.as_bytes()), black_box(value.as_bytes()))
            .expect("fjall insert");
        checksum ^= value.as_bytes()[0] as u64;
    }
    if let Some(persist) = persist {
        state.db.persist(persist).expect("persist fjall database");
    }
    checksum
}

fn fjall_put_sync_all_autocommit(state: &mut FjallState) -> u64 {
    let mut checksum = 0u64;
    for (key, value) in state.keys.iter().zip(state.values.iter()) {
        state
            .keyspace
            .insert(black_box(key.as_bytes()), black_box(value.as_bytes()))
            .expect("fjall insert");
        state
            .db
            .persist(fjall::PersistMode::SyncAll)
            .expect("persist fjall database");
        checksum ^= value.as_bytes()[0] as u64;
    }
    checksum
}

fn cleanup_fjall_state(state: FjallState) {
    let path = state.path.clone();
    drop(state);
    let _ = fs::remove_dir_all(path);
}

fn remove_sqlite_family(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite-shm"));
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

fn value_bytes(seed: u64, len: usize, trial: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| seed.wrapping_add(offset as u64).wrapping_add(trial as u64) as u8)
        .collect()
}

fn tmp_path(label: &str, trial: usize, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "mfs-local-db-{label}-{}-{trial}-{nanos}.{extension}",
        std::process::id()
    ));
    path
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}
