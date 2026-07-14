use mfs_compat::schema_flush::{
    SchemaFlushRecord, SqlValue, create_table_sql, ensure_schema_sql, upsert_sql, upsert_values,
};
use mfs_compat::schema_store::{SchemaKey, SchemaStore};
use mfs_core::durability::{WalBackend, WalConfig};
use mfs_core::writeback::{WriteBehindCache, WriteBehindConfig, WriteBehindStats};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use mfs_store::schema::{Reference, Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::{
    SchemaValue, SchemaValueCodec, decode_schema_value, encode_schema_value,
};
use mfs_store::value::MfsValue;
use rusqlite::types::Value;
use rusqlite::{Connection, params, params_from_iter};
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const COUNT: u64 = 10_000;
const TRIALS: usize = 5;

struct Stats {
    label: &'static str,
    count: u64,
    min: Duration,
    median: Duration,
    max: Duration,
}

struct NullBackend;

impl FlushBackend<SchemaKey, SchemaValue> for NullBackend {
    type Error = ();

    fn flush(
        &mut self,
        _records: &[FlushRecord<SchemaKey, SchemaValue>],
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "{:<34} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op (peak ops/s={:.0})",
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
    let mut samples = Vec::with_capacity(TRIALS);
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

fn measure_with_setup<T, Setup, Body>(
    label: &'static str,
    count: u64,
    mut setup: Setup,
    mut body: Body,
) -> Stats
where
    Setup: FnMut() -> T,
    Body: FnMut(T) -> u64,
{
    let mut samples = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let state = setup();
        let start = Instant::now();
        let acc = body(state);
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

fn config() -> WriteBehindConfig {
    WriteBehindConfig {
        initial_capacity: 1_000_000,
        dirty_queue_capacity: COUNT as usize,
        ..WriteBehindConfig::default()
    }
}

fn user_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::Int64);
    id.primary = true;
    id.indexed = true;
    id.unique = true;

    let mut email = SchemaField::new("email", SchemaFieldType::String);
    email.indexed = true;
    email.unique = true;

    let mut company_id = SchemaField::new("company_id", SchemaFieldType::String);
    company_id.indexed = true;
    company_id.reference = Some(Reference::new("companies", "id"));

    let mut age = SchemaField::new("age", SchemaFieldType::Int32);
    age.indexed = true;
    age.optional = true;

    Schema::new("users", vec![id, email, company_id, age])
}

fn company_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;
    let name = SchemaField::new("name", SchemaFieldType::String);
    Schema::new("companies", vec![id, name])
}

fn document(i: u64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::Int64(i as i64)),
        (
            "email".to_string(),
            SchemaValue::String(format!("u{i}@example.com")),
        ),
        (
            "company_id".to_string(),
            SchemaValue::String(format!("c{}", i % 128)),
        ),
        ("age".to_string(), SchemaValue::Int32((i % 128) as i32)),
    ])
}

fn company(i: u64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(format!("c{i}"))),
        (
            "name".to_string(),
            SchemaValue::String(format!("Company {i}")),
        ),
    ])
}

fn populate() -> SchemaStore {
    let store = SchemaStore::with_config(config());
    store.register_collection(company_schema()).unwrap();
    store.register_collection(user_schema()).unwrap();
    for i in 0..128 {
        store.load_clean("companies", company(i)).unwrap();
    }
    for i in 0..COUNT {
        store.load_clean("users", document(i)).unwrap();
    }
    store
}

fn raw_schema_cache() -> WriteBehindCache<Vec<u8>, SchemaValue> {
    let cache = WriteBehindCache::<Vec<u8>, SchemaValue>::with_config(config());
    let pinned = cache.pin();
    for i in 0..COUNT {
        pinned.load_clean(SchemaKey::Int64(i as i64).encoded(), document(i));
    }
    drop(pinned);
    cache
}

fn raw_object_cache() -> WriteBehindCache<Vec<u8>, MfsValue> {
    let cache = WriteBehindCache::<Vec<u8>, MfsValue>::with_config(config());
    let pinned = cache.pin();
    for i in 0..COUNT {
        pinned.load_clean(
            SchemaKey::Int64(i as i64).encoded(),
            MfsValue::String(format!("u{i}@example.com")),
        );
    }
    drop(pinned);
    cache
}

fn wal_path() -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("mfs_schema_store_bench_{ts}.wal"));
    path
}

fn write_wal(path: &std::path::Path) {
    let mut wal = WalBackend::open(path, SchemaValueCodec, WalConfig::default()).unwrap();
    let records = (0..COUNT)
        .map(|i| FlushRecord {
            key: SchemaKey::Int64(i as i64).encoded(),
            value: Some(Arc::new(document(i))),
            version: i + 1,
            op: Operation::Put,
        })
        .collect::<Vec<_>>();
    wal.flush(&records).unwrap();
    wal.sync_now().unwrap();
}

fn sqlite_value(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(value) => Value::Integer(value),
        SqlValue::Real(value) => Value::Real(value),
        SqlValue::Text(value) => Value::Text(value),
        SqlValue::Blob(value) => Value::Blob(value),
    }
}

fn sqlite_flush_setup() -> (Connection, String, Vec<Vec<Value>>) {
    let users = user_schema();
    let companies = company_schema();
    let conn = Connection::open_in_memory().unwrap();
    for sql in ensure_schema_sql(&companies).unwrap() {
        conn.execute_batch(&sql).unwrap();
    }
    for sql in ensure_schema_sql(&users).unwrap() {
        conn.execute_batch(&sql).unwrap();
    }
    for i in 0..128u64 {
        let id = format!("c{i}");
        let name = format!("Company {i}");
        let key = SchemaKey::String(id.clone()).encoded();
        conn.execute(
            "INSERT INTO \"companies\" (\"id\", \"name\", \"mfs_key\", \"mfs_lsn\") VALUES (?1, ?2, ?3, ?4)",
            params![id, name, key, 1i64],
        )
        .unwrap();
    }
    let sql = upsert_sql(&users).unwrap();
    let params = (0..COUNT)
        .map(|i| {
            let record = SchemaFlushRecord {
                collection: "users".to_string(),
                key: SchemaKey::Int64(i as i64).encoded(),
                op: Operation::Put,
                document: Some(document(i)),
                lsn: i + 1,
            };
            upsert_values(&users, &record)
                .unwrap()
                .into_iter()
                .map(sqlite_value)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    (conn, sql, params)
}

fn main() {
    println!("=== SchemaStore benchmark (COUNT={COUNT}, trials={TRIALS}) ===");

    let schema = user_schema();
    let sample = document(0);
    measure("schema_validate", COUNT, || {
        let mut checksum = 0u64;
        for _ in 0..COUNT {
            checksum ^= schema.validate().is_ok() as u64;
        }
        checksum
    })
    .print();

    measure("document_validate", COUNT, || {
        let mut checksum = 0u64;
        for _ in 0..COUNT {
            checksum ^= sample.validate_against(&schema).is_ok() as u64;
        }
        checksum
    })
    .print();

    let store = populate();
    measure("schema_store_get", COUNT, || {
        let mut checksum = 0u64;
        for i in 0..COUNT {
            let key = SchemaKey::Int64(i as i64);
            checksum ^= store
                .read_with("users", &key, |doc| match doc.field("age") {
                    Some(SchemaValue::Int32(age)) => *age as u64,
                    _ => 0,
                })
                .unwrap()
                .unwrap_or(0);
        }
        checksum
    })
    .print();

    measure("schema_store_lookup", COUNT, || {
        let mut total = 0u64;
        for i in 0..COUNT {
            let keys = store
                .lookup("users", "age", &SchemaValue::Int32((i % 128) as i32))
                .unwrap();
            total = total.wrapping_add(keys.len() as u64);
        }
        total
    })
    .print();

    measure("schema_include_one", COUNT, || {
        let mut count = 0u64;
        for i in 0..COUNT {
            let key = SchemaKey::Int64(i as i64);
            if store
                .include_one("users", &key, "company_id")
                .unwrap()
                .and_then(|included| included.referenced)
                .is_some()
            {
                count += 1;
            }
        }
        count
    })
    .print();

    measure("schema_include_reverse", COUNT, || {
        let mut count = 0u64;
        for i in 0..COUNT {
            let key = SchemaKey::String(format!("c{}", i % 128));
            count += store
                .include_reverse("companies", &key, "users", "company_id")
                .unwrap()
                .len() as u64;
        }
        count
    })
    .print();

    let encoded = (0..COUNT)
        .map(|i| {
            let mut out = Vec::new();
            encode_schema_value(&document(i), &mut out);
            out
        })
        .collect::<Vec<_>>();
    measure("schema_value_decode", COUNT, || {
        let mut checksum = 0u64;
        for bytes in &encoded {
            checksum ^= decode_schema_value(bytes).unwrap().kind() as u64;
        }
        checksum
    })
    .print();

    let path = wal_path();
    write_wal(&path);
    measure("schema_wal_replay", COUNT, || {
        let mut count = 0u64;
        WalBackend::<Vec<u8>, SchemaValue, SchemaValueCodec>::replay(
            &path,
            &SchemaValueCodec,
            |_| {
                count += 1;
            },
        )
        .unwrap();
        count
    })
    .print();
    std::fs::remove_file(&path).ok();

    let upsert_record = SchemaFlushRecord {
        collection: "users".to_string(),
        key: SchemaKey::Int64(0).encoded(),
        op: Operation::Put,
        document: Some(document(0)),
        lsn: 1,
    };
    measure("schema_sql_plan", COUNT, || {
        let mut checksum = create_table_sql(&schema).unwrap().len() as u64;
        checksum ^= upsert_sql(&schema).unwrap().len() as u64;
        for _ in 0..COUNT {
            checksum ^= upsert_values(&schema, &upsert_record).unwrap().len() as u64;
        }
        checksum
    })
    .print();

    measure_with_setup(
        "schema_sqlite_flush",
        COUNT,
        sqlite_flush_setup,
        |(mut conn, sql, params)| {
            let tx = conn.transaction().unwrap();
            {
                let mut stmt = tx.prepare_cached(&sql).unwrap();
                for values in params {
                    stmt.execute(params_from_iter(values.iter())).unwrap();
                }
            }
            tx.commit().unwrap();
            COUNT
        },
    )
    .print();

    measure_with_setup(
        "schema_flush_scan",
        COUNT,
        || {
            let flush_store = SchemaStore::with_config(config());
            flush_store.register_collection(user_schema()).unwrap();
            for i in 0..COUNT {
                flush_store.upsert("users", document(i)).unwrap();
            }
            flush_store
        },
        |flush_store| {
            let mut backend = NullBackend;
            flush_store
                .flush_collection_idle("users", &mut backend, 0, COUNT as usize)
                .unwrap() as u64
        },
    )
    .print();

    let raw = raw_schema_cache();
    measure("raw_schema_read_with", COUNT, || {
        let pinned = raw.pin();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= pinned
                .read_with(&SchemaKey::Int64(i as i64).encoded(), |doc| {
                    match doc.field("age") {
                        Some(SchemaValue::Int32(age)) => *age as u64,
                        _ => 0,
                    }
                })
                .unwrap_or(0);
        }
        checksum
    })
    .print();

    let object = raw_object_cache();
    measure("object_value_read_with", COUNT, || {
        let pinned = object.pin();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= pinned
                .read_with(&SchemaKey::Int64(i as i64).encoded(), |value| match value {
                    MfsValue::String(email) => email.len() as u64,
                    _ => 0,
                })
                .unwrap_or(0);
        }
        checksum
    })
    .print();

    let thread_count = std::env::var("MFS_SCHEMA_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .max(1);
    measure_with_setup(
        "schema_threaded_put",
        COUNT,
        || {
            let threaded = Arc::new(SchemaStore::with_config(WriteBehindConfig {
                initial_capacity: 1_000_000,
                dirty_queue_capacity: COUNT as usize,
                ..WriteBehindConfig::default()
            }));
            threaded.register_collection(user_schema()).unwrap();
            threaded
        },
        |threaded| {
            let barrier = Arc::new(Barrier::new(thread_count));
            let mut handles = Vec::new();
            for thread_id in 0..thread_count {
                let store = Arc::clone(&threaded);
                let barrier = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    barrier.wait();
                    let start = thread_id * COUNT as usize / thread_count;
                    let end = (thread_id + 1) * COUNT as usize / thread_count;
                    for i in start..end {
                        store
                            .upsert("users", document((i + COUNT as usize) as u64))
                            .unwrap();
                    }
                    (end - start) as u64
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum()
        },
    )
    .print();

    measure_with_setup("schema_store_upsert", COUNT, populate, |store| {
        for i in 0..COUNT {
            store.upsert("users", black_box(document(i))).unwrap();
        }
        store
            .stats("users")
            .unwrap_or(WriteBehindStats {
                len: 0,
                dirty: 0,
                logical_clock: 0,
            })
            .len as u64
    })
    .print();
}
