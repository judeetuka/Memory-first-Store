//! Query-engine lane benchmark for the NoSQL engine.
//!
//! Measures throughput for range queries, sort, pagination, count,
//! multi-get, and partial updates under the schema-mode path.
//!
//! Run: `cargo bench -p mfs-db --bench mfs_nosql_query --all-features`
//! or:  `make bench-nosql-query`

use mfs_db::engine::{
    EngineConfig, FieldUpdate, FieldUpdateOp, FilterClause, FilterOp, NoSqlEngine, QueryOptions,
    ReadOptions, SortDirection, WriteOptions,
};
use mfs_db::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_db::schema_value::SchemaValue;
use std::hint::black_box;
use std::time::{Duration, Instant};

const TRIALS: usize = 5;
const KEY_COUNT: usize = 10_000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    key_count: usize,
    trials: usize,
}

struct Stats {
    label: &'static str,
    count: u64,
    trials: usize,
    min: Duration,
    median: Duration,
    max: Duration,
}

// Per-lane state types.

struct QueryEqState {
    engine: NoSqlEngine,
    schema: Schema,
    /// Keys to query for (each lands on a different age bucket).
    filter_values: Vec<SchemaValue>,
}

struct QueryRangeState {
    engine: NoSqlEngine,
    schema: Schema,
    /// Age thresholds to filter by.
    thresholds: Vec<i64>,
}

struct QuerySortState {
    engine: NoSqlEngine,
    schema: Schema,
}

struct QueryPaginateState {
    engine: NoSqlEngine,
    schema: Schema,
}

struct CountUnfilteredState {
    engine: NoSqlEngine,
    schema: Schema,
}

struct CountFilteredState {
    engine: NoSqlEngine,
    schema: Schema,
    filters: Vec<FilterClause>,
}

struct MultiGetState {
    engine: NoSqlEngine,
    schema: Schema,
    /// Batches of primary keys to retrieve.
    key_batches: Vec<Vec<SchemaValue>>,
}

struct UpdateSetState {
    engine: NoSqlEngine,
    schema: Schema,
    updates: Vec<(SchemaValue, FieldUpdateOp)>,
}

struct UpdateIncrementState {
    engine: NoSqlEngine,
    schema: Schema,
    updates: Vec<(SchemaValue, FieldUpdateOp)>,
}

// ---------------------------------------------------------------------------
// BenchConfig
// ---------------------------------------------------------------------------

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            key_count: env_usize("MFS_NOSQL_QUERY_KEYS", KEY_COUNT),
            trials: env_usize("MFS_NOSQL_QUERY_TRIALS", TRIALS),
        }
    }
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "lane={:<34} keys={} trials={} min={:.1} ns/op median={:.1} ns/op max={:.1} ns/op peak_ops_per_sec={:.0}",
            self.label,
            self.count,
            self.trials,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min),
        );
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn open_engine(_capacity: usize) -> NoSqlEngine {
    NoSqlEngine::open_memory(EngineConfig::default()).expect("open NoSqlEngine")
}

fn bench_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::Int64);
    id.primary = true;
    id.indexed = true;
    id.unique = true;

    let mut name = SchemaField::new("name", SchemaFieldType::String);
    name.indexed = true;

    let age = SchemaField::new("age", SchemaFieldType::Int64);

    Schema::new("bench_users", vec![id, name, age])
}

fn bench_document(id: i64, name: &str, age: i64) -> SchemaValue {
    SchemaValue::object([
        ("id".into(), SchemaValue::Int64(id)),
        ("name".into(), SchemaValue::String(name.to_string())),
        ("age".into(), SchemaValue::Int64(age)),
    ])
}

fn seed_documents(engine: &NoSqlEngine, schema: &Schema, count: usize) {
    for i in 0..count {
        let name = format!("user{i}");
        let age = (i % 100) as i64;
        engine
            .put_schema(
                schema,
                bench_document(i as i64, &name, age),
                WriteOptions::default(),
            )
            .expect("seed document");
    }
}

// ---------------------------------------------------------------------------
// measure_with_state — generic measurement harness (same pattern as nosql_engine)
// ---------------------------------------------------------------------------

fn measure_with_state<T, Setup, Body, Cleanup>(
    label: &'static str,
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
        count,
        trials: config.trials,
        min: samples[0],
        median: samples[samples.len() / 2],
        max: *samples.last().expect("at least one sample"),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let config = BenchConfig::from_env();
    println!(
        "=== NoSqlEngine query lane benchmark (keys={}, trials={}) ===",
        config.key_count, config.trials,
    );
    println!(
        "All operations are MemoryOnly — measuring in-memory query throughput only."
    );

    bench_query_eq_indexed(&config).print();
    bench_query_range_scan(&config).print();
    bench_query_sort(&config).print();
    bench_query_paginate(&config).print();
    bench_count_unfiltered(&config).print();
    bench_count_filtered(&config).print();
    bench_multi_get(&config).print();
    bench_update_set_field(&config).print();
    bench_update_increment(&config).print();
}

// ---------------------------------------------------------------------------
// Lane: query_schema with EQ filter (index-accelerated)
// ---------------------------------------------------------------------------

fn bench_query_eq_indexed(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(100) as u64;
    measure_with_state(
        "query_eq_indexed",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            // Pick distinct age values to query (each picks ~1% of docs).
            let filter_values: Vec<_> = (0..count)
                .map(|i| SchemaValue::Int64((i % 100) as i64))
                .collect();
            QueryEqState {
                engine,
                schema,
                filter_values,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for fv in &state.filter_values {
                let result = state
                    .engine
                    .query_schema(
                        &state.schema,
                        QueryOptions {
                            filter: Some(FilterClause {
                                field: "age".into(),
                                op: FilterOp::Eq,
                                value: black_box(fv.clone()),
                            }),
                            sort_field: None,
                            sort_direction: SortDirection::Asc,
                            limit: None,
                            offset: None,
                        },
                    )
                    .expect("query_schema eq");
                checksum ^= result.documents.len() as u64;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: query_schema with GT range filter (full-scan)
// ---------------------------------------------------------------------------

fn bench_query_range_scan(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(100) as u64;
    measure_with_state(
        "query_range_scan",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            let thresholds: Vec<_> = (0..count).map(|i| (i % 100) as i64).collect();
            QueryRangeState {
                engine,
                schema,
                thresholds,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for &thresh in &state.thresholds {
                let result = state
                    .engine
                    .query_schema(
                        &state.schema,
                        QueryOptions {
                            filter: Some(FilterClause {
                                field: "age".into(),
                                op: FilterOp::Gt,
                                value: black_box(SchemaValue::Int64(thresh)),
                            }),
                            sort_field: None,
                            sort_direction: SortDirection::Asc,
                            limit: None,
                            offset: None,
                        },
                    )
                    .expect("query_schema gt");
                checksum ^= result.documents.len() as u64;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: query_schema with sort (field-based, full materialization)
// ---------------------------------------------------------------------------

fn bench_query_sort(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(100) as u64;
    measure_with_state(
        "query_sort",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            QuerySortState { engine, schema }
        },
        |state| {
            let mut checksum = 0u64;
            for i in 0..count {
                let result = state
                    .engine
                    .query_schema(
                        &state.schema,
                        QueryOptions {
                            filter: None,
                            sort_field: Some(black_box("age".into())),
                            sort_direction: if i % 2 == 0 {
                                SortDirection::Asc
                            } else {
                                SortDirection::Desc
                            },
                            limit: None,
                            offset: None,
                        },
                    )
                    .expect("query_schema sort");
                checksum ^= result.documents.len() as u64;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: query_schema with limit/offset pagination
// ---------------------------------------------------------------------------

fn bench_query_paginate(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(100) as u64;
    measure_with_state(
        "query_paginate",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            QueryPaginateState { engine, schema }
        },
        |state| {
            let mut checksum = 0u64;
            // Rotate through pages of 50.
            for i in 0..count {
                let result = state
                    .engine
                    .query_schema(
                        &state.schema,
                        QueryOptions {
                            filter: None,
                            sort_field: None,
                            sort_direction: SortDirection::Asc,
                            limit: Some(black_box(50)),
                            offset: Some(black_box(((i % 200) * 50) as usize)),
                        },
                    )
                    .expect("query_schema paginate");
                checksum ^= result.documents.len() as u64;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: count_schema without filter (O(1) atomic counter)
// ---------------------------------------------------------------------------

fn bench_count_unfiltered(config: &BenchConfig) -> Stats {
    let count = 10_000u64.min(config.key_count as u64);
    measure_with_state(
        "count_unfiltered",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            CountUnfilteredState { engine, schema }
        },
        |state| {
            let mut checksum = 0u64;
            for _ in 0..count {
                let n = state
                    .engine
                    .count_schema(&state.schema, black_box(None))
                    .expect("count_schema unfiltered");
                checksum ^= n;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: count_schema with filter (scans and counts matches)
// ---------------------------------------------------------------------------

fn bench_count_filtered(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(100) as u64;
    measure_with_state(
        "count_filtered",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            let filters: Vec<_> = (0..count)
                .map(|i| FilterClause {
                    field: "age".into(),
                    op: FilterOp::Gt,
                    value: SchemaValue::Int64((i % 100) as i64),
                })
                .collect();
            CountFilteredState {
                engine,
                schema,
                filters,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for f in &state.filters {
                let n = state
                    .engine
                    .count_schema(
                        &state.schema,
                        black_box(Some(f.clone())),
                    )
                    .expect("count_schema filtered");
                checksum ^= n;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: multi_get_schema (batch read by primary keys)
// ---------------------------------------------------------------------------

fn bench_multi_get(config: &BenchConfig) -> Stats {
    let batch_count = config.key_count.min(500) as u64;
    let batch_size = 16usize;
    measure_with_state(
        "multi_get",
        *config,
        batch_count,
        |trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            // Build key batches, cycling through the document space.
            let offset = (trial * batch_size) % config.key_count;
            let batches: Vec<Vec<SchemaValue>> = (0..batch_count as usize)
                .map(|b| {
                    let base = (offset + b * batch_size) % config.key_count;
                    (0..batch_size)
                        .map(|k| {
                            let idx = (base + k) % config.key_count;
                            SchemaValue::Int64(idx as i64)
                        })
                        .collect()
                })
                .collect();
            MultiGetState {
                engine,
                schema,
                key_batches: batches,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for keys in &state.key_batches {
                let results = state
                    .engine
                    .multi_get_schema(
                        &state.schema,
                        black_box(keys.as_slice()),
                        ReadOptions::default(),
                    )
                    .expect("multi_get_schema");
                checksum ^= results.len() as u64;
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: update_schema with $set (read-mutate-CAS)
// ---------------------------------------------------------------------------

fn bench_update_set_field(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(500) as u64;
    measure_with_state(
        "update_set",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            let updates: Vec<_> = (0..count)
                .map(|i| {
                    let key = SchemaValue::Int64(i as i64);
                    let op = FieldUpdateOp {
                        updates: vec![FieldUpdate::Set {
                            field: "name".into(),
                            value: SchemaValue::String(format!("updated{i}")),
                        }],
                    };
                    (key, op)
                })
                .collect();
            UpdateSetState {
                engine,
                schema,
                updates,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for (key, op) in &state.updates {
                let result = state
                    .engine
                    .update_schema(
                        &state.schema,
                        black_box(key),
                        black_box(op.clone()),
                        WriteOptions::default(),
                    )
                    .expect("update_schema set");
                checksum ^= result.version.get();
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Lane: update_schema with $inc (read-mutate-CAS)
// ---------------------------------------------------------------------------

fn bench_update_increment(config: &BenchConfig) -> Stats {
    let count = config.key_count.min(500) as u64;
    measure_with_state(
        "update_increment",
        *config,
        count,
        |_trial| {
            let engine = open_engine(config.key_count * 4);
            let schema = bench_schema();
            engine.create_schema_collection(&schema).expect("create");
            seed_documents(&engine, &schema, config.key_count);
            let updates: Vec<_> = (0..count)
                .map(|i| {
                    let key = SchemaValue::Int64(i as i64);
                    let op = FieldUpdateOp {
                        updates: vec![FieldUpdate::Increment {
                            field: "age".into(),
                            delta: 1,
                        }],
                    };
                    (key, op)
                })
                .collect();
            UpdateIncrementState {
                engine,
                schema,
                updates,
            }
        },
        |state| {
            let mut checksum = 0u64;
            for (key, op) in &state.updates {
                let result = state
                    .engine
                    .update_schema(
                        &state.schema,
                        black_box(key),
                        black_box(op.clone()),
                        WriteOptions::default(),
                    )
                    .expect("update_schema increment");
                checksum ^= result.version.get();
            }
            checksum
        },
        drop,
    )
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}
