//! The hot layer, the flush worker, and the SQLite query copy.

mod common;

use common::{Scratch, hot_store, user_document, user_schema, write_and_record};
use mfs_compat::flush_worker::{FlushConfig, FlushWorker};
use mfs_compat::schema_flush::SchemaFlushRecord;
use mfs_compat::schema_flush_sqlite::SqliteFlushBackend;
use mfs_store::schema_value::SchemaValue;
use mfs_store::WriteOptions;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;

fn fast_config() -> FlushConfig {
    FlushConfig {
        batch_max: 64,
        interval: Duration::from_millis(5),
        backoff_max: Duration::from_millis(20),
        barrier_timeout: Duration::from_secs(10),
    }
}

#[test]
fn hot_write_is_visible_in_the_sqlite_copy() {
    let scratch = Scratch::new("visible");
    let schema = user_schema();
    let store = hot_store(&scratch);
    store.create_schema_collection(&schema).expect("create collection");

    let backend = Arc::new(Mutex::new(
        SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy"),
    ));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&schema).expect("ensure schema");
    }
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let document = user_document("u1", "ada@example.com", 10);
    let record = write_and_record(&store, &schema, document.clone());
    worker.enqueue(record.clone()).expect("enqueue");

    worker.barrier().expect("barrier");

    let guard = backend.lock();
    let row = guard
        .flushed_row("users", &record.key)
        .expect("read copy")
        .expect("row in copy");
    assert_eq!(row.document, document);
    assert_eq!(row.lsn, record.lsn);

    // The typed columns are there too, so SQL can filter on them.
    let emails: Vec<String> = guard
        .query_values("SELECT \"email\" FROM \"users\"", &[])
        .expect("query typed column");
    assert_eq!(emails, vec!["ada@example.com".to_string()]);
}

#[test]
fn generic_path_round_trips_the_document() {
    let scratch = Scratch::new("document");
    let schema = user_schema();
    let store = hot_store(&scratch);
    store.create_schema_collection(&schema).expect("create collection");

    let backend = Arc::new(Mutex::new(
        SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy"),
    ));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&schema).expect("ensure schema");
    }
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let document = user_document("u1", "ada@example.com", 10);
    let record = write_and_record(&store, &schema, document.clone());
    worker.enqueue(record.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    // A collection with no bespoke writer still lands with its document.
    let guard = backend.lock();
    let stored = guard
        .document_by_key("users", &record.key)
        .expect("read copy")
        .expect("document in copy");
    assert_eq!(stored, document);
    assert_eq!(stored.field("email"), Some(&SchemaValue::String("ada@example.com".into())));
}

#[test]
fn barrier_drains_everything_queued_before_it() {
    let scratch = Scratch::new("barrier");
    let schema = user_schema();
    let store = hot_store(&scratch);
    store.create_schema_collection(&schema).expect("create collection");

    let backend = Arc::new(Mutex::new(
        SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy"),
    ));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&schema).expect("ensure schema");
    }
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let mut records = Vec::new();
    for index in 0..25 {
        let document = user_document(&format!("u{index}"), &format!("u{index}@example.com"), index);
        let record = write_and_record(&store, &schema, document);
        worker.enqueue(record.clone()).expect("enqueue");
        records.push(record);
    }

    worker.barrier().expect("barrier");

    let guard = backend.lock();
    for record in &records {
        assert!(
            guard
                .flushed_row("users", &record.key)
                .expect("read copy")
                .is_some(),
            "record queued before the barrier is missing from the copy"
        );
    }
    assert_eq!(worker.queued_records(), 0);
}

#[test]
fn lsn_watermark_advances_with_the_copy() {
    let scratch = Scratch::new("watermark");
    let schema = user_schema();
    let store = hot_store(&scratch);
    store.create_schema_collection(&schema).expect("create collection");

    let backend = Arc::new(Mutex::new(
        SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy"),
    ));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&schema).expect("ensure schema");
    }
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    {
        let guard = backend.lock();
        assert_eq!(guard.lsn_watermark().expect("watermark"), 0);
    }

    let first = write_and_record(&store, &schema, user_document("u1", "ada@example.com", 10));
    worker.enqueue(first.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    let second = write_and_record(&store, &schema, user_document("u2", "grace@example.com", 11));
    worker.enqueue(second.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    let guard = backend.lock();
    let watermark = guard.lsn_watermark().expect("watermark");
    assert_eq!(watermark, second.lsn);
    assert!(watermark > first.lsn);
}

#[test]
fn deleted_document_leaves_the_copy() {
    let scratch = Scratch::new("delete");
    let schema = user_schema();
    let store = hot_store(&scratch);
    store.create_schema_collection(&schema).expect("create collection");

    let backend = Arc::new(Mutex::new(
        SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy"),
    ));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&schema).expect("ensure schema");
    }
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let put = write_and_record(&store, &schema, user_document("u1", "ada@example.com", 10));
    worker.enqueue(put.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    let deleted = store
        .delete_schema(
            &schema,
            &SchemaValue::String("u1".to_string()),
            WriteOptions::default(),
        )
        .expect("delete schema document");
    let delete_record = SchemaFlushRecord::from_schema_write(
        "users",
        &schema,
        &SchemaValue::String("u1".to_string()),
        None,
        deleted.lsn.expect("wal lsn").get(),
    )
    .expect("delete record");
    worker.enqueue(delete_record.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    let guard = backend.lock();
    assert!(
        guard
            .flushed_row("users", &delete_record.key)
            .expect("read copy")
            .is_none(),
        "a deleted document must not survive in the copy"
    );
}
