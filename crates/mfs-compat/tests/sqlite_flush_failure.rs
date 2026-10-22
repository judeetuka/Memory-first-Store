//! What happens when a batch fails: it is retried, never dropped, and it
//! never leaves the copy half-written.

mod common;

use common::{FlakyBackend, Scratch, user_document, user_schema, write_and_record};
use mfs_compat::flush_worker::{FlushConfig, FlushWorker};
use mfs_compat::schema_flush::SchemaFlushRecord;
use mfs_compat::schema_flush_sqlite::SqliteFlushBackend;
use mfs_store::schema_value::SchemaValue;
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
fn failed_batch_is_requeued_and_nothing_is_lost() {
    let scratch = Scratch::new("requeue");
    let schema = user_schema();

    let mut inner = SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy");
    inner.ensure_schema(&schema).expect("ensure schema");
    let backend = Arc::new(Mutex::new(FlakyBackend::new(inner, 2)));
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let mut records = Vec::new();
    for index in 0..5 {
        let document = user_document(
            &format!("u{index}"),
            &format!("u{index}@example.com"),
            index,
        );
        let record = SchemaFlushRecord::from_schema_write(
            "users",
            &schema,
            &SchemaValue::String(format!("u{index}")),
            Some(document),
            index as u64 + 1,
        )
        .expect("flush record");
        worker.enqueue(record.clone()).expect("enqueue");
        records.push(record);
    }

    // The barrier is only answered once the batch committed, so it also
    // proves the failed batches were retried instead of dropped.
    worker.barrier().expect("barrier");

    let guard = backend.lock();
    assert!(
        guard.attempts() >= 3,
        "the batch must be retried after a failure, attempts: {}",
        guard.attempts()
    );
    for record in &records {
        assert!(
            guard
                .reader()
                .flushed_row("users", &record.key)
                .expect("read copy")
                .is_some(),
            "a requeued record went missing"
        );
    }
    assert_eq!(worker.queued_records(), 0);
}

#[test]
fn aborted_batch_leaves_the_copy_consistent() {
    let scratch = Scratch::new("atomic");
    let schema = user_schema();

    let mut backend = SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy");
    backend.ensure_schema(&schema).expect("ensure schema");

    let good = SchemaFlushRecord::from_schema_write(
        "users",
        &schema,
        &SchemaValue::String("u1".to_string()),
        Some(user_document("u1", "ada@example.com", 10)),
        1,
    )
    .expect("flush record");
    // Same unique email as the first record: the second statement fails.
    let clash = SchemaFlushRecord::from_schema_write(
        "users",
        &schema,
        &SchemaValue::String("u2".to_string()),
        Some(user_document("u2", "ada@example.com", 11)),
        2,
    )
    .expect("flush record");

    let outcome = backend.flush_records(&[good.clone(), clash.clone()]);
    assert!(outcome.is_err(), "the clashing batch must fail");

    // The transaction is atomic: the first record is not in the copy either.
    assert!(
        backend
            .flushed_row("users", &good.key)
            .expect("read copy")
            .is_none(),
        "a failed batch must not leave rows behind"
    );
    assert_eq!(
        backend.lsn_watermark().expect("watermark"),
        0,
        "a failed batch must not advance the watermark"
    );

    // The copy is still usable: the good record flushes on its own.
    backend
        .flush_records(std::slice::from_ref(&good))
        .expect("flush");
    assert!(
        backend
            .flushed_row("users", &good.key)
            .expect("read copy")
            .is_some()
    );
    assert_eq!(backend.lsn_watermark().expect("watermark"), good.lsn);
}

#[test]
fn stale_write_does_not_move_the_copy_backwards() {
    let scratch = Scratch::new("stale");
    let schema = user_schema();

    let mut backend = SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy");
    backend.ensure_schema(&schema).expect("ensure schema");

    let fresh = SchemaFlushRecord::from_schema_write(
        "users",
        &schema,
        &SchemaValue::String("u1".to_string()),
        Some(user_document("u1", "fresh@example.com", 20)),
        20,
    )
    .expect("flush record");
    let stale = SchemaFlushRecord::from_schema_write(
        "users",
        &schema,
        &SchemaValue::String("u1".to_string()),
        Some(user_document("u1", "stale@example.com", 10)),
        10,
    )
    .expect("flush record");

    backend
        .flush_records(std::slice::from_ref(&fresh))
        .expect("flush fresh");
    backend
        .flush_records(std::slice::from_ref(&stale))
        .expect("flush stale");

    let row = backend
        .flushed_row("users", &fresh.key)
        .expect("read copy")
        .expect("row in copy");
    assert_eq!(
        row.document.field("email"),
        Some(&SchemaValue::String("fresh@example.com".into())),
        "a stale write must not overwrite a newer one"
    );
    assert_eq!(
        backend.lsn_watermark().expect("watermark"),
        fresh.lsn,
        "the watermark must not move backwards"
    );
}

#[test]
fn hot_write_survives_a_flushing_failure() {
    let scratch = Scratch::new("survive");
    let schema = user_schema();
    let store = common::hot_store(&scratch);
    store
        .create_schema_collection(&schema)
        .expect("create collection");

    let mut inner = SqliteFlushBackend::open(&scratch.join("query.db")).expect("open copy");
    inner.ensure_schema(&schema).expect("ensure schema");
    let backend = Arc::new(Mutex::new(FlakyBackend::new(inner, 1)));
    let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), fast_config())
        .expect("spawn flush worker");

    let record = write_and_record(&store, &schema, user_document("u1", "ada@example.com", 10));
    worker.enqueue(record.clone()).expect("enqueue");
    worker.barrier().expect("barrier");

    let guard = backend.lock();
    let row = guard
        .reader()
        .flushed_row("users", &record.key)
        .expect("read copy")
        .expect("row in copy");
    assert_eq!(row.document, user_document("u1", "ada@example.com", 10));
}
