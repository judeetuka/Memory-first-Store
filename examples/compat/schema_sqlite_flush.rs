//! Flush the hot layer into a durable, queryable SQLite copy.
//!
//! The hot layer answers reads from memory and owns its own WAL. This example
//! mirrors the same writes into a SQLite file, in the background, so a report,
//! an ad-hoc `sqlite3` session, or a second process can query them.
//!
//! The shape is always the same:
//!
//! 1. `SqliteFlushBackend::open` owns the write connection.
//! 2. `ensure_schema` once per collection, before anything is queued.
//! 3. `FlushWorker::spawn` starts the background writer.
//! 4. Every hot write becomes a `SchemaFlushRecord` and is enqueued.
//! 5. `barrier` before reading, if the read must see everything.
//! 6. `shutdown` drains the queue and stops the worker.

use mfs_compat::flush_worker::{FlushConfig, FlushWorker};
use mfs_compat::schema_flush::SchemaFlushRecord;
use mfs_compat::schema_flush_sqlite::SqliteFlushBackend;
use mfs_store::schema::{Reference, Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;
use mfs_store::{DurabilityMode, MfsStore, MfsStoreConfig, ReadOptions, WriteOptions};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
enum ExampleError {
    Sqlite(mfs_compat::schema_flush_sqlite::SqliteFlushError),
    Flush(mfs_compat::flush_worker::FlushWorkerError),
    Store(mfs_store::StoreError),
    Schema(mfs_compat::schema_flush::SchemaFlushError),
}

impl std::fmt::Display for ExampleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite: {error}"),
            Self::Flush(error) => write!(f, "flush: {error}"),
            Self::Store(error) => write!(f, "store: {error}"),
            Self::Schema(error) => write!(f, "schema: {error}"),
        }
    }
}

impl std::error::Error for ExampleError {}

impl From<mfs_compat::schema_flush_sqlite::SqliteFlushError> for ExampleError {
    fn from(error: mfs_compat::schema_flush_sqlite::SqliteFlushError) -> Self {
        Self::Sqlite(error)
    }
}

impl From<mfs_compat::flush_worker::FlushWorkerError> for ExampleError {
    fn from(error: mfs_compat::flush_worker::FlushWorkerError) -> Self {
        Self::Flush(error)
    }
}

impl From<mfs_store::StoreError> for ExampleError {
    fn from(error: mfs_store::StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<mfs_compat::schema_flush::SchemaFlushError> for ExampleError {
    fn from(error: mfs_compat::schema_flush::SchemaFlushError) -> Self {
        Self::Schema(error)
    }
}

fn companies_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;
    let name = SchemaField::new("name", SchemaFieldType::String);
    Schema::new("companies", vec![id, name])
}

fn users_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;
    let mut email = SchemaField::new("email", SchemaFieldType::String);
    email.indexed = true;
    email.unique = true;
    let mut company_id = SchemaField::new("company_id", SchemaFieldType::String);
    company_id.indexed = true;
    company_id.reference = Some(Reference::new("companies", "id"));
    let mut created_at = SchemaField::new("created_at", SchemaFieldType::Int64);
    created_at.indexed = true;
    Schema::new("users", vec![id, email, company_id, created_at])
}

fn company(id: &str, name: &str) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("name".to_string(), SchemaValue::String(name.to_string())),
    ])
}

fn user(id: &str, email: &str, company_id: &str, created_at: i64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("email".to_string(), SchemaValue::String(email.to_string())),
        (
            "company_id".to_string(),
            SchemaValue::String(company_id.to_string()),
        ),
        ("created_at".to_string(), SchemaValue::Int64(created_at)),
    ])
}

/// A throwaway directory, so the example does not litter the tree.
fn scratch_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("mfs-flush-example-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&path).expect("create scratch directory");
    path
}

/// Write one document to the hot layer and return the record for the copy.
///
/// The LSN is the store-wide WAL LSN, not a per-document version: the copy's
/// watermark is store-wide, so a per-document number would make it lie.
fn write_and_enqueue(
    store: &MfsStore,
    schema: &Schema,
    document: SchemaValue,
    worker: &FlushWorker<SqliteFlushBackend>,
) -> Result<Vec<u8>, ExampleError> {
    let primary = document
        .field(&schema.primary_field().expect("primary field").name)
        .expect("primary value")
        .clone();
    let result = store.put_schema(schema, document.clone(), WriteOptions::default())?;
    let lsn = result.lsn.expect("wal lsn").get();
    let record = SchemaFlushRecord::from_schema_write(
        schema.name.clone(),
        schema,
        &primary,
        Some(document),
        lsn,
    )?;
    let key = record.key.clone();
    worker.enqueue(record)?;
    Ok(key)
}

fn run_demo() -> Result<(), ExampleError> {
    let scratch = scratch_dir();
    let companies = companies_schema();
    let users = users_schema();

    // The hot layer. It owns its own WAL and answers reads from memory; the
    // SQLite copy below is a separate durability domain.
    let config = MfsStoreConfig::default()
        .with_durability(DurabilityMode::WalAsync)
        .with_wal_path(scratch.join("hot.wal"));
    let store = MfsStore::open_memory(config)?;
    store.create_schema_collection(&companies)?;
    store.create_schema_collection(&users)?;

    // The copy. One connection, one writer, WAL + synchronous NORMAL.
    let backend = Arc::new(Mutex::new(SqliteFlushBackend::open(&scratch.join("query.db"))?));
    {
        let mut guard = backend.lock();
        guard.ensure_schema(&companies)?;
        guard.ensure_schema(&users)?;
    }

    let worker = FlushWorker::spawn_with_config(
        Arc::clone(&backend),
        FlushConfig {
            batch_max: 500,
            interval: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            barrier_timeout: Duration::from_secs(10),
        },
    )?;

    let company_key =
        write_and_enqueue(&store, &companies, company("c1", "Acme"), &worker)?;
    let user_key =
        write_and_enqueue(&store, &users, user("u1", "ada@example.com", "c1", 10), &worker)?;

    // The hot reads are answered from memory and never touch SQLite.
    let from_hot = store
        .get_schema(&users, &SchemaValue::String("u1".to_string()), ReadOptions::default())?
        .expect("user in hot layer")
        .document;
    assert_eq!(
        from_hot.field("email"),
        Some(&SchemaValue::String("ada@example.com".to_string()))
    );

    // A barrier is answered only after everything queued before it committed.
    worker.barrier()?;

    {
        let guard = backend.lock();
        let watermark = guard.lsn_watermark()?;
        println!("copy watermark: LSN {watermark}");

        // A join, which is the whole point of a query copy. The typed columns
        // let SQL correlate the two tables without decoding a document.
        let joined: Vec<String> = guard.query_values(
            "SELECT u.email || ' @ ' || c.name FROM users u \
             JOIN companies c ON u.company_id = c.id WHERE u.id = 'u1'",
            &[],
        )?;
        assert_eq!(joined, vec!["ada@example.com @ Acme".to_string()]);

        // The document round-trips through the store's own codec.
        let stored = guard
            .document_by_key("users", &user_key)?
            .expect("user in copy");
        assert_eq!(stored, from_hot);

        // Typed columns, so SQL can filter without decoding the whole document.
        let emails: Vec<String> = guard.query_values("SELECT email FROM users", &[])?;
        assert_eq!(emails, vec!["ada@example.com".to_string()]);
    }

    // A delete leaves the copy, not just the hot layer.
    let deleted = store.delete_schema(
        &users,
        &SchemaValue::String("u1".to_string()),
        WriteOptions::default(),
    )?;
    let delete_record = SchemaFlushRecord::from_schema_write(
        "users",
        &users,
        &SchemaValue::String("u1".to_string()),
        None,
        deleted.lsn.expect("wal lsn").get(),
    )?;
    worker.enqueue(delete_record)?;
    worker.barrier()?;

    {
        let guard = backend.lock();
        assert!(
            guard.flushed_row("users", &user_key)?.is_none(),
            "a deleted document must not survive in the copy"
        );
        assert!(
            guard.flushed_row("companies", &company_key)?.is_some(),
            "the company row must still be there"
        );
    }

    // Drain the queue and stop the worker. Only now may the copy fall behind.
    worker.shutdown()?;
    println!("schema SQLite flush example completed");
    Ok(())
}

fn main() -> Result<(), ExampleError> {
    run_demo()?;
    Ok(())
}
