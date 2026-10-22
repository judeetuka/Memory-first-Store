//! Shared fixtures for the SQLite flush tests.

#![allow(dead_code)]

use mfs_compat::schema_flush::{SchemaFlushBackend, SchemaFlushRecord};
use mfs_compat::schema_flush_sqlite::{SqliteFlushBackend, SqliteFlushError};
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;
use mfs_store::{DurabilityMode, MfsStore, MfsStoreConfig, WriteOptions};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A schema with a primary key and a unique field, so a flush can violate
/// the unique constraint on purpose.
pub fn user_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;
    let mut email = SchemaField::new("email", SchemaFieldType::String);
    email.indexed = true;
    email.unique = true;
    let mut created_at = SchemaField::new("created_at", SchemaFieldType::Int64);
    created_at.indexed = true;
    Schema::new("users", vec![id, email, created_at])
}

pub fn user_document(id: &str, email: &str, created_at: i64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("email".to_string(), SchemaValue::String(email.to_string())),
        ("created_at".to_string(), SchemaValue::Int64(created_at)),
    ])
}

/// A scratch directory that cleans itself up.
pub struct Scratch {
    path: PathBuf,
}

impl Scratch {
    pub fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("mfs-flush-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create scratch directory");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Hot layer with WAL durability, so every write carries a store-wide LSN.
pub fn hot_store(scratch: &Scratch) -> MfsStore {
    let config = MfsStoreConfig::default()
        .with_durability(DurabilityMode::WalAsync)
        .with_wal_path(scratch.join("hot.wal"));
    MfsStore::open_memory(config).expect("open hot store")
}

/// Write a document to the hot layer and return the flush record for it.
pub fn write_and_record(
    store: &MfsStore,
    schema: &Schema,
    document: SchemaValue,
) -> SchemaFlushRecord {
    let primary = document
        .field(&schema.primary_field().expect("primary field").name)
        .expect("primary value")
        .clone();
    let result = store
        .put_schema(schema, document.clone(), WriteOptions::default())
        .expect("put schema document");
    let lsn = result.lsn.expect("wal lsn").get();
    SchemaFlushRecord::from_schema_write(schema.name.clone(), schema, &primary, Some(document), lsn)
        .expect("flush record")
}

/// Backend that fails the first `failures` batches and then delegates.
///
/// It stands in for a database that is briefly unavailable: the batch has to
/// survive the failure and be retried, not dropped.
pub struct FlakyBackend {
    inner: SqliteFlushBackend,
    failures: Mutex<usize>,
    attempts: AtomicUsize,
}

/// Error of [`FlakyBackend`].
#[derive(Debug)]
pub enum FlakyError {
    Injected,
    Sqlite(SqliteFlushError),
}

impl fmt::Display for FlakyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Injected => write!(f, "injected flush failure"),
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
        }
    }
}

impl std::error::Error for FlakyError {}

impl From<SqliteFlushError> for FlakyError {
    fn from(error: SqliteFlushError) -> Self {
        Self::Sqlite(error)
    }
}

impl FlakyBackend {
    pub fn new(inner: SqliteFlushBackend, failures: usize) -> Self {
        Self {
            inner,
            failures: Mutex::new(failures),
            attempts: AtomicUsize::new(0),
        }
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    /// The copy behind the flaky wrapper, for readers.
    pub fn reader(&self) -> &SqliteFlushBackend {
        &self.inner
    }
}

impl SchemaFlushBackend for FlakyBackend {
    type Error = FlakyError;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error> {
        self.inner.ensure_schema(schema).map_err(FlakyError::from)
    }

    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let mut failures = self.failures.lock().expect("failure counter");
        if *failures > 0 {
            *failures -= 1;
            return Err(FlakyError::Injected);
        }
        self.inner.flush_records(batch).map_err(FlakyError::from)
    }
}
