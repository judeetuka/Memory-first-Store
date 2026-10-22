//! Concrete [`SchemaFlushBackend`] over SQLite.
//!
//! The planner in [`crate::schema_flush`] turns a schema plus flush records
//! into SQL. This module is the other half: a real database that executes
//! that SQL, so the hot layer can be flushed into a durable, queryable copy.
//!
//! Durability is split on purpose. The hot layer owns its own WAL and
//! checkpoints; this backend owns the *query copy*. A crash of the copy costs
//! nothing but the records that had not been flushed yet, because the hot
//! layer still has them.
//!
//! Foreign keys are **not** enforced. The copy is written in flush order,
//! which is not dependency order, so enforcing `REFERENCES` would make a
//! valid flush order fail.

use crate::schema_flush::{
    MFS_DOC_COLUMN, SchemaFlushBackend, SchemaFlushError, SchemaFlushRecord, SqlValue, delete_sql,
    delete_values, ensure_schema_sql, quote_ident, upsert_sql, upsert_values,
};
use mfs_core::Operation;
use mfs_store::schema::Schema;
use mfs_store::schema_value::SchemaValue;
use rusqlite::types::Value as SqliteValue;
use rusqlite::{Connection, OpenFlags, params, params_from_iter};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

/// Table that carries the flush watermark.
const CREATE_STATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS \"mfs_flush_state\" (
    \"mfs_flush_id\" INTEGER PRIMARY KEY CHECK (\"mfs_flush_id\" = 1),
    \"mfs_watermark\" INTEGER NOT NULL
);";

pub(crate) const SELECT_WATERMARK: &str =
    "SELECT \"mfs_watermark\" FROM \"mfs_flush_state\" WHERE \"mfs_flush_id\" = 1";

const UPSERT_WATERMARK: &str =
    "INSERT INTO \"mfs_flush_state\" (\"mfs_flush_id\", \"mfs_watermark\")
    VALUES (1, ?1)
    ON CONFLICT(\"mfs_flush_id\") DO UPDATE SET \"mfs_watermark\" = excluded.\"mfs_watermark\"";

/// Errors raised while flushing into or reading from the SQLite copy.
#[derive(Debug)]
pub enum SqliteFlushError {
    Sqlite(rusqlite::Error),
    Schema(SchemaFlushError),
    /// A record named a collection that was never registered with
    /// [`SchemaFlushBackend::ensure_schema`].
    MissingSchema {
        collection: String,
    },
    /// A stored document could not be decoded.
    Decode {
        message: String,
    },
}

impl From<rusqlite::Error> for SqliteFlushError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<SchemaFlushError> for SqliteFlushError {
    fn from(error: SchemaFlushError) -> Self {
        Self::Schema(error)
    }
}

impl fmt::Display for SqliteFlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
            Self::Schema(error) => write!(f, "schema flush error: {error}"),
            Self::MissingSchema { collection } => {
                write!(f, "collection `{collection}` has no registered schema")
            }
            Self::Decode { message } => {
                write!(f, "stored document could not be decoded: {message}")
            }
        }
    }
}

impl std::error::Error for SqliteFlushError {}

/// One row of the SQLite copy: the hot layer key, the LSN it was written at,
/// and the decoded document.
#[derive(Debug, Clone, PartialEq)]
pub struct FlushedRow {
    pub key: Vec<u8>,
    pub lsn: u64,
    pub document: SchemaValue,
}

/// SQLite backend for the schema→SQL flush planner.
///
/// One connection, one writer. The copy is owned by the process that owns the
/// hot layer; other processes read the file with their own SQLite client.
pub struct SqliteFlushBackend {
    pub(crate) conn: Connection,
    schemas: HashMap<String, Schema>,
}

impl SqliteFlushBackend {
    /// Open (or create) the copy at `path` and prepare it for writing.
    pub fn open(path: &Path) -> Result<Self, SqliteFlushError> {
        let conn = Connection::open(path)?;
        Self::configure(&conn)?;
        Self::init_state(&conn)?;
        Ok(Self {
            conn,
            schemas: HashMap::new(),
        })
    }

    /// Open a private in-memory copy. Useful for tests.
    pub fn open_in_memory() -> Result<Self, SqliteFlushError> {
        let conn = Connection::open_in_memory()?;
        Self::configure(&conn)?;
        Self::init_state(&conn)?;
        Ok(Self {
            conn,
            schemas: HashMap::new(),
        })
    }

    /// Open an existing copy read-only.
    ///
    /// This is the reader side of the single-owner rule: the process that
    /// flushes owns the write connection, everybody else opens the file.
    pub fn open_reader(path: &Path) -> Result<Self, SqliteFlushError> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        Ok(Self {
            conn,
            schemas: HashMap::new(),
        })
    }

    /// Create the table and indexes for `schema` and remember it.
    pub fn ensure_schema(&mut self, schema: &Schema) -> Result<(), SqliteFlushError> {
        for sql in ensure_schema_sql(schema)? {
            self.conn.execute_batch(&sql)?;
        }
        // A table written by an older build has no document column; add it
        // so the generic path can still store the document it was given.
        self.add_column_if_missing(&schema.name, MFS_DOC_COLUMN, "BLOB")?;
        self.schemas.insert(schema.name.clone(), schema.clone());
        Ok(())
    }

    /// Write one batch of records inside a single transaction.
    ///
    /// The batch is atomic: either every record lands or none does. The
    /// watermark moves in the same transaction, so a reader can never see a
    /// watermark that promises more than the copy holds.
    pub fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), SqliteFlushError> {
        if batch.is_empty() {
            return Ok(());
        }

        // Plan every statement before opening the transaction. A record that
        // cannot be planned (missing schema, wrong shape) must not leave a
        // half-written batch behind.
        let mut planned: Vec<(String, Vec<SqlValue>)> = Vec::with_capacity(batch.len());
        for record in batch {
            let schema = self.schemas.get(&record.collection).ok_or_else(|| {
                SqliteFlushError::MissingSchema {
                    collection: record.collection.clone(),
                }
            })?;
            planned.push(match record.op {
                Operation::Put => (upsert_sql(schema)?, upsert_values(schema, record)?),
                Operation::Delete => (delete_sql(schema)?, delete_values(record)?),
            });
        }

        let tx = self.conn.transaction()?;
        for (sql, values) in &planned {
            let params: Vec<SqliteValue> = values.iter().map(sql_value).collect();
            tx.execute(sql, params_from_iter(params.iter()))?;
        }
        let watermark = batch.iter().map(|record| record.lsn).max().unwrap_or(0);
        advance_watermark(&tx, watermark)?;
        tx.commit()?;
        Ok(())
    }

    fn configure(conn: &Connection) -> Result<(), SqliteFlushError> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA busy_timeout = 5000;
             PRAGMA wal_autocheckpoint = 1000;
             PRAGMA cache_size = -32000;
             PRAGMA temp_store = MEMORY;",
        )?;
        Ok(())
    }

    fn init_state(conn: &Connection) -> Result<(), SqliteFlushError> {
        conn.execute_batch(CREATE_STATE_TABLE)?;
        conn.execute(
            "INSERT OR IGNORE INTO \"mfs_flush_state\" (\"mfs_flush_id\", \"mfs_watermark\")
             VALUES (1, 0)",
            [],
        )?;
        Ok(())
    }

    fn add_column_if_missing(
        &self,
        table: &str,
        column: &str,
        sql_type: &str,
    ) -> Result<(), SqliteFlushError> {
        let mut stmt = self
            .conn
            .prepare(&format!("PRAGMA table_info({})", quote_ident(table)))?;
        let mut rows = stmt.query([])?;
        let mut exists = false;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                exists = true;
                break;
            }
        }
        drop(rows);
        drop(stmt);
        if !exists {
            self.conn.execute_batch(&format!(
                "ALTER TABLE {} ADD COLUMN {} {sql_type}",
                quote_ident(table),
                quote_ident(column)
            ))?;
        }
        Ok(())
    }
}

impl SchemaFlushBackend for SqliteFlushBackend {
    type Error = SqliteFlushError;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error> {
        SqliteFlushBackend::ensure_schema(self, schema)
    }

    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error> {
        SqliteFlushBackend::flush_records(self, batch)
    }
}

fn advance_watermark(tx: &rusqlite::Transaction<'_>, lsn: u64) -> Result<(), SqliteFlushError> {
    let current: i64 = match tx.query_row(SELECT_WATERMARK, [], |row| row.get(0)) {
        Ok(value) => value,
        Err(rusqlite::Error::QueryReturnedNoRows) => 0,
        Err(error) => return Err(error.into()),
    };
    let candidate = i64::try_from(lsn).map_err(|_| SchemaFlushError::LsnTooLarge { lsn })?;
    // The watermark only ever moves forward: a late batch with a lower LSN
    // must not drag the copy backwards.
    let next = current.max(candidate);
    tx.execute(UPSERT_WATERMARK, params![next])?;
    Ok(())
}

fn sql_value(value: &SqlValue) -> SqliteValue {
    match value {
        SqlValue::Null => SqliteValue::Null,
        SqlValue::Integer(value) => SqliteValue::Integer(*value),
        SqlValue::Real(value) => SqliteValue::Real(*value),
        SqlValue::Text(value) => SqliteValue::Text(value.clone()),
        SqlValue::Blob(value) => SqliteValue::Blob(value.clone()),
    }
}
