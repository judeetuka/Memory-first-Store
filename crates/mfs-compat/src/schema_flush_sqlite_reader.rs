//! Reader side of the SQLite query copy.
//!
//! The hot layer never reads from here: reads come from memory. This module
//! exists for the *other* consumer of the copy -- the reporting query, the
//! ad-hoc `sqlite3` session, the second process that opened the file read-only.
//!
//! Every helper decodes documents with the store's own codec, so a value
//! written by the flush path reads back as the exact [`SchemaValue`] the hot
//! layer held. Nothing here writes; a reader that can mutate the copy has
//! broken the single-owner rule.

use crate::schema_flush::MFS_DOC_COLUMN;
use crate::schema_flush_sqlite::{FlushedRow, SqliteFlushBackend, SqliteFlushError};
use mfs_store::schema_value::{SchemaValue, decode_schema_value};
use rusqlite::types::ToSql;

impl SqliteFlushBackend {
    /// Highest LSN committed to the copy.
    ///
    /// The watermark is the high-water mark of committed records, not a
    /// contiguous prefix: it says how far the copy has seen, not that every
    /// LSN below it is present.
    pub fn lsn_watermark(&self) -> Result<u64, SqliteFlushError> {
        let value: i64 =
            self.conn
                .query_row(crate::schema_flush_sqlite::SELECT_WATERMARK, [], |row| {
                    row.get(0)
                })?;
        Ok(u64::try_from(value).unwrap_or(0))
    }

    /// Decode the documents selected by `sql`.
    ///
    /// The first selected column of every row is read as an encoded document,
    /// the same way the store's own WAL codec reads it.
    pub fn query_docs(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Vec<SchemaValue>, SqliteFlushError> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut documents = Vec::new();
        while let Some(row) = rows.next()? {
            let bytes: Vec<u8> = row.get(0)?;
            documents.push(decode_document(&bytes)?);
        }
        Ok(documents)
    }

    /// Read one typed column value for every row of `sql`.
    ///
    /// The typed columns exist so a reader can filter and join on individual
    /// fields without decoding the whole document.
    pub fn query_values<T>(
        &self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<Vec<T>, SqliteFlushError>
    where
        T: rusqlite::types::FromSql,
    {
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut values = Vec::new();
        while let Some(row) = rows.next()? {
            values.push(row.get(0)?);
        }
        Ok(values)
    }

    /// Read one row of the copy by hot-layer key.
    pub fn flushed_row(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<FlushedRow>, SqliteFlushError> {
        let sql = format!(
            "SELECT \"mfs_key\", \"mfs_lsn\", \"{}\" FROM {} WHERE \"mfs_key\" = ?1",
            MFS_DOC_COLUMN,
            crate::schema_flush::quote_ident(collection)
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([key])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let key: Vec<u8> = row.get(0)?;
        let lsn: i64 = row.get(1)?;
        let bytes: Vec<u8> = row.get(2)?;
        Ok(Some(FlushedRow {
            key,
            lsn: u64::try_from(lsn).unwrap_or(0),
            document: decode_document(&bytes)?,
        }))
    }

    /// Read one document of the copy by hot-layer key.
    pub fn document_by_key(
        &self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<SchemaValue>, SqliteFlushError> {
        Ok(self.flushed_row(collection, key)?.map(|row| row.document))
    }
}

fn decode_document(bytes: &[u8]) -> Result<SchemaValue, SqliteFlushError> {
    decode_schema_value(bytes).map_err(|error| SqliteFlushError::Decode {
        message: error.to_string(),
    })
}
