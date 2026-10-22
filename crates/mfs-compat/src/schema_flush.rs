//! Schema-aware SQL flush planning.

use crate::schema_store::SchemaKey;
use mfs_core::{FlushRecord, Operation};
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::{
    SchemaValue, SchemaValueError, encode_schema_value, validate_codec_safe,
};
use mfs_store::{StoreError, schema_primary_key_raw_key};
use std::collections::HashMap;
use std::fmt;

const MFS_KEY_COLUMN: &str = "mfs_key";
const MFS_LSN_COLUMN: &str = "mfs_lsn";

/// Column that carries the encoded source document.
///
/// The typed columns exist so SQL can filter and join on individual fields,
/// but a generic collection has no bespoke writer to reproduce them. The
/// encoded document is the lossless copy: it is what a reader decodes when it
/// wants the document the hot layer actually holds.
pub const MFS_DOC_COLUMN: &str = "doc";

pub trait SchemaFlushBackend {
    type Error;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error>;
    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaFlushRecord {
    pub collection: String,
    pub key: Vec<u8>,
    pub op: Operation,
    pub document: Option<SchemaValue>,
    pub lsn: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaFlushError {
    InvalidSchema(String),
    MissingDocument {
        collection: String,
    },
    MissingPrimaryField {
        collection: String,
    },
    ReservedColumnName {
        collection: String,
        field: String,
        column: String,
    },
    DuplicateColumnName {
        collection: String,
        first: String,
        second: String,
        column: String,
    },
    MissingField {
        collection: String,
        field: String,
    },
    TypeMismatch {
        field: String,
        expected: &'static str,
    },
    NonFiniteFloat {
        field: String,
    },
    LsnTooLarge {
        lsn: u64,
    },
    EncodeValue {
        field: String,
        error: SchemaValueError,
    },
    /// The document itself is not encodable (non-finite float, too deep, ...).
    ///
    /// Kept separate from [`Self::EncodeValue`] because the failure is about
    /// the whole document, not about one schema field.
    EncodeDocument {
        error: SchemaValueError,
    },
    /// The primary key could not be encoded into the hot layer's key bytes.
    KeyEncoding {
        error: StoreError,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumn {
    pub field_name: String,
    pub column_name: String,
    pub sql_type: &'static str,
    pub primary: bool,
    pub unique: bool,
    pub indexed: bool,
    pub reference: Option<(String, String)>,
}

impl SchemaFlushRecord {
    pub fn from_flush_record(
        collection: impl Into<String>,
        record: &FlushRecord<SchemaKey, SchemaValue>,
    ) -> Self {
        Self {
            collection: collection.into(),
            key: record.key.encoded(),
            op: record.op,
            document: record.value.as_ref().map(|value| value.as_ref().clone()),
            lsn: record.version,
        }
    }

    /// Build a flush record for a schema write the hot layer just committed.
    ///
    /// `primary_key` is the primary field value; the encoded key bytes are
    /// derived with the same encoding the hot layer uses, so the copy can be
    /// matched against a store read.
    ///
    /// `lsn` must be the store-wide WAL LSN of the write (see
    /// [`mfs_store::WriteResult::lsn`]). The flush copy tracks the highest
    /// committed LSN as its watermark, and a per-document version would make
    /// that watermark meaningless.
    ///
    /// A `None` document means the record is a delete: the copy drops the row
    /// instead of writing one.
    pub fn from_schema_write(
        collection: impl Into<String>,
        schema: &Schema,
        primary_key: &SchemaValue,
        document: Option<SchemaValue>,
        lsn: u64,
    ) -> Result<Self, SchemaFlushError> {
        let key = schema_primary_key_raw_key(schema, primary_key)
            .map_err(|error| SchemaFlushError::KeyEncoding { error })?;
        Ok(Self {
            collection: collection.into(),
            key: key.into_bytes(),
            op: if document.is_some() {
                Operation::Put
            } else {
                Operation::Delete
            },
            document,
            lsn,
        })
    }
}

impl fmt::Display for SchemaFlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema(message) => write!(f, "invalid schema: {message}"),
            Self::MissingDocument { collection } => {
                write!(f, "put record for `{collection}` has no document")
            }
            Self::MissingPrimaryField { collection } => {
                write!(f, "collection `{collection}` has no primary field")
            }
            Self::ReservedColumnName {
                collection,
                field,
                column,
            } => write!(
                f,
                "collection `{collection}` field `{field}` maps to reserved SQL column `{column}`"
            ),
            Self::DuplicateColumnName {
                collection,
                first,
                second,
                column,
            } => write!(
                f,
                "collection `{collection}` fields `{first}` and `{second}` both map to SQL column `{column}`"
            ),
            Self::MissingField { collection, field } => {
                write!(f, "document for `{collection}` is missing field `{field}`")
            }
            Self::TypeMismatch { field, expected } => {
                write!(f, "field `{field}` cannot be converted to SQL {expected}")
            }
            Self::NonFiniteFloat { field } => write!(f, "field `{field}` must be finite"),
            Self::LsnTooLarge { lsn } => write!(f, "lsn `{lsn}` does not fit SQLite INTEGER"),
            Self::EncodeValue { field, error } => {
                write!(
                    f,
                    "field `{field}` could not be encoded for SQL flush: {error}"
                )
            }
            Self::EncodeDocument { error } => {
                write!(f, "document could not be encoded for SQL flush: {error}")
            }
            Self::KeyEncoding { error } => {
                write!(f, "primary key could not be encoded: {error}")
            }
        }
    }
}

impl std::error::Error for SchemaFlushError {}

pub fn sql_columns(schema: &Schema) -> Result<Vec<SqlColumn>, SchemaFlushError> {
    schema
        .validate()
        .map_err(|error| SchemaFlushError::InvalidSchema(error.to_string()))?;

    let mut seen = HashMap::<String, String>::new();
    let mut columns = Vec::new();

    for field in schema.fields.iter().filter(|field| field.stored) {
        let column = sql_column(field);
        if column.column_name == MFS_KEY_COLUMN
            || column.column_name == MFS_LSN_COLUMN
            || column.column_name == MFS_DOC_COLUMN
        {
            return Err(SchemaFlushError::ReservedColumnName {
                collection: schema.name.clone(),
                field: field.name.clone(),
                column: column.column_name,
            });
        }

        if let Some(first) = seen.insert(column.column_name.clone(), field.name.clone()) {
            return Err(SchemaFlushError::DuplicateColumnName {
                collection: schema.name.clone(),
                first,
                second: field.name.clone(),
                column: column.column_name,
            });
        }

        columns.push(column);
    }

    Ok(columns)
}

pub fn create_table_sql(schema: &Schema) -> Result<String, SchemaFlushError> {
    let columns = sql_columns(schema)?;
    let table = quote_ident(&schema.name);
    let mut parts = Vec::with_capacity(columns.len() + 2);

    for column in &columns {
        let mut definition = format!("{} {}", quote_ident(&column.column_name), column.sql_type);
        if column.primary {
            definition.push_str(" PRIMARY KEY");
        }
        if column.unique && !column.primary {
            definition.push_str(" UNIQUE");
        }
        if let Some((collection, field)) = &column.reference {
            definition.push_str(&format!(
                " REFERENCES {}({})",
                quote_ident(collection),
                quote_ident(&column_name(field))
            ));
        }
        parts.push(definition);
    }

    parts.push(format!("{} BLOB", quote_ident(MFS_DOC_COLUMN)));
    parts.push(format!(
        "{} BLOB NOT NULL UNIQUE",
        quote_ident(MFS_KEY_COLUMN)
    ));
    parts.push(format!("{} INTEGER NOT NULL", quote_ident(MFS_LSN_COLUMN)));

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {table} ({});",
        parts.join(", ")
    ))
}

pub fn create_index_sql(schema: &Schema) -> Result<Vec<String>, SchemaFlushError> {
    let columns = sql_columns(schema)?;
    let table = quote_ident(&schema.name);
    let mut out = Vec::new();

    for column in columns {
        if column.primary || column.unique || !column.indexed {
            continue;
        }

        let idx_name = quote_ident(&format!("{}_{}_idx", schema.name, column.column_name));
        out.push(format!(
            "CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({});",
            quote_ident(&column.column_name)
        ));
    }

    Ok(out)
}

pub fn ensure_schema_sql(schema: &Schema) -> Result<Vec<String>, SchemaFlushError> {
    let mut sql = vec![create_table_sql(schema)?];
    sql.extend(create_index_sql(schema)?);
    Ok(sql)
}

pub fn upsert_sql(schema: &Schema) -> Result<String, SchemaFlushError> {
    let columns = sql_columns(schema)?;
    let primary = columns
        .iter()
        .find(|column| column.primary)
        .ok_or_else(|| SchemaFlushError::MissingPrimaryField {
            collection: schema.name.clone(),
        })?;
    let table = quote_ident(&schema.name);
    let mut insert_columns = columns
        .iter()
        .map(|column| quote_ident(&column.column_name))
        .collect::<Vec<_>>();
    insert_columns.push(quote_ident(MFS_DOC_COLUMN));
    insert_columns.push(quote_ident(MFS_KEY_COLUMN));
    insert_columns.push(quote_ident(MFS_LSN_COLUMN));
    let placeholders = (0..insert_columns.len())
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let assignments = insert_columns
        .iter()
        .filter(|column| column.as_str() != quote_ident(&primary.column_name))
        .map(|column| format!("{column} = excluded.{column}"))
        .collect::<Vec<_>>()
        .join(", ");

    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({placeholders}) ON CONFLICT ({}) DO UPDATE SET {assignments} WHERE {table}.{} < excluded.{};",
        insert_columns.join(", "),
        quote_ident(&primary.column_name),
        quote_ident(MFS_LSN_COLUMN),
        quote_ident(MFS_LSN_COLUMN),
    ))
}

pub fn delete_sql(schema: &Schema) -> Result<String, SchemaFlushError> {
    schema
        .validate()
        .map_err(|error| SchemaFlushError::InvalidSchema(error.to_string()))?;
    let table = quote_ident(&schema.name);
    Ok(format!(
        "DELETE FROM {table} WHERE {} = ? AND {} <= ?;",
        quote_ident(MFS_KEY_COLUMN),
        quote_ident(MFS_LSN_COLUMN),
    ))
}

pub fn upsert_values(
    schema: &Schema,
    record: &SchemaFlushRecord,
) -> Result<Vec<SqlValue>, SchemaFlushError> {
    let document = record
        .document
        .as_ref()
        .ok_or_else(|| SchemaFlushError::MissingDocument {
            collection: record.collection.clone(),
        })?;
    let mut values = Vec::new();

    for field in schema.fields.iter().filter(|field| field.stored) {
        match document.field(&field.name) {
            Some(value) => values.push(sql_value_for_field(field, value)?),
            None if field.optional => values.push(SqlValue::Null),
            None => {
                return Err(SchemaFlushError::MissingField {
                    collection: record.collection.clone(),
                    field: field.name.clone(),
                });
            }
        }
    }

    // The typed columns above are a projection of the document; the encoded
    // document is the document itself. Without it a collection that has no
    // bespoke writer would flush a key with no payload.
    values.push(SqlValue::Blob(encode_document(document)?));
    values.push(SqlValue::Blob(record.key.clone()));
    values.push(SqlValue::Integer(lsn_to_i64(record.lsn)?));
    Ok(values)
}

pub fn delete_values(record: &SchemaFlushRecord) -> Result<Vec<SqlValue>, SchemaFlushError> {
    Ok(vec![
        SqlValue::Blob(record.key.clone()),
        SqlValue::Integer(lsn_to_i64(record.lsn)?),
    ])
}

fn sql_column(field: &SchemaField) -> SqlColumn {
    SqlColumn {
        field_name: field.name.clone(),
        column_name: column_name(&field.name),
        sql_type: sql_type(&field.field_type),
        primary: field.primary,
        unique: field.unique,
        indexed: field.indexed || field.sort || field.range_index || field.reference.is_some(),
        reference: field
            .reference
            .as_ref()
            .map(|reference| (reference.collection.clone(), reference.field.clone())),
    }
}

fn sql_type(field_type: &SchemaFieldType) -> &'static str {
    match field_type {
        SchemaFieldType::String => "TEXT",
        SchemaFieldType::Int32 | SchemaFieldType::Int64 | SchemaFieldType::Bool => "INTEGER",
        SchemaFieldType::Float => "REAL",
        SchemaFieldType::Bytes => "BLOB",
        SchemaFieldType::Object(_) | SchemaFieldType::Array(_) | SchemaFieldType::Json => "BLOB",
    }
}

fn sql_value_for_field(
    field: &SchemaField,
    value: &SchemaValue,
) -> Result<SqlValue, SchemaFlushError> {
    match (&field.field_type, value) {
        (_, SchemaValue::Null) => Ok(SqlValue::Null),
        (SchemaFieldType::String, SchemaValue::String(value)) => Ok(SqlValue::Text(value.clone())),
        (SchemaFieldType::Int32, SchemaValue::Int32(value)) => {
            Ok(SqlValue::Integer(i64::from(*value)))
        }
        (SchemaFieldType::Int32, SchemaValue::Int64(value)) => i32::try_from(*value)
            .map(|value| SqlValue::Integer(i64::from(value)))
            .map_err(|_| SchemaFlushError::TypeMismatch {
                field: field.name.clone(),
                expected: "int32",
            }),
        (SchemaFieldType::Int64, SchemaValue::Int32(value)) => {
            Ok(SqlValue::Integer(i64::from(*value)))
        }
        (SchemaFieldType::Int64, SchemaValue::Int64(value)) => Ok(SqlValue::Integer(*value)),
        (SchemaFieldType::Bool, SchemaValue::Bool(value)) => {
            Ok(SqlValue::Integer(if *value { 1 } else { 0 }))
        }
        (SchemaFieldType::Float, SchemaValue::Int32(value)) => {
            Ok(SqlValue::Real(f64::from(*value)))
        }
        (SchemaFieldType::Float, SchemaValue::Int64(value)) => Ok(SqlValue::Real(*value as f64)),
        (SchemaFieldType::Float, SchemaValue::Float(value)) if value.is_finite() => {
            Ok(SqlValue::Real(*value))
        }
        (SchemaFieldType::Float, SchemaValue::Float(_)) => Err(SchemaFlushError::NonFiniteFloat {
            field: field.name.clone(),
        }),
        (SchemaFieldType::Bytes, SchemaValue::Bytes(value)) => Ok(SqlValue::Blob(value.clone())),
        (SchemaFieldType::Json, SchemaValue::Json(value)) => Ok(SqlValue::Blob(value.clone())),
        (SchemaFieldType::Object(_), value @ SchemaValue::Object(_))
        | (SchemaFieldType::Array(_), value @ SchemaValue::Array(_))
        | (SchemaFieldType::Json, value) => encode_value_blob(&field.name, value),
        _ => Err(SchemaFlushError::TypeMismatch {
            field: field.name.clone(),
            expected: sql_type(&field.field_type),
        }),
    }
}

fn encode_value_blob(field: &str, value: &SchemaValue) -> Result<SqlValue, SchemaFlushError> {
    validate_codec_safe(value).map_err(|error| SchemaFlushError::EncodeValue {
        field: field.to_string(),
        error,
    })?;
    let mut encoded = Vec::new();
    encode_schema_value(value, &mut encoded);
    Ok(SqlValue::Blob(encoded))
}

/// Encode a whole document for the `doc` column.
///
/// `encode_schema_value` panics on values the codec cannot represent, so the
/// document is validated first and the failure is reported, never asserted.
fn encode_document(document: &SchemaValue) -> Result<Vec<u8>, SchemaFlushError> {
    validate_codec_safe(document).map_err(|error| SchemaFlushError::EncodeDocument { error })?;
    let mut encoded = Vec::new();
    encode_schema_value(document, &mut encoded);
    Ok(encoded)
}

fn lsn_to_i64(lsn: u64) -> Result<i64, SchemaFlushError> {
    i64::try_from(lsn).map_err(|_| SchemaFlushError::LsnTooLarge { lsn })
}

fn column_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
