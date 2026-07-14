use mfs_compat::schema_flush::{
    SchemaFlushBackend, SchemaFlushError, SchemaFlushRecord, SqlValue, delete_sql, delete_values,
    ensure_schema_sql, upsert_sql, upsert_values,
};
use mfs_compat::schema_store::{SchemaKey, SchemaStore};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use mfs_store::schema::{Reference, Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use std::collections::HashMap;

#[derive(Debug)]
enum SqliteFlushError {
    Sqlite(rusqlite::Error),
    Schema(SchemaFlushError),
    MissingSchema(String),
}

struct SqliteSchemaFlush {
    conn: Connection,
    schemas: HashMap<String, Schema>,
}

struct CollectionFlush<'a> {
    inner: &'a mut SqliteSchemaFlush,
    collection: &'static str,
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

impl std::fmt::Display for SqliteFlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "sqlite error: {error}"),
            Self::Schema(error) => write!(f, "schema flush error: {error}"),
            Self::MissingSchema(collection) => write!(f, "missing schema `{collection}`"),
        }
    }
}

impl std::error::Error for SqliteFlushError {}

impl SqliteSchemaFlush {
    fn open_in_memory() -> Result<Self, SqliteFlushError> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        Ok(Self {
            conn,
            schemas: HashMap::new(),
        })
    }

    fn collection<'a>(&'a mut self, collection: &'static str) -> CollectionFlush<'a> {
        CollectionFlush {
            inner: self,
            collection,
        }
    }
}

impl SchemaFlushBackend for SqliteSchemaFlush {
    type Error = SqliteFlushError;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error> {
        for sql in ensure_schema_sql(schema)? {
            self.conn.execute_batch(&sql)?;
        }
        self.schemas.insert(schema.name.clone(), schema.clone());
        Ok(())
    }

    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error> {
        let mut statements = Vec::with_capacity(batch.len());
        for record in batch {
            let schema = self
                .schemas
                .get(&record.collection)
                .ok_or_else(|| SqliteFlushError::MissingSchema(record.collection.clone()))?;
            match record.op {
                Operation::Put => {
                    statements.push((upsert_sql(schema)?, upsert_values(schema, record)?));
                }
                Operation::Delete => {
                    statements.push((delete_sql(schema)?, delete_values(record)?));
                }
            }
        }

        let tx = self.conn.transaction()?;
        for (sql, values) in statements {
            let params = values.into_iter().map(sql_value).collect::<Vec<_>>();
            tx.execute(&sql, params_from_iter(params.iter()))?;
        }
        tx.commit()?;
        Ok(())
    }
}

impl FlushBackend<SchemaKey, SchemaValue> for CollectionFlush<'_> {
    type Error = SqliteFlushError;

    fn flush(
        &mut self,
        records: &[FlushRecord<SchemaKey, SchemaValue>],
    ) -> Result<(), Self::Error> {
        let records = records
            .iter()
            .map(|record| SchemaFlushRecord::from_flush_record(self.collection, record))
            .collect::<Vec<_>>();
        self.inner.flush_records(&records)
    }
}

fn sql_value(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(value) => Value::Integer(value),
        SqlValue::Real(value) => Value::Real(value),
        SqlValue::Text(value) => Value::Text(value),
        SqlValue::Blob(value) => Value::Blob(value),
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

fn run_demo() -> Result<(), SqliteFlushError> {
    let store = SchemaStore::new();
    let companies = companies_schema();
    let users = users_schema();
    store.register_collection(companies.clone()).unwrap();
    store.register_collection(users.clone()).unwrap();

    let mut sqlite = SqliteSchemaFlush::open_in_memory()?;
    sqlite.ensure_schema(&companies)?;
    sqlite.ensure_schema(&users)?;

    store.create("companies", company("c1", "Acme")).unwrap();
    store
        .create("users", user("u1", "ada@example.com", "c1", 10))
        .unwrap();

    {
        let mut backend = sqlite.collection("companies");
        store
            .flush_collection_idle("companies", &mut backend, 0, 16)
            .unwrap();
    }
    {
        let mut backend = sqlite.collection("users");
        store
            .flush_collection_idle("users", &mut backend, 0, 16)
            .unwrap();
    }

    let joined: (String, String) = sqlite.conn.query_row(
        "SELECT users.email, companies.name \
         FROM users JOIN companies ON users.company_id = companies.id \
         WHERE users.id = 'u1'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(joined, ("ada@example.com".to_string(), "Acme".to_string()));

    let key = SchemaKey::String("u1".to_string()).encoded();
    sqlite.flush_records(&[SchemaFlushRecord {
        collection: "users".to_string(),
        key: key.clone(),
        op: Operation::Put,
        document: Some(user("u1", "stale@example.com", "c1", 9)),
        lsn: 1,
    }])?;
    let email: String =
        sqlite
            .conn
            .query_row("SELECT email FROM users WHERE id = 'u1'", [], |row| {
                row.get(0)
            })?;
    assert_eq!(email, "ada@example.com");

    sqlite.flush_records(&[SchemaFlushRecord {
        collection: "users".to_string(),
        key: key.clone(),
        op: Operation::Put,
        document: Some(user("u1", "fresh@example.com", "c1", 11)),
        lsn: 100,
    }])?;
    let email: String =
        sqlite
            .conn
            .query_row("SELECT email FROM users WHERE id = 'u1'", [], |row| {
                row.get(0)
            })?;
    assert_eq!(email, "fresh@example.com");

    sqlite.flush_records(&[SchemaFlushRecord {
        collection: "users".to_string(),
        key,
        op: Operation::Delete,
        document: None,
        lsn: 101,
    }])?;
    let count: i64 =
        sqlite
            .conn
            .query_row("SELECT COUNT(*) FROM users WHERE id = 'u1'", [], |row| {
                row.get(0)
            })?;
    assert_eq!(count, 0);

    Ok(())
}

fn main() -> Result<(), SqliteFlushError> {
    run_demo()?;
    println!("schema SQLite flush example completed");
    Ok(())
}
