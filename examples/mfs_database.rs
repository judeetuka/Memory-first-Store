//! Use MfS as a file-backed in-process database with SQLite as the durable sink.
//!
//! MfS owns the hot path: reads, writes, indexed lookups, updates, deletes, and
//! relationship includes all hit [`SchemaStore`] in memory. SQLite is only the
//! swappable persistence layer. This example opens a real SQLite file, enables
//! SQLite WAL journal mode, rehydrates MfS from that file on startup, then
//! flushes dirty MfS records back to SQLite.
//!
//! Run with `cargo run --release --example mfs_database`.
//! Delete `/tmp/mfs_database.sqlite*` to start from an empty database.

use mfs_compat::schema_flush::{
    SchemaFlushBackend, SchemaFlushRecord, SqlValue, delete_sql, delete_values, ensure_schema_sql,
    upsert_sql, upsert_values,
};
use mfs_compat::schema_store::{SchemaKey, SchemaStore};
use mfs_core::{FlushBackend, FlushRecord, Operation};
use mfs_db::schema::{Reference, Schema, SchemaField, SchemaFieldType};
use mfs_db::schema_value::SchemaValue;
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

type ExampleError = Box<dyn std::error::Error + Send + Sync>;
type ExampleResult<T> = Result<T, ExampleError>;

struct SqliteDatabase {
    conn: Connection,
    schemas: HashMap<String, Schema>,
}

struct CollectionFlush<'a> {
    inner: &'a mut SqliteDatabase,
    collection: &'static str,
}

impl SqliteDatabase {
    fn open(path: &Path) -> ExampleResult<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA foreign_keys = ON;\
             PRAGMA journal_mode = WAL;\
             PRAGMA wal_autocheckpoint = 0;",
        )?;
        Ok(Self {
            conn,
            schemas: HashMap::new(),
        })
    }

    fn ensure_schema(&mut self, schema: &Schema) -> ExampleResult<()> {
        for sql in ensure_schema_sql(schema)? {
            self.conn.execute_batch(&sql)?;
        }
        self.schemas.insert(schema.name.clone(), schema.clone());
        Ok(())
    }

    fn collection<'a>(&'a mut self, collection: &'static str) -> CollectionFlush<'a> {
        CollectionFlush {
            inner: self,
            collection,
        }
    }

    fn journal_mode(&self) -> ExampleResult<String> {
        Ok(self
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?)
    }

    fn user_count(&self) -> ExampleResult<i64> {
        Ok(self
            .conn
            .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?)
    }
}

impl SchemaFlushBackend for SqliteDatabase {
    type Error = ExampleError;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error> {
        SqliteDatabase::ensure_schema(self, schema)
    }

    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error> {
        let mut statements = Vec::with_capacity(batch.len());
        for record in batch {
            let schema = self
                .schemas
                .get(&record.collection)
                .ok_or_else(|| format!("missing schema `{}`", record.collection))?;
            match record.op {
                Operation::Put => {
                    statements.push((upsert_sql(schema)?, upsert_values(schema, record)?))
                }
                Operation::Delete => statements.push((delete_sql(schema)?, delete_values(record)?)),
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
    type Error = ExampleError;

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

    let mut name = SchemaField::new("name", SchemaFieldType::String);
    name.indexed = true;

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

    let mut age = SchemaField::new("age", SchemaFieldType::Int32);
    age.indexed = true;
    age.optional = true;

    Schema::new("users", vec![id, email, company_id, age])
}

fn company(id: &str, name: &str) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("name".to_string(), SchemaValue::String(name.to_string())),
    ])
}

fn user(id: &str, email: &str, company_id: &str, age: i32) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("email".to_string(), SchemaValue::String(email.to_string())),
        (
            "company_id".to_string(),
            SchemaValue::String(company_id.to_string()),
        ),
        ("age".to_string(), SchemaValue::Int32(age)),
    ])
}

fn user_from_sql(id: String, email: String, company_id: String, age: Option<i32>) -> SchemaValue {
    let mut fields = vec![
        ("id".to_string(), SchemaValue::String(id)),
        ("email".to_string(), SchemaValue::String(email)),
        ("company_id".to_string(), SchemaValue::String(company_id)),
    ];
    if let Some(age) = age {
        fields.push(("age".to_string(), SchemaValue::Int32(age)));
    }
    SchemaValue::object(fields)
}

fn string_field(doc: &SchemaValue, field: &str) -> String {
    match doc.field(field) {
        Some(SchemaValue::String(value)) => value.clone(),
        _ => "<missing>".to_string(),
    }
}

fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut wal = OsString::from(path.as_os_str());
    wal.push("-wal");
    PathBuf::from(wal)
}

fn database_path() -> PathBuf {
    PathBuf::from("/tmp/mfs_database.sqlite")
}

fn register_schemas(store: &SchemaStore) {
    store.register_collection(companies_schema()).unwrap();
    store.register_collection(users_schema()).unwrap();
}

fn ensure_schemas(db: &mut SqliteDatabase) -> ExampleResult<()> {
    db.ensure_schema(&companies_schema())?;
    db.ensure_schema(&users_schema())?;
    Ok(())
}

fn load_from_sqlite(db: &SqliteDatabase, store: &SchemaStore) -> ExampleResult<usize> {
    let mut loaded = 0usize;

    {
        let mut stmt = db.conn.prepare("SELECT id, name FROM companies")?;
        let rows = stmt.query_map([], |row| {
            Ok(company(
                &row.get::<_, String>(0)?,
                &row.get::<_, String>(1)?,
            ))
        })?;
        for row in rows {
            store.load_clean("companies", row?)?;
            loaded += 1;
        }
    }

    {
        let mut stmt = db
            .conn
            .prepare("SELECT id, email, company_id, age FROM users")?;
        let rows = stmt.query_map([], |row| {
            Ok(user_from_sql(
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i32>>(3)?,
            ))
        })?;
        for row in rows {
            store.load_clean("users", row?)?;
            loaded += 1;
        }
    }

    Ok(loaded)
}

fn flush_collection(
    db: &mut SqliteDatabase,
    store: &SchemaStore,
    collection: &'static str,
) -> ExampleResult<usize> {
    let mut backend = db.collection(collection);
    store
        .flush_collection_idle(collection, &mut backend, 0, usize::MAX)
        .map_err(|error| format!("flush `{collection}` failed: {error}"))
        .map_err(Into::into)
}

fn run_demo(path: &Path) -> ExampleResult<()> {
    let store = SchemaStore::new();
    register_schemas(&store);

    let mut db = SqliteDatabase::open(path)?;
    ensure_schemas(&mut db)?;
    let loaded = load_from_sqlite(&db, &store)?;
    println!("loaded {loaded} rows from {}", path.display());
    println!("sqlite journal_mode = {}", db.journal_mode()?);

    // Create/upsert seed data. Upsert keeps the example rerunnable.
    store.upsert("companies", company("acme", "Acme Corp"))?;
    store.upsert("users", user("u1", "ada@example.com", "acme", 37))?;
    store.upsert("users", user("u2", "grace@example.com", "acme", 40))?;

    // Read by primary key.
    let u1_key = SchemaKey::String("u1".to_string());
    let email = store
        .read_with("users", &u1_key, |doc| string_field(doc, "email"))?
        .unwrap();
    println!("read u1.email = {email}");

    // Indexed query.
    let age_40 = store.lookup("users", "age", &SchemaValue::Int32(40))?;
    println!("users where age=40: {age_40:?}");

    // Forward include: user -> company.
    let included = store.include_one("users", &u1_key, "company_id")?.unwrap();
    let company_name = included
        .referenced
        .as_ref()
        .map(|doc| string_field(doc, "name"))
        .unwrap_or_else(|| "<missing>".to_string());
    println!("u1 works at {company_name}");

    // Reverse include: company -> users.
    let acme_key = SchemaKey::String("acme".to_string());
    let acme_users = store.include_reverse("companies", &acme_key, "users", "company_id")?;
    println!("Acme has {} users in hot MfS memory", acme_users.len());

    // Update.
    store.update("users", &u1_key, |doc| {
        let SchemaValue::Object(fields) = doc else {
            return;
        };
        fields.insert("age".to_string(), SchemaValue::Int32(38));
    })?;

    // Upsert.
    store.upsert("users", user("u3", "linus@example.com", "acme", 54))?;

    // Delete.
    store.delete("users", &SchemaKey::String("u2".to_string()))?;

    // Persist dirty records to the SQLite database file.
    let company_flushes = flush_collection(&mut db, &store, "companies")?;
    let user_flushes = flush_collection(&mut db, &store, "users")?;
    println!("flushed companies={company_flushes}, users={user_flushes}");

    let joined: (String, String) = db.conn.query_row(
        "SELECT users.email, companies.name \
         FROM users JOIN companies ON users.company_id = companies.id \
         WHERE users.id = 'u1'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    println!("sqlite join says: {} works at {}", joined.0, joined.1);

    println!(
        "sqlite rows: users={}, path={}",
        db.user_count()?,
        path.display()
    );
    println!("sqlite WAL path: {}", sqlite_wal_path(path).display());
    Ok(())
}

fn main() -> ExampleResult<()> {
    run_demo(&database_path())
}

#[cfg(test)]
mod tests {
    #[test]
    fn sqlite_file_backed_database_demo_runs() {
        let mut path = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("mfs_database_test_{ts}.sqlite"));

        super::run_demo(&path).unwrap();
        super::run_demo(&path).unwrap();
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(super::sqlite_wal_path(&path)).ok();
    }
}
