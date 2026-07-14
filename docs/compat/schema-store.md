# SchemaStore: Schema-Aware Document Store

`SchemaStore` is an in-process document store that validates every write
against a registered schema. It supports secondary indexes, unique
constraints, foreign-key references, and SQL flush planning for persisting
to SQLite or other SQL backends.

## What it is

A collection of named schemas, each backed by a `WriteBehindCache` keyed
on the schema's primary key. Documents are `SchemaValue` objects (from
`mfs-store`). Every put, upsert, create, update, and delete validates the
document against the schema, maintains secondary indexes, and enforces
unique constraints.

## When to use it

- You need schema validation on every write.
- You want secondary indexes and unique constraints without a full SQL
  engine.
- You need foreign-key references between collections.
- You want to flush documents to SQL tables with LSN-guarded upserts.

## When to skip it

- You don't need schema validation. Use `MfsObjectStore` or
  `MfsMutableObjectStore` for untyped key-value storage.
- You need a full SQL engine. Use the SQLite VFS adapter or an external
  database.

## Construction

```rust
use mfs_compat::schema_store::{SchemaStore, SchemaKey};

let store = SchemaStore::new();
// or with capacity hint:
let store = SchemaStore::with_capacity(100_000);
```

## Registering a schema

```rust
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType, Reference};

let mut id = SchemaField::new("id", SchemaFieldType::String);
id.primary = true;
id.indexed = true;
id.unique = true;

let mut email = SchemaField::new("email", SchemaFieldType::String);
email.indexed = true;
email.unique = true;

let mut age = SchemaField::new("age", SchemaFieldType::Int32);
age.optional = true;
age.indexed = true;

let schema = Schema::new("users", vec![id, email, age]);
store.register_collection(schema)?;
```

Each schema must have exactly one primary field. Indexed fields support
`String`, `Int32`, `Int64`, `Float`, `Bool`, and `Bytes` types.

## CRUD operations

### Create (reject if exists)

```rust
use mfs_store::schema_value::SchemaValue;

let doc = SchemaValue::object([
    ("id".to_string(), SchemaValue::String("u1".into())),
    ("email".to_string(), SchemaValue::String("ada@example.com".into())),
    ("age".to_string(), SchemaValue::Int32(37)),
]);

store.create("users", doc)?;
```

### Upsert (create or replace)

```rust
store.upsert("users", doc)?;
// or equivalently:
store.put("users", doc)?;
```

### Load clean (from backend, won't flush back)

```rust
store.load_clean("users", doc)?;
```

### Get

```rust
let key = SchemaKey::String("u1".into());
let doc: Option<Arc<SchemaValue>> = store.get("users", &key)?;

// Zero-copy read:
let email = store.read_with("users", &key, |doc| {
    match doc.field("email") {
        Some(SchemaValue::String(e)) => e.clone(),
        _ => String::new(),
    }
})?;
```

### Update

```rust
store.update("users", &key, |doc| {
    let SchemaValue::Object(fields) = doc else { return; };
    fields.insert("age".to_string(), SchemaValue::Int32(38));
})?;
```

Update validates the modified document against the schema, rejects primary
key changes, and re-indexes affected fields.

### Try update

```rust
let result = store.try_update("users", &key, |doc| { /* ... */ })?;
// Returns Ok(None) if the document doesn't exist, Ok(Some(version)) on success.
```

### Delete

```rust
store.delete("users", &key)?;
```

## Secondary indexes

Mark a field as `indexed = true` in the schema to enable lookups:

```rust
let keys = store.lookup("users", "age", &SchemaValue::Int32(37))?;
// Returns Vec<SchemaKey> of all documents where age == 37.
```

Indexed fields must be scalar types (`String`, `Int32`, `Int64`, `Float`,
`Bool`, `Bytes`). Indexes are maintained automatically on put, upsert,
update, and delete.

### Unique constraints

Mark a field as `unique = true` to reject duplicate values:

```rust
// If "email" is unique, this fails with SchemaStoreError::UniqueViolation:
store.create("users", duplicate_email_doc)?;
```

Unique violations are checked before the write is applied.

## Foreign-key references

```rust
let mut company_id = SchemaField::new("company_id", SchemaFieldType::String);
company_id.optional = true;
company_id.reference = Some(Reference::new("companies", "id"));

let user_schema = Schema::new("users", vec![id, email, company_id]);
```

References enforce that the target field is the primary key of the target
collection. Forward and reverse lookups are supported:

### Forward include (document + referenced document)

```rust
let include = store.include_one("users", &user_key, "company_id")?;
// include.document     — the user document
// include.reference_key — the company primary key (if set)
// include.referenced   — the company document (if it exists)
```

### Reverse include (all documents referencing a target)

```rust
let refs = store.include_reverse(
    "companies", &company_key,
    "users", "company_id",
)?;
// Returns Vec<SchemaReverseInclude> with all users referencing this company.
```

## Write-behind flush

```rust
use mfs_core::{FlushBackend, FlushRecord};

struct MyBackend;
impl FlushBackend<SchemaKey, SchemaValue> for MyBackend {
    type Error = std::io::Error;
    fn flush(&mut self, records: &[FlushRecord<SchemaKey, SchemaValue>]) -> Result<(), Self::Error> {
        Ok(())
    }
}

let mut backend = MyBackend;
let flushed = store.flush_collection_idle("users", &mut backend, 32, 10_000)
    .map_err(|e| match e {
        SchemaFlushError::Store(e) => e.to_string(),
        SchemaFlushError::Backend(e) => e.to_string(),
    })?;
```

## SQL flush planning

The `schema_flush` module generates SQL from a schema definition. Use it
to persist documents to SQLite or another SQL database.

### SchemaFlushBackend trait

```rust
use mfs_compat::schema_flush::{SchemaFlushBackend, SchemaFlushRecord};

struct SqliteBackend { /* ... */ }

impl SchemaFlushBackend for SqliteBackend {
    type Error = rusqlite::Error;

    fn ensure_schema(&mut self, schema: &Schema) -> Result<(), Self::Error> {
        // Run CREATE TABLE and CREATE INDEX statements.
        Ok(())
    }

    fn flush_records(&mut self, batch: &[SchemaFlushRecord]) -> Result<(), Self::Error> {
        // Execute UPSERT or DELETE for each record.
        Ok(())
    }
}
```

### SQL generation helpers

```rust
use mfs_compat::schema_flush::{
    create_table_sql, create_index_sql, ensure_schema_sql,
    upsert_sql, delete_sql, upsert_values, delete_values,
    SchemaFlushRecord, quote_ident,
};

// DDL
let create_table = create_table_sql(&schema)?;
let create_indexes = create_index_sql(&schema)?;
let all_ddl = ensure_schema_sql(&schema)?;

// DML
let upsert_stmt = upsert_sql(&schema)?;
let delete_stmt = delete_sql(&schema)?;

// Convert a flush record to SQL values.
let record = SchemaFlushRecord::from_flush_record("users", &flush_record);
let values = upsert_values(&schema, &record)?;
let del_values = delete_values(&record)?;
```

### Generated SQL shape

For a schema with fields `id TEXT PRIMARY KEY`, `email TEXT UNIQUE`,
`company_id TEXT REFERENCES companies(id)`:

```sql
CREATE TABLE IF NOT EXISTS "users" (
    "id" TEXT PRIMARY KEY,
    "email" TEXT UNIQUE,
    "company_id" TEXT REFERENCES "companies"("id"),
    "mfs_key" BLOB NOT NULL UNIQUE,
    "mfs_lsn" INTEGER NOT NULL
);

-- Non-unique indexed fields get separate indexes:
CREATE INDEX IF NOT EXISTS "users_company_id_idx" ON "users" ("company_id");

-- Upsert with LSN guard (only applies if incoming LSN is newer):
INSERT INTO "users" ("id", "email", "company_id", "mfs_key", "mfs_lsn")
VALUES (?, ?, ?, ?, ?)
ON CONFLICT ("id") DO UPDATE SET
    "email" = excluded."email",
    "company_id" = excluded."company_id",
    "mfs_key" = excluded."mfs_key",
    "mfs_lsn" = excluded."mfs_lsn"
WHERE "users"."mfs_lsn" < excluded."mfs_lsn";

-- Delete with LSN guard:
DELETE FROM "users" WHERE "mfs_key" = ? AND "mfs_lsn" <= ?;
```

The `mfs_key` column stores the encoded primary key bytes. The `mfs_lsn`
column stores the logical sequence number, used to prevent out-of-order
flushes from overwriting newer data.

### SqlValue type

```rust
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
```

Field types map to SQL types:

| SchemaFieldType | SQL type |
|---|---|
| String | TEXT |
| Int32, Int64, Bool | INTEGER |
| Float | REAL |
| Bytes | BLOB |
| Object, Array, Json | BLOB (encoded) |

## Error handling

```rust
pub enum SchemaStoreError {
    Schema(SchemaError),
    SchemaValue(SchemaValueError),
    CollectionAlreadyRegistered { collection: String },
    CollectionNotFound { collection: String },
    DocumentAlreadyExists { collection: String, key: SchemaKey },
    DocumentNotFound { collection: String, key: SchemaKey },
    StoreFull { collection: String },
    MissingPrimaryField { collection: String },
    MissingPrimaryKey { collection: String, field: String },
    PrimaryKeyTypeMismatch { ... },
    PrimaryKeyChanged { ... },
    ReferenceFieldNotFound { ... },
    ReferenceTargetCollectionNotFound { ... },
    ReferenceTargetNotPrimary { ... },
    ReferenceTargetMismatch { ... },
    UniqueViolation { collection: String, field: String, existing: SchemaKey },
    UnindexedField { ... },
    IndexKeyTypeMismatch { ... },
}
```

## SchemaKey types

```rust
pub enum SchemaKey {
    String(String),
    Int32(i32),
    Int64(i64),
    Bytes(Vec<u8>),
}
```

The key type is determined by the primary field's schema type. Keys have
an `encoded()` method that produces a deterministic byte representation
for storage in `mfs_key`.

## Cross-links

- [Object Store](object-store.md) for untyped Redis-like storage.
- [Mutable Object Store](mutable-object-store.md) for growable storage with TTL.
- [SQLite VFS](sqlite-vfs.md) for the page-level SQLite adapter.
- [mfs-store: Schema](../db/overview.md) for schema definition types.
