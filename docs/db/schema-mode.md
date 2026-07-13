# Schema Mode

Schema-validated document storage with secondary indexes and declared references.

Schema mode validates `Schema` definitions and `SchemaValue` documents before writing them to the shared storage kernel. It adds declared secondary indexes and declared references on top of the raw KV layer.

## When to use schema mode

- You need document validation against a declared schema.
- You want secondary indexes for exact-match lookups on specific fields.
- You need unique constraints on field values.
- You want declared references between collections.
- Your data model is document-oriented with typed fields.

## Core types

### `Schema`

```rust
pub struct Schema {
    pub name: String,
    pub fields: Vec<SchemaField>,
    pub enable_nested_fields: bool,
    pub default_sort_field: Option<String>,
}

impl Schema {
    pub fn new(name: impl Into<String>, fields: Vec<SchemaField>) -> Self;
    pub fn validate(&self) -> Result<(), SchemaError>;
    pub fn field(&self, name: &str) -> Option<&SchemaField>;
    pub fn primary_field(&self) -> Option<&SchemaField>;
}
```

A schema defines a collection's structure. It must have exactly one primary field. The primary field must be stored, not optional, and use a reference-compatible type (string, int32, int64, or bytes).

### `SchemaField`

```rust
pub struct SchemaField {
    pub name: String,
    pub field_type: SchemaFieldType,
    pub primary: bool,
    pub optional: bool,
    pub indexed: bool,
    pub stored: bool,
    pub unique: bool,
    pub reference: Option<Reference>,
    pub sort: bool,
    pub range_index: bool,
}

impl SchemaField {
    pub fn new(name: impl Into<String>, field_type: SchemaFieldType) -> Self;
}
```

Field flags:

- `primary`: exactly one field per schema must be primary.
- `optional`: field can be missing or null.
- `indexed`: field has a secondary exact-match index.
- `unique`: indexed field with unique constraint.
- `sort`: field is sortable (requires `indexed`).
- `range_index`: field has a range index (requires `indexed`, numeric type only).
- `reference`: declared reference to another collection's primary field.

### `SchemaFieldType`

```rust
pub enum SchemaFieldType {
    String,
    Int32,
    Int64,
    Float,
    Bool,
    Object(Vec<SchemaField>),
    Array(Box<SchemaFieldType>),
    Json,
    Bytes,
}

impl SchemaFieldType {
    pub fn parse(input: &str) -> Result<Self, SchemaError>;
    pub fn is_indexable(&self) -> bool;
    pub fn is_sortable(&self) -> bool;
    pub fn is_numeric(&self) -> bool;
    pub fn is_reference_compatible(&self) -> bool;
}
```

Indexable types: string, int32, int64, float, bool, bytes.
Sortable types: same as indexable.
Numeric types: int32, int64, float.
Reference-compatible types: string, int32, int64, bytes.

### `SchemaValue`

```rust
pub enum SchemaValue {
    Null,
    String(String),
    Int32(i32),
    Int64(i64),
    Float(f64),
    Bool(bool),
    Object(BTreeMap<String, SchemaValue>),
    Array(Vec<SchemaValue>),
    Json(Vec<u8>),
    Bytes(Vec<u8>),
}

impl SchemaValue {
    pub fn object(fields: impl IntoIterator<Item = (String, SchemaValue)>) -> Self;
    pub fn kind(&self) -> SchemaValueKind;
    pub fn tag(&self) -> SchemaValueTag;
    pub fn as_object(&self) -> Option<&BTreeMap<String, SchemaValue>>;
    pub fn field(&self, path: &str) -> Option<&SchemaValue>;
    pub fn validate_against(&self, schema: &Schema) -> Result<(), SchemaValueError>;
}
```

`SchemaValue::field` supports dot-notation paths for nested objects. For example, `"profile.name"` resolves to `object["profile"]["name"]`.

### `Reference`

```rust
pub struct Reference {
    pub collection: String,
    pub field: String,
}

impl Reference {
    pub fn new(collection: impl Into<String>, field: impl Into<String>) -> Self;
    pub fn parse(input: &str) -> Result<Self, SchemaError>;
    pub fn validate(&self) -> Result<(), SchemaError>;
}
```

References are declared in the schema and maintained by the engine. The target field must be the primary field of the target collection.

## API reference

### `NoSqlEngine::create_schema_collection`

```rust
pub fn create_schema_collection(&self, schema: &Schema) -> EngineResult<CollectionId>
```

Create a collection with schema validation and indexes. Validates the schema, creates the underlying raw collection, and installs secondary indexes.

### `NoSqlEngine::put_schema`

```rust
pub fn put_schema(
    &self,
    schema: &Schema,
    document: SchemaValue,
    options: WriteOptions,
) -> EngineResult<WriteResult>
```

Insert or replace a document. Validates the document against the schema, encodes it, and writes it to the raw layer. Updates secondary indexes and references atomically.

The document root must be an object. The primary key is derived from the primary field's value.

### `NoSqlEngine::get_schema`

```rust
pub fn get_schema(
    &self,
    schema: &Schema,
    primary_key: &SchemaValue,
    options: ReadOptions,
) -> EngineResult<Option<SchemaReadResult>>
```

Read a document by primary key. Returns the decoded `SchemaValue` and version.

### `NoSqlEngine::delete_schema`

```rust
pub fn delete_schema(
    &self,
    schema: &Schema,
    primary_key: &SchemaValue,
    options: WriteOptions,
) -> EngineResult<WriteResult>
```

Delete a document by primary key. Removes secondary index entries and reference edges atomically.

### `NoSqlEngine::lookup_schema`

```rust
pub fn lookup_schema(
    &self,
    schema: &Schema,
    field: &str,
    value: &SchemaValue,
    options: ReadOptions,
) -> EngineResult<Vec<SchemaLookupResult>>
```

Exact-match lookup on an indexed field. Returns all documents where the field equals the given value. The field must be marked `indexed` in the schema.

### `SchemaReadResult`

```rust
pub struct SchemaReadResult {
    pub document: SchemaValue,
    pub version: DocumentVersion,
}
```

### `SchemaLookupResult`

```rust
pub struct SchemaLookupResult {
    pub primary_key: RawKey,
    pub document: SchemaValue,
    pub version: DocumentVersion,
}
```

### `NoSqlEngine::query_schema`

```rust
pub fn query_schema(
    &self,
    schema: &Schema,
    options: QueryOptions,
) -> EngineResult<QueryResult>
```

Filter, sort, and paginate documents in a collection. The filter uses a single `FilterClause` with operators `Eq`, `Neq`, `Gt`, `Gte`, `Lt`, `Lte`. Sort by any indexed field with `SortDirection::Asc` or `SortDirection::Desc`. Ties broken by primary key bytes. Pagination via `offset` and `limit`.

### `NoSqlEngine::count_schema`

```rust
pub fn count_schema(
    &self,
    schema: &Schema,
    filter: Option<FilterClause>,
) -> EngineResult<u64>
```

Count documents in a collection. Pass `None` for total count (uses atomic counter). Pass `Some(filter)` to count matching documents.

### `NoSqlEngine::multi_get_schema`

```rust
pub fn multi_get_schema(
    &self,
    schema: &Schema,
    keys: &[SchemaValue],
    options: ReadOptions,
) -> EngineResult<Vec<SchemaReadResult>>
```

Batch read by primary keys. Duplicate keys are deduplicated. Missing keys are silently skipped. No snapshot isolation across keys.

### `NoSqlEngine::update_schema`

```rust
pub fn update_schema(
    &self,
    schema: &Schema,
    primary_key: &SchemaValue,
    operations: FieldUpdateOp,
    options: WriteOptions,
) -> EngineResult<WriteResult>
```

Partial update with optimistic CAS retry (max 3 attempts). Supports `Set` (assign field value), `Unset` (remove field), and `Increment` (add delta to numeric field). Primary key field cannot be modified. Document is re-validated against schema after all mutations.

### `QueryOptions`

```rust
pub struct QueryOptions {
    pub filter: Option<FilterClause>,
    pub sort_field: Option<String>,
    pub sort_direction: SortDirection,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
```

### `FilterClause`

```rust
pub struct FilterClause {
    pub field: String,
    pub op: FilterOp,
    pub value: SchemaValue,
}

pub enum FilterOp {
    Eq, Neq, Gt, Gte, Lt, Lte,
}
```

### `QueryResult`

```rust
pub struct QueryResult {
    pub documents: Vec<SchemaReadResult>,
    pub total_count: Option<u64>,
}
```

### `FieldUpdateOp`

```rust
pub struct FieldUpdateOp {
    pub updates: Vec<FieldUpdate>,
}

pub enum FieldUpdate {
    Set { field: String, value: SchemaValue },
    Unset { field: String },
    Increment { field: String, delta: i64 },
}
```

## Schema validation rules

- Exactly one primary field.
- Primary field must be stored, not optional, and reference-compatible.
- Unique fields must be indexed and not optional.
- Sort fields must be indexed and sortable.
- Range index fields must be indexed and numeric.
- Object child fields cannot use operational flags (primary, indexed, unique, sort, range_index, reference).
- Field names must be valid identifiers. Dot-notation names require `enable_nested_fields: true`.

## Code example

```rust
use mfs_db::{
    NoSqlEngine, EngineConfig, Schema, SchemaField, SchemaFieldType,
    SchemaValue, WriteOptions, ReadOptions,
};

// Define schema
let mut id = SchemaField::new("id", SchemaFieldType::String);
id.primary = true;
id.indexed = true;
id.unique = true;

let mut email = SchemaField::new("email", SchemaFieldType::String);
email.indexed = true;
email.unique = true;

let mut age = SchemaField::new("age", SchemaFieldType::Int64);
age.indexed = true;

let schema = Schema::new("users", vec![id, email, age]);

// Create engine and collection
let engine = NoSqlEngine::open_memory(EngineConfig::default())?;
engine.create_schema_collection(&schema)?;

// Put document
let doc = SchemaValue::object([
    ("id".to_string(), SchemaValue::String("u1".to_string())),
    ("email".to_string(), SchemaValue::String("ada@example.com".to_string())),
    ("age".to_string(), SchemaValue::Int64(37)),
]);

let result = engine.put_schema(&schema, doc.clone(), WriteOptions::default())?;
assert_eq!(result.version.get(), 1);

// Get by primary key
let read = engine.get_schema(
    &schema,
    &SchemaValue::String("u1".to_string()),
    ReadOptions::default(),
)?.expect("document exists");
assert_eq!(read.document, doc);

// Lookup by indexed field
let hits = engine.lookup_schema(
    &schema,
    "age",
    &SchemaValue::Int64(37),
    ReadOptions::default(),
)?;
assert_eq!(hits.len(), 1);
assert_eq!(
    hits[0].document.field("id"),
    Some(&SchemaValue::String("u1".to_string()))
);

// Delete
engine.delete_schema(
    &schema,
    &SchemaValue::String("u1".to_string()),
    WriteOptions::default(),
)?;
```

## Cross-links

- [Overview](./overview.md) -- engine contract, durability modes
- [Raw KV API](./raw-kv.md) -- underlying raw key-value layer
- [Values](./values.md) -- `SchemaValue` encoding and decoding
- [WAL](./wal.md) -- write-ahead log for crash recovery
- [Checkpoint](./checkpoint.md) -- full-state snapshots
