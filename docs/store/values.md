# Values

Two value models for `mfs-store`: `MfsValue` for the object-store API and `SchemaValue` for schema-mode documents.

## `MfsValue`: Redis-like value model

`MfsValue` is the value type for the object-store API. It represents variable-sized data and is meant to be stored behind `Arc` by object-store writers. This is intentionally separate from the dense writers, which are limited to 8-byte `Copy` values.

### `ValueTag`

```rust
#[repr(u8)]
pub enum ValueTag {
    Bytes = 0,
    String = 1,
    Integer = 2,
    List = 3,
    Hash = 4,
    Set = 5,
    SortedSet = 6,
    Json = 7,
    Stream = 8,
    Null = 9,
}
```

### `MfsValue` enum

```rust
pub enum MfsValue {
    Bytes(Vec<u8>),
    String(String),
    Integer(i64),
    List(Vec<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    SortedSet(Vec<SortedSetEntry>),
    Json(Vec<u8>),
    Stream(Vec<StreamEntry>),
    Null,
}

impl MfsValue {
    pub fn tag(&self) -> ValueTag;
}
```

### `SortedSetEntry`

```rust
pub struct SortedSetEntry {
    pub score: f64,
    pub member: Vec<u8>,
}
```

Scores must be finite. The decoder rejects non-finite scores.

### `StreamEntry` and `StreamId`

```rust
pub struct StreamId {
    pub millis: u64,
    pub sequence: u64,
}

pub struct StreamEntry {
    pub id: StreamId,
    pub fields: BTreeMap<Vec<u8>, Vec<u8>>,
}
```

### Limits

```rust
pub const MAX_ENCODED_VALUE_BYTES: usize = 64 * 1024 * 1024;  // 64 MB
pub const MAX_COLLECTION_ITEMS: usize = 1_048_576;             // 1M items
pub const MAX_BLOB_BYTES: usize = 64 * 1024 * 1024;           // 64 MB
```

### Encode and decode

```rust
pub fn encode_value(value: &MfsValue, out: &mut Vec<u8>);
pub fn decode_value(bytes: &[u8]) -> io::Result<MfsValue>;
```

The encoding format is:

1. One-byte tag.
2. Type-specific payload:
   - `Bytes`, `String`, `Json`: length-prefixed bytes (4-byte LE length + data).
   - `Integer`: 8-byte LE i64.
   - `List`: 4-byte LE count, then length-prefixed items.
   - `Hash`: 4-byte LE count, then pairs of length-prefixed key-value.
   - `Set`: 4-byte LE count, then length-prefixed members.
   - `SortedSet`: 4-byte LE count, then (8-byte LE f64 score + length-prefixed member) per entry.
   - `Stream`: 4-byte LE count, then per entry: 8-byte millis, 8-byte sequence, 4-byte field count, then field key-value pairs.
   - `Null`: no payload.

The decoder rejects payloads exceeding `MAX_ENCODED_VALUE_BYTES`, collections exceeding `MAX_COLLECTION_ITEMS`, blobs exceeding `MAX_BLOB_BYTES`, non-finite sorted set scores, and trailing bytes.

### `MfsValueCodec`

```rust
pub struct MfsValueCodec;

impl WalCodec<Vec<u8>, MfsValue> for MfsValueCodec {
    fn encode_key(&self, key: &Vec<u8>, out: &mut Vec<u8>);
    fn encode_value(&self, value: &MfsValue, out: &mut Vec<u8>);
    fn decode_key(&self, bytes: &[u8]) -> io::Result<Vec<u8>>;
    fn decode_value(&self, bytes: &[u8]) -> io::Result<MfsValue>;
}
```

Use `MfsValueCodec` with `mfs_core::durability::WalBackend` to persist `MfsValue` records through the WAL.

## `SchemaValue`: schema-mode document model

`SchemaValue` is the document type for schema mode. It mirrors JSON-like structure with typed fields.

### `SchemaValueTag`

```rust
#[repr(u8)]
pub enum SchemaValueTag {
    Null = 0,
    String = 1,
    Int32 = 2,
    Int64 = 3,
    Float = 4,
    Bool = 5,
    Object = 6,
    Array = 7,
    Json = 8,
    Bytes = 9,
}
```

### `SchemaValue` enum

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

### Limits

```rust
pub const MAX_SCHEMA_VALUE_BYTES: usize = 64 * 1024 * 1024;   // 64 MB
pub const MAX_SCHEMA_COLLECTION_ITEMS: usize = 1_048_576;      // 1M items
pub const MAX_SCHEMA_BLOB_BYTES: usize = 64 * 1024 * 1024;    // 64 MB
pub const MAX_SCHEMA_VALUE_DEPTH: usize = 64;                  // max nesting
```

### Encode and decode

```rust
pub fn encode_schema_value(value: &SchemaValue, out: &mut Vec<u8>);
pub fn decode_schema_value(bytes: &[u8]) -> io::Result<SchemaValue>;
```

The encoding format is:

1. One-byte tag.
2. Type-specific payload:
   - `Null`: no payload.
   - `String`: length-prefixed UTF-8 bytes.
   - `Int32`: 4-byte LE i32.
   - `Int64`: 8-byte LE i64.
   - `Float`: 8-byte LE f64 (must be finite).
   - `Bool`: 1 byte (0 or 1).
   - `Object`: 4-byte LE count, then (length-prefixed name + recursive value) per field.
   - `Array`: 4-byte LE count, then recursive values.
   - `Json`, `Bytes`: length-prefixed bytes.

The encoder panics if the value is not codec-safe (non-finite float, excessive nesting, oversized payload). The decoder rejects malformed payloads, duplicate object fields, excessive nesting, oversized lengths, and trailing bytes.

### `SchemaValueCodec`

```rust
pub struct SchemaValueCodec;

impl WalCodec<Vec<u8>, SchemaValue> for SchemaValueCodec {
    fn encode_key(&self, key: &Vec<u8>, out: &mut Vec<u8>);
    fn encode_value(&self, value: &SchemaValue, out: &mut Vec<u8>);
    fn decode_key(&self, bytes: &[u8]) -> io::Result<Vec<u8>>;
    fn decode_value(&self, bytes: &[u8]) -> io::Result<SchemaValue>;
}
```

### Document validation

```rust
pub fn validate_document(schema: &Schema, value: &SchemaValue) -> Result<(), SchemaValueError>;
pub fn validate_codec_safe(value: &SchemaValue) -> Result<(), SchemaValueError>;
```

`validate_document` checks the value against a schema: required fields present, types match, int32 bounds respected, floats finite, nesting within limits.

`validate_codec_safe` checks that the value can be encoded without panicking: finite floats, nesting within `MAX_SCHEMA_VALUE_DEPTH`, total encoded size within `MAX_SCHEMA_VALUE_BYTES`.

## Code example

```rust
use mfs_store::{MfsValue, encode_value, decode_value};
use mfs_store::{SchemaValue, encode_schema_value, decode_schema_value};

// MfsValue round-trip
let value = MfsValue::String("hello".to_string());
let mut encoded = Vec::new();
encode_value(&value, &mut encoded);
let decoded = decode_value(&encoded)?;
assert_eq!(decoded, value);

// SchemaValue round-trip
let doc = SchemaValue::object([
    ("id".to_string(), SchemaValue::String("u1".to_string())),
    ("age".to_string(), SchemaValue::Int64(37)),
]);
let mut encoded = Vec::new();
encode_schema_value(&doc, &mut encoded);
let decoded = decode_schema_value(&encoded)?;
assert_eq!(decoded, doc);
```

## Cross-links

- [Overview](./overview.md) -- engine contract and modes
- [Raw KV API](./raw-kv.md) -- raw byte key-value storage
- [Schema Mode](./schema-mode.md) -- schema-validated document storage
- [WAL](./wal.md) -- write-ahead log uses these codecs for persistence
