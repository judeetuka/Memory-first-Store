//! Schema-aware document values and WAL codec.

use crate::schema::{Schema, SchemaError, SchemaField, SchemaFieldType};
use mfs_core::durability::WalCodec;
use std::collections::BTreeMap;
use std::fmt;
use std::io;

pub const MAX_SCHEMA_VALUE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCHEMA_COLLECTION_ITEMS: usize = 1_048_576;
pub const MAX_SCHEMA_BLOB_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SCHEMA_VALUE_DEPTH: usize = 64;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaValueKind {
    Null,
    String,
    Int32,
    Int64,
    Float,
    Bool,
    Object,
    Array,
    Json,
    Bytes,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaValueError {
    InvalidSchema(SchemaError),
    RootMustBeObject {
        actual: SchemaValueKind,
    },
    MissingRequiredField {
        name: String,
    },
    NullNotAllowed {
        name: String,
    },
    TypeMismatch {
        name: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
    Int32OutOfRange {
        name: String,
        value: i64,
    },
    NonFiniteFloat {
        name: String,
    },
    ValueNestingTooDeep {
        name: String,
    },
    CollectionTooLarge {
        name: String,
        len: usize,
    },
    BlobTooLarge {
        name: String,
        len: usize,
    },
    EncodedValueTooLarge {
        len: usize,
    },
}

impl SchemaValue {
    pub fn object(fields: impl IntoIterator<Item = (String, SchemaValue)>) -> Self {
        Self::Object(fields.into_iter().collect())
    }

    pub fn kind(&self) -> SchemaValueKind {
        match self {
            Self::Null => SchemaValueKind::Null,
            Self::String(_) => SchemaValueKind::String,
            Self::Int32(_) => SchemaValueKind::Int32,
            Self::Int64(_) => SchemaValueKind::Int64,
            Self::Float(_) => SchemaValueKind::Float,
            Self::Bool(_) => SchemaValueKind::Bool,
            Self::Object(_) => SchemaValueKind::Object,
            Self::Array(_) => SchemaValueKind::Array,
            Self::Json(_) => SchemaValueKind::Json,
            Self::Bytes(_) => SchemaValueKind::Bytes,
        }
    }

    pub fn tag(&self) -> SchemaValueTag {
        match self {
            Self::Null => SchemaValueTag::Null,
            Self::String(_) => SchemaValueTag::String,
            Self::Int32(_) => SchemaValueTag::Int32,
            Self::Int64(_) => SchemaValueTag::Int64,
            Self::Float(_) => SchemaValueTag::Float,
            Self::Bool(_) => SchemaValueTag::Bool,
            Self::Object(_) => SchemaValueTag::Object,
            Self::Array(_) => SchemaValueTag::Array,
            Self::Json(_) => SchemaValueTag::Json,
            Self::Bytes(_) => SchemaValueTag::Bytes,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, SchemaValue>> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }

    pub fn field(&self, path: &str) -> Option<&SchemaValue> {
        resolve_path(self.as_object()?, path)
    }

    pub fn validate_against(&self, schema: &Schema) -> Result<(), SchemaValueError> {
        validate_document(schema, self)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SchemaValueCodec;

impl WalCodec<Vec<u8>, SchemaValue> for SchemaValueCodec {
    fn encode_key(&self, key: &Vec<u8>, out: &mut Vec<u8>) {
        out.extend_from_slice(key);
    }

    fn encode_value(&self, value: &SchemaValue, out: &mut Vec<u8>) {
        encode_schema_value(value, out);
    }

    fn decode_key(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        Ok(bytes.to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> io::Result<SchemaValue> {
        decode_schema_value(bytes)
    }
}

pub fn validate_document(schema: &Schema, value: &SchemaValue) -> Result<(), SchemaValueError> {
    schema.validate().map_err(SchemaValueError::InvalidSchema)?;
    let object = value
        .as_object()
        .ok_or_else(|| SchemaValueError::RootMustBeObject {
            actual: value.kind(),
        })?;
    validate_fields(&schema.fields, object, "")?;
    validate_codec_safe(value)
}

pub fn encode_schema_value(value: &SchemaValue, out: &mut Vec<u8>) {
    validate_codec_safe(value).expect("SchemaValue must be codec-safe before encoding");
    encode_schema_value_unchecked(value, out);
}

pub fn validate_codec_safe(value: &SchemaValue) -> Result<(), SchemaValueError> {
    let len = encoded_len(value, "", 0)?;
    if len > MAX_SCHEMA_VALUE_BYTES {
        return Err(SchemaValueError::EncodedValueTooLarge { len });
    }
    Ok(())
}

fn encode_schema_value_unchecked(value: &SchemaValue, out: &mut Vec<u8>) {
    out.push(value.tag() as u8);
    match value {
        SchemaValue::Null => {}
        SchemaValue::String(value) => encode_bytes(value.as_bytes(), out),
        SchemaValue::Int32(value) => out.extend_from_slice(&value.to_le_bytes()),
        SchemaValue::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
        SchemaValue::Float(value) => out.extend_from_slice(&value.to_le_bytes()),
        SchemaValue::Bool(value) => out.push(u8::from(*value)),
        SchemaValue::Object(fields) => {
            encode_len(fields.len(), out);
            for (name, value) in fields {
                encode_bytes(name.as_bytes(), out);
                encode_schema_value_unchecked(value, out);
            }
        }
        SchemaValue::Array(values) => {
            encode_len(values.len(), out);
            for value in values {
                encode_schema_value_unchecked(value, out);
            }
        }
        SchemaValue::Json(bytes) | SchemaValue::Bytes(bytes) => encode_bytes(bytes, out),
    }
}

pub fn decode_schema_value(bytes: &[u8]) -> io::Result<SchemaValue> {
    if bytes.len() > MAX_SCHEMA_VALUE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SchemaValue payload exceeds decoder limit",
        ));
    }

    let mut cursor = 0usize;
    decode_schema_value_at(bytes, &mut cursor, 0).and_then(|value| {
        if cursor == bytes.len() {
            Ok(value)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing SchemaValue bytes",
            ))
        }
    })
}

impl fmt::Display for SchemaValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Null => "null",
            Self::String => "string",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Object => "object",
            Self::Array => "array",
            Self::Json => "json",
            Self::Bytes => "bytes",
        })
    }
}

impl fmt::Display for SchemaValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema(error) => write!(f, "invalid schema: {error}"),
            Self::RootMustBeObject { actual } => {
                write!(f, "schema document root must be object, got {actual}")
            }
            Self::MissingRequiredField { name } => {
                write!(f, "missing required field `{name}`")
            }
            Self::NullNotAllowed { name } => write!(f, "field `{name}` cannot be null"),
            Self::TypeMismatch {
                name,
                expected,
                actual,
            } => write!(f, "field `{name}` expected {expected}, got {actual}"),
            Self::Int32OutOfRange { name, value } => {
                write!(f, "field `{name}` int32 value {value} is out of range")
            }
            Self::NonFiniteFloat { name } => write!(f, "field `{name}` float must be finite"),
            Self::ValueNestingTooDeep { name } => {
                write!(f, "field `{name}` exceeds max schema value nesting")
            }
            Self::CollectionTooLarge { name, len } => {
                write!(f, "field `{name}` collection length {len} exceeds limit")
            }
            Self::BlobTooLarge { name, len } => {
                write!(f, "field `{name}` byte length {len} exceeds limit")
            }
            Self::EncodedValueTooLarge { len } => {
                write!(f, "encoded schema value length {len} exceeds limit")
            }
        }
    }
}

impl std::error::Error for SchemaValueError {}

fn validate_fields(
    schema_fields: &[SchemaField],
    object: &BTreeMap<String, SchemaValue>,
    parent: &str,
) -> Result<(), SchemaValueError> {
    for field in schema_fields {
        let path = join_path(parent, &field.name);
        let Some(value) = resolve_path(object, &field.name) else {
            if field.optional {
                continue;
            }

            return Err(SchemaValueError::MissingRequiredField { name: path });
        };

        if matches!(value, SchemaValue::Null) {
            if field.optional {
                continue;
            }

            return Err(SchemaValueError::NullNotAllowed { name: path });
        }

        validate_value_type(value, &field.field_type, &path)?;
    }

    Ok(())
}

fn validate_value_type(
    value: &SchemaValue,
    expected: &SchemaFieldType,
    name: &str,
) -> Result<(), SchemaValueError> {
    match expected {
        SchemaFieldType::String => match value {
            SchemaValue::String(_) => Ok(()),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Int32 => match value {
            SchemaValue::Int32(_) => Ok(()),
            SchemaValue::Int64(value) if i32::try_from(*value).is_ok() => Ok(()),
            SchemaValue::Int64(value) => Err(SchemaValueError::Int32OutOfRange {
                name: name.to_string(),
                value: *value,
            }),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Int64 => match value {
            SchemaValue::Int32(_) | SchemaValue::Int64(_) => Ok(()),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Float => match value {
            SchemaValue::Int32(_) | SchemaValue::Int64(_) => Ok(()),
            SchemaValue::Float(value) if value.is_finite() => Ok(()),
            SchemaValue::Float(_) => Err(SchemaValueError::NonFiniteFloat {
                name: name.to_string(),
            }),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Bool => match value {
            SchemaValue::Bool(_) => Ok(()),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Object(fields) => match value {
            SchemaValue::Object(object) => validate_fields(fields, object, name),
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Array(inner) => match value {
            SchemaValue::Array(values) => {
                for (idx, value) in values.iter().enumerate() {
                    validate_value_type(value, inner, &format!("{name}[{idx}]"))?;
                }
                Ok(())
            }
            _ => type_mismatch(name, expected, value),
        },
        SchemaFieldType::Json => Ok(()),
        SchemaFieldType::Bytes => match value {
            SchemaValue::Bytes(_) => Ok(()),
            _ => type_mismatch(name, expected, value),
        },
    }
}

fn type_mismatch(
    name: &str,
    expected: &SchemaFieldType,
    actual: &SchemaValue,
) -> Result<(), SchemaValueError> {
    Err(SchemaValueError::TypeMismatch {
        name: name.to_string(),
        expected: schema_type_name(expected),
        actual: actual.kind(),
    })
}

fn schema_type_name(field_type: &SchemaFieldType) -> &'static str {
    match field_type {
        SchemaFieldType::String => "string",
        SchemaFieldType::Int32 => "int32",
        SchemaFieldType::Int64 => "int64",
        SchemaFieldType::Float => "float",
        SchemaFieldType::Bool => "bool",
        SchemaFieldType::Object(_) => "object",
        SchemaFieldType::Array(_) => "array",
        SchemaFieldType::Json => "json",
        SchemaFieldType::Bytes => "bytes",
    }
}

fn encoded_len(value: &SchemaValue, name: &str, depth: usize) -> Result<usize, SchemaValueError> {
    if depth > MAX_SCHEMA_VALUE_DEPTH {
        return Err(SchemaValueError::ValueNestingTooDeep {
            name: name.to_string(),
        });
    }

    match value {
        SchemaValue::Null => Ok(1),
        SchemaValue::String(value) => encoded_blob_len(name, value.len()),
        SchemaValue::Int32(_) => Ok(1 + 4),
        SchemaValue::Int64(_) => Ok(1 + 8),
        SchemaValue::Float(value) if value.is_finite() => Ok(1 + 8),
        SchemaValue::Float(_) => Err(SchemaValueError::NonFiniteFloat {
            name: name.to_string(),
        }),
        SchemaValue::Bool(_) => Ok(1 + 1),
        SchemaValue::Json(bytes) | SchemaValue::Bytes(bytes) => encoded_blob_len(name, bytes.len()),
        SchemaValue::Array(values) => {
            if values.len() > MAX_SCHEMA_COLLECTION_ITEMS {
                return Err(SchemaValueError::CollectionTooLarge {
                    name: name.to_string(),
                    len: values.len(),
                });
            }

            let mut total = 1usize + 4;
            for (idx, value) in values.iter().enumerate() {
                total = checked_add_len(
                    total,
                    encoded_len(value, &format!("{name}[{idx}]"), depth + 1)?,
                )?;
            }
            Ok(total)
        }
        SchemaValue::Object(fields) => {
            if fields.len() > MAX_SCHEMA_COLLECTION_ITEMS {
                return Err(SchemaValueError::CollectionTooLarge {
                    name: name.to_string(),
                    len: fields.len(),
                });
            }

            let mut total = 1usize + 4;
            for (field, value) in fields {
                let path = join_path(name, field);
                total = checked_add_len(total, encoded_raw_bytes_len(&path, field.len())?)?;
                total = checked_add_len(total, encoded_len(value, &path, depth + 1)?)?;
            }
            Ok(total)
        }
    }
}

fn encoded_blob_len(name: &str, len: usize) -> Result<usize, SchemaValueError> {
    checked_add_len(1, encoded_raw_bytes_len(name, len)?)
}

fn encoded_raw_bytes_len(name: &str, len: usize) -> Result<usize, SchemaValueError> {
    if len > MAX_SCHEMA_BLOB_BYTES {
        return Err(SchemaValueError::BlobTooLarge {
            name: name.to_string(),
            len,
        });
    }
    checked_add_len(4, len)
}

fn checked_add_len(left: usize, right: usize) -> Result<usize, SchemaValueError> {
    left.checked_add(right)
        .ok_or(SchemaValueError::EncodedValueTooLarge { len: usize::MAX })
}

fn resolve_path<'a>(
    object: &'a BTreeMap<String, SchemaValue>,
    path: &str,
) -> Option<&'a SchemaValue> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    let mut current = object.get(first)?;

    for segment in segments {
        let nested = current.as_object()?;
        current = nested.get(segment)?;
    }

    Some(current)
}

fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}.{name}")
    }
}

fn decode_schema_value_at(
    bytes: &[u8],
    cursor: &mut usize,
    depth: usize,
) -> io::Result<SchemaValue> {
    if depth > MAX_SCHEMA_VALUE_DEPTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SchemaValue nesting exceeds decoder limit",
        ));
    }

    let tag = read_u8(bytes, cursor)?;
    match tag {
        0 => Ok(SchemaValue::Null),
        1 => {
            let value = String::from_utf8(read_bytes(bytes, cursor)?.to_vec())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(SchemaValue::String(value))
        }
        2 => Ok(SchemaValue::Int32(i32::from_le_bytes(read_exact::<4>(
            bytes, cursor,
        )?))),
        3 => Ok(SchemaValue::Int64(i64::from_le_bytes(read_exact::<8>(
            bytes, cursor,
        )?))),
        4 => {
            let value = f64::from_le_bytes(read_exact::<8>(bytes, cursor)?);
            if !value.is_finite() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SchemaValue float must be finite",
                ));
            }
            Ok(SchemaValue::Float(value))
        }
        5 => match read_u8(bytes, cursor)? {
            0 => Ok(SchemaValue::Bool(false)),
            1 => Ok(SchemaValue::Bool(true)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "SchemaValue bool must be 0 or 1",
            )),
        },
        6 => {
            let count = read_count(bytes, cursor, 5)?;
            let mut fields = BTreeMap::new();
            for _ in 0..count {
                let name = String::from_utf8(read_bytes(bytes, cursor)?.to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if fields.contains_key(&name) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SchemaValue object contains duplicate field",
                    ));
                }
                let value = decode_schema_value_at(bytes, cursor, depth + 1)?;
                fields.insert(name, value);
            }
            Ok(SchemaValue::Object(fields))
        }
        7 => {
            let count = read_count(bytes, cursor, 1)?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(decode_schema_value_at(bytes, cursor, depth + 1)?);
            }
            Ok(SchemaValue::Array(values))
        }
        8 => Ok(SchemaValue::Json(read_bytes(bytes, cursor)?.to_vec())),
        9 => Ok(SchemaValue::Bytes(read_bytes(bytes, cursor)?.to_vec())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown SchemaValue tag",
        )),
    }
}

fn encode_len(len: usize, out: &mut Vec<u8>) {
    let len = u32::try_from(len).expect("SchemaValue collection too large");
    out.extend_from_slice(&len.to_le_bytes());
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    encode_len(bytes.len(), out);
    out.extend_from_slice(bytes);
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    if bytes.len().saturating_sub(*cursor) < 1 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "tag truncated",
        ));
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> io::Result<usize> {
    Ok(u32::from_le_bytes(read_exact::<4>(bytes, cursor)?) as usize)
}

fn read_count(bytes: &[u8], cursor: &mut usize, min_bytes_per_item: usize) -> io::Result<usize> {
    let count = read_len(bytes, cursor)?;
    if count > MAX_SCHEMA_COLLECTION_ITEMS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SchemaValue collection exceeds item limit",
        ));
    }
    let remaining = bytes.len().saturating_sub(*cursor);
    if min_bytes_per_item > 0 && count > remaining / min_bytes_per_item {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "SchemaValue collection length exceeds remaining payload",
        ));
    }
    Ok(count)
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> io::Result<&'a [u8]> {
    let len = read_len(bytes, cursor)?;
    if len > MAX_SCHEMA_BLOB_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SchemaValue byte field exceeds decoder limit",
        ));
    }
    if bytes.len().saturating_sub(*cursor) < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "bytes truncated",
        ));
    }
    let out = &bytes[*cursor..*cursor + len];
    *cursor += len;
    Ok(out)
}

fn read_exact<const N: usize>(bytes: &[u8], cursor: &mut usize) -> io::Result<[u8; N]> {
    if bytes.len().saturating_sub(*cursor) < N {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "fixed field truncated",
        ));
    }
    let out = bytes[*cursor..*cursor + N]
        .try_into()
        .expect("fixed length");
    *cursor += N;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfs_core::{FlushBackend, FlushRecord, Operation};
    use std::sync::Arc;

    fn primary_id() -> SchemaField {
        let mut field = SchemaField::new("id", SchemaFieldType::String);
        field.primary = true;
        field.indexed = true;
        field.unique = true;
        field
    }

    fn user_schema() -> Schema {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let profile = SchemaField::new(
            "profile",
            SchemaFieldType::Object(vec![SchemaField::new("name", SchemaFieldType::String)]),
        );

        let created_at = SchemaField {
            indexed: true,
            sort: true,
            range_index: true,
            ..SchemaField::new("created_at", SchemaFieldType::Int64)
        };

        let tags = SchemaField {
            optional: true,
            ..SchemaField::new(
                "tags",
                SchemaFieldType::Array(Box::new(SchemaFieldType::String)),
            )
        };

        Schema {
            name: "users".to_string(),
            fields: vec![primary_id(), email, profile, created_at, tags],
            enable_nested_fields: true,
            default_sort_field: Some("created_at".to_string()),
        }
    }

    fn valid_user() -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String("u1".to_string())),
            (
                "email".to_string(),
                SchemaValue::String("ada@example.com".to_string()),
            ),
            (
                "profile".to_string(),
                SchemaValue::object([("name".to_string(), SchemaValue::String("Ada".to_string()))]),
            ),
            ("created_at".to_string(), SchemaValue::Int64(1_700_000_000)),
        ])
    }

    fn object_fields_mut(value: &mut SchemaValue) -> &mut BTreeMap<String, SchemaValue> {
        match value {
            SchemaValue::Object(fields) => fields,
            _ => panic!("document should be object"),
        }
    }

    #[test]
    fn valid_document_is_accepted() {
        let schema = user_schema();
        let document = valid_user();

        assert_eq!(document.validate_against(&schema), Ok(()));
        assert_eq!(
            document.field("profile.name"),
            Some(&SchemaValue::String("Ada".to_string()))
        );
    }

    #[test]
    fn missing_required_field_is_rejected() {
        let schema = user_schema();
        let document = SchemaValue::object([("id".to_string(), SchemaValue::String("u1".into()))]);

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::MissingRequiredField {
                name: "email".to_string()
            })
        );
    }

    #[test]
    fn optional_field_can_be_missing_or_null() {
        let schema = user_schema();
        let mut document = valid_user();

        assert_eq!(validate_document(&schema, &document), Ok(()));

        let fields = object_fields_mut(&mut document);
        fields.insert("tags".to_string(), SchemaValue::Null);

        assert_eq!(validate_document(&schema, &document), Ok(()));
    }

    #[test]
    fn null_required_field_is_rejected() {
        let schema = user_schema();
        let mut document = valid_user();
        let fields = object_fields_mut(&mut document);
        fields.insert("email".to_string(), SchemaValue::Null);

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::NullNotAllowed {
                name: "email".to_string()
            })
        );
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let schema = user_schema();
        let mut document = valid_user();
        let fields = object_fields_mut(&mut document);
        fields.insert("email".to_string(), SchemaValue::Int64(9));

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::TypeMismatch {
                name: "email".to_string(),
                expected: "string",
                actual: SchemaValueKind::Int64,
            })
        );
    }

    #[test]
    fn int32_bounds_are_checked() {
        let mut age = SchemaField::new("age", SchemaFieldType::Int32);
        age.optional = false;
        let schema = Schema::new("users", vec![primary_id(), age]);
        let document = SchemaValue::object([
            ("id".to_string(), SchemaValue::String("u1".to_string())),
            (
                "age".to_string(),
                SchemaValue::Int64(i64::from(i32::MAX) + 1),
            ),
        ]);

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::Int32OutOfRange {
                name: "age".to_string(),
                value: i64::from(i32::MAX) + 1,
            })
        );
    }

    #[test]
    fn arrays_validate_each_element() {
        let schema = user_schema();
        let mut document = valid_user();
        let fields = object_fields_mut(&mut document);
        fields.insert(
            "tags".to_string(),
            SchemaValue::Array(vec![
                SchemaValue::String("rust".to_string()),
                SchemaValue::Int64(1),
            ]),
        );

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::TypeMismatch {
                name: "tags[1]".to_string(),
                expected: "string",
                actual: SchemaValueKind::Int64,
            })
        );
    }

    #[test]
    fn dot_notation_fields_resolve_nested_objects() {
        let schema = Schema {
            name: "users".to_string(),
            fields: vec![
                primary_id(),
                SchemaField::new("profile.name", SchemaFieldType::String),
            ],
            enable_nested_fields: true,
            default_sort_field: None,
        };
        let document = SchemaValue::object([
            ("id".to_string(), SchemaValue::String("u1".to_string())),
            (
                "profile".to_string(),
                SchemaValue::object([("name".to_string(), SchemaValue::String("Ada".to_string()))]),
            ),
        ]);

        assert_eq!(validate_document(&schema, &document), Ok(()));
    }

    #[test]
    fn extra_fields_are_accepted() {
        let schema = user_schema();
        let mut document = valid_user();
        let fields = object_fields_mut(&mut document);
        fields.insert("extra".to_string(), SchemaValue::Bool(true));

        assert_eq!(validate_document(&schema, &document), Ok(()));
    }

    #[test]
    fn codec_unsafe_extra_fields_are_rejected() {
        let schema = user_schema();
        let mut document = valid_user();
        let fields = object_fields_mut(&mut document);
        fields.insert("extra".to_string(), SchemaValue::Float(f64::NAN));

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::NonFiniteFloat {
                name: "extra".to_string()
            })
        );
    }

    #[test]
    fn codec_unsafe_json_fields_are_rejected() {
        let mut payload = SchemaField::new("payload", SchemaFieldType::Json);
        payload.optional = false;
        let schema = Schema::new("events", vec![primary_id(), payload]);
        let document = SchemaValue::object([
            ("id".to_string(), SchemaValue::String("e1".to_string())),
            ("payload".to_string(), SchemaValue::Float(f64::INFINITY)),
        ]);

        assert_eq!(
            validate_document(&schema, &document),
            Err(SchemaValueError::NonFiniteFloat {
                name: "payload".to_string()
            })
        );
    }

    #[test]
    fn encode_decode_round_trips_schema_values() {
        let mut nested = BTreeMap::new();
        nested.insert("ok".to_string(), SchemaValue::Bool(true));
        let values = [
            SchemaValue::Null,
            SchemaValue::String("hello".to_string()),
            SchemaValue::Int32(-7),
            SchemaValue::Int64(9_000_000_000),
            SchemaValue::Float(1.25),
            SchemaValue::Bool(true),
            SchemaValue::Object(nested),
            SchemaValue::Array(vec![SchemaValue::String("a".to_string())]),
            SchemaValue::Json(br#"{"a":1}"#.to_vec()),
            SchemaValue::Bytes(b"raw".to_vec()),
        ];

        for value in values {
            let mut encoded = Vec::new();
            encode_schema_value(&value, &mut encoded);
            assert_eq!(decode_schema_value(&encoded).unwrap(), value);
        }
    }

    #[test]
    fn decoder_rejects_malformed_values() {
        assert!(decode_schema_value(&[]).is_err());
        assert!(decode_schema_value(&[255]).is_err());

        let mut trailing = Vec::new();
        encode_schema_value(&SchemaValue::Int32(1), &mut trailing);
        trailing.push(0);
        assert!(decode_schema_value(&trailing).is_err());

        let mut bad_string = vec![SchemaValueTag::String as u8];
        bad_string.extend_from_slice(&1u32.to_le_bytes());
        bad_string.push(0xff);
        assert!(decode_schema_value(&bad_string).is_err());

        let mut bad_float = vec![SchemaValueTag::Float as u8];
        bad_float.extend_from_slice(&f64::NAN.to_le_bytes());
        assert!(decode_schema_value(&bad_float).is_err());
    }

    #[test]
    #[should_panic(expected = "SchemaValue must be codec-safe before encoding")]
    fn encoder_refuses_codec_unsafe_values() {
        let mut encoded = Vec::new();
        encode_schema_value(&SchemaValue::Float(f64::NAN), &mut encoded);
    }

    #[test]
    fn decoder_rejects_duplicate_object_fields() {
        let mut encoded = vec![SchemaValueTag::Object as u8];
        encoded.extend_from_slice(&2u32.to_le_bytes());
        encode_bytes(&b"same"[..], &mut encoded);
        encoded.push(SchemaValueTag::Null as u8);
        encode_bytes(&b"same"[..], &mut encoded);
        encoded.push(SchemaValueTag::Null as u8);

        assert!(decode_schema_value(&encoded).is_err());
    }

    #[test]
    fn decoder_rejects_excessive_nesting() {
        let mut encoded = vec![SchemaValueTag::Null as u8];
        for _ in 0..=MAX_SCHEMA_VALUE_DEPTH {
            let mut wrapper = vec![SchemaValueTag::Array as u8];
            wrapper.extend_from_slice(&1u32.to_le_bytes());
            wrapper.extend_from_slice(&encoded);
            encoded = wrapper;
        }

        assert!(decode_schema_value(&encoded).is_err());
    }

    #[test]
    fn decoder_rejects_oversized_lengths_before_allocating() {
        let mut huge_array = vec![SchemaValueTag::Array as u8];
        huge_array.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_schema_value(&huge_array).is_err());

        let mut huge_bytes = vec![SchemaValueTag::Bytes as u8];
        huge_bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_schema_value(&huge_bytes).is_err());
    }

    #[test]
    fn wal_codec_round_trips_schema_records() {
        let mut path = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("mfs_schema_value_codec_{ts}.wal"));

        let document = valid_user();
        {
            let mut wal = mfs_core::durability::WalBackend::open(
                &path,
                SchemaValueCodec,
                mfs_core::durability::WalConfig::default(),
            )
            .unwrap();
            wal.flush(&[
                FlushRecord {
                    key: b"u1".to_vec(),
                    value: Some(Arc::new(document.clone())),
                    version: 1,
                    op: Operation::Put,
                },
                FlushRecord {
                    key: b"u2".to_vec(),
                    value: None,
                    version: 2,
                    op: Operation::Delete,
                },
            ])
            .unwrap();
            wal.sync_now().unwrap();
        }

        let mut seen = Vec::new();
        mfs_core::durability::WalBackend::<Vec<u8>, SchemaValue, SchemaValueCodec>::replay(
            &path,
            &SchemaValueCodec,
            |record| seen.push((record.key, record.value, record.version, record.op)),
        )
        .unwrap();

        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], (b"u1".to_vec(), Some(document), 1, Operation::Put));
        assert_eq!(seen[1], (b"u2".to_vec(), None, 2, Operation::Delete));
        std::fs::remove_file(&path).ok();
    }
}
