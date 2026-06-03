use crate::engine::index::{
    SchemaCollectionIndexes, SchemaIndexWritePlan, decode_schema_raw_value,
};
use crate::engine::{
    CollectionId, DocumentVersion, EngineError, EngineResult, NoSqlEngine, RawKey, RawValue,
    ReadOptions, WriteOptions, WriteResult,
};
use crate::schema::{Schema, SchemaField, SchemaFieldType};
use crate::schema_value::{SchemaValue, SchemaValueError, encode_schema_value, validate_document};
use std::cell::RefCell;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaReadResult {
    pub document: SchemaValue,
    pub version: DocumentVersion,
}

impl NoSqlEngine {
    pub fn create_schema_collection(&self, schema: &Schema) -> EngineResult<CollectionId> {
        validate_schema(schema)?;
        let indexes = SchemaCollectionIndexes::new(schema)?;
        let id = self.create_raw_collection(schema.name.clone())?;
        self.install_schema_indexes(schema, indexes)?;
        Ok(id)
    }

    pub fn put_schema(
        &self,
        schema: &Schema,
        document: SchemaValue,
        options: WriteOptions,
    ) -> EngineResult<WriteResult> {
        let key = schema_document_raw_key(schema, &document)?;
        validate_document(schema, &document)
            .map_err(|error| schema_document_error(schema, error))?;

        let state = self.ensure_schema_indexes(schema)?;
        let mut encoded = Vec::new();
        encode_schema_value(&document, &mut encoded);
        let prepared: RefCell<Option<SchemaIndexWritePlan>> = RefCell::new(None);
        let _write_unit = state.lock_write_unit();

        self.put_raw_with_hooks(
            &schema.name,
            key.clone(),
            RawValue::from(encoded),
            options,
            |old_raw, _actual| {
                let old_document = match old_raw {
                    Some(raw) => Some(decode_schema_raw_value(schema, raw)?),
                    None => None,
                };
                let plan = state.prepare_put(self, &key, old_document.as_ref(), &document)?;
                *prepared.borrow_mut() = Some(plan);
                Ok(())
            },
            |_, _version| {
                let plan = prepared
                    .borrow_mut()
                    .take()
                    .expect("schema index plan prepared before raw write");
                state.apply_write(&key, plan);
            },
        )
    }

    pub fn delete_schema(
        &self,
        schema: &Schema,
        primary_key: &SchemaValue,
        options: WriteOptions,
    ) -> EngineResult<WriteResult> {
        let key = schema_primary_key_raw_key(schema, primary_key)?;
        let state = self.ensure_schema_indexes(schema)?;
        let prepared: RefCell<Option<SchemaIndexWritePlan>> = RefCell::new(None);
        let _write_unit = state.lock_write_unit();

        self.delete_raw_with_hooks(
            &schema.name,
            key.clone(),
            options,
            |old_raw, _actual| {
                let old_document = match old_raw {
                    Some(raw) => Some(decode_schema_raw_value(schema, raw)?),
                    None => None,
                };
                let plan = state.prepare_delete(self, old_document.as_ref())?;
                *prepared.borrow_mut() = Some(plan);
                Ok(())
            },
            |_, _version| {
                let plan = prepared
                    .borrow_mut()
                    .take()
                    .expect("schema delete plan prepared before raw write");
                state.apply_write(&key, plan);
            },
        )
    }

    pub fn get_schema(
        &self,
        schema: &Schema,
        primary_key: &SchemaValue,
        options: ReadOptions,
    ) -> EngineResult<Option<SchemaReadResult>> {
        let key = schema_primary_key_raw_key(schema, primary_key)?;
        let state = self.ensure_schema_indexes(schema)?;
        let _read_unit = state.lock_read_unit();
        self.get_schema_by_raw_key(schema, &key, options)
    }

    pub(crate) fn get_schema_by_raw_key(
        &self,
        schema: &Schema,
        key: &RawKey,
        options: ReadOptions,
    ) -> EngineResult<Option<SchemaReadResult>> {
        let Some(raw) = self.get_raw(&schema.name, key, options)? else {
            return Ok(None);
        };

        let document = decode_schema_raw_value(schema, &raw.value)?;

        Ok(Some(SchemaReadResult {
            document,
            version: raw.version,
        }))
    }
}

pub fn schema_document_raw_key(schema: &Schema, document: &SchemaValue) -> EngineResult<RawKey> {
    validate_schema(schema)?;
    ensure_document_root(schema, document)?;
    let primary = primary_field(schema)?;
    let value =
        document
            .field(&primary.name)
            .ok_or_else(|| EngineError::SchemaMissingPrimaryKey {
                collection: schema.name.clone(),
                field: primary.name.clone(),
            })?;

    primary_value_raw_key(schema, primary, value)
}

pub fn schema_primary_key_raw_key(
    schema: &Schema,
    primary_key: &SchemaValue,
) -> EngineResult<RawKey> {
    validate_schema(schema)?;
    let primary = primary_field(schema)?;
    primary_value_raw_key(schema, primary, primary_key)
}

pub(crate) fn validate_schema(schema: &Schema) -> EngineResult<()> {
    schema
        .validate()
        .map_err(|error| EngineError::SchemaDefinition {
            collection: schema.name.clone(),
            error,
        })
}

fn ensure_document_root(schema: &Schema, document: &SchemaValue) -> EngineResult<()> {
    if document.as_object().is_some() {
        return Ok(());
    }

    Err(schema_document_error(
        schema,
        SchemaValueError::RootMustBeObject {
            actual: document.kind(),
        },
    ))
}

fn primary_field(schema: &Schema) -> EngineResult<&SchemaField> {
    schema
        .primary_field()
        .ok_or_else(|| EngineError::SchemaMissingPrimaryField {
            collection: schema.name.clone(),
        })
}

fn primary_value_raw_key(
    schema: &Schema,
    primary: &SchemaField,
    value: &SchemaValue,
) -> EngineResult<RawKey> {
    match (&primary.field_type, value) {
        (SchemaFieldType::String, SchemaValue::String(value)) => {
            Ok(RawKey::from(encode_key_bytes(0, value.as_bytes())))
        }
        (SchemaFieldType::Int32, SchemaValue::Int32(value)) => {
            Ok(RawKey::from(encode_i32_key(*value)))
        }
        (SchemaFieldType::Int32, SchemaValue::Int64(int64)) => i32::try_from(*int64)
            .map(encode_i32_key)
            .map(RawKey::from)
            .map_err(|_| primary_key_mismatch(schema, primary, value)),
        (SchemaFieldType::Int64, SchemaValue::Int32(value)) => {
            Ok(RawKey::from(encode_i64_key(i64::from(*value))))
        }
        (SchemaFieldType::Int64, SchemaValue::Int64(value)) => {
            Ok(RawKey::from(encode_i64_key(*value)))
        }
        (SchemaFieldType::Bytes, SchemaValue::Bytes(value)) => {
            Ok(RawKey::from(encode_key_bytes(3, value)))
        }
        _ => Err(primary_key_mismatch(schema, primary, value)),
    }
}

fn encode_i32_key(value: i32) -> Vec<u8> {
    let mut out = vec![1];
    out.extend_from_slice(&value.to_le_bytes());
    out
}

fn encode_i64_key(value: i64) -> Vec<u8> {
    let mut out = vec![2];
    out.extend_from_slice(&value.to_le_bytes());
    out
}

fn encode_key_bytes(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + bytes.len());
    out.push(tag);
    out.extend_from_slice(bytes);
    out
}

fn primary_key_mismatch(
    schema: &Schema,
    primary: &SchemaField,
    value: &SchemaValue,
) -> EngineError {
    EngineError::SchemaPrimaryKeyTypeMismatch {
        collection: schema.name.clone(),
        field: primary.name.clone(),
        expected: schema_type_name(&primary.field_type),
        actual: value.kind(),
    }
}

pub(crate) fn schema_document_error(schema: &Schema, error: SchemaValueError) -> EngineError {
    EngineError::SchemaDocument {
        collection: schema.name.clone(),
        error,
    }
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
