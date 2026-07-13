use crate::engine::index::{
    SchemaCollectionIndexes, SchemaIndexWritePlan, decode_schema_raw_value,
};
use crate::engine::{
    CollectionId, DocumentVersion, EngineError, EngineResult, FieldUpdate, FieldUpdateOp,
    NoSqlEngine, RawKey, RawValue, ReadOptions, WriteOptions, WriteResult,
};
use crate::schema::{Schema, SchemaField, SchemaFieldType};
use crate::schema_value::{
    SchemaValue, SchemaValueError, SchemaValueKind, decode_schema_value, encode_schema_value,
    validate_document,
};
use std::cell::RefCell;
use std::collections::HashSet;

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

    /// Batch read by primary keys.
    ///
    /// Reads each key independently (no snapshot isolation).
    /// Duplicate keys are deduplicated. Missing keys are silently skipped.
    pub fn multi_get_schema(
        &self,
        schema: &Schema,
        keys: &[SchemaValue],
        options: ReadOptions,
    ) -> EngineResult<Vec<SchemaReadResult>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut seen = HashSet::new();
        let mut results = Vec::new();

        for key in keys {
            let raw_key = schema_primary_key_raw_key(schema, key)?;
            if !seen.insert(raw_key) {
                continue;
            }
            if let Some(doc) = self.get_schema(schema, key, options)? {
                results.push(doc);
            }
        }

        Ok(results)
    }

    /// Count documents in a schema collection.
    ///
    /// Returns the total number of documents using the atomic per-collection counter.
    pub fn count_schema(&self, schema: &Schema) -> EngineResult<u64> {
        self.collection_count(&schema.name)
    }

    /// Partial update with optimistic CAS retry (max 3 attempts).
    ///
    /// Reads the current document, applies all mutations in order,
    /// re-validates against the schema, then writes with expected version.
    /// On conflict, retries from a fresh read. Primary key field cannot be modified.
    pub fn update_schema(
        &self,
        schema: &Schema,
        primary_key: &SchemaValue,
        operations: FieldUpdateOp,
        options: WriteOptions,
    ) -> EngineResult<WriteResult> {
        const MAX_RETRIES: u32 = 3;
        let mut retries = 0;

        loop {
            let current = match self.get_schema(schema, primary_key, ReadOptions::default())? {
                Some(doc) => doc,
                None => {
                    return Err(EngineError::DocumentNotFound {
                        collection: schema.name.clone(),
                    });
                }
            };

            let mut encoded = Vec::new();
            encode_schema_value(&current.document, &mut encoded);
            let mut mutated = decode_schema_value(&encoded).map_err(|e| {
                EngineError::SchemaDecode {
                    collection: schema.name.clone(),
                    message: e.to_string(),
                }
            })?;

            let primary_field =
                schema
                    .primary_field()
                    .ok_or_else(|| EngineError::SchemaMissingPrimaryField {
                        collection: schema.name.clone(),
                    })?;

            for update in &operations.updates {
                match update {
                    FieldUpdate::Set { field, value } => {
                        if field == &primary_field.name {
                            return Err(EngineError::PrimaryKeyUpdateForbidden);
                        }
                        mutated.set_field(field, value.clone()).map_err(|_| {
                            EngineError::InvalidUpdatePath {
                                field: field.clone(),
                                reason: "path traverses non-object or is otherwise invalid",
                            }
                        })?;
                    }
                    FieldUpdate::Unset { field } => {
                        if field == &primary_field.name {
                            return Err(EngineError::PrimaryKeyUpdateForbidden);
                        }
                        mutated.unset_field(field);
                    }
                    FieldUpdate::Increment { field, delta } => {
                        if field == &primary_field.name {
                            return Err(EngineError::PrimaryKeyUpdateForbidden);
                        }
                        mutated
                            .apply_increment(field, *delta)
                            .map_err(|e| map_increment_error(field, &e))?;
                    }
                }
            }

            validate_document(schema, &mutated).map_err(|error| {
                EngineError::SchemaDocument {
                    collection: schema.name.clone(),
                    error,
                }
            })?;

            match self.put_schema(
                schema,
                mutated,
                WriteOptions {
                    expected_version: Some(current.version),
                    ..options
                },
            ) {
                Ok(result) => return Ok(result),
                Err(EngineError::Conflict { .. }) => {
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        return Err(EngineError::Conflict {
                            collection: schema.name.clone(),
                            key: schema_primary_key_raw_key(schema, primary_key)?,
                            expected: current.version,
                            actual: DocumentVersion::ZERO,
                        });
                    }
                }
                Err(e) => return Err(e),
            }
        }
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

fn map_increment_error(field: &str, err: &SchemaValueError) -> EngineError {
    let msg = err.to_string();
    if msg.contains("overflow") || msg.contains("NumericOverflow") {
        EngineError::NumericOverflow {
            field: field.to_string(),
        }
    } else if msg.contains("incrementable") || msg.contains("NotIncrementable") {
        match err {
            SchemaValueError::NotIncrementable { actual, .. } => {
                EngineError::UpdateTypeMismatch {
                    field: field.to_string(),
                    expected: "numeric",
                    actual: *actual,
                }
            }
            _ => EngineError::UpdateTypeMismatch {
                field: field.to_string(),
                expected: "numeric",
                actual: SchemaValueKind::Null,
            },
        }
    } else {
        EngineError::InvalidUpdatePath {
            field: field.to_string(),
            reason: "invalid for increment",
        }
    }
}
