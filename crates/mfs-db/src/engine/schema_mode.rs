use crate::engine::index::{
    SchemaCollectionIndexes, SchemaIndexWritePlan, decode_schema_raw_value,
};
use crate::engine::query::{compare_for_sort, evaluate_filter};
use crate::engine::{
    CollectionId, DocumentVersion, EngineError, EngineResult, FilterClause, FilterOp, NoSqlEngine,
    QueryOptions, QueryResult, RawKey, RawValue, ReadOptions, SortDirection, WriteOptions,
    WriteResult,
};
use crate::schema::{Schema, SchemaField, SchemaFieldType};
use crate::schema_value::{SchemaValue, SchemaValueError, encode_schema_value, validate_document};
use std::cell::RefCell;
use std::cmp::Ordering;
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

    pub fn query_schema(
        &self,
        schema: &Schema,
        options: QueryOptions,
    ) -> EngineResult<QueryResult> {
        let state = self.ensure_schema_indexes(schema)?;
        let _read_unit = state.lock_read_unit();

        // Phase 1: Filter — collect matching (key, document, version) triples.
        let mut results: Vec<(RawKey, SchemaValue, DocumentVersion)> = Vec::new();

        if let Some(ref filter) = options.filter {
            if filter.op == FilterOp::Eq {
                match self.lookup_schema(
                    schema,
                    &filter.field,
                    &filter.value,
                    ReadOptions::default(),
                ) {
                    Ok(lookup_results) => {
                        results = lookup_results
                            .into_iter()
                            .map(|r| (r.primary_key, r.document, r.version))
                            .collect();
                    }
                    Err(EngineError::UnindexedField { .. }) => {
                        scan_collect(self, schema, Some(filter), &mut results)?;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                scan_collect(self, schema, Some(filter), &mut results)?;
            }
        } else {
            scan_collect(self, schema, None, &mut results)?;
        }

        // Phase 2: Sort.
        let descending = options.sort_direction == SortDirection::Desc;
        if let Some(ref sort_field) = options.sort_field {
            results.sort_by(|(key_a, doc_a, _), (key_b, doc_b, _)| {
                let field_a = doc_a.field(sort_field);
                let field_b = doc_b.field(sort_field);
                let null_val = SchemaValue::Null;
                let ord = compare_for_sort(
                    field_a.unwrap_or(&null_val),
                    field_b.unwrap_or(&null_val),
                );
                if ord != Ordering::Equal {
                    if descending {
                        ord.reverse()
                    } else {
                        ord
                    }
                } else {
                    let key_ord = key_a.as_bytes().cmp(key_b.as_bytes());
                    if descending {
                        key_ord.reverse()
                    } else {
                        key_ord
                    }
                }
            });
        } else {
            results.sort_by(|(key_a, _, _), (key_b, _, _)| {
                key_a.as_bytes().cmp(key_b.as_bytes())
            });
        }

        // Phase 3: Paginate.
        let offset = options.offset.unwrap_or(0);
        let limit = options.limit.unwrap_or(usize::MAX);
        let documents: Vec<SchemaReadResult> = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(_, document, version)| SchemaReadResult { document, version })
            .collect();

        Ok(QueryResult {
            documents,
            total_count: None,
        })
    }

    pub fn count_schema(
        &self,
        schema: &Schema,
        filter: Option<FilterClause>,
    ) -> EngineResult<u64> {
        match filter {
            None => self.collection_count(&schema.name),
            Some(filter) => {
                let options = QueryOptions {
                    filter: Some(filter),
                    sort_field: None,
                    sort_direction: SortDirection::Asc,
                    limit: None,
                    offset: None,
                };
                let result = self.query_schema(schema, options)?;
                Ok(result.documents.len() as u64)
            }
        }
    }
}

fn scan_collect(
    engine: &NoSqlEngine,
    schema: &Schema,
    filter: Option<&FilterClause>,
    results: &mut Vec<(RawKey, SchemaValue, DocumentVersion)>,
) -> EngineResult<()> {
    let mut error: Option<EngineError> = None;

    engine.for_each_raw_record(&schema.name, |key, raw_value, version| {
        if error.is_some() {
            return;
        }
        let document = match decode_schema_raw_value(schema, raw_value) {
            Ok(d) => d,
            Err(e) => {
                error = Some(e);
                return;
            }
        };
        if let Some(f) = filter {
            let field_val = match document.field(&f.field) {
                Some(v) => v,
                None => return,
            };
            match evaluate_filter(field_val, f.op, &f.value) {
                Ok(true) => results.push((key.clone(), document, version)),
                Ok(false) => {}
                Err(e) => error = Some(e),
            }
        } else {
            results.push((key.clone(), document, version));
        }
    })?;

    if let Some(e) = error {
        return Err(e);
    }
    Ok(())
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
