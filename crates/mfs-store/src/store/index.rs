use crate::store::reference::{SchemaCollectionReferences, SchemaReferenceWritePlan};
use crate::store::{
    DocumentVersion, StoreError, StoreResult, MfsStore, RawKey, RawValue, ReadOptions,
    schema_document_raw_key,
};
use crate::schema::{Schema, SchemaFieldType};
use crate::schema_value::{SchemaValue, SchemaValueKind, decode_schema_value, validate_document};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::Arc;

type SchemaIndexMap = HashMap<SchemaExactIndexKey, Vec<RawKey>>;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaLookupResult {
    pub primary_key: RawKey,
    pub document: SchemaValue,
    pub version: DocumentVersion,
}

pub(crate) struct SchemaCollectionIndexes {
    schema: Schema,
    write_unit_lock: RwLock<()>,
    indexed_fields: Vec<IndexedField>,
    indexes: Box<[RwLock<SchemaIndexMap>]>,
    pub(crate) references: SchemaCollectionReferences,
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaIndexWritePlan {
    old_entries: Vec<PreparedIndexEntry>,
    new_entries: Vec<PreparedIndexEntry>,
    reference_plan: SchemaReferenceWritePlan,
}

#[derive(Debug, Clone)]
struct IndexedField {
    name: String,
    field_type: SchemaFieldType,
    unique: bool,
}

#[derive(Debug, Clone)]
struct PreparedIndexEntry {
    field_idx: usize,
    key: SchemaExactIndexKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SchemaExactIndexKey {
    String(String),
    Int32(i32),
    Int64(i64),
    Float(u64),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl MfsStore {
    pub(crate) fn install_schema_indexes(
        &self,
        schema: &Schema,
        indexes: SchemaCollectionIndexes,
    ) -> StoreResult<()> {
        let mut schema_indexes = self.inner.schema_indexes.write();
        if let Some(existing) = schema_indexes.get(&schema.name) {
            if existing.schema() != schema {
                return Err(StoreError::SchemaDeclarationMismatch {
                    collection: schema.name.clone(),
                });
            }
            return Ok(());
        }

        schema_indexes.insert(schema.name.clone(), Arc::new(indexes));
        Ok(())
    }

    pub(crate) fn ensure_schema_indexes(
        &self,
        schema: &Schema,
    ) -> StoreResult<Arc<SchemaCollectionIndexes>> {
        schema
            .validate()
            .map_err(|error| StoreError::SchemaDefinition {
                collection: schema.name.clone(),
                error,
            })?;

        if let Some(existing) = self.schema_indexes_for_collection(&schema.name) {
            if existing.schema() != schema {
                return Err(StoreError::SchemaDeclarationMismatch {
                    collection: schema.name.clone(),
                });
            }
            return Ok(existing);
        }

        let indexes = Arc::new(SchemaCollectionIndexes::new(schema)?);
        let mut schema_indexes = self.inner.schema_indexes.write();
        match schema_indexes.get(&schema.name) {
            Some(existing) if existing.schema() == schema => Ok(Arc::clone(existing)),
            Some(_) => Err(StoreError::SchemaDeclarationMismatch {
                collection: schema.name.clone(),
            }),
            None => {
                schema_indexes.insert(schema.name.clone(), Arc::clone(&indexes));
                Ok(indexes)
            }
        }
    }

    pub(crate) fn schema_indexes_for_collection(
        &self,
        collection: &str,
    ) -> Option<Arc<SchemaCollectionIndexes>> {
        self.inner.schema_indexes.read().get(collection).cloned()
    }

    pub fn lookup_schema(
        &self,
        schema: &Schema,
        field: &str,
        value: &SchemaValue,
        options: ReadOptions,
    ) -> StoreResult<Vec<SchemaLookupResult>> {
        let state = self.ensure_schema_indexes(schema)?;
        let _read_unit = state.lock_read_unit();
        let (field_idx, query_key, keys) = state.lookup_keys(field, value)?;
        let indexed_field = state.indexed_fields[field_idx].clone();

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(read) = self.get_schema_by_raw_key(schema, &key, options)? else {
                continue;
            };
            if state.document_matches_index(&indexed_field, &query_key, &read.document)?
                && schema_document_raw_key(schema, &read.document)? == key
            {
                out.push(SchemaLookupResult {
                    primary_key: key,
                    document: read.document,
                    version: read.version,
                });
            }
        }
        out.sort_by(|left, right| {
            left.primary_key
                .as_bytes()
                .cmp(right.primary_key.as_bytes())
        });
        Ok(out)
    }

    pub fn rebuild_schema_indexes(&self, schema: &Schema) -> StoreResult<usize> {
        let state = self.ensure_schema_indexes(schema)?;
        let snapshot = self.raw_snapshot();
        let collection = snapshot
            .collections
            .into_iter()
            .find(|collection| collection.name == schema.name)
            .ok_or_else(|| StoreError::CollectionNotFound {
                collection: schema.name.clone(),
            })?;

        let mut documents = Vec::new();
        for record in collection.records {
            let Some(value) = record.value else {
                continue;
            };
            let document = decode_schema_raw_value(schema, &value)?;
            if schema_document_raw_key(schema, &document)? == record.key {
                documents.push((record.key, document));
            }
        }

        let count = documents.len();
        state.rebuild_from_documents(self, documents)?;
        Ok(count)
    }
}

impl SchemaCollectionIndexes {
    pub(crate) fn new(schema: &Schema) -> StoreResult<Self> {
        schema
            .validate()
            .map_err(|error| StoreError::SchemaDefinition {
                collection: schema.name.clone(),
                error,
            })?;

        let mut indexed_fields = Vec::new();
        for field in schema
            .fields
            .iter()
            .filter(|field| field.indexed || field.unique)
        {
            if !is_exact_index_type(&field.field_type) {
                return Err(StoreError::UnsupportedExactIndex {
                    collection: schema.name.clone(),
                    field: field.name.clone(),
                });
            }
            indexed_fields.push(IndexedField {
                name: field.name.clone(),
                field_type: field.field_type.clone(),
                unique: field.unique,
            });
        }

        let indexes = (0..indexed_fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Ok(Self {
            schema: schema.clone(),
            write_unit_lock: RwLock::new(()),
            indexed_fields,
            indexes,
            references: SchemaCollectionReferences::new(schema),
        })
    }

    pub(crate) fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn lock_write_unit(&self) -> RwLockWriteGuard<'_, ()> {
        self.write_unit_lock.write()
    }

    pub(crate) fn lock_read_unit(&self) -> RwLockReadGuard<'_, ()> {
        self.write_unit_lock.read()
    }

    pub(crate) fn prepare_put(
        &self,
        engine: &MfsStore,
        primary_key: &RawKey,
        old: Option<&SchemaValue>,
        new: &SchemaValue,
    ) -> StoreResult<SchemaIndexWritePlan> {
        let old_entries = match old {
            Some(document) => self.document_index_entries(document)?,
            None => Vec::new(),
        };
        let new_entries = self.document_index_entries(new)?;
        self.validate_unique_constraints(engine, primary_key, &new_entries)?;

        let reference_plan = self
            .references
            .prepare_put(engine, &self.schema, old, new)?;
        Ok(SchemaIndexWritePlan {
            old_entries,
            new_entries,
            reference_plan,
        })
    }

    pub(crate) fn prepare_delete(
        &self,
        engine: &MfsStore,
        old: Option<&SchemaValue>,
    ) -> StoreResult<SchemaIndexWritePlan> {
        let old_entries = match old {
            Some(document) => self.document_index_entries(document)?,
            None => Vec::new(),
        };
        let reference_plan = self.references.prepare_delete(engine, &self.schema, old)?;
        Ok(SchemaIndexWritePlan {
            old_entries,
            new_entries: Vec::new(),
            reference_plan,
        })
    }

    pub(crate) fn apply_write(&self, primary_key: &RawKey, plan: SchemaIndexWritePlan) {
        for entry in plan.old_entries {
            let mut index = self.indexes[entry.field_idx].write();
            if let Some(keys) = index.get_mut(&entry.key) {
                keys.retain(|existing| existing != primary_key);
                if keys.is_empty() {
                    index.remove(&entry.key);
                }
            }
        }

        for entry in plan.new_entries {
            let mut index = self.indexes[entry.field_idx].write();
            let keys = index.entry(entry.key).or_default();
            if !keys.iter().any(|existing| existing == primary_key) {
                keys.push(primary_key.clone());
            }
        }

        self.references
            .apply_write(primary_key, plan.reference_plan);
    }

    fn lookup_keys(
        &self,
        field: &str,
        value: &SchemaValue,
    ) -> StoreResult<(usize, SchemaExactIndexKey, Vec<RawKey>)> {
        let Some((idx, indexed_field)) = self.indexed_field(field) else {
            return Err(StoreError::UnindexedField {
                collection: self.schema.name.clone(),
                field: field.to_string(),
            });
        };
        let key = index_key_from_value(&self.schema.name, field, &indexed_field.field_type, value)?;
        let index = self.indexes[idx].read();
        Ok((
            idx,
            key.clone(),
            index.get(&key).cloned().unwrap_or_default(),
        ))
    }

    fn indexed_field(&self, field: &str) -> Option<(usize, &IndexedField)> {
        self.indexed_fields
            .iter()
            .enumerate()
            .find(|(_, indexed)| indexed.name == field)
    }

    fn document_index_entries(
        &self,
        document: &SchemaValue,
    ) -> StoreResult<Vec<PreparedIndexEntry>> {
        let mut entries = Vec::new();
        for (field_idx, field) in self.indexed_fields.iter().enumerate() {
            let Some(value) = document.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            entries.push(PreparedIndexEntry {
                field_idx,
                key: index_key_from_value(
                    &self.schema.name,
                    &field.name,
                    &field.field_type,
                    value,
                )?,
            });
        }
        Ok(entries)
    }

    fn validate_unique_constraints(
        &self,
        engine: &MfsStore,
        primary_key: &RawKey,
        entries: &[PreparedIndexEntry],
    ) -> StoreResult<()> {
        for entry in entries {
            let field = &self.indexed_fields[entry.field_idx];
            if !field.unique {
                continue;
            }

            let keys = self.indexes[entry.field_idx]
                .read()
                .get(&entry.key)
                .cloned()
                .unwrap_or_default();

            for existing in keys {
                if existing == *primary_key {
                    continue;
                }
                if self.raw_document_matches_index(engine, &existing, field, &entry.key)? {
                    return Err(StoreError::UniqueIndexConflict {
                        collection: self.schema.name.clone(),
                        field: field.name.clone(),
                        existing,
                    });
                }
            }
        }
        Ok(())
    }

    fn raw_document_matches_index(
        &self,
        engine: &MfsStore,
        primary_key: &RawKey,
        field: &IndexedField,
        expected_key: &SchemaExactIndexKey,
    ) -> StoreResult<bool> {
        // Writer already holds the write lock; use raw access directly
        // so we never try to acquire a read lock on the same RwLock.
        let Some(raw) = engine.get_raw(&self.schema.name, primary_key, ReadOptions::default())?
        else {
            return Ok(false);
        };
        let document = decode_schema_raw_value(&self.schema, &raw.value)?;
        if schema_document_raw_key(&self.schema, &document)? != *primary_key {
            return Ok(false);
        }
        self.document_matches_index(field, expected_key, &document)
    }

    fn document_matches_index(
        &self,
        field: &IndexedField,
        expected_key: &SchemaExactIndexKey,
        document: &SchemaValue,
    ) -> StoreResult<bool> {
        let Some(value) = document.field(&field.name) else {
            return Ok(false);
        };
        if matches!(value, SchemaValue::Null) {
            return Ok(false);
        }
        Ok(
            index_key_from_value(&self.schema.name, &field.name, &field.field_type, value)?
                == *expected_key,
        )
    }

    fn rebuild_from_documents(
        &self,
        engine: &MfsStore,
        documents: Vec<(RawKey, SchemaValue)>,
    ) -> StoreResult<()> {
        let mut rebuilt_indexes = (0..self.indexed_fields.len())
            .map(|_| HashMap::new())
            .collect::<Vec<SchemaIndexMap>>();
        let mut rebuilt_references = self.references.empty_maps();

        for (primary_key, document) in documents {
            let entries = self.document_index_entries(&document)?;
            for entry in entries {
                let field = &self.indexed_fields[entry.field_idx];
                let keys = rebuilt_indexes[entry.field_idx]
                    .entry(entry.key)
                    .or_insert_with(Vec::new);
                if field.unique && keys.iter().any(|existing| existing != &primary_key) {
                    let existing = keys
                        .iter()
                        .find(|existing| *existing != &primary_key)
                        .expect("unique conflict key exists")
                        .clone();
                    return Err(StoreError::UniqueIndexConflict {
                        collection: self.schema.name.clone(),
                        field: field.name.clone(),
                        existing,
                    });
                }
                if !keys.iter().any(|existing| existing == &primary_key) {
                    keys.push(primary_key.clone());
                }
            }

            self.references.rebuild_document(
                engine,
                &self.schema,
                &primary_key,
                &document,
                &mut rebuilt_references,
            )?;
        }

        let _guard = self.lock_write_unit();
        for (idx, rebuilt) in rebuilt_indexes.into_iter().enumerate() {
            *self.indexes[idx].write() = rebuilt;
        }
        self.references.replace_maps(rebuilt_references);
        Ok(())
    }
}

pub(crate) fn decode_schema_raw_value(
    schema: &Schema,
    raw: &RawValue,
) -> StoreResult<SchemaValue> {
    let document =
        decode_schema_value(raw.as_bytes()).map_err(|error| StoreError::SchemaDecode {
            collection: schema.name.clone(),
            message: error.to_string(),
        })?;
    validate_document(schema, &document).map_err(|error| StoreError::SchemaDocument {
        collection: schema.name.clone(),
        error,
    })?;
    Ok(document)
}

pub(crate) fn index_key_from_value(
    collection: &str,
    field: &str,
    field_type: &SchemaFieldType,
    value: &SchemaValue,
) -> StoreResult<SchemaExactIndexKey> {
    match (field_type, value) {
        (SchemaFieldType::String, SchemaValue::String(value)) => {
            Ok(SchemaExactIndexKey::String(value.clone()))
        }
        (SchemaFieldType::Int32, SchemaValue::Int32(value)) => {
            Ok(SchemaExactIndexKey::Int32(*value))
        }
        (SchemaFieldType::Int32, SchemaValue::Int64(value)) => i32::try_from(*value)
            .map(SchemaExactIndexKey::Int32)
            .map_err(|_| index_key_mismatch(collection, field, field_type, SchemaValueKind::Int64)),
        (SchemaFieldType::Int64, SchemaValue::Int32(value)) => {
            Ok(SchemaExactIndexKey::Int64(i64::from(*value)))
        }
        (SchemaFieldType::Int64, SchemaValue::Int64(value)) => {
            Ok(SchemaExactIndexKey::Int64(*value))
        }
        (SchemaFieldType::Float, SchemaValue::Int32(value)) => {
            Ok(SchemaExactIndexKey::Float(f64::from(*value).to_bits()))
        }
        (SchemaFieldType::Float, SchemaValue::Int64(value)) => {
            Ok(SchemaExactIndexKey::Float((*value as f64).to_bits()))
        }
        (SchemaFieldType::Float, SchemaValue::Float(value)) if value.is_finite() => {
            Ok(SchemaExactIndexKey::Float(value.to_bits()))
        }
        (SchemaFieldType::Bool, SchemaValue::Bool(value)) => Ok(SchemaExactIndexKey::Bool(*value)),
        (SchemaFieldType::Bytes, SchemaValue::Bytes(value)) => {
            Ok(SchemaExactIndexKey::Bytes(value.clone()))
        }
        _ => Err(index_key_mismatch(
            collection,
            field,
            field_type,
            value.kind(),
        )),
    }
}

fn is_exact_index_type(field_type: &SchemaFieldType) -> bool {
    matches!(
        field_type,
        SchemaFieldType::String
            | SchemaFieldType::Int32
            | SchemaFieldType::Int64
            | SchemaFieldType::Float
            | SchemaFieldType::Bool
            | SchemaFieldType::Bytes
    )
}

fn index_key_mismatch(
    collection: &str,
    field: &str,
    expected: &SchemaFieldType,
    actual: SchemaValueKind,
) -> StoreError {
    StoreError::SchemaIndexKeyTypeMismatch {
        collection: collection.to_string(),
        field: field.to_string(),
        expected: schema_type_name(expected),
        actual,
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
