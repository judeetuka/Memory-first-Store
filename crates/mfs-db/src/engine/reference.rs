use crate::engine::{
    EngineError, EngineResult, NoSqlEngine, RawKey, ReadOptions, SchemaReadResult,
    schema_primary_key_raw_key,
};
use crate::schema::{Reference, Schema};
use crate::schema_value::SchemaValue;
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaForwardReferenceInclude {
    pub document: SchemaReadResult,
    pub reference_key: Option<RawKey>,
    pub referenced: Option<SchemaReadResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaReverseReferenceInclude {
    pub primary_key: RawKey,
    pub document: SchemaReadResult,
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaReferenceWritePlan {
    old_entries: Vec<PreparedReferenceEntry>,
    new_entries: Vec<PreparedReferenceEntry>,
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaReferenceMaps {
    forward: Vec<HashMap<RawKey, RawKey>>,
    reverse: Vec<HashMap<RawKey, Vec<RawKey>>>,
}

type ForwardReferenceShard = RwLock<HashMap<RawKey, RawKey>>;
type ReverseReferenceShard = RwLock<HashMap<RawKey, Vec<RawKey>>>;

pub(crate) struct SchemaCollectionReferences {
    fields: Vec<ReferenceField>,
    forward: Box<[ForwardReferenceShard]>,
    reverse: Box<[ReverseReferenceShard]>,
}

#[derive(Debug, Clone)]
struct ReferenceField {
    name: String,
    reference: Reference,
}

#[derive(Debug, Clone)]
struct PreparedReferenceEntry {
    field_idx: usize,
    target_key: RawKey,
}

impl NoSqlEngine {
    pub fn include_schema_reference(
        &self,
        schema: &Schema,
        primary_key: &SchemaValue,
        reference_field: &str,
        options: ReadOptions,
    ) -> EngineResult<Option<SchemaForwardReferenceInclude>> {
        let state = self.ensure_schema_indexes(schema)?;
        let _read_unit = state.lock_read_unit();
        let source_key = schema_primary_key_raw_key(schema, primary_key)?;
        let Some(document) = self.get_schema_by_raw_key(schema, &source_key, options)? else {
            return Ok(None);
        };

        let reference_key =
            state.reference_key_from_document(self, reference_field, &document.document)?;
        let referenced = match &reference_key {
            Some(target_key) => {
                let target_schema = state.reference_target_schema(self, reference_field)?;
                if target_schema.name == schema.name {
                    self.get_schema_by_raw_key(&target_schema, target_key, options)?
                } else {
                    let target_state = self.ensure_schema_indexes(&target_schema)?;
                    let _target_read_unit = target_state.lock_read_unit();
                    self.get_schema_by_raw_key(&target_schema, target_key, options)?
                }
            }
            None => None,
        };

        Ok(Some(SchemaForwardReferenceInclude {
            document,
            reference_key,
            referenced,
        }))
    }

    pub fn include_schema_reverse(
        &self,
        target_schema: &Schema,
        target_primary_key: &SchemaValue,
        source_schema: &Schema,
        reference_field: &str,
        options: ReadOptions,
    ) -> EngineResult<Vec<SchemaReverseReferenceInclude>> {
        let target_state = self.ensure_schema_indexes(target_schema)?;
        if target_state.schema() != target_schema {
            return Err(EngineError::SchemaDeclarationMismatch {
                collection: target_schema.name.clone(),
            });
        }
        let source_state = self.ensure_schema_indexes(source_schema)?;
        source_state.ensure_reference_targets(reference_field, target_schema)?;
        let _read_unit = source_state.lock_read_unit();

        let target_key = schema_primary_key_raw_key(target_schema, target_primary_key)?;
        let keys = source_state.reverse_reference_keys(reference_field, &target_key)?;
        let mut out = Vec::with_capacity(keys.len());

        for key in keys {
            let Some(document) = self.get_schema_by_raw_key(source_schema, &key, options)? else {
                continue;
            };
            if source_state.reference_key_from_document(
                self,
                reference_field,
                &document.document,
            )? == Some(target_key.clone())
            {
                out.push(SchemaReverseReferenceInclude {
                    primary_key: key,
                    document,
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
}

impl SchemaCollectionReferences {
    pub(crate) fn new(schema: &Schema) -> Self {
        let fields = schema
            .fields
            .iter()
            .filter_map(|field| {
                field.reference.as_ref().map(|reference| ReferenceField {
                    name: field.name.clone(),
                    reference: reference.clone(),
                })
            })
            .collect::<Vec<_>>();

        let forward = (0..fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let reverse = (0..fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            fields,
            forward,
            reverse,
        }
    }

    pub(crate) fn prepare_put(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        old: Option<&SchemaValue>,
        new: &SchemaValue,
    ) -> EngineResult<SchemaReferenceWritePlan> {
        Ok(SchemaReferenceWritePlan {
            old_entries: match old {
                Some(document) => self.document_entries(engine, schema, document)?,
                None => Vec::new(),
            },
            new_entries: self.document_entries(engine, schema, new)?,
        })
    }

    pub(crate) fn prepare_delete(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        old: Option<&SchemaValue>,
    ) -> EngineResult<SchemaReferenceWritePlan> {
        Ok(SchemaReferenceWritePlan {
            old_entries: match old {
                Some(document) => self.document_entries(engine, schema, document)?,
                None => Vec::new(),
            },
            new_entries: Vec::new(),
        })
    }

    pub(crate) fn apply_write(&self, source_key: &RawKey, plan: SchemaReferenceWritePlan) {
        for entry in plan.old_entries {
            self.forward[entry.field_idx].write().remove(source_key);
            let mut reverse = self.reverse[entry.field_idx].write();
            if let Some(sources) = reverse.get_mut(&entry.target_key) {
                sources.retain(|existing| existing != source_key);
                if sources.is_empty() {
                    reverse.remove(&entry.target_key);
                }
            }
        }

        for entry in plan.new_entries {
            self.forward[entry.field_idx]
                .write()
                .insert(source_key.clone(), entry.target_key.clone());
            let mut reverse = self.reverse[entry.field_idx].write();
            let sources = reverse.entry(entry.target_key).or_default();
            if !sources.iter().any(|existing| existing == source_key) {
                sources.push(source_key.clone());
            }
        }
    }

    pub(crate) fn empty_maps(&self) -> SchemaReferenceMaps {
        SchemaReferenceMaps {
            forward: (0..self.fields.len()).map(|_| HashMap::new()).collect(),
            reverse: (0..self.fields.len()).map(|_| HashMap::new()).collect(),
        }
    }

    pub(crate) fn rebuild_document(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        source_key: &RawKey,
        document: &SchemaValue,
        maps: &mut SchemaReferenceMaps,
    ) -> EngineResult<()> {
        for entry in self.document_entries(engine, schema, document)? {
            maps.forward[entry.field_idx].insert(source_key.clone(), entry.target_key.clone());
            let sources = maps.reverse[entry.field_idx]
                .entry(entry.target_key)
                .or_default();
            if !sources.iter().any(|existing| existing == source_key) {
                sources.push(source_key.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn replace_maps(&self, maps: SchemaReferenceMaps) {
        for (idx, map) in maps.forward.into_iter().enumerate() {
            *self.forward[idx].write() = map;
        }
        for (idx, map) in maps.reverse.into_iter().enumerate() {
            *self.reverse[idx].write() = map;
        }
    }

    fn document_entries(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        document: &SchemaValue,
    ) -> EngineResult<Vec<PreparedReferenceEntry>> {
        let mut entries = Vec::new();
        for (field_idx, field) in self.fields.iter().enumerate() {
            let Some(value) = document.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            let target_key = self.target_key_from_value(engine, schema, field, value)?;
            entries.push(PreparedReferenceEntry {
                field_idx,
                target_key,
            });
        }
        Ok(entries)
    }

    fn target_key_from_value(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        field: &ReferenceField,
        value: &SchemaValue,
    ) -> EngineResult<RawKey> {
        let target = engine
            .schema_indexes_for_collection(&field.reference.collection)
            .ok_or_else(|| EngineError::ReferenceTargetCollectionNotFound {
                collection: schema.name.clone(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
            })?;
        let primary = target.schema().primary_field().ok_or_else(|| {
            EngineError::SchemaMissingPrimaryField {
                collection: target.schema().name.clone(),
            }
        })?;
        if primary.name != field.reference.field {
            return Err(EngineError::ReferenceTargetNotPrimary {
                collection: schema.name.clone(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
                target_field: field.reference.field.clone(),
            });
        }
        schema_primary_key_raw_key(target.schema(), value)
    }

    pub(crate) fn reference_key_from_document(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        reference_field: &str,
        document: &SchemaValue,
    ) -> EngineResult<Option<RawKey>> {
        let (_, field) = self.reference_field(schema, reference_field)?;
        let Some(value) = document.field(&field.name) else {
            return Ok(None);
        };
        if matches!(value, SchemaValue::Null) {
            return Ok(None);
        }
        self.target_key_from_value(engine, schema, field, value)
            .map(Some)
    }

    pub(crate) fn reference_target_schema(
        &self,
        engine: &NoSqlEngine,
        schema: &Schema,
        reference_field: &str,
    ) -> EngineResult<Schema> {
        let (_, field) = self.reference_field(schema, reference_field)?;
        let target = engine
            .schema_indexes_for_collection(&field.reference.collection)
            .ok_or_else(|| EngineError::ReferenceTargetCollectionNotFound {
                collection: schema.name.clone(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
            })?;
        let primary = target.schema().primary_field().ok_or_else(|| {
            EngineError::SchemaMissingPrimaryField {
                collection: target.schema().name.clone(),
            }
        })?;
        if primary.name != field.reference.field {
            return Err(EngineError::ReferenceTargetNotPrimary {
                collection: schema.name.clone(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
                target_field: field.reference.field.clone(),
            });
        }
        Ok(target.schema().clone())
    }

    pub(crate) fn reverse_reference_keys(
        &self,
        schema: &Schema,
        reference_field: &str,
        target_key: &RawKey,
    ) -> EngineResult<Vec<RawKey>> {
        let (idx, _) = self.reference_field(schema, reference_field)?;
        Ok(self.reverse[idx]
            .read()
            .get(target_key)
            .cloned()
            .unwrap_or_default())
    }

    pub(crate) fn ensure_reference_targets(
        &self,
        schema: &Schema,
        reference_field: &str,
        target_schema: &Schema,
    ) -> EngineResult<()> {
        let (_, field) = self.reference_field(schema, reference_field)?;
        if field.reference.collection != target_schema.name {
            return Err(EngineError::ReferenceTargetMismatch {
                collection: schema.name.clone(),
                field: reference_field.to_string(),
                expected_collection: target_schema.name.clone(),
                actual_collection: field.reference.collection.clone(),
            });
        }
        let primary = target_schema.primary_field().ok_or_else(|| {
            EngineError::SchemaMissingPrimaryField {
                collection: target_schema.name.clone(),
            }
        })?;
        if primary.name != field.reference.field {
            return Err(EngineError::ReferenceTargetNotPrimary {
                collection: schema.name.clone(),
                field: reference_field.to_string(),
                target_collection: field.reference.collection.clone(),
                target_field: field.reference.field.clone(),
            });
        }
        Ok(())
    }

    fn reference_field(
        &self,
        schema: &Schema,
        reference_field: &str,
    ) -> EngineResult<(usize, &ReferenceField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == reference_field)
            .ok_or_else(|| EngineError::ReferenceFieldNotFound {
                collection: schema.name.clone(),
                field: reference_field.to_string(),
            })
    }
}

impl crate::engine::index::SchemaCollectionIndexes {
    pub(crate) fn reference_key_from_document(
        &self,
        engine: &NoSqlEngine,
        reference_field: &str,
        document: &SchemaValue,
    ) -> EngineResult<Option<RawKey>> {
        self.references.reference_key_from_document(
            engine,
            self.schema(),
            reference_field,
            document,
        )
    }

    pub(crate) fn reference_target_schema(
        &self,
        engine: &NoSqlEngine,
        reference_field: &str,
    ) -> EngineResult<Schema> {
        self.references
            .reference_target_schema(engine, self.schema(), reference_field)
    }

    pub(crate) fn reverse_reference_keys(
        &self,
        reference_field: &str,
        target_key: &RawKey,
    ) -> EngineResult<Vec<RawKey>> {
        self.references
            .reverse_reference_keys(self.schema(), reference_field, target_key)
    }

    pub(crate) fn ensure_reference_targets(
        &self,
        reference_field: &str,
        target_schema: &Schema,
    ) -> EngineResult<()> {
        self.references
            .ensure_reference_targets(self.schema(), reference_field, target_schema)
    }
}
