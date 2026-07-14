//! Schema-aware in-process document store.

use crossbeam_utils::CachePadded;
use hashbrown::HashMap;
use mfs_core::writeback::{WriteBehindCache, WriteBehindConfig, WriteBehindStats};
use mfs_core::{FastBuildHasher, FlushBackend};
use mfs_store::schema::{Reference, Schema, SchemaError, SchemaFieldType};
use mfs_store::schema_value::{SchemaValue, SchemaValueError, SchemaValueKind, validate_document};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

const DEFAULT_MUTATION_LOCKS: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchemaKey {
    String(String),
    Int32(i32),
    Int64(i64),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchemaIndexKey {
    String(String),
    Int32(i32),
    Int64(i64),
    Float(u64),
    Bool(bool),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaStoreError {
    Schema(SchemaError),
    SchemaValue(SchemaValueError),
    CollectionAlreadyRegistered {
        collection: String,
    },
    CollectionNotFound {
        collection: String,
    },
    DocumentAlreadyExists {
        collection: String,
        key: SchemaKey,
    },
    DocumentNotFound {
        collection: String,
        key: SchemaKey,
    },
    StoreFull {
        collection: String,
    },
    MissingPrimaryField {
        collection: String,
    },
    MissingPrimaryKey {
        collection: String,
        field: String,
    },
    PrimaryKeyTypeMismatch {
        collection: String,
        field: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
    PrimaryKeyChanged {
        collection: String,
        expected: SchemaKey,
        actual: SchemaKey,
    },
    ReferenceFieldNotFound {
        collection: String,
        field: String,
    },
    ReferenceTargetCollectionNotFound {
        collection: String,
        field: String,
        target_collection: String,
    },
    ReferenceTargetNotPrimary {
        collection: String,
        field: String,
        target_collection: String,
        target_field: String,
    },
    ReferenceTargetMismatch {
        collection: String,
        field: String,
        expected_collection: String,
        actual_collection: String,
    },
    UniqueViolation {
        collection: String,
        field: String,
        existing: SchemaKey,
    },
    UnindexedField {
        collection: String,
        field: String,
    },
    IndexKeyTypeMismatch {
        collection: String,
        field: String,
        expected: &'static str,
        actual: SchemaValueKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaFlushError<E> {
    Store(SchemaStoreError),
    Backend(E),
}

#[derive(Debug, Clone)]
pub struct SchemaForwardInclude {
    pub document: Arc<SchemaValue>,
    pub reference_key: Option<SchemaKey>,
    pub referenced: Option<Arc<SchemaValue>>,
}

#[derive(Debug, Clone)]
pub struct SchemaReverseInclude {
    pub key: SchemaKey,
    pub document: Arc<SchemaValue>,
}

type IndexMap = HashMap<SchemaIndexKey, Vec<SchemaKey>>;
type ForwardReferenceMap = HashMap<SchemaKey, SchemaKey>;
type ReverseReferenceMap = HashMap<SchemaKey, Vec<SchemaKey>>;

pub struct SchemaStore<S = FastBuildHasher>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    collections: RwLock<HashMap<String, Arc<Collection<S>>>>,
    hash_builder: S,
    config: WriteBehindConfig,
}

struct Collection<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    schema: Arc<Schema>,
    inner: Arc<WriteBehindCache<SchemaKey, SchemaValue, S>>,
    mutation_locks: Box<[CachePadded<Mutex<()>>]>,
    mutation_lock_mask: usize,
    index_write_lock: Mutex<()>,
    indexed_fields: Vec<IndexedField>,
    indexes: Box<[RwLock<IndexMap>]>,
    reference_fields: Vec<ReferenceField>,
    forward_references: Box<[RwLock<ForwardReferenceMap>]>,
    reverse_references: Box<[RwLock<ReverseReferenceMap>]>,
}

#[derive(Debug, Clone)]
struct IndexedField {
    name: String,
    field_type: SchemaFieldType,
    unique: bool,
}

#[derive(Debug, Clone)]
struct ReferenceField {
    name: String,
    field_type: SchemaFieldType,
    reference: Reference,
}

impl SchemaKey {
    pub fn encoded(&self) -> Vec<u8> {
        match self {
            Self::String(value) => encode_key_bytes(0, value.as_bytes()),
            Self::Int32(value) => {
                let mut out = vec![1];
                out.extend_from_slice(&value.to_le_bytes());
                out
            }
            Self::Int64(value) => {
                let mut out = vec![2];
                out.extend_from_slice(&value.to_le_bytes());
                out
            }
            Self::Bytes(value) => encode_key_bytes(3, value),
        }
    }
}

impl fmt::Display for SchemaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(value) => write!(f, "{value}"),
            Self::Int32(value) => write!(f, "{value}"),
            Self::Int64(value) => write!(f, "{value}"),
            Self::Bytes(value) => write!(f, "{} bytes", value.len()),
        }
    }
}

impl fmt::Display for SchemaStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => write!(f, "invalid schema: {error}"),
            Self::SchemaValue(error) => write!(f, "invalid schema value: {error}"),
            Self::CollectionAlreadyRegistered { collection } => {
                write!(f, "collection `{collection}` is already registered")
            }
            Self::CollectionNotFound { collection } => {
                write!(f, "collection `{collection}` is not registered")
            }
            Self::DocumentAlreadyExists { collection, key } => {
                write!(
                    f,
                    "document `{key}` already exists in collection `{collection}`"
                )
            }
            Self::DocumentNotFound { collection, key } => {
                write!(
                    f,
                    "document `{key}` was not found in collection `{collection}`"
                )
            }
            Self::StoreFull { collection } => {
                write!(f, "collection `{collection}` store is full")
            }
            Self::MissingPrimaryField { collection } => {
                write!(f, "collection `{collection}` has no primary field")
            }
            Self::MissingPrimaryKey { collection, field } => {
                write!(
                    f,
                    "document for `{collection}` is missing primary key `{field}`"
                )
            }
            Self::PrimaryKeyTypeMismatch {
                collection,
                field,
                expected,
                actual,
            } => write!(
                f,
                "collection `{collection}` primary key `{field}` expected {expected}, got {actual}"
            ),
            Self::PrimaryKeyChanged {
                collection,
                expected,
                actual,
            } => write!(
                f,
                "collection `{collection}` update changed primary key from `{expected}` to `{actual}`"
            ),
            Self::ReferenceFieldNotFound { collection, field } => {
                write!(
                    f,
                    "field `{field}` is not a reference in collection `{collection}`"
                )
            }
            Self::ReferenceTargetCollectionNotFound {
                collection,
                field,
                target_collection,
            } => write!(
                f,
                "collection `{collection}` reference `{field}` targets unregistered collection `{target_collection}`"
            ),
            Self::ReferenceTargetNotPrimary {
                collection,
                field,
                target_collection,
                target_field,
            } => write!(
                f,
                "collection `{collection}` reference `{field}` targets `{target_collection}.{target_field}`, not the target primary key"
            ),
            Self::ReferenceTargetMismatch {
                collection,
                field,
                expected_collection,
                actual_collection,
            } => write!(
                f,
                "collection `{collection}` reference `{field}` targets `{actual_collection}`, expected `{expected_collection}`"
            ),
            Self::UniqueViolation {
                collection,
                field,
                existing,
            } => write!(
                f,
                "collection `{collection}` unique field `{field}` already belongs to `{existing}`"
            ),
            Self::UnindexedField { collection, field } => {
                write!(
                    f,
                    "field `{field}` is not indexed in collection `{collection}`"
                )
            }
            Self::IndexKeyTypeMismatch {
                collection,
                field,
                expected,
                actual,
            } => write!(
                f,
                "collection `{collection}` index `{field}` expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for SchemaStoreError {}

impl<E: fmt::Display> fmt::Display for SchemaFlushError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(error) => write!(f, "{error}"),
            Self::Backend(error) => write!(f, "flush backend error: {error}"),
        }
    }
}

impl SchemaStore {
    pub fn new() -> Self {
        Self::with_config(WriteBehindConfig::default())
    }

    pub fn with_capacity(expected_entries: usize) -> Self {
        Self::with_config(WriteBehindConfig {
            initial_capacity: expected_entries,
            ..WriteBehindConfig::default()
        })
    }

    pub fn with_config(config: WriteBehindConfig) -> Self {
        Self::with_hasher_and_config(FastBuildHasher::default(), config)
    }
}

impl Default for SchemaStore {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> SchemaStore<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        Self {
            collections: RwLock::new(HashMap::new()),
            hash_builder,
            config,
        }
    }

    pub fn register_collection(&self, schema: Schema) -> Result<(), SchemaStoreError> {
        schema.validate().map_err(SchemaStoreError::Schema)?;
        let name = schema.name.clone();
        let mut collections = write_lock(&self.collections);
        if collections.contains_key(&name) {
            return Err(SchemaStoreError::CollectionAlreadyRegistered { collection: name });
        }

        collections.insert(
            name,
            Arc::new(Collection::new(
                schema,
                self.hash_builder.clone(),
                self.config,
            )),
        );
        Ok(())
    }

    pub fn schema(&self, collection: &str) -> Result<Arc<Schema>, SchemaStoreError> {
        Ok(Arc::clone(&self.collection(collection)?.schema))
    }

    pub fn collection_names(&self) -> Vec<String> {
        let collections = read_lock(&self.collections);
        let mut names = collections.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub fn put(&self, collection: &str, document: SchemaValue) -> Result<u64, SchemaStoreError> {
        self.upsert(collection, document)
    }

    pub fn upsert(&self, collection: &str, document: SchemaValue) -> Result<u64, SchemaStoreError> {
        let handle = self.collection(collection)?;
        self.put_into(&handle, document, true, PutMode::Upsert)
    }

    pub fn create(&self, collection: &str, document: SchemaValue) -> Result<u64, SchemaStoreError> {
        let handle = self.collection(collection)?;
        self.put_into(&handle, document, true, PutMode::Create)
    }

    pub fn load_clean(
        &self,
        collection: &str,
        document: SchemaValue,
    ) -> Result<u64, SchemaStoreError> {
        let handle = self.collection(collection)?;
        self.put_into(&handle, document, false, PutMode::Upsert)
    }

    pub fn get(
        &self,
        collection: &str,
        key: &SchemaKey,
    ) -> Result<Option<Arc<SchemaValue>>, SchemaStoreError> {
        let handle = self.collection(collection)?;
        Ok(handle.inner.get(key))
    }

    pub fn read_with<R, F>(
        &self,
        collection: &str,
        key: &SchemaKey,
        f: F,
    ) -> Result<Option<R>, SchemaStoreError>
    where
        F: FnOnce(&SchemaValue) -> R,
    {
        let handle = self.collection(collection)?;
        Ok(handle.inner.read_with(key, f))
    }

    pub fn delete(&self, collection: &str, key: &SchemaKey) -> Result<u64, SchemaStoreError> {
        let handle = self.collection(collection)?;
        let _guard = handle.lock_key(&self.hash_builder, key);
        let _index_guard = handle.lock_indexes();

        if let Some(old) = handle.inner.get(key) {
            handle.remove_from_indexes(key, old.as_ref());
        }

        handle
            .inner
            .try_delete(key.clone())
            .map_err(|_| SchemaStoreError::StoreFull {
                collection: collection.to_string(),
            })
    }

    pub fn update<F>(
        &self,
        collection: &str,
        key: &SchemaKey,
        update: F,
    ) -> Result<u64, SchemaStoreError>
    where
        F: FnOnce(&mut SchemaValue),
    {
        self.try_update(collection, key, update)?.ok_or_else(|| {
            SchemaStoreError::DocumentNotFound {
                collection: collection.to_string(),
                key: key.clone(),
            }
        })
    }

    pub fn try_update<F>(
        &self,
        collection: &str,
        key: &SchemaKey,
        update: F,
    ) -> Result<Option<u64>, SchemaStoreError>
    where
        F: FnOnce(&mut SchemaValue),
    {
        let handle = self.collection(collection)?;
        let _guard = handle.lock_key(&self.hash_builder, key);
        let _index_guard = handle.lock_indexes();
        let Some(old) = handle.inner.get(key) else {
            return Ok(None);
        };
        let mut next = old.as_ref().clone();
        update(&mut next);
        validate_document(&handle.schema, &next).map_err(SchemaStoreError::SchemaValue)?;
        let next_key = extract_primary_key(&handle.schema, &next)?;
        if next_key != *key {
            return Err(SchemaStoreError::PrimaryKeyChanged {
                collection: collection.to_string(),
                expected: key.clone(),
                actual: next_key,
            });
        }
        handle.validate_unique_constraints(key, Some(old.as_ref()), &next)?;
        let version =
            handle
                .inner
                .try_put(key.clone(), next)
                .map_err(|_| SchemaStoreError::StoreFull {
                    collection: collection.to_string(),
                })?;
        let stored = handle.inner.get(key).expect("stored document");
        handle.update_indexes_on_put(key, Some(old.as_ref()), stored.as_ref())?;
        Ok(Some(version))
    }

    pub fn lookup(
        &self,
        collection: &str,
        field: &str,
        value: &SchemaValue,
    ) -> Result<Vec<SchemaKey>, SchemaStoreError> {
        let handle = self.collection(collection)?;
        let Some((idx, indexed_field)) = handle.indexed_field(field) else {
            return Err(SchemaStoreError::UnindexedField {
                collection: collection.to_string(),
                field: field.to_string(),
            });
        };
        let key = index_key_from_value(
            collection,
            &indexed_field.name,
            &indexed_field.field_type,
            value,
        )?;
        let _index_guard = handle.lock_indexes();
        let index = read_lock(&handle.indexes[idx]);
        Ok(index.get(&key).cloned().unwrap_or_default())
    }

    pub fn include_one(
        &self,
        collection: &str,
        key: &SchemaKey,
        reference_field: &str,
    ) -> Result<Option<SchemaForwardInclude>, SchemaStoreError> {
        let handle = self.collection(collection)?;
        let Some((idx, field)) = handle.reference_field(reference_field) else {
            return Err(SchemaStoreError::ReferenceFieldNotFound {
                collection: collection.to_string(),
                field: reference_field.to_string(),
            });
        };
        let target = self.reference_target(collection, field)?;
        let document = match handle.inner.get(key) {
            Some(document) => document,
            None => return Ok(None),
        };
        let _index_guard = handle.lock_indexes();
        let forward = read_lock(&handle.forward_references[idx]);
        let Some(reference_key) = forward.get(key).cloned() else {
            return Ok(Some(SchemaForwardInclude {
                document,
                reference_key: None,
                referenced: None,
            }));
        };
        let referenced = target.inner.get(&reference_key);
        Ok(Some(SchemaForwardInclude {
            document,
            reference_key: Some(reference_key),
            referenced,
        }))
    }

    pub fn include_reverse(
        &self,
        target_collection: &str,
        target_key: &SchemaKey,
        source_collection: &str,
        reference_field: &str,
    ) -> Result<Vec<SchemaReverseInclude>, SchemaStoreError> {
        let source = self.collection(source_collection)?;
        let Some((idx, field)) = source.reference_field(reference_field) else {
            return Err(SchemaStoreError::ReferenceFieldNotFound {
                collection: source_collection.to_string(),
                field: reference_field.to_string(),
            });
        };
        if field.reference.collection != target_collection {
            return Err(SchemaStoreError::ReferenceTargetMismatch {
                collection: source_collection.to_string(),
                field: reference_field.to_string(),
                expected_collection: target_collection.to_string(),
                actual_collection: field.reference.collection.clone(),
            });
        }
        let _ = self.reference_target(source_collection, field)?;
        let _index_guard = source.lock_indexes();
        let reverse = read_lock(&source.reverse_references[idx]);
        let keys = reverse.get(target_key).cloned().unwrap_or_default();
        drop(reverse);
        drop(_index_guard);

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(document) = source.inner.get(&key) {
                out.push(SchemaReverseInclude { key, document });
            }
        }
        Ok(out)
    }

    pub fn flush_collection_idle<B>(
        &self,
        collection: &str,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, SchemaFlushError<B::Error>>
    where
        B: FlushBackend<SchemaKey, SchemaValue>,
    {
        let handle = self
            .collection(collection)
            .map_err(SchemaFlushError::Store)?;
        handle
            .inner
            .flush_idle(backend, idle_ticks, max_records)
            .map_err(SchemaFlushError::Backend)
    }

    pub fn try_flush_collection_idle<B>(
        &self,
        collection: &str,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, SchemaFlushError<B::Error>>
    where
        B: FlushBackend<SchemaKey, SchemaValue>,
    {
        self.flush_collection_idle(collection, backend, idle_ticks, max_records)
    }

    pub fn stats(&self, collection: &str) -> Result<WriteBehindStats, SchemaStoreError> {
        Ok(self.collection(collection)?.inner.stats())
    }

    fn put_into(
        &self,
        handle: &Arc<Collection<S>>,
        document: SchemaValue,
        dirty: bool,
        mode: PutMode,
    ) -> Result<u64, SchemaStoreError> {
        validate_document(&handle.schema, &document).map_err(SchemaStoreError::SchemaValue)?;
        let key = extract_primary_key(&handle.schema, &document)?;
        let _guard = handle.lock_key(&self.hash_builder, &key);
        let _index_guard = handle.lock_indexes();
        let old = handle.inner.get(&key);
        if mode == PutMode::Create && old.is_some() {
            return Err(SchemaStoreError::DocumentAlreadyExists {
                collection: handle.schema.name.clone(),
                key,
            });
        }
        handle.validate_unique_constraints(&key, old.as_deref(), &document)?;

        let version = if dirty {
            handle.inner.try_put(key.clone(), document)
        } else {
            handle.inner.try_load_clean(key.clone(), document)
        }
        .map_err(|_| SchemaStoreError::StoreFull {
            collection: handle.schema.name.clone(),
        })?;
        let stored = handle.inner.get(&key).expect("stored document");
        handle.update_indexes_on_put(&key, old.as_deref(), stored.as_ref())?;
        Ok(version)
    }

    fn collection(&self, collection: &str) -> Result<Arc<Collection<S>>, SchemaStoreError> {
        let collections = read_lock(&self.collections);
        collections
            .get(collection)
            .cloned()
            .ok_or_else(|| SchemaStoreError::CollectionNotFound {
                collection: collection.to_string(),
            })
    }

    fn reference_target(
        &self,
        collection: &str,
        field: &ReferenceField,
    ) -> Result<Arc<Collection<S>>, SchemaStoreError> {
        let target = self.collection(&field.reference.collection).map_err(|_| {
            SchemaStoreError::ReferenceTargetCollectionNotFound {
                collection: collection.to_string(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
            }
        })?;
        let primary =
            target
                .schema
                .primary_field()
                .ok_or_else(|| SchemaStoreError::MissingPrimaryField {
                    collection: target.schema.name.clone(),
                })?;
        if primary.name != field.reference.field {
            return Err(SchemaStoreError::ReferenceTargetNotPrimary {
                collection: collection.to_string(),
                field: field.name.clone(),
                target_collection: field.reference.collection.clone(),
                target_field: field.reference.field.clone(),
            });
        }
        Ok(target)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PutMode {
    Create,
    Upsert,
}

impl<S> Collection<S>
where
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    fn new(schema: Schema, hash_builder: S, config: WriteBehindConfig) -> Self {
        let indexed_fields = schema
            .fields
            .iter()
            .filter(|field| field.indexed && is_exact_index_type(&field.field_type))
            .map(|field| IndexedField {
                name: field.name.clone(),
                field_type: field.field_type.clone(),
                unique: field.unique,
            })
            .collect::<Vec<_>>();
        let reference_fields = schema
            .fields
            .iter()
            .filter_map(|field| {
                field.reference.clone().map(|reference| ReferenceField {
                    name: field.name.clone(),
                    field_type: field.field_type.clone(),
                    reference,
                })
            })
            .collect::<Vec<_>>();
        let indexes = (0..indexed_fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>();
        let forward_references = (0..reference_fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>();
        let reverse_references = (0..reference_fields.len())
            .map(|_| RwLock::new(HashMap::new()))
            .collect::<Vec<_>>();
        let lock_count = DEFAULT_MUTATION_LOCKS.next_power_of_two();
        let mutation_locks = (0..lock_count)
            .map(|_| CachePadded::new(Mutex::new(())))
            .collect::<Vec<_>>();

        Self {
            schema: Arc::new(schema),
            inner: Arc::new(WriteBehindCache::with_hasher_and_config(
                hash_builder,
                config,
            )),
            mutation_locks: mutation_locks.into_boxed_slice(),
            mutation_lock_mask: lock_count - 1,
            index_write_lock: Mutex::new(()),
            indexed_fields,
            indexes: indexes.into_boxed_slice(),
            reference_fields,
            forward_references: forward_references.into_boxed_slice(),
            reverse_references: reverse_references.into_boxed_slice(),
        }
    }

    fn lock_key<'a>(&'a self, hash_builder: &S, key: &SchemaKey) -> MutexGuard<'a, ()> {
        let idx = (hash_builder.hash_one(key) as usize) & self.mutation_lock_mask;
        self.mutation_locks[idx].lock()
    }

    fn lock_indexes(&self) -> MutexGuard<'_, ()> {
        self.index_write_lock.lock()
    }

    fn indexed_field(&self, name: &str) -> Option<(usize, &IndexedField)> {
        self.indexed_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == name)
    }

    fn reference_field(&self, name: &str) -> Option<(usize, &ReferenceField)> {
        self.reference_fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == name)
    }

    fn update_indexes_on_put(
        &self,
        key: &SchemaKey,
        old: Option<&SchemaValue>,
        new: &SchemaValue,
    ) -> Result<(), SchemaStoreError> {
        if let Some(old) = old {
            self.remove_from_indexes(key, old);
            self.remove_from_references(key, old);
        }

        for (idx, field) in self.indexed_fields.iter().enumerate() {
            let Some(value) = new.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            let index_key =
                index_key_from_value(&self.schema.name, &field.name, &field.field_type, value)?;
            let mut index = write_lock(&self.indexes[idx]);
            let keys = index.entry(index_key).or_default();
            if !keys.iter().any(|existing| existing == key) {
                keys.push(key.clone());
            }
        }

        self.insert_references(key, new)?;

        Ok(())
    }

    fn validate_unique_constraints(
        &self,
        key: &SchemaKey,
        old: Option<&SchemaValue>,
        new: &SchemaValue,
    ) -> Result<(), SchemaStoreError> {
        for (idx, field) in self.indexed_fields.iter().enumerate() {
            if !field.unique {
                continue;
            }

            let Some(value) = new.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }

            let index_key =
                index_key_from_value(&self.schema.name, &field.name, &field.field_type, value)?;
            let index = read_lock(&self.indexes[idx]);
            let Some(keys) = index.get(&index_key) else {
                continue;
            };

            for existing in keys {
                if existing != key {
                    return Err(SchemaStoreError::UniqueViolation {
                        collection: self.schema.name.clone(),
                        field: field.name.clone(),
                        existing: existing.clone(),
                    });
                }
            }

            if old.is_none() && keys.iter().any(|existing| existing == key) {
                return Err(SchemaStoreError::UniqueViolation {
                    collection: self.schema.name.clone(),
                    field: field.name.clone(),
                    existing: key.clone(),
                });
            }
        }

        Ok(())
    }

    fn remove_from_indexes(&self, key: &SchemaKey, old: &SchemaValue) {
        for (idx, field) in self.indexed_fields.iter().enumerate() {
            let Some(value) = old.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            let Ok(index_key) =
                index_key_from_value(&self.schema.name, &field.name, &field.field_type, value)
            else {
                continue;
            };
            let mut index = write_lock(&self.indexes[idx]);
            if let Some(keys) = index.get_mut(&index_key) {
                keys.retain(|existing| existing != key);
                if keys.is_empty() {
                    index.remove(&index_key);
                }
            }
        }
    }

    fn insert_references(
        &self,
        source_key: &SchemaKey,
        document: &SchemaValue,
    ) -> Result<(), SchemaStoreError> {
        for (idx, field) in self.reference_fields.iter().enumerate() {
            let Some(value) = document.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            let target_key =
                primary_key_from_value(&self.schema.name, &field.name, &field.field_type, value)?;

            let mut forward = write_lock(&self.forward_references[idx]);
            forward.insert(source_key.clone(), target_key.clone());
            drop(forward);

            let mut reverse = write_lock(&self.reverse_references[idx]);
            let sources = reverse.entry(target_key).or_default();
            if !sources.iter().any(|existing| existing == source_key) {
                sources.push(source_key.clone());
            }
        }

        Ok(())
    }

    fn remove_from_references(&self, source_key: &SchemaKey, document: &SchemaValue) {
        for (idx, field) in self.reference_fields.iter().enumerate() {
            let Some(value) = document.field(&field.name) else {
                continue;
            };
            if matches!(value, SchemaValue::Null) {
                continue;
            }
            let Ok(target_key) =
                primary_key_from_value(&self.schema.name, &field.name, &field.field_type, value)
            else {
                continue;
            };

            let mut forward = write_lock(&self.forward_references[idx]);
            forward.remove(source_key);
            drop(forward);

            let mut reverse = write_lock(&self.reverse_references[idx]);
            if let Some(sources) = reverse.get_mut(&target_key) {
                sources.retain(|existing| existing != source_key);
                if sources.is_empty() {
                    reverse.remove(&target_key);
                }
            }
        }
    }
}

pub fn extract_primary_key(
    schema: &Schema,
    document: &SchemaValue,
) -> Result<SchemaKey, SchemaStoreError> {
    let primary = schema
        .primary_field()
        .ok_or_else(|| SchemaStoreError::MissingPrimaryField {
            collection: schema.name.clone(),
        })?;
    let value =
        document
            .field(&primary.name)
            .ok_or_else(|| SchemaStoreError::MissingPrimaryKey {
                collection: schema.name.clone(),
                field: primary.name.clone(),
            })?;

    primary_key_from_value(&schema.name, &primary.name, &primary.field_type, value)
}

fn primary_key_from_value(
    collection: &str,
    field: &str,
    field_type: &SchemaFieldType,
    value: &SchemaValue,
) -> Result<SchemaKey, SchemaStoreError> {
    match (field_type, value) {
        (SchemaFieldType::String, SchemaValue::String(value)) => {
            Ok(SchemaKey::String(value.clone()))
        }
        (SchemaFieldType::Int32, SchemaValue::Int32(value)) => Ok(SchemaKey::Int32(*value)),
        (SchemaFieldType::Int32, SchemaValue::Int64(value)) => {
            i32::try_from(*value).map(SchemaKey::Int32).map_err(|_| {
                primary_key_mismatch(collection, field, field_type, SchemaValueKind::Int64)
            })
        }
        (SchemaFieldType::Int64, SchemaValue::Int32(value)) => {
            Ok(SchemaKey::Int64(i64::from(*value)))
        }
        (SchemaFieldType::Int64, SchemaValue::Int64(value)) => Ok(SchemaKey::Int64(*value)),
        (SchemaFieldType::Bytes, SchemaValue::Bytes(value)) => Ok(SchemaKey::Bytes(value.clone())),
        _ => Err(primary_key_mismatch(
            collection,
            field,
            field_type,
            value.kind(),
        )),
    }
}

fn index_key_from_value(
    collection: &str,
    field: &str,
    field_type: &SchemaFieldType,
    value: &SchemaValue,
) -> Result<SchemaIndexKey, SchemaStoreError> {
    match (field_type, value) {
        (SchemaFieldType::String, SchemaValue::String(value)) => {
            Ok(SchemaIndexKey::String(value.clone()))
        }
        (SchemaFieldType::Int32, SchemaValue::Int32(value)) => Ok(SchemaIndexKey::Int32(*value)),
        (SchemaFieldType::Int32, SchemaValue::Int64(value)) => i32::try_from(*value)
            .map(SchemaIndexKey::Int32)
            .map_err(|_| index_key_mismatch(collection, field, field_type, SchemaValueKind::Int64)),
        (SchemaFieldType::Int64, SchemaValue::Int32(value)) => {
            Ok(SchemaIndexKey::Int64(i64::from(*value)))
        }
        (SchemaFieldType::Int64, SchemaValue::Int64(value)) => Ok(SchemaIndexKey::Int64(*value)),
        (SchemaFieldType::Float, SchemaValue::Int32(value)) => {
            Ok(SchemaIndexKey::Float(f64::from(*value).to_bits()))
        }
        (SchemaFieldType::Float, SchemaValue::Int64(value)) => {
            Ok(SchemaIndexKey::Float((*value as f64).to_bits()))
        }
        (SchemaFieldType::Float, SchemaValue::Float(value)) if value.is_finite() => {
            Ok(SchemaIndexKey::Float(value.to_bits()))
        }
        (SchemaFieldType::Bool, SchemaValue::Bool(value)) => Ok(SchemaIndexKey::Bool(*value)),
        (SchemaFieldType::Bytes, SchemaValue::Bytes(value)) => {
            Ok(SchemaIndexKey::Bytes(value.clone()))
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

fn primary_key_mismatch(
    collection: &str,
    field: &str,
    expected: &SchemaFieldType,
    actual: SchemaValueKind,
) -> SchemaStoreError {
    SchemaStoreError::PrimaryKeyTypeMismatch {
        collection: collection.to_string(),
        field: field.to_string(),
        expected: schema_type_name(expected),
        actual,
    }
}

fn index_key_mismatch(
    collection: &str,
    field: &str,
    expected: &SchemaFieldType,
    actual: SchemaValueKind,
) -> SchemaStoreError {
    SchemaStoreError::IndexKeyTypeMismatch {
        collection: collection.to_string(),
        field: field.to_string(),
        expected: schema_type_name(expected),
        actual,
    }
}

fn encode_key_bytes(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + bytes.len());
    out.push(tag);
    out.extend_from_slice(bytes);
    out
}

fn read_lock<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
}

fn write_lock<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfs_core::{FlushRecord, Operation};
    use std::sync::Barrier;
    use std::thread;

    fn primary_id() -> mfs_store::schema::SchemaField {
        let mut field = mfs_store::schema::SchemaField::new("id", SchemaFieldType::String);
        field.primary = true;
        field.indexed = true;
        field.unique = true;
        field
    }

    fn user_schema() -> Schema {
        let mut email = mfs_store::schema::SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let age = mfs_store::schema::SchemaField {
            optional: true,
            indexed: true,
            ..mfs_store::schema::SchemaField::new("age", SchemaFieldType::Int32)
        };

        Schema::new("users", vec![primary_id(), email, age])
    }

    fn user(id: &str, email: &str, age: i32) -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("email".to_string(), SchemaValue::String(email.to_string())),
            ("age".to_string(), SchemaValue::Int32(age)),
        ])
    }

    fn company_schema() -> Schema {
        let mut id = mfs_store::schema::SchemaField::new("id", SchemaFieldType::String);
        id.primary = true;
        id.indexed = true;
        id.unique = true;
        let name = mfs_store::schema::SchemaField::new("name", SchemaFieldType::String);
        Schema::new("companies", vec![id, name])
    }

    fn user_company_schema() -> Schema {
        let mut email = mfs_store::schema::SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;
        let mut company_id =
            mfs_store::schema::SchemaField::new("company_id", SchemaFieldType::String);
        company_id.optional = true;
        company_id.reference = Some(Reference::new("companies", "id"));
        Schema::new("users", vec![primary_id(), email, company_id])
    }

    fn company(id: &str, name: &str) -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("name".to_string(), SchemaValue::String(name.to_string())),
        ])
    }

    fn user_with_company(id: &str, email: &str, company_id: Option<&str>) -> SchemaValue {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("id".to_string(), SchemaValue::String(id.to_string()));
        fields.insert("email".to_string(), SchemaValue::String(email.to_string()));
        if let Some(company_id) = company_id {
            fields.insert(
                "company_id".to_string(),
                SchemaValue::String(company_id.to_string()),
            );
        }
        SchemaValue::Object(fields)
    }

    #[test]
    fn register_and_list_collections() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();

        assert_eq!(store.collection_names(), vec!["users".to_string()]);
        assert!(matches!(
            store.register_collection(user_schema()),
            Err(SchemaStoreError::CollectionAlreadyRegistered { collection }) if collection == "users"
        ));
    }

    #[test]
    fn put_get_delete_round_trip() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let doc = user("u1", "ada@example.com", 37);
        let key = SchemaKey::String("u1".to_string());

        assert_eq!(store.put("users", doc.clone()).unwrap(), 1);
        assert_eq!(*store.get("users", &key).unwrap().unwrap(), doc);

        store.delete("users", &key).unwrap();
        assert!(store.get("users", &key).unwrap().is_none());
    }

    #[test]
    fn put_replaces_existing_document_and_indexes() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());

        store
            .put("users", user("u1", "old@example.com", 20))
            .unwrap();
        store
            .put("users", user("u1", "new@example.com", 21))
            .unwrap();

        assert_eq!(
            store
                .lookup(
                    "users",
                    "email",
                    &SchemaValue::String("old@example.com".to_string())
                )
                .unwrap(),
            Vec::<SchemaKey>::new()
        );
        assert_eq!(
            store
                .lookup(
                    "users",
                    "email",
                    &SchemaValue::String("new@example.com".to_string())
                )
                .unwrap(),
            vec![key]
        );
    }

    #[test]
    fn create_rejects_existing_document() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();

        assert!(matches!(
            store.create("users", user("u1", "other@example.com", 38)),
            Err(SchemaStoreError::DocumentAlreadyExists { collection, key })
                if collection == "users" && key == SchemaKey::String("u1".to_string())
        ));
    }

    #[test]
    fn upsert_replaces_existing_document() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();
        store
            .upsert("users", user("u1", "ada-updated@example.com", 38))
            .unwrap();

        let email = store
            .read_with("users", &key, |doc| match doc.field("email") {
                Some(SchemaValue::String(value)) => value.clone(),
                _ => String::new(),
            })
            .unwrap();
        assert_eq!(email, Some("ada-updated@example.com".to_string()));
    }

    #[test]
    fn update_modifies_existing_document_and_indexes() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());

        store
            .create("users", user("u1", "old@example.com", 37))
            .unwrap();
        store
            .update("users", &key, |doc| {
                let SchemaValue::Object(fields) = doc else {
                    panic!("document should be object");
                };
                fields.insert(
                    "email".to_string(),
                    SchemaValue::String("new@example.com".to_string()),
                );
                fields.insert("age".to_string(), SchemaValue::Int32(38));
            })
            .unwrap();

        assert_eq!(
            store
                .lookup(
                    "users",
                    "email",
                    &SchemaValue::String("old@example.com".to_string())
                )
                .unwrap(),
            Vec::<SchemaKey>::new()
        );
        assert_eq!(
            store
                .lookup(
                    "users",
                    "email",
                    &SchemaValue::String("new@example.com".to_string())
                )
                .unwrap(),
            vec![key]
        );
    }

    #[test]
    fn update_missing_document_returns_error_and_try_update_returns_none() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("missing".to_string());

        assert_eq!(store.try_update("users", &key, |_| {}).unwrap(), None);
        assert!(matches!(
            store.update("users", &key, |_| {}),
            Err(SchemaStoreError::DocumentNotFound { collection, key: missing_key })
                if collection == "users" && missing_key == key
        ));
    }

    #[test]
    fn update_rejects_invalid_document_without_changing_existing_value() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();
        assert!(matches!(
            store.update("users", &key, |doc| {
                let SchemaValue::Object(fields) = doc else {
                    panic!("document should be object");
                };
                fields.remove("email");
            }),
            Err(SchemaStoreError::SchemaValue(SchemaValueError::MissingRequiredField { name }))
                if name == "email"
        ));

        let email = store
            .read_with("users", &key, |doc| match doc.field("email") {
                Some(SchemaValue::String(value)) => value.clone(),
                _ => String::new(),
            })
            .unwrap();
        assert_eq!(email, Some("ada@example.com".to_string()));
    }

    #[test]
    fn update_rejects_primary_key_changes() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();

        assert!(matches!(
            store.update("users", &key, |doc| {
                let SchemaValue::Object(fields) = doc else {
                    panic!("document should be object");
                };
                fields.insert("id".to_string(), SchemaValue::String("u2".to_string()));
            }),
            Err(SchemaStoreError::PrimaryKeyChanged { collection, expected, actual })
                if collection == "users"
                    && expected == SchemaKey::String("u1".to_string())
                    && actual == SchemaKey::String("u2".to_string())
        ));
    }

    #[test]
    fn put_rejects_invalid_document() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let doc = SchemaValue::object([("id".to_string(), SchemaValue::String("u1".to_string()))]);

        assert!(matches!(
            store.put("users", doc),
            Err(SchemaStoreError::SchemaValue(SchemaValueError::MissingRequiredField { name })) if name == "email"
        ));
    }

    #[test]
    fn read_with_reads_without_returning_arc() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());
        store
            .put("users", user("u1", "ada@example.com", 37))
            .unwrap();

        let email = store
            .read_with("users", &key, |doc| match doc.field("email") {
                Some(SchemaValue::String(value)) => value.clone(),
                _ => String::new(),
            })
            .unwrap();

        assert_eq!(email, Some("ada@example.com".to_string()));
    }

    #[test]
    fn lookup_returns_matching_primary_keys() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        store
            .put("users", user("u1", "ada@example.com", 37))
            .unwrap();
        store
            .put("users", user("u2", "grace@example.com", 37))
            .unwrap();
        store
            .put("users", user("u3", "linus@example.com", 54))
            .unwrap();

        let mut keys = store
            .lookup("users", "age", &SchemaValue::Int32(37))
            .unwrap();
        keys.sort_by_key(|key| key.encoded());

        assert_eq!(
            keys,
            vec![
                SchemaKey::String("u1".to_string()),
                SchemaKey::String("u2".to_string())
            ]
        );
    }

    #[test]
    fn unique_fields_reject_duplicate_values() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();

        assert!(matches!(
            store.create("users", user("u2", "ada@example.com", 38)),
            Err(SchemaStoreError::UniqueViolation { collection, field, existing })
                if collection == "users"
                    && field == "email"
                    && existing == SchemaKey::String("u1".to_string())
        ));
    }

    #[test]
    fn update_rejects_unique_value_collision() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u2".to_string());

        store
            .create("users", user("u1", "ada@example.com", 37))
            .unwrap();
        store
            .create("users", user("u2", "grace@example.com", 38))
            .unwrap();

        assert!(matches!(
            store.update("users", &key, |doc| {
                let SchemaValue::Object(fields) = doc else {
                    panic!("document should be object");
                };
                fields.insert(
                    "email".to_string(),
                    SchemaValue::String("ada@example.com".to_string()),
                );
            }),
            Err(SchemaStoreError::UniqueViolation { collection, field, existing })
                if collection == "users"
                    && field == "email"
                    && existing == SchemaKey::String("u1".to_string())
        ));
    }

    #[test]
    fn float_index_lookup_works() {
        let mut score = mfs_store::schema::SchemaField::new("score", SchemaFieldType::Float);
        score.indexed = true;
        let schema = Schema::new("scores", vec![primary_id(), score]);
        let store = SchemaStore::new();
        store.register_collection(schema).unwrap();

        store
            .create(
                "scores",
                SchemaValue::object([
                    ("id".to_string(), SchemaValue::String("s1".to_string())),
                    ("score".to_string(), SchemaValue::Float(1.5)),
                ]),
            )
            .unwrap();
        store
            .create(
                "scores",
                SchemaValue::object([
                    ("id".to_string(), SchemaValue::String("s2".to_string())),
                    ("score".to_string(), SchemaValue::Int32(2)),
                ]),
            )
            .unwrap();

        assert_eq!(
            store
                .lookup("scores", "score", &SchemaValue::Float(1.5))
                .unwrap(),
            vec![SchemaKey::String("s1".to_string())]
        );
        assert_eq!(
            store
                .lookup("scores", "score", &SchemaValue::Float(2.0))
                .unwrap(),
            vec![SchemaKey::String("s2".to_string())]
        );
    }

    #[test]
    fn lookup_removed_on_delete() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        let key = SchemaKey::String("u1".to_string());
        store
            .put("users", user("u1", "ada@example.com", 37))
            .unwrap();
        store.delete("users", &key).unwrap();

        assert_eq!(
            store
                .lookup("users", "age", &SchemaValue::Int32(37))
                .unwrap(),
            Vec::<SchemaKey>::new()
        );
    }

    #[test]
    fn forward_include_resolves_reference() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();
        let company_key = SchemaKey::String("c1".to_string());
        let user_key = SchemaKey::String("u1".to_string());
        let company_doc = company("c1", "Acme");
        let user_doc = user_with_company("u1", "ada@example.com", Some("c1"));

        store.create("companies", company_doc.clone()).unwrap();
        store.create("users", user_doc.clone()).unwrap();

        let included = store
            .include_one("users", &user_key, "company_id")
            .unwrap()
            .unwrap();

        assert_eq!(*included.document, user_doc);
        assert_eq!(included.reference_key, Some(company_key));
        assert_eq!(*included.referenced.unwrap(), company_doc);
    }

    #[test]
    fn forward_include_missing_target_returns_none() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();
        let user_key = SchemaKey::String("u1".to_string());
        store
            .create(
                "users",
                user_with_company("u1", "ada@example.com", Some("missing")),
            )
            .unwrap();

        let included = store
            .include_one("users", &user_key, "company_id")
            .unwrap()
            .unwrap();

        assert_eq!(
            included.reference_key,
            Some(SchemaKey::String("missing".to_string()))
        );
        assert!(included.referenced.is_none());
    }

    #[test]
    fn forward_include_missing_optional_reference_returns_empty_include() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();
        let user_key = SchemaKey::String("u1".to_string());
        store
            .create("users", user_with_company("u1", "ada@example.com", None))
            .unwrap();

        let included = store
            .include_one("users", &user_key, "company_id")
            .unwrap()
            .unwrap();

        assert!(included.reference_key.is_none());
        assert!(included.referenced.is_none());
    }

    #[test]
    fn reverse_include_returns_referring_documents() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();
        let company_key = SchemaKey::String("c1".to_string());
        store.create("companies", company("c1", "Acme")).unwrap();
        store.create("companies", company("c2", "Globex")).unwrap();
        store
            .create(
                "users",
                user_with_company("u1", "ada@example.com", Some("c1")),
            )
            .unwrap();
        store
            .create(
                "users",
                user_with_company("u2", "grace@example.com", Some("c1")),
            )
            .unwrap();
        store
            .create(
                "users",
                user_with_company("u3", "linus@example.com", Some("c2")),
            )
            .unwrap();

        let mut included = store
            .include_reverse("companies", &company_key, "users", "company_id")
            .unwrap();
        included.sort_by_key(|entry| entry.key.encoded());

        assert_eq!(
            included
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<Vec<_>>(),
            vec![
                SchemaKey::String("u1".to_string()),
                SchemaKey::String("u2".to_string())
            ]
        );
    }

    #[test]
    fn reverse_include_updates_on_reassign_and_delete() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();
        let user_key = SchemaKey::String("u1".to_string());
        let c1 = SchemaKey::String("c1".to_string());
        let c2 = SchemaKey::String("c2".to_string());
        store.create("companies", company("c1", "Acme")).unwrap();
        store.create("companies", company("c2", "Globex")).unwrap();
        store
            .create(
                "users",
                user_with_company("u1", "ada@example.com", Some("c1")),
            )
            .unwrap();

        assert_eq!(
            store
                .include_reverse("companies", &c1, "users", "company_id")
                .unwrap()
                .len(),
            1
        );

        store
            .update("users", &user_key, |doc| {
                let SchemaValue::Object(fields) = doc else {
                    panic!("document should be object");
                };
                fields.insert(
                    "company_id".to_string(),
                    SchemaValue::String("c2".to_string()),
                );
            })
            .unwrap();

        assert!(
            store
                .include_reverse("companies", &c1, "users", "company_id")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .include_reverse("companies", &c2, "users", "company_id")
                .unwrap()
                .len(),
            1
        );

        store.delete("users", &user_key).unwrap();
        assert!(
            store
                .include_reverse("companies", &c2, "users", "company_id")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn include_rejects_unknown_reference_field() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();

        assert!(matches!(
            store.include_one("users", &SchemaKey::String("u1".to_string()), "email"),
            Err(SchemaStoreError::ReferenceFieldNotFound { collection, field })
                if collection == "users" && field == "email"
        ));
    }

    #[test]
    fn reverse_include_rejects_wrong_target_collection() {
        let store = SchemaStore::new();
        store.register_collection(company_schema()).unwrap();
        store.register_collection(user_company_schema()).unwrap();

        assert!(matches!(
            store.include_reverse(
                "wrong",
                &SchemaKey::String("c1".to_string()),
                "users",
                "company_id"
            ),
            Err(SchemaStoreError::ReferenceTargetMismatch {
                collection,
                field,
                expected_collection,
                actual_collection,
            }) if collection == "users"
                && field == "company_id"
                && expected_collection == "wrong"
                && actual_collection == "companies"
        ));
    }

    #[test]
    fn lookup_rejects_unindexed_field() {
        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();

        assert!(matches!(
            store.lookup("users", "missing", &SchemaValue::String("x".to_string())),
            Err(SchemaStoreError::UnindexedField { collection, field })
                if collection == "users" && field == "missing"
        ));
    }

    #[test]
    fn load_clean_does_not_flush() {
        #[derive(Default)]
        struct CollectBackend {
            records: Vec<FlushRecord<SchemaKey, SchemaValue>>,
        }

        impl FlushBackend<SchemaKey, SchemaValue> for CollectBackend {
            type Error = SchemaStoreError;

            fn flush(
                &mut self,
                records: &[FlushRecord<SchemaKey, SchemaValue>],
            ) -> Result<(), Self::Error> {
                self.records.extend(records.iter().cloned());
                Ok(())
            }
        }

        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        store
            .load_clean("users", user("u1", "ada@example.com", 37))
            .unwrap();

        let mut backend = CollectBackend::default();
        assert_eq!(
            store
                .flush_collection_idle("users", &mut backend, 0, 16)
                .unwrap(),
            0
        );
        assert!(backend.records.is_empty());
    }

    #[test]
    fn flush_missing_collection_returns_error_without_panic() {
        #[derive(Default)]
        struct CollectBackend;

        impl FlushBackend<SchemaKey, SchemaValue> for CollectBackend {
            type Error = SchemaStoreError;

            fn flush(
                &mut self,
                _records: &[FlushRecord<SchemaKey, SchemaValue>],
            ) -> Result<(), Self::Error> {
                Ok(())
            }
        }

        let store = SchemaStore::new();
        let mut backend = CollectBackend;

        assert!(matches!(
            store.flush_collection_idle("missing", &mut backend, 0, 16),
            Err(SchemaFlushError::Store(SchemaStoreError::CollectionNotFound { collection }))
                if collection == "missing"
        ));
        assert!(matches!(
            store.try_flush_collection_idle("missing", &mut backend, 0, 16),
            Err(SchemaFlushError::Store(SchemaStoreError::CollectionNotFound { collection }))
                if collection == "missing"
        ));
    }

    #[test]
    fn flush_collection_idle_emits_dirty_records() {
        #[derive(Default)]
        struct CollectBackend {
            records: Vec<FlushRecord<SchemaKey, SchemaValue>>,
        }

        impl FlushBackend<SchemaKey, SchemaValue> for CollectBackend {
            type Error = SchemaStoreError;

            fn flush(
                &mut self,
                records: &[FlushRecord<SchemaKey, SchemaValue>],
            ) -> Result<(), Self::Error> {
                self.records.extend(records.iter().cloned());
                Ok(())
            }
        }

        let store = SchemaStore::new();
        store.register_collection(user_schema()).unwrap();
        store
            .put("users", user("u1", "ada@example.com", 37))
            .unwrap();
        let mut backend = CollectBackend::default();

        assert_eq!(
            store
                .flush_collection_idle("users", &mut backend, 0, 16)
                .unwrap(),
            1
        );
        assert_eq!(backend.records.len(), 1);
        assert_eq!(backend.records[0].op, Operation::Put);
    }

    #[test]
    fn threaded_put_get_uses_hot_path_without_sql() {
        let store = Arc::new(SchemaStore::new());
        store.register_collection(user_schema()).unwrap();
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        for thread_id in 0..4 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..64 {
                    let id = format!("u{thread_id}_{i}");
                    let email = format!("{id}@example.com");
                    store.put("users", user(&id, &email, i)).unwrap();
                    let key = SchemaKey::String(id);
                    assert!(store.get("users", &key).unwrap().is_some());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn capacity_full_write_does_not_leave_stale_index_entry() {
        let store = SchemaStore::with_config(WriteBehindConfig {
            initial_capacity: 1,
            dirty_shards: 1,
            dirty_queue_capacity: 64,
        });
        store.register_collection(user_schema()).unwrap();

        let mut rejected = None;
        for i in 0..64 {
            let id = format!("u{i}");
            let email = format!("u{i}@example.com");
            match store.create("users", user(&id, &email, i)) {
                Ok(_) => {}
                Err(SchemaStoreError::StoreFull { collection }) => {
                    assert_eq!(collection, "users");
                    rejected = Some(email);
                    break;
                }
                Err(error) => panic!("unexpected error: {error:?}"),
            }
        }

        let rejected = rejected.expect("small store should fill");
        assert_eq!(
            store
                .lookup("users", "email", &SchemaValue::String(rejected))
                .unwrap(),
            Vec::<SchemaKey>::new()
        );
    }
}
