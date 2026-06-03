//! Redis-like object-store façade.
//!
//! This module provides a developer-facing CRUD API over arbitrary
//! [`mfs_db::value::MfsValue`] values. The first implementation is intentionally
//! conservative: it uses [`mfs_core::writeback::WriteBehindCache`] because the
//! current `MfsValue` writer benchmark shows it is still the fastest baseline
//! for arbitrary heap-backed values. Slot and atomic writers remain candidate
//! backends once their object-value benchmarks beat the boxed path.

use crossbeam_utils::CachePadded;
use mfs_core::writeback::{
    AutoFlusher, AutoFlusherConfig, Pinned as WriteBehindPinned, WriteBehindCache,
    WriteBehindConfig, WriteBehindError, WriteBehindStats,
};
use mfs_core::{FastBuildHasher, FlushBackend, FlushRecord, Operation};
use mfs_db::value::{MfsValue, SortedSetEntry, ValueTag};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque, hash_map::Entry};
use std::hash::BuildHasher;
use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicU64, Ordering},
};

const DEFAULT_MUTATION_LOCKS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreError {
    WrongType {
        expected: &'static str,
        actual: ValueTag,
    },
    InvalidValue(&'static str),
    CapacityFull,
}

pub struct MfsObjectStore<S = FastBuildHasher>
where
    S: BuildHasher,
{
    inner: Arc<WriteBehindCache<Vec<u8>, MfsValue, S>>,
    mutation_locks: Box<[CachePadded<Mutex<()>>]>,
    mutation_lock_mask: usize,
    hash_builder: S,
}

impl MfsObjectStore {
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

impl<S> MfsObjectStore<S>
where
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        let lock_count = DEFAULT_MUTATION_LOCKS.next_power_of_two();
        let locks = (0..lock_count)
            .map(|_| CachePadded::new(Mutex::new(())))
            .collect::<Vec<_>>();
        Self {
            inner: Arc::new(WriteBehindCache::with_hasher_and_config(
                hash_builder.clone(),
                config,
            )),
            mutation_locks: locks.into_boxed_slice(),
            mutation_lock_mask: lock_count - 1,
            hash_builder,
        }
    }

    #[inline]
    fn lock_key(&self, key: &[u8]) -> MutexGuard<'_, ()> {
        let idx = (self.hash_builder.hash_one(key) as usize) & self.mutation_lock_mask;
        match self.mutation_locks[idx].lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Arc<MfsValue>> {
        self.inner.get(&key.to_vec())
    }

    #[inline]
    pub fn read_with<R, F>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&MfsValue) -> R,
    {
        self.inner.read_with(&key.to_vec(), f)
    }

    #[inline]
    pub fn put(&self, key: impl Into<Vec<u8>>, value: MfsValue) -> u64 {
        self.try_put(key, value).expect("object-store write failed")
    }

    #[inline]
    pub fn try_put(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
    ) -> Result<u64, ObjectStoreError> {
        validate_value(&value)?;
        let key = key.into();
        let _guard = self.lock_key(&key);
        self.put_locked(key, value)
    }

    #[inline]
    pub fn put_arc(&self, key: impl Into<Vec<u8>>, value: Arc<MfsValue>) -> u64 {
        self.try_put_arc(key, value)
            .expect("object-store write failed")
    }

    #[inline]
    pub fn try_put_arc(
        &self,
        key: impl Into<Vec<u8>>,
        value: Arc<MfsValue>,
    ) -> Result<u64, ObjectStoreError> {
        validate_value(value.as_ref())?;
        let key = key.into();
        let _guard = self.lock_key(&key);
        self.inner
            .try_put_arc(key, value)
            .map_err(object_store_error)
    }

    #[inline]
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> u64 {
        self.try_delete(key).expect("object-store delete failed")
    }

    #[inline]
    pub fn try_delete(&self, key: impl Into<Vec<u8>>) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        self.delete_locked(key)
    }

    #[inline]
    pub fn load_clean(&self, key: impl Into<Vec<u8>>, value: MfsValue) -> u64 {
        self.try_load_clean(key, value)
            .expect("object-store clean load failed")
    }

    #[inline]
    pub fn try_load_clean(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
    ) -> Result<u64, ObjectStoreError> {
        validate_value(&value)?;
        let key = key.into();
        let _guard = self.lock_key(&key);
        self.load_clean_locked(key, value)
    }

    #[inline]
    pub fn load_clean_arc(&self, key: impl Into<Vec<u8>>, value: Arc<MfsValue>) -> u64 {
        self.try_load_clean_arc(key, value)
            .expect("object-store clean load failed")
    }

    #[inline]
    pub fn try_load_clean_arc(
        &self,
        key: impl Into<Vec<u8>>,
        value: Arc<MfsValue>,
    ) -> Result<u64, ObjectStoreError> {
        validate_value(value.as_ref())?;
        let key = key.into();
        let _guard = self.lock_key(&key);
        self.inner
            .try_load_clean_arc(key, value)
            .map_err(object_store_error)
    }

    #[inline]
    fn put_locked(&self, key: Vec<u8>, value: MfsValue) -> Result<u64, ObjectStoreError> {
        validate_value(&value)?;
        self.inner.try_put(key, value).map_err(object_store_error)
    }

    #[inline]
    fn load_clean_locked(&self, key: Vec<u8>, value: MfsValue) -> Result<u64, ObjectStoreError> {
        validate_value(&value)?;
        self.inner
            .try_load_clean(key, value)
            .map_err(object_store_error)
    }

    #[inline]
    fn delete_locked(&self, key: Vec<u8>) -> Result<u64, ObjectStoreError> {
        self.inner.try_delete(key).map_err(object_store_error)
    }

    #[inline]
    fn put_pinned_locked(
        &self,
        pinned: &WriteBehindPinned<'_, Vec<u8>, MfsValue, S>,
        key: Vec<u8>,
        value: MfsValue,
    ) -> Result<u64, ObjectStoreError> {
        validate_value(&value)?;
        pinned.try_put(key, value).map_err(object_store_error)
    }

    #[inline]
    fn delete_pinned_locked(
        &self,
        pinned: &WriteBehindPinned<'_, Vec<u8>, MfsValue, S>,
        key: Vec<u8>,
    ) -> Result<u64, ObjectStoreError> {
        pinned.try_delete(key).map_err(object_store_error)
    }

    pub fn set_bytes(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Bytes(value.into()))
    }

    pub fn set_string(&self, key: impl Into<Vec<u8>>, value: impl Into<String>) -> u64 {
        self.put(key, MfsValue::String(value.into()))
    }

    pub fn set_integer(&self, key: impl Into<Vec<u8>>, value: i64) -> u64 {
        self.put(key, MfsValue::Integer(value))
    }

    pub fn set_json_bytes(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Json(value.into()))
    }

    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.typed_get(key, "bytes", |value| match value {
            MfsValue::Bytes(bytes) => Some(bytes.clone()),
            _ => None,
        })
    }

    pub fn get_string(&self, key: &[u8]) -> Result<Option<String>, ObjectStoreError> {
        self.typed_get(key, "string", |value| match value {
            MfsValue::String(value) => Some(value.clone()),
            _ => None,
        })
    }

    pub fn get_integer(&self, key: &[u8]) -> Result<Option<i64>, ObjectStoreError> {
        self.typed_get(key, "integer", |value| match value {
            MfsValue::Integer(value) => Some(*value),
            _ => None,
        })
    }

    fn typed_get<T>(
        &self,
        key: &[u8],
        expected: &'static str,
        extract: impl FnOnce(&MfsValue) -> Option<T>,
    ) -> Result<Option<T>, ObjectStoreError> {
        let Some(value) = self.inner.get(&key.to_vec()) else {
            return Ok(None);
        };
        match extract(value.as_ref()) {
            Some(value) => Ok(Some(value)),
            None => Err(wrong_type(expected, value.as_ref())),
        }
    }

    pub fn append_bytes(
        &self,
        key: impl Into<Vec<u8>>,
        suffix: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut bytes = match pinned.read_with(&key, |value| match value {
            MfsValue::Bytes(bytes) => Ok(bytes.clone()),
            other => Err(wrong_type("bytes", other)),
        }) {
            Some(result) => result?,
            None => Vec::new(),
        };
        bytes.extend_from_slice(suffix.as_ref());
        self.put_pinned_locked(&pinned, key, MfsValue::Bytes(bytes))
    }

    pub fn incr_by(&self, key: impl Into<Vec<u8>>, delta: i64) -> Result<i64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let current = match pinned.read_with(&key, |value| match value {
            MfsValue::Integer(value) => Ok(*value),
            other => Err(wrong_type("integer", other)),
        }) {
            Some(result) => result?,
            None => 0,
        };
        let next = current
            .checked_add(delta)
            .ok_or(ObjectStoreError::InvalidValue("integer increment overflow"))?;
        self.put_pinned_locked(&pinned, key, MfsValue::Integer(next))?;
        Ok(next)
    }

    pub fn set_list(&self, key: impl Into<Vec<u8>>, values: Vec<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::List(values))
    }

    pub fn set_hash(&self, key: impl Into<Vec<u8>>, fields: BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Hash(fields))
    }

    pub fn set_set(&self, key: impl Into<Vec<u8>>, members: BTreeSet<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Set(members))
    }

    pub fn set_sorted_set(
        &self,
        key: impl Into<Vec<u8>>,
        entries: Vec<SortedSetEntry>,
    ) -> Result<u64, ObjectStoreError> {
        validate_sorted_set_entries(&entries)?;
        let mut by_member = BTreeMap::new();
        for entry in entries {
            by_member.insert(entry.member, entry.score);
        }
        let mut entries: Vec<_> = by_member
            .into_iter()
            .map(|(member, score)| SortedSetEntry { score, member })
            .collect();
        entries.sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.member.cmp(&b.member))
        });
        self.try_put(key, MfsValue::SortedSet(entries))
    }

    pub fn list_push(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut list = match pinned.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(items.clone()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result?,
            None => Vec::new(),
        };
        list.push(value.into());
        self.put_pinned_locked(&pinned, key, MfsValue::List(list))
    }

    pub fn list_extend<I, V>(
        &self,
        key: impl Into<Vec<u8>>,
        values: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<u8>>,
    {
        let values: Vec<Vec<u8>> = values.into_iter().map(Into::into).collect();
        if values.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut list = match pinned.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(items.clone()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result?,
            None => Vec::new(),
        };
        list.extend(values);
        self.put_pinned_locked(&pinned, key, MfsValue::List(list))
    }

    pub fn list_pop_front(
        &self,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut list = match pinned.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(items.clone()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result?,
            None => return Ok(None),
        };
        if list.is_empty() {
            return Ok(None);
        }
        let item = list.remove(0);
        if list.is_empty() {
            self.delete_pinned_locked(&pinned, key)?;
        } else {
            self.put_pinned_locked(&pinned, key, MfsValue::List(list))?;
        }
        Ok(Some(item))
    }

    pub fn list_pop_back(
        &self,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut list = match pinned.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(items.clone()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result?,
            None => return Ok(None),
        };
        let item = list.pop();
        let Some(item) = item else { return Ok(None) };
        if list.is_empty() {
            self.delete_pinned_locked(&pinned, key)?;
        } else {
            self.put_pinned_locked(&pinned, key, MfsValue::List(list))?;
        }
        Ok(Some(item))
    }

    pub fn list_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(items.len()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result,
            None => Ok(0),
        }
    }

    pub fn list_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::List(items) => {
                let Some((start, stop)) = inclusive_range_bounds(items.len(), start, stop) else {
                    return Ok(Vec::new());
                };
                Ok(items[start..=stop].to_vec())
            }
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result,
            None => Ok(Vec::new()),
        }
    }

    pub fn list_index(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::List(items) => Ok(resolve_index(items.len(), index)
                .and_then(|idx| items.get(idx))
                .cloned()),
            other => Err(wrong_type("list", other)),
        }) {
            Some(result) => result,
            None => Ok(None),
        }
    }

    pub fn hash_set(
        &self,
        key: impl Into<Vec<u8>>,
        field: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut fields = match pinned.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.clone()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result?,
            None => BTreeMap::new(),
        };
        fields.insert(field.into(), value.into());
        self.put_pinned_locked(&pinned, key, MfsValue::Hash(fields))
    }

    pub fn hash_set_many<I, F, V>(
        &self,
        key: impl Into<Vec<u8>>,
        fields: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = (F, V)>,
        F: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let fields: Vec<(Vec<u8>, Vec<u8>)> = fields
            .into_iter()
            .map(|(field, value)| (field.into(), value.into()))
            .collect();
        if fields.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut existing = match pinned.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.clone()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result?,
            None => BTreeMap::new(),
        };
        for (field, value) in fields {
            existing.insert(field, value);
        }
        self.put_pinned_locked(&pinned, key, MfsValue::Hash(existing))
    }

    pub fn hash_get(
        &self,
        key: &[u8],
        field: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        let field = field.as_ref();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.get(field).cloned()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result,
            None => Ok(None),
        }
    }

    pub fn hash_del(
        &self,
        key: impl Into<Vec<u8>>,
        field: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let field = field.as_ref();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut fields = match pinned.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.clone()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result?,
            None => return Ok(0),
        };
        if fields.remove(field).is_none() {
            return Ok(0);
        }
        if fields.is_empty() {
            self.delete_pinned_locked(&pinned, key)?;
        } else {
            self.put_pinned_locked(&pinned, key, MfsValue::Hash(fields))?;
        }
        Ok(1)
    }

    pub fn hash_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.len()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result,
            None => Ok(0),
        }
    }

    pub fn hash_get_all(&self, key: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.clone()),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result,
            None => Ok(BTreeMap::new()),
        }
    }

    pub fn hash_exists(
        &self,
        key: &[u8],
        field: impl AsRef<[u8]>,
    ) -> Result<bool, ObjectStoreError> {
        let key = key.to_vec();
        let field = field.as_ref();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Hash(fields) => Ok(fields.contains_key(field)),
            other => Err(wrong_type("hash", other)),
        }) {
            Some(result) => result,
            None => Ok(false),
        }
    }

    pub fn set_add(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut members = match pinned.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.clone()),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result?,
            None => BTreeSet::new(),
        };
        members.insert(member.into());
        self.put_pinned_locked(&pinned, key, MfsValue::Set(members))
    }

    pub fn set_add_many<I, M>(
        &self,
        key: impl Into<Vec<u8>>,
        members: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = M>,
        M: Into<Vec<u8>>,
    {
        let members: Vec<Vec<u8>> = members.into_iter().map(Into::into).collect();
        if members.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut existing = match pinned.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.clone()),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result?,
            None => BTreeSet::new(),
        };
        existing.extend(members);
        self.put_pinned_locked(&pinned, key, MfsValue::Set(existing))
    }

    pub fn set_remove(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let member = member.as_ref();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut members = match pinned.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.clone()),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result?,
            None => return Ok(0),
        };
        if !members.remove(member) {
            return Ok(0);
        }
        if members.is_empty() {
            self.delete_pinned_locked(&pinned, key)?;
        } else {
            self.put_pinned_locked(&pinned, key, MfsValue::Set(members))?;
        }
        Ok(1)
    }

    pub fn set_contains(
        &self,
        key: &[u8],
        member: impl AsRef<[u8]>,
    ) -> Result<bool, ObjectStoreError> {
        let key = key.to_vec();
        let member = member.as_ref();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.contains(member)),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result,
            None => Ok(false),
        }
    }

    pub fn set_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.len()),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result,
            None => Ok(0),
        }
    }

    pub fn set_members(&self, key: &[u8]) -> Result<BTreeSet<Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::Set(members) => Ok(members.clone()),
            other => Err(wrong_type("set", other)),
        }) {
            Some(result) => result,
            None => Ok(BTreeSet::new()),
        }
    }

    pub fn zadd(
        &self,
        key: impl Into<Vec<u8>>,
        score: f64,
        member: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        if !score.is_finite() {
            return Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite",
            ));
        }
        let key = key.into();
        let member = member.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut by_member = match pinned.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => Ok(entries
                .iter()
                .map(|entry| (entry.member.clone(), entry.score))
                .collect::<BTreeMap<_, _>>()),
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result?,
            None => BTreeMap::new(),
        };
        by_member.insert(member, score);
        let mut entries: Vec<_> = by_member
            .into_iter()
            .map(|(member, score)| SortedSetEntry { score, member })
            .collect();
        entries.sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.member.cmp(&b.member))
        });
        self.put_pinned_locked(&pinned, key, MfsValue::SortedSet(entries))
    }

    pub fn zadd_many<I, M>(
        &self,
        key: impl Into<Vec<u8>>,
        entries: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = (f64, M)>,
        M: Into<Vec<u8>>,
    {
        let entries: Vec<(f64, Vec<u8>)> = entries
            .into_iter()
            .map(|(score, member)| (score, member.into()))
            .collect();
        if entries.is_empty() {
            return Ok(0);
        }
        if entries.iter().any(|(score, _)| !score.is_finite()) {
            return Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite",
            ));
        }
        let key = key.into();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut by_member = match pinned.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => Ok(entries
                .iter()
                .map(|entry| (entry.member.clone(), entry.score))
                .collect::<BTreeMap<_, _>>()),
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result?,
            None => BTreeMap::new(),
        };
        for (score, member) in entries {
            by_member.insert(member, score);
        }
        let mut entries: Vec<_> = by_member
            .into_iter()
            .map(|(member, score)| SortedSetEntry { score, member })
            .collect();
        entries.sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.member.cmp(&b.member))
        });
        self.put_pinned_locked(&pinned, key, MfsValue::SortedSet(entries))
    }

    pub fn zscore(
        &self,
        key: &[u8],
        member: impl AsRef<[u8]>,
    ) -> Result<Option<f64>, ObjectStoreError> {
        let key = key.to_vec();
        let member = member.as_ref();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => Ok(entries
                .iter()
                .find(|entry| entry.member.as_slice() == member)
                .map(|entry| entry.score)),
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result,
            None => Ok(None),
        }
    }

    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => {
                let Some((start, stop)) = inclusive_range_bounds(entries.len(), start, stop) else {
                    return Ok(Vec::new());
                };
                Ok(entries[start..=stop]
                    .iter()
                    .map(|entry| entry.member.clone())
                    .collect())
            }
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result,
            None => Ok(Vec::new()),
        }
    }

    pub fn zrem(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        let member = member.as_ref();
        let _guard = self.lock_key(&key);
        let pinned = self.inner.pin();
        let mut entries = match pinned.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => Ok(entries.clone()),
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result?,
            None => return Ok(0),
        };
        let len_before = entries.len();
        entries.retain(|entry| entry.member.as_slice() != member);
        if entries.len() == len_before {
            return Ok(0);
        }
        if entries.is_empty() {
            self.delete_pinned_locked(&pinned, key)?;
        } else {
            self.put_pinned_locked(&pinned, key, MfsValue::SortedSet(entries))?;
        }
        Ok(1)
    }

    pub fn zlen(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        let key = key.to_vec();
        match self.inner.read_with(&key, |value| match value {
            MfsValue::SortedSet(entries) => Ok(entries.len()),
            other => Err(wrong_type("sorted set", other)),
        }) {
            Some(result) => result,
            None => Ok(0),
        }
    }

    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<Vec<u8>, MfsValue>,
    {
        self.inner.flush_idle(backend, idle_ticks, max_records)
    }

    #[inline]
    pub fn stats(&self) -> WriteBehindStats {
        self.inner.stats()
    }
}

impl MfsObjectStore {
    pub fn spawn_auto_flusher<B, F>(
        &self,
        backend_factory: F,
        config: AutoFlusherConfig,
    ) -> AutoFlusher
    where
        B: FlushBackend<Vec<u8>, MfsValue> + Send + 'static,
        F: FnMut(usize) -> B,
    {
        AutoFlusher::spawn(Arc::clone(&self.inner), backend_factory, config)
    }
}

enum MutableObjectValue {
    List(VecDeque<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    SortedSet(MutableSortedSet),
    Direct(MfsValue),
}

#[derive(Clone)]
struct ScoredMember {
    score: f64,
    member: Vec<u8>,
}

impl PartialEq for ScoredMember {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score).is_eq() && self.member == other.member
    }
}

impl Eq for ScoredMember {}

impl PartialOrd for ScoredMember {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredMember {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

struct MutableSortedSet {
    by_member: BTreeMap<Vec<u8>, f64>,
    by_score: BTreeSet<ScoredMember>,
}

impl MutableSortedSet {
    fn from_entries(entries: Vec<SortedSetEntry>) -> Self {
        let mut by_member = BTreeMap::new();
        for entry in entries {
            by_member.insert(entry.member, entry.score);
        }
        let by_score = by_member
            .iter()
            .map(|(member, score)| ScoredMember {
                score: *score,
                member: member.clone(),
            })
            .collect();
        Self {
            by_member,
            by_score,
        }
    }

    fn to_entries(&self) -> Vec<SortedSetEntry> {
        self.by_score
            .iter()
            .map(|entry| SortedSetEntry {
                score: entry.score,
                member: entry.member.clone(),
            })
            .collect()
    }

    fn insert(&mut self, member: Vec<u8>, score: f64) {
        if let Some(old_score) = self.by_member.insert(member.clone(), score) {
            self.by_score.remove(&ScoredMember {
                score: old_score,
                member: member.clone(),
            });
        }
        self.by_score.insert(ScoredMember { score, member });
    }

    fn remove(&mut self, member: &[u8]) -> bool {
        let member_vec = member.to_vec();
        let Some(score) = self.by_member.remove(member) else {
            return false;
        };
        self.by_score.remove(&ScoredMember {
            score,
            member: member_vec,
        });
        true
    }

    fn score(&self, member: &[u8]) -> Option<f64> {
        self.by_member.get(member).copied()
    }

    fn range(&self, start: i64, stop: i64) -> Vec<Vec<u8>> {
        let Some((start, stop)) = inclusive_range_bounds(self.by_score.len(), start, stop) else {
            return Vec::new();
        };
        self.by_score
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|entry| entry.member.clone())
            .collect()
    }

    fn len(&self) -> usize {
        self.by_member.len()
    }

    fn is_empty(&self) -> bool {
        self.by_member.is_empty()
    }
}

impl MutableObjectValue {
    fn from_mfs_value(value: MfsValue) -> Result<Self, ObjectStoreError> {
        validate_value(&value)?;
        Ok(match value {
            MfsValue::List(values) => Self::List(VecDeque::from(values)),
            MfsValue::Hash(fields) => Self::Hash(fields),
            MfsValue::Set(members) => Self::Set(members),
            MfsValue::SortedSet(entries) => {
                Self::SortedSet(MutableSortedSet::from_entries(entries))
            }
            other => Self::Direct(other),
        })
    }

    fn to_mfs_value(&self) -> MfsValue {
        match self {
            Self::List(values) => MfsValue::List(values.iter().cloned().collect()),
            Self::Hash(fields) => MfsValue::Hash(fields.clone()),
            Self::Set(members) => MfsValue::Set(members.clone()),
            Self::SortedSet(entries) => MfsValue::SortedSet(entries.to_entries()),
            Self::Direct(value) => value.clone(),
        }
    }

    fn tag(&self) -> ValueTag {
        match self {
            Self::List(_) => ValueTag::List,
            Self::Hash(_) => ValueTag::Hash,
            Self::Set(_) => ValueTag::Set,
            Self::SortedSet(_) => ValueTag::SortedSet,
            Self::Direct(value) => value.tag(),
        }
    }
}

type MutableObjectMap<S> = HashMap<Vec<u8>, MutableObjectValue, S>;
type MutableObjectMetaMap<S> = HashMap<Vec<u8>, MutableObjectMeta, S>;
type MutableObjectShard<S> = CachePadded<Mutex<MutableObjectMap<S>>>;
type MutableObjectMetaShard<S> = CachePadded<Mutex<MutableObjectMetaMap<S>>>;
type MutableDirtyShard = CachePadded<Mutex<VecDeque<MutableDirtyEntry>>>;

#[derive(Clone, Copy)]
struct MutableObjectMeta {
    version: u64,
    last_touch: u64,
    expires_at: u64,
    tti_ticks: u64,
    tombstone: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutableObjectExpiryMeta {
    pub version: u64,
    pub last_touch: u64,
    pub expires_at: u64,
    pub tti_ticks: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct MutableObjectTieringRecord {
    pub key: Vec<u8>,
    pub value: Arc<MfsValue>,
    pub meta: MutableObjectExpiryMeta,
    pub pending_dirty: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct MutableObjectTieringSnapshot {
    pub now: u64,
    pub hot_len: usize,
    pub records: Vec<MutableObjectTieringRecord>,
    pub skipped_empty: usize,
}

#[derive(Clone)]
struct MutableDirtyEntry {
    key: Vec<u8>,
    version: u64,
    pushed_at: u64,
    op: Operation,
}

pub struct MfsMutableObjectStore<S = FastBuildHasher>
where
    S: BuildHasher,
{
    shards: Box<[MutableObjectShard<S>]>,
    metadata_shards: Box<[MutableObjectMetaShard<S>]>,
    dirty_shards: Box<[MutableDirtyShard]>,
    lifecycle: CachePadded<RwLock<()>>,
    shard_mask: usize,
    hash_builder: S,
    clock: CachePadded<AtomicU64>,
    write_clock: CachePadded<AtomicU64>,
}

impl MfsMutableObjectStore {
    pub fn with_capacity(expected_entries: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), expected_entries)
    }
}

impl<S> MfsMutableObjectStore<S>
where
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_capacity(hash_builder: S, expected_entries: usize) -> Self {
        let shard_count = DEFAULT_MUTATION_LOCKS.next_power_of_two();
        let capacity_per_shard = expected_entries.div_ceil(shard_count);
        let shards = (0..shard_count)
            .map(|_| {
                CachePadded::new(Mutex::new(HashMap::with_capacity_and_hasher(
                    capacity_per_shard,
                    hash_builder.clone(),
                )))
            })
            .collect::<Vec<_>>();
        let metadata_shards = (0..shard_count)
            .map(|_| {
                CachePadded::new(Mutex::new(HashMap::with_capacity_and_hasher(
                    capacity_per_shard,
                    hash_builder.clone(),
                )))
            })
            .collect::<Vec<_>>();
        let dirty_shards = (0..shard_count)
            .map(|_| CachePadded::new(Mutex::new(VecDeque::new())))
            .collect::<Vec<_>>();
        Self {
            shards: shards.into_boxed_slice(),
            metadata_shards: metadata_shards.into_boxed_slice(),
            dirty_shards: dirty_shards.into_boxed_slice(),
            lifecycle: CachePadded::new(RwLock::new(())),
            shard_mask: shard_count - 1,
            hash_builder,
            clock: CachePadded::new(AtomicU64::new(1)),
            write_clock: CachePadded::new(AtomicU64::new(0)),
        }
    }
}

impl<S> MfsMutableObjectStore<S>
where
    S: BuildHasher,
{
    #[inline]
    fn shard_idx(&self, key: &[u8]) -> usize {
        (self.hash_builder.hash_one(key) as usize) & self.shard_mask
    }

    #[inline]
    fn lock_shard(&self, key: &[u8]) -> MutexGuard<'_, MutableObjectMap<S>> {
        let idx = self.shard_idx(key);
        match self.shards[idx].lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    fn lock_metadata_shard(&self, key: &[u8]) -> MutexGuard<'_, MutableObjectMetaMap<S>> {
        let idx = self.shard_idx(key);
        match self.metadata_shards[idx].lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    fn lock_dirty_shard(&self, key: &[u8]) -> MutexGuard<'_, VecDeque<MutableDirtyEntry>> {
        let idx = self.shard_idx(key);
        match self.dirty_shards[idx].lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    fn lifecycle_read(&self) -> RwLockReadGuard<'_, ()> {
        match self.lifecycle.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    fn lifecycle_write(&self) -> RwLockWriteGuard<'_, ()> {
        match self.lifecycle.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[inline]
    fn next_version(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    fn advance_clock_past(&self, version: u64) {
        let target = version.saturating_add(1);
        let mut current = self.clock.load(Ordering::Relaxed);
        while current < target {
            match self.clock.compare_exchange_weak(
                current,
                target,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    fn advance_write_clock(&self, version: u64) {
        let mut current = self.write_clock.load(Ordering::Relaxed);
        while current < version {
            match self.write_clock.compare_exchange_weak(
                current,
                version,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    #[inline]
    fn record_touch(&self, key: &[u8]) {
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut metadata = self.lock_metadata_shard(key);
        if let Some(meta) = metadata.get_mut(key) {
            meta.last_touch = tick;
        }
    }

    #[inline]
    fn mark_clean_delete(&self, key: &[u8]) {
        let mut metadata = self.lock_metadata_shard(key);
        metadata.remove(key);
    }

    #[inline]
    fn mark_dirty_with_expiry(
        &self,
        key: Vec<u8>,
        version: u64,
        op: Operation,
        expires_at: u64,
        tti_ticks: u64,
    ) {
        {
            let mut metadata = self.lock_metadata_shard(&key);
            metadata.insert(
                key.clone(),
                MutableObjectMeta {
                    version,
                    last_touch: version,
                    expires_at,
                    tti_ticks,
                    tombstone: op == Operation::Delete,
                },
            );
        }
        let mut dirty = self.lock_dirty_shard(&key);
        dirty.push_back(MutableDirtyEntry {
            key,
            version,
            pushed_at: version,
            op,
        });
        self.advance_write_clock(version);
    }

    #[inline]
    fn mark_dirty(&self, key: Vec<u8>, version: u64, op: Operation) {
        self.mark_dirty_with_expiry(key, version, op, 0, 0);
    }

    #[inline]
    fn mark_dirty_put(&self, key: Vec<u8>) -> u64 {
        let version = self.next_version();
        self.mark_dirty(key, version, Operation::Put);
        version
    }

    #[inline]
    fn mark_dirty_delete(&self, key: Vec<u8>) -> u64 {
        let version = self.next_version();
        self.mark_dirty(key, version, Operation::Delete);
        version
    }

    #[inline]
    fn mark_dirty_put_with_expiry(&self, key: Vec<u8>, ttl_ticks: u64, tti_ticks: u64) -> u64 {
        let version = self.next_version();
        let expires_at = if ttl_ticks == 0 {
            0
        } else {
            version.saturating_add(ttl_ticks)
        };
        self.mark_dirty_with_expiry(key, version, Operation::Put, expires_at, tti_ticks);
        version
    }

    fn current_meta(&self, key: &[u8]) -> Option<MutableObjectMeta> {
        let metadata = self.lock_metadata_shard(key);
        metadata.get(key).copied()
    }

    fn pending_dirty_keys(&self) -> BTreeSet<Vec<u8>> {
        let mut keys = BTreeSet::new();
        for queue in self.dirty_shards.iter() {
            let dirty = match queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            keys.extend(dirty.iter().map(|entry| entry.key.clone()));
        }
        keys
    }

    fn has_pending_dirty_key(&self, key: &[u8]) -> bool {
        let dirty = self.lock_dirty_shard(key);
        dirty.iter().any(|entry| entry.key == key)
    }

    pub(crate) fn tiering_snapshot(&self) -> MutableObjectTieringSnapshot {
        let _lifecycle = self.lifecycle_write();
        let now = self.clock.load(Ordering::Relaxed);
        let dirty_keys = self.pending_dirty_keys();
        let mut hot_len = 0usize;
        let mut skipped_empty = 0usize;
        let mut records = Vec::new();

        for idx in 0..self.shards.len() {
            let shard = match self.shards[idx].lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            hot_len = hot_len.saturating_add(shard.len());
            let metadata = match self.metadata_shards[idx].lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };

            for (key, meta) in metadata.iter() {
                if !meta.tombstone && !Self::is_expired(*meta, now) && !shard.contains_key(key) {
                    skipped_empty = skipped_empty.saturating_add(1);
                }
            }

            records.extend(shard.iter().filter_map(|(key, value)| {
                let meta = metadata.get(key)?;
                if meta.tombstone || Self::is_expired(*meta, now) {
                    return None;
                }
                Some(MutableObjectTieringRecord {
                    key: key.clone(),
                    value: Arc::new(value.to_mfs_value()),
                    meta: MutableObjectExpiryMeta {
                        version: meta.version,
                        last_touch: meta.last_touch,
                        expires_at: meta.expires_at,
                        tti_ticks: meta.tti_ticks,
                    },
                    pending_dirty: dirty_keys.contains(key.as_slice()),
                })
            }));
        }

        records.sort_by(|left, right| left.key.cmp(&right.key));
        MutableObjectTieringSnapshot {
            now,
            hot_len,
            records,
            skipped_empty,
        }
    }

    pub fn expiry_meta(&self, key: &[u8]) -> Option<MutableObjectExpiryMeta> {
        let metadata = self.lock_metadata_shard(key);
        let meta = metadata.get(key).copied()?;
        if meta.tombstone {
            return None;
        }
        Some(MutableObjectExpiryMeta {
            version: meta.version,
            last_touch: meta.last_touch,
            expires_at: meta.expires_at,
            tti_ticks: meta.tti_ticks,
        })
    }

    #[inline]
    fn is_expired(meta: MutableObjectMeta, now: u64) -> bool {
        (meta.expires_at != 0 && now >= meta.expires_at)
            || (meta.tti_ticks != 0 && now.saturating_sub(meta.last_touch) >= meta.tti_ticks)
    }

    fn expire_key_if_needed(&self, key: &[u8]) -> bool {
        let _lifecycle = self.lifecycle_write();
        let Some(meta) = self.current_meta(key) else {
            return false;
        };
        if meta.tombstone || !Self::is_expired(meta, self.clock.load(Ordering::Relaxed)) {
            return false;
        }
        {
            let mut shard = self.lock_shard(key);
            shard.remove(key);
        }
        self.mark_dirty_delete(key.to_vec());
        true
    }

    pub fn durable_high_water_mark(&self) -> u64 {
        self.write_clock.load(Ordering::Relaxed)
    }

    fn requeue_dirty<I>(&self, entries: I)
    where
        I: IntoIterator<Item = MutableDirtyEntry>,
    {
        for entry in entries {
            let mut dirty = self.lock_dirty_shard(&entry.key);
            dirty.push_back(entry);
        }
    }

    #[inline]
    fn put_mutable(&self, key: impl Into<Vec<u8>>, value: MutableObjectValue) -> u64 {
        let _lifecycle = self.lifecycle_read();
        let key = key.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        shard.insert(key, value);
        drop(shard);
        self.mark_dirty_put(dirty_key)
    }

    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Arc<MfsValue>> {
        if self.expire_key_if_needed(key) {
            return None;
        }
        let value = {
            let shard = self.lock_shard(key);
            shard.get(key).map(|value| Arc::new(value.to_mfs_value()))
        };
        if value.is_some() {
            self.record_touch(key);
        }
        value
    }

    #[inline]
    pub fn read_with<R, F>(&self, key: &[u8], f: F) -> Option<R>
    where
        F: FnOnce(&MfsValue) -> R,
    {
        if self.expire_key_if_needed(key) {
            return None;
        }
        let value = {
            let shard = self.lock_shard(key);
            shard.get(key).map(MutableObjectValue::to_mfs_value)
        }?;
        self.record_touch(key);
        Some(f(&value))
    }

    #[inline]
    pub fn put(&self, key: impl Into<Vec<u8>>, value: MfsValue) -> u64 {
        self.try_put(key, value)
            .expect("mutable object-store write failed")
    }

    #[inline]
    pub fn try_put(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
    ) -> Result<u64, ObjectStoreError> {
        Ok(self.put_mutable(key, MutableObjectValue::from_mfs_value(value)?))
    }

    pub fn put_with_ttl_ticks(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        ttl_ticks: u64,
    ) -> Result<u64, ObjectStoreError> {
        self.put_with_expiry_ticks(key, value, ttl_ticks, 0)
    }

    pub fn put_with_tti_ticks(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        tti_ticks: u64,
    ) -> Result<u64, ObjectStoreError> {
        self.put_with_expiry_ticks(key, value, 0, tti_ticks)
    }

    pub fn put_with_expiry_ticks(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        ttl_ticks: u64,
        tti_ticks: u64,
    ) -> Result<u64, ObjectStoreError> {
        let value = MutableObjectValue::from_mfs_value(value)?;
        let _lifecycle = self.lifecycle_read();
        let key = key.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        shard.insert(key, value);
        drop(shard);
        Ok(self.mark_dirty_put_with_expiry(dirty_key, ttl_ticks, tti_ticks))
    }

    pub fn set_bytes(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Bytes(value.into()))
    }

    pub fn set_string(&self, key: impl Into<Vec<u8>>, value: impl Into<String>) -> u64 {
        self.put(key, MfsValue::String(value.into()))
    }

    pub fn set_integer(&self, key: impl Into<Vec<u8>>, value: i64) -> u64 {
        self.put(key, MfsValue::Integer(value))
    }

    pub fn set_json_bytes(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> u64 {
        self.put(key, MfsValue::Json(value.into()))
    }

    pub fn get_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.typed_get(key, "bytes", |value| match value {
            MfsValue::Bytes(bytes) => Some(bytes.clone()),
            _ => None,
        })
    }

    pub fn get_string(&self, key: &[u8]) -> Result<Option<String>, ObjectStoreError> {
        self.typed_get(key, "string", |value| match value {
            MfsValue::String(value) => Some(value.clone()),
            _ => None,
        })
    }

    pub fn get_integer(&self, key: &[u8]) -> Result<Option<i64>, ObjectStoreError> {
        self.typed_get(key, "integer", |value| match value {
            MfsValue::Integer(value) => Some(*value),
            _ => None,
        })
    }

    fn typed_get<T>(
        &self,
        key: &[u8],
        expected: &'static str,
        extract: impl FnOnce(&MfsValue) -> Option<T>,
    ) -> Result<Option<T>, ObjectStoreError> {
        let Some(value) = self.get(key) else {
            return Ok(None);
        };
        match extract(value.as_ref()) {
            Some(value) => Ok(Some(value)),
            None => Err(wrong_type(expected, value.as_ref())),
        }
    }

    pub fn append_bytes(
        &self,
        key: impl Into<Vec<u8>>,
        suffix: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Direct(MfsValue::Bytes(bytes)) => {
                    bytes.extend_from_slice(suffix.as_ref());
                }
                MutableObjectValue::Direct(other) => return Err(wrong_type("bytes", other)),
                other => return Err(wrong_type_tag("bytes", other.tag())),
            },
            Entry::Vacant(entry) => {
                entry.insert(MutableObjectValue::Direct(MfsValue::Bytes(
                    suffix.as_ref().to_vec(),
                )));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn incr_by(&self, key: impl Into<Vec<u8>>, delta: i64) -> Result<i64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        let next = match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Direct(MfsValue::Integer(value)) => {
                    let next = value
                        .checked_add(delta)
                        .ok_or(ObjectStoreError::InvalidValue("integer increment overflow"))?;
                    *value = next;
                    next
                }
                MutableObjectValue::Direct(other) => return Err(wrong_type("integer", other)),
                other => return Err(wrong_type_tag("integer", other.tag())),
            },
            Entry::Vacant(entry) => {
                entry.insert(MutableObjectValue::Direct(MfsValue::Integer(delta)));
                delta
            }
        };
        drop(shard);
        self.mark_dirty_put(dirty_key);
        Ok(next)
    }

    #[inline]
    pub fn load_clean(&self, key: impl Into<Vec<u8>>, value: MfsValue) -> u64 {
        self.try_load_clean(key, value)
            .expect("mutable object-store clean load failed")
    }

    #[inline]
    pub fn try_load_clean(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
    ) -> Result<u64, ObjectStoreError> {
        let version = self.next_version();
        self.try_load_clean_versioned(key, value, version)
    }

    #[inline]
    pub fn load_clean_versioned(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        version: u64,
    ) -> u64 {
        self.try_load_clean_versioned(key, value, version)
            .expect("mutable object-store clean load failed")
    }

    #[inline]
    pub fn try_load_clean_versioned(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        version: u64,
    ) -> Result<u64, ObjectStoreError> {
        self.try_load_clean_with_expiry_meta(
            key,
            value,
            MutableObjectExpiryMeta {
                version,
                last_touch: version,
                expires_at: 0,
                tti_ticks: 0,
            },
        )
    }

    pub fn try_load_clean_with_expiry_meta(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        meta: MutableObjectExpiryMeta,
    ) -> Result<u64, ObjectStoreError> {
        let _lifecycle = self.lifecycle_read();
        let value = MutableObjectValue::from_mfs_value(value)?;
        let key = key.into();
        let metadata_key = key.clone();
        let mut shard = self.lock_shard(&key);
        shard.insert(key, value);
        drop(shard);
        self.mark_clean_put_with_expiry_meta(metadata_key, meta);
        self.advance_clock_past(meta.version.max(meta.last_touch));
        self.advance_write_clock(meta.version);
        Ok(meta.version)
    }

    pub fn try_promote_clean_with_expiry_meta(
        &self,
        key: impl Into<Vec<u8>>,
        value: MfsValue,
        meta: MutableObjectExpiryMeta,
    ) -> Result<bool, ObjectStoreError> {
        let value = MutableObjectValue::from_mfs_value(value)?;
        let _lifecycle = self.lifecycle_write();
        let key = key.into();
        let cold_meta = MutableObjectMeta {
            version: meta.version,
            last_touch: meta.last_touch,
            expires_at: meta.expires_at,
            tti_ticks: meta.tti_ticks,
            tombstone: false,
        };
        if Self::is_expired(cold_meta, self.clock.load(Ordering::Relaxed)) {
            return Ok(false);
        }
        if let Some(current) = self.current_meta(&key)
            && (current.tombstone || current.version >= meta.version)
        {
            return Ok(false);
        }

        let metadata_key = key.clone();
        let mut shard = self.lock_shard(&key);
        shard.insert(key, value);
        drop(shard);
        self.mark_clean_put_with_expiry_meta(metadata_key, meta);
        self.advance_clock_past(meta.version.max(meta.last_touch));
        self.advance_write_clock(meta.version);
        Ok(true)
    }

    fn mark_clean_put_with_expiry_meta(&self, key: Vec<u8>, meta: MutableObjectExpiryMeta) {
        let mut metadata = self.lock_metadata_shard(&key);
        metadata.insert(
            key,
            MutableObjectMeta {
                version: meta.version,
                last_touch: meta.last_touch,
                expires_at: meta.expires_at,
                tti_ticks: meta.tti_ticks,
                tombstone: false,
            },
        );
    }

    #[inline]
    pub fn load_clean_arc(&self, key: impl Into<Vec<u8>>, value: Arc<MfsValue>) -> u64 {
        self.try_load_clean_arc(key, value)
            .expect("mutable object-store clean load failed")
    }

    #[inline]
    pub fn try_load_clean_arc(
        &self,
        key: impl Into<Vec<u8>>,
        value: Arc<MfsValue>,
    ) -> Result<u64, ObjectStoreError> {
        self.try_load_clean(key, value.as_ref().clone())
    }

    #[inline]
    pub fn load_clean_delete(&self, key: impl Into<Vec<u8>>) -> u64 {
        let version = self.next_version();
        self.load_clean_delete_versioned(key, version)
    }

    #[inline]
    pub fn load_clean_delete_versioned(&self, key: impl Into<Vec<u8>>, version: u64) -> u64 {
        let _lifecycle = self.lifecycle_read();
        let key = key.into();
        let mut shard = self.lock_shard(&key);
        shard.remove(&key);
        drop(shard);
        self.mark_clean_delete(&key);
        self.advance_clock_past(version);
        self.advance_write_clock(version);
        version
    }

    pub fn evict_clean(&self, key: &[u8]) -> bool {
        let _lifecycle = self.lifecycle_read();
        let mut shard = self.lock_shard(key);
        let removed = shard.remove(key).is_some();
        drop(shard);
        self.mark_clean_delete(key);
        removed
    }

    pub(crate) fn evict_clean_versioned(&self, key: &[u8], version: u64) -> bool {
        let _lifecycle = self.lifecycle_write();
        if self.has_pending_dirty_key(key) {
            return false;
        }
        let Some(meta) = self.current_meta(key) else {
            return false;
        };
        if meta.version != version
            || meta.tombstone
            || Self::is_expired(meta, self.clock.load(Ordering::Relaxed))
        {
            return false;
        }

        let mut shard = self.lock_shard(key);
        let removed = shard.remove(key).is_some();
        drop(shard);
        if removed {
            self.mark_clean_delete(key);
        }
        removed
    }

    #[inline]
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> u64 {
        let _lifecycle = self.lifecycle_read();
        let key = key.into();
        let mut shard = self.lock_shard(&key);
        shard.remove(&key);
        drop(shard);
        self.mark_dirty_delete(key)
    }

    pub fn set_list(&self, key: impl Into<Vec<u8>>, values: Vec<Vec<u8>>) -> u64 {
        self.put_mutable(key, MutableObjectValue::List(VecDeque::from(values)))
    }

    pub fn set_hash(&self, key: impl Into<Vec<u8>>, fields: BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
        self.put_mutable(key, MutableObjectValue::Hash(fields))
    }

    pub fn set_set(&self, key: impl Into<Vec<u8>>, members: BTreeSet<Vec<u8>>) -> u64 {
        self.put_mutable(key, MutableObjectValue::Set(members))
    }

    pub fn list_push(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let value = value.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::List(values) => values.push_back(value),
                other => return Err(wrong_type_tag("list", other.tag())),
            },
            Entry::Vacant(entry) => {
                let mut values = VecDeque::new();
                values.push_back(value);
                entry.insert(MutableObjectValue::List(values));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn list_extend<I, V>(
        &self,
        key: impl Into<Vec<u8>>,
        values: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = V>,
        V: Into<Vec<u8>>,
    {
        let values: Vec<Vec<u8>> = values.into_iter().map(Into::into).collect();
        if values.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::List(existing) => existing.extend(values),
                other => return Err(wrong_type_tag("list", other.tag())),
            },
            Entry::Vacant(entry) => {
                entry.insert(MutableObjectValue::List(VecDeque::from(values)));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn list_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(0);
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::List(values)) => Ok(values.len()),
            Some(other) => Err(wrong_type_tag("list", other.tag())),
            None => Ok(0),
        }
    }

    pub fn list_range(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(Vec::new());
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::List(values)) => {
                let Some((start, stop)) = inclusive_range_bounds(values.len(), start, stop) else {
                    return Ok(Vec::new());
                };
                Ok(values
                    .iter()
                    .skip(start)
                    .take(stop - start + 1)
                    .cloned()
                    .collect())
            }
            Some(other) => Err(wrong_type_tag("list", other.tag())),
            None => Ok(Vec::new()),
        }
    }

    pub fn list_index(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(None);
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::List(values)) => Ok(resolve_index(values.len(), index)
                .and_then(|idx| values.get(idx))
                .cloned()),
            Some(other) => Err(wrong_type_tag("list", other.tag())),
            None => Ok(None),
        }
    }

    pub fn list_pop_front(
        &self,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let mut shard = self.lock_shard(&key);
        let (item, remove_entry) = match shard.get_mut(&key) {
            Some(MutableObjectValue::List(values)) => {
                let item = values.pop_front();
                let remove_entry = item.is_some() && values.is_empty();
                (item, remove_entry)
            }
            Some(other) => return Err(wrong_type_tag("list", other.tag())),
            None => return Ok(None),
        };
        if remove_entry {
            shard.remove(&key);
        }
        if item.is_some() {
            drop(shard);
            if remove_entry {
                self.mark_dirty_delete(key);
            } else {
                self.mark_dirty_put(key);
            }
        }
        Ok(item)
    }

    pub fn list_pop_back(
        &self,
        key: impl Into<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let mut shard = self.lock_shard(&key);
        let (item, remove_entry) = match shard.get_mut(&key) {
            Some(MutableObjectValue::List(values)) => {
                let item = values.pop_back();
                let remove_entry = item.is_some() && values.is_empty();
                (item, remove_entry)
            }
            Some(other) => return Err(wrong_type_tag("list", other.tag())),
            None => return Ok(None),
        };
        if remove_entry {
            shard.remove(&key);
        }
        if item.is_some() {
            drop(shard);
            if remove_entry {
                self.mark_dirty_delete(key);
            } else {
                self.mark_dirty_put(key);
            }
        }
        Ok(item)
    }

    pub fn hash_set(
        &self,
        key: impl Into<Vec<u8>>,
        field: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let field = field.into();
        let value = value.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Hash(fields) => {
                    fields.insert(field, value);
                }
                other => return Err(wrong_type_tag("hash", other.tag())),
            },
            Entry::Vacant(entry) => {
                let mut fields = BTreeMap::new();
                fields.insert(field, value);
                entry.insert(MutableObjectValue::Hash(fields));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn hash_set_many<I, F, V>(
        &self,
        key: impl Into<Vec<u8>>,
        fields: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = (F, V)>,
        F: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let fields: Vec<(Vec<u8>, Vec<u8>)> = fields
            .into_iter()
            .map(|(field, value)| (field.into(), value.into()))
            .collect();
        if fields.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Hash(existing) => {
                    for (field, value) in fields {
                        existing.insert(field, value);
                    }
                }
                other => return Err(wrong_type_tag("hash", other.tag())),
            },
            Entry::Vacant(entry) => {
                entry.insert(MutableObjectValue::Hash(fields.into_iter().collect()));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn hash_get(
        &self,
        key: &[u8],
        field: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(None);
        }
        let field = field.as_ref();
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Hash(fields)) => Ok(fields.get(field).cloned()),
            Some(other) => Err(wrong_type_tag("hash", other.tag())),
            None => Ok(None),
        }
    }

    pub fn hash_del(
        &self,
        key: impl Into<Vec<u8>>,
        field: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let field = field.as_ref();
        let mut shard = self.lock_shard(&key);
        let (removed, remove_entry) = match shard.get_mut(&key) {
            Some(MutableObjectValue::Hash(fields)) => {
                let removed = fields.remove(field).is_some();
                (removed, removed && fields.is_empty())
            }
            Some(other) => return Err(wrong_type_tag("hash", other.tag())),
            None => return Ok(0),
        };
        if !removed {
            return Ok(0);
        }
        if remove_entry {
            shard.remove(&key);
        }
        drop(shard);
        if remove_entry {
            self.mark_dirty_delete(key);
        } else {
            self.mark_dirty_put(key);
        }
        Ok(1)
    }

    pub fn hash_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(0);
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Hash(fields)) => Ok(fields.len()),
            Some(other) => Err(wrong_type_tag("hash", other.tag())),
            None => Ok(0),
        }
    }

    pub fn hash_get_all(&self, key: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(BTreeMap::new());
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Hash(fields)) => Ok(fields.clone()),
            Some(other) => Err(wrong_type_tag("hash", other.tag())),
            None => Ok(BTreeMap::new()),
        }
    }

    pub fn hash_exists(
        &self,
        key: &[u8],
        field: impl AsRef<[u8]>,
    ) -> Result<bool, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(false);
        }
        let field = field.as_ref();
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Hash(fields)) => Ok(fields.contains_key(field)),
            Some(other) => Err(wrong_type_tag("hash", other.tag())),
            None => Ok(false),
        }
    }

    pub fn set_add(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let member = member.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Set(members) => {
                    members.insert(member);
                }
                other => return Err(wrong_type_tag("set", other.tag())),
            },
            Entry::Vacant(entry) => {
                let mut members = BTreeSet::new();
                members.insert(member);
                entry.insert(MutableObjectValue::Set(members));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn set_add_many<I, M>(
        &self,
        key: impl Into<Vec<u8>>,
        members: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = M>,
        M: Into<Vec<u8>>,
    {
        let members: Vec<Vec<u8>> = members.into_iter().map(Into::into).collect();
        if members.is_empty() {
            return Ok(0);
        }
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::Set(existing) => existing.extend(members),
                other => return Err(wrong_type_tag("set", other.tag())),
            },
            Entry::Vacant(entry) => {
                entry.insert(MutableObjectValue::Set(members.into_iter().collect()));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn set_remove(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let member = member.as_ref();
        let mut shard = self.lock_shard(&key);
        let (removed, remove_entry) = match shard.get_mut(&key) {
            Some(MutableObjectValue::Set(members)) => {
                let removed = members.remove(member);
                (removed, removed && members.is_empty())
            }
            Some(other) => return Err(wrong_type_tag("set", other.tag())),
            None => return Ok(0),
        };
        if !removed {
            return Ok(0);
        }
        if remove_entry {
            shard.remove(&key);
        }
        drop(shard);
        if remove_entry {
            self.mark_dirty_delete(key);
        } else {
            self.mark_dirty_put(key);
        }
        Ok(1)
    }

    pub fn set_contains(
        &self,
        key: &[u8],
        member: impl AsRef<[u8]>,
    ) -> Result<bool, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(false);
        }
        let member = member.as_ref();
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Set(members)) => Ok(members.contains(member)),
            Some(other) => Err(wrong_type_tag("set", other.tag())),
            None => Ok(false),
        }
    }

    pub fn set_len(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(0);
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Set(members)) => Ok(members.len()),
            Some(other) => Err(wrong_type_tag("set", other.tag())),
            None => Ok(0),
        }
    }

    pub fn set_members(&self, key: &[u8]) -> Result<BTreeSet<Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(BTreeSet::new());
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::Set(members)) => Ok(members.clone()),
            Some(other) => Err(wrong_type_tag("set", other.tag())),
            None => Ok(BTreeSet::new()),
        }
    }

    pub fn set_sorted_set(
        &self,
        key: impl Into<Vec<u8>>,
        entries: Vec<SortedSetEntry>,
    ) -> Result<u64, ObjectStoreError> {
        validate_sorted_set_entries(&entries)?;
        Ok(self.put_mutable(
            key,
            MutableObjectValue::SortedSet(MutableSortedSet::from_entries(entries)),
        ))
    }

    pub fn zadd(
        &self,
        key: impl Into<Vec<u8>>,
        score: f64,
        member: impl Into<Vec<u8>>,
    ) -> Result<u64, ObjectStoreError> {
        if !score.is_finite() {
            return Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite",
            ));
        }
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let member = member.into();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::SortedSet(entries) => {
                    entries.insert(member, score);
                }
                other => return Err(wrong_type_tag("sorted set", other.tag())),
            },
            Entry::Vacant(entry) => {
                let mut entries = MutableSortedSet::from_entries(Vec::new());
                entries.insert(member, score);
                entry.insert(MutableObjectValue::SortedSet(entries));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn zadd_many<I, M>(
        &self,
        key: impl Into<Vec<u8>>,
        entries: I,
    ) -> Result<u64, ObjectStoreError>
    where
        I: IntoIterator<Item = (f64, M)>,
        M: Into<Vec<u8>>,
    {
        let entries: Vec<(f64, Vec<u8>)> = entries
            .into_iter()
            .map(|(score, member)| (score, member.into()))
            .collect();
        if entries.is_empty() {
            return Ok(0);
        }
        if entries.iter().any(|(score, _)| !score.is_finite()) {
            return Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite",
            ));
        }
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let dirty_key = key.clone();
        let mut shard = self.lock_shard(&key);
        match shard.entry(key) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                MutableObjectValue::SortedSet(existing) => {
                    for (score, member) in entries {
                        existing.insert(member, score);
                    }
                }
                other => return Err(wrong_type_tag("sorted set", other.tag())),
            },
            Entry::Vacant(entry) => {
                let mut sorted_set = MutableSortedSet::from_entries(Vec::new());
                for (score, member) in entries {
                    sorted_set.insert(member, score);
                }
                entry.insert(MutableObjectValue::SortedSet(sorted_set));
            }
        }
        drop(shard);
        Ok(self.mark_dirty_put(dirty_key))
    }

    pub fn zscore(
        &self,
        key: &[u8],
        member: impl AsRef<[u8]>,
    ) -> Result<Option<f64>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(None);
        }
        let member = member.as_ref();
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::SortedSet(entries)) => Ok(entries.score(member)),
            Some(other) => Err(wrong_type_tag("sorted set", other.tag())),
            None => Ok(None),
        }
    }

    pub fn zrange(
        &self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Vec<u8>>, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(Vec::new());
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::SortedSet(entries)) => Ok(entries.range(start, stop)),
            Some(other) => Err(wrong_type_tag("sorted set", other.tag())),
            None => Ok(Vec::new()),
        }
    }

    pub fn zrem(
        &self,
        key: impl Into<Vec<u8>>,
        member: impl AsRef<[u8]>,
    ) -> Result<u64, ObjectStoreError> {
        let key = key.into();
        self.expire_key_if_needed(&key);
        let _lifecycle = self.lifecycle_read();
        let member = member.as_ref();
        let mut shard = self.lock_shard(&key);
        let (removed, remove_entry) = match shard.get_mut(&key) {
            Some(MutableObjectValue::SortedSet(entries)) => {
                let removed = entries.remove(member);
                (removed, removed && entries.is_empty())
            }
            Some(other) => return Err(wrong_type_tag("sorted set", other.tag())),
            None => return Ok(0),
        };
        if !removed {
            return Ok(0);
        }
        if remove_entry {
            shard.remove(&key);
        }
        drop(shard);
        if remove_entry {
            self.mark_dirty_delete(key);
        } else {
            self.mark_dirty_put(key);
        }
        Ok(1)
    }

    pub fn zlen(&self, key: &[u8]) -> Result<usize, ObjectStoreError> {
        if self.expire_key_if_needed(key) {
            return Ok(0);
        }
        let shard = self.lock_shard(key);
        match shard.get(key) {
            Some(MutableObjectValue::SortedSet(entries)) => Ok(entries.len()),
            Some(other) => Err(wrong_type_tag("sorted set", other.tag())),
            None => Ok(0),
        }
    }

    fn drain_dirty(&self, max_records: usize) -> Vec<MutableDirtyEntry> {
        if max_records == 0 {
            return Vec::new();
        }
        let mut drained = Vec::with_capacity(max_records.min(1024));
        for queue in self.dirty_shards.iter() {
            let mut dirty = match queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            while drained.len() < max_records {
                let Some(entry) = dirty.pop_front() else {
                    break;
                };
                drained.push(entry);
            }
            if drained.len() == max_records {
                break;
            }
        }
        drained
    }

    fn cleanup_after_flush(&self, entries: &[MutableDirtyEntry]) {
        for entry in entries {
            if entry.op != Operation::Delete {
                continue;
            }
            let Some(meta) = self.current_meta(&entry.key) else {
                continue;
            };
            if meta.version != entry.version || !meta.tombstone {
                continue;
            }
            {
                let mut shard = self.lock_shard(&entry.key);
                shard.remove(&entry.key);
            }
            self.mark_clean_delete(&entry.key);
        }
    }

    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<Vec<u8>, MfsValue>,
    {
        let (records, flushed_entries, deferred_entries) = {
            let _lifecycle = self.lifecycle_write();
            let drained = self.drain_dirty(max_records);
            if drained.is_empty() {
                return Ok(0);
            }

            let now = self.clock.load(Ordering::Relaxed);
            let mut records = Vec::with_capacity(drained.len());
            let mut flushed_entries = Vec::with_capacity(drained.len());
            let mut deferred_entries = Vec::new();

            for entry in drained {
                let Some(meta) = self.current_meta(&entry.key) else {
                    continue;
                };
                if meta.version != entry.version {
                    continue;
                }
                if now.saturating_sub(meta.last_touch.max(entry.pushed_at)) < idle_ticks {
                    deferred_entries.push(entry);
                    continue;
                }

                match entry.op {
                    Operation::Put => {
                        if meta.tombstone {
                            continue;
                        }
                        let value = {
                            let shard = self.lock_shard(&entry.key);
                            shard.get(&entry.key).map(MutableObjectValue::to_mfs_value)
                        };
                        let Some(value) = value else {
                            continue;
                        };
                        let Some(confirm) = self.current_meta(&entry.key) else {
                            continue;
                        };
                        if confirm.version != entry.version || confirm.tombstone {
                            continue;
                        }
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value: Some(Arc::new(value)),
                            version: entry.version,
                            op: Operation::Put,
                        });
                        flushed_entries.push(entry);
                    }
                    Operation::Delete => {
                        if !meta.tombstone {
                            continue;
                        }
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value: None,
                            version: entry.version,
                            op: Operation::Delete,
                        });
                        flushed_entries.push(entry);
                    }
                }
            }
            (records, flushed_entries, deferred_entries)
        };

        if records.is_empty() {
            let _lifecycle = self.lifecycle_write();
            self.requeue_dirty(deferred_entries);
            return Ok(0);
        }

        match backend.flush(&records) {
            Ok(()) => {
                let written = records.len();
                let _lifecycle = self.lifecycle_write();
                self.cleanup_after_flush(&flushed_entries);
                self.requeue_dirty(deferred_entries);
                Ok(written)
            }
            Err(error) => {
                let _lifecycle = self.lifecycle_write();
                self.requeue_dirty(flushed_entries.into_iter().chain(deferred_entries));
                Err(error)
            }
        }
    }

    pub fn expire(&self) -> usize {
        let now = self.clock.load(Ordering::Relaxed);
        let mut keys = Vec::new();
        for metadata_shard in self.metadata_shards.iter() {
            let metadata = match metadata_shard.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            keys.extend(metadata.iter().filter_map(|(key, meta)| {
                if !meta.tombstone && Self::is_expired(*meta, now) {
                    Some(key.clone())
                } else {
                    None
                }
            }));
        }

        let mut expired = 0usize;
        for key in keys {
            if self.expire_key_if_needed(&key) {
                expired += 1;
            }
        }
        expired
    }

    pub fn snapshot_records(&self) -> Vec<FlushRecord<Vec<u8>, MfsValue>> {
        let _lifecycle = self.lifecycle_write();
        let now = self.clock.load(Ordering::Relaxed);
        let mut records = Vec::new();
        for idx in 0..self.shards.len() {
            let shard = match self.shards[idx].lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let metadata = match self.metadata_shards[idx].lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            records.extend(shard.iter().filter_map(|(key, value)| {
                let meta = metadata.get(key)?;
                if meta.tombstone || Self::is_expired(*meta, now) {
                    return None;
                }
                Some(FlushRecord {
                    key: key.clone(),
                    value: Some(Arc::new(value.to_mfs_value())),
                    version: meta.version,
                    op: Operation::Put,
                })
            }));
        }
        records.sort_by(|left, right| {
            let order = left.key.cmp(&right.key);
            if order.is_eq() {
                left.version.cmp(&right.version)
            } else {
                order
            }
        });
        records
    }

    pub fn stats(&self) -> WriteBehindStats {
        let len = self
            .shards
            .iter()
            .map(|shard| match shard.lock() {
                Ok(guard) => guard.len(),
                Err(poisoned) => poisoned.into_inner().len(),
            })
            .sum();
        let dirty = self
            .dirty_shards
            .iter()
            .map(|queue| match queue.lock() {
                Ok(guard) => guard.len(),
                Err(poisoned) => poisoned.into_inner().len(),
            })
            .sum();
        WriteBehindStats {
            len,
            dirty,
            logical_clock: self.clock.load(Ordering::Relaxed),
        }
    }

    pub fn len(&self) -> usize {
        self.stats().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn wrong_type(expected: &'static str, value: &MfsValue) -> ObjectStoreError {
    wrong_type_tag(expected, value.tag())
}

fn wrong_type_tag(expected: &'static str, actual: ValueTag) -> ObjectStoreError {
    ObjectStoreError::WrongType { expected, actual }
}

fn object_store_error(error: WriteBehindError) -> ObjectStoreError {
    match error {
        WriteBehindError::CapacityFull => ObjectStoreError::CapacityFull,
    }
}

fn validate_value(value: &MfsValue) -> Result<(), ObjectStoreError> {
    if let MfsValue::SortedSet(entries) = value {
        validate_sorted_set_entries(entries)?;
    }
    Ok(())
}

fn validate_sorted_set_entries(entries: &[SortedSetEntry]) -> Result<(), ObjectStoreError> {
    if entries.iter().any(|entry| !entry.score.is_finite()) {
        return Err(ObjectStoreError::InvalidValue(
            "sorted set scores must be finite",
        ));
    }
    Ok(())
}

fn resolve_index(len: usize, index: i64) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let len = i128::try_from(len).ok()?;
    let index = if index < 0 {
        len + i128::from(index)
    } else {
        i128::from(index)
    };
    if index < 0 || index >= len {
        return None;
    }
    usize::try_from(index).ok()
}

fn inclusive_range_bounds(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = i128::try_from(len).ok()?;
    let mut start = if start < 0 {
        len + i128::from(start)
    } else {
        i128::from(start)
    };
    let mut stop = if stop < 0 {
        len + i128::from(stop)
    } else {
        i128::from(stop)
    };
    if start < 0 {
        start = 0;
    }
    if stop < 0 || start >= len {
        return None;
    }
    if stop >= len {
        stop = len - 1;
    }
    if start > stop {
        return None;
    }
    Some((usize::try_from(start).ok()?, usize::try_from(stop).ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mfs_core::durability::{WalBackend, WalConfig};
    use mfs_core::{FlushRecord, Operation};
    use mfs_db::value::MfsValueCodec;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Default)]
    struct CollectBackend {
        records: Mutex<Vec<FlushRecord<Vec<u8>, MfsValue>>>,
    }

    impl FlushBackend<Vec<u8>, MfsValue> for CollectBackend {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    struct FailOnceBackend {
        fail_next: bool,
        records: Mutex<Vec<FlushRecord<Vec<u8>, MfsValue>>>,
    }

    impl FailOnceBackend {
        fn new() -> Self {
            Self {
                fail_next: true,
                records: Mutex::new(Vec::new()),
            }
        }
    }

    impl FlushBackend<Vec<u8>, MfsValue> for FailOnceBackend {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
            if self.fail_next {
                self.fail_next = false;
                return Err(());
            }
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    fn temp_wal_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after Unix epoch")
            .as_nanos();
        path.push(format!("mfs_mutable_object_store_{name}_{unique}.wal"));
        path
    }

    #[test]
    fn crud_round_trip_for_values() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_string(b"name".to_vec(), "Ada");
        assert_eq!(
            store.read_with(b"name", |v| v.clone()),
            Some(MfsValue::String("Ada".to_string()))
        );
        store.delete(b"name".to_vec());
        assert!(store.get(b"name").is_none());
    }

    #[test]
    fn helpers_build_composite_values() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_list(b"list".to_vec(), vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(
            store.read_with(b"list", |v| v.clone()),
            Some(MfsValue::List(vec![b"a".to_vec(), b"b".to_vec()]))
        );

        let mut hash = BTreeMap::new();
        hash.insert(b"field".to_vec(), b"value".to_vec());
        store.set_hash(b"hash".to_vec(), hash);
        assert!(matches!(
            store.get(b"hash").as_deref(),
            Some(MfsValue::Hash(_))
        ));

        let mut set = BTreeSet::new();
        set.insert(b"member".to_vec());
        store.set_set(b"set".to_vec(), set);
        assert!(matches!(
            store.get(b"set").as_deref(),
            Some(MfsValue::Set(_))
        ));

        store
            .set_sorted_set(
                b"z".to_vec(),
                vec![
                    SortedSetEntry {
                        score: 2.0,
                        member: b"b".to_vec(),
                    },
                    SortedSetEntry {
                        score: 1.0,
                        member: b"a".to_vec(),
                    },
                ],
            )
            .unwrap();
        let members = store.read_with(b"z", |v| match v {
            MfsValue::SortedSet(entries) => {
                entries.iter().map(|e| e.member.clone()).collect::<Vec<_>>()
            }
            _ => Vec::new(),
        });
        assert_eq!(members, Some(vec![b"a".to_vec(), b"b".to_vec()]));
    }

    #[test]
    fn sorted_set_rejects_non_finite_scores() {
        let store = MfsObjectStore::with_capacity(32);
        assert!(
            store
                .set_sorted_set(
                    b"z".to_vec(),
                    vec![SortedSetEntry {
                        score: f64::NAN,
                        member: b"bad".to_vec()
                    }],
                )
                .is_err()
        );
        assert_eq!(
            store.zadd(b"z".to_vec(), f64::NAN, b"bad".to_vec()),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.try_put(
                b"z".to_vec(),
                MfsValue::SortedSet(vec![SortedSetEntry {
                    score: f64::INFINITY,
                    member: b"bad".to_vec(),
                }]),
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.try_load_clean(
                b"z".to_vec(),
                MfsValue::SortedSet(vec![SortedSetEntry {
                    score: f64::NEG_INFINITY,
                    member: b"bad".to_vec(),
                }]),
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.try_put_arc(
                b"z".to_vec(),
                Arc::new(MfsValue::SortedSet(vec![SortedSetEntry {
                    score: f64::NAN,
                    member: b"bad".to_vec(),
                }])),
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.try_load_clean_arc(
                b"z".to_vec(),
                Arc::new(MfsValue::SortedSet(vec![SortedSetEntry {
                    score: f64::INFINITY,
                    member: b"bad".to_vec(),
                }])),
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert!(store.get(b"z").is_none());
    }

    #[test]
    fn arc_writes_preserve_allocation_identity() {
        let store = MfsObjectStore::with_capacity(32);
        let value = Arc::new(MfsValue::String("alpha".to_string()));
        store.put_arc(b"key".to_vec(), Arc::clone(&value));

        let loaded = store.get(b"key").expect("stored object value");
        assert!(Arc::ptr_eq(&value, &loaded));
    }

    #[test]
    fn load_clean_arc_does_not_flush() {
        let store = MfsObjectStore::with_capacity(32);
        let value = Arc::new(MfsValue::String("clean".to_string()));
        store.load_clean_arc(b"key".to_vec(), Arc::clone(&value));

        let loaded = store.get(b"key").expect("stored object value");
        assert!(Arc::ptr_eq(&value, &loaded));

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(0));
        assert!(backend.records.lock().unwrap().is_empty());
    }

    #[test]
    fn fallible_writes_report_capacity_full() {
        let store = MfsObjectStore::with_capacity(1);
        let mut full_key = None;
        for id in 0..100_000u64 {
            let key = id.to_le_bytes().to_vec();
            match store.try_put(key.clone(), MfsValue::Integer(id as i64)) {
                Ok(_) => {}
                Err(ObjectStoreError::CapacityFull) => {
                    full_key = Some(key);
                    break;
                }
                Err(error) => panic!("unexpected object-store error: {error:?}"),
            }
        }
        let full_key = full_key.expect("tiny object store should report capacity full");

        assert_eq!(
            store.try_load_clean(full_key.clone(), MfsValue::Integer(1)),
            Err(ObjectStoreError::CapacityFull)
        );
        assert_eq!(
            store.append_bytes(full_key.clone(), b"x"),
            Err(ObjectStoreError::CapacityFull)
        );
        assert_eq!(
            store.set_sorted_set(
                full_key.clone(),
                vec![SortedSetEntry {
                    score: 1.0,
                    member: b"member".to_vec(),
                }],
            ),
            Err(ObjectStoreError::CapacityFull)
        );
        assert_eq!(
            store.try_delete(full_key),
            Err(ObjectStoreError::CapacityFull)
        );
    }

    #[test]
    fn mutation_helpers_reject_wrong_type() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "not-a-list");
        assert_eq!(
            store.list_push(b"key".to_vec(), b"value".to_vec()),
            Err(ObjectStoreError::WrongType {
                expected: "list",
                actual: ValueTag::String,
            })
        );
    }

    #[test]
    fn typed_getters_report_wrong_type() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_bytes(b"bytes".to_vec(), b"abc".to_vec());
        store.set_integer(b"int".to_vec(), 7);

        assert_eq!(store.get_bytes(b"bytes"), Ok(Some(b"abc".to_vec())));
        assert_eq!(store.get_integer(b"int"), Ok(Some(7)));
        assert_eq!(store.get_string(b"missing"), Ok(None));
        assert_eq!(
            store.get_integer(b"bytes"),
            Err(ObjectStoreError::WrongType {
                expected: "integer",
                actual: ValueTag::Bytes,
            })
        );
    }

    #[test]
    fn append_bytes_and_incr_by_are_serialized() {
        let store = Arc::new(MfsObjectStore::with_capacity(256));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    store.append_bytes(b"bytes".to_vec(), b"x").unwrap();
                    store.incr_by(b"int".to_vec(), 1).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(store.get_bytes(b"bytes").unwrap().unwrap().len(), 200);
        assert_eq!(store.get_integer(b"int"), Ok(Some(200)));
    }

    #[test]
    fn incr_by_rejects_overflow() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_integer(b"int".to_vec(), i64::MAX);
        assert_eq!(
            store.incr_by(b"int".to_vec(), 1),
            Err(ObjectStoreError::InvalidValue("integer increment overflow"))
        );
    }

    #[test]
    fn list_push_is_serialized_per_key() {
        let store = Arc::new(MfsObjectStore::with_capacity(256));
        let mut handles = Vec::new();
        for worker in 0..4u8 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..50u8 {
                    store.list_push(b"list".to_vec(), vec![worker, i]).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let len = store.read_with(b"list", |value| match value {
            MfsValue::List(items) => items.len(),
            _ => 0,
        });
        assert_eq!(len, Some(200));
    }

    #[test]
    fn list_commands_handle_ranges_pops_and_missing_keys() {
        let store = MfsObjectStore::with_capacity(32);
        assert_eq!(store.list_len(b"missing"), Ok(0));
        assert_eq!(store.list_range(b"missing", 0, -1), Ok(Vec::new()));
        assert_eq!(store.list_index(b"missing", 0), Ok(None));
        assert_eq!(store.list_pop_front(b"missing".to_vec()), Ok(None));

        store.set_list(
            b"list".to_vec(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
        );

        assert_eq!(store.list_len(b"list"), Ok(4));
        assert_eq!(
            store.list_range(b"list", 1, 2),
            Ok(vec![b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(
            store.list_range(b"list", -3, -1),
            Ok(vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()])
        );
        assert_eq!(
            store.list_range(b"list", -99, 1),
            Ok(vec![b"a".to_vec(), b"b".to_vec()])
        );
        assert_eq!(store.list_range(b"list", 3, 1), Ok(Vec::new()));
        assert_eq!(store.list_index(b"list", -1), Ok(Some(b"d".to_vec())));
        assert_eq!(store.list_index(b"list", 99), Ok(None));

        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"a".to_vec()))
        );
        assert_eq!(
            store.list_pop_back(b"list".to_vec()),
            Ok(Some(b"d".to_vec()))
        );
        assert_eq!(store.list_len(b"list"), Ok(2));
        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"b".to_vec()))
        );
        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"c".to_vec()))
        );
        assert_eq!(store.list_pop_front(b"list".to_vec()), Ok(None));
        assert!(store.get(b"list").is_none());
    }

    #[test]
    fn list_pop_back_is_serialized_per_key() {
        let store = Arc::new(MfsObjectStore::with_capacity(256));
        for i in 0..200u8 {
            store.list_push(b"list".to_vec(), vec![i]).unwrap();
        }

        let popped = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let store = Arc::clone(&store);
            let popped = Arc::clone(&popped);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    if store.list_pop_back(b"list".to_vec()).unwrap().is_some() {
                        popped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(popped.load(Ordering::Relaxed), 200);
        assert_eq!(store.list_len(b"list"), Ok(0));
        assert!(store.get(b"list").is_none());
    }

    #[test]
    fn hash_commands_read_update_and_delete_fields() {
        let store = MfsObjectStore::with_capacity(32);
        assert_eq!(store.hash_len(b"missing"), Ok(0));
        assert_eq!(store.hash_get(b"missing", b"field"), Ok(None));
        assert_eq!(store.hash_exists(b"missing", b"field"), Ok(false));
        assert_eq!(store.hash_get_all(b"missing"), Ok(BTreeMap::new()));
        assert_eq!(store.hash_del(b"missing".to_vec(), b"field"), Ok(0));

        store
            .hash_set(b"hash".to_vec(), b"a".to_vec(), b"1".to_vec())
            .unwrap();
        store
            .hash_set(b"hash".to_vec(), b"b".to_vec(), b"2".to_vec())
            .unwrap();
        store
            .hash_set(b"hash".to_vec(), b"a".to_vec(), b"3".to_vec())
            .unwrap();

        assert_eq!(store.hash_len(b"hash"), Ok(2));
        assert_eq!(store.hash_get(b"hash", b"a"), Ok(Some(b"3".to_vec())));
        assert_eq!(store.hash_exists(b"hash", b"b"), Ok(true));

        let mut expected = BTreeMap::new();
        expected.insert(b"a".to_vec(), b"3".to_vec());
        expected.insert(b"b".to_vec(), b"2".to_vec());
        assert_eq!(store.hash_get_all(b"hash"), Ok(expected));

        assert_eq!(store.hash_del(b"hash".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.hash_del(b"hash".to_vec(), b"a"), Ok(1));
        assert_eq!(store.hash_get(b"hash", b"a"), Ok(None));
        assert_eq!(store.hash_del(b"hash".to_vec(), b"b"), Ok(1));
        assert!(store.get(b"hash").is_none());
    }

    #[test]
    fn set_commands_keep_unique_ordered_members() {
        let store = MfsObjectStore::with_capacity(32);
        assert_eq!(store.set_len(b"missing"), Ok(0));
        assert_eq!(store.set_contains(b"missing", b"a"), Ok(false));
        assert_eq!(store.set_members(b"missing"), Ok(BTreeSet::new()));
        assert_eq!(store.set_remove(b"missing".to_vec(), b"a"), Ok(0));

        store.set_add(b"set".to_vec(), b"b".to_vec()).unwrap();
        store.set_add(b"set".to_vec(), b"a".to_vec()).unwrap();
        store.set_add(b"set".to_vec(), b"b".to_vec()).unwrap();

        let mut expected = BTreeSet::new();
        expected.insert(b"a".to_vec());
        expected.insert(b"b".to_vec());
        assert_eq!(store.set_len(b"set"), Ok(2));
        assert_eq!(store.set_contains(b"set", b"a"), Ok(true));
        assert_eq!(store.set_members(b"set"), Ok(expected));

        assert_eq!(store.set_remove(b"set".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.set_remove(b"set".to_vec(), b"a"), Ok(1));
        assert_eq!(store.set_remove(b"set".to_vec(), b"b"), Ok(1));
        assert!(store.get(b"set").is_none());
    }

    #[test]
    fn sorted_set_commands_order_update_and_remove_members() {
        let store = MfsObjectStore::with_capacity(32);
        assert_eq!(store.zlen(b"missing"), Ok(0));
        assert_eq!(store.zrange(b"missing", 0, -1), Ok(Vec::new()));
        assert_eq!(store.zscore(b"missing", b"a"), Ok(None));
        assert_eq!(store.zrem(b"missing".to_vec(), b"a"), Ok(0));

        store.zadd(b"z".to_vec(), 2.0, b"b".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 1.0, b"a".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 2.0, b"c".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 0.5, b"b".to_vec()).unwrap();

        assert_eq!(store.zlen(b"z"), Ok(3));
        assert_eq!(store.zscore(b"z", b"b"), Ok(Some(0.5)));
        assert_eq!(
            store.zrange(b"z", 0, -1),
            Ok(vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec()])
        );
        assert_eq!(
            store.zrange(b"z", -2, -1),
            Ok(vec![b"a".to_vec(), b"c".to_vec()])
        );
        assert_eq!(store.zrange(b"z", 3, 2), Ok(Vec::new()));

        let store_same_score = MfsObjectStore::with_capacity(32);
        store_same_score
            .zadd(b"z".to_vec(), 1.0, b"b".to_vec())
            .unwrap();
        store_same_score
            .zadd(b"z".to_vec(), 1.0, b"a".to_vec())
            .unwrap();
        assert_eq!(
            store_same_score.zrange(b"z", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec()])
        );

        assert_eq!(store.zrem(b"z".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.zrem(b"z".to_vec(), b"c"), Ok(1));
        assert_eq!(store.zlen(b"z"), Ok(2));
        assert_eq!(store.zrem(b"z".to_vec(), b"b"), Ok(1));
        assert_eq!(store.zrem(b"z".to_vec(), b"a"), Ok(1));
        assert!(store.get(b"z").is_none());
    }

    #[test]
    fn batch_mutation_helpers_update_containers_once() {
        let store = MfsObjectStore::with_capacity(32);
        assert_eq!(
            store.list_extend(b"empty".to_vec(), Vec::<Vec<u8>>::new()),
            Ok(0)
        );
        assert!(store.get(b"empty").is_none());

        store
            .list_extend(b"list".to_vec(), [b"a".to_vec(), b"b".to_vec()])
            .unwrap();
        store
            .list_extend(b"list".to_vec(), [b"c".to_vec()])
            .unwrap();
        assert_eq!(
            store.list_range(b"list", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );

        store
            .hash_set_many(
                b"hash".to_vec(),
                [
                    (b"a".to_vec(), b"1".to_vec()),
                    (b"b".to_vec(), b"2".to_vec()),
                    (b"a".to_vec(), b"3".to_vec()),
                ],
            )
            .unwrap();
        assert_eq!(store.hash_len(b"hash"), Ok(2));
        assert_eq!(store.hash_get(b"hash", b"a"), Ok(Some(b"3".to_vec())));

        store
            .set_add_many(
                b"set".to_vec(),
                [b"b".to_vec(), b"a".to_vec(), b"b".to_vec()],
            )
            .unwrap();
        let mut expected_set = BTreeSet::new();
        expected_set.insert(b"a".to_vec());
        expected_set.insert(b"b".to_vec());
        assert_eq!(store.set_members(b"set"), Ok(expected_set));

        store
            .zadd_many(
                b"z".to_vec(),
                [
                    (2.0, b"b".to_vec()),
                    (1.0, b"a".to_vec()),
                    (0.5, b"b".to_vec()),
                ],
            )
            .unwrap();
        assert_eq!(store.zscore(b"z", b"b"), Ok(Some(0.5)));
        assert_eq!(
            store.zrange(b"z", 0, -1),
            Ok(vec![b"b".to_vec(), b"a".to_vec()])
        );
    }

    #[test]
    fn batch_mutation_helpers_reject_wrong_types_and_bad_scores() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "not-a-container");

        assert_eq!(
            store.list_extend(b"key".to_vec(), [b"value".to_vec()]),
            Err(ObjectStoreError::WrongType {
                expected: "list",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.hash_set_many(b"key".to_vec(), [(b"field".to_vec(), b"value".to_vec())]),
            Err(ObjectStoreError::WrongType {
                expected: "hash",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.set_add_many(b"key".to_vec(), [b"member".to_vec()]),
            Err(ObjectStoreError::WrongType {
                expected: "set",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.zadd_many(b"key".to_vec(), [(1.0, b"member".to_vec())]),
            Err(ObjectStoreError::WrongType {
                expected: "sorted set",
                actual: ValueTag::String,
            })
        );

        assert_eq!(
            store.zadd_many(b"bad-z".to_vec(), [(f64::NAN, b"bad".to_vec())]),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert!(store.get(b"bad-z").is_none());
    }

    #[test]
    fn new_typed_commands_reject_wrong_type() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "not-a-container");
        assert_eq!(
            store.list_len(b"key"),
            Err(ObjectStoreError::WrongType {
                expected: "list",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.hash_get(b"key", b"field"),
            Err(ObjectStoreError::WrongType {
                expected: "hash",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.set_contains(b"key", b"member"),
            Err(ObjectStoreError::WrongType {
                expected: "set",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.zscore(b"key", b"member"),
            Err(ObjectStoreError::WrongType {
                expected: "sorted set",
                actual: ValueTag::String,
            })
        );
    }

    #[test]
    fn mutable_list_commands_handle_ranges_pops_and_missing_keys() {
        let store = MfsMutableObjectStore::with_capacity(32);
        assert_eq!(store.list_len(b"missing"), Ok(0));
        assert_eq!(store.list_range(b"missing", 0, -1), Ok(Vec::new()));
        assert_eq!(store.list_index(b"missing", 0), Ok(None));
        assert_eq!(store.list_pop_front(b"missing".to_vec()), Ok(None));
        assert_eq!(
            store.list_extend(b"empty".to_vec(), Vec::<Vec<u8>>::new()),
            Ok(0)
        );
        assert!(store.get(b"empty").is_none());

        store.set_list(b"list".to_vec(), vec![b"a".to_vec(), b"b".to_vec()]);
        let v1 = store
            .list_push(b"list".to_vec(), b"c".to_vec())
            .expect("list push");
        let v2 = store
            .list_extend(b"list".to_vec(), [b"d".to_vec()])
            .expect("list extend");
        assert!(v2 > v1);

        assert_eq!(store.list_len(b"list"), Ok(4));
        assert_eq!(
            store.list_range(b"list", 1, -1),
            Ok(vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()])
        );
        assert_eq!(store.list_index(b"list", -1), Ok(Some(b"d".to_vec())));
        assert_eq!(
            store.read_with(b"list", |value| value.clone()),
            Some(MfsValue::List(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
            ]))
        );

        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"a".to_vec()))
        );
        assert_eq!(
            store.list_pop_back(b"list".to_vec()),
            Ok(Some(b"d".to_vec()))
        );
        assert_eq!(store.list_len(b"list"), Ok(2));
        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"b".to_vec()))
        );
        assert_eq!(
            store.list_pop_front(b"list".to_vec()),
            Ok(Some(b"c".to_vec()))
        );
        assert_eq!(store.list_pop_front(b"list".to_vec()), Ok(None));
        assert!(store.get(b"list").is_none());
    }

    #[test]
    fn mutable_hash_commands_read_update_and_delete_fields() {
        let store = MfsMutableObjectStore::with_capacity(32);
        assert_eq!(store.hash_len(b"missing"), Ok(0));
        assert_eq!(store.hash_get(b"missing", b"field"), Ok(None));
        assert_eq!(store.hash_exists(b"missing", b"field"), Ok(false));
        assert_eq!(store.hash_get_all(b"missing"), Ok(BTreeMap::new()));
        assert_eq!(store.hash_del(b"missing".to_vec(), b"field"), Ok(0));

        let mut initial = BTreeMap::new();
        initial.insert(b"a".to_vec(), b"1".to_vec());
        store.set_hash(b"hash".to_vec(), initial);
        store
            .hash_set(b"hash".to_vec(), b"b".to_vec(), b"2".to_vec())
            .unwrap();
        store
            .hash_set_many(
                b"hash".to_vec(),
                [
                    (b"a".to_vec(), b"3".to_vec()),
                    (b"c".to_vec(), b"4".to_vec()),
                ],
            )
            .unwrap();

        assert_eq!(store.hash_len(b"hash"), Ok(3));
        assert_eq!(store.hash_get(b"hash", b"a"), Ok(Some(b"3".to_vec())));
        assert_eq!(store.hash_exists(b"hash", b"b"), Ok(true));

        let mut expected = BTreeMap::new();
        expected.insert(b"a".to_vec(), b"3".to_vec());
        expected.insert(b"b".to_vec(), b"2".to_vec());
        expected.insert(b"c".to_vec(), b"4".to_vec());
        assert_eq!(store.hash_get_all(b"hash"), Ok(expected.clone()));
        assert_eq!(
            store.get(b"hash").as_deref(),
            Some(&MfsValue::Hash(expected))
        );

        assert_eq!(store.hash_del(b"hash".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.hash_del(b"hash".to_vec(), b"a"), Ok(1));
        assert_eq!(store.hash_del(b"hash".to_vec(), b"b"), Ok(1));
        assert_eq!(store.hash_del(b"hash".to_vec(), b"c"), Ok(1));
        assert!(store.get(b"hash").is_none());
    }

    #[test]
    fn mutable_set_commands_keep_unique_ordered_members() {
        let store = MfsMutableObjectStore::with_capacity(32);
        assert_eq!(store.set_len(b"missing"), Ok(0));
        assert_eq!(store.set_contains(b"missing", b"a"), Ok(false));
        assert_eq!(store.set_members(b"missing"), Ok(BTreeSet::new()));
        assert_eq!(store.set_remove(b"missing".to_vec(), b"a"), Ok(0));

        let mut initial = BTreeSet::new();
        initial.insert(b"b".to_vec());
        store.set_set(b"set".to_vec(), initial);
        store.set_add(b"set".to_vec(), b"a".to_vec()).unwrap();
        store.set_add(b"set".to_vec(), b"b".to_vec()).unwrap();
        store
            .set_add_many(b"set".to_vec(), [b"c".to_vec(), b"a".to_vec()])
            .unwrap();

        let mut expected = BTreeSet::new();
        expected.insert(b"a".to_vec());
        expected.insert(b"b".to_vec());
        expected.insert(b"c".to_vec());
        assert_eq!(store.set_len(b"set"), Ok(3));
        assert_eq!(store.set_contains(b"set", b"a"), Ok(true));
        assert_eq!(store.set_members(b"set"), Ok(expected.clone()));
        assert_eq!(
            store.read_with(b"set", |value| value.clone()),
            Some(MfsValue::Set(expected))
        );

        assert_eq!(store.set_remove(b"set".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.set_remove(b"set".to_vec(), b"a"), Ok(1));
        assert_eq!(store.set_remove(b"set".to_vec(), b"b"), Ok(1));
        assert_eq!(store.set_remove(b"set".to_vec(), b"c"), Ok(1));
        assert!(store.get(b"set").is_none());
    }

    #[test]
    fn mutable_sorted_set_commands_order_update_and_remove_members() {
        let store = MfsMutableObjectStore::with_capacity(32);
        assert_eq!(store.zlen(b"missing"), Ok(0));
        assert_eq!(store.zscore(b"missing", b"a"), Ok(None));
        assert_eq!(store.zrange(b"missing", 0, -1), Ok(Vec::new()));
        assert_eq!(store.zrem(b"missing".to_vec(), b"a"), Ok(0));

        store
            .set_sorted_set(
                b"z".to_vec(),
                vec![
                    SortedSetEntry {
                        score: 2.0,
                        member: b"b".to_vec(),
                    },
                    SortedSetEntry {
                        score: 1.0,
                        member: b"a".to_vec(),
                    },
                ],
            )
            .unwrap();
        store.zadd(b"z".to_vec(), 3.0, b"c".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 0.5, b"b".to_vec()).unwrap();
        store
            .zadd_many(b"z".to_vec(), [(4.0, b"d".to_vec()), (2.5, b"e".to_vec())])
            .unwrap();

        assert_eq!(store.zlen(b"z"), Ok(5));
        assert_eq!(store.zscore(b"z", b"b"), Ok(Some(0.5)));
        assert_eq!(store.zscore(b"z", b"missing"), Ok(None));
        assert_eq!(
            store.zrange(b"z", 0, -1),
            Ok(vec![
                b"b".to_vec(),
                b"a".to_vec(),
                b"e".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
            ])
        );
        assert_eq!(
            store.read_with(b"z", |value| value.clone()),
            Some(MfsValue::SortedSet(vec![
                SortedSetEntry {
                    score: 0.5,
                    member: b"b".to_vec(),
                },
                SortedSetEntry {
                    score: 1.0,
                    member: b"a".to_vec(),
                },
                SortedSetEntry {
                    score: 2.5,
                    member: b"e".to_vec(),
                },
                SortedSetEntry {
                    score: 3.0,
                    member: b"c".to_vec(),
                },
                SortedSetEntry {
                    score: 4.0,
                    member: b"d".to_vec(),
                },
            ]))
        );

        assert_eq!(store.zrem(b"z".to_vec(), b"missing"), Ok(0));
        assert_eq!(store.zrem(b"z".to_vec(), b"b"), Ok(1));
        assert_eq!(store.zlen(b"z"), Ok(4));
        assert_eq!(store.zrem(b"z".to_vec(), b"a"), Ok(1));
        assert_eq!(store.zrem(b"z".to_vec(), b"e"), Ok(1));
        assert_eq!(store.zrem(b"z".to_vec(), b"c"), Ok(1));
        assert_eq!(store.zrem(b"z".to_vec(), b"d"), Ok(1));
        assert!(store.get(b"z").is_none());
    }

    #[test]
    fn mutable_sorted_set_orders_equal_scores_by_member() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.zadd(b"z".to_vec(), 1.0, b"c".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 1.0, b"a".to_vec()).unwrap();
        store.zadd(b"z".to_vec(), 1.0, b"b".to_vec()).unwrap();

        assert_eq!(
            store.zrange(b"z", 0, -1),
            Ok(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        );
        assert_eq!(
            store.read_with(b"z", |value| value.clone()),
            Some(MfsValue::SortedSet(vec![
                SortedSetEntry {
                    score: 1.0,
                    member: b"a".to_vec(),
                },
                SortedSetEntry {
                    score: 1.0,
                    member: b"b".to_vec(),
                },
                SortedSetEntry {
                    score: 1.0,
                    member: b"c".to_vec(),
                },
            ]))
        );
    }

    #[test]
    fn mutable_list_hash_and_set_updates_are_serialized() {
        let store = Arc::new(MfsMutableObjectStore::with_capacity(512));
        let mut handles = Vec::new();
        for worker in 0..4u8 {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..50u8 {
                    let item = vec![worker, i];
                    store.list_push(b"list".to_vec(), item.clone()).unwrap();
                    store
                        .hash_set(b"hash".to_vec(), item.clone(), vec![i])
                        .unwrap();
                    store.set_add(b"set".to_vec(), item).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(store.list_len(b"list"), Ok(200));
        assert_eq!(store.hash_len(b"hash"), Ok(200));
        assert_eq!(store.set_len(b"set"), Ok(200));
    }

    #[test]
    fn mutable_commands_reject_wrong_types_and_keep_direct_values() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.put(
            b"key".to_vec(),
            MfsValue::String("not-a-container".to_string()),
        );

        assert_eq!(
            store.list_push(b"key".to_vec(), b"value".to_vec()),
            Err(ObjectStoreError::WrongType {
                expected: "list",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.hash_set(b"key".to_vec(), b"field".to_vec(), b"value".to_vec()),
            Err(ObjectStoreError::WrongType {
                expected: "hash",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.set_add(b"key".to_vec(), b"member".to_vec()),
            Err(ObjectStoreError::WrongType {
                expected: "set",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.list_len(b"key"),
            Err(ObjectStoreError::WrongType {
                expected: "list",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.hash_get(b"key", b"field"),
            Err(ObjectStoreError::WrongType {
                expected: "hash",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.set_contains(b"key", b"member"),
            Err(ObjectStoreError::WrongType {
                expected: "set",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.zadd(b"key".to_vec(), 1.0, b"member".to_vec()),
            Err(ObjectStoreError::WrongType {
                expected: "sorted set",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.zscore(b"key", b"member"),
            Err(ObjectStoreError::WrongType {
                expected: "sorted set",
                actual: ValueTag::String,
            })
        );

        assert_eq!(
            store.try_put(
                b"bad-z".to_vec(),
                MfsValue::SortedSet(vec![SortedSetEntry {
                    score: f64::NAN,
                    member: b"bad".to_vec(),
                }]),
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert!(store.get(b"bad-z").is_none());
        assert_eq!(
            store.set_sorted_set(
                b"bad-z".to_vec(),
                vec![SortedSetEntry {
                    score: f64::INFINITY,
                    member: b"bad".to_vec(),
                }],
            ),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.zadd(b"bad-z".to_vec(), f64::NAN, b"bad".to_vec()),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );
        assert_eq!(
            store.zadd_many(b"bad-z".to_vec(), [(f64::NEG_INFINITY, b"bad".to_vec())]),
            Err(ObjectStoreError::InvalidValue(
                "sorted set scores must be finite"
            ))
        );

        let zset = MfsValue::SortedSet(vec![SortedSetEntry {
            score: 1.0,
            member: b"member".to_vec(),
        }]);
        store.put(b"z".to_vec(), zset.clone());
        assert_eq!(store.get(b"z").as_deref(), Some(&zset));
        store.delete(b"z".to_vec());
        assert!(store.get(b"z").is_none());
    }

    #[test]
    fn mutable_scalar_helpers_append_and_increment() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.append_bytes(b"bytes".to_vec(), b"a").unwrap();
        store.append_bytes(b"bytes".to_vec(), b"bc").unwrap();
        assert_eq!(store.get_bytes(b"bytes"), Ok(Some(b"abc".to_vec())));

        assert_eq!(store.incr_by(b"int".to_vec(), 7), Ok(7));
        assert_eq!(store.incr_by(b"int".to_vec(), -2), Ok(5));
        assert_eq!(store.get_integer(b"int"), Ok(Some(5)));

        store.set_string(b"wrong".to_vec(), "text");
        assert_eq!(
            store.append_bytes(b"wrong".to_vec(), b"x"),
            Err(ObjectStoreError::WrongType {
                expected: "bytes",
                actual: ValueTag::String,
            })
        );
        assert_eq!(
            store.incr_by(b"wrong".to_vec(), 1),
            Err(ObjectStoreError::WrongType {
                expected: "integer",
                actual: ValueTag::String,
            })
        );
    }

    #[test]
    fn mutable_store_grows_beyond_initial_capacity_hint() {
        let store = MfsMutableObjectStore::with_capacity(1);
        for i in 0..4096u64 {
            store.set_string(i.to_le_bytes().to_vec(), format!("value-{i}"));
        }

        assert_eq!(store.len(), 4096);
        assert_eq!(
            store.get_string(&4095u64.to_le_bytes()),
            Ok(Some("value-4095".to_string()))
        );
    }

    #[test]
    fn mutable_ttl_expiry_removes_and_flushes_delete() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store
            .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::String("expires".to_string()), 2)
            .unwrap();
        assert_eq!(store.get_string(b"ttl"), Ok(Some("expires".to_string())));

        store.load_clean(b"tick".to_vec(), MfsValue::Null);
        store.load_clean(b"tick2".to_vec(), MfsValue::Null);
        assert_eq!(store.expire(), 1);
        assert!(store.get(b"ttl").is_none());

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"ttl".to_vec());
        assert_eq!(records[0].op, Operation::Delete);
        assert!(records[0].value.is_none());
    }

    #[test]
    fn mutable_tti_expiry_resets_on_access() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store
            .put_with_tti_ticks(b"tti".to_vec(), MfsValue::String("idle".to_string()), 3)
            .unwrap();
        store.load_clean(b"tick".to_vec(), MfsValue::Null);
        assert_eq!(store.get_string(b"tti"), Ok(Some("idle".to_string())));

        store.load_clean(b"tick2".to_vec(), MfsValue::Null);
        assert_eq!(store.expire(), 0);
        assert_eq!(store.get_string(b"tti"), Ok(Some("idle".to_string())));

        store.load_clean(b"tick3".to_vec(), MfsValue::Null);
        store.load_clean(b"tick4".to_vec(), MfsValue::Null);
        store.load_clean(b"tick5".to_vec(), MfsValue::Null);
        assert_eq!(store.expire(), 1);
        assert!(store.get(b"tti").is_none());
    }

    #[test]
    fn mutable_snapshot_records_skip_expired_values() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store
            .put_with_ttl_ticks(b"ttl".to_vec(), MfsValue::Bytes(b"dead".to_vec()), 2)
            .unwrap();
        store.load_clean(b"tick".to_vec(), MfsValue::Null);
        store.load_clean_delete(b"tick".to_vec());
        store.load_clean(b"tick2".to_vec(), MfsValue::Null);
        store.load_clean_delete(b"tick2".to_vec());
        assert!(store.snapshot_records().is_empty());
    }

    #[test]
    fn mutable_writes_treat_expired_keys_as_missing() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store
            .put_with_ttl_ticks(b"expired".to_vec(), MfsValue::String("old".to_string()), 2)
            .unwrap();
        store.load_clean(b"tick".to_vec(), MfsValue::Null);
        store.load_clean(b"tick2".to_vec(), MfsValue::Null);

        store
            .list_push(b"expired".to_vec(), b"new".to_vec())
            .expect("expired key should be treated as missing");
        assert_eq!(store.list_len(b"expired"), Ok(1));
        assert_eq!(
            store.list_range(b"expired", 0, -1),
            Ok(vec![b"new".to_vec()])
        );
    }

    #[test]
    fn mutable_load_clean_does_not_flush() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.load_clean(b"key".to_vec(), MfsValue::String("clean".to_string()));
        assert_eq!(store.stats().len, 1);
        assert_eq!(store.stats().dirty, 0);

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(0));
        assert!(backend.records.lock().unwrap().is_empty());

        store.load_clean_delete(b"key".to_vec());
        assert!(store.get(b"key").is_none());
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(0));
    }

    #[test]
    fn mutable_flush_emits_latest_logical_record() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.set_list(b"list".to_vec(), vec![b"a".to_vec()]);
        store
            .list_push(b"list".to_vec(), b"b".to_vec())
            .expect("list push");
        store
            .list_push(b"list".to_vec(), b"c".to_vec())
            .expect("list push");

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        assert_eq!(store.stats().dirty, 0);

        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"list".to_vec());
        assert_eq!(records[0].op, Operation::Put);
        assert_eq!(
            records[0].value.as_deref(),
            Some(&MfsValue::List(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ]))
        );
    }

    #[test]
    fn mutable_flush_emits_delete_tombstone() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "value");

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        backend.records.lock().unwrap().clear();

        store.delete(b"key".to_vec());
        assert!(store.get(b"key").is_none());
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        assert_eq!(store.stats().dirty, 0);
        assert_eq!(store.stats().len, 0);

        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"key".to_vec());
        assert_eq!(records[0].op, Operation::Delete);
        assert!(records[0].value.is_none());
    }

    #[test]
    fn mutable_flush_requeues_on_backend_failure() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "value");

        let mut backend = FailOnceBackend::new();
        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Err(()));
        assert_eq!(store.stats().dirty, 1);

        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        assert_eq!(store.stats().dirty, 0);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"key".to_vec());
    }

    #[test]
    fn mutable_flush_respects_idle_ticks() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.set_string(b"key".to_vec(), "value");
        assert_eq!(store.get_string(b"key"), Ok(Some("value".to_string())));

        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 1_000, usize::MAX), Ok(0));
        assert_eq!(store.stats().dirty, 1);
        assert!(backend.records.lock().unwrap().is_empty());

        assert_eq!(store.flush_idle(&mut backend, 0, usize::MAX), Ok(1));
        assert_eq!(store.stats().dirty, 0);
    }

    #[test]
    fn mutable_snapshot_records_materialize_live_values() {
        let store = MfsMutableObjectStore::with_capacity(32);
        store.set_string(b"a".to_vec(), "alpha");
        store.set_list(b"b".to_vec(), vec![b"one".to_vec(), b"two".to_vec()]);
        store.set_bytes(b"deleted".to_vec(), b"gone".to_vec());
        store.delete(b"deleted".to_vec());

        let records = store.snapshot_records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"a".to_vec());
        assert_eq!(
            records[0].value.as_deref(),
            Some(&MfsValue::String("alpha".to_string()))
        );
        assert_eq!(records[1].key, b"b".to_vec());
        assert_eq!(
            records[1].value.as_deref(),
            Some(&MfsValue::List(vec![b"one".to_vec(), b"two".to_vec()]))
        );
    }

    #[test]
    fn mutable_store_replays_existing_wal_records_cleanly() {
        let path = temp_wal_path("replay");
        let result = (|| -> std::io::Result<()> {
            let store = MfsMutableObjectStore::with_capacity(32);
            store.set_string(b"name".to_vec(), "Ada");
            store
                .hash_set(
                    b"profile".to_vec(),
                    b"email".to_vec(),
                    b"ada@example.com".to_vec(),
                )
                .expect("hash set");
            store.set_string(b"deleted".to_vec(), "gone");
            store.delete(b"deleted".to_vec());

            let mut wal = WalBackend::open(&path, MfsValueCodec, WalConfig::default())?;
            assert_eq!(store.flush_idle(&mut wal, 0, usize::MAX)?, 3);
            wal.sync_now()?;
            drop(wal);

            let recovered = MfsMutableObjectStore::with_capacity(32);
            let replayed = WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(
                &path,
                &MfsValueCodec,
                |record| match record.op {
                    Operation::Put => {
                        if let Some(value) = record.value {
                            recovered.load_clean_versioned(record.key, value, record.version);
                        }
                    }
                    Operation::Delete => {
                        recovered.load_clean_delete_versioned(record.key, record.version);
                    }
                },
            )?;

            assert_eq!(replayed, 3);
            assert_eq!(recovered.get_string(b"name"), Ok(Some("Ada".to_string())));
            assert_eq!(
                recovered.hash_get(b"profile", b"email"),
                Ok(Some(b"ada@example.com".to_vec()))
            );
            assert!(recovered.get(b"deleted").is_none());

            let mut backend = CollectBackend::default();
            assert_eq!(recovered.flush_idle(&mut backend, 0, usize::MAX), Ok(0));
            Ok(())
        })();
        let _ = fs::remove_file(&path);
        result.unwrap();
    }

    #[test]
    fn flush_emits_object_records() {
        let store = MfsObjectStore::with_capacity(32);
        store.set_bytes(b"key".to_vec(), b"value".to_vec());
        let mut backend = CollectBackend::default();
        assert_eq!(store.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"key".to_vec());
        assert_eq!(
            records[0].value.as_deref(),
            Some(&MfsValue::Bytes(b"value".to_vec()))
        );
        assert_eq!(records[0].op, Operation::Put);
    }
}
