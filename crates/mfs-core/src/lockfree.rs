//! Lock-free read-heavy cache.
//!
//! [`LockFreeCache`] is a thin facade over
//! [`crate::concurrent_map::ConcurrentMap`], the crate's in-house
//! open-addressed lock-free hash table with seize hyaline
//! reclamation. The previous version of this type wrapped
//! `papaya::HashMap`; the migration off papaya kept the API
//! shape identical so existing call sites don't need changes.
//!
//! ## When to use this vs [`crate::MemoryFirstStore`]
//!
//! Use [`LockFreeCache`] when you want the fastest possible
//! concurrent get/insert path and you do **not** need:
//!
//! - per-slot dirty tracking + idle-driven write-behind
//! - per-slot version numbers for safe flush-and-evict
//! - sampled `last_touch` LRU hints
//! - the [`crate::FlushBackend`] trait integration
//!
//! Use [`crate::MemoryFirstStore`] when you do need any of the above.
//!
//! ## API shape
//!
//! All access goes through a [`Pinned`] guard obtained from
//! [`LockFreeCache::pin`]. The guard pins the current epoch so that
//! values returned via `&V` cannot be reclaimed underfoot. Drop the
//! guard quickly — long-lived guards delay reclamation.
//!
//! ```
//! use mfs_core::lockfree::LockFreeCache;
//!
//! let cache = LockFreeCache::<u64, u64>::new();
//! {
//!     let pinned = cache.pin();
//!     pinned.insert(1, 100);
//!     assert_eq!(pinned.get(&1).copied(), Some(100));
//! }
//! ```

use crate::FastBuildHasher;
use crate::concurrent_map::{ConcurrentMap, InsertOutcome};
use std::hash::{BuildHasher, Hash};

/// Default initial capacity used by [`LockFreeCache::new`].
const DEFAULT_CAPACITY: usize = 1_000_000;

pub struct LockFreeCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    map: ConcurrentMap<K, V, S>,
}

impl<K, V> LockFreeCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: ConcurrentMap::with_capacity(capacity),
        }
    }
}

impl<K, V, S> LockFreeCache<K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        Self {
            map: ConcurrentMap::with_hasher_and_capacity(hash_builder, capacity),
        }
    }

    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            inner: self.map.pin(),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K, V> Default for LockFreeCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Guard pinning the current epoch. Returned from
/// [`LockFreeCache::pin`]. Drop the guard quickly — long-lived
/// guards delay reclamation.
pub struct Pinned<'g, K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    inner: crate::concurrent_map::Pinned<'g, K, V, S>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    /// Lookup. Returns a reference valid for the guard's lifetime.
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// Insert or replace. Returns whether the operation actually
    /// landed (false if the table is full).
    #[inline]
    pub fn insert(&self, key: K, value: V) -> bool {
        match self.try_insert(key, value) {
            InsertOutcome::Inserted | InsertOutcome::Replaced => true,
            InsertOutcome::Full => false,
        }
    }

    /// Insert or replace and expose the underlying capacity outcome.
    #[inline]
    pub fn try_insert(&self, key: K, value: V) -> InsertOutcome {
        self.inner.insert(key, value)
    }

    #[inline]
    pub fn remove(&self, key: &K) -> bool {
        self.inner.remove(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let cache = LockFreeCache::<u64, u64>::with_capacity(1024);
        {
            let p = cache.pin();
            p.insert(1, 100);
            p.insert(2, 200);
            assert_eq!(p.get(&1).copied(), Some(100));
            assert_eq!(p.get(&2).copied(), Some(200));
            p.remove(&1);
            assert!(p.get(&1).is_none());
            assert_eq!(p.get(&2).copied(), Some(200));
        }
    }

    #[test]
    fn many_inserts_round_trip() {
        let cache = LockFreeCache::<u64, u64>::with_capacity(2048);
        let p = cache.pin();
        for i in 0..1024u64 {
            p.insert(i, i.wrapping_mul(7));
        }
        for i in 0..1024u64 {
            assert_eq!(p.get(&i).copied(), Some(i.wrapping_mul(7)));
        }
        assert_eq!(p.len(), 1024);
    }

    #[test]
    fn try_insert_reports_full() {
        let cache = LockFreeCache::<u64, u64>::with_capacity(1);
        let p = cache.pin();
        let mut inserted = 0usize;
        let mut full = 0usize;
        for key in 0..32u64 {
            match p.try_insert(key, key) {
                InsertOutcome::Inserted => inserted += 1,
                InsertOutcome::Replaced => unreachable!(),
                InsertOutcome::Full => full += 1,
            }
        }

        assert!(inserted > 0);
        assert!(full > 0, "expected at least one full-capacity insert");
        assert_eq!(p.len(), inserted);
    }
}
