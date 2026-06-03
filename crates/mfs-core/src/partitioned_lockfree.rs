//! Partitioned lock-free cache for contention-heavy workloads.
//!
//! [`PartitionedLockFreeCache`] keeps the same in-house
//! [`crate::concurrent_map::ConcurrentMap`] storage as [`crate::lockfree`], but
//! splits the keyspace across independent maps. This is a targeted experiment for
//! hot-key tail latency: one busy partition should not share metadata or entry
//! cache lines with every other key in the cache.

use crate::FastBuildHasher;
use crate::concurrent_map::{ConcurrentMap, InsertOutcome};
use crossbeam_utils::CachePadded;
use std::hash::{BuildHasher, Hash};

const DEFAULT_CAPACITY: usize = 1_000_000;

pub struct PartitionedLockFreeCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    shards: Box<[CachePadded<ConcurrentMap<K, V, S>>]>,
    shard_mask: usize,
    hash_builder: S,
}

impl<K, V> PartitionedLockFreeCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_partitions(capacity, default_partitions())
    }

    pub fn with_capacity_and_partitions(capacity: usize, partitions: usize) -> Self {
        Self::with_hasher_capacity_and_partitions(FastBuildHasher::default(), capacity, partitions)
    }
}

impl<K, V, S> PartitionedLockFreeCache<K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn with_hasher_capacity_and_partitions(
        hash_builder: S,
        capacity: usize,
        partitions: usize,
    ) -> Self {
        let partitions = partitions.max(1).next_power_of_two();
        let per_shard = capacity.max(partitions).div_ceil(partitions);
        let shards = (0..partitions)
            .map(|_| {
                CachePadded::new(ConcurrentMap::with_hasher_and_capacity(
                    hash_builder.clone(),
                    per_shard,
                ))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            shard_mask: partitions - 1,
            hash_builder,
        }
    }
}

impl<K, V> Default for PartitionedLockFreeCache<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> PartitionedLockFreeCache<K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        self.hash_builder.hash_one(key)
    }

    #[inline]
    fn shard_idx(&self, hash: u64) -> usize {
        (hash.rotate_right(7) as usize) & self.shard_mask
    }

    #[inline]
    fn shard_for_key(&self, key: &K) -> &ConcurrentMap<K, V, S> {
        let hash = self.hash_key(key);
        &self.shards[self.shard_idx(hash)]
    }

    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        let shards = self
            .shards
            .iter()
            .map(|shard| shard.pin())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Pinned {
            cache: self,
            shards,
        }
    }

    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.shard_for_key(key).pin().get(key).map(f)
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        match self.try_insert(key, value) {
            InsertOutcome::Inserted | InsertOutcome::Replaced => true,
            InsertOutcome::Full => false,
        }
    }

    pub fn try_insert(&self, key: K, value: V) -> InsertOutcome {
        self.shard_for_key(&key).pin().insert(key, value)
    }

    pub fn remove(&self, key: &K) -> bool {
        self.shard_for_key(key).pin().remove(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn partition_count(&self) -> usize {
        self.shards.len()
    }
}

pub struct Pinned<'g, K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    cache: &'g PartitionedLockFreeCache<K, V, S>,
    shards: Box<[crate::concurrent_map::Pinned<'g, K, V, S>]>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    pub fn get(&self, key: &K) -> Option<&V> {
        let hash = self.cache.hash_key(key);
        self.shards[self.cache.shard_idx(hash)].get(key)
    }

    pub fn insert(&self, key: K, value: V) -> bool {
        match self.try_insert(key, value) {
            InsertOutcome::Inserted | InsertOutcome::Replaced => true,
            InsertOutcome::Full => false,
        }
    }

    pub fn try_insert(&self, key: K, value: V) -> InsertOutcome {
        let hash = self.cache.hash_key(&key);
        self.shards[self.cache.shard_idx(hash)].insert(key, value)
    }

    pub fn remove(&self, key: &K) -> bool {
        let hash = self.cache.hash_key(key);
        self.shards[self.cache.shard_idx(hash)].remove(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn default_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .next_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let cache = PartitionedLockFreeCache::<u64, u64>::with_capacity_and_partitions(128, 4);
        assert_eq!(cache.partition_count(), 4);
        assert!(cache.insert(1, 10));
        assert_eq!(cache.read_with(&1, |value| *value), Some(10));

        let pinned = cache.pin();
        assert!(pinned.insert(2, 20));
        assert_eq!(pinned.get(&2).copied(), Some(20));
        assert!(pinned.remove(&1));
        assert!(pinned.get(&1).is_none());
    }

    #[test]
    fn many_inserts_round_trip() {
        let cache = PartitionedLockFreeCache::<u64, u64>::with_capacity_and_partitions(4096, 8);
        let pinned = cache.pin();
        for key in 0..2048u64 {
            assert!(pinned.insert(key, key.wrapping_mul(9)));
        }
        for key in 0..2048u64 {
            assert_eq!(pinned.get(&key).copied(), Some(key.wrapping_mul(9)));
        }
        assert_eq!(pinned.len(), 2048);
    }

    #[test]
    fn try_insert_reports_full() {
        let cache = PartitionedLockFreeCache::<u64, u64>::with_capacity_and_partitions(1, 1);
        let pinned = cache.pin();
        let mut inserted = 0usize;
        let mut full = 0usize;
        for key in 0..32u64 {
            match pinned.try_insert(key, key) {
                InsertOutcome::Inserted => inserted += 1,
                InsertOutcome::Replaced => unreachable!(),
                InsertOutcome::Full => full += 1,
            }
        }

        assert!(inserted > 0);
        assert!(full > 0, "expected at least one full-capacity insert");
        assert_eq!(pinned.len(), inserted);
    }
}
