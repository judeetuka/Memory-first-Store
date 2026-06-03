//! Fixed-capacity bucketed index for `K -> u64` handles.
//!
//! Clean-room response to SCC's write-path lesson: store index entries inline
//! inside fixed-size buckets instead of allocating a boxed hash-table entry per
//! key. This prototype is intentionally conservative and safe: each bucket has
//! its own `parking_lot::RwLock`, and each bucket stores up to 32 `(K, handle)`
//! pairs inline.
//!
//! This is not a replacement for [`mfs_core::concurrent_map::ConcurrentMap`] yet.
//! It is a candidate index layer for dense/slot write-behind variants where
//! write speed matters more than fully lock-free reads.

use mfs_core::FastBuildHasher;
use parking_lot::{Mutex, RwLock};
use std::array;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicUsize, Ordering};

const BUCKET_LEN: usize = 32;
const H2_MASK: u8 = 0x7F;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketedInsertOutcome {
    Inserted,
    Replaced,
    Full,
}

struct BucketEntry<K> {
    key: K,
    h2: u8,
    handle: u64,
}

enum BucketSlot<K> {
    Empty,
    Tombstone,
    Occupied(BucketEntry<K>),
}

struct BucketInner<K> {
    entries: [BucketSlot<K>; BUCKET_LEN],
}

impl<K> BucketInner<K> {
    fn new() -> Self {
        Self {
            entries: array::from_fn(|_| BucketSlot::Empty),
        }
    }
}

struct Bucket<K> {
    inner: RwLock<BucketInner<K>>,
}

impl<K> Bucket<K> {
    fn new() -> Self {
        Self {
            inner: RwLock::new(BucketInner::new()),
        }
    }
}

pub struct BucketedIndex<K, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    buckets: Box<[Bucket<K>]>,
    bucket_mask: usize,
    probe_limit: usize,
    len: AtomicUsize,
    hash_builder: S,
    tombstone_reuse_locks: Box<[Mutex<()>]>,
}

impl<K> BucketedIndex<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }
}

impl<K, S> BucketedIndex<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: BuildHasher,
{
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        let target_slots = capacity.max(BUCKET_LEN).saturating_mul(4) / 3;
        let bucket_count = target_slots.div_ceil(BUCKET_LEN).next_power_of_two().max(1);
        let buckets = (0..bucket_count).map(|_| Bucket::new()).collect::<Vec<_>>();
        let tombstone_reuse_locks = (0..bucket_count)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>();
        Self {
            buckets: buckets.into_boxed_slice(),
            bucket_mask: bucket_count - 1,
            probe_limit: probe_limit(bucket_count),
            len: AtomicUsize::new(0),
            hash_builder,
            tombstone_reuse_locks: tombstone_reuse_locks.into_boxed_slice(),
        }
    }

    #[inline]
    fn h1_h2(&self, key: &K) -> (usize, u8) {
        let h = self.hash_builder.hash_one(key);
        let h1 = (h as usize) & self.bucket_mask;
        let h2 = ((h >> 57) as u8) & H2_MASK;
        (h1, h2)
    }

    #[inline]
    fn tombstone_reuse_lock_idx(&self, bucket_idx: usize) -> usize {
        bucket_idx & (self.tombstone_reuse_locks.len() - 1)
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len() * BUCKET_LEN
    }

    pub fn get(&self, key: &K) -> Option<u64> {
        let (h1, h2) = self.h1_h2(key);
        let mut bucket_idx = h1;
        let mut len = 0usize;
        loop {
            let bucket = self.buckets[bucket_idx].inner.read();
            let mut has_empty = false;
            for entry in &bucket.entries {
                match entry {
                    BucketSlot::Occupied(entry) if entry.h2 == h2 && entry.key == *key => {
                        return Some(entry.handle);
                    }
                    BucketSlot::Empty => has_empty = true,
                    _ => {}
                }
            }
            if has_empty {
                return None;
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            bucket_idx = (bucket_idx + len) & self.bucket_mask;
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn insert_returning_old(
        &self,
        key: K,
        handle: u64,
    ) -> (BucketedInsertOutcome, Option<u64>) {
        let (h1, h2) = self.h1_h2(&key);
        let mut bucket_idx = h1;
        let mut len = 0usize;
        let mut first_tombstone = None;
        loop {
            let mut bucket = self.buckets[bucket_idx].inner.write();
            let mut first_empty = None;
            for (idx, entry) in bucket.entries.iter_mut().enumerate() {
                match entry {
                    BucketSlot::Occupied(existing) if existing.h2 == h2 && existing.key == key => {
                        let old = existing.handle;
                        existing.handle = handle;
                        return (BucketedInsertOutcome::Replaced, Some(old));
                    }
                    BucketSlot::Tombstone if first_tombstone.is_none() => {
                        first_tombstone = Some((bucket_idx, idx))
                    }
                    BucketSlot::Empty if first_empty.is_none() => first_empty = Some(idx),
                    _ => {}
                }
            }
            if let Some(idx) = first_empty {
                bucket.entries[idx] = BucketSlot::Occupied(BucketEntry { key, h2, handle });
                self.len.fetch_add(1, Ordering::Relaxed);
                return (BucketedInsertOutcome::Inserted, None);
            }
            len += 1;
            if len > self.probe_limit {
                if let Some(tombstone) = first_tombstone {
                    drop(bucket);
                    return self.insert_tombstone_slow(key, h2, handle, tombstone);
                }
                return (BucketedInsertOutcome::Full, None);
            }
            bucket_idx = (bucket_idx + len) & self.bucket_mask;
        }
    }

    fn insert_tombstone_slow(
        &self,
        key: K,
        h2: u8,
        handle: u64,
        mut candidate: (usize, usize),
    ) -> (BucketedInsertOutcome, Option<u64>) {
        loop {
            let reuse_lock_idx = self.tombstone_reuse_lock_idx(candidate.0);
            let _reuse = self.tombstone_reuse_locks[reuse_lock_idx].lock();
            let (h1, _) = self.h1_h2(&key);
            let mut bucket_idx = h1;
            let mut len = 0usize;
            let mut first_tombstone = None;
            let mut first_locked_tombstone = None;

            loop {
                let mut bucket = self.buckets[bucket_idx].inner.write();
                let mut first_empty = None;
                for (idx, entry) in bucket.entries.iter_mut().enumerate() {
                    match entry {
                        BucketSlot::Occupied(existing)
                            if existing.h2 == h2 && existing.key == key =>
                        {
                            let old = existing.handle;
                            existing.handle = handle;
                            return (BucketedInsertOutcome::Replaced, Some(old));
                        }
                        BucketSlot::Tombstone => {
                            let location = (bucket_idx, idx);
                            if first_tombstone.is_none() {
                                first_tombstone = Some(location);
                            }
                            if first_locked_tombstone.is_none()
                                && self.tombstone_reuse_lock_idx(bucket_idx) == reuse_lock_idx
                            {
                                first_locked_tombstone = Some(location);
                            }
                        }
                        BucketSlot::Empty if first_empty.is_none() => first_empty = Some(idx),
                        _ => {}
                    }
                }

                if let Some(empty_idx) = first_empty {
                    if let Some(location) = first_locked_tombstone {
                        drop(bucket);
                        self.insert_at(location, key, h2, handle);
                    } else {
                        bucket.entries[empty_idx] =
                            BucketSlot::Occupied(BucketEntry { key, h2, handle });
                    }
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return (BucketedInsertOutcome::Inserted, None);
                }

                len += 1;
                if len > self.probe_limit {
                    if let Some(location) = first_locked_tombstone {
                        drop(bucket);
                        self.insert_at(location, key, h2, handle);
                        self.len.fetch_add(1, Ordering::Relaxed);
                        return (BucketedInsertOutcome::Inserted, None);
                    }
                    if let Some(location) = first_tombstone {
                        candidate = location;
                        break;
                    }
                    return (BucketedInsertOutcome::Full, None);
                }
                bucket_idx = (bucket_idx + len) & self.bucket_mask;
            }
        }
    }

    fn insert_at(&self, location: (usize, usize), key: K, h2: u8, handle: u64) {
        let (bucket_idx, slot_idx) = location;
        let mut bucket = self.buckets[bucket_idx].inner.write();
        match bucket.entries[slot_idx] {
            BucketSlot::Empty | BucketSlot::Tombstone => {
                bucket.entries[slot_idx] = BucketSlot::Occupied(BucketEntry { key, h2, handle });
            }
            BucketSlot::Occupied(_) => unreachable!("mutation lock preserves insertion target"),
        }
    }

    pub fn remove(&self, key: &K) -> Option<u64> {
        let (h1, h2) = self.h1_h2(key);
        let mut bucket_idx = h1;
        let mut len = 0usize;
        loop {
            let mut bucket = self.buckets[bucket_idx].inner.write();
            let mut has_empty = false;
            for entry in &mut bucket.entries {
                match entry {
                    BucketSlot::Occupied(existing) if existing.h2 == h2 && existing.key == *key => {
                        let old = existing.handle;
                        *entry = BucketSlot::Tombstone;
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        return Some(old);
                    }
                    BucketSlot::Empty => has_empty = true,
                    _ => {}
                }
            }
            if has_empty {
                return None;
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            bucket_idx = (bucket_idx + len) & self.bucket_mask;
        }
    }

    pub fn remove_if_value(&self, key: &K, expected: &u64) -> bool {
        let (h1, h2) = self.h1_h2(key);
        let mut bucket_idx = h1;
        let mut len = 0usize;
        loop {
            let mut bucket = self.buckets[bucket_idx].inner.write();
            let mut has_empty = false;
            for entry in &mut bucket.entries {
                match entry {
                    BucketSlot::Occupied(existing) if existing.h2 == h2 && existing.key == *key => {
                        if existing.handle != *expected {
                            return false;
                        }
                        *entry = BucketSlot::Tombstone;
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        return true;
                    }
                    BucketSlot::Empty => has_empty = true,
                    _ => {}
                }
            }
            if has_empty {
                return false;
            }
            len += 1;
            if len > self.probe_limit {
                return false;
            }
            bucket_idx = (bucket_idx + len) & self.bucket_mask;
        }
    }
}

#[inline]
fn probe_limit(bucket_count: usize) -> usize {
    let log2 = (usize::BITS as usize)
        .saturating_sub(bucket_count.leading_zeros() as usize)
        .saturating_sub(1);
    5 * log2.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;
    use std::sync::Arc;
    use std::thread;

    #[derive(Clone, Default)]
    struct ZeroBuildHasher;

    #[derive(Clone, Default)]
    struct IdentityBuildHasher;

    struct ZeroHasher;
    struct IdentityHasher(u64);

    impl BuildHasher for ZeroBuildHasher {
        type Hasher = ZeroHasher;

        fn build_hasher(&self) -> Self::Hasher {
            ZeroHasher
        }
    }

    impl Hasher for ZeroHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    impl BuildHasher for IdentityBuildHasher {
        type Hasher = IdentityHasher;

        fn build_hasher(&self) -> Self::Hasher {
            IdentityHasher(0)
        }
    }

    impl Hasher for IdentityHasher {
        fn finish(&self) -> u64 {
            self.0
        }

        fn write(&mut self, bytes: &[u8]) {
            let mut raw = [0u8; 8];
            let len = bytes.len().min(raw.len());
            raw[..len].copy_from_slice(&bytes[..len]);
            self.0 = u64::from_ne_bytes(raw);
        }

        fn write_u64(&mut self, i: u64) {
            self.0 = i;
        }
    }

    #[test]
    fn insert_get_replace_remove() {
        let idx = BucketedIndex::<u64>::with_capacity(64);
        assert_eq!(
            idx.insert_returning_old(1, 10),
            (BucketedInsertOutcome::Inserted, None)
        );
        assert_eq!(idx.get(&1), Some(10));
        assert_eq!(
            idx.insert_returning_old(1, 20),
            (BucketedInsertOutcome::Replaced, Some(10))
        );
        assert_eq!(idx.get(&1), Some(20));
        assert_eq!(idx.remove(&1), Some(20));
        assert_eq!(idx.get(&1), None);
    }

    #[test]
    fn remove_if_value_checks_handle() {
        let idx = BucketedIndex::<u64>::with_capacity(64);
        idx.insert_returning_old(1, 10);
        assert!(!idx.remove_if_value(&1, &99));
        assert_eq!(idx.get(&1), Some(10));
        assert!(idx.remove_if_value(&1, &10));
        assert_eq!(idx.get(&1), None);
    }

    #[test]
    fn tombstone_does_not_break_overflow_probe_chain() {
        let idx = BucketedIndex::<u64, ZeroBuildHasher>::with_hasher_and_capacity(
            ZeroBuildHasher,
            BUCKET_LEN,
        );

        for key in 0..=BUCKET_LEN as u64 {
            assert_eq!(
                idx.insert_returning_old(key, key * 10),
                (BucketedInsertOutcome::Inserted, None)
            );
        }

        let overflow_key = BUCKET_LEN as u64;
        assert_eq!(idx.get(&overflow_key), Some(overflow_key * 10));
        assert_eq!(idx.remove(&0), Some(0));
        assert_eq!(idx.get(&overflow_key), Some(overflow_key * 10));

        assert!(idx.remove_if_value(&1, &10));
        assert_eq!(idx.get(&overflow_key), Some(overflow_key * 10));
    }

    #[test]
    fn tombstone_saturated_probe_chain_accepts_reinsert() {
        let idx = BucketedIndex::<u64, ZeroBuildHasher>::with_hasher_and_capacity(
            ZeroBuildHasher,
            BUCKET_LEN,
        );
        let live_capacity = BUCKET_LEN as u64 * 2;

        for key in 0..live_capacity {
            assert_eq!(
                idx.insert_returning_old(key, key * 10),
                (BucketedInsertOutcome::Inserted, None)
            );
        }
        assert_eq!(idx.len(), live_capacity as usize);

        assert_eq!(idx.remove(&0), Some(0));
        assert!(idx.remove_if_value(&(BUCKET_LEN as u64), &(BUCKET_LEN as u64 * 10)));
        assert_eq!(idx.len(), live_capacity as usize - 2);

        assert_eq!(
            idx.insert_returning_old(10_000, 55),
            (BucketedInsertOutcome::Inserted, None)
        );
        assert_eq!(idx.get(&10_000), Some(55));
    }

    #[test]
    fn tombstone_reuse_locks_are_bucket_scoped() {
        let idx = BucketedIndex::<u64, IdentityBuildHasher>::with_hasher_and_capacity(
            IdentityBuildHasher,
            BUCKET_LEN,
        );
        assert_eq!(idx.tombstone_reuse_locks.len(), idx.buckets.len());
        assert!(idx.tombstone_reuse_locks.len() > 1);

        for key in 0..idx.capacity() as u64 {
            assert_eq!(
                idx.insert_returning_old(key, key * 10),
                (BucketedInsertOutcome::Inserted, None)
            );
        }

        assert_eq!(idx.remove(&1), Some(10));
        let _unrelated_reuse_lock = idx.tombstone_reuse_locks[0].lock();
        assert_eq!(
            idx.insert_returning_old(65, 650),
            (BucketedInsertOutcome::Inserted, None)
        );
        assert_eq!(idx.get(&65), Some(650));
    }

    #[test]
    fn concurrent_saturated_tombstone_reuse_remains_correct() {
        const THREADS: usize = 16;
        let idx = Arc::new(
            BucketedIndex::<u64, ZeroBuildHasher>::with_hasher_and_capacity(
                ZeroBuildHasher,
                BUCKET_LEN,
            ),
        );
        let live_capacity = idx.capacity() as u64;

        for key in 0..live_capacity {
            assert_eq!(
                idx.insert_returning_old(key, key * 10),
                (BucketedInsertOutcome::Inserted, None)
            );
        }

        for key in 0..THREADS as u64 {
            assert_eq!(idx.remove(&key), Some(key * 10));
        }
        assert_eq!(idx.len(), live_capacity as usize - THREADS);

        let mut handles = Vec::new();
        for t in 0..THREADS as u64 {
            let idx = Arc::clone(&idx);
            handles.push(thread::spawn(move || {
                let key = 10_000 + t;
                assert_eq!(
                    idx.insert_returning_old(key, key * 10),
                    (BucketedInsertOutcome::Inserted, None)
                );
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(idx.len(), live_capacity as usize);

        for key in 0..THREADS as u64 {
            assert_eq!(idx.get(&key), None);
            let inserted = 10_000 + key;
            assert_eq!(idx.get(&inserted), Some(inserted * 10));
        }
    }

    #[test]
    fn string_keys_work() {
        let idx = BucketedIndex::<String>::with_capacity(64);
        idx.insert_returning_old("alpha".to_string(), 7);
        assert_eq!(idx.get(&"alpha".to_string()), Some(7));
    }

    #[test]
    fn concurrent_distinct_inserts() {
        let idx = Arc::new(BucketedIndex::<u64>::with_capacity(4096));
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let idx = Arc::clone(&idx);
            handles.push(thread::spawn(move || {
                let base = t * 256;
                for k in base..base + 256 {
                    idx.insert_returning_old(k, k * 3);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for k in 0..2048u64 {
            assert_eq!(idx.get(&k), Some(k * 3));
        }
    }
}
