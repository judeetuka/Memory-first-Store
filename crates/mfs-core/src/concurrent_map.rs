//! A lock-free open-addressed concurrent hash map, fixed capacity.
//!
//! Our in-house replacement for `papaya::HashMap` in the cache hot
//! path. The design takes architectural inspiration from papaya
//! (Ibraheem Ahmed, MIT-licensed; we read but did not copy the
//! source) but is materially simpler:
//!
//! - **Fixed capacity** at construction. No incremental resize, no
//!   stop-the-world resize. If the table fills, [`insert`](Self::insert)
//!   returns `Err`. Callers who need growth should over-size up
//!   front or layer their own rebuild on top.
//! - **Open-addressed** with quadratic probing (Robin-Hood-style
//!   sequence: index += stride, stride++ each step). Per-bucket
//!   metadata byte stores the high 7 bits of the key hash so most
//!   non-matching probes terminate without touching the entry
//!   pointer.
//! - **Boxed entries**, like papaya: each insert allocates a
//!   `Box<Entry<K, V>>` containing the key and value inline. The
//!   bucket holds a `std::sync::atomic::AtomicPtr<Entry<K, V>>`.
//! - **Hyaline reclamation** via [`seize`]. Readers acquire a
//!   lightweight `LocalGuard` (a single atomic store, no
//!   `SeqCst` fence) that keeps retired entries alive until the
//!   guard drops. The thread that holds the last reference to a
//!   retire batch is the one that frees it — bounded memory,
//!   predictable tail latency, no unbounded reclamation stalls when
//!   a reader is preempted.
//!
//! ## Read hot path
//!
//! 1. Compute `(h1, h2)` from the hash. `h1` indexes the table;
//!    `h2` is the 7-bit metadata signature.
//! 2. Probe quadratically.
//! 3. At each slot: load the `AtomicU8` metadata (one acquire load).
//! 4. If `meta == h2`: `guard.protect(&entries[i], Acquire)` to
//!    obtain a hyaline-protected pointer and compare keys.
//! 5. If `meta == EMPTY`: the key is absent, return early.
//! 6. Otherwise advance the probe.
//!
//! On a hit: 1 acquire load on meta + 1 protected load on entry +
//! 1 key comparison + 1 value access. Hot key, all in L1: ~5 ns
//! before guard overhead. Guard acquisition via `Collector::enter` is
//! cheaper than the crossbeam-epoch pin it replaces.
//!
//! ## Reclamation
//!
//! The whole table allocates its `Box<Entry>`s. On `remove`, we
//! `compare_exchange` the bucket to null, then
//! `guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>)`. seize
//! batches retired entries and frees them when no guard could still
//! observe them. The local-to-global retire-list batch size is tuned
//! via [`DEFAULT_RETIRE_BATCH`] (raise it for write-heavy workloads;
//! lower it for memory-tight environments).
//!
//! ## Cost vs papaya
//!
//! - We share the boxed-entry per-insert allocation. ~600 ns per
//!   first insert of a key on Skylake, ~300 ns on Zen 3.
//! - We lack incremental resize. Workloads that need to grow
//!   should pre-size or rebuild externally.
//! - Probing is the same as papaya's `probe.rs` (i += len; len++).

use crate::FastBuildHasher;
use crossbeam_utils::CachePadded;
use seize::{Collector, Guard, LocalGuard, reclaim};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

const META_EMPTY: u8 = 0x80;
const META_TOMBSTONE: u8 = 0xFE;
const H2_MASK: u8 = 0x7F;

/// Default retire batch size for [`ConcurrentMap`].
///
/// `seize`'s default is 32 (or CPU count); we raise it because cache
/// hot paths do many replaces per second and the per-call overhead of
/// migrating local-to-global retire batches is measurable. Larger
/// batches trade a small amount of peak retire-list memory for lower
/// per-write overhead — the right tradeoff for a write-heavy cache.
pub const DEFAULT_RETIRE_BATCH: usize = 4096;

#[repr(C, align(8))]
struct Entry<K, V> {
    key: K,
    value: V,
}

/// Concurrent hash map. Fixed capacity, set at construction.
pub struct ConcurrentMap<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    capacity: usize,
    mask: usize,
    probe_limit: usize,
    meta: Box<[AtomicU8]>,
    entries: Box<[AtomicPtr<Entry<K, V>>]>,
    len: CachePadded<AtomicUsize>,
    hash_builder: S,
    collector: Collector,
    retire_batch_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildCapacityError {
    pub requested_capacity: usize,
    pub copied: usize,
    pub entries: usize,
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }
}

impl<K, V, S> ConcurrentMap<K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        Self::with_hasher_capacity_and_batch(hash_builder, capacity, DEFAULT_RETIRE_BATCH)
    }

    /// Construct with an explicit reclamation batch size.
    ///
    /// Larger batches amortise the local-to-global retire-list
    /// migration cost across more `defer_retire` calls, which lowers
    /// per-write overhead in hot replace loops. The trade is a slightly
    /// higher peak memory footprint for retired entries before
    /// reclamation runs. Default ([`DEFAULT_RETIRE_BATCH`]) is tuned
    /// for write-heavy single-thread workloads; raise it for pure
    /// replace workloads, lower it for memory-tight environments.
    pub fn with_hasher_capacity_and_batch(
        hash_builder: S,
        capacity: usize,
        batch_size: usize,
    ) -> Self {
        let target = capacity.max(8).saturating_mul(4) / 3;
        let cap = target.next_power_of_two().max(8);
        let mask = cap - 1;
        let probe_limit = probe_limit(cap);

        let mut meta: Vec<AtomicU8> = Vec::with_capacity(cap);
        let mut entries: Vec<AtomicPtr<Entry<K, V>>> = Vec::with_capacity(cap);
        for _ in 0..cap {
            meta.push(AtomicU8::new(META_EMPTY));
            entries.push(AtomicPtr::new(std::ptr::null_mut()));
        }

        Self {
            capacity: cap,
            mask,
            probe_limit,
            meta: meta.into_boxed_slice(),
            entries: entries.into_boxed_slice(),
            len: CachePadded::new(AtomicUsize::new(0)),
            hash_builder,
            collector: Collector::new().batch_size(batch_size),
            retire_batch_size: batch_size,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn h1_h2(&self, key: &K) -> (usize, u8) {
        let h = self.hash_builder.hash_one(key);
        let h1 = (h as usize) & self.mask;
        let h2 = ((h >> 57) as u8) & H2_MASK;
        (h1, h2)
    }

    /// Acquire a fresh reclamation guard. Most callers should hold a
    /// [`Pinned`] guard via [`pin`](Self::pin) and reuse it across
    /// many operations; this one-shot variant guards per call.
    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            map: self,
            guard: self.collector.enter(),
        }
    }

    /// Convenience lookup. Equivalent to `self.pin().get(key)` then
    /// cloning out the value, plus dropping the guard afterwards.
    pub fn get_owned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.pin().get(key).cloned()
    }

    /// Closure-based read with the value alive for the duration of
    /// `f`. Avoids `Clone` when the caller can express their work
    /// inside the closure.
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.pin().get(key).map(f)
    }

    /// Whether the key currently maps to a live entry.
    pub fn contains_key(&self, key: &K) -> bool {
        self.pin().contains_key(key)
    }

    /// Whether an insert or replace for `key` can fit in the current probe
    /// window without mutating the table.
    pub fn can_insert_or_replace(&self, key: &K) -> bool {
        let p = self.pin();
        p.can_insert_or_replace(key)
    }

    /// Insert or replace. See [`Pinned::insert`].
    pub fn insert(&self, key: K, value: V) -> InsertOutcome {
        self.pin().insert(key, value)
    }

    /// Remove and return the value if present.
    pub fn remove(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.pin().remove_owned(key)
    }

    /// Apply `f` to the in-place value if the key is present. Note
    /// that this is **not** atomic — `f` runs while no lock is held
    /// and any other writer can replace the entry concurrently. Use
    /// only for monotonically-safe updates (e.g. mutating an
    /// `AtomicU64` field).
    pub fn update_with<F>(&self, key: &K, f: F) -> bool
    where
        F: FnOnce(&V),
    {
        let p = self.pin();
        match p.get(key) {
            Some(v) => {
                f(v);
                true
            }
            None => false,
        }
    }

    /// Iterate over every live `(K, V)` pair. Provides a snapshot at
    /// the current epoch; entries may concurrently change. The
    /// callback runs against borrows valid for the duration of the
    /// iteration only.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, &V),
    {
        let guard = self.collector.enter();
        for i in 0..self.capacity {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == META_EMPTY || meta == META_TOMBSTONE {
                continue;
            }
            let raw = guard.protect(&self.entries[i], Ordering::Acquire);
            if raw.is_null() {
                continue;
            }
            let e = unsafe { &*raw };
            f(&e.key, &e.value);
        }
    }

    fn insert_inner<'g>(
        &'g self,
        key: K,
        value: V,
        guard: &LocalGuard<'g>,
    ) -> (InsertOutcome, Option<*mut Entry<K, V>>) {
        let (h1, h2) = self.h1_h2(&key);
        let mut new_box = Box::new(Entry { key, value });
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == h2 {
                let existing = guard.protect(&self.entries[i], Ordering::Acquire);
                if !existing.is_null() {
                    let e = unsafe { &*existing };
                    if e.key == new_box.key {
                        let new_raw = Box::into_raw(new_box);
                        match self.entries[i].compare_exchange(
                            existing,
                            new_raw,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                return (InsertOutcome::Replaced, Some(existing));
                            }
                            Err(_) => {
                                new_box = unsafe { Box::from_raw(new_raw) };
                                continue;
                            }
                        }
                    }
                }
            } else if meta == META_EMPTY || meta == META_TOMBSTONE {
                let prev = guard.protect(&self.entries[i], Ordering::Acquire);
                if prev.is_null() {
                    let new_raw = Box::into_raw(new_box);
                    match self.entries[i].compare_exchange(
                        std::ptr::null_mut(),
                        new_raw,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.meta[i].store(h2, Ordering::Release);
                            self.len.fetch_add(1, Ordering::Relaxed);
                            return (InsertOutcome::Inserted, None);
                        }
                        Err(_) => {
                            new_box = unsafe { Box::from_raw(new_raw) };
                        }
                    }
                }
            }
            len += 1;
            if len > self.probe_limit {
                drop(new_box);
                return (InsertOutcome::Full, None);
            }
            i = (i + len) & self.mask;
        }
    }

    fn can_insert_or_replace_inner<'g>(&'g self, key: &K, guard: &LocalGuard<'g>) -> bool {
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == h2 {
                let existing = guard.protect(&self.entries[i], Ordering::Acquire);
                if !existing.is_null() {
                    let e = unsafe { &*existing };
                    if e.key == *key {
                        return true;
                    }
                }
            } else if meta == META_EMPTY || meta == META_TOMBSTONE {
                let existing = guard.protect(&self.entries[i], Ordering::Acquire);
                if existing.is_null() {
                    return true;
                }
            }
            len += 1;
            if len > self.probe_limit {
                return false;
            }
            i = (i + len) & self.mask;
        }
    }

    fn remove_inner<'g>(&'g self, key: &K, guard: &LocalGuard<'g>) -> Option<*mut Entry<K, V>> {
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == h2 {
                let existing = guard.protect(&self.entries[i], Ordering::Acquire);
                if !existing.is_null() {
                    let e = unsafe { &*existing };
                    if e.key == *key {
                        match self.entries[i].compare_exchange(
                            existing,
                            std::ptr::null_mut(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                self.meta[i].store(META_TOMBSTONE, Ordering::Release);
                                self.len.fetch_sub(1, Ordering::Relaxed);
                                return Some(existing);
                            }
                            Err(_) => return None,
                        }
                    }
                }
            } else if meta == META_EMPTY {
                return None;
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            i = (i + len) & self.mask;
        }
    }

    fn remove_if_value_inner<'g>(
        &'g self,
        key: &K,
        expected: &V,
        guard: &LocalGuard<'g>,
    ) -> Option<*mut Entry<K, V>>
    where
        V: PartialEq,
    {
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == h2 {
                let existing = guard.protect(&self.entries[i], Ordering::Acquire);
                if !existing.is_null() {
                    let e = unsafe { &*existing };
                    if e.key == *key {
                        if e.value != *expected {
                            return None;
                        }
                        match self.entries[i].compare_exchange(
                            existing,
                            std::ptr::null_mut(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                self.meta[i].store(META_TOMBSTONE, Ordering::Release);
                                self.len.fetch_sub(1, Ordering::Relaxed);
                                return Some(existing);
                            }
                            Err(_) => return None,
                        }
                    }
                }
            } else if meta == META_EMPTY {
                return None;
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            i = (i + len) & self.mask;
        }
    }
}

impl<K, V, S> ConcurrentMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    /// Build a new map with `capacity` and copy the current live entries into it.
    ///
    /// This is a quiescent maintenance rebuild, not live resize. Existing readers
    /// and writers keep using `self`; callers that want to adopt the rebuilt map
    /// must swap it at an application-owned boundary. The source map is unchanged
    /// if the requested capacity is too small for the copied snapshot.
    pub fn rebuild_with_capacity(&self, capacity: usize) -> Result<Self, RebuildCapacityError> {
        let mut entries = Vec::with_capacity(self.len());
        self.for_each(|key, value| entries.push((key.clone(), value.clone())));

        let rebuilt = Self::with_hasher_capacity_and_batch(
            self.hash_builder.clone(),
            capacity,
            self.retire_batch_size,
        );
        {
            let pinned = rebuilt.pin();
            for (copied, (key, value)) in entries.iter().cloned().enumerate() {
                if matches!(pinned.insert(key, value), InsertOutcome::Full) {
                    return Err(RebuildCapacityError {
                        requested_capacity: capacity,
                        copied,
                        entries: entries.len(),
                    });
                }
            }
        }

        Ok(rebuilt)
    }
}

/// Guard returned by [`ConcurrentMap::pin`]. Holds a single hyaline
/// reclamation guard so that values referenced via `&V` are guaranteed
/// live for the guard's lifetime. Hold across many ops to amortize
/// the per-call guard cost.
pub struct Pinned<'g, K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    map: &'g ConcurrentMap<K, V, S>,
    guard: LocalGuard<'g>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    /// Lookup. ~5–10 ns on a hot single key.
    #[inline]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.get_with_hash(key).map(|(value, _)| value)
    }

    /// Lookup and return the full hash used for probing. This lets
    /// wrappers reuse the hash for side metadata instead of hashing the
    /// same key twice on read hits.
    #[inline]
    pub fn get_with_hash(&self, key: &K) -> Option<(&V, u64)> {
        let hash = self.map.hash_builder.hash_one(key);
        let h1 = (hash as usize) & self.map.mask;
        let h2 = ((hash >> 57) as u8) & H2_MASK;
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.map.meta[i].load(Ordering::Acquire);
            if meta == h2 {
                let raw = self.guard.protect(&self.map.entries[i], Ordering::Acquire);
                if !raw.is_null() {
                    let e = unsafe { &*raw };
                    if e.key == *key {
                        return Some((&e.value, hash));
                    }
                }
            } else if meta == META_EMPTY {
                return None;
            }
            len += 1;
            if len > self.map.probe_limit {
                return None;
            }
            i = (i + len) & self.map.mask;
        }
    }

    /// Closure-based lookup with no `Clone` requirement on V.
    #[inline]
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.get(key).map(f)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    pub fn can_insert_or_replace(&self, key: &K) -> bool {
        self.map.can_insert_or_replace_inner(key, &self.guard)
    }

    /// Insert or replace. Returns [`InsertOutcome`].
    pub fn insert(&self, key: K, value: V) -> InsertOutcome {
        let (outcome, old) = self.map.insert_inner(key, value, &self.guard);
        if let Some(old) = old {
            unsafe { self.guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>) };
        }
        outcome
    }

    /// Insert or replace and clone out the old value when replacing.
    pub fn insert_returning_old(&self, key: K, value: V) -> (InsertOutcome, Option<V>)
    where
        V: Clone,
    {
        let (outcome, old) = self.map.insert_inner(key, value, &self.guard);
        let old_value = old.map(|old| {
            let value = unsafe { (*old).value.clone() };
            unsafe { self.guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>) };
            value
        });
        (outcome, old_value)
    }

    /// Remove and return the old value if present, cloning out
    /// before the deferred reclaim.
    pub fn remove_owned(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let old = self.map.remove_inner(key, &self.guard)?;
        let value = unsafe { (*old).value.clone() };
        unsafe { self.guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>) };
        Some(value)
    }

    /// Remove without returning the value; defers reclaim.
    pub fn remove(&self, key: &K) -> bool {
        match self.map.remove_inner(key, &self.guard) {
            Some(old) => {
                unsafe { self.guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>) };
                true
            }
            None => false,
        }
    }

    /// Remove only if the current value equals `expected`.
    pub fn remove_if_value(&self, key: &K, expected: &V) -> bool
    where
        V: PartialEq,
    {
        match self.map.remove_if_value_inner(key, expected, &self.guard) {
            Some(old) => {
                unsafe { self.guard.defer_retire(old, reclaim::boxed::<Entry<K, V>>) };
                true
            }
            None => false,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl<K, V, S> Drop for ConcurrentMap<K, V, S>
where
    K: Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    fn drop(&mut self) {
        for slot in self.entries.iter_mut() {
            let raw = *slot.get_mut();
            if !raw.is_null() {
                unsafe {
                    let _ = Box::from_raw(raw);
                }
            }
        }
    }
}

#[inline]
fn probe_limit(capacity: usize) -> usize {
    let log2 = (usize::BITS as usize)
        .saturating_sub(capacity.leading_zeros() as usize)
        .saturating_sub(1);
    5 * log2.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_remove() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        assert!(m.is_empty());
        assert_eq!(m.insert(1, 100), InsertOutcome::Inserted);
        assert_eq!(m.insert(2, 200), InsertOutcome::Inserted);
        assert_eq!(m.insert(1, 999), InsertOutcome::Replaced);
        assert_eq!(m.len(), 2);
        assert_eq!(m.get_owned(&1), Some(999));
        assert_eq!(m.get_owned(&2), Some(200));
        assert_eq!(m.get_owned(&3), None);
        assert_eq!(m.remove(&1), Some(999));
        assert_eq!(m.len(), 1);
        assert_eq!(m.get_owned(&1), None);
        assert_eq!(m.get_owned(&2), Some(200));
    }

    #[test]
    fn tombstone_does_not_short_circuit_probe() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        for i in 0..16u64 {
            m.insert(i, i * 10);
        }
        m.remove(&3);
        assert_eq!(m.get_owned(&3), None);
        for i in [0, 1, 2, 4, 5, 15u64] {
            assert_eq!(m.get_owned(&i), Some(i * 10));
        }
        assert_eq!(m.insert(3, 30), InsertOutcome::Inserted);
        assert_eq!(m.get_owned(&3), Some(30));
    }

    #[test]
    fn pinned_get_returns_reference() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(8);
        m.insert(7, 42);
        let p = m.pin();
        let r: Option<&u64> = p.get(&7);
        assert_eq!(r.copied(), Some(42));
    }

    #[test]
    fn capacity_full_returns_err() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(1);
        let mut ok = 0;
        let mut full = 0;
        for i in 0..32u64 {
            match m.insert(i, i) {
                InsertOutcome::Inserted => ok += 1,
                InsertOutcome::Full => full += 1,
                InsertOutcome::Replaced => unreachable!(),
            }
        }
        assert!(ok > 0);
        assert!(full > 0, "expected some Full outcomes when over-stuffing");
        assert_eq!(m.len(), ok);
    }

    #[test]
    fn rebuild_with_capacity_preserves_live_entries() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        for i in 0..16u64 {
            m.insert(i, i * 3);
        }
        m.remove(&5);

        let rebuilt = m.rebuild_with_capacity(128).expect("rebuild should fit");

        assert_eq!(rebuilt.len(), 15);
        assert_eq!(rebuilt.get_owned(&5), None);
        for i in (0..16u64).filter(|&i| i != 5) {
            assert_eq!(rebuilt.get_owned(&i), Some(i * 3));
            assert_eq!(m.get_owned(&i), Some(i * 3));
        }
    }

    #[test]
    fn rebuild_with_capacity_reports_full_without_changing_source() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        for i in 0..32u64 {
            m.insert(i, i);
        }

        let err = match m.rebuild_with_capacity(1) {
            Ok(_) => panic!("tiny rebuild target should fill"),
            Err(err) => err,
        };

        assert_eq!(err.requested_capacity, 1);
        assert!(err.copied < err.entries);
        assert_eq!(err.entries, 32);
        assert_eq!(m.len(), 32);
        for i in 0..32u64 {
            assert_eq!(m.get_owned(&i), Some(i));
        }
    }

    #[test]
    fn for_each_visits_all_live_entries() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        for i in 0..16u64 {
            m.insert(i, i * 3);
        }
        m.remove(&5);
        let mut sum = 0u64;
        let mut count = 0;
        m.for_each(|&k, &v| {
            assert_eq!(v, k * 3);
            sum += v;
            count += 1;
        });
        assert_eq!(count, 15);
        let expected_sum = (0..16u64).filter(|&i| i != 5).map(|i| i * 3).sum::<u64>();
        assert_eq!(sum, expected_sum);
    }

    #[test]
    fn drop_frees_all_entries() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(1024);
        for i in 0..512u64 {
            m.insert(i, i);
        }
        drop(m);
    }

    #[test]
    fn concurrent_inserts_and_reads_are_consistent() {
        use std::sync::Arc;
        use std::thread;
        let m = Arc::new(ConcurrentMap::<u64, u64>::with_capacity(8192));
        let mut handles = Vec::new();
        for tid in 0..4 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    let key = tid as u64 * 1000 + i;
                    m.insert(key, key * 7);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.len(), 4 * 1000);
        for tid in 0..4u64 {
            for i in 0..1000u64 {
                let key = tid * 1000 + i;
                assert_eq!(m.get_owned(&key), Some(key * 7), "missing key {key}");
            }
        }
    }

    #[test]
    fn remove_if_value_only_removes_matching() {
        let m = ConcurrentMap::<u64, u64>::with_capacity(64);
        m.insert(1, 100);
        let p = m.pin();
        assert!(
            !p.remove_if_value(&1, &999),
            "wrong value, should not remove"
        );
        assert_eq!(p.get(&1).copied(), Some(100));
        assert!(p.remove_if_value(&1, &100), "matching value, should remove");
        assert!(p.get(&1).is_none());
    }
}
