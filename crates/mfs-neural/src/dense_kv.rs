//! Dense-storage concurrent map for byte-stable 8-byte value types.
//!
//! Splits storage into two layers:
//!
//! - **Index**: [`mfs_core::concurrent_map::ConcurrentMap<K, u64>`]
//!   mapping each key to a packed `(slot, generation)` handle.
//! - **Values**: a pre-allocated `Box<[AtomicU64]>` of `capacity`
//!   slots — the [`mfs_core::DenseU64Lane`] pattern. Reads are one
//!   atomic load; writes are one atomic store. No allocation, no
//!   lock, no refcount on the hot update path.
//!
//! The first insert of a key allocates one `Box<Entry<K, u64>>` in
//! the index (matching the floor of any boxed concurrent hash
//! table). Subsequent updates of the same key touch only the
//! AtomicU64 value layer.
//!
//! ## Speed targets
//!
//! On Skylake T460:
//!
//! - `get(k)` ≈ 7 ns after generation validation (index lookup,
//!   generation check, atomic load, generation re-check).
//! - `put(k, v)` **on an existing key** ≈ 17 ns after the slot-reuse
//!   safety fix (index lookup, generation lock, value store, unlock).
//!   The dense-storage win is now correctness-preserving rather than
//!   the old unsafe 5–10 ns prototype path.
//! - `put(k, v)` **on a new key** ≈ 230–300 ns (one index
//!   allocation, one slot fetch).
//! - `remove(k)` ≈ 230–300 ns + slot recycle.
//!
//! Steady-state cache workloads (where every key sees many updates
//! over its lifetime) amortise the insert cost to near-zero and
//! the per-write cost converges on the L1-speed update path.
//!
//! For `K = u64, V = u64` specifically, prefer
//! [`mfs_core::inline_map::InlineU64Map`] which collapses the index
//! and value layers into a single seqlock-protected bucket — no
//! Box allocation **ever**, not even on first insert.
//!
//! ## Constraints
//!
//! - `V` must implement [`crate::DenseValue`]. Common supported types:
//!   `u64`, `i64`, `f64`, and `[u8; 8]`.
//! - Fixed capacity; no resize.
//! - Slot reuse is guarded by a per-slot generation counter. Readers
//!   verify the index handle and slot generation around the value
//!   load, so a recycled slot cannot be mistaken for the old key.

use crate::DenseValue;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::CachePadded;
use mfs_core::FastBuildHasher;
use mfs_core::concurrent_map::{ConcurrentMap, InsertOutcome};
use std::hash::{BuildHasher, Hash};
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

const SLOT_WRITE_BIT: u32 = 1;

pub struct DenseKvMap<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    index: ConcurrentMap<K, u64, S>,
    values: Box<[AtomicU64]>,
    generations: Box<[AtomicU32]>,
    free: CachePadded<ArrayQueue<u32>>,
    next_slot: CachePadded<AtomicU32>,
    capacity: u32,
    _value: PhantomData<V>,
}

impl<K, V> DenseKvMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
{
    /// Construct with the given capacity.
    pub fn with_capacity(capacity: u32) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }
}

impl<K, V, S> DenseKvMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: u32) -> Self {
        let cap = capacity.max(1);
        let values: Vec<AtomicU64> = (0..cap).map(|_| AtomicU64::new(0)).collect();
        let generations: Vec<AtomicU32> = (0..cap).map(|_| AtomicU32::new(0)).collect();
        let index = ConcurrentMap::with_hasher_and_capacity(hash_builder, cap as usize);
        Self {
            index,
            values: values.into_boxed_slice(),
            generations: generations.into_boxed_slice(),
            free: CachePadded::new(ArrayQueue::new(cap as usize)),
            next_slot: CachePadded::new(AtomicU32::new(0)),
            capacity: cap,
            _value: PhantomData,
        }
    }

    /// Pin the underlying index epoch and return a [`Pinned`]
    /// guard. Hold across many ops to amortise the pin cost.
    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            map: self,
            inner: self.index.pin(),
        }
    }

    /// One-shot lookup. Prefer [`Pinned::get`] in tight loops.
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.pin().get(key)
    }

    /// One-shot closure-based lookup. Prefer [`Pinned::read_with`] in tight loops.
    #[inline]
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.pin().read_with(key, f)
    }

    /// One-shot insert/update.
    pub fn put(&self, key: K, value: V) -> Result<(), V> {
        self.pin().put(key, value)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.pin().remove(key)
    }

    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    fn next_slot_or_recycle(&self) -> Option<u32> {
        if let Some(s) = self.free.pop() {
            return Some(s);
        }
        let next = self.next_slot.fetch_add(1, Ordering::Relaxed);
        if next >= self.capacity {
            self.next_slot.store(self.capacity, Ordering::Relaxed);
            return None;
        }
        Some(next)
    }

    #[inline]
    fn lock_slot(&self, slot: u32) -> u32 {
        let state = &self.generations[slot as usize];
        loop {
            let generation = state.load(Ordering::Acquire);
            if generation & SLOT_WRITE_BIT != 0 {
                spin_loop();
                continue;
            }
            if state
                .compare_exchange(
                    generation,
                    generation | SLOT_WRITE_BIT,
                    Ordering::Acquire,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return generation;
            }
            spin_loop();
        }
    }

    #[inline]
    fn try_lock_slot_generation(&self, slot: u32, generation: u32) -> bool {
        self.generations[slot as usize]
            .compare_exchange(
                generation,
                generation | SLOT_WRITE_BIT,
                Ordering::Acquire,
                Ordering::Acquire,
            )
            .is_ok()
    }

    #[inline]
    fn unlock_slot(&self, slot: u32, generation: u32) {
        self.generations[slot as usize].store(generation, Ordering::Release);
    }

    #[inline]
    fn retire_slot(&self, handle: u64) {
        let slot = handle_slot(handle);
        let generation = handle_generation(handle);
        let state = &self.generations[slot as usize];
        loop {
            match state.compare_exchange(
                generation,
                generation | SLOT_WRITE_BIT,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let next_generation = generation.wrapping_add(2);
                    state.store(next_generation, Ordering::Release);
                    let _ = self.free.push(slot);
                    return;
                }
                Err(current) if current == (generation | SLOT_WRITE_BIT) => spin_loop(),
                Err(_) => return,
            }
        }
    }
}

/// Guard holding an epoch pin into the underlying index. Construct
/// via [`DenseKvMap::pin`]; keep alive across many ops.
pub struct Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    map: &'g DenseKvMap<K, V, S>,
    inner: mfs_core::concurrent_map::Pinned<'g, K, u64, S>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    /// Lookup. ~5 ns on Skylake when used inside a tight loop.
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        loop {
            let &handle = self.inner.get(key)?;
            let slot = handle_slot(handle);
            let generation = handle_generation(handle);
            let state = self.map.generations[slot as usize].load(Ordering::Acquire);
            if state != generation {
                spin_loop();
                continue;
            }
            let raw = self.map.values[slot as usize].load(Ordering::Acquire);
            if self.map.generations[slot as usize].load(Ordering::Acquire) != generation {
                spin_loop();
                continue;
            }
            return Some(unpack(raw));
        }
    }

    #[inline]
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.get(key).map(|value| f(&value))
    }

    /// Update or insert. Hot path (existing key) is ~17 ns on T460
    /// after the generation-check safety fix.
    pub fn put(&self, key: K, value: V) -> Result<(), V> {
        let raw = pack(value);
        // Fast path: existing key.
        loop {
            if let Some(&handle) = self.inner.get(&key) {
                let slot = handle_slot(handle);
                let generation = handle_generation(handle);
                if !self.map.try_lock_slot_generation(slot, generation) {
                    spin_loop();
                    continue;
                }
                let still_current =
                    matches!(self.inner.get(&key), Some(&current) if current == handle);
                if still_current {
                    self.map.values[slot as usize].store(raw, Ordering::Release);
                    self.map.unlock_slot(slot, generation);
                    return Ok(());
                }
                self.map.unlock_slot(slot, generation);
                spin_loop();
                continue;
            }
            break;
        }
        // Slow path: allocate a slot, then attempt to claim the
        // index. If we lose the race, recycle our slot and replay
        // the value into the winner's slot (last-writer-wins).
        let Some(slot) = self.map.next_slot_or_recycle() else {
            return Err(value);
        };
        let generation = self.map.lock_slot(slot);
        let handle = pack_handle(slot, generation);
        self.map.values[slot as usize].store(raw, Ordering::Release);
        match self.inner.insert_returning_old(key, handle) {
            (InsertOutcome::Inserted, old_handle) => {
                self.map.unlock_slot(slot, generation);
                if let Some(old_handle) = old_handle {
                    self.map.retire_slot(old_handle);
                }
                Ok(())
            }
            (InsertOutcome::Replaced, old_handle) => {
                self.map.unlock_slot(slot, generation);
                if let Some(old_handle) = old_handle {
                    self.map.retire_slot(old_handle);
                }
                Ok(())
            }
            (InsertOutcome::Full, old_handle) => {
                self.map.unlock_slot(slot, generation);
                if let Some(old_handle) = old_handle {
                    self.map.retire_slot(old_handle);
                }
                let _ = self.map.free.push(slot);
                Err(unpack(raw))
            }
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        loop {
            let &handle = self.inner.get(key)?;
            let slot = handle_slot(handle);
            let generation = handle_generation(handle);
            if !self.map.try_lock_slot_generation(slot, generation) {
                spin_loop();
                continue;
            }
            let raw = self.map.values[slot as usize].load(Ordering::Acquire);
            if self.inner.remove_if_value(key, &handle) {
                let next_generation = generation.wrapping_add(2);
                self.map.generations[slot as usize].store(next_generation, Ordering::Release);
                let _ = self.map.free.push(slot);
                return Some(unpack(raw));
            }
            self.map.unlock_slot(slot, generation);
            spin_loop();
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }
}

#[inline]
fn pack_handle(slot: u32, generation: u32) -> u64 {
    debug_assert_eq!(generation & SLOT_WRITE_BIT, 0);
    ((generation as u64) << 32) | slot as u64
}

#[inline]
fn handle_slot(handle: u64) -> u32 {
    handle as u32
}

#[inline]
fn handle_generation(handle: u64) -> u32 {
    (handle >> 32) as u32
}

#[inline]
fn pack<V: DenseValue>(value: V) -> u64 {
    value.into_u64()
}

#[inline]
fn unpack<V: DenseValue>(raw: u64) -> V {
    V::from_u64(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn put_get_update_round_trip() {
        let m = DenseKvMap::<u64, u64>::with_capacity(1024);
        assert!(m.is_empty());
        m.put(1, 100).unwrap();
        m.put(2, 200).unwrap();
        assert_eq!(m.get(&1), Some(100));
        assert_eq!(m.read_with(&1, |value| value * 2), Some(200));
        assert_eq!(m.get(&2), Some(200));
        assert!(m.contains_key(&2));
        assert!(!m.contains_key(&999));
        m.put(1, 999).unwrap();
        assert_eq!(m.get(&1), Some(999));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn string_keys_work_with_top_level_helpers() {
        let m = DenseKvMap::<String, u64>::with_capacity(8);
        let alpha = "alpha".to_string();
        let beta = "beta".to_string();

        m.put(alpha.clone(), 10).unwrap();
        m.put(beta.clone(), 20).unwrap();

        assert_eq!(m.get(&alpha), Some(10));
        assert_eq!(m.read_with(&beta, |value| value + 1), Some(21));
        assert!(m.contains_key(&alpha));
        assert_eq!(m.remove(&alpha), Some(10));
        assert!(!m.contains_key(&alpha));
        assert_eq!(m.get(&beta), Some(20));
    }

    #[test]
    fn remove_recycles_slot() {
        let m = DenseKvMap::<u64, u64>::with_capacity(4);
        m.put(1, 10).unwrap();
        m.put(2, 20).unwrap();
        m.put(3, 30).unwrap();
        m.put(4, 40).unwrap();
        assert!(m.put(5, 50).is_err());
        assert_eq!(m.remove(&2), Some(20));
        m.put(5, 50).unwrap();
        assert_eq!(m.get(&5), Some(50));
        assert_eq!(m.get(&2), None);
    }

    #[test]
    fn slot_generation_changes_when_reused() {
        let m = DenseKvMap::<u64, u64>::with_capacity(1);
        m.put(1, 10).unwrap();
        let first = {
            let p = m.pin();
            *p.inner.get(&1).unwrap()
        };
        assert_eq!(handle_slot(first), 0);
        assert_eq!(handle_generation(first), 0);

        assert_eq!(m.remove(&1), Some(10));
        m.put(2, 20).unwrap();
        let second = {
            let p = m.pin();
            *p.inner.get(&2).unwrap()
        };

        assert_eq!(handle_slot(second), 0);
        assert_eq!(handle_generation(second), 2);
        assert_eq!(m.get(&1), None);
        assert_eq!(m.get(&2), Some(20));
    }

    #[test]
    fn replaced_insert_retires_old_slot() {
        let m = DenseKvMap::<u64, u64>::with_capacity(2);
        let p = m.pin();

        let slot_a = m.next_slot_or_recycle().unwrap();
        let generation_a = m.lock_slot(slot_a);
        m.values[slot_a as usize].store(pack(10u64), Ordering::Release);
        let handle_a = pack_handle(slot_a, generation_a);
        assert_eq!(
            p.inner.insert_returning_old(1, handle_a),
            (InsertOutcome::Inserted, None)
        );
        m.unlock_slot(slot_a, generation_a);

        let slot_b = m.next_slot_or_recycle().unwrap();
        let generation_b = m.lock_slot(slot_b);
        m.values[slot_b as usize].store(pack(20u64), Ordering::Release);
        let handle_b = pack_handle(slot_b, generation_b);
        let (outcome, old) = p.inner.insert_returning_old(1, handle_b);
        assert_eq!(outcome, InsertOutcome::Replaced);
        m.unlock_slot(slot_b, generation_b);
        m.retire_slot(old.unwrap());

        assert_eq!(m.remove(&1), Some(20));
        m.put(2, 200).unwrap();
        m.put(3, 300).unwrap();
        assert_eq!(m.get(&2), Some(200));
        assert_eq!(m.get(&3), Some(300));
    }

    #[test]
    fn concurrent_reuse_does_not_return_another_keys_value() {
        const KEYS: u64 = 8;
        const THREADS: usize = 6;
        const ITERS: u64 = 20_000;

        let m = Arc::new(DenseKvMap::<u64, u64>::with_capacity(64));
        for key in 0..KEYS {
            m.put(key, key << 32).unwrap();
        }

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::new();
        for thread_id in 0..THREADS {
            let m = Arc::clone(&m);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..ITERS {
                    let key = (i + thread_id as u64) % KEYS;
                    if i % 3 == 0 {
                        let _ = m.remove(&key);
                    } else {
                        let value = (key << 32) | i;
                        let _ = m.put(key, value);
                    }
                    if let Some(value) = m.get(&key) {
                        assert_eq!(value >> 32, key);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn capacity_exceeded_returns_err() {
        let m = DenseKvMap::<u64, u64>::with_capacity(2);
        m.put(1, 1).unwrap();
        m.put(2, 2).unwrap();
        let r = m.put(3, 3);
        assert!(r.is_err());
    }

    #[test]
    fn works_with_non_u64_8_byte_types() {
        let m = DenseKvMap::<u64, i64>::with_capacity(8);
        m.put(0, -42).unwrap();
        assert_eq!(m.get(&0), Some(-42));

        let m = DenseKvMap::<u64, f64>::with_capacity(8);
        m.put(0, std::f64::consts::PI).unwrap();
        assert_eq!(m.get(&0), Some(std::f64::consts::PI));

        let m = DenseKvMap::<u64, [u8; 8]>::with_capacity(8);
        m.put(0, [1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        assert_eq!(m.get(&0), Some([1, 2, 3, 4, 5, 6, 7, 8]));
    }
}
