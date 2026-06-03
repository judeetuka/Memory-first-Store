//! Inline `u64 -> u64 handle` index for dense write-behind experiments.
//!
//! This is intentionally narrower than [`mfs_core::inline_map::InlineU64Map`].
//! Values are opaque handles, not user data. Empty-slot insertions publish
//! the target h2 while the bucket is locked, so same-key writers wait for
//! publication without making unrelated keys spin behind every in-flight
//! writer.

use crossbeam_utils::CachePadded;
use mfs_core::FastBuildHasher;
use mfs_core::inline_map::{InsertOutcome, RESERVED_KEY};
use std::hash::BuildHasher;
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};

const WRITING_BIT: u16 = 0x0001;
const H2_MASK: u16 = 0x00FE;
const H2_SHIFT: u32 = 1;
const VERSION_MASK: u16 = 0xFF00;
const VERSION_INC: u16 = 0x0100;
const TOMBSTONE: u16 = 0x0002;
const EMPTY: u16 = 0x0000;

#[inline]
fn h2_of(hash: u64) -> u16 {
    ((hash >> 57) as u16 & 0x7F) << H2_SHIFT
}

#[inline]
fn meta_h2(meta: u16) -> u16 {
    meta & H2_MASK
}

#[inline]
fn is_writing_match(meta: u16, h2: u16) -> bool {
    meta != EMPTY && meta != TOMBSTONE && meta_h2(meta) == h2 && meta & WRITING_BIT != 0
}

pub struct InlineHandleIndex<S = FastBuildHasher>
where
    S: BuildHasher,
{
    meta: Box<[AtomicU16]>,
    keys: Box<[AtomicU64]>,
    handles: Box<[AtomicU64]>,
    capacity: usize,
    mask: usize,
    probe_limit: usize,
    len: CachePadded<AtomicUsize>,
    hash_builder: S,
}

impl InlineHandleIndex<FastBuildHasher> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }
}

impl<S: BuildHasher> InlineHandleIndex<S> {
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        let target = capacity.max(8).saturating_mul(4) / 3;
        let cap = target.next_power_of_two().max(8);
        let mask = cap - 1;
        let probe_limit = probe_limit(cap);

        let mut meta = Vec::with_capacity(cap);
        let mut keys = Vec::with_capacity(cap);
        let mut handles = Vec::with_capacity(cap);
        for _ in 0..cap {
            meta.push(AtomicU16::new(EMPTY));
            keys.push(AtomicU64::new(RESERVED_KEY));
            handles.push(AtomicU64::new(0));
        }

        Self {
            meta: meta.into_boxed_slice(),
            keys: keys.into_boxed_slice(),
            handles: handles.into_boxed_slice(),
            capacity: cap,
            mask,
            probe_limit,
            len: CachePadded::new(AtomicUsize::new(0)),
            hash_builder,
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
    fn h1_h2(&self, key: u64) -> (usize, u16) {
        let h = self.hash_builder.hash_one(key);
        let h1 = (h as usize) & self.mask;
        let h2 = match h2_of(h) {
            EMPTY | TOMBSTONE => TOMBSTONE + 2,
            h2 => h2,
        };
        (h1, h2)
    }

    #[inline]
    pub fn get(&self, key: u64) -> Option<u64> {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let m1 = self.meta[i].load(Ordering::Acquire);
            if m1 == EMPTY {
                return None;
            }
            if is_writing_match(m1, h2) {
                spin_loop();
                continue;
            }
            if meta_h2(m1) == h2 && m1 & WRITING_BIT == 0 {
                let k = self.keys[i].load(Ordering::Acquire);
                let handle = self.handles[i].load(Ordering::Acquire);
                let m2 = self.meta[i].load(Ordering::Acquire);
                if m1 == m2 && k == key {
                    return Some(handle);
                }
                if m1 != m2 {
                    spin_loop();
                    continue;
                }
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            i = (i + len) & self.mask;
        }
    }

    pub fn insert_returning_old(&self, key: u64, handle: u64) -> (InsertOutcome, Option<u64>) {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        let mut first_tombstone = None;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if is_writing_match(meta, h2) {
                spin_loop();
                continue;
            }
            if meta_h2(meta) == h2 && meta & WRITING_BIT == 0 && meta != EMPTY && meta != TOMBSTONE
            {
                let existing_key = self.keys[i].load(Ordering::Acquire);
                if existing_key == key {
                    let locked = meta | WRITING_BIT;
                    if self.meta[i]
                        .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        let old = self.handles[i].load(Ordering::Acquire);
                        self.handles[i].store(handle, Ordering::Release);
                        let new_meta = (meta.wrapping_add(VERSION_INC) & VERSION_MASK) | h2;
                        self.meta[i].store(new_meta, Ordering::Release);
                        return (InsertOutcome::Replaced, Some(old));
                    }
                    continue;
                }
            } else if meta == TOMBSTONE {
                if first_tombstone.is_none() {
                    first_tombstone = Some(i);
                }
            } else if meta == EMPTY {
                let target = first_tombstone.unwrap_or(i);
                let expected = if first_tombstone.is_some() {
                    TOMBSTONE
                } else {
                    EMPTY
                };
                if self.claim_insert_slot(target, expected, key, handle, h2) {
                    return (InsertOutcome::Inserted, None);
                }
                i = h1;
                len = 0;
                first_tombstone = None;
                spin_loop();
                continue;
            }
            len += 1;
            if len > self.probe_limit {
                if let Some(target) = first_tombstone {
                    if self.claim_insert_slot(target, TOMBSTONE, key, handle, h2) {
                        return (InsertOutcome::Inserted, None);
                    }
                    i = h1;
                    len = 0;
                    first_tombstone = None;
                    spin_loop();
                    continue;
                }
                return (InsertOutcome::Full, None);
            }
            i = (i + len) & self.mask;
        }
    }

    fn claim_insert_slot(
        &self,
        index: usize,
        expected: u16,
        key: u64,
        handle: u64,
        h2: u16,
    ) -> bool {
        let locked = h2 | WRITING_BIT;
        if self.meta[index]
            .compare_exchange(expected, locked, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        self.keys[index].store(key, Ordering::Release);
        self.handles[index].store(handle, Ordering::Release);
        let initial_version = if expected == TOMBSTONE {
            VERSION_INC
        } else {
            0
        };
        self.meta[index].store(initial_version | h2, Ordering::Release);
        self.len.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub fn remove_if_value(&self, key: u64, expected: u64) -> bool {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == EMPTY {
                return false;
            }
            if is_writing_match(meta, h2) {
                spin_loop();
                continue;
            }
            if meta_h2(meta) == h2 && meta & WRITING_BIT == 0 && meta != TOMBSTONE {
                let existing_key = self.keys[i].load(Ordering::Acquire);
                if existing_key == key {
                    if self.handles[i].load(Ordering::Acquire) != expected {
                        return false;
                    }
                    let locked = meta | WRITING_BIT;
                    if self.meta[i]
                        .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        if self.handles[i].load(Ordering::Acquire) != expected {
                            self.meta[i].store(meta, Ordering::Release);
                            return false;
                        }
                        self.keys[i].store(RESERVED_KEY, Ordering::Release);
                        self.handles[i].store(0, Ordering::Release);
                        self.meta[i].store(TOMBSTONE, Ordering::Release);
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        return true;
                    }
                    continue;
                }
            }
            len += 1;
            if len > self.probe_limit {
                return false;
            }
            i = (i + len) & self.mask;
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
    use crossbeam_queue::ArrayQueue;
    use std::hash::Hasher;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};

    const SLOT_WRITE_BIT: u32 = 1;

    #[derive(Clone)]
    struct ConstantBuildHasher;

    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    impl BuildHasher for ConstantBuildHasher {
        type Hasher = ConstantHasher;

        fn build_hasher(&self) -> Self::Hasher {
            ConstantHasher
        }
    }

    struct SlotHarness {
        index: InlineHandleIndex,
        generations: Vec<AtomicU32>,
        values: Vec<AtomicU64>,
        free: ArrayQueue<u32>,
        next_slot: AtomicU32,
        capacity: u32,
    }

    impl SlotHarness {
        fn with_capacity(capacity: u32) -> Self {
            Self {
                index: InlineHandleIndex::with_capacity(capacity as usize),
                generations: (0..capacity).map(|_| AtomicU32::new(0)).collect(),
                values: (0..capacity).map(|_| AtomicU64::new(0)).collect(),
                free: ArrayQueue::new(capacity as usize),
                next_slot: AtomicU32::new(0),
                capacity,
            }
        }

        fn pack(slot: u32, generation: u32) -> u64 {
            ((generation as u64) << 32) | slot as u64
        }

        fn slot(handle: u64) -> u32 {
            handle as u32
        }

        fn generation(handle: u64) -> u32 {
            (handle >> 32) as u32
        }

        fn next_slot_or_recycle(&self) -> Option<u32> {
            if let Some(slot) = self.free.pop() {
                return Some(slot);
            }
            let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
            if slot >= self.capacity {
                self.next_slot.store(self.capacity, Ordering::Relaxed);
                return None;
            }
            Some(slot)
        }

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

        fn stale_generation(&self, slot: u32, generation: u32) -> bool {
            let current = self.generations[slot as usize].load(Ordering::Acquire);
            current != generation && current != (generation | SLOT_WRITE_BIT)
        }

        fn unlock_slot(&self, slot: u32, generation: u32) {
            self.generations[slot as usize].store(generation, Ordering::Release);
        }

        fn retire_locked_slot(&self, slot: u32, generation: u32) {
            self.generations[slot as usize].store(generation.wrapping_add(2), Ordering::Release);
            let _ = self.free.push(slot);
        }

        fn retire_slot(&self, handle: u64) {
            let slot = Self::slot(handle);
            let generation = Self::generation(handle);
            let state = &self.generations[slot as usize];
            loop {
                match state.compare_exchange(
                    generation,
                    generation | SLOT_WRITE_BIT,
                    Ordering::Acquire,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.retire_locked_slot(slot, generation);
                        return;
                    }
                    Err(current) if current == (generation | SLOT_WRITE_BIT) => spin_loop(),
                    Err(_) => return,
                }
            }
        }

        fn put(&self, key: u64, value: u64) {
            loop {
                if let Some(handle) = self.index.get(key) {
                    let slot = Self::slot(handle);
                    let generation = Self::generation(handle);
                    if !self.try_lock_slot_generation(slot, generation) {
                        if self.stale_generation(slot, generation) {
                            self.index.remove_if_value(key, handle);
                        }
                        spin_loop();
                        continue;
                    }
                    let still_current =
                        matches!(self.index.get(key), Some(current) if current == handle);
                    if still_current {
                        self.values[slot as usize].store(value, Ordering::Release);
                        self.unlock_slot(slot, generation);
                        return;
                    }
                    self.unlock_slot(slot, generation);
                    spin_loop();
                    continue;
                }
                break;
            }

            let slot = self.next_slot_or_recycle().expect("slot harness is full");
            let generation = self.lock_slot(slot);
            let handle = Self::pack(slot, generation);
            self.values[slot as usize].store(value, Ordering::Release);
            match self.index.insert_returning_old(key, handle) {
                (InsertOutcome::Inserted | InsertOutcome::Replaced, old) => {
                    self.unlock_slot(slot, generation);
                    if let Some(old) = old {
                        self.retire_slot(old);
                    }
                }
                (InsertOutcome::Full, old) => {
                    self.unlock_slot(slot, generation);
                    if let Some(old) = old {
                        self.retire_slot(old);
                    }
                    let _ = self.free.push(slot);
                    panic!("slot harness index is full");
                }
            }
        }

        fn delete(&self, key: u64) {
            loop {
                let Some(handle) = self.index.get(key) else {
                    return;
                };
                let slot = Self::slot(handle);
                let generation = Self::generation(handle);
                if !self.try_lock_slot_generation(slot, generation) {
                    if self.stale_generation(slot, generation) {
                        self.index.remove_if_value(key, handle);
                    }
                    spin_loop();
                    continue;
                }
                if self.index.remove_if_value(key, handle) {
                    self.retire_locked_slot(slot, generation);
                    return;
                }
                self.unlock_slot(slot, generation);
                spin_loop();
            }
        }

        fn get(&self, key: u64) -> Option<u64> {
            let handle = self.index.get(key)?;
            let slot = Self::slot(handle);
            let generation = Self::generation(handle);
            if self.generations[slot as usize].load(Ordering::Acquire) != generation {
                if self.stale_generation(slot, generation) {
                    self.index.remove_if_value(key, handle);
                }
                return None;
            }
            let value = self.values[slot as usize].load(Ordering::Acquire);
            if self.generations[slot as usize].load(Ordering::Acquire) == generation {
                return Some(value);
            }
            None
        }
    }

    #[test]
    fn insert_get_replace_remove_if_value() {
        let m = InlineHandleIndex::with_capacity(64);
        assert_eq!(
            m.insert_returning_old(1, 100),
            (InsertOutcome::Inserted, None)
        );
        assert_eq!(m.get(1), Some(100));
        assert_eq!(
            m.insert_returning_old(1, 200),
            (InsertOutcome::Replaced, Some(100))
        );
        assert_eq!(m.get(1), Some(200));
        assert!(!m.remove_if_value(1, 100));
        assert!(m.remove_if_value(1, 200));
        assert_eq!(m.get(1), None);
        assert!(m.is_empty());
    }

    #[test]
    fn insert_checks_past_tombstone_before_reusing_it() {
        let m = InlineHandleIndex::with_hasher_and_capacity(ConstantBuildHasher, 8);
        assert_eq!(
            m.insert_returning_old(1, 10),
            (InsertOutcome::Inserted, None)
        );
        assert_eq!(
            m.insert_returning_old(2, 20),
            (InsertOutcome::Inserted, None)
        );
        assert!(m.remove_if_value(1, 10));

        assert_eq!(
            m.insert_returning_old(2, 30),
            (InsertOutcome::Replaced, Some(20))
        );
        assert_eq!(m.len(), 1);
        assert_eq!(m.get(1), None);
        assert_eq!(m.get(2), Some(30));
    }

    #[test]
    fn concurrent_remove_insert_reuse_is_consistent() {
        const KEYS: u64 = 8;
        const THREADS: usize = 6;
        const ITERS: u64 = 20_000;

        let m = Arc::new(InlineHandleIndex::with_capacity(64));
        for key in 0..KEYS {
            m.insert_returning_old(key, key << 32);
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
                        if let Some(handle) = m.get(key) {
                            m.remove_if_value(key, handle);
                        }
                    } else {
                        m.insert_returning_old(key, (key << 32) | i);
                    }
                    if let Some(handle) = m.get(key) {
                        assert_eq!(handle >> 32, key);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        assert!(m.len() <= KEYS as usize);
    }

    #[test]
    fn concurrent_same_key_insert_has_single_entry() {
        const THREADS: usize = 8;
        const ITERS: u64 = 10_000;

        let m = Arc::new(InlineHandleIndex::with_capacity(64));
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::new();
        for thread_id in 0..THREADS {
            let m = Arc::clone(&m);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..ITERS {
                    m.insert_returning_old(1, ((thread_id as u64) << 32) | i);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(m.len(), 1);
        assert!(m.get(1).is_some());
    }

    #[test]
    fn slot_generation_lifecycle_is_consistent() {
        const KEYS: u64 = 8;
        const THREADS: usize = 6;
        const ITERS: u64 = 20_000;

        let h = Arc::new(SlotHarness::with_capacity(64));
        for key in 0..KEYS {
            h.put(key, key << 32);
        }

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::new();
        for thread_id in 0..THREADS {
            let h = Arc::clone(&h);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..ITERS {
                    let key = (i + thread_id as u64) % KEYS;
                    if i % 3 == 0 {
                        h.delete(key);
                    } else {
                        h.put(key, (key << 32) | i);
                    }
                    if let Some(value) = h.get(key) {
                        assert_eq!(value >> 32, key);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
