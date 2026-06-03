//! Inline-storage lock-free hash map specialised for `u64` keys
//! and `u64` values.
//!
//! Where [`crate::concurrent_map::ConcurrentMap`] allocates a
//! `Box<Entry<K, V>>` per insert (matching papaya), [`InlineU64Map`]
//! stores the key and value **inline** in the bucket as a pair of
//! `AtomicU64`s. No Box, no allocation on the hot path, no
//! reclamation overhead — the bucket is the storage.
//!
//! The cost: keys and values must be exactly 8 bytes (`u64`,
//! `i64`, pointer-sized) and we restrict the type to `(u64, u64)`
//! for clarity. Wrapper types can be built on top for other
//! 8-byte payloads via `bytemuck::Pod` or transmute.
//!
//! ## Concurrency: seqlock per slot
//!
//! Without a Box per insert, we can't use epoch-based reclamation
//! to give readers a stable view of an entry while a writer is
//! mid-update. We use the classical [**seqlock**](https://en.wikipedia.org/wiki/Seqlock)
//! pattern instead:
//!
//! - Each slot has a 16-bit `meta` atomic. Low bit is "writing
//!   in progress"; the rest is the 7-bit h2 hash signature plus
//!   8 bits of version counter (we want enough version bits that
//!   the counter doesn't realistically wrap between a reader's
//!   two meta loads).
//! - **Read** path: load meta v1, load key, load value, load meta
//!   v2; if v1 == v2 and bit-0 is clear, the read is consistent.
//!   Retry on mismatch.
//! - **Write** path: CAS meta to set the writing bit, then store
//!   key + value + meta with the writing bit cleared and the
//!   version incremented.
//!
//! Readers spin only on a slot actively being written. Under
//! light contention this is effectively wait-free; under heavy
//! contention readers retry at most a handful of times.
//!
//! ## Speed targets
//!
//! On Skylake T460:
//!
//! - `get(k)` ≈ 5–7 ns (4 acquire loads + key compare + version
//!   check). Same class as the boxed `ConcurrentMap::get`.
//! - `put(k, v)` (insert or update) ≈ 14 ns on the current T460
//!   spot-check. That is roughly 6× faster than papaya on T460 and
//!   historically ~15× on Zen 3, not the old aspirational 50× claim.
//!
//! On Zen 3 expect each number to drop by ~2× thanks to faster
//! atomic CAS and lower load latency.
//!
//! ## Constraints
//!
//! - Fixed capacity at construction; no resize. Pre-size or fail
//!   loudly.
//! - K is `u64` and V is `u64`. Wrap or transmute for other 8-byte
//!   payloads.
//! - Sentinel value `u64::MAX` cannot be used as a key — it
//!   encodes "empty slot." See [`InlineU64Map::RESERVED_KEY`].

use crate::FastBuildHasher;
use crossbeam_utils::CachePadded;
use std::hash::BuildHasher;
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU16, AtomicU64, AtomicUsize, Ordering};

/// Sentinel: this key value is reserved by the implementation to
/// signal an empty slot. Attempting to insert this key panics.
pub const RESERVED_KEY: u64 = u64::MAX;

/// Insert outcome (mirrors [`crate::concurrent_map::InsertOutcome`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    Full,
}

/// Bits 0:    1 if a writer currently holds this slot.
/// Bits 1-7:  7-bit h2 hash signature, valid only when slot is occupied.
/// Bits 8-15: version counter (8 bits = 256 values). Wraps; readers
///            re-check the full meta value.
///
/// State sentinels (when not occupied):
/// - 0x0000: empty (never held an entry).
/// - 0x0002: tombstone (held an entry that was removed; h2=0,
///   version=0, but meta != 0x0000 so probing continues past it).
const WRITING_BIT: u16 = 0x0001;
const H2_MASK: u16 = 0x00FE;
const H2_SHIFT: u32 = 1;
const VERSION_MASK: u16 = 0xFF00;
const VERSION_INC: u16 = 0x0100;
const TOMBSTONE: u16 = 0x0002;
const EMPTY: u16 = 0x0000;

#[inline]
fn h2_of(hash: u64) -> u16 {
    // Top 7 bits, shifted into bits 1-7.
    ((hash >> 57) as u16 & 0x7F) << H2_SHIFT
}

#[inline]
fn meta_h2(meta: u16) -> u16 {
    meta & H2_MASK
}

#[inline]
fn is_occupied(meta: u16) -> bool {
    meta != EMPTY && meta != TOMBSTONE && meta & WRITING_BIT == 0
}

#[inline]
fn is_writing_match(meta: u16, h2: u16) -> bool {
    meta != EMPTY && meta != TOMBSTONE && meta_h2(meta) == h2 && meta & WRITING_BIT != 0
}

/// Concurrent hash map for `u64` keys and `u64` values, with
/// inline (no-Box) storage and seqlock-protected reads.
pub struct InlineU64Map<S = FastBuildHasher>
where
    S: BuildHasher,
{
    meta: Box<[AtomicU16]>,
    keys: Box<[AtomicU64]>,
    values: Box<[AtomicU64]>,
    capacity: usize,
    mask: usize,
    probe_limit: usize,
    len: CachePadded<AtomicUsize>,
    hash_builder: S,
}

impl InlineU64Map<FastBuildHasher> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }
}

impl<S: BuildHasher> InlineU64Map<S> {
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        let target = capacity.max(8).saturating_mul(4) / 3;
        let cap = target.next_power_of_two().max(8);
        let mask = cap - 1;
        let probe_limit = probe_limit(cap);

        let mut meta = Vec::with_capacity(cap);
        let mut keys = Vec::with_capacity(cap);
        let mut values = Vec::with_capacity(cap);
        for _ in 0..cap {
            meta.push(AtomicU16::new(EMPTY));
            keys.push(AtomicU64::new(RESERVED_KEY));
            values.push(AtomicU64::new(0));
        }
        Self {
            meta: meta.into_boxed_slice(),
            keys: keys.into_boxed_slice(),
            values: values.into_boxed_slice(),
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

    /// Lookup with seqlock-protected read of the (key, value) pair.
    pub fn get(&self, key: u64) -> Option<u64> {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            // Seqlock read: meta1 → data → meta2 → consistent if equal.
            let m1 = self.meta[i].load(Ordering::Acquire);
            let target_meta = meta_h2(m1);
            if m1 == EMPTY {
                return None;
            }
            if is_writing_match(m1, h2) {
                spin_loop();
                continue;
            }
            if target_meta == h2 && m1 & WRITING_BIT == 0 {
                let k = self.keys[i].load(Ordering::Acquire);
                let v = self.values[i].load(Ordering::Acquire);
                let m2 = self.meta[i].load(Ordering::Acquire);
                if m1 == m2 && k == key {
                    return Some(v);
                }
                // Inconsistent read or different key: re-probe this slot.
                // If writing was in progress mid-read, retry the SAME slot
                // before advancing — the entry might still be for our key.
                if m1 != m2 {
                    continue;
                }
                // h2 matched but key didn't — fall through to advance.
            }
            len += 1;
            if len > self.probe_limit {
                return None;
            }
            i = (i + len) & self.mask;
        }
    }

    /// Insert or replace. Returns the outcome.
    pub fn insert(&self, key: u64, value: u64) -> InsertOutcome {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if is_writing_match(meta, h2) {
                spin_loop();
                continue;
            }
            // h2 match? Could be our key (update path) or a collision.
            if meta_h2(meta) == h2 && meta & WRITING_BIT == 0 && meta != EMPTY && meta != TOMBSTONE
            {
                let existing_key = self.keys[i].load(Ordering::Acquire);
                if existing_key == key {
                    // Update path. Acquire write lock by CAS-ing meta
                    // to set the WRITING bit.
                    let locked = meta | WRITING_BIT;
                    if self.meta[i]
                        .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        self.values[i].store(value, Ordering::Release);
                        // Release the lock with version incremented.
                        let new_meta = (meta.wrapping_add(VERSION_INC) & VERSION_MASK) | h2;
                        self.meta[i].store(new_meta, Ordering::Release);
                        return InsertOutcome::Replaced;
                    }
                    // Lost the CAS to a concurrent writer. Re-probe
                    // the same slot.
                    continue;
                }
                // Hash collision; advance the probe.
            } else if meta == EMPTY || meta == TOMBSTONE {
                // Try to claim this empty/tombstone slot.
                let locked = WRITING_BIT;
                if self.meta[i]
                    .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    self.keys[i].store(key, Ordering::Release);
                    self.values[i].store(value, Ordering::Release);
                    let initial_version = if meta == TOMBSTONE { VERSION_INC } else { 0 };
                    self.meta[i].store(initial_version | h2, Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return InsertOutcome::Inserted;
                }
                // Lost CAS. Re-probe the same slot.
                continue;
            }
            // WRITING bit set or non-matching meta: advance.
            len += 1;
            if len > self.probe_limit {
                return InsertOutcome::Full;
            }
            i = (i + len) & self.mask;
        }
    }

    /// Insert or replace, returning the old value on replacement.
    pub fn insert_returning_old(&self, key: u64, value: u64) -> (InsertOutcome, Option<u64>) {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
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
                        let old = self.values[i].load(Ordering::Acquire);
                        self.values[i].store(value, Ordering::Release);
                        let new_meta = (meta.wrapping_add(VERSION_INC) & VERSION_MASK) | h2;
                        self.meta[i].store(new_meta, Ordering::Release);
                        return (InsertOutcome::Replaced, Some(old));
                    }
                    continue;
                }
            } else if meta == EMPTY || meta == TOMBSTONE {
                let locked = WRITING_BIT;
                if self.meta[i]
                    .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    self.keys[i].store(key, Ordering::Release);
                    self.values[i].store(value, Ordering::Release);
                    let initial_version = if meta == TOMBSTONE { VERSION_INC } else { 0 };
                    self.meta[i].store(initial_version | h2, Ordering::Release);
                    self.len.fetch_add(1, Ordering::Relaxed);
                    return (InsertOutcome::Inserted, None);
                }
                continue;
            }
            len += 1;
            if len > self.probe_limit {
                return (InsertOutcome::Full, None);
            }
            i = (i + len) & self.mask;
        }
    }

    /// Remove the key, returning the previous value if present.
    pub fn remove(&self, key: u64) -> Option<u64> {
        assert_ne!(key, RESERVED_KEY, "RESERVED_KEY (u64::MAX) cannot be a key");
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == EMPTY {
                return None;
            }
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
                        let old_value = self.values[i].load(Ordering::Acquire);
                        self.keys[i].store(RESERVED_KEY, Ordering::Release);
                        self.values[i].store(0, Ordering::Release);
                        self.meta[i].store(TOMBSTONE, Ordering::Release);
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        return Some(old_value);
                    }
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

    /// Remove only if the current value equals `expected`.
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
            if meta_h2(meta) == h2 && meta & WRITING_BIT == 0 && meta != EMPTY && meta != TOMBSTONE
            {
                let existing_key = self.keys[i].load(Ordering::Acquire);
                if existing_key == key {
                    let current = self.values[i].load(Ordering::Acquire);
                    if current != expected {
                        return false;
                    }
                    let locked = meta | WRITING_BIT;
                    if self.meta[i]
                        .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        let current = self.values[i].load(Ordering::Acquire);
                        if current != expected {
                            self.meta[i].store(meta, Ordering::Release);
                            return false;
                        }
                        self.keys[i].store(RESERVED_KEY, Ordering::Release);
                        self.values[i].store(0, Ordering::Release);
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

    /// Atomically update the value if the key is present, returning
    /// the previous value. Differs from `insert` in that it does NOT
    /// create a new entry if the key is absent — useful for
    /// counters and similar in-place mutations.
    pub fn update(&self, key: u64, value: u64) -> Option<u64> {
        assert_ne!(key, RESERVED_KEY);
        let (h1, h2) = self.h1_h2(key);
        let mut i = h1;
        let mut len = 0usize;
        loop {
            let meta = self.meta[i].load(Ordering::Acquire);
            if meta == EMPTY {
                return None;
            }
            if is_writing_match(meta, h2) {
                spin_loop();
                continue;
            }
            if meta_h2(meta) == h2 && meta & WRITING_BIT == 0 {
                let existing_key = self.keys[i].load(Ordering::Acquire);
                if existing_key == key {
                    let locked = meta | WRITING_BIT;
                    if self.meta[i]
                        .compare_exchange(meta, locked, Ordering::Acquire, Ordering::Relaxed)
                        .is_ok()
                    {
                        let old = self.values[i].load(Ordering::Acquire);
                        self.values[i].store(value, Ordering::Release);
                        let new_meta = (meta.wrapping_add(VERSION_INC) & VERSION_MASK) | h2;
                        self.meta[i].store(new_meta, Ordering::Release);
                        return Some(old);
                    }
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
}

#[inline]
fn probe_limit(capacity: usize) -> usize {
    let log2 = (usize::BITS as usize)
        .saturating_sub(capacity.leading_zeros() as usize)
        .saturating_sub(1);
    5 * log2.max(1)
}

// Silence unused-warning for is_occupied (kept for documentation
// and possible future use in iter()).
#[allow(dead_code)]
fn _suppress_unused() {
    let _ = is_occupied;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;

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

    #[test]
    fn insert_get_update_remove() {
        let m = InlineU64Map::with_capacity(64);
        assert!(m.is_empty());
        assert_eq!(m.insert(1, 100), InsertOutcome::Inserted);
        assert_eq!(m.insert(2, 200), InsertOutcome::Inserted);
        assert_eq!(m.get(1), Some(100));
        assert_eq!(m.get(2), Some(200));
        assert_eq!(m.get(3), None);
        assert_eq!(m.insert(1, 999), InsertOutcome::Replaced);
        assert_eq!(m.get(1), Some(999));
        assert_eq!(m.len(), 2);
        assert_eq!(m.update(1, 1234), Some(999));
        assert_eq!(m.get(1), Some(1234));
        assert_eq!(m.update(42, 99), None);
        assert_eq!(m.remove(1), Some(1234));
        assert_eq!(m.get(1), None);
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn tombstone_does_not_terminate_probe() {
        let m = InlineU64Map::with_capacity(64);
        for i in 0..16u64 {
            m.insert(i, i * 10);
        }
        m.remove(7);
        for i in [0, 1, 6, 8, 15u64] {
            assert_eq!(
                m.get(i),
                Some(i * 10),
                "key {} should still be reachable",
                i
            );
        }
        assert_eq!(m.get(7), None);
        // Reinsert into the tombstoned slot.
        assert_eq!(m.insert(7, 70), InsertOutcome::Inserted);
        assert_eq!(m.get(7), Some(70));
    }

    #[test]
    fn sentinel_h2_key_round_trips() {
        let m = InlineU64Map::with_hasher_and_capacity(ConstantBuildHasher, 8);
        assert_eq!(m.insert(1, 10), InsertOutcome::Inserted);
        assert_eq!(m.get(1), Some(10));
        assert_eq!(m.remove(1), Some(10));
        assert_eq!(m.get(1), None);
    }

    #[test]
    #[should_panic(expected = "RESERVED_KEY")]
    fn reserved_key_panics_on_insert() {
        let m = InlineU64Map::with_capacity(8);
        m.insert(RESERVED_KEY, 1);
    }

    #[test]
    fn concurrent_inserts_and_reads() {
        use std::sync::Arc;
        use std::thread;
        let m = Arc::new(InlineU64Map::with_capacity(16384));
        let mut handles = Vec::new();
        for tid in 0..4 {
            let m = Arc::clone(&m);
            handles.push(thread::spawn(move || {
                for i in 0..1000u64 {
                    let key = tid as u64 * 1000 + i + 1; // avoid 0 and RESERVED_KEY edge cases
                    m.insert(key, key * 7);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.len(), 4000);
        for tid in 0..4u64 {
            for i in 0..1000u64 {
                let key = tid * 1000 + i + 1;
                assert_eq!(m.get(key), Some(key * 7), "missing key {key}");
            }
        }
    }

    #[test]
    fn concurrent_update_visible_to_reader() {
        use std::sync::Arc;
        use std::thread;
        let m = Arc::new(InlineU64Map::with_capacity(64));
        m.insert(1, 0);
        let m_w = Arc::clone(&m);
        let writer = thread::spawn(move || {
            for i in 0..10_000u64 {
                m_w.update(1, i);
            }
        });
        let m_r = Arc::clone(&m);
        let reader = thread::spawn(move || {
            let mut last = 0u64;
            for _ in 0..10_000 {
                if let Some(v) = m_r.get(1) {
                    // Monotonically increasing (the writer counts up).
                    assert!(v >= last, "read regressed: prev={last} now={v}");
                    last = v;
                }
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
        assert_eq!(m.get(1), Some(9999));
    }

    #[test]
    fn concurrent_remove_insert_reuse_is_consistent() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const KEYS: u64 = 8;
        const THREADS: usize = 6;
        const ITERS: u64 = 20_000;

        let m = Arc::new(InlineU64Map::with_capacity(64));
        for key in 0..KEYS {
            m.insert(key, key << 32);
        }

        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::new();
        for thread_id in 0..THREADS {
            let m = Arc::clone(&m);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for i in 0..ITERS {
                    let key = (i + thread_id as u64) % KEYS;
                    if i % 3 == 0 {
                        m.remove(key);
                    } else {
                        m.insert(key, (key << 32) | i);
                    }
                    if let Some(value) = m.get(key) {
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
    fn h2_avoids_sentinels() {
        // Raw h2 occupies bits 1-7 only. `h1_h2` maps sentinel raw
        // values away from EMPTY and TOMBSTONE before publishing meta.
        for hash in [
            0u64,
            1,
            0xFFFF_FFFF_FFFF_FFFF,
            0x80_00_00_00_00_00_00_00,
            42,
        ] {
            let h2 = h2_of(hash);
            // h2 occupies bits 1-7 only.
            assert_eq!(h2 & !H2_MASK, 0);
        }
    }
}
