//! High-throughput memory-first storage primitives.
//!
//! The hot path is in-process RAM only. Nanosecond timings apply to cached
//! memory operations such as [`DenseU64Lane::load`], not to database, disk,
//! network, serialization, or fsync work. Persistence is intentionally exposed
//! as a write-behind API through [`FlushBackend`]; the [`durability`] module
//! ships a reference WAL implementation. Backend implementations must be
//! idempotent because failed flushes can be retried with the same records.
//!
//! ## Hot-path design notes
//!
//! [`DenseU64Lane`] packs the dirty flag into bit 63 of the value itself.
//! Stores complete in a single atomic write and the parallel dirty array is
//! gone, so writes touch one cache line and are immune to cross-index false
//! sharing on the dirty side. Values are therefore restricted to 63 bits.
//!
//! [`MemoryFirstStore`] is sharded with [`parking_lot::RwLock`] (single-word
//! uncontended fast path) over [`hashbrown::HashTable`] (SIMD-tagged
//! probing, no double-hashing because we pre-compute the key hash for shard
//! selection and reuse it for the bucket lookup). Each shard sits in a
//! [`CachePadded`] cell so adjacent shards never share a cache line, and
//! the global logical clock sits in its own [`CachePadded`] cell.
//!
//! Sampled access tracking: only ~1/64 of `get` calls advance the logical
//! clock and update `last_touch`. The sample rate keys off the hash of the
//! key itself (a value already needed to pick a shard) and so adds zero
//! additional work. The idle heuristic is therefore coarse but free;
//! W-TinyLFU and Caffeine-style caches use the same trick.
//!
//! [`MemoryFirstStore::read_with`] avoids the `Arc::clone` on reads when the
//! caller can express the read as a closure scoped to the read guard.

mod arch;
pub mod atomic_writeback;
pub mod bounded_reclaim;
pub mod concurrent_map;
pub mod durability;
mod hasher;
pub mod inline_map;
pub mod lockfree;
pub mod partitioned_lockfree;
pub mod s3fifo;
pub mod slot_writeback;
pub mod tiny_lfu;
pub mod writeback;

use crossbeam_utils::CachePadded;
use hashbrown::HashTable;
use parking_lot::RwLock;
use std::fmt;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub use arch::{
    CPU_FALLBACK_PATH, CpuDispatchPath, CpuFeatures, avx2_supported, avx512_supported, cpu_relax,
    prefetch_read, prefetch_write, sse42_supported,
};
pub use hasher::{FastBuildHasher, FastHasher};

#[cfg(feature = "ahash")]
pub use ahash::RandomState as AHashState;

/// Pick a sensible thread count for caller-driven worker pools.
///
/// Policy:
/// - `None` ⇒ `available_parallelism() / 2` (rounded up to ≥ 1).
/// - `Some(0)` ⇒ same as `None`.
/// - `Some(n)` where `1 ≤ n ≤ nproc` ⇒ `n` (caller's request honoured).
/// - `Some(n)` where `n > nproc` ⇒ silent fallback to
///   `available_parallelism() / 2`. We don't punish a too-high request,
///   just refuse to oversubscribe past the logical thread count, since
///   that tends to add scheduler noise without adding throughput.
///
/// The half-of-nproc default reflects the rule of thumb that hash-table
/// workloads hit memory-bandwidth or shared-cache limits well before
/// they consume all logical CPUs. If you have telemetry showing your
/// workload scales further, pass an explicit `Some(n)`.
pub fn auto_thread_count(requested: Option<usize>) -> usize {
    let nproc = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let half = (nproc / 2).max(1);
    match requested {
        None => half,
        Some(0) => half,
        Some(n) if n <= nproc => n,
        Some(_) => half,
    }
}

/// Bit reserved for the dirty flag inside a packed dense value word.
const DIRTY_BIT: u64 = 1 << 63;
/// Mask for the user-visible 63-bit value payload.
const VALUE_MASK: u64 = !DIRTY_BIT;
/// Maximum representable value in a [`DenseU64Lane`] slot.
pub const DENSE_VALUE_MAX: u64 = VALUE_MASK;

/// Sample mask for access tracking. With 6 bits the expected ratio is 1 in
/// 64, which preserves LRU ordering within ~1.5% of perfect while reducing
/// contended atomic traffic on the global clock 64x.
const SAMPLE_BITS: u32 = 6;
const SAMPLE_MASK: u64 = (1 << SAMPLE_BITS) - 1;

/// Dense atomic numeric lane for ultra-hot SNN/GNN-style `u64` state.
///
/// This path avoids hashing and locks. It is suitable for cache-resident
/// numeric reads/writes where the in-memory value is authoritative and
/// persistence is a best-effort write-behind snapshot.
///
/// Storage layout: each slot is a single `AtomicU64` whose bit 63 is the
/// dirty flag and whose low 63 bits are the value payload. Stores set the
/// dirty bit in the same atomic write that updates the value. There is no
/// parallel dirty array so writes touch exactly one cache line and adjacent
/// indices cannot ping-pong an unrelated dirty line.
///
/// `dirty_values` + `mark_clean` do not carry per-slot versions; a concurrent
/// writer can race a cleaner. Use the generic [`MemoryFirstStore`] if you
/// need version-checked write-behind semantics for arbitrary data.
pub struct DenseU64Lane {
    slots: Box<[AtomicU64]>,
}

impl DenseU64Lane {
    pub fn with_len(len: usize) -> Self {
        let mut slots = Vec::with_capacity(len);
        for _ in 0..len {
            slots.push(AtomicU64::new(0));
        }
        Self {
            slots: slots.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Load the current value at `index`. The dirty flag is masked off.
    #[inline]
    pub fn load(&self, index: usize) -> u64 {
        self.slots[index].load(Ordering::Relaxed) & VALUE_MASK
    }

    /// Read the raw packed slot word, including the dirty bit. Useful for
    /// flush scans that want to inspect dirty + value in one atomic load.
    #[inline]
    pub fn load_raw(&self, index: usize) -> u64 {
        self.slots[index].load(Ordering::Relaxed)
    }

    /// Store `value` and mark the slot dirty in a single atomic operation.
    ///
    /// Panics in debug builds if `value` exceeds [`DENSE_VALUE_MAX`].
    #[inline]
    pub fn store(&self, index: usize, value: u64) {
        debug_assert!(value <= DENSE_VALUE_MAX, "value uses reserved dirty bit");
        self.slots[index].store(value | DIRTY_BIT, Ordering::Release);
    }

    /// Atomically add `value` to the payload at `index` and mark dirty,
    /// returning the previous payload. Implemented as a compare-and-swap
    /// loop because the dirty bit must remain pinned across the update.
    #[inline]
    pub fn fetch_add(&self, index: usize, value: u64) -> u64 {
        let slot = &self.slots[index];
        let mut current = slot.load(Ordering::Relaxed);
        loop {
            let prev_value = current & VALUE_MASK;
            let next_value = prev_value.wrapping_add(value) & VALUE_MASK;
            let new_word = next_value | DIRTY_BIT;
            match slot.compare_exchange_weak(
                current,
                new_word,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return prev_value,
                Err(observed) => {
                    current = observed;
                    arch::cpu_relax();
                }
            }
        }
    }

    /// Mark a slot dirty without changing its value.
    #[inline]
    pub fn mark_dirty(&self, index: usize) {
        self.slots[index].fetch_or(DIRTY_BIT, Ordering::Release);
    }

    /// Clear the dirty flag at `index` while preserving the value.
    #[inline]
    pub fn mark_clean(&self, index: usize) {
        self.slots[index].fetch_and(VALUE_MASK, Ordering::Release);
    }

    pub fn mark_many_clean(&self, indices: impl IntoIterator<Item = usize>) {
        for index in indices {
            self.mark_clean(index);
        }
    }

    /// Returns whether the dirty bit is currently set at `index`.
    #[inline]
    pub fn is_dirty(&self, index: usize) -> bool {
        self.slots[index].load(Ordering::Acquire) & DIRTY_BIT != 0
    }

    /// Collect up to `max` dirty `(index, value)` pairs.
    pub fn dirty_values(&self, max: usize) -> Vec<(usize, u64)> {
        const LOOKAHEAD: usize = 8;
        let mut out = Vec::with_capacity(max.min(self.slots.len()));
        let n = self.slots.len();
        let mut i = 0;
        while i < n && out.len() < max {
            let ahead = i + LOOKAHEAD;
            if ahead < n {
                prefetch_read(&self.slots[ahead]);
            }
            let word = self.slots[i].load(Ordering::Acquire);
            if word & DIRTY_BIT != 0 {
                out.push((i, word & VALUE_MASK));
            }
            i += 1;
        }
        out
    }

    /// Pipelined batch load. Reads `indices.len()` slots into `out`, issuing
    /// software prefetches `lookahead` iterations ahead of the consuming
    /// load. The consumer should size `out` to `indices.len()`.
    ///
    /// **Use only for irregular access patterns.** For sequential reads, the
    /// hardware streamer prefetches automatically and the explicit prefetch
    /// instructions issued here are pure overhead — scalar [`load`] in a
    /// loop is faster.
    ///
    /// [`load`]: Self::load
    pub fn load_many(&self, indices: &[usize], out: &mut [u64]) {
        const LOOKAHEAD: usize = 8;
        debug_assert_eq!(indices.len(), out.len());
        let n = indices.len();
        let cap = self.slots.len();
        let prefetch_floor = LOOKAHEAD.min(n);
        for &idx in &indices[..prefetch_floor] {
            if idx < cap {
                prefetch_read(&self.slots[idx]);
            }
        }
        for k in 0..n {
            let ahead_pos = k + LOOKAHEAD;
            if ahead_pos < n {
                let ahead = indices[ahead_pos];
                if ahead < cap {
                    prefetch_read(&self.slots[ahead]);
                }
            }
            let idx = indices[k];
            out[k] = self.slots[idx].load(Ordering::Relaxed) & VALUE_MASK;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Put,
    Delete,
}

#[derive(Debug)]
pub struct FlushRecord<K, V> {
    pub key: K,
    pub value: Option<Arc<V>>,
    pub version: u64,
    pub op: Operation,
}

impl<K: Clone, V> Clone for FlushRecord<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            version: self.version,
            op: self.op,
        }
    }
}

/// Write-behind target for dirty records.
///
/// Implementations should be idempotent/version-aware: the store can retry the
/// same records if a previous flush returns an error or crashes mid-flight.
pub trait FlushBackend<K, V> {
    type Error;

    fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy)]
pub struct StoreConfig {
    pub shards: usize,
    pub initial_capacity_per_shard: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            shards: std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(2))
                .unwrap_or(16)
                .next_power_of_two(),
            initial_capacity_per_shard: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub shards: usize,
    pub len: usize,
    pub dirty: usize,
    pub logical_clock: u64,
}

struct Slot<V> {
    value: Option<Arc<V>>,
    version: u64,
    last_touch: AtomicU64,
    dirty: AtomicBool,
    deleted: bool,
}

impl<V> Slot<V> {
    #[inline]
    fn put(value: V, version: u64, tick: u64, dirty: bool) -> Self {
        Self {
            value: Some(Arc::new(value)),
            version,
            last_touch: AtomicU64::new(tick),
            dirty: AtomicBool::new(dirty),
            deleted: false,
        }
    }

    #[inline]
    fn delete(version: u64, tick: u64) -> Self {
        Self {
            value: None,
            version,
            last_touch: AtomicU64::new(tick),
            dirty: AtomicBool::new(true),
            deleted: true,
        }
    }
}

type ShardTable<K, V> = HashTable<(K, Slot<V>)>;
type Shard<K, V> = CachePadded<RwLock<ShardTable<K, V>>>;

pub struct MemoryFirstStore<K, V, S = FastBuildHasher> {
    shards: Box<[Shard<K, V>]>,
    clock: CachePadded<AtomicU64>,
    hash_builder: S,
}

impl<K, V> MemoryFirstStore<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new() -> Self {
        Self::with_config(StoreConfig::default())
    }

    pub fn with_config(config: StoreConfig) -> Self {
        Self::with_hasher_and_config(FastBuildHasher::default(), config)
    }
}

impl<K, V, S> MemoryFirstStore<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    pub fn with_hasher_and_config(hash_builder: S, config: StoreConfig) -> Self {
        let shard_count = config.shards.max(1).next_power_of_two();
        let mut shards: Vec<Shard<K, V>> = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(CachePadded::new(RwLock::new(HashTable::with_capacity(
                config.initial_capacity_per_shard,
            ))));
        }
        Self {
            shards: shards.into_boxed_slice(),
            clock: CachePadded::new(AtomicU64::new(1)),
            hash_builder,
        }
    }
}

impl<K, V> Default for MemoryFirstStore<K, V>
where
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> MemoryFirstStore<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    #[inline]
    fn shard_count_mask(&self) -> usize {
        self.shards.len() - 1
    }

    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        self.hash_builder.hash_one(key)
    }

    #[inline]
    fn shard_for_hash(&self, hash: u64) -> &RwLock<ShardTable<K, V>> {
        &self.shards[hash as usize & self.shard_count_mask()]
    }

    #[inline]
    fn entry_hasher<'a>(&'a self) -> impl Fn(&(K, Slot<V>)) -> u64 + 'a {
        move |(k, _)| self.hash_builder.hash_one(k)
    }

    /// Sampled access tracking. Only updates `last_touch` on ~1/64 of gets
    /// to keep the global clock cache line cold.
    #[inline]
    fn record_touch(&self, hash: u64, slot: &Slot<V>) {
        if hash & SAMPLE_MASK == 0 {
            let tick = self.clock.fetch_add(1, Ordering::Relaxed);
            slot.last_touch.store(tick, Ordering::Relaxed);
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let hash = self.hash_key(key);
        let table = self.shard_for_hash(hash).read();
        let (_, slot) = table.find(hash, |(k, _)| k == key)?;
        if slot.deleted {
            return None;
        }
        self.record_touch(hash, slot);
        slot.value.clone()
    }

    /// Closure-based read that holds the shard read guard for the duration
    /// of `f` and returns whatever `f` produces. Skips `Arc::clone` on the
    /// hot path entirely.
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let hash = self.hash_key(key);
        let table = self.shard_for_hash(hash).read();
        let (_, slot) = table.find(hash, |(k, _)| k == key)?;
        if slot.deleted {
            return None;
        }
        self.record_touch(hash, slot);
        slot.value.as_ref().map(|arc| f(arc.as_ref()))
    }

    pub fn peek(&self, key: &K) -> Option<Arc<V>> {
        let hash = self.hash_key(key);
        let table = self.shard_for_hash(hash).read();
        let (_, slot) = table.find(hash, |(k, _)| k == key)?;
        if slot.deleted {
            return None;
        }
        slot.value.clone()
    }

    /// Pipelined batch get. Pre-hashes all keys, then walks contiguous
    /// runs of same-shard keys under a single read lock acquisition.
    ///
    /// `hashbrown::HashTable` uses SIMD-tagged probing internally, so the
    /// per-key probe is cache-efficient even without explicit user-side
    /// bucket prefetching.
    pub fn get_batch(&self, keys: &[K]) -> Vec<Option<Arc<V>>> {
        let n = keys.len();
        let mut out: Vec<Option<Arc<V>>> = Vec::with_capacity(n);
        if n == 0 {
            return out;
        }

        let mut hashes: Vec<u64> = Vec::with_capacity(n);
        let mut shard_idxs: Vec<usize> = Vec::with_capacity(n);
        let mask = self.shard_count_mask();
        for k in keys {
            let h = self.hash_key(k);
            hashes.push(h);
            shard_idxs.push(h as usize & mask);
        }

        const LOOKAHEAD: usize = 4;
        for &si in shard_idxs.iter().take(LOOKAHEAD.min(n)) {
            prefetch_read(&self.shards[si]);
        }

        let mut i = 0;
        while i < n {
            let ahead = i + LOOKAHEAD;
            if ahead < n {
                prefetch_read(&self.shards[shard_idxs[ahead]]);
            }
            let si = shard_idxs[i];
            let mut j = i + 1;
            while j < n && shard_idxs[j] == si {
                j += 1;
            }
            let table = self.shards[si].read();
            for k in i..j {
                let h = hashes[k];
                let key = &keys[k];
                let entry = table.find(h, |(stored_k, _)| stored_k == key);
                match entry {
                    Some((_, slot)) if !slot.deleted => {
                        self.record_touch(h, slot);
                        out.push(slot.value.clone());
                    }
                    _ => out.push(None),
                }
            }
            i = j;
        }
        out
    }

    pub fn put(&self, key: K, value: V) -> u64 {
        self.insert_with(key, |version, tick| {
            Slot::put(value, version, tick, /*dirty=*/ true)
        })
    }

    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.insert_with(key, |version, tick| {
            Slot::put(value, version, tick, /*dirty=*/ false)
        })
    }

    pub fn delete(&self, key: K) -> u64 {
        self.insert_with(key, |version, tick| Slot::delete(version, tick))
    }

    fn insert_with(&self, key: K, build_slot: impl FnOnce(u64, u64) -> Slot<V>) -> u64 {
        let hash = self.hash_key(&key);
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut table = self.shard_for_hash(hash).write();
        let mut next_version = 1u64;
        if let Some(existing) = table.find_mut(hash, |(k, _)| k == &key) {
            next_version = existing.1.version + 1;
            existing.1 = build_slot(next_version, tick);
            return next_version;
        }
        let slot = build_slot(next_version, tick);
        let entry_hasher = self.entry_hasher();
        table.insert_unique(hash, (key, slot), entry_hasher);
        next_version
    }

    pub fn stats(&self) -> StoreStats {
        let mut len = 0;
        let mut dirty = 0;
        for shard in self.shards.iter() {
            let table = shard.read();
            for (_, slot) in table.iter() {
                if !slot.deleted {
                    len += 1;
                }
                if slot.dirty.load(Ordering::Relaxed) {
                    dirty += 1;
                }
            }
        }
        StoreStats {
            shards: self.shards.len(),
            len,
            dirty,
            logical_clock: self.clock.load(Ordering::Relaxed),
        }
    }
}

impl<K, V, S> MemoryFirstStore<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    pub fn collect_idle_dirty(
        &self,
        idle_ticks: u64,
        max_records: usize,
    ) -> Vec<FlushRecord<K, V>> {
        let now = self.clock.load(Ordering::Relaxed);
        let mut records = Vec::new();
        'outer: for shard in self.shards.iter() {
            let table = shard.read();
            for (key, slot) in table.iter() {
                if records.len() >= max_records {
                    break 'outer;
                }
                if !slot.dirty.load(Ordering::Relaxed) {
                    continue;
                }
                let idle = now.saturating_sub(slot.last_touch.load(Ordering::Relaxed));
                if idle < idle_ticks {
                    continue;
                }
                if let Some(value) = &slot.value
                    && Arc::strong_count(value) != 1
                {
                    continue;
                }
                records.push(FlushRecord {
                    key: key.clone(),
                    value: slot.value.clone(),
                    version: slot.version,
                    op: if slot.deleted {
                        Operation::Delete
                    } else {
                        Operation::Put
                    },
                });
            }
        }
        records
    }

    pub fn mark_flushed_and_evict(&self, records: &[FlushRecord<K, V>]) -> usize {
        let mut evicted = 0;
        for record in records {
            let hash = self.hash_key(&record.key);
            let mut table = self.shard_for_hash(hash).write();
            let entry = table.find_entry(hash, |(k, _)| k == &record.key);
            let Ok(occupied) = entry else { continue };
            let (_, slot) = occupied.get();
            if slot.version != record.version {
                continue;
            }
            match (&slot.value, record.op) {
                (Some(value), Operation::Put) if Arc::strong_count(value) <= 2 => {
                    occupied.remove();
                    evicted += 1;
                }
                (None, Operation::Delete) => {
                    occupied.remove();
                    evicted += 1;
                }
                _ => {
                    let (_, slot) = occupied.into_mut();
                    slot.dirty.store(false, Ordering::Relaxed);
                }
            }
        }
        evicted
    }

    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>,
    {
        let records = self.collect_idle_dirty(idle_ticks, max_records);
        if records.is_empty() {
            return Ok(0);
        }
        backend.flush(&records)?;
        Ok(self.mark_flushed_and_evict(&records))
    }
}

impl<K, V, S> fmt::Debug for MemoryFirstStore<K, V, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryFirstStore")
            .field("shards", &self.shards.len())
            .field("logical_clock", &self.clock.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct VecBackend<K, V> {
        flushed: usize,
        _marker: std::marker::PhantomData<(K, V)>,
    }

    impl<K: Clone, V> FlushBackend<K, V> for VecBackend<K, V> {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            self.flushed += records.len();
            Ok(())
        }
    }

    #[test]
    fn put_get_delete() {
        let store = MemoryFirstStore::<u64, u64>::new();
        store.put(7, 11);
        assert_eq!(*store.get(&7).unwrap(), 11);
        store.delete(7);
        assert!(store.get(&7).is_none());
    }

    #[test]
    fn put_replaces_increments_version() {
        let store = MemoryFirstStore::<u64, u64>::new();
        let v1 = store.put(1, 100);
        let v2 = store.put(1, 200);
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(*store.get(&1).unwrap(), 200);
    }

    #[test]
    fn read_with_avoids_clone() {
        let store = MemoryFirstStore::<u64, u64>::new();
        store.put(1, 42);
        let doubled = store.read_with(&1, |v| v * 2);
        assert_eq!(doubled, Some(84));
    }

    #[test]
    fn get_batch_returns_in_order() {
        let store = MemoryFirstStore::<u64, u64>::new();
        for i in 0..16u64 {
            store.put(i, i * 10);
        }
        let keys: Vec<u64> = (0..16u64).collect();
        let results = store.get_batch(&keys);
        assert_eq!(results.len(), 16);
        for (i, slot) in results.into_iter().enumerate() {
            assert_eq!(*slot.unwrap(), (i as u64) * 10);
        }
    }

    #[test]
    fn flush_idle_only_evicts_unreferenced_versions() {
        let store = MemoryFirstStore::<u64, u64>::new();
        store.put(1, 10);
        store.put(2, 20);
        let pinned = store.get(&2).unwrap();
        for key in 10..18 {
            store.load_clean(key, key * 2);
        }

        let mut backend = VecBackend::default();
        let evicted = store.flush_idle(&mut backend, 2, 64).unwrap();
        assert_eq!(evicted, 1);
        assert!(store.get(&1).is_none());
        assert_eq!(*store.get(&2).unwrap(), 20);
        assert_eq!(*pinned, 20);
    }

    #[test]
    fn failed_flush_keeps_data_hot() {
        struct FailingBackend;
        impl FlushBackend<u64, u64> for FailingBackend {
            type Error = &'static str;

            fn flush(&mut self, _records: &[FlushRecord<u64, u64>]) -> Result<(), Self::Error> {
                Err("down")
            }
        }

        let store = MemoryFirstStore::<u64, u64>::new();
        store.put(1, 99);
        for _ in 0..4 {
            store.peek(&1);
        }
        assert!(store.flush_idle(&mut FailingBackend, 1, 64).is_err());
        assert_eq!(*store.get(&1).unwrap(), 99);
    }

    #[test]
    fn dense_lane_tracks_dirty_values() {
        let lane = DenseU64Lane::with_len(4);
        lane.store(2, 77);
        assert_eq!(lane.load(2), 77);
        assert!(lane.is_dirty(2));
        assert_eq!(lane.fetch_add(2, 5), 77);
        assert_eq!(lane.load(2), 82);
        assert_eq!(lane.dirty_values(8), vec![(2, 82)]);
        lane.mark_clean(2);
        assert!(!lane.is_dirty(2));
        assert!(lane.dirty_values(8).is_empty());
    }

    #[test]
    fn dense_lane_load_many_pipelines_correctly() {
        let lane = DenseU64Lane::with_len(64);
        for i in 0..64usize {
            lane.store(i, i as u64);
        }
        let indices: Vec<usize> = (0..64usize).rev().collect();
        let mut out = vec![0u64; indices.len()];
        lane.load_many(&indices, &mut out);
        for (k, &i) in indices.iter().enumerate() {
            assert_eq!(out[k], i as u64);
        }
    }

    #[test]
    fn dense_lane_value_max_round_trips() {
        let lane = DenseU64Lane::with_len(1);
        lane.store(0, DENSE_VALUE_MAX);
        assert_eq!(lane.load(0), DENSE_VALUE_MAX);
        assert!(lane.is_dirty(0));
    }

    #[cfg(feature = "ahash")]
    #[test]
    fn ahash_hasher_works() {
        let store = MemoryFirstStore::<String, u64, AHashState>::with_hasher_and_config(
            AHashState::new(),
            StoreConfig::default(),
        );
        store.put("hello".to_string(), 123);
        assert_eq!(*store.get(&"hello".to_string()).unwrap(), 123);
    }

    #[test]
    fn auto_thread_count_policy() {
        let nproc = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let half = (nproc / 2).max(1);

        assert_eq!(auto_thread_count(None), half);
        assert_eq!(auto_thread_count(Some(0)), half);
        assert_eq!(auto_thread_count(Some(1)), 1);
        assert_eq!(auto_thread_count(Some(nproc)), nproc);
        // Over-request silently falls back to the safe default.
        assert_eq!(auto_thread_count(Some(nproc + 1)), half);
        assert_eq!(auto_thread_count(Some(nproc * 4)), half);
        // Always at least 1.
        assert!(auto_thread_count(None) >= 1);
    }
}
