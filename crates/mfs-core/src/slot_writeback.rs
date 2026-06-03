//! Generic slot-index write-behind cache for arbitrary value types.
//!
//! This is the second v3 slice after [`crate::dense_writeback_map`]. It keeps
//! `ConcurrentMap<K, packed_slot_handle>` as an index and stores the live
//! `Arc<V>` pointer, version, dirty bit, and generation in preallocated slots.
//! Existing-key writes do not replace the hash-table entry; they swap the slot's
//! value pointer and enqueue dirty work.

use crate::concurrent_map::{ConcurrentMap, InsertOutcome};
use crate::writeback::{AutoFlusherConfig, WriteBehindConfig, WriteBehindError};
use crate::{FastBuildHasher, FlushBackend, FlushRecord, Operation};
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::{Backoff, CachePadded};
use seize::{Collector, Guard, LocalGuard};
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash};
use std::hint::spin_loop;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

const SLOT_WRITE_BIT: u32 = 1;
const DIRTY_VERSION_BIT: u64 = 1;

#[derive(Clone)]
struct DirtyEntry<K> {
    key: K,
    version: u64,
    pushed_at: u64,
    op: Operation,
    slot: u32,
}

type DrainedEntry<K> = (usize, DirtyEntry<K>);
type DrainBatch<K, V> = (Vec<FlushRecord<K, V>>, Vec<DrainedEntry<K>>);
type FlusherWakeup = Arc<[CachePadded<(Mutex<bool>, Condvar)>]>;
type DeferredDirty<K> = Box<[CachePadded<Mutex<VecDeque<DirtyEntry<K>>>>]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotWriteBehindStats {
    pub len: usize,
    pub dirty: usize,
    pub logical_clock: u64,
}

pub struct SlotWriteBehindCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    index: ConcurrentMap<K, u64, S>,
    values: Box<[AtomicPtr<V>]>,
    generations: Box<[AtomicU32]>,
    versions: Box<[AtomicU64]>,
    free: CachePadded<ArrayQueue<u32>>,
    next_slot: CachePadded<AtomicU32>,
    capacity: u32,
    dirty_shards: Box<[CachePadded<ArrayQueue<DirtyEntry<K>>>]>,
    deferred_dirty: DeferredDirty<K>,
    flusher_wakeup: FlusherWakeup,
    clock: CachePadded<AtomicU64>,
    hash_builder: S,
    collector: Collector,
}

impl<K, V> SlotWriteBehindCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
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

impl<K, V, S> SlotWriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        let cap = u32::try_from(config.initial_capacity.max(1))
            .expect("SlotWriteBehindCache capacity exceeds u32::MAX");
        let values: Vec<AtomicPtr<V>> = (0..cap)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        let generations: Vec<AtomicU32> = (0..cap).map(|_| AtomicU32::new(0)).collect();
        let versions: Vec<AtomicU64> = (0..cap).map(|_| AtomicU64::new(0)).collect();
        let dirty_shards = config.dirty_shards.max(1).next_power_of_two();
        let dirty_capacity = config.dirty_queue_capacity.max(1);
        let dirty: Vec<_> = (0..dirty_shards)
            .map(|_| CachePadded::new(ArrayQueue::new(dirty_capacity)))
            .collect();
        let deferred: Vec<_> = (0..dirty_shards)
            .map(|_| CachePadded::new(Mutex::new(VecDeque::new())))
            .collect();
        let parks: Vec<_> = (0..dirty_shards)
            .map(|_| CachePadded::new((Mutex::new(false), Condvar::new())))
            .collect();
        Self {
            index: ConcurrentMap::with_hasher_and_capacity(hash_builder.clone(), cap as usize),
            values: values.into_boxed_slice(),
            generations: generations.into_boxed_slice(),
            versions: versions.into_boxed_slice(),
            free: CachePadded::new(ArrayQueue::new(cap as usize)),
            next_slot: CachePadded::new(AtomicU32::new(0)),
            capacity: cap,
            dirty_shards: dirty.into_boxed_slice(),
            deferred_dirty: deferred.into_boxed_slice(),
            flusher_wakeup: Arc::from(parks.into_boxed_slice()),
            clock: CachePadded::new(AtomicU64::new(1)),
            hash_builder,
            collector: Collector::new().batch_size(crate::concurrent_map::DEFAULT_RETIRE_BATCH),
        }
    }

    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            cache: self,
            index: self.index.pin(),
            value_guard: self.collector.enter(),
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.pin().get(key)
    }

    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.pin().read_with(key, f)
    }

    pub fn put(&self, key: K, value: V) -> u64 {
        self.pin().put(key, value)
    }

    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.pin().try_put(key, value)
    }

    pub fn put_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.pin().put_arc(key, value)
    }

    pub fn try_put_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.pin().try_put_arc(key, value)
    }

    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.pin().load_clean(key, value)
    }

    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.pin().try_load_clean(key, value)
    }

    pub fn load_clean_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.pin().load_clean_arc(key, value)
    }

    pub fn try_load_clean_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.pin().try_load_clean_arc(key, value)
    }

    pub fn delete(&self, key: K) -> u64 {
        self.pin().delete(key)
    }

    pub fn try_delete(&self, key: K) -> Result<u64, WriteBehindError> {
        self.pin().try_delete(key)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn stats(&self) -> SlotWriteBehindStats {
        SlotWriteBehindStats {
            len: self.len(),
            dirty: self.dirty_shards.iter().map(|q| q.len()).sum::<usize>()
                + (0..self.deferred_dirty.len())
                    .map(|idx| self.deferred_dirty_len(idx))
                    .sum::<usize>(),
            logical_clock: self.clock.load(Ordering::Relaxed),
        }
    }

    pub fn shard_count(&self) -> usize {
        self.dirty_shards.len()
    }

    pub fn shard_dirty_depth(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].len() + self.deferred_dirty_len(shard_idx)
    }

    pub fn shard_dirty_capacity(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].capacity()
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
        let (records, drained) = self.drain_eligible(idle_ticks, max_records);
        self.flush_drained(backend, records, drained)
    }

    pub fn flush_shard_idle<B>(
        &self,
        shard_idx: usize,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>,
    {
        let (records, drained) =
            self.drain_eligible_shards(shard_idx..shard_idx + 1, idle_ticks, max_records);
        self.flush_drained(backend, records, drained)
    }

    fn flush_drained<B>(
        &self,
        backend: &mut B,
        records: Vec<FlushRecord<K, V>>,
        drained: Vec<DrainedEntry<K>>,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>,
    {
        if records.is_empty() {
            return Ok(0);
        }
        match backend.flush(&records) {
            Ok(()) => {
                let n = records.len();
                self.cleanup_after_flush(&drained);
                Ok(n)
            }
            Err(e) => {
                self.requeue(drained);
                Err(e)
            }
        }
    }

    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        self.hash_builder.hash_one(key)
    }

    #[inline]
    fn dirty_shard_idx(&self, hash: u64) -> usize {
        (hash as usize) & (self.dirty_shards.len() - 1)
    }

    #[inline]
    fn next_absent_delete_version(&self) -> u64 {
        self.next_dirty_tick()
    }

    #[inline]
    fn next_dirty_tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn version_from_word(word: u64) -> u64 {
        word >> 1
    }

    #[inline]
    fn next_slot_version(&self, slot: u32) -> u64 {
        Self::version_from_word(self.versions[slot as usize].load(Ordering::Relaxed))
            .wrapping_add(1)
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

    fn unlock_slot(&self, slot: u32, generation: u32) {
        self.generations[slot as usize].store(generation, Ordering::Release);
    }

    fn retire_locked_slot(&self, slot: u32, generation: u32) {
        let old = self.values[slot as usize].swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !old.is_null() {
            let guard = self.collector.enter();
            unsafe { guard.defer_retire(old, retire_arc_raw::<V>) };
        }
        let v = self.versions[slot as usize].load(Ordering::Relaxed);
        self.versions[slot as usize].store(v & !DIRTY_VERSION_BIT, Ordering::Release);
        self.generations[slot as usize].store(generation.wrapping_add(2), Ordering::Release);
        let _ = self.free.push(slot);
    }

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
                    self.retire_locked_slot(slot, generation);
                    return;
                }
                Err(current) if current == (generation | SLOT_WRITE_BIT) => spin_loop(),
                Err(_) => return,
            }
        }
    }

    fn write_slot(
        &self,
        slot: u32,
        value: Arc<V>,
        mark_dirty: bool,
        guard: &LocalGuard<'_>,
    ) -> (u64, bool) {
        let new_raw = Arc::into_raw(value) as *mut V;
        let old = self.values[slot as usize].swap(new_raw, Ordering::AcqRel);
        if !old.is_null() {
            unsafe { guard.defer_retire(old, retire_arc_raw::<V>) };
        }
        let pre_load = self.versions[slot as usize].load(Ordering::Relaxed);
        let version = Self::version_from_word(pre_load).wrapping_add(1);
        let new_word = (version << 1) | if mark_dirty { DIRTY_VERSION_BIT } else { 0 };
        let old_word = self.versions[slot as usize].swap(new_word, Ordering::AcqRel);
        (version, old_word & DIRTY_VERSION_BIT == 0)
    }

    fn read_arc_handle<'g>(
        &self,
        handle: u64,
        guard: &'g LocalGuard<'g>,
    ) -> Option<(*mut V, &'g V, u64, bool)> {
        let slot = handle_slot(handle);
        let generation = handle_generation(handle);
        loop {
            if self.generations[slot as usize].load(Ordering::Acquire) != generation {
                return None;
            }
            let v1 = self.versions[slot as usize].load(Ordering::Acquire);
            let ptr = guard.protect(&self.values[slot as usize], Ordering::Acquire);
            if ptr.is_null() {
                return None;
            }
            let value = unsafe { &*ptr };
            let v2 = self.versions[slot as usize].load(Ordering::Acquire);
            let g2 = self.generations[slot as usize].load(Ordering::Acquire);
            if v1 == v2 && g2 == generation {
                return Some((
                    ptr,
                    value,
                    Self::version_from_word(v2),
                    v2 & DIRTY_VERSION_BIT != 0,
                ));
            }
            spin_loop();
        }
    }

    fn push_dirty_with_backoff(&self, shard_idx: usize, mut entry: DirtyEntry<K>) {
        let queue = &self.dirty_shards[shard_idx];
        let cap = queue.capacity();
        match queue.push(entry) {
            Ok(()) => {
                if queue.len().saturating_mul(2) >= cap {
                    self.notify_flusher(shard_idx);
                }
                return;
            }
            Err(e) => entry = e,
        }
        self.notify_flusher(shard_idx);
        let backoff = Backoff::new();
        loop {
            match queue.push(entry) {
                Ok(()) => return,
                Err(e) => {
                    entry = e;
                    backoff.snooze();
                }
            }
        }
    }

    fn notify_flusher(&self, shard_idx: usize) {
        let park = &self.flusher_wakeup[shard_idx];
        let mut pending = match park.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !*pending {
            *pending = true;
            park.1.notify_one();
        }
    }

    fn deferred_dirty_len(&self, shard_idx: usize) -> usize {
        match self.deferred_dirty[shard_idx].lock() {
            Ok(deferred) => deferred.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }

    fn pop_deferred_dirty_for_drain(&self, shard_idx: usize) -> Option<DirtyEntry<K>> {
        let mut deferred = match self.deferred_dirty[shard_idx].lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        deferred.pop_front()
    }

    fn defer_dirty_from_flusher(&self, shard_idx: usize, entry: DirtyEntry<K>) {
        let queue = &self.dirty_shards[shard_idx];
        let cap = queue.capacity();
        match queue.push(entry) {
            Ok(()) => {
                if queue.len().saturating_mul(2) >= cap {
                    self.notify_flusher(shard_idx);
                }
            }
            Err(entry) => {
                let mut deferred = match self.deferred_dirty[shard_idx].lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                deferred.push_back(entry);
                drop(deferred);
                self.notify_flusher(shard_idx);
            }
        }
    }

    fn drain_eligible(&self, idle_ticks: u64, max_records: usize) -> DrainBatch<K, V> {
        self.drain_eligible_shards(0..self.dirty_shards.len(), idle_ticks, max_records)
    }

    fn drain_eligible_shards(
        &self,
        shard_range: std::ops::Range<usize>,
        idle_ticks: u64,
        max_records: usize,
    ) -> DrainBatch<K, V> {
        let now = self.clock.load(Ordering::Relaxed);
        let mut records = Vec::new();
        let mut drained = Vec::new();
        let p = self.index.pin();
        let guard = self.collector.enter();
        macro_rules! drain_entry {
            ($shard_idx:expr, $entry:expr) => {{
                let mut entry = $entry;
                if now.saturating_sub(entry.pushed_at) < idle_ticks {
                    self.defer_dirty_from_flusher($shard_idx, entry);
                    continue;
                }
                match entry.op {
                    Operation::Put => {
                        let Some(handle) = p.get(&entry.key).copied() else {
                            continue;
                        };
                        let Some((raw, _, version, is_dirty)) =
                            self.read_arc_handle(handle, &guard)
                        else {
                            self.defer_dirty_from_flusher($shard_idx, entry);
                            continue;
                        };
                        if !is_dirty {
                            continue;
                        }
                        if version != entry.version || handle_slot(handle) != entry.slot {
                            entry.version = version;
                            entry.slot = handle_slot(handle);
                            if idle_ticks > 0 {
                                entry.pushed_at = now;
                                self.defer_dirty_from_flusher($shard_idx, entry);
                                continue;
                            }
                        }
                        entry.version = version;
                        entry.slot = handle_slot(handle);
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value: Some(clone_arc_raw(raw)),
                            version,
                            op: Operation::Put,
                        });
                        drained.push(($shard_idx, entry));
                    }
                    Operation::Delete => {
                        if p.get(&entry.key).is_some() {
                            continue;
                        }
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value: None,
                            version: entry.version,
                            op: Operation::Delete,
                        });
                        drained.push(($shard_idx, entry));
                    }
                }
            }};
        }
        'outer: for shard_idx in shard_range {
            let shard = &self.dirty_shards[shard_idx];
            let deferred_snapshot_len = self.deferred_dirty_len(shard_idx);
            for _ in 0..deferred_snapshot_len {
                if records.len() >= max_records {
                    break 'outer;
                }
                let Some(entry) = self.pop_deferred_dirty_for_drain(shard_idx) else {
                    break;
                };
                drain_entry!(shard_idx, entry);
            }
            let ring_snapshot_len = shard.len();
            for _ in 0..ring_snapshot_len {
                if records.len() >= max_records {
                    break 'outer;
                }
                let Some(entry) = shard.pop() else { break };
                drain_entry!(shard_idx, entry);
            }
        }
        (records, drained)
    }

    fn cleanup_after_flush(&self, drained: &[DrainedEntry<K>]) {
        for (_, entry) in drained {
            if entry.op != Operation::Put {
                continue;
            }
            let expected = (entry.version << 1) | DIRTY_VERSION_BIT;
            let cleared = entry.version << 1;
            if self.versions[entry.slot as usize]
                .compare_exchange(expected, cleared, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                continue;
            }
            let Some(handle) = self.index.get_owned(&entry.key) else {
                continue;
            };
            if handle_slot(handle) != entry.slot {
                continue;
            }
            let current_word = self.versions[entry.slot as usize].load(Ordering::Acquire);
            if current_word & DIRTY_VERSION_BIT == 0 {
                continue;
            }
            let current_version = Self::version_from_word(current_word);
            let shard_idx = self.dirty_shard_idx(self.hash_key(&entry.key));
            self.defer_dirty_from_flusher(
                shard_idx,
                DirtyEntry {
                    key: entry.key.clone(),
                    version: current_version,
                    pushed_at: self.clock.load(Ordering::Relaxed),
                    op: Operation::Put,
                    slot: entry.slot,
                },
            );
        }
    }

    fn requeue(&self, drained: Vec<DrainedEntry<K>>) {
        for (shard_idx, entry) in drained {
            self.defer_dirty_from_flusher(shard_idx, entry);
        }
    }
}

impl<K, V, S> Drop for SlotWriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    fn drop(&mut self) {
        for slot in self.values.iter_mut() {
            let raw = *slot.get_mut();
            if !raw.is_null() {
                unsafe {
                    drop(Arc::from_raw(raw as *const V));
                }
            }
        }
    }
}

/// One adaptive flusher thread per slot dirty shard.
///
/// This is deliberately a flusher, not a queued mutation wrapper: callers still
/// use synchronous `put`/`delete`/`try_*` methods, so write completion means the
/// in-memory slot update and dirty enqueue finished. While this handle is
/// running, do not manually drain the same cache shard from another thread;
/// each spawned worker owns its shard/backend until [`SlotAutoFlusher::stop`]
/// joins it and performs the final drain passes.
pub struct SlotAutoFlusher {
    handles: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    flusher_wakeup: FlusherWakeup,
}

impl SlotAutoFlusher {
    pub fn spawn<K, V, B, F>(
        cache: Arc<SlotWriteBehindCache<K, V>>,
        mut backend_factory: F,
        config: AutoFlusherConfig,
    ) -> Self
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
        V: Send + Sync + 'static,
        B: FlushBackend<K, V> + Send + 'static,
        F: FnMut(usize) -> B,
    {
        let shards = cache.shard_count();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flusher_wakeup = Arc::clone(&cache.flusher_wakeup);
        let mut handles = Vec::with_capacity(shards);
        for shard_idx in 0..shards {
            let backend = backend_factory(shard_idx);
            let cache = Arc::clone(&cache);
            let stop = Arc::clone(&stop);
            let cfg = config;
            handles.push(std::thread::spawn(move || {
                run_slot_flusher_loop(shard_idx, cache, backend, stop, cfg);
            }));
        }
        Self {
            handles,
            stop,
            flusher_wakeup,
        }
    }

    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for park in self.flusher_wakeup.iter() {
            let mut pending = match park.0.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *pending = true;
            park.1.notify_one();
        }
        for handle in self.handles {
            let _ = handle.join();
        }
    }
}

fn run_slot_flusher_loop<K, V, B>(
    shard_idx: usize,
    cache: Arc<SlotWriteBehindCache<K, V>>,
    mut backend: B,
    stop: Arc<std::sync::atomic::AtomicBool>,
    cfg: AutoFlusherConfig,
) where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    B: FlushBackend<K, V>,
{
    let mut tick = std::time::Duration::from_millis(cfg.max_tick_ms);
    while !stop.load(Ordering::Relaxed) {
        let _ = cache.flush_shard_idle(
            shard_idx,
            &mut backend,
            cfg.idle_ticks_threshold,
            cfg.max_records_per_drain,
        );
        let depth = cache.shard_dirty_depth(shard_idx);
        tick = adapt_tick(
            tick,
            depth,
            cfg.target_depth,
            cfg.min_tick_ms,
            cfg.max_tick_ms,
        );
        let park = &cache.flusher_wakeup[shard_idx];
        let mut pending = match park.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *pending {
            *pending = false;
        } else {
            let (guard, _) = match park.1.wait_timeout(pending, tick) {
                Ok(result) => result,
                Err(poisoned) => poisoned.into_inner(),
            };
            pending = guard;
            *pending = false;
        }
        drop(pending);
    }
    for _ in 0..cfg.final_drain_passes {
        let drained = cache
            .flush_shard_idle(shard_idx, &mut backend, 0, cfg.max_records_per_drain)
            .unwrap_or(0);
        if drained == 0 {
            break;
        }
    }
}

#[inline]
fn adapt_tick(
    current: std::time::Duration,
    observed: usize,
    target: usize,
    min_ms: u64,
    max_ms: u64,
) -> std::time::Duration {
    let min = std::time::Duration::from_millis(min_ms);
    let max = std::time::Duration::from_millis(max_ms);
    let clamp = |duration: std::time::Duration| {
        if duration < min {
            min
        } else if duration > max {
            max
        } else {
            duration
        }
    };
    if observed >= target.saturating_mul(2) {
        min
    } else if observed >= target {
        clamp(current.mul_f64(0.6))
    } else if observed >= target / 2 {
        clamp(current)
    } else {
        clamp(current.mul_f64(1.5))
    }
}

pub struct Pinned<'g, K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    cache: &'g SlotWriteBehindCache<K, V, S>,
    index: crate::concurrent_map::Pinned<'g, K, u64, S>,
    value_guard: LocalGuard<'g>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let handle = self.index.get(key).copied()?;
        let (raw, _, _, _) = self.cache.read_arc_handle(handle, &self.value_guard)?;
        Some(clone_arc_raw(raw))
    }

    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let handle = self.index.get(key).copied()?;
        let (_, value, _, _) = self.cache.read_arc_handle(handle, &self.value_guard)?;
        Some(f(value))
    }

    pub fn put(&self, key: K, value: V) -> u64 {
        self.try_put(key, value)
            .expect("SlotWriteBehindCache is full")
    }

    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, true)
    }

    pub fn put_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_put_arc(key, value)
            .expect("SlotWriteBehindCache is full")
    }

    pub fn try_put_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.write(key, Some(value), Operation::Put, true)
    }

    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.try_load_clean(key, value)
            .expect("SlotWriteBehindCache is full")
    }

    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, false)
    }

    pub fn load_clean_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_load_clean_arc(key, value)
            .expect("SlotWriteBehindCache is full")
    }

    pub fn try_load_clean_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.write(key, Some(value), Operation::Put, false)
    }

    pub fn delete(&self, key: K) -> u64 {
        self.try_delete(key).expect("SlotWriteBehindCache is full")
    }

    pub fn try_delete(&self, key: K) -> Result<u64, WriteBehindError> {
        self.write(key, None, Operation::Delete, true)
    }

    fn write(
        &self,
        key: K,
        value: Option<Arc<V>>,
        op: Operation,
        queue_dirty: bool,
    ) -> Result<u64, WriteBehindError> {
        let hash = self.cache.hash_key(&key);
        if let Some(value) = value {
            loop {
                if let Some(handle) = self.index.get(&key).copied() {
                    let slot = handle_slot(handle);
                    let generation = handle_generation(handle);
                    if !self.cache.try_lock_slot_generation(slot, generation) {
                        spin_loop();
                        continue;
                    }
                    let still_current =
                        matches!(self.index.get(&key), Some(&current) if current == handle);
                    if still_current {
                        let pushed_at = if queue_dirty {
                            self.cache.next_dirty_tick()
                        } else {
                            0
                        };
                        let (version, was_clean) =
                            self.cache
                                .write_slot(slot, value, queue_dirty, &self.value_guard);
                        self.cache.unlock_slot(slot, generation);
                        if queue_dirty && was_clean {
                            self.cache.push_dirty_with_backoff(
                                self.cache.dirty_shard_idx(hash),
                                DirtyEntry {
                                    key,
                                    version,
                                    pushed_at,
                                    op,
                                    slot,
                                },
                            );
                        }
                        return Ok(version);
                    }
                    self.cache.unlock_slot(slot, generation);
                    spin_loop();
                    continue;
                }
                break;
            }
            let Some(slot) = self.cache.next_slot_or_recycle() else {
                return Err(WriteBehindError::CapacityFull);
            };
            let generation = self.cache.lock_slot(slot);
            let handle = pack_handle(slot, generation);
            let pushed_at = if queue_dirty {
                self.cache.next_dirty_tick()
            } else {
                0
            };
            let (version, was_clean) =
                self.cache
                    .write_slot(slot, value, queue_dirty, &self.value_guard);
            match self.index.insert_returning_old(key.clone(), handle) {
                (InsertOutcome::Inserted | InsertOutcome::Replaced, old_handle) => {
                    self.cache.unlock_slot(slot, generation);
                    if let Some(old_handle) = old_handle {
                        self.cache.retire_slot(old_handle);
                    }
                    if queue_dirty && was_clean {
                        self.cache.push_dirty_with_backoff(
                            self.cache.dirty_shard_idx(hash),
                            DirtyEntry {
                                key,
                                version,
                                pushed_at,
                                op,
                                slot,
                            },
                        );
                    }
                    Ok(version)
                }
                (InsertOutcome::Full, old_handle) => {
                    if let Some(old_handle) = old_handle {
                        self.cache.retire_slot(old_handle);
                    }
                    self.cache.retire_locked_slot(slot, generation);
                    Err(WriteBehindError::CapacityFull)
                }
            }
        } else {
            loop {
                let Some(handle) = self.index.get(&key).copied() else {
                    let version = self.cache.next_absent_delete_version();
                    self.cache.push_dirty_with_backoff(
                        self.cache.dirty_shard_idx(hash),
                        DirtyEntry {
                            key,
                            version,
                            pushed_at: version,
                            op,
                            slot: 0,
                        },
                    );
                    return Ok(version);
                };
                let slot = handle_slot(handle);
                let generation = handle_generation(handle);
                if !self.cache.try_lock_slot_generation(slot, generation) {
                    spin_loop();
                    continue;
                }
                if self.index.remove_if_value(&key, &handle) {
                    let version = self.cache.next_slot_version(slot);
                    let pushed_at = self.cache.next_dirty_tick();
                    self.cache.retire_locked_slot(slot, generation);
                    self.cache.push_dirty_with_backoff(
                        self.cache.dirty_shard_idx(hash),
                        DirtyEntry {
                            key,
                            version,
                            pushed_at,
                            op,
                            slot,
                        },
                    );
                    return Ok(version);
                }
                self.cache.unlock_slot(slot, generation);
                spin_loop();
            }
        }
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

fn clone_arc_raw<V>(raw: *mut V) -> Arc<V> {
    // `raw` must come from `read_arc_handle` while its seize guard is alive.
    // The guard keeps the slot-owned strong ref from being retired between the
    // load and this refcount bump; the returned Arc owns the bumped strong ref.
    unsafe {
        Arc::increment_strong_count(raw as *const V);
        Arc::from_raw(raw as *const V)
    }
}

unsafe fn retire_arc_raw<V>(raw: *mut V, _: &Collector) {
    // `raw` was produced by `Arc::into_raw` when the slot stored the value.
    // Rebuilding and dropping the Arc releases exactly that slot-owned ref.
    unsafe {
        drop(Arc::from_raw(raw as *const V));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{BuildHasherDefault, Hasher};
    use std::sync::Mutex;

    #[derive(Default)]
    struct ConstantHasher;

    impl Hasher for ConstantHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[derive(Default)]
    struct CollectBackend<K, V> {
        records: Mutex<Vec<FlushRecord<K, V>>>,
        fail_next: std::sync::atomic::AtomicBool,
    }

    impl<K: Clone, V> FlushBackend<K, V> for CollectBackend<K, V> {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            if self.fail_next.swap(false, Ordering::Relaxed) {
                return Err(());
            }
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct SharedRecords<K, V>(Arc<Mutex<Vec<FlushRecord<K, V>>>>);

    impl<K: Clone, V> FlushBackend<K, V> for SharedRecords<K, V> {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            self.0.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    #[test]
    fn string_values_round_trip() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        cache.put(1, "beta".to_string());
        assert_eq!(cache.read_with(&1, |v| v.clone()), Some("beta".to_string()));
    }

    #[test]
    fn returned_arc_survives_replace_and_delete() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        let held = cache.get(&1).expect("value exists");

        cache.put(1, "beta".to_string());
        cache.delete(1);

        assert_eq!(held.as_str(), "alpha");
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn put_arc_preserves_allocation_identity() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        let value = Arc::new("alpha".to_string());
        let original_ptr = Arc::as_ptr(&value);

        cache.put_arc(1, Arc::clone(&value));
        let stored = cache.get(&1).expect("stored value");

        assert_eq!(Arc::as_ptr(&stored), original_ptr);
        assert_eq!(stored.as_str(), "alpha");
    }

    #[test]
    fn put_arc_accepts_owned_arc() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        let value = Arc::new("owned".to_string());
        let original_ptr = Arc::as_ptr(&value);

        cache.put_arc(1, value);
        let stored = cache.get(&1).expect("stored value");

        assert_eq!(Arc::as_ptr(&stored), original_ptr);
        assert_eq!(stored.as_str(), "owned");
    }

    #[test]
    fn load_clean_arc_does_not_flush() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.load_clean_arc(1, Arc::new("alpha".to_string()));
        let mut backend = CollectBackend::<u64, String>::default();

        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 0);
        assert_eq!(
            cache.read_with(&1, |value| value.clone()),
            Some("alpha".to_string())
        );
    }

    #[test]
    fn load_clean_arc_clears_dirty_state() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "dirty".to_string());
        cache.load_clean_arc(1, Arc::new("clean".to_string()));
        let mut backend = CollectBackend::<u64, String>::default();

        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 0);
        assert_eq!(
            cache.read_with(&1, |value| value.clone()),
            Some("clean".to_string())
        );
    }

    #[test]
    fn fallible_put_reports_capacity_full() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(1);
        assert_eq!(cache.try_put(1, "one".to_string()), Ok(1));
        assert_eq!(cache.try_put(1, "one again".to_string()), Ok(2));
        assert_eq!(
            cache.try_put(2, "two".to_string()),
            Err(WriteBehindError::CapacityFull)
        );
        assert_eq!(
            cache.try_load_clean(3, "three".to_string()),
            Err(WriteBehindError::CapacityFull)
        );
    }

    #[test]
    fn index_full_failure_retires_unpublished_slot() {
        type BadBuildHasher = BuildHasherDefault<ConstantHasher>;

        let cache = SlotWriteBehindCache::<u64, String, BadBuildHasher>::with_hasher_and_config(
            BadBuildHasher::default(),
            WriteBehindConfig {
                dirty_shards: 1,
                initial_capacity: 512,
                dirty_queue_capacity: 1024,
            },
        );
        let mut full_key = None;
        for key in 0..512u64 {
            match cache.try_put(key, format!("value-{key}")) {
                Ok(_) => {}
                Err(WriteBehindError::CapacityFull) => {
                    full_key = Some(key);
                    break;
                }
            }
        }
        let full_key = full_key.expect("constant hasher should hit index probe limit");

        cache.delete(0);
        let reused_key = full_key + 10_000;
        let reused_version = cache
            .try_put(reused_key, "reused-failed-slot".to_string())
            .expect(
                "new key should reuse the cleaned failed slot after one delete opens the index",
            );
        let mut backend = CollectBackend::<u64, String>::default();
        assert!(cache.flush_idle(&mut backend, 0, usize::MAX).unwrap() > 0);

        let records = backend.records.lock().unwrap();
        assert!(records.iter().any(|record| {
            record.key == reused_key
                && record.version == reused_version
                && record.value.as_deref().map(String::as_str) == Some("reused-failed-slot")
        }));
        assert!(cache.get(&full_key).is_none());
        assert_eq!(
            cache.get(&reused_key).as_deref().map(String::as_str),
            Some("reused-failed-slot")
        );
    }

    #[test]
    fn flush_emits_latest_arc_value() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        cache.put(1, "beta".to_string());
        let mut backend = CollectBackend::<u64, String>::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].value.as_deref().map(String::as_str),
            Some("beta")
        );
    }

    #[test]
    fn failed_flush_requeues_slot_entries() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        let latest = cache.put(1, "beta".to_string());
        let mut backend = CollectBackend::<u64, String>::default();
        backend.fail_next.store(true, Ordering::Relaxed);

        assert!(cache.flush_idle(&mut backend, 0, 1024).is_err());
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);

        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].version, latest);
        assert_eq!(
            records[0].value.as_deref().map(String::as_str),
            Some("beta")
        );
    }

    #[test]
    fn backend_error_requeue_does_not_spin_when_queue_refills() {
        struct RefillingBackend {
            cache: Arc<SlotWriteBehindCache<u64, String>>,
            records: Arc<Mutex<Vec<FlushRecord<u64, String>>>>,
            refilled: bool,
        }

        impl FlushBackend<u64, String> for RefillingBackend {
            type Error = ();

            fn flush(&mut self, records: &[FlushRecord<u64, String>]) -> Result<(), Self::Error> {
                if !self.refilled {
                    self.refilled = true;
                    self.cache.put(2, "two".to_string());
                    return Err(());
                }
                self.records.lock().unwrap().extend_from_slice(records);
                Ok(())
            }
        }

        let cache = Arc::new(SlotWriteBehindCache::<u64, String>::with_config(
            WriteBehindConfig {
                dirty_shards: 1,
                initial_capacity: 16,
                dirty_queue_capacity: 1,
            },
        ));
        cache.put(1, "one".to_string());
        let records = Arc::new(Mutex::new(Vec::new()));
        let mut backend = RefillingBackend {
            cache: Arc::clone(&cache),
            records: Arc::clone(&records),
            refilled: false,
        };

        assert!(cache.flush_idle(&mut backend, 0, 1024).is_err());
        assert_eq!(cache.stats().dirty, 2);
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 2);

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().any(|record| record.key == 1));
        assert!(records.iter().any(|record| record.key == 2));
    }

    #[test]
    fn deferred_young_entry_does_not_starve_ring_work() {
        let cache = SlotWriteBehindCache::<u64, String>::with_config(WriteBehindConfig {
            dirty_shards: 1,
            initial_capacity: 16,
            dirty_queue_capacity: 1,
        });
        cache.put(1, "one".to_string());
        let mut deferred_entry = cache.dirty_shards[0].pop().expect("key 1 dirty entry");
        cache.put(2, "two".to_string());
        let mut ring_entry = cache.dirty_shards[0].pop().expect("key 2 dirty entry");
        ring_entry.pushed_at = 0;
        assert!(cache.dirty_shards[0].push(ring_entry).is_ok());
        cache.clock.fetch_add(2_000, Ordering::Relaxed);
        deferred_entry.pushed_at = cache.clock.load(Ordering::Relaxed);
        cache.defer_dirty_from_flusher(0, deferred_entry);
        assert_eq!(cache.shard_dirty_depth(0), 2);

        let mut backend = CollectBackend::<u64, String>::default();
        assert_eq!(cache.flush_idle(&mut backend, 1_000, 1024).unwrap(), 1);

        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, 2);
        drop(records);
        assert_eq!(cache.shard_dirty_depth(0), 1);
    }

    #[test]
    fn flush_shard_idle_only_drains_one_shard() {
        let cache = SlotWriteBehindCache::<u64, String>::with_config(WriteBehindConfig {
            dirty_shards: 2,
            initial_capacity: 16,
            dirty_queue_capacity: 16,
        });
        for key in 0..16u64 {
            cache.put(key, format!("value-{key}"));
        }
        let target = (0..cache.shard_count())
            .find(|&idx| cache.shard_dirty_depth(idx) > 0)
            .expect("at least one dirty shard");
        let other_depths: Vec<_> = (0..cache.shard_count())
            .filter(|&idx| idx != target)
            .map(|idx| cache.shard_dirty_depth(idx))
            .collect();

        let mut backend = CollectBackend::<u64, String>::default();
        let drained = cache
            .flush_shard_idle(target, &mut backend, 0, usize::MAX)
            .unwrap();
        assert!(drained > 0);
        assert_eq!(cache.shard_dirty_depth(target), 0);
        let after_other_depths: Vec<_> = (0..cache.shard_count())
            .filter(|&idx| idx != target)
            .map(|idx| cache.shard_dirty_depth(idx))
            .collect();
        assert_eq!(after_other_depths, other_depths);
    }

    #[test]
    fn idle_flush_uses_logical_clock() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        let mut backend = CollectBackend::<u64, String>::default();

        assert_eq!(cache.flush_idle(&mut backend, 2, 1024).unwrap(), 0);
        cache.put(2, "beta".to_string());
        assert_eq!(cache.flush_idle(&mut backend, 2, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, 1);
    }

    #[test]
    fn delete_emits_delete_record() {
        let cache = SlotWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        cache.delete(1);
        assert!(cache.get(&1).is_none());
        let mut backend = CollectBackend::<u64, String>::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records[0].op, Operation::Delete);
        assert!(records[0].value.is_none());
    }

    #[test]
    fn auto_flusher_drains_in_background() {
        let cache = Arc::new(SlotWriteBehindCache::<u64, String>::with_config(
            WriteBehindConfig {
                dirty_shards: 2,
                initial_capacity: 64,
                dirty_queue_capacity: 64,
            },
        ));
        let flushed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        struct CountingBackend(Arc<std::sync::atomic::AtomicUsize>);

        impl FlushBackend<u64, String> for CountingBackend {
            type Error = ();

            fn flush(&mut self, records: &[FlushRecord<u64, String>]) -> Result<(), Self::Error> {
                self.0.fetch_add(records.len(), Ordering::Relaxed);
                Ok(())
            }
        }

        let flushed_factory = Arc::clone(&flushed);
        let auto = SlotAutoFlusher::spawn(
            Arc::clone(&cache),
            move |_| CountingBackend(Arc::clone(&flushed_factory)),
            AutoFlusherConfig {
                min_tick_ms: 1,
                max_tick_ms: 2,
                target_depth: 1,
                max_records_per_drain: 64,
                idle_ticks_threshold: 0,
                final_drain_passes: 8,
            },
        );

        for key in 0..16u64 {
            cache.put(key, format!("value-{key}"));
        }
        for _ in 0..50 {
            if flushed.load(Ordering::Relaxed) >= 16 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        auto.stop();

        assert!(flushed.load(Ordering::Relaxed) >= 16);
    }

    #[test]
    fn adaptive_slot_flusher_stop_drains_remaining() {
        let cache = Arc::new(SlotWriteBehindCache::<u64, String>::with_config(
            WriteBehindConfig {
                dirty_shards: 2,
                initial_capacity: 64,
                dirty_queue_capacity: 64,
            },
        ));
        let records = Arc::new(Mutex::new(Vec::new()));
        let records_factory = Arc::clone(&records);
        let auto = SlotAutoFlusher::spawn(
            Arc::clone(&cache),
            move |_| SharedRecords::<u64, String>(Arc::clone(&records_factory)),
            AutoFlusherConfig {
                min_tick_ms: 50,
                max_tick_ms: 50,
                target_depth: 1024,
                max_records_per_drain: 64,
                idle_ticks_threshold: u64::MAX,
                final_drain_passes: 8,
            },
        );

        for key in 0..16u64 {
            cache.put(key, format!("value-{key}"));
        }
        auto.stop();

        assert_eq!(cache.stats().dirty, 0);
        assert_eq!(records.lock().unwrap().len(), 16);
    }

    #[test]
    fn adaptive_slot_flusher_preserves_latest_under_concurrent_puts() {
        let cache = Arc::new(SlotWriteBehindCache::<u64, String>::with_config(
            WriteBehindConfig {
                dirty_shards: 1,
                initial_capacity: 64,
                dirty_queue_capacity: 64,
            },
        ));
        let records = Arc::new(Mutex::new(Vec::new()));
        let records_factory = Arc::clone(&records);
        let auto = SlotAutoFlusher::spawn(
            Arc::clone(&cache),
            move |_| SharedRecords::<u64, String>(Arc::clone(&records_factory)),
            AutoFlusherConfig {
                min_tick_ms: 50,
                max_tick_ms: 50,
                target_depth: 1024,
                max_records_per_drain: 64,
                idle_ticks_threshold: u64::MAX,
                final_drain_passes: 8,
            },
        );

        let mut handles = Vec::new();
        for thread_idx in 0..4u64 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for write_idx in 0..64u64 {
                    cache.put(1, format!("thread-{thread_idx}-{write_idx}"));
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let latest = cache.put(1, "final".to_string());
        auto.stop();

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].version, latest);
        assert_eq!(
            records[0].value.as_deref().map(String::as_str),
            Some("final")
        );
    }

    #[test]
    fn dirty_queue_pressure_does_not_silently_ack() {
        let cache = Arc::new(SlotWriteBehindCache::<u64, String>::with_config(
            WriteBehindConfig {
                dirty_shards: 1,
                initial_capacity: 64,
                dirty_queue_capacity: 1,
            },
        ));
        let records = Arc::new(Mutex::new(Vec::new()));
        let records_factory = Arc::clone(&records);
        let auto = SlotAutoFlusher::spawn(
            Arc::clone(&cache),
            move |_| SharedRecords::<u64, String>(Arc::clone(&records_factory)),
            AutoFlusherConfig {
                min_tick_ms: 1,
                max_tick_ms: 2,
                target_depth: 1,
                max_records_per_drain: 8,
                idle_ticks_threshold: 0,
                final_drain_passes: 8,
            },
        );

        for key in 0..16u64 {
            cache.put(key, format!("value-{key}"));
        }
        auto.stop();

        assert_eq!(records.lock().unwrap().len(), 16);
    }
}
