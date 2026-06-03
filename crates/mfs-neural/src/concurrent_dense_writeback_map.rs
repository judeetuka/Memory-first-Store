//! Experimental generic dense write-behind map using `ConcurrentMap` as the handle index.
//!
//! This is the index-strategy sibling of [`crate::dense_writeback_map::DenseWriteBehindMap`].
//! It keeps the same slot/value/version/dirty layout, but uses
//! [`mfs_core::concurrent_map::ConcurrentMap`] as the sparse handle index instead
//! of [`crate::bucketed_index::BucketedIndex`]. The goal is to test whether the
//! lock-free handle index gives better hot existing-key write tails for generic
//! keys while preserving the dense write-behind semantics.

use crate::DenseValue;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::{Backoff, CachePadded};
use mfs_core::concurrent_map::{ConcurrentMap, InsertOutcome};
use mfs_core::writeback::{AutoFlusherConfig, WriteBehindConfig};
use mfs_core::{FastBuildHasher, FlushBackend, FlushRecord, Operation};
use std::hash::{BuildHasher, Hash};
use std::hint::spin_loop;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrentDenseWriteBehindStats {
    pub len: usize,
    pub dirty: usize,
    pub logical_clock: u64,
}

pub struct ConcurrentDenseWriteBehindMap<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    index: ConcurrentMap<K, u64, S>,
    values: Box<[AtomicU64]>,
    generations: Box<[AtomicU32]>,
    versions: Box<[AtomicU64]>,
    free: CachePadded<ArrayQueue<u32>>,
    next_slot: CachePadded<AtomicU32>,
    capacity: u32,
    dirty_shards: Box<[CachePadded<ArrayQueue<DirtyEntry<K>>>]>,
    flusher_wakeup: FlusherWakeup,
    clock: CachePadded<AtomicU64>,
    hash_builder: S,
    _value: PhantomData<V>,
}

impl<K, V> ConcurrentDenseWriteBehindMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
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

impl<K, V, S> ConcurrentDenseWriteBehindMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        let cap = u32::try_from(config.initial_capacity.max(1))
            .expect("ConcurrentDenseWriteBehindMap capacity exceeds u32::MAX");
        let values: Vec<AtomicU64> = (0..cap).map(|_| AtomicU64::new(0)).collect();
        let generations: Vec<AtomicU32> = (0..cap).map(|_| AtomicU32::new(0)).collect();
        let versions: Vec<AtomicU64> = (0..cap).map(|_| AtomicU64::new(0)).collect();
        let dirty_shards = config.dirty_shards.max(1).next_power_of_two();
        let dirty_capacity = config.dirty_queue_capacity.max(1);
        let dirty: Vec<_> = (0..dirty_shards)
            .map(|_| CachePadded::new(ArrayQueue::new(dirty_capacity)))
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
            flusher_wakeup: Arc::from(parks.into_boxed_slice()),
            clock: CachePadded::new(AtomicU64::new(1)),
            hash_builder,
            _value: PhantomData,
        }
    }

    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            cache: self,
            index: self.index.pin(),
        }
    }

    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        self.pin().get(key)
    }

    pub fn put(&self, key: K, value: V) -> u64 {
        self.pin().put(key, value)
    }

    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.pin().load_clean(key, value)
    }

    pub fn delete(&self, key: K) -> u64 {
        self.pin().delete(key)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn stats(&self) -> ConcurrentDenseWriteBehindStats {
        ConcurrentDenseWriteBehindStats {
            len: self.len(),
            dirty: self.dirty_shards.iter().map(|q| q.len()).sum(),
            logical_clock: self.clock.load(Ordering::Relaxed),
        }
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
        assert!(
            shard_idx < self.dirty_shards.len(),
            "shard_idx out of range"
        );
        let (records, drained) =
            self.drain_eligible_shards(shard_idx..shard_idx + 1, idle_ticks, max_records);
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

    pub fn shard_count(&self) -> usize {
        self.dirty_shards.len()
    }

    pub fn shard_dirty_depth(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].len()
    }

    pub fn shard_dirty_capacity(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].capacity()
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
    fn retire_locked_slot(&self, slot: u32, generation: u32) {
        let v = self.versions[slot as usize].load(Ordering::Relaxed);
        self.versions[slot as usize].store(v & !DIRTY_VERSION_BIT, Ordering::Release);
        self.generations[slot as usize].store(generation.wrapping_add(2), Ordering::Release);
        let _ = self.free.push(slot);
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
                    self.retire_locked_slot(slot, generation);
                    return;
                }
                Err(current) if current == (generation | SLOT_WRITE_BIT) => spin_loop(),
                Err(_) => return,
            }
        }
    }

    #[inline]
    fn write_slot(&self, slot: u32, value: V, mark_dirty: bool) -> (u64, bool) {
        let pre_load = self.versions[slot as usize].load(Ordering::Relaxed);
        let version = Self::version_from_word(pre_load).wrapping_add(1);
        let new_word = (version << 1) | if mark_dirty { DIRTY_VERSION_BIT } else { 0 };
        self.values[slot as usize].store(pack(value), Ordering::Release);
        let old_word = self.versions[slot as usize].swap(new_word, Ordering::AcqRel);
        (version, old_word & DIRTY_VERSION_BIT == 0)
    }

    #[inline]
    fn read_value_handle(&self, handle: u64) -> Option<V> {
        let slot = handle_slot(handle);
        let generation = handle_generation(handle);
        loop {
            let state = self.generations[slot as usize].load(Ordering::Acquire);
            if state == (generation | SLOT_WRITE_BIT) {
                spin_loop();
                continue;
            }
            if state != generation {
                return None;
            }
            let value = self.values[slot as usize].load(Ordering::Acquire);
            let state = self.generations[slot as usize].load(Ordering::Acquire);
            if state == generation {
                return Some(unpack(value));
            }
            if state == (generation | SLOT_WRITE_BIT) {
                spin_loop();
                continue;
            }
            return None;
        }
    }

    fn read_handle(&self, handle: u64) -> Option<(V, u64, bool)> {
        let slot = handle_slot(handle);
        let generation = handle_generation(handle);
        loop {
            if self.generations[slot as usize].load(Ordering::Acquire) != generation {
                return None;
            }
            let v1 = self.versions[slot as usize].load(Ordering::Acquire);
            let value = self.values[slot as usize].load(Ordering::Acquire);
            let v2 = self.versions[slot as usize].load(Ordering::Acquire);
            let g2 = self.generations[slot as usize].load(Ordering::Acquire);
            if v1 == v2 && g2 == generation {
                return Some((
                    unpack(value),
                    Self::version_from_word(v2),
                    v2 & DIRTY_VERSION_BIT != 0,
                ));
            }
            spin_loop();
        }
    }

    #[inline]
    fn push_dirty_with_backoff(&self, shard_idx: usize, mut entry: DirtyEntry<K>) {
        let queue = &self.dirty_shards[shard_idx];
        match queue.push(entry) {
            Ok(()) => return,
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

    #[inline]
    fn notify_flusher(&self, shard_idx: usize) {
        let park = &self.flusher_wakeup[shard_idx];
        let mut pending = match park.0.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if !*pending {
            *pending = true;
            park.1.notify_one();
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
        'outer: for shard_idx in shard_range {
            let shard = &self.dirty_shards[shard_idx];
            let snapshot_len = shard.len();
            for _ in 0..snapshot_len {
                if records.len() >= max_records {
                    break 'outer;
                }
                let Some(mut entry) = shard.pop() else { break };
                if now.saturating_sub(entry.pushed_at) < idle_ticks {
                    self.push_dirty_with_backoff(shard_idx, entry);
                    continue;
                }
                match entry.op {
                    Operation::Put => {
                        let Some(handle) = p.get(&entry.key).copied() else {
                            continue;
                        };
                        let Some((value, version, is_dirty)) = self.read_handle(handle) else {
                            self.push_dirty_with_backoff(shard_idx, entry);
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
                                self.push_dirty_with_backoff(shard_idx, entry);
                                continue;
                            }
                        }
                        entry.version = version;
                        entry.slot = handle_slot(handle);
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value: Some(Arc::new(value)),
                            version,
                            op: Operation::Put,
                        });
                        drained.push((shard_idx, entry));
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
                        drained.push((shard_idx, entry));
                    }
                }
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
            self.push_dirty_with_backoff(
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
            self.push_dirty_with_backoff(shard_idx, entry);
        }
    }
}

pub struct ConcurrentDenseMapAutoFlusher {
    handles: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    flusher_wakeup: FlusherWakeup,
}

impl ConcurrentDenseMapAutoFlusher {
    pub fn spawn<K, V, S, B, F>(
        cache: Arc<ConcurrentDenseWriteBehindMap<K, V, S>>,
        mut backend_factory: F,
        config: AutoFlusherConfig,
    ) -> Self
    where
        K: Eq + Hash + Clone + Send + Sync + 'static,
        V: DenseValue,
        S: BuildHasher + Clone + Send + Sync + 'static,
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
                run_concurrent_dense_map_flusher_loop(shard_idx, cache, backend, stop, cfg);
            }));
        }
        Self {
            handles,
            stop,
            flusher_wakeup,
        }
    }

    pub fn stop(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for park in self.flusher_wakeup.iter() {
            let mut pending = match park.0.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            *pending = true;
            park.1.notify_one();
        }
        for h in self.handles {
            let _ = h.join();
        }
    }
}

fn run_concurrent_dense_map_flusher_loop<K, V, S, B>(
    shard_idx: usize,
    cache: Arc<ConcurrentDenseWriteBehindMap<K, V, S>>,
    mut backend: B,
    stop: Arc<std::sync::atomic::AtomicBool>,
    cfg: AutoFlusherConfig,
) where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone,
    B: FlushBackend<K, V>,
{
    let mut tick = std::time::Duration::from_millis(cfg.max_tick_ms);
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        let _ = cache.flush_shard_idle(
            shard_idx,
            &mut backend,
            cfg.idle_ticks_threshold,
            cfg.max_records_per_drain,
        );
        let depth = cache.shard_dirty_depth(shard_idx);
        tick = concurrent_dense_map_adapt_tick(
            tick,
            depth,
            cfg.target_depth,
            cfg.min_tick_ms,
            cfg.max_tick_ms,
        );
        let park = &cache.flusher_wakeup[shard_idx];
        let mut pending = match park.0.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *pending {
            *pending = false;
        } else {
            let (g, _) = match park.1.wait_timeout(pending, tick) {
                Ok(v) => v,
                Err(poisoned) => poisoned.into_inner(),
            };
            pending = g;
            *pending = false;
        }
        drop(pending);
    }
    for _ in 0..cfg.final_drain_passes {
        let n = cache
            .flush_shard_idle(shard_idx, &mut backend, 0, cfg.max_records_per_drain)
            .unwrap_or(0);
        if n == 0 {
            break;
        }
    }
}

#[inline]
fn concurrent_dense_map_adapt_tick(
    current: std::time::Duration,
    observed: usize,
    target: usize,
    min_ms: u64,
    max_ms: u64,
) -> std::time::Duration {
    let min = std::time::Duration::from_millis(min_ms);
    let max = std::time::Duration::from_millis(max_ms);
    let clamp = |d: std::time::Duration| {
        if d < min {
            min
        } else if d > max {
            max
        } else {
            d
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
    V: DenseValue,
    S: BuildHasher,
{
    cache: &'g ConcurrentDenseWriteBehindMap<K, V, S>,
    index: mfs_core::concurrent_map::Pinned<'g, K, u64, S>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone,
{
    #[inline]
    pub fn get(&self, key: &K) -> Option<V> {
        loop {
            let handle = self.index.get(key).copied()?;
            if let Some(value) = self.cache.read_value_handle(handle) {
                return Some(value);
            }
            spin_loop();
        }
    }

    pub fn put(&self, key: K, value: V) -> u64 {
        match self.write(key, value, true) {
            Ok(version) => version,
            Err(_) => panic!("ConcurrentDenseWriteBehindMap is full"),
        }
    }

    pub fn load_clean(&self, key: K, value: V) -> u64 {
        match self.write(key, value, false) {
            Ok(version) => version,
            Err(_) => panic!("ConcurrentDenseWriteBehindMap is full"),
        }
    }

    fn write(&self, key: K, value: V, queue_dirty: bool) -> Result<u64, V> {
        let hash = self.cache.hash_key(&key);
        loop {
            if let Some(&handle) = self.index.get(&key) {
                let slot = handle_slot(handle);
                let generation = handle_generation(handle);
                if !self.cache.try_lock_slot_generation(slot, generation) {
                    spin_loop();
                    continue;
                }
                let still_current =
                    matches!(self.index.get(&key), Some(&current) if current == handle);
                if still_current {
                    let (version, was_clean) = self.cache.write_slot(slot, value, queue_dirty);
                    self.cache.unlock_slot(slot, generation);
                    if queue_dirty && was_clean {
                        let pushed_at = self.cache.next_dirty_tick();
                        self.cache.push_dirty_with_backoff(
                            self.cache.dirty_shard_idx(hash),
                            DirtyEntry {
                                key,
                                version,
                                pushed_at,
                                op: Operation::Put,
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
            return Err(value);
        };
        let generation = self.cache.lock_slot(slot);
        let handle = pack_handle(slot, generation);
        let (version, was_clean) = self.cache.write_slot(slot, value, queue_dirty);
        match self.index.insert_returning_old(key.clone(), handle) {
            (InsertOutcome::Inserted | InsertOutcome::Replaced, old_handle) => {
                self.cache.unlock_slot(slot, generation);
                if let Some(old_handle) = old_handle {
                    self.cache.retire_slot(old_handle);
                }
                if queue_dirty && was_clean {
                    let pushed_at = self.cache.next_dirty_tick();
                    self.cache.push_dirty_with_backoff(
                        self.cache.dirty_shard_idx(hash),
                        DirtyEntry {
                            key,
                            version,
                            pushed_at,
                            op: Operation::Put,
                            slot,
                        },
                    );
                }
                Ok(version)
            }
            (InsertOutcome::Full, old_handle) => {
                self.cache.unlock_slot(slot, generation);
                if let Some(old_handle) = old_handle {
                    self.cache.retire_slot(old_handle);
                }
                let _ = self.cache.free.push(slot);
                Err(value)
            }
        }
    }

    pub fn delete(&self, key: K) -> u64 {
        let hash = self.cache.hash_key(&key);
        loop {
            let Some(&handle) = self.index.get(&key) else {
                let version = self.cache.next_absent_delete_version();
                let pushed_at = version;
                self.cache.push_dirty_with_backoff(
                    self.cache.dirty_shard_idx(hash),
                    DirtyEntry {
                        key,
                        version,
                        pushed_at,
                        op: Operation::Delete,
                        slot: 0,
                    },
                );
                return version;
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
                        op: Operation::Delete,
                        slot,
                    },
                );
                return version;
            }
            self.cache.unlock_slot(slot, generation);
            spin_loop();
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
    use std::sync::{Arc, Barrier, Mutex};

    #[derive(Default)]
    struct CollectBackend<K, V> {
        records: Mutex<Vec<FlushRecord<K, V>>>,
    }

    impl<K: Clone, V> FlushBackend<K, V> for CollectBackend<K, V> {
        type Error = ();

        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }

    #[test]
    fn generic_key_and_value_round_trip() {
        let cache = ConcurrentDenseWriteBehindMap::<String, [u8; 8]>::with_capacity(16);
        cache.put("alpha".to_string(), *b"12345678");
        cache.put("beta".to_string(), *b"abcdefgh");
        assert_eq!(cache.get(&"alpha".to_string()), Some(*b"12345678"));
        assert_eq!(cache.get(&"beta".to_string()), Some(*b"abcdefgh"));
        cache.put("alpha".to_string(), *b"87654321");
        assert_eq!(cache.get(&"alpha".to_string()), Some(*b"87654321"));
    }

    #[test]
    fn flush_emits_latest_generic_record() {
        let cache = ConcurrentDenseWriteBehindMap::<String, [u8; 8]>::with_capacity(16);
        cache.put("alpha".to_string(), *b"first___");
        cache.put("alpha".to_string(), *b"second__");
        let mut backend = CollectBackend::<String, [u8; 8]>::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "alpha");
        assert_eq!(records[0].value.as_deref().copied(), Some(*b"second__"));
    }

    #[test]
    fn idle_flush_uses_logical_clock() {
        let cache = ConcurrentDenseWriteBehindMap::<String, [u8; 8]>::with_capacity(16);
        cache.put("alpha".to_string(), *b"first___");
        let mut backend = CollectBackend::<String, [u8; 8]>::default();
        assert_eq!(cache.flush_idle(&mut backend, 2, 1024).unwrap(), 0);
        cache.put("beta".to_string(), *b"second__");
        assert_eq!(cache.flush_idle(&mut backend, 2, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "alpha");
        assert_eq!(records[0].value.as_deref().copied(), Some(*b"first___"));
    }

    #[test]
    fn same_key_dirty_update_refreshes_queued_entry() {
        let cache = ConcurrentDenseWriteBehindMap::<String, [u8; 8]>::with_capacity(16);
        cache.put("alpha".to_string(), *b"first___");
        cache.put("alpha".to_string(), *b"second__");
        let mut backend = CollectBackend::<String, [u8; 8]>::default();
        assert_eq!(cache.flush_idle(&mut backend, 1, 1024).unwrap(), 0);
        cache.clock.fetch_add(1, Ordering::Relaxed);
        assert_eq!(cache.flush_idle(&mut backend, 1, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, "alpha");
        assert_eq!(records[0].value.as_deref().copied(), Some(*b"second__"));
    }

    #[test]
    fn delete_emits_delete_record() {
        let cache = ConcurrentDenseWriteBehindMap::<String, u64>::with_capacity(16);
        cache.put("alpha".to_string(), 1);
        cache.delete("alpha".to_string());
        assert_eq!(cache.get(&"alpha".to_string()), None);
        let mut backend = CollectBackend::<String, u64>::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records[0].key, "alpha");
        assert_eq!(records[0].op, Operation::Delete);
        assert!(records[0].value.is_none());
    }

    #[test]
    fn auto_flusher_drains_generic_writes_in_background() {
        struct CountingBackend(Arc<std::sync::atomic::AtomicUsize>);
        impl FlushBackend<String, [u8; 8]> for CountingBackend {
            type Error = ();
            fn flush(
                &mut self,
                records: &[FlushRecord<String, [u8; 8]>],
            ) -> Result<(), Self::Error> {
                self.0.fetch_add(records.len(), Ordering::Relaxed);
                Ok(())
            }
        }

        let cache = Arc::new(
            ConcurrentDenseWriteBehindMap::<String, [u8; 8]>::with_config(WriteBehindConfig {
                dirty_shards: 2,
                initial_capacity: 64,
                dirty_queue_capacity: 64,
            }),
        );
        let flushed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let auto = ConcurrentDenseMapAutoFlusher::spawn(
            Arc::clone(&cache),
            |_| CountingBackend(Arc::clone(&flushed)),
            AutoFlusherConfig {
                min_tick_ms: 1,
                max_tick_ms: 2,
                target_depth: 1,
                max_records_per_drain: 64,
                idle_ticks_threshold: 1,
                final_drain_passes: 8,
            },
        );
        for i in 0..16u64 {
            cache.put(format!("key_{i}"), i.to_le_bytes());
        }
        for _ in 0..50 {
            if flushed.load(Ordering::Relaxed) >= 16 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        auto.stop();
        assert!(flushed.load(Ordering::Relaxed) >= 16);
    }

    #[test]
    fn concurrent_readers_do_not_observe_none_during_puts() {
        const READERS: usize = 4;
        const WRITES: u64 = 20_000;
        let cache = Arc::new(ConcurrentDenseWriteBehindMap::<u64, [u8; 8]>::with_capacity(16));
        cache.load_clean(1, 0u64.to_le_bytes());
        let barrier = Arc::new(Barrier::new(READERS + 1));
        let mut handles = Vec::new();

        for _ in 0..READERS {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..WRITES {
                    assert!(cache.get(&1).is_some());
                }
            }));
        }

        barrier.wait();
        for value in 0..WRITES {
            cache.put(1, value.to_le_bytes());
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
