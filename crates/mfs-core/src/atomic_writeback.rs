//! Atomic-record write-behind cache prototype.
//!
//! `WriteBehindCache` currently replaces the whole hash-table entry on every
//! write. This v3 slice keeps one stable hash-table entry per key and swaps an
//! atomic `Arc<V>` pointer inside the record instead. Existing-key writes avoid
//! `ConcurrentMap` entry replacement and retire only the old value pointer.

use crate::concurrent_map::{ConcurrentMap, InsertOutcome};
use crate::writeback::{AutoFlusherConfig, WriteBehindConfig, WriteBehindError, WriteBehindStats};
use crate::{FastBuildHasher, FlushBackend, FlushRecord, Operation};
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::{Backoff, CachePadded};
use seize::{Collector, Guard, LocalGuard, reclaim};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

struct ValueRecord<V> {
    value: AtomicPtr<Arc<V>>,
    version: AtomicU64,
    last_touch: AtomicU64,
}

impl<V> ValueRecord<V> {
    fn new(value: Option<Arc<V>>, version: u64) -> Self {
        let raw = value
            .map(|v| Box::into_raw(Box::new(v)))
            .unwrap_or(std::ptr::null_mut());
        Self {
            value: AtomicPtr::new(raw),
            version: AtomicU64::new(version),
            last_touch: AtomicU64::new(version),
        }
    }
}

impl<V> Drop for ValueRecord<V> {
    fn drop(&mut self) {
        let raw = *self.value.get_mut();
        if !raw.is_null() {
            unsafe {
                let _ = Box::from_raw(raw);
            }
        }
    }
}

#[derive(Clone)]
struct DirtyEntry<K> {
    key: K,
    version: u64,
    pushed_at: u64,
    op: Operation,
}

type DrainedEntry<K> = (usize, DirtyEntry<K>);
type DrainBatch<K, V> = (Vec<FlushRecord<K, V>>, Vec<DrainedEntry<K>>);
type FlusherWakeup = Arc<[CachePadded<(Mutex<bool>, Condvar)>]>;

const SAMPLE_BITS: u32 = 6;
const SAMPLE_MASK: u64 = (1 << SAMPLE_BITS) - 1;

pub struct AtomicWriteBehindCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    map: ConcurrentMap<K, ValueRecord<V>, S>,
    dirty_shards: Box<[CachePadded<ArrayQueue<DirtyEntry<K>>>]>,
    flusher_wakeup: FlusherWakeup,
    value_collector: Collector,
    clock: CachePadded<AtomicU64>,
    hash_builder: S,
}

impl<K, V> AtomicWriteBehindCache<K, V>
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

impl<K, V, S> AtomicWriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        let n = config.dirty_shards.max(1).next_power_of_two();
        let queue_cap = config.dirty_queue_capacity.max(64);
        let shards: Vec<_> = (0..n)
            .map(|_| CachePadded::new(ArrayQueue::new(queue_cap)))
            .collect();
        let parks: Vec<_> = (0..n)
            .map(|_| CachePadded::new((Mutex::new(false), Condvar::new())))
            .collect();
        Self {
            map: ConcurrentMap::with_hasher_and_capacity(
                hash_builder.clone(),
                config.initial_capacity,
            ),
            dirty_shards: shards.into_boxed_slice(),
            flusher_wakeup: Arc::from(parks.into_boxed_slice()),
            value_collector: Collector::new()
                .batch_size(crate::concurrent_map::DEFAULT_RETIRE_BATCH),
            clock: CachePadded::new(AtomicU64::new(1)),
            hash_builder,
        }
    }

    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            cache: self,
            map_ref: self.map.pin(),
            value_guard: self.value_collector.enter(),
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
        self.try_put(key, value)
            .expect("AtomicWriteBehindCache is full")
    }
    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.pin().try_put(key, value)
    }
    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.try_load_clean(key, value)
            .expect("AtomicWriteBehindCache is full")
    }
    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.pin().try_load_clean(key, value)
    }
    pub fn delete(&self, key: K) -> u64 {
        self.try_delete(key)
            .expect("AtomicWriteBehindCache is full")
    }
    pub fn try_delete(&self, key: K) -> Result<u64, WriteBehindError> {
        self.pin().try_delete(key)
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn stats(&self) -> WriteBehindStats {
        WriteBehindStats {
            len: self.len(),
            dirty: self.dirty_shards.iter().map(|q| q.len()).sum(),
            logical_clock: self.clock.load(Ordering::Relaxed),
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
        hash as usize & (self.dirty_shards.len() - 1)
    }
    #[inline]
    fn record_touch(&self, hash: u64, record: &ValueRecord<V>) {
        if hash & SAMPLE_MASK == 0 {
            let tick = self.clock.fetch_add(1, Ordering::Relaxed);
            record.last_touch.store(tick, Ordering::Relaxed);
        }
    }
    #[inline]
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
        let p = self.pin();
        'outer: for shard_idx in shard_range {
            let shard = &self.dirty_shards[shard_idx];
            let snapshot_len = shard.len();
            for _ in 0..snapshot_len {
                if records.len() >= max_records {
                    break 'outer;
                }
                let Some(entry) = shard.pop() else { break };
                if now.saturating_sub(entry.pushed_at) < idle_ticks {
                    self.push_dirty_with_backoff(shard_idx, entry);
                    continue;
                }
                match p.read_record(&entry.key) {
                    Some((value, version)) if version == entry.version => {
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value,
                            version,
                            op: entry.op,
                        });
                        drained.push((shard_idx, entry));
                    }
                    _ => {}
                }
            }
        }
        (records, drained)
    }
    fn requeue(&self, drained: Vec<DrainedEntry<K>>) {
        for (shard_idx, entry) in drained {
            self.push_dirty_with_backoff(shard_idx, entry);
        }
    }
    fn cleanup_after_flush(&self, drained: &[DrainedEntry<K>]) {
        let p = self.pin();
        for (_, entry) in drained {
            if entry.op == Operation::Delete
                && let Some((std::option::Option::None, version)) = p.read_record(&entry.key)
                && version == entry.version
            {
                p.map_ref.remove(&entry.key);
            }
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
}

pub struct AtomicAutoFlusher {
    handles: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    flusher_wakeup: FlusherWakeup,
}

impl AtomicAutoFlusher {
    pub fn spawn<K, V, B, F>(
        cache: Arc<AtomicWriteBehindCache<K, V>>,
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
                run_atomic_flusher_loop(shard_idx, cache, backend, stop, cfg);
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

fn run_atomic_flusher_loop<K, V, B>(
    shard_idx: usize,
    cache: Arc<AtomicWriteBehindCache<K, V>>,
    mut backend: B,
    stop: Arc<std::sync::atomic::AtomicBool>,
    cfg: AutoFlusherConfig,
) where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
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
        tick = adapt_tick(
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
fn adapt_tick(
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
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    cache: &'g AtomicWriteBehindCache<K, V, S>,
    map_ref: crate::concurrent_map::Pinned<'g, K, ValueRecord<V>, S>,
    value_guard: LocalGuard<'g>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    fn read_arc(&self, record: &'g ValueRecord<V>) -> Option<&'g Arc<V>> {
        let raw = self.value_guard.protect(&record.value, Ordering::Acquire);
        if raw.is_null() {
            None
        } else {
            Some(unsafe { &*raw })
        }
    }
    fn read_record(&self, key: &K) -> Option<(Option<Arc<V>>, u64)> {
        let record = self.map_ref.get(key)?;
        let version = record.version.load(Ordering::Acquire);
        let value = self.read_arc(record).map(Arc::clone);
        Some((value, version))
    }
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let record = self.map_ref.get(key)?;
        let hash = self.cache.hash_key(key);
        self.cache.record_touch(hash, record);
        self.read_arc(record).map(Arc::clone)
    }
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let record = self.map_ref.get(key)?;
        let hash = self.cache.hash_key(key);
        self.cache.record_touch(hash, record);
        let value = self.read_arc(record)?;
        Some(f(value.as_ref()))
    }
    pub fn put(&self, key: K, value: V) -> u64 {
        self.try_put(key, value)
            .expect("AtomicWriteBehindCache is full")
    }
    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, true)
    }
    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.try_load_clean(key, value)
            .expect("AtomicWriteBehindCache is full")
    }
    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, false)
    }
    pub fn delete(&self, key: K) -> u64 {
        self.try_delete(key)
            .expect("AtomicWriteBehindCache is full")
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
        let tick = self.cache.clock.fetch_add(1, Ordering::Relaxed);
        if let Some(record) = self.map_ref.get(&key) {
            let new_raw = value
                .clone()
                .map(|v| Box::into_raw(Box::new(v)))
                .unwrap_or(std::ptr::null_mut());
            let old = record.value.swap(new_raw, Ordering::AcqRel);
            if !old.is_null() {
                unsafe { self.value_guard.defer_retire(old, reclaim::boxed::<Arc<V>>) };
            }
            record.version.store(tick, Ordering::Release);
            if queue_dirty {
                self.cache.push_dirty_with_backoff(
                    self.cache.dirty_shard_idx(hash),
                    DirtyEntry {
                        key,
                        version: tick,
                        pushed_at: tick,
                        op,
                    },
                );
            }
            return Ok(tick);
        }
        match self
            .map_ref
            .insert(key.clone(), ValueRecord::new(value, tick))
        {
            InsertOutcome::Inserted | InsertOutcome::Replaced => {}
            InsertOutcome::Full => return Err(WriteBehindError::CapacityFull),
        }
        if queue_dirty {
            self.cache.push_dirty_with_backoff(
                self.cache.dirty_shard_idx(hash),
                DirtyEntry {
                    key,
                    version: tick,
                    pushed_at: tick,
                    op,
                },
            );
        }
        Ok(tick)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicBool};

    #[derive(Default)]
    struct CollectBackend<K, V> {
        records: Mutex<Vec<FlushRecord<K, V>>>,
        fail_next: AtomicBool,
    }
    impl<K: Clone, V> FlushBackend<K, V> for CollectBackend<K, V> {
        type Error = &'static str;
        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            if self.fail_next.swap(false, Ordering::Relaxed) {
                return Err("forced");
            }
            self.records.lock().unwrap().extend_from_slice(records);
            Ok(())
        }
    }
    #[test]
    fn put_get_delete_round_trip() {
        let cache = AtomicWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        cache.put(1, "beta".to_string());
        assert_eq!(cache.read_with(&1, |v| v.clone()), Some("beta".to_string()));
        cache.delete(1);
        assert!(cache.get(&1).is_none());
    }
    #[test]
    fn flush_emits_latest() {
        let cache = AtomicWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        let v2 = cache.put(1, "beta".to_string());
        let mut backend = CollectBackend::<u64, String>::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records[0].version, v2);
        assert_eq!(
            records[0].value.as_deref().map(String::as_str),
            Some("beta")
        );
    }

    #[test]
    fn failed_flush_requeues() {
        let cache = AtomicWriteBehindCache::<u64, String>::with_capacity(16);
        cache.put(1, "alpha".to_string());
        let mut backend = CollectBackend::<u64, String>::default();
        backend.fail_next.store(true, Ordering::Relaxed);
        assert!(cache.flush_idle(&mut backend, 0, 1024).is_err());
        assert_eq!(cache.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        let records = backend.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].value.as_deref().map(String::as_str),
            Some("alpha")
        );
    }

    #[test]
    fn flush_shard_only_drains_one_shard() {
        let cache = AtomicWriteBehindCache::<u64, String>::with_config(WriteBehindConfig {
            dirty_shards: 2,
            initial_capacity: 16,
            dirty_queue_capacity: 16,
        });
        cache.put(1, "one".to_string());
        cache.put(2, "two".to_string());
        let before_total = cache.stats().dirty;
        let mut backend = CollectBackend::<u64, String>::default();
        let _ = cache.flush_shard_idle(0, &mut backend, 0, 1024).unwrap();
        assert!(cache.stats().dirty < before_total);
    }

    #[test]
    fn auto_flusher_drains_in_background() {
        let cache = Arc::new(AtomicWriteBehindCache::<u64, String>::with_config(
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
        let auto = AtomicAutoFlusher::spawn(
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
        for i in 0..16u64 {
            cache.put(i, format!("v{i}"));
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
    fn fallible_put_reports_capacity_full() {
        let cache = AtomicWriteBehindCache::<u64, String>::with_config(WriteBehindConfig {
            dirty_shards: 1,
            initial_capacity: 1,
            dirty_queue_capacity: 64,
        });

        let mut inserted = 0usize;
        let mut full = 0usize;
        for key in 0..32u64 {
            match cache.try_put(key, format!("value-{key}")) {
                Ok(_) => inserted += 1,
                Err(WriteBehindError::CapacityFull) => full += 1,
            }
        }

        assert!(inserted > 0);
        assert!(full > 0, "expected at least one full-capacity write");
        assert_eq!(cache.len(), inserted);
        assert_eq!(cache.stats().dirty, inserted);
    }
}
