//! Lock-free write-behind cache.
//!
//! [`WriteBehindCache`] gives you the read-path speed of our in-house
//! [`crate::concurrent_map::ConcurrentMap`] (epoch-protected lookups,
//! no `RwLock`, no `Arc::clone` on the [`read_with`] path) **plus**
//! the dirty-tracking + write-behind flush semantics of
//! [`crate::MemoryFirstStore`] (per-key version, idle detection,
//! [`crate::FlushBackend`] integration).
//!
//! ## Architecture
//!
//! - **Storage**: [`crate::concurrent_map::ConcurrentMap<K, ValueRecord<V>>`].
//!   Each value record carries the per-write version, a sampled
//!   `last_touch` tick, and either `Some(Arc<V>)` (live) or `None`
//!   (tombstone awaiting flush).
//! - **Dirty tracking**: per-shard lock-free bounded MPMC ring buffers
//!   ([`crossbeam_queue::ArrayQueue`]) of `(key, version, op)` triples.
//!   Each shard sits in its own [`CachePadded`] cell so writers on
//!   different shards never bounce a cache line. Concurrent writers on
//!   the same shard never serialise on a mutex; pushes are CAS-based.
//! - **Versioning**: every mutation tags the record with the current
//!   logical clock tick. The flusher uses this tick as a version to
//!   detect stale dirty entries: if a queued entry's version no longer
//!   matches the entry currently in the map, a later mutation already
//!   superseded it and the flusher skips that entry entirely.
//! - **Pinned writes**: [`Pinned::put`] / [`Pinned::delete`] reuse the
//!   existing epoch pin instead of constructing a fresh one inside
//!   `cache.put()`. In tight write loops the saved pin construction is
//!   measurable.
//!
//! ## Read path cost
//!
//! ```text
//! get          : pin epoch + map lookup + sampled tick update + Arc::clone
//! read_with    : pin epoch + map lookup + sampled tick update + closure
//! ```
//!
//! No `RwLock` acquire. No global atomic on the hot path (the clock is
//! sampled at ~1/64 of `get`s). The `Arc::clone` in `get` is the only
//! refcount RMW; use `read_with` to skip it.
//!
//! ## Failure semantics
//!
//! [`flush_idle`] drains eligible dirty entries from each shard, builds
//! [`FlushRecord`]s, calls the backend, and on success drops the drained
//! entries. On backend error the drained entries are pushed back to the
//! shard tail and the error is propagated. The map state is unchanged
//! either way; data remains hot in RAM until the next successful flush.
//! Backends should still be idempotent because retried records may be
//! visible to the backend before the error is observed.
//!
//! ## A note on V3
//!
//! Today's write path allocates one `Box<Entry<K, ValueRecord<V>>>`
//! per mutation (matching papaya's cost; we beat papaya by ~16 % on
//! reads but not on writes). A planned V3 refactor splits storage
//! into `ConcurrentMap<K, u32>` (key → slot) plus a pre-allocated
//! slot array of `ValueRecord<V>`, mirroring [`crate::dense_kv`].
//! Updates of existing keys would then drop from ~150 ns to ~10 ns
//! per write. See `docs/DESIGN-v3.md` for the architectural sketch
//! and the ABA-hazard handling plan.

use crate::concurrent_map::ConcurrentMap;
use crate::{FastBuildHasher, FlushBackend, FlushRecord, Operation};
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::{Backoff, CachePadded};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

const SAMPLE_BITS: u32 = 6;
const SAMPLE_MASK: u64 = (1 << SAMPLE_BITS) - 1;

/// Configuration for a [`WriteBehindCache`].
#[derive(Debug, Clone, Copy)]
pub struct WriteBehindConfig {
    /// Number of lock-free dirty queues. More shards = better write
    /// scaling under high concurrency; rounded up to the next power of
    /// two. Defaults to `2 * available_parallelism`.
    pub dirty_shards: usize,
    /// Initial capacity of the underlying [`ConcurrentMap`].
    ///
    /// **Sizing matters.** Our [`ConcurrentMap`] is fixed-capacity
    /// (no resize for v1); over-stuffing past the load-factor limit
    /// returns `InsertOutcome::Full` on inserts. Pre-size to the
    /// expected working set. [`WriteBehindCache::compact`] is a
    /// quiescent maintenance rebuild that returns a new cache after
    /// flushing, not live growth. The 1 M default is generous on
    /// purpose for typical caches; tune up if you know you'll hold
    /// more.
    pub initial_capacity: usize,
    /// Capacity of each dirty queue (per-shard). The dirty queues are
    /// bounded Vyukov-style MPMC ring buffers
    /// ([`crossbeam_queue::ArrayQueue`]); a full queue means writers
    /// spin (`Backoff::snooze`) until the flusher drains. The bound
    /// gives natural backpressure: when the flusher can't keep up,
    /// producers slow rather than the queue growing unboundedly.
    /// Default: 16 384 entries per shard. Total in-flight dirty
    /// budget is `dirty_queue_capacity × dirty_shards` records.
    pub dirty_queue_capacity: usize,
}

impl Default for WriteBehindConfig {
    fn default() -> Self {
        Self {
            dirty_shards: std::thread::available_parallelism()
                .map(|n| n.get().saturating_mul(2))
                .unwrap_or(16)
                .next_power_of_two(),
            initial_capacity: 1_000_000,
            dirty_queue_capacity: 16 * 1024,
        }
    }
}

/// One entry in a dirty FIFO queue. The whole struct travels through
/// the [`ArrayQueue`] without locks.
struct DirtyEntry<K> {
    key: K,
    version: u64,
    pushed_at: u64,
    op: Operation,
}

type DrainedEntry<K> = (usize, DirtyEntry<K>);
type DrainBatch<K, V> = (Vec<FlushRecord<K, V>>, Vec<DrainedEntry<K>>);

/// Value record stored in the map. Holds the live value (or `None` for a
/// tombstone awaiting flush), the per-key monotonic version, and a
/// sampled last-touch tick used by [`flush_idle`] for idle detection.
struct ValueRecord<V> {
    value: Option<Arc<V>>,
    version: u64,
    last_touch: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteBehindStats {
    pub len: usize,
    pub dirty: usize,
    pub logical_clock: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteBehindError {
    CapacityFull,
}

type FlusherWakeup = Arc<[CachePadded<(Mutex<bool>, Condvar)>]>;

pub struct WriteBehindCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    map: ConcurrentMap<K, ValueRecord<V>, S>,
    dirty_shards: Box<[CachePadded<ArrayQueue<DirtyEntry<K>>>]>,
    /// Per-shard wakeup primitives for the AutoFlusher. The `bool`
    /// inside the mutex is the "signal pending" flag: writers set it
    /// to `true` and `notify_one` when their push crosses a queue
    /// fill watermark, so the flusher can drain immediately instead
    /// of sleeping out the rest of its adaptive tick. Shared as an
    /// `Arc<[...]>` with the [`AutoFlusher`] so that
    /// [`AutoFlusher::stop`] can wake every sleeping flusher to
    /// observe the stop flag without waiting for the next tick.
    flusher_wakeup: FlusherWakeup,
    clock: CachePadded<AtomicU64>,
    hash_builder: S,
}

impl<K, V> WriteBehindCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::with_config(WriteBehindConfig::default())
    }

    pub fn with_config(config: WriteBehindConfig) -> Self {
        Self::with_hasher_and_config(FastBuildHasher::default(), config)
    }

    /// Convenience constructor that pre-sizes the underlying map
    /// to `expected_entries`. Strongly preferred over [`new`](Self::new)
    /// when you have any estimate of the working set: under-sized
    /// the map cannot resize, which scatters value allocations
    /// across the heap and causes read latency to fall off a cliff
    /// (10x+ regression measured at 1M entries).
    pub fn with_capacity(expected_entries: usize) -> Self {
        Self::with_config(WriteBehindConfig {
            initial_capacity: expected_entries,
            ..WriteBehindConfig::default()
        })
    }
}

impl<K, V> Default for WriteBehindCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, S> WriteBehindCache<K, V, S>
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
        let map =
            ConcurrentMap::with_hasher_and_capacity(hash_builder.clone(), config.initial_capacity);
        Self {
            map,
            dirty_shards: shards.into_boxed_slice(),
            flusher_wakeup: Arc::from(parks.into_boxed_slice()),
            clock: CachePadded::new(AtomicU64::new(1)),
            hash_builder,
        }
    }
}

impl<K, V, S> WriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
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

    /// Pin the underlying epoch and return a [`Pinned`] guard. Hold the
    /// guard across many reads to amortize the per-call pin cost; this
    /// is the fast path. Drop the guard quickly to let reclamation
    /// proceed.
    #[inline]
    pub fn pin(&self) -> Pinned<'_, K, V, S> {
        Pinned {
            cache: self,
            map_ref: self.map.pin(),
        }
    }

    /// Convenience one-shot read. Internally pins, performs the lookup,
    /// and drops the pin. For tight loops, prefer holding a [`Pinned`]
    /// guard via [`pin`](Self::pin) to amortize the pin cost.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.pin().get(key)
    }

    /// Convenience one-shot closure-based read. Same caveat as
    /// [`get`](Self::get): for hot loops prefer
    /// [`Pinned::read_with`].
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        self.pin().read_with(key, f)
    }

    /// Read without updating last_touch. Diagnostic; convenience
    /// one-shot wrapper.
    pub fn peek(&self, key: &K) -> Option<Arc<V>> {
        self.pin().peek(key)
    }

    /// Insert or replace `key`, marking the entry dirty. Returns the new
    /// version assigned to this write.
    pub fn put(&self, key: K, value: V) -> u64 {
        self.try_put(key, value).expect("WriteBehindCache is full")
    }

    /// Fallible variant of [`put`](Self::put).
    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, true)
    }

    /// Insert or replace `key` with an existing [`Arc`], marking the entry dirty.
    ///
    /// Use this when the caller already owns a shared value allocation and wants
    /// to avoid wrapping it in a second `Arc` before handing it to the cache.
    pub fn put_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_put_arc(key, value)
            .expect("WriteBehindCache is full")
    }

    /// Fallible variant of [`put_arc`](Self::put_arc).
    pub fn try_put_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.write(key, Some(value), Operation::Put, true)
    }

    /// Insert without marking dirty. Use this when rehydrating from a
    /// backend on startup so that the loaded data does not immediately
    /// flush itself back.
    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.try_load_clean(key, value)
            .expect("WriteBehindCache is full")
    }

    /// Fallible variant of [`load_clean`](Self::load_clean).
    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.write(key, Some(Arc::new(value)), Operation::Put, false)
    }

    /// Insert an existing [`Arc`] without marking the entry dirty.
    pub fn load_clean_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_load_clean_arc(key, value)
            .expect("WriteBehindCache is full")
    }

    /// Fallible variant of [`load_clean_arc`](Self::load_clean_arc).
    pub fn try_load_clean_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.write(key, Some(value), Operation::Put, false)
    }

    /// Mark `key` as deleted. Stored as a tombstone in the map; the
    /// tombstone is removed from the map the next time a delete record
    /// for it is successfully flushed.
    pub fn delete(&self, key: K) -> u64 {
        self.try_delete(key).expect("WriteBehindCache is full")
    }

    /// Fallible variant of [`delete`](Self::delete).
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
        // Construct a fresh pin and delegate to the pin-aware variant.
        // Callers in tight write loops should prefer
        // [`Pinned::put`] / [`Pinned::delete`] which reuse a held pin.
        self.write_with_pin(&self.map.pin(), key, value, op, queue_dirty)
    }

    fn write_with_pin<'g>(
        &self,
        map_ref: &crate::concurrent_map::Pinned<'g, K, ValueRecord<V>, S>,
        key: K,
        value: Option<Arc<V>>,
        op: Operation,
        queue_dirty: bool,
    ) -> Result<u64, WriteBehindError> {
        let hash = self.hash_key(&key);
        let tick = self.clock.fetch_add(1, Ordering::Relaxed);

        // Use the global clock tick as the version. It's monotonic
        // across the whole map, so per-key it's monotonic too —
        // sufficient for the flusher's stale-detection. No CAS-loop
        // needed; we just insert the new record (last writer wins).
        // Two concurrent writers to the same key both see distinct
        // tick values; whichever insert lands last is observed.
        let version = tick;
        let outcome = map_ref.insert(
            key.clone(),
            ValueRecord {
                value: value.clone(),
                version,
                last_touch: AtomicU64::new(tick),
            },
        );
        if outcome == crate::concurrent_map::InsertOutcome::Full {
            return Err(WriteBehindError::CapacityFull);
        }

        if queue_dirty {
            self.push_dirty_with_backoff(
                self.dirty_shard_idx(hash),
                DirtyEntry {
                    key,
                    version,
                    pushed_at: tick,
                    op,
                },
            );
        }
        Ok(version)
    }

    #[inline]
    fn push_dirty_with_backoff(&self, shard_idx: usize, mut entry: DirtyEntry<K>) {
        let queue = &self.dirty_shards[shard_idx];
        let cap = queue.capacity();
        // Fast path: the queue almost always has slack.
        match queue.push(entry) {
            Ok(()) => {
                // Event-driven flusher wake: if this push pushed the
                // queue past 50% full, signal the per-shard flusher
                // so it drains immediately instead of sleeping out
                // the remainder of its adaptive tick. The Mutex<bool>
                // gate ensures only the first writer per
                // wake-cycle pays the notify cost; subsequent writers
                // see `*pending == true` and skip the notify.
                if queue.len().saturating_mul(2) >= cap {
                    self.notify_flusher(shard_idx);
                }
                return;
            }
            Err(e) => entry = e,
        }
        // Slow path: queue is full because the flusher is behind. Wake
        // the flusher unconditionally (the queue is definitely past the
        // watermark), then spin via [`Backoff::snooze`] which escalates
        // from spin to yield; never blocks indefinitely.
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

    /// Snapshot statistics — count of live entries (excluding
    /// tombstones), total dirty queue length across all shards, and the
    /// current logical clock value.
    ///
    /// `SegQueue::len()` is O(N) so this method is not cheap; call it
    /// out of the hot path.
    pub fn stats(&self) -> WriteBehindStats {
        let mut len = 0;
        self.map.for_each(|_, r| {
            if r.value.is_some() {
                len += 1;
            }
        });
        let dirty: usize = self.dirty_shards.iter().map(|q| q.len()).sum();
        WriteBehindStats {
            len,
            dirty,
            logical_clock: self.clock.load(Ordering::Relaxed),
        }
    }

    /// Total number of live (non-tombstone) entries.
    pub fn len(&self) -> usize {
        self.stats().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Guard returned by [`WriteBehindCache::pin`]. Holds a
/// `crossbeam-epoch` pin so that values returned via `&V` are guaranteed
/// live for the guard's lifetime. Constructing the guard is the
/// dominant cost of an epoch-pinned read; in tight loops, hold one
/// guard and run many reads against it.
pub struct Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    cache: &'g WriteBehindCache<K, V, S>,
    map_ref: crate::concurrent_map::Pinned<'g, K, ValueRecord<V>, S>,
}

impl<'g, K, V, S> Pinned<'g, K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    /// Lookup `key`, returning a cloned `Arc<V>` of the live value.
    /// `None` if missing or tombstoned.
    #[inline]
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let (record, hash) = self.map_ref.get_with_hash(key)?;
        let value = record.value.as_ref()?.clone();
        self.cache.record_touch(hash, record);
        Some(value)
    }

    /// Lookup `key` and pass the live value to `f`. Skips the
    /// `Arc::clone` entirely.
    #[inline]
    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let (record, hash) = self.map_ref.get_with_hash(key)?;
        let value = record.value.as_ref()?;
        self.cache.record_touch(hash, record);
        Some(f(value.as_ref()))
    }

    /// Lookup `key` and return a reference to the live value, bounded by
    /// this guard's lifetime. Skips the `Arc::clone` that
    /// [`get`](Self::get) pays — about 4x faster on hot keys (measured
    /// ~36 ns vs ~151 ns on Skylake T460, ~25 ns vs ~80 ns on Zen 3).
    ///
    /// The returned reference is valid until this `Pinned` is dropped.
    /// For ownership past the guard's lifetime, use [`get`](Self::get)
    /// which clones the underlying `Arc<V>`. For one-shot inspection
    /// without holding a reference, use [`read_with`](Self::read_with).
    #[inline]
    pub fn get_ref(&self, key: &K) -> Option<&V> {
        let (record, hash) = self.map_ref.get_with_hash(key)?;
        let value = record.value.as_ref()?;
        self.cache.record_touch(hash, record);
        Some(value.as_ref())
    }

    /// Diagnostic read that does not update `last_touch`.
    #[inline]
    pub fn peek(&self, key: &K) -> Option<Arc<V>> {
        let record = self.map_ref.get(key)?;
        record.value.as_ref().cloned()
    }

    /// Whether the key is present and live (not tombstoned).
    #[inline]
    pub fn contains_key(&self, key: &K) -> bool {
        match self.map_ref.get(key) {
            Some(r) => r.value.is_some(),
            None => false,
        }
    }

    /// Insert or replace `key` while reusing the held pin. Saves a
    /// per-call epoch-pin construction relative to
    /// [`WriteBehindCache::put`]. Returns the new version.
    pub fn put(&self, key: K, value: V) -> u64 {
        self.try_put(key, value).expect("WriteBehindCache is full")
    }

    pub fn try_put(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.cache.write_with_pin(
            &self.map_ref,
            key,
            Some(Arc::new(value)),
            Operation::Put,
            true,
        )
    }

    pub fn put_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_put_arc(key, value)
            .expect("WriteBehindCache is full")
    }

    pub fn try_put_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.cache
            .write_with_pin(&self.map_ref, key, Some(value), Operation::Put, true)
    }

    /// Insert without marking dirty, while reusing the held pin.
    pub fn load_clean(&self, key: K, value: V) -> u64 {
        self.try_load_clean(key, value)
            .expect("WriteBehindCache is full")
    }

    pub fn try_load_clean(&self, key: K, value: V) -> Result<u64, WriteBehindError> {
        self.cache.write_with_pin(
            &self.map_ref,
            key,
            Some(Arc::new(value)),
            Operation::Put,
            false,
        )
    }

    pub fn load_clean_arc(&self, key: K, value: Arc<V>) -> u64 {
        self.try_load_clean_arc(key, value)
            .expect("WriteBehindCache is full")
    }

    pub fn try_load_clean_arc(&self, key: K, value: Arc<V>) -> Result<u64, WriteBehindError> {
        self.cache
            .write_with_pin(&self.map_ref, key, Some(value), Operation::Put, false)
    }

    /// Mark `key` deleted while reusing the held pin.
    pub fn delete(&self, key: K) -> u64 {
        self.try_delete(key).expect("WriteBehindCache is full")
    }

    pub fn try_delete(&self, key: K) -> Result<u64, WriteBehindError> {
        self.cache
            .write_with_pin(&self.map_ref, key, None, Operation::Delete, true)
    }
}

impl<K, V, S> WriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    /// Drain dirty entries that have been idle for at least `idle_ticks`
    /// from each shard, building [`FlushRecord`]s by looking up the
    /// current version in the map. If the queued entry's version is
    /// stale (a later mutation superseded it) it is dropped without
    /// emitting a FlushRecord. The drained entries are returned in a
    /// parallel `Vec` so they can be re-queued on backend failure.
    ///
    /// With the lock-free [`SegQueue`] the flusher pops entries one at
    /// a time. Entries that are not yet eligible (too young) are pushed
    /// back to the same shard's tail. To avoid an infinite cycle when
    /// nothing is eligible, each shard is processed at most as many
    /// times as the queue's snapshot length at flush start; concurrent
    /// pushes during the drain stay queued for the next call.
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
        let mut records: Vec<FlushRecord<K, V>> = Vec::new();
        let mut drained: Vec<DrainedEntry<K>> = Vec::new();

        let p = self.map.pin();
        'outer: for shard_idx in shard_range {
            let shard = &self.dirty_shards[shard_idx];
            let snapshot_len = shard.len();
            for _ in 0..snapshot_len {
                if records.len() >= max_records {
                    break 'outer;
                }
                let Some(entry) = shard.pop() else {
                    break;
                };
                let idle = now.saturating_sub(entry.pushed_at);
                if idle < idle_ticks {
                    // Not yet eligible — requeue at the tail. Going via
                    // `push_dirty_with_backoff` keeps the bounded-queue
                    // semantics consistent if a writer raced us and
                    // filled the queue between our pop and the push.
                    self.push_dirty_with_backoff(shard_idx, entry);
                    continue;
                }
                match p.get(&entry.key) {
                    Some(record) if record.version == entry.version => {
                        let value = record.value.as_ref().cloned();
                        records.push(FlushRecord {
                            key: entry.key.clone(),
                            value,
                            version: entry.version,
                            op: entry.op,
                        });
                        drained.push((shard_idx, entry));
                    }
                    _ => {
                        // Stale entry — superseded by a later write or
                        // tombstone already removed. Drop without emit.
                    }
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

    /// Apply post-flush bookkeeping to the map. For
    /// [`Operation::Delete`] records we remove the matching tombstone if
    /// its version still matches.
    fn cleanup_after_flush(&self, drained: &[DrainedEntry<K>]) {
        let p = self.map.pin();
        for (_, entry) in drained {
            if entry.op == Operation::Delete {
                // Remove the tombstone iff version still matches.
                // Note: there's a race window between our read and
                // remove — if a concurrent writer racing us bumped
                // the version, we'd remove their record. We accept
                // this in v1 (matches `last writer wins` semantics
                // of the rest of the cache); a versioned-remove
                // primitive on ConcurrentMap is future work.
                if let Some(record) = p.get(&entry.key)
                    && record.version == entry.version
                    && record.value.is_none()
                {
                    p.remove(&entry.key);
                }
            }
        }
    }

    /// Drain → flush → cleanup. On backend error the drained entries are
    /// re-queued (push to back of their original shard), preserving
    /// retry semantics, and the error is propagated.
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

    /// Drain only one shard's dirty queue. Building block for
    /// [`AutoFlusher`] which spawns one flusher thread per shard so
    /// flushers never share a backend or contend on each other's
    /// queue. `shard_idx` must be `< self.shard_count()`.
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

    /// Total number of dirty shards. Determined at construction by
    /// `WriteBehindConfig::dirty_shards` (rounded up to a power of two).
    pub fn shard_count(&self) -> usize {
        self.dirty_shards.len()
    }

    /// O(N)-in-queue-length depth of one shard's dirty queue. Used by
    /// adaptive flushers to gauge pressure. Cheap relative to the
    /// flush itself; safe to call from a hot loop.
    pub fn shard_dirty_depth(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].len()
    }

    /// Per-shard capacity (the bounded ring's max length). Returned
    /// once at construction; useful for adaptive flushers that want to
    /// reason about the headroom-vs-watermark relationship.
    pub fn shard_dirty_capacity(&self, shard_idx: usize) -> usize {
        self.dirty_shards[shard_idx].capacity()
    }

    /// Walk all live entries in the map and remove those whose
    /// `last_touch` is older than `now - idle_ticks`. Useful as a
    /// memory-pressure relief paired with a periodic flush.
    ///
    /// Tombstones are never evicted by this function because they are
    /// load-bearing for delete propagation; only successful
    /// [`flush_idle`] removes tombstones.
    pub fn evict_idle(&self, idle_ticks: u64) -> usize {
        let now = self.clock.load(Ordering::Relaxed);
        let mut candidates: Vec<K> = Vec::new();
        self.map.for_each(|k, r| {
            if r.value.is_none() {
                return;
            }
            let touch = r.last_touch.load(Ordering::Relaxed);
            if now.saturating_sub(touch) >= idle_ticks {
                candidates.push(k.clone());
            }
        });
        let mut evicted = 0;
        let p = self.map.pin();
        for k in candidates {
            if p.remove(&k) {
                evicted += 1;
            }
        }
        evicted
    }
}

/// Configuration for an [`AutoFlusher`].
///
/// The adaptive-tick algorithm is inspired by the Linux page-cache
/// writeback (per-bdi, ~`Documentation/admin-guide/sysctl/vm.rst`):
/// flushers sleep longer when shards are quiet and shorter when they
/// fill, with watermarks bracketing the steady state.
#[derive(Debug, Clone, Copy)]
pub struct AutoFlusherConfig {
    /// Minimum tick interval (busiest case). Default 1 ms. Lower
    /// values reduce the worst-case dirty-queue residency at the cost
    /// of more wake-ups when the workload is steady.
    pub min_tick_ms: u64,
    /// Maximum tick interval (quietest case). Default 50 ms. Higher
    /// values reduce idle CPU but let small bursts queue up.
    pub max_tick_ms: u64,
    /// Target queue depth (entries). The adaptive algorithm tries to
    /// keep observed depth near this value: above it, ticks shrink;
    /// well below it, ticks grow. Default 1024.
    pub target_depth: usize,
    /// Hard upper bound on records a single flush call processes,
    /// passed through to [`WriteBehindCache::flush_shard_idle`].
    /// Default 8192.
    pub max_records_per_drain: usize,
    /// `idle_ticks` argument forwarded to
    /// [`WriteBehindCache::flush_shard_idle`]. Default 32.
    pub idle_ticks_threshold: u64,
    /// On shutdown, how many extra full-drain passes (with
    /// `idle_ticks=0`) to attempt before giving up. Default 16.
    pub final_drain_passes: usize,
}

impl Default for AutoFlusherConfig {
    fn default() -> Self {
        Self {
            min_tick_ms: 1,
            max_tick_ms: 50,
            target_depth: 1024,
            max_records_per_drain: 8192,
            idle_ticks_threshold: 32,
            final_drain_passes: 16,
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
        // Urgent: queue is filling.
        min
    } else if observed >= target {
        // Above target — speed up multiplicatively.
        clamp(current.mul_f64(0.6))
    } else if observed >= target / 2 {
        // Steady state — hold.
        clamp(current)
    } else {
        // Quiet — slow down to save CPU.
        clamp(current.mul_f64(1.5))
    }
}

/// One flusher thread per shard, each owning its [`FlushBackend`].
///
/// Spawned via [`AutoFlusher::spawn`]; stopped via [`AutoFlusher::stop`]
/// (which performs a final-drain pass on each shard before joining the
/// threads). Handles are private; the cache itself stays usable
/// throughout.
///
/// ## Why per-shard
///
/// A single background flusher caps write throughput at the rate one
/// thread can drain. With N shards × N flushers there is no
/// inter-flusher synchronisation: each thread reads only its own
/// dirty queue, calls a backend that nobody else touches, and runs an
/// independent adaptive tick. RocksDB and Cassandra take the same
/// approach (`max_background_flushes`, `memtable_flush_writers`).
///
/// ## Backend ownership
///
/// Each shard's flusher owns its own backend, constructed by the
/// factory passed to `spawn(shard_idx)`. The factory is called N
/// times in the spawning thread and the resulting backends are moved
/// into the workers. This means:
///
/// - For per-shard WALs, return a fresh `WalBackend` opened on a
///   shard-suffixed path (e.g. `data.{idx}.wal`).
/// - For shared state (counter, single DB connection), wrap it in
///   `Arc<Mutex<…>>` and have the factory clone the Arc.
pub struct AutoFlusher {
    handles: Vec<std::thread::JoinHandle<()>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    /// Shared per-shard wakeup primitives. Cloned from the cache at
    /// spawn time so [`stop`](Self::stop) can wake every parked
    /// flusher to observe the stop flag instead of waiting out the
    /// rest of its adaptive tick.
    flusher_wakeup: FlusherWakeup,
}

impl AutoFlusher {
    /// Spawn one flusher thread per shard. Returns an `AutoFlusher`
    /// handle; call [`stop`](Self::stop) when shutting down.
    pub fn spawn<K, V, B, F>(
        cache: Arc<WriteBehindCache<K, V>>,
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
                run_flusher_loop(shard_idx, cache, backend, stop, cfg);
            }));
        }
        Self {
            handles,
            stop,
            flusher_wakeup,
        }
    }

    /// Signal all flusher threads to stop, wake any that are parked
    /// on their condvars, perform a best-effort final drain on each
    /// shard, and join the threads. Idempotent.
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

fn run_flusher_loop<K, V, B>(
    shard_idx: usize,
    cache: Arc<WriteBehindCache<K, V>>,
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
        // Errors are deliberately ignored at this layer — the entries
        // are already requeued internally on `Err`, and the user's
        // backend is responsible for any logging it needs.
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
        // Event-driven wait: park on the per-shard Condvar with a
        // timeout equal to the adaptive tick. Writers crossing the
        // 50%-fill watermark notify, so a burst gets drained within a
        // notify+context-switch instead of waiting out the remaining
        // tick. If the wait times out, fall through to the next drain
        // (preserves the original adaptive timer behaviour).
        let park = &cache.flusher_wakeup[shard_idx];
        let mut pending = match park.0.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if *pending {
            *pending = false;
        } else {
            let (g, _timeout_res) = match park.1.wait_timeout(pending, tick) {
                Ok(v) => v,
                Err(poisoned) => poisoned.into_inner(),
            };
            pending = g;
            *pending = false;
        }
        drop(pending);
    }
    // Best-effort final drain on shutdown. `idle_ticks=0` means
    // "everything is eligible".
    for _ in 0..cfg.final_drain_passes {
        let n = cache
            .flush_shard_idle(shard_idx, &mut backend, 0, cfg.max_records_per_drain)
            .unwrap_or(0);
        if n == 0 {
            break;
        }
    }
}

impl<K, V, S> WriteBehindCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    /// Build a fresh, properly-sized cache from the live entries of
    /// this one. Use this to remediate read-time fragmentation after
    /// the underlying map has resized many times: copying live
    /// values into a freshly-built map relays them out contiguously
    /// in the heap, restoring read locality.
    ///
    /// The returned cache:
    ///
    /// - is sized to `2 × current live entries` (rounded up to the
    ///   next power of two), generous on purpose so it doesn't
    ///   immediately re-fragment;
    /// - inherits the same `dirty_shards` count and hash builder;
    /// - contains only the **live** entries (tombstones are dropped);
    /// - has an **empty** dirty queue — any entries that hadn't been
    ///   flushed yet are lost. Call [`flush_idle`](Self::flush_idle)
    ///   to drain dirty work _before_ compacting, otherwise you'll
    ///   silently drop pending writes.
    ///
    /// Compaction is `O(n)` over live entries plus the cost of
    /// constructing a freshly-sized ConcurrentMap. It is intended for
    /// occasional use (during a maintenance window or once an hour),
    /// not as a hot-path operation.
    pub fn compact(&self) -> Self {
        let mut live_count = 0usize;
        self.map.for_each(|_, r| {
            if r.value.is_some() {
                live_count += 1;
            }
        });

        let new_capacity = (live_count.saturating_mul(2)).max(64).next_power_of_two();
        let dirty_shards = self.dirty_shards.len();

        let new = Self::with_hasher_and_config(
            self.hash_builder.clone(),
            WriteBehindConfig {
                dirty_shards,
                initial_capacity: new_capacity,
                ..WriteBehindConfig::default()
            },
        );
        new.clock
            .store(self.clock.load(Ordering::Relaxed), Ordering::Relaxed);

        let mut to_copy: Vec<(K, V)> = Vec::new();
        self.map.for_each(|k, r| {
            if let Some(v) = &r.value {
                to_copy.push((k.clone(), v.as_ref().clone()));
            }
        });
        for (k, v) in to_copy {
            new.load_clean(k, v);
        }
        new
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type CollectedFlushes = Arc<std::sync::Mutex<Vec<(u64, Option<u64>, Operation)>>>;

    #[derive(Default)]
    struct CollectBackend<K: Clone, V: Clone> {
        flushed: std::sync::Mutex<Vec<FlushRecord<K, V>>>,
        fail_next: std::sync::atomic::AtomicBool,
    }

    impl<K: Clone, V: Clone> FlushBackend<K, V> for CollectBackend<K, V> {
        type Error = &'static str;

        fn flush(&mut self, records: &[FlushRecord<K, V>]) -> Result<(), Self::Error> {
            if self
                .fail_next
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                return Err("forced");
            }
            let mut g = self.flushed.lock().unwrap();
            for r in records {
                g.push(r.clone());
            }
            Ok(())
        }
    }

    #[test]
    fn put_get_delete_cycle() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 100);
        assert_eq!(*cache.get(&1).unwrap(), 100);
        cache.delete(1);
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn put_arc_preserves_allocation_identity() {
        let cache = WriteBehindCache::<u64, String>::new();
        let value = Arc::new("alpha".to_string());
        cache.put_arc(1, Arc::clone(&value));

        let loaded = cache.get(&1).expect("stored value");
        assert!(Arc::ptr_eq(&value, &loaded));
    }

    #[test]
    fn load_clean_arc_does_not_flush() {
        let cache = WriteBehindCache::<u64, String>::new();
        let value = Arc::new("clean".to_string());
        cache.load_clean_arc(1, Arc::clone(&value));

        let loaded = cache.get(&1).expect("stored value");
        assert!(Arc::ptr_eq(&value, &loaded));

        let mut backend = CollectBackend::default();
        assert_eq!(cache.flush_idle(&mut backend, 0, usize::MAX), Ok(0));
        assert!(backend.flushed.lock().unwrap().is_empty());
    }

    #[test]
    fn version_increments_across_writes() {
        let cache = WriteBehindCache::<u64, u64>::new();
        let v1 = cache.put(1, 10);
        let v2 = cache.put(1, 20);
        let v3 = cache.delete(1);
        let v4 = cache.put(1, 30);
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);
        assert_eq!(v4, 4);
        assert_eq!(*cache.get(&1).unwrap(), 30);
    }

    #[test]
    fn read_with_avoids_arc_clone() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 42);
        let doubled = cache.read_with(&1, |v| v * 2);
        assert_eq!(doubled, Some(84));
    }

    #[test]
    fn get_ref_returns_reference_without_clone() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 42);
        cache.put(2, 100);
        let p = cache.pin();

        let r1 = p.get_ref(&1).expect("key 1 present");
        assert_eq!(*r1, 42);

        let r2 = p.get_ref(&2).expect("key 2 present");
        assert_eq!(*r2, 100);

        assert!(p.get_ref(&999).is_none(), "missing key returns None");
    }

    #[test]
    fn get_ref_returns_none_for_tombstoned_keys() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 42);
        cache.delete(1);
        let p = cache.pin();
        assert!(p.get_ref(&1).is_none(), "tombstoned key returns None");
    }

    #[test]
    fn flush_idle_drains_dirty_queue_in_version_order() {
        let cache = WriteBehindCache::<u64, u64>::new();
        for i in 0..8u64 {
            cache.put(i, i * 10);
        }
        // Advance clock to ensure idle threshold is met
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let mut backend = CollectBackend::<u64, u64>::default();
        let n = cache.flush_idle(&mut backend, 1, 64).unwrap();
        assert_eq!(n, 8);
        let flushed = backend.flushed.lock().unwrap();
        assert_eq!(flushed.len(), 8);
        for r in flushed.iter() {
            assert_eq!(r.op, Operation::Put);
            assert_eq!(*r.value.as_ref().unwrap().as_ref(), r.key * 10);
        }
    }

    #[test]
    fn flush_idle_skips_stale_versions() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 100); // version 1, queued
        cache.put(1, 200); // version 2, queued — supersedes v1
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let mut backend = CollectBackend::<u64, u64>::default();
        let n = cache.flush_idle(&mut backend, 1, 64).unwrap();
        // Both queue entries get drained, but only v2 emits a record;
        // v1 is detected stale (current version is 2) and dropped.
        assert_eq!(n, 1);
        let flushed = backend.flushed.lock().unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].version, 2);
        assert_eq!(*flushed[0].value.as_ref().unwrap().as_ref(), 200);
    }

    #[test]
    fn flush_idle_requeues_on_backend_failure() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 100);
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let mut backend = CollectBackend::<u64, u64>::default();
        backend
            .fail_next
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(cache.flush_idle(&mut backend, 1, 64).is_err());
        // Data still readable in cache.
        assert_eq!(*cache.get(&1).unwrap(), 100);
        // Retry succeeds.
        let n = cache.flush_idle(&mut backend, 1, 64).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn delete_tombstone_removed_after_flush() {
        let cache = WriteBehindCache::<u64, u64>::new();
        cache.put(1, 100);
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let mut backend = CollectBackend::<u64, u64>::default();
        cache.flush_idle(&mut backend, 1, 64).unwrap();

        cache.delete(1);
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        cache.flush_idle(&mut backend, 1, 64).unwrap();

        // Tombstone should have been cleaned up; a fresh put should
        // succeed and the value should be readable. Version is the
        // global clock tick (not per-key serial), so we don't check
        // exact value — just that the put landed.
        let v = cache.put(1, 200);
        assert!(v > 0);
        assert_eq!(*cache.get(&1).unwrap(), 200);
    }

    #[test]
    fn evict_idle_drops_untouched_entries() {
        let cache = WriteBehindCache::<u64, u64>::new();
        for i in 0..4u64 {
            cache.load_clean(i, i * 10);
        }
        // Force the clock far past every entry's last_touch.
        for _ in 0..1000u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let evicted = cache.evict_idle(100);
        assert_eq!(evicted, 4);
        assert!(cache.get(&0).is_none());
    }

    #[test]
    fn pinned_put_and_delete_round_trip() {
        let cache = WriteBehindCache::<u64, u64>::new();
        {
            let p = cache.pin();
            let v1 = p.put(1, 100);
            let v2 = p.put(1, 200);
            assert_eq!(v1, 1);
            assert_eq!(v2, 2);
            assert_eq!(p.get(&1).map(|a| *a), Some(200));
            let v3 = p.delete(1);
            assert_eq!(v3, 3);
            assert!(p.get(&1).is_none());
        }
        // The dirty queue should reflect 3 entries pushed (put, put, delete).
        let stats = cache.stats();
        assert!(stats.dirty >= 1);
    }

    #[test]
    fn flush_via_pinned_writes_drains_correctly() {
        let cache = WriteBehindCache::<u64, u64>::new();
        {
            let p = cache.pin();
            for i in 0..16u64 {
                p.put(i, i * 100);
            }
        }
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        let mut backend = CollectBackend::<u64, u64>::default();
        let n = cache.flush_idle(&mut backend, 1, 64).unwrap();
        assert_eq!(n, 16);
        let g = backend.flushed.lock().unwrap();
        assert_eq!(g.len(), 16);
    }

    #[test]
    fn flush_shard_idle_only_drains_one_shard() {
        let cache = WriteBehindCache::<u64, u64>::new();
        for i in 0..256u64 {
            cache.put(i, i * 11);
        }
        for _ in 0..32u64 {
            let _ = cache.clock.fetch_add(1, Ordering::Relaxed);
        }
        // Measure dirty depth distribution: across `shard_count`
        // shards we should see roughly 256 / N entries each.
        let total_before: usize = (0..cache.shard_count())
            .map(|i| cache.shard_dirty_depth(i))
            .sum();
        assert_eq!(total_before, 256);

        let before_shard_0 = cache.shard_dirty_depth(0);
        let mut backend = CollectBackend::<u64, u64>::default();
        // Drain only shard 0.
        let n = cache.flush_shard_idle(0, &mut backend, 1, 4096).unwrap();
        assert_eq!(n, before_shard_0);
        assert_eq!(cache.shard_dirty_depth(0), 0);

        let total_after: usize = (0..cache.shard_count())
            .map(|i| cache.shard_dirty_depth(i))
            .sum();
        assert_eq!(total_after, 256 - n);
    }

    #[test]
    fn auto_flusher_drains_dirty_writes_in_background() {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;

        let cache = Arc::new(WriteBehindCache::<u64, u64>::new());
        let flushed = Arc::new(AtomicUsize::new(0));
        let collected = Arc::new(Mutex::new(Vec::<(u64, Option<u64>, Operation)>::new()));

        struct CountingBackend {
            flushed: Arc<AtomicUsize>,
            collected: CollectedFlushes,
        }
        impl FlushBackend<u64, u64> for CountingBackend {
            type Error = ();
            fn flush(&mut self, records: &[FlushRecord<u64, u64>]) -> Result<(), ()> {
                self.flushed
                    .fetch_add(records.len(), std::sync::atomic::Ordering::Relaxed);
                let mut g = self.collected.lock().unwrap();
                for r in records {
                    g.push((r.key, r.value.as_deref().copied(), r.op));
                }
                Ok(())
            }
        }

        let cfg = AutoFlusherConfig {
            min_tick_ms: 1,
            max_tick_ms: 10,
            target_depth: 128,
            max_records_per_drain: 1024,
            idle_ticks_threshold: 1,
            final_drain_passes: 16,
        };
        let flushed_for_factory = Arc::clone(&flushed);
        let collected_for_factory = Arc::clone(&collected);
        let auto = AutoFlusher::spawn(
            Arc::clone(&cache),
            move |_shard_idx| CountingBackend {
                flushed: Arc::clone(&flushed_for_factory),
                collected: Arc::clone(&collected_for_factory),
            },
            cfg,
        );

        // Push a wave of writes. Each write also bumps the clock, so
        // entries become idle-eligible quickly.
        for i in 0..512u64 {
            cache.put(i, i.wrapping_mul(17));
        }
        // Give the flushers a moment to wake up, observe high depth,
        // and drain.
        std::thread::sleep(Duration::from_millis(150));

        // Stop and final-drain.
        auto.stop();

        let total_flushed = flushed.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            total_flushed >= 512,
            "expected at least 512 records flushed, got {total_flushed}",
        );
        let g = collected.lock().unwrap();
        // Spot-check a handful of records made it through.
        let v100 = g.iter().rfind(|r| r.0 == 100).expect("key 100 flushed");
        assert_eq!(v100.1, Some(100u64.wrapping_mul(17)));
    }

    #[test]
    fn auto_flusher_stop_is_idempotent_and_drains_remaining() {
        use std::sync::Mutex;
        use std::time::Duration;

        let cache = Arc::new(WriteBehindCache::<u64, u64>::new());
        let collected: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));

        struct B {
            seen: Arc<Mutex<Vec<u64>>>,
        }
        impl FlushBackend<u64, u64> for B {
            type Error = ();
            fn flush(&mut self, records: &[FlushRecord<u64, u64>]) -> Result<(), ()> {
                let mut g = self.seen.lock().unwrap();
                for r in records {
                    g.push(r.key);
                }
                Ok(())
            }
        }

        let collected_for_factory = Arc::clone(&collected);
        let auto = AutoFlusher::spawn(
            Arc::clone(&cache),
            move |_| B {
                seen: Arc::clone(&collected_for_factory),
            },
            AutoFlusherConfig {
                min_tick_ms: 1,
                max_tick_ms: 5,
                target_depth: 32,
                max_records_per_drain: 128,
                idle_ticks_threshold: 1,
                final_drain_passes: 32,
            },
        );

        // Push writes right up to stop and rely on the final-drain.
        for i in 0..64u64 {
            cache.put(i, i);
        }
        std::thread::sleep(Duration::from_millis(20));
        auto.stop(); // final drain runs here

        let g = collected.lock().unwrap();
        assert!(
            g.len() >= 64,
            "expected at least 64 keys flushed (got {}); final drain should have caught laggards",
            g.len(),
        );
    }

    #[test]
    fn compact_preserves_live_entries_and_drops_dirty_queue() {
        let cache = WriteBehindCache::<u64, u64>::new();
        for i in 0..32u64 {
            cache.load_clean(i, i * 7);
        }
        // Some dirty writes that will be lost on compact.
        cache.put(100, 999);
        cache.put(101, 888);
        let dirty_before = cache.stats().dirty;
        assert!(dirty_before >= 2);

        let compacted = cache.compact();

        // Live entries copied across.
        for i in 0..32u64 {
            assert_eq!(*compacted.get(&i).unwrap(), i * 7);
        }
        // The two dirty entries' values were *also* live in the source
        // map at compaction time, so they're preserved as live data.
        assert_eq!(*compacted.get(&100).unwrap(), 999);
        assert_eq!(*compacted.get(&101).unwrap(), 888);

        // But the dirty queue starts empty in the new cache.
        assert_eq!(compacted.stats().dirty, 0);
    }
}
