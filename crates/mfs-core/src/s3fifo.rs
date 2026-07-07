//! Bounded in-process cache with an S3-FIFO-style admission/eviction policy.
//!
//! This is a separate policy-bearing cache type. It intentionally does **not**
//! modify [`crate::lockfree::LockFreeCache`] or [`crate::writeback::WriteBehindCache`],
//! because every admission policy has read/write overhead and the raw hot path
//! must stay policy-free.
//!
//! Design, clean-room from S3-FIFO/quick_cache:
//! - per-shard `RwLock` so reads can run concurrently inside a shard;
//! - resident entries live in a hash map;
//! - cold FIFO receives new entries;
//! - hot FIFO receives entries that were referenced while cold or found in the
//!   ghost filter;
//! - ghost FIFO stores only hashes of recently evicted cold entries;
//! - hits only bump a tiny saturated reference counter, they do not mutate FIFO
//!   lists.

use crate::FastBuildHasher;
use crate::tiny_lfu::TinyLfu;
use crossbeam_utils::CachePadded;
use hashbrown::{HashMap, HashSet};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const MAX_REF: u8 = 2;
const MAX_FREQUENCY: u8 = 15;
const DEFAULT_HOT_RATIO_PERCENT: usize = 90;
const DEFAULT_GHOST_RATIO_PERCENT: usize = 25;
const DEFAULT_SMALL_TO_MAIN_THRESHOLD: u8 = 1;
const MIN_SHARD_CAPACITY: usize = 32;
const FREQUENCY_DEPTH: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResidentState {
    Cold,
    Hot,
}

struct Resident<V> {
    value: Arc<V>,
    state: ResidentState,
    referenced: AtomicU8,
    hash: u64,
}

struct AdmissionCandidate<K> {
    key: K,
    hash: u64,
    frequency: u8,
}

struct AdmissionSample {
    prior_frequency: u8,
    frequency: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdmissionCapacityGate {
    maximum_frequency: u8,
}

struct Shard<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    map: HashMap<K, Resident<V>, S>,
    cold: VecDeque<K>,
    hot: VecDeque<K>,
    ghost: VecDeque<u64>,
    ghost_set: HashSet<u64>,
    capacity: usize,
    hot_capacity: usize,
    ghost_capacity: usize,
    hot_len: usize,
    cold_len: usize,
    small_to_main_threshold: u8,
    frequency: Option<FrequencySketch>,
    admission: Option<TinyLfu>,
    admission_gate: Option<AdmissionCapacityGate>,
}

impl<K, V, S> Shard<K, V, S>
where
    K: Eq + Hash + Clone,
    S: BuildHasher,
{
    fn new(
        capacity: usize,
        hot_ratio_percent: usize,
        ghost_ratio_percent: usize,
        small_to_main_threshold: u8,
        admission_enabled: bool,
        admission_experiment: Option<S3FifoAdmissionExperiment>,
        hash_builder: S,
    ) -> Self {
        let cap = capacity.max(1);
        let hot_capacity = ((cap.saturating_mul(hot_ratio_percent)) / 100).clamp(1, cap);
        let ghost_capacity = (cap.saturating_mul(ghost_ratio_percent)) / 100;
        let admission =
            admission_enabled.then(|| build_admission_sketch(cap, admission_experiment));
        let admission_gate = if admission_enabled {
            match admission_experiment {
                Some(S3FifoAdmissionExperiment::CapacityGate { maximum_frequency }) => {
                    Some(AdmissionCapacityGate { maximum_frequency })
                }
                _ => None,
            }
        } else {
            None
        };
        Self {
            map: HashMap::with_hasher(hash_builder),
            cold: VecDeque::new(),
            hot: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: HashSet::new(),
            capacity: cap,
            hot_capacity,
            ghost_capacity,
            hot_len: 0,
            cold_len: 0,
            small_to_main_threshold: small_to_main_threshold.clamp(1, MAX_FREQUENCY),
            frequency: if small_to_main_threshold > 1 {
                Some(FrequencySketch::with_capacity(cap))
            } else {
                None
            },
            admission,
            admission_gate,
        }
    }

    fn insert(&mut self, key: K, hash: u64, value: V) -> Option<Arc<V>> {
        let frequency = self.frequency.as_ref();
        let admission = self.admission.as_ref();
        if let Some(existing) = self.map.get_mut(&key) {
            if let Some(frequency) = frequency {
                frequency.increment(existing.hash);
            }
            if let Some(admission) = admission {
                admission.increment(existing.hash);
            }
            let old = std::mem::replace(&mut existing.value, Arc::new(value));
            bump_ref(&existing.referenced);
            return Some(old);
        }

        let admission_sample = self.record_admission(hash);
        if self.reject_candidate_by_capacity_gate(admission_sample.as_ref()) {
            return None;
        }
        self.record_frequency(hash);
        let candidate_key = key.clone();

        let from_ghost = self.ghost_set.remove(&hash);
        let state = if from_ghost {
            ResidentState::Hot
        } else {
            ResidentState::Cold
        };
        let resident = Resident {
            value: Arc::new(value),
            state,
            referenced: AtomicU8::new(if from_ghost { 1 } else { 0 }),
            hash,
        };
        match state {
            ResidentState::Hot => {
                self.hot.push_back(key.clone());
                self.hot_len += 1;
            }
            ResidentState::Cold => {
                self.cold.push_back(key.clone());
                self.cold_len += 1;
            }
        }
        self.map.insert(key, resident);
        let admission_candidate = admission_sample.map(|sample| AdmissionCandidate {
            key: candidate_key,
            hash,
            frequency: sample.frequency,
        });
        self.evict_to_capacity(admission_candidate.as_ref());
        None
    }

    fn insert_diagnostics(
        &mut self,
        key: K,
        hash: u64,
        value: V,
    ) -> (Option<Arc<V>>, S3FifoOpDiagnostics) {
        let mut diagnostics = S3FifoOpDiagnostics::default();
        let frequency = self.frequency.as_ref();
        let admission = self.admission.as_ref();

        let map_lookup_start = Instant::now();
        if let Some(existing) = self.map.get_mut(&key) {
            diagnostics.map_lookup_read_closure += map_lookup_start.elapsed();

            let admission_start = Instant::now();
            if let Some(frequency) = frequency {
                frequency.increment(existing.hash);
            }
            if let Some(admission) = admission {
                admission.increment(existing.hash);
            }
            diagnostics.admission_bookkeeping += admission_start.elapsed();

            let fifo_start = Instant::now();
            let old = std::mem::replace(&mut existing.value, Arc::new(value));
            bump_ref(&existing.referenced);
            diagnostics.fifo_maintenance += fifo_start.elapsed();
            return (Some(old), diagnostics);
        }
        diagnostics.map_lookup_read_closure += map_lookup_start.elapsed();

        let admission_start = Instant::now();
        let admission_sample = self.record_admission(hash);
        if self.reject_candidate_by_capacity_gate(admission_sample.as_ref()) {
            diagnostics.admission_bookkeeping += admission_start.elapsed();
            return (None, diagnostics);
        }
        self.record_frequency(hash);
        diagnostics.admission_bookkeeping += admission_start.elapsed();

        let candidate_key = key.clone();

        let ghost_start = Instant::now();
        let from_ghost = self.ghost_set.remove(&hash);
        diagnostics.ghost_bookkeeping += ghost_start.elapsed();

        let state = if from_ghost {
            ResidentState::Hot
        } else {
            ResidentState::Cold
        };
        let resident = Resident {
            value: Arc::new(value),
            state,
            referenced: AtomicU8::new(if from_ghost { 1 } else { 0 }),
            hash,
        };
        let fifo_start = Instant::now();
        match state {
            ResidentState::Hot => {
                self.hot.push_back(key.clone());
                self.hot_len += 1;
            }
            ResidentState::Cold => {
                self.cold.push_back(key.clone());
                self.cold_len += 1;
            }
        }
        self.map.insert(key, resident);
        diagnostics.fifo_maintenance += fifo_start.elapsed();

        let admission_candidate = admission_sample.map(|sample| AdmissionCandidate {
            key: candidate_key,
            hash,
            frequency: sample.frequency,
        });
        self.evict_to_capacity_diagnostics(admission_candidate.as_ref(), &mut diagnostics);
        (None, diagnostics)
    }

    fn get(&self, key: &K) -> Option<Arc<V>> {
        let resident = self.map.get(key)?;
        self.record_frequency(resident.hash);
        bump_ref(&resident.referenced);
        Some(Arc::clone(&resident.value))
    }

    fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let resident = self.map.get(key)?;
        self.record_frequency(resident.hash);
        bump_ref(&resident.referenced);
        Some(f(resident.value.as_ref()))
    }

    fn read_with_diagnostics<R, F>(&self, key: &K, f: F) -> (Option<R>, S3FifoOpDiagnostics)
    where
        F: FnOnce(&V) -> R,
    {
        let mut diagnostics = S3FifoOpDiagnostics::default();
        let map_lookup_start = Instant::now();
        let result = self.map.get(key).map(|resident| {
            self.record_frequency(resident.hash);
            bump_ref(&resident.referenced);
            f(resident.value.as_ref())
        });
        diagnostics.map_lookup_read_closure += map_lookup_start.elapsed();
        (result, diagnostics)
    }

    fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let resident = self.map.remove(key)?;
        match resident.state {
            ResidentState::Hot => self.hot_len = self.hot_len.saturating_sub(1),
            ResidentState::Cold => self.cold_len = self.cold_len.saturating_sub(1),
        }
        Some(resident.value)
    }

    fn evict_to_capacity(&mut self, admission_candidate: Option<&AdmissionCandidate<K>>) {
        while self.map.len() > self.capacity {
            let progressed = if self.cold_len > 0 {
                self.evict_cold_once(admission_candidate)
            } else {
                self.evict_hot_once(admission_candidate)
            };
            if !progressed {
                if self.cold_len > 0 {
                    self.cold_len = 0;
                    continue;
                }
                if !self.evict_any_once() {
                    break;
                }
            }
        }
        while self.hot_len > self.hot_capacity && self.hot_len > 0 {
            if !self.evict_hot_once(admission_candidate) {
                self.hot_len = 0;
                break;
            }
        }
        while self.ghost.len() > self.ghost_capacity {
            if let Some(hash) = self.ghost.pop_front() {
                self.ghost_set.remove(&hash);
            }
        }
    }

    fn evict_to_capacity_diagnostics(
        &mut self,
        admission_candidate: Option<&AdmissionCandidate<K>>,
        diagnostics: &mut S3FifoOpDiagnostics,
    ) {
        while self.map.len() > self.capacity {
            let progressed = if self.cold_len > 0 {
                self.evict_cold_once_diagnostics(admission_candidate, diagnostics)
            } else {
                self.evict_hot_once_diagnostics(admission_candidate, diagnostics)
            };
            if !progressed {
                if self.cold_len > 0 {
                    let fifo_start = Instant::now();
                    self.cold_len = 0;
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    continue;
                }
                if !self.evict_any_once_diagnostics(diagnostics) {
                    break;
                }
            }
        }
        while self.hot_len > self.hot_capacity && self.hot_len > 0 {
            if !self.evict_hot_once_diagnostics(admission_candidate, diagnostics) {
                let fifo_start = Instant::now();
                self.hot_len = 0;
                diagnostics.fifo_maintenance += fifo_start.elapsed();
                break;
            }
        }
        while self.ghost.len() > self.ghost_capacity {
            let ghost_start = Instant::now();
            if let Some(hash) = self.ghost.pop_front() {
                self.ghost_set.remove(&hash);
            }
            diagnostics.ghost_bookkeeping += ghost_start.elapsed();
        }
    }

    fn evict_cold_once(&mut self, admission_candidate: Option<&AdmissionCandidate<K>>) -> bool {
        while let Some(key) = self.cold.pop_front() {
            let hash = {
                let Some(resident) = self.map.get_mut(&key) else {
                    continue;
                };
                if resident.state != ResidentState::Cold {
                    continue;
                }
                let referenced = resident.referenced.load(Ordering::Relaxed);
                let frequency = self
                    .frequency
                    .as_ref()
                    .map_or(0, |frequency| frequency.frequency(resident.hash));
                if referenced >= self.small_to_main_threshold
                    || frequency >= self.small_to_main_threshold
                {
                    if referenced != 0 {
                        resident.referenced.fetch_sub(1, Ordering::Relaxed);
                    }
                    resident.state = ResidentState::Hot;
                    self.cold_len = self.cold_len.saturating_sub(1);
                    self.hot_len += 1;
                    self.hot.push_back(key);
                    return true;
                }
                resident.hash
            };
            if self.reject_candidate_if_victim_hotter(admission_candidate, hash) {
                return true;
            }
            self.map.remove(&key);
            self.cold_len = self.cold_len.saturating_sub(1);
            self.ghost.push_back(hash);
            self.ghost_set.insert(hash);
            return true;
        }
        false
    }

    fn evict_cold_once_diagnostics(
        &mut self,
        admission_candidate: Option<&AdmissionCandidate<K>>,
        diagnostics: &mut S3FifoOpDiagnostics,
    ) -> bool {
        loop {
            let fifo_start = Instant::now();
            let Some(key) = self.cold.pop_front() else {
                diagnostics.fifo_maintenance += fifo_start.elapsed();
                return false;
            };
            diagnostics.fifo_maintenance += fifo_start.elapsed();

            let fifo_start = Instant::now();
            let hash = {
                let Some(resident) = self.map.get_mut(&key) else {
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    continue;
                };
                if resident.state != ResidentState::Cold {
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    continue;
                }
                let referenced = resident.referenced.load(Ordering::Relaxed);
                let frequency = self
                    .frequency
                    .as_ref()
                    .map_or(0, |frequency| frequency.frequency(resident.hash));
                if referenced >= self.small_to_main_threshold
                    || frequency >= self.small_to_main_threshold
                {
                    if referenced != 0 {
                        resident.referenced.fetch_sub(1, Ordering::Relaxed);
                    }
                    resident.state = ResidentState::Hot;
                    self.cold_len = self.cold_len.saturating_sub(1);
                    self.hot_len += 1;
                    self.hot.push_back(key);
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    return true;
                }
                let hash = resident.hash;
                diagnostics.fifo_maintenance += fifo_start.elapsed();
                hash
            };
            if self.reject_candidate_if_victim_hotter_diagnostics(
                admission_candidate,
                hash,
                diagnostics,
            ) {
                return true;
            }

            let fifo_start = Instant::now();
            self.map.remove(&key);
            self.cold_len = self.cold_len.saturating_sub(1);
            diagnostics.fifo_maintenance += fifo_start.elapsed();

            let ghost_start = Instant::now();
            self.ghost.push_back(hash);
            self.ghost_set.insert(hash);
            diagnostics.ghost_bookkeeping += ghost_start.elapsed();
            return true;
        }
    }

    fn evict_hot_once(&mut self, admission_candidate: Option<&AdmissionCandidate<K>>) -> bool {
        while let Some(key) = self.hot.pop_front() {
            let hash = {
                let Some(resident) = self.map.get_mut(&key) else {
                    continue;
                };
                if resident.state != ResidentState::Hot {
                    continue;
                }
                if resident.referenced.load(Ordering::Relaxed) != 0 {
                    resident.referenced.fetch_sub(1, Ordering::Relaxed);
                    self.hot.push_back(key);
                    return true;
                }
                resident.hash
            };
            if self.reject_candidate_if_victim_hotter(admission_candidate, hash) {
                return true;
            }
            self.map.remove(&key);
            self.hot_len = self.hot_len.saturating_sub(1);
            return true;
        }
        false
    }

    fn evict_hot_once_diagnostics(
        &mut self,
        admission_candidate: Option<&AdmissionCandidate<K>>,
        diagnostics: &mut S3FifoOpDiagnostics,
    ) -> bool {
        loop {
            let fifo_start = Instant::now();
            let Some(key) = self.hot.pop_front() else {
                diagnostics.fifo_maintenance += fifo_start.elapsed();
                return false;
            };
            diagnostics.fifo_maintenance += fifo_start.elapsed();

            let fifo_start = Instant::now();
            let hash = {
                let Some(resident) = self.map.get_mut(&key) else {
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    continue;
                };
                if resident.state != ResidentState::Hot {
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    continue;
                }
                if resident.referenced.load(Ordering::Relaxed) != 0 {
                    resident.referenced.fetch_sub(1, Ordering::Relaxed);
                    self.hot.push_back(key);
                    diagnostics.fifo_maintenance += fifo_start.elapsed();
                    return true;
                }
                let hash = resident.hash;
                diagnostics.fifo_maintenance += fifo_start.elapsed();
                hash
            };
            if self.reject_candidate_if_victim_hotter_diagnostics(
                admission_candidate,
                hash,
                diagnostics,
            ) {
                return true;
            }

            let fifo_start = Instant::now();
            self.map.remove(&key);
            self.hot_len = self.hot_len.saturating_sub(1);
            diagnostics.fifo_maintenance += fifo_start.elapsed();
            return true;
        }
    }

    fn evict_any_once(&mut self) -> bool {
        let Some(key) = self.map.keys().next().cloned() else {
            return false;
        };
        let Some(removed) = self.map.remove(&key) else {
            return false;
        };
        match removed.state {
            ResidentState::Cold => self.cold_len = self.cold_len.saturating_sub(1),
            ResidentState::Hot => self.hot_len = self.hot_len.saturating_sub(1),
        }
        true
    }

    fn evict_any_once_diagnostics(&mut self, diagnostics: &mut S3FifoOpDiagnostics) -> bool {
        let fifo_start = Instant::now();
        let Some(key) = self.map.keys().next().cloned() else {
            diagnostics.fifo_maintenance += fifo_start.elapsed();
            return false;
        };
        let Some(removed) = self.map.remove(&key) else {
            diagnostics.fifo_maintenance += fifo_start.elapsed();
            return false;
        };
        match removed.state {
            ResidentState::Cold => self.cold_len = self.cold_len.saturating_sub(1),
            ResidentState::Hot => self.hot_len = self.hot_len.saturating_sub(1),
        }
        diagnostics.fifo_maintenance += fifo_start.elapsed();
        true
    }

    #[inline]
    fn record_frequency(&self, hash: u64) {
        if let Some(frequency) = &self.frequency {
            frequency.increment(hash);
        }
    }

    #[inline]
    fn record_admission(&self, hash: u64) -> Option<AdmissionSample> {
        let admission = self.admission.as_ref()?;
        let prior_frequency = admission.estimate(hash);
        let frequency = admission.increment(hash);
        Some(AdmissionSample {
            prior_frequency,
            frequency,
        })
    }

    #[inline]
    fn reject_candidate_by_capacity_gate(&self, sample: Option<&AdmissionSample>) -> bool {
        let Some(gate) = self.admission_gate else {
            return false;
        };
        let Some(sample) = sample else {
            return false;
        };
        self.map.len() >= self.capacity && sample.prior_frequency <= gate.maximum_frequency
    }

    fn reject_candidate_if_victim_hotter(
        &mut self,
        candidate: Option<&AdmissionCandidate<K>>,
        victim_hash: u64,
    ) -> bool {
        let Some(candidate) = candidate else {
            return false;
        };
        if candidate.hash == victim_hash {
            return false;
        }
        let Some(admission) = &self.admission else {
            return false;
        };
        if admission.estimate(victim_hash) <= candidate.frequency {
            return false;
        }
        let Some(removed) = self.map.remove(&candidate.key) else {
            return false;
        };
        match removed.state {
            ResidentState::Cold => {
                self.cold_len = self.cold_len.saturating_sub(1);
                remove_queued_key(&mut self.cold, &candidate.key);
            }
            ResidentState::Hot => {
                self.hot_len = self.hot_len.saturating_sub(1);
                remove_queued_key(&mut self.hot, &candidate.key);
            }
        }
        true
    }

    fn reject_candidate_if_victim_hotter_diagnostics(
        &mut self,
        candidate: Option<&AdmissionCandidate<K>>,
        victim_hash: u64,
        diagnostics: &mut S3FifoOpDiagnostics,
    ) -> bool {
        let admission_start = Instant::now();
        let Some(candidate) = candidate else {
            diagnostics.admission_bookkeeping += admission_start.elapsed();
            return false;
        };
        if candidate.hash == victim_hash {
            diagnostics.admission_bookkeeping += admission_start.elapsed();
            return false;
        }
        let Some(admission) = &self.admission else {
            diagnostics.admission_bookkeeping += admission_start.elapsed();
            return false;
        };
        if admission.estimate(victim_hash) <= candidate.frequency {
            diagnostics.admission_bookkeeping += admission_start.elapsed();
            return false;
        }
        diagnostics.admission_bookkeeping += admission_start.elapsed();

        let fifo_start = Instant::now();
        let Some(removed) = self.map.remove(&candidate.key) else {
            diagnostics.fifo_maintenance += fifo_start.elapsed();
            return false;
        };
        match removed.state {
            ResidentState::Cold => {
                self.cold_len = self.cold_len.saturating_sub(1);
                remove_queued_key(&mut self.cold, &candidate.key);
            }
            ResidentState::Hot => {
                self.hot_len = self.hot_len.saturating_sub(1);
                remove_queued_key(&mut self.hot, &candidate.key);
            }
        }
        diagnostics.fifo_maintenance += fifo_start.elapsed();
        true
    }
}

fn build_admission_sketch(
    capacity: usize,
    experiment: Option<S3FifoAdmissionExperiment>,
) -> TinyLfu {
    match experiment {
        Some(S3FifoAdmissionExperiment::WideSketch {
            min_width,
            sample_size_floor,
        }) => TinyLfu::with_capacity_and_floor(capacity, min_width, sample_size_floor),
        Some(S3FifoAdmissionExperiment::Packed4Bit {
            min_width,
            sample_size_floor,
        }) => TinyLfu::with_packed_4bit(capacity, min_width, sample_size_floor),
        Some(S3FifoAdmissionExperiment::Doorkeeper {
            min_width,
            sample_size_floor,
        }) => TinyLfu::with_doorkeeper(capacity, min_width, sample_size_floor),
        Some(S3FifoAdmissionExperiment::TwoCounterDecay {
            min_width,
            sample_size_floor,
        }) => TinyLfu::with_two_counter_decay(capacity, min_width, sample_size_floor),
        Some(S3FifoAdmissionExperiment::CapacityGate { .. }) | None => {
            TinyLfu::with_capacity(capacity)
        }
    }
}

fn remove_queued_key<K>(queue: &mut VecDeque<K>, key: &K)
where
    K: Eq,
{
    if queue.back() == Some(key) {
        queue.pop_back();
    }
}

struct FrequencySketch {
    width: usize,
    mask: usize,
    sample_size: usize,
    samples: AtomicUsize,
    counters: Box<[AtomicU8]>,
}

impl FrequencySketch {
    fn with_capacity(capacity: usize) -> Self {
        let width = capacity
            .max(16)
            .saturating_mul(4)
            .next_power_of_two()
            .max(64);
        let counters = (0..width.saturating_mul(FREQUENCY_DEPTH))
            .map(|_| AtomicU8::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            width,
            mask: width - 1,
            sample_size: capacity.max(1).saturating_mul(10).max(width),
            samples: AtomicUsize::new(0),
            counters,
        }
    }

    fn increment(&self, hash: u64) {
        for row in 0..FREQUENCY_DEPTH {
            let idx = self.index(hash, row);
            saturating_increment(&self.counters[idx]);
        }

        let samples = self.samples.fetch_add(1, Ordering::Relaxed) + 1;
        if samples >= self.sample_size
            && self
                .samples
                .compare_exchange(samples, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.age();
        }
    }

    fn frequency(&self, hash: u64) -> u8 {
        let mut min = MAX_FREQUENCY;
        for row in 0..FREQUENCY_DEPTH {
            let value = self.counters[self.index(hash, row)].load(Ordering::Relaxed);
            min = min.min(value);
        }
        min
    }

    #[inline]
    fn index(&self, hash: u64, row: usize) -> usize {
        let mixed = mix_frequency_hash(hash, row as u64);
        row * self.width + ((mixed as usize) & self.mask)
    }

    fn age(&self) {
        for counter in self.counters.iter() {
            counter
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    Some(value >> 1)
                })
                .ok();
        }
    }
}

#[inline]
fn mix_frequency_hash(hash: u64, row: u64) -> u64 {
    let mut mixed = hash ^ row.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    mixed ^ (mixed >> 29)
}

#[inline]
fn saturating_increment(counter: &AtomicU8) {
    let mut current = counter.load(Ordering::Relaxed);
    while current < MAX_FREQUENCY {
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

#[inline]
fn bump_ref(counter: &AtomicU8) {
    let mut current = counter.load(Ordering::Relaxed);
    while current < MAX_REF {
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

pub struct S3FifoCache<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher,
{
    shards: S3FifoShards<K, V, S>,
    shard_mask: usize,
    hash_builder: S,
}

type S3FifoShards<K, V, S> = Box<[CachePadded<RwLock<Shard<K, V, S>>>]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3FifoAdmissionExperiment {
    CapacityGate {
        maximum_frequency: u8,
    },
    WideSketch {
        min_width: usize,
        sample_size_floor: usize,
    },
    Packed4Bit {
        min_width: usize,
        sample_size_floor: usize,
    },
    Doorkeeper {
        min_width: usize,
        sample_size_floor: usize,
    },
    TwoCounterDecay {
        min_width: usize,
        sample_size_floor: usize,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct S3FifoOpDiagnostics {
    pub rwlock_read_acquire: Duration,
    pub rwlock_write_acquire: Duration,
    pub map_lookup_read_closure: Duration,
    pub fifo_maintenance: Duration,
    pub ghost_bookkeeping: Duration,
    pub admission_bookkeeping: Duration,
}

impl S3FifoOpDiagnostics {
    pub fn add_assign(&mut self, other: Self) {
        self.rwlock_read_acquire += other.rwlock_read_acquire;
        self.rwlock_write_acquire += other.rwlock_write_acquire;
        self.map_lookup_read_closure += other.map_lookup_read_closure;
        self.fifo_maintenance += other.fifo_maintenance;
        self.ghost_bookkeeping += other.ghost_bookkeeping;
        self.admission_bookkeeping += other.admission_bookkeeping;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3FifoReadDiagnostics<R> {
    pub result: Option<R>,
    pub metrics: S3FifoOpDiagnostics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3FifoInsertDiagnostics<V> {
    pub previous: Option<Arc<V>>,
    pub metrics: S3FifoOpDiagnostics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3FifoConfig {
    pub capacity: usize,
    pub shards: usize,
    pub hot_ratio_percent: usize,
    pub ghost_ratio_percent: usize,
    pub small_to_main_threshold: u8,
    pub admission_enabled: bool,
    pub admission_experiment: Option<S3FifoAdmissionExperiment>,
}

impl S3FifoConfig {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            shards: default_shards(),
            hot_ratio_percent: DEFAULT_HOT_RATIO_PERCENT,
            ghost_ratio_percent: DEFAULT_GHOST_RATIO_PERCENT,
            small_to_main_threshold: DEFAULT_SMALL_TO_MAIN_THRESHOLD,
            admission_enabled: false,
            admission_experiment: None,
        }
    }

    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards.max(1).next_power_of_two();
        self
    }

    pub fn with_hot_ratio_percent(mut self, percent: usize) -> Self {
        self.hot_ratio_percent = percent.clamp(1, 100);
        self
    }

    pub fn with_ghost_ratio_percent(mut self, percent: usize) -> Self {
        self.ghost_ratio_percent = percent;
        self
    }

    pub fn with_small_to_main_threshold(mut self, threshold: u8) -> Self {
        self.small_to_main_threshold = threshold.clamp(1, MAX_FREQUENCY);
        self
    }

    pub fn with_admission_filter(mut self, enabled: bool) -> Self {
        self.admission_enabled = enabled;
        self
    }

    pub fn with_admission_experiment(mut self, experiment: S3FifoAdmissionExperiment) -> Self {
        self.admission_enabled = true;
        self.admission_experiment = Some(experiment);
        self
    }

    pub fn with_experimental_capacity_gate(mut self, maximum_frequency: u8) -> Self {
        self.admission_enabled = true;
        self.admission_experiment =
            Some(S3FifoAdmissionExperiment::CapacityGate { maximum_frequency });
        self
    }

    pub fn with_experimental_wide_sketch_floor(
        mut self,
        min_width: usize,
        sample_size_floor: usize,
    ) -> Self {
        self.admission_enabled = true;
        self.admission_experiment = Some(S3FifoAdmissionExperiment::WideSketch {
            min_width,
            sample_size_floor,
        });
        self
    }

    pub fn with_experimental_packed_4bit(
        mut self,
        min_width: usize,
        sample_size_floor: usize,
    ) -> Self {
        self.admission_enabled = true;
        self.admission_experiment = Some(S3FifoAdmissionExperiment::Packed4Bit {
            min_width,
            sample_size_floor,
        });
        self
    }

    pub fn with_experimental_doorkeeper(
        mut self,
        min_width: usize,
        sample_size_floor: usize,
    ) -> Self {
        self.admission_enabled = true;
        self.admission_experiment = Some(S3FifoAdmissionExperiment::Doorkeeper {
            min_width,
            sample_size_floor,
        });
        self
    }

    pub fn with_experimental_two_counter_decay(
        mut self,
        min_width: usize,
        sample_size_floor: usize,
    ) -> Self {
        self.admission_enabled = true;
        self.admission_experiment = Some(S3FifoAdmissionExperiment::TwoCounterDecay {
            min_width,
            sample_size_floor,
        });
        self
    }
}

impl<K, V> S3FifoCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_hasher_and_capacity(FastBuildHasher::default(), capacity)
    }

    pub fn with_config(config: S3FifoConfig) -> Self {
        Self::with_hasher_and_config(FastBuildHasher::default(), config)
    }
}

impl<K, V, S> S3FifoCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    pub fn with_hasher_and_capacity(hash_builder: S, capacity: usize) -> Self {
        Self::with_hasher_and_config(hash_builder, S3FifoConfig::new(capacity))
    }

    pub fn with_hasher_and_config(hash_builder: S, config: S3FifoConfig) -> Self {
        let capacity = config.capacity.max(1);
        let mut shards = config.shards.max(1).next_power_of_two();
        while shards > 1 && capacity.div_ceil(shards) < MIN_SHARD_CAPACITY {
            shards >>= 1;
        }
        let per_shard = capacity.max(shards).div_ceil(shards);
        let mut vec = Vec::with_capacity(shards);
        for _ in 0..shards {
            vec.push(CachePadded::new(RwLock::new(Shard::new(
                per_shard,
                config.hot_ratio_percent.clamp(1, 100),
                config.ghost_ratio_percent,
                config.small_to_main_threshold,
                config.admission_enabled,
                config.admission_experiment,
                hash_builder.clone(),
            ))));
        }
        Self {
            shards: vec.into_boxed_slice(),
            shard_mask: shards - 1,
            hash_builder,
        }
    }

    #[inline]
    fn hash_key(&self, key: &K) -> u64 {
        self.hash_builder.hash_one(key)
    }
    #[inline]
    fn shard_idx(&self, hash: u64) -> usize {
        (hash.rotate_right(7) as usize) & self.shard_mask
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let hash = self.hash_key(key);
        self.shards[self.shard_idx(hash)].read().get(key)
    }

    pub fn read_with<R, F>(&self, key: &K, f: F) -> Option<R>
    where
        F: FnOnce(&V) -> R,
    {
        let hash = self.hash_key(key);
        self.shards[self.shard_idx(hash)].read().read_with(key, f)
    }

    pub fn read_with_diagnostics<R, F>(&self, key: &K, f: F) -> S3FifoReadDiagnostics<R>
    where
        F: FnOnce(&V) -> R,
    {
        let hash = self.hash_key(key);
        let lock_start = Instant::now();
        let shard = self.shards[self.shard_idx(hash)].read();
        let rwlock_read_acquire = lock_start.elapsed();
        let (result, mut metrics) = shard.read_with_diagnostics(key, f);
        metrics.rwlock_read_acquire += rwlock_read_acquire;
        S3FifoReadDiagnostics { result, metrics }
    }

    pub fn insert(&self, key: K, value: V) -> Option<Arc<V>> {
        let hash = self.hash_key(&key);
        self.shards[self.shard_idx(hash)]
            .write()
            .insert(key, hash, value)
    }

    pub fn insert_diagnostics(&self, key: K, value: V) -> S3FifoInsertDiagnostics<V> {
        let hash = self.hash_key(&key);
        let lock_start = Instant::now();
        let mut shard = self.shards[self.shard_idx(hash)].write();
        let rwlock_write_acquire = lock_start.elapsed();
        let (previous, mut metrics) = shard.insert_diagnostics(key, hash, value);
        metrics.rwlock_write_acquire += rwlock_write_acquire;
        S3FifoInsertDiagnostics { previous, metrics }
    }

    pub fn remove(&self, key: &K) -> Option<Arc<V>> {
        let hash = self.hash_key(key);
        self.shards[self.shard_idx(hash)].write().remove(key)
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().map.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn default_shards() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get() * 4)
        .unwrap_or(16)
        .next_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiny_lfu::TinyLfuCounterKind;

    #[test]
    fn insert_get_remove() {
        let cache = S3FifoCache::<u64, u64>::with_capacity(8);
        cache.insert(1, 10);
        assert_eq!(cache.get(&1).map(|v| *v), Some(10));
        assert_eq!(cache.remove(&1).map(|v| *v), Some(10));
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn capacity_is_bounded() {
        let cache = S3FifoCache::<u64, u64>::with_capacity(16);
        for i in 0..256u64 {
            cache.insert(i, i);
        }
        assert!(cache.len() <= 16 + cache.shards.len());
    }

    #[test]
    fn config_controls_policy_shape() {
        let cache = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(4)
                .with_shards(1)
                .with_hot_ratio_percent(97)
                .with_ghost_ratio_percent(0),
        );
        assert_eq!(cache.shards.len(), 1);
        for i in 0..32u64 {
            cache.insert(i, i);
        }
        assert!(cache.len() <= 4);
    }

    #[test]
    fn small_caches_avoid_tiny_shards() {
        let cache = S3FifoCache::<u64, u64>::with_config(S3FifoConfig::new(50).with_shards(16));
        assert!(cache.shards.len() < 16);
        for shard in cache.shards.iter() {
            assert!(shard.read().capacity >= MIN_SHARD_CAPACITY || cache.shards.len() == 1);
        }
    }

    #[test]
    fn default_threshold_does_not_allocate_frequency_sketch() {
        let default_config = S3FifoConfig::new(64);
        assert!(!default_config.admission_enabled);
        assert!(default_config.admission_experiment.is_none());

        let default_cache = S3FifoCache::<u64, u64>::with_capacity(64);
        for shard in default_cache.shards.iter() {
            let shard = shard.read();
            assert!(shard.frequency.is_none());
            assert!(shard.admission.is_none());
            assert!(shard.admission_gate.is_none());
        }

        let cache = S3FifoCache::<u64, u64>::with_config(S3FifoConfig::new(64).with_shards(1));
        assert!(cache.shards[0].read().frequency.is_none());
        assert!(cache.shards[0].read().admission.is_none());
        assert!(cache.shards[0].read().admission_gate.is_none());

        let tuned = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(64)
                .with_shards(1)
                .with_small_to_main_threshold(2),
        );
        assert!(tuned.shards[0].read().frequency.is_some());
        assert!(tuned.shards[0].read().admission.is_none());
        assert!(tuned.shards[0].read().admission_gate.is_none());

        let admission = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(64)
                .with_shards(1)
                .with_admission_filter(true),
        );
        assert!(admission.shards[0].read().admission.is_some());
        assert!(admission.shards[0].read().admission_gate.is_none());
        assert!(
            admission.shards[0]
                .read()
                .admission
                .as_ref()
                .is_some_and(
                    |admission| admission.counter_kind_for_test() == TinyLfuCounterKind::U8
                )
        );
    }

    #[test]
    fn capacity_gate_rejects_cold_candidate_only_when_configured() {
        let mut ungated = Shard::<u64, &'static str, FastBuildHasher>::new(
            1,
            50,
            0,
            1,
            true,
            None,
            FastBuildHasher::default(),
        );
        ungated.insert(1, 1, "resident");
        ungated.insert(2, 2, "newcomer");
        assert!(!ungated.map.contains_key(&1));
        assert!(ungated.map.contains_key(&2));

        let mut gated = Shard::<u64, &'static str, FastBuildHasher>::new(
            1,
            50,
            0,
            1,
            true,
            Some(S3FifoAdmissionExperiment::CapacityGate {
                maximum_frequency: 0,
            }),
            FastBuildHasher::default(),
        );
        gated.insert(1, 1, "resident");
        gated.insert(2, 2, "newcomer");
        assert!(gated.map.contains_key(&1));
        assert!(!gated.map.contains_key(&2));

        gated.insert(2, 2, "newcomer");
        assert!(!gated.map.contains_key(&1));
        assert!(gated.map.contains_key(&2));
    }

    #[test]
    fn wide_sketch_floor_changes_only_configured_admission() {
        let default = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(16)
                .with_shards(1)
                .with_admission_filter(true),
        );
        let default_shard = default.shards[0].read();
        let default_admission = default_shard.admission.as_ref().unwrap();
        assert_eq!(default_admission.width_for_test(), 64);
        assert_eq!(default_admission.sample_size_for_test(), 128);

        let wide = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(16)
                .with_shards(1)
                .with_experimental_wide_sketch_floor(4096, 4096),
        );
        let wide_shard = wide.shards[0].read();
        let wide_admission = wide_shard.admission.as_ref().unwrap();
        assert_eq!(wide_admission.width_for_test(), 4096);
        assert_eq!(wide_admission.sample_size_for_test(), 4096);
        assert_eq!(
            wide_admission.counter_kind_for_test(),
            TinyLfuCounterKind::U8
        );
    }

    #[test]
    fn packed_4bit_experiment_uses_packed_admission_sketch() {
        let cache = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(16)
                .with_shards(1)
                .with_experimental_packed_4bit(4096, 4096),
        );
        let shard = cache.shards[0].read();
        let admission = shard.admission.as_ref().unwrap();
        assert_eq!(admission.width_for_test(), 4096);
        assert_eq!(admission.sample_size_for_test(), 4096);
        assert_eq!(
            admission.counter_kind_for_test(),
            TinyLfuCounterKind::Packed4Bit
        );
    }

    #[test]
    fn doorkeeper_experiment_gates_first_touch_before_backing_sketch() {
        let cache = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(16)
                .with_shards(1)
                .with_experimental_doorkeeper(4096, 4096),
        );
        let shard = cache.shards[0].read();
        let admission = shard.admission.as_ref().unwrap();
        let hash = 12345;

        assert_eq!(admission.increment(hash), 0);
        assert!(admission.doorkeeper_contains_for_test(hash));
        assert_eq!(admission.estimate(hash), 0);
        assert_eq!(admission.increment(hash), 1);
        assert_eq!(admission.estimate(hash), 1);
    }

    #[test]
    fn hot_items_survive_scan_better_than_cold() {
        let cache = S3FifoCache::<u64, u64>::with_capacity(1024);
        for i in 0..512u64 {
            cache.insert(i, i);
        }
        for _ in 0..4 {
            let _ = cache.get(&1);
        }
        for i in 512..900u64 {
            cache.insert(i, i);
        }
        assert_eq!(cache.get(&1).map(|v| *v), Some(1));
    }

    #[test]
    fn small_to_main_threshold_controls_promotion_strength() {
        let threshold_two = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(2)
                .with_shards(1)
                .with_hot_ratio_percent(50)
                .with_ghost_ratio_percent(0)
                .with_small_to_main_threshold(2),
        );
        threshold_two.insert(1, 1);
        threshold_two.insert(2, 2);
        let _ = threshold_two.get(&1);
        threshold_two.insert(3, 3);
        assert_eq!(threshold_two.get(&1).map(|v| *v), Some(1));

        let threshold_three = S3FifoCache::<u64, u64>::with_config(
            S3FifoConfig::new(2)
                .with_shards(1)
                .with_hot_ratio_percent(50)
                .with_ghost_ratio_percent(0)
                .with_small_to_main_threshold(3),
        );
        threshold_three.insert(1, 1);
        threshold_three.insert(2, 2);
        let _ = threshold_three.get(&1);
        threshold_three.insert(3, 3);
        assert!(threshold_three.get(&1).is_none());
    }

    #[test]
    fn admission_filter_rejects_one_hit_newcomer() {
        let mut shard = Shard::<u64, &'static str, FastBuildHasher>::new(
            1,
            50,
            0,
            1,
            true,
            None,
            FastBuildHasher::default(),
        );
        shard.insert(1, 1, "resident");
        for _ in 0..8 {
            let _ = shard.record_admission(1);
        }

        shard.insert(2, 2, "newcomer");

        assert!(shard.map.contains_key(&1));
        assert!(!shard.map.contains_key(&2));
        assert_eq!(shard.map.len(), 1);
    }

    #[test]
    fn admission_filter_allows_frequent_newcomer() {
        let mut shard = Shard::<u64, &'static str, FastBuildHasher>::new(
            1,
            50,
            0,
            1,
            true,
            None,
            FastBuildHasher::default(),
        );
        shard.insert(1, 1, "resident");
        for _ in 0..8 {
            let _ = shard.record_admission(2);
        }

        shard.insert(2, 2, "newcomer");

        assert!(!shard.map.contains_key(&1));
        assert!(shard.map.contains_key(&2));
        assert_eq!(shard.map.len(), 1);
    }
}
