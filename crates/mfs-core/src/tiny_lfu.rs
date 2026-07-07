//! TinyLFU frequency estimator for opt-in cache admission.
//!
//! This is the frequency-estimation half of TinyLFU: a small Count-Min Sketch
//! with periodic aging. The default constructor intentionally keeps the original
//! 8-bit counter path without a doorkeeper; alternate layouts are opt-in for
//! S3FIFO admission experiments.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

const DEPTH: usize = 4;

pub struct TinyLfu {
    width: usize,
    mask: usize,
    sample_size: usize,
    samples: AtomicUsize,
    counters: TinyLfuCounters,
    doorkeeper: Option<DoorKeeper>,
}

impl TinyLfu {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_options(
            capacity,
            64,
            capacity.max(1).saturating_mul(8).max(64),
            TinyLfuCounterKind::U8,
            false,
        )
    }

    pub fn with_capacity_and_floor(
        capacity: usize,
        min_width: usize,
        sample_size_floor: usize,
    ) -> Self {
        Self::with_options(
            capacity,
            min_width,
            capacity
                .max(1)
                .saturating_mul(8)
                .max(64)
                .max(sample_size_floor),
            TinyLfuCounterKind::U8,
            false,
        )
    }

    pub fn with_packed_4bit(capacity: usize, min_width: usize, sample_size_floor: usize) -> Self {
        Self::with_options(
            capacity,
            min_width,
            capacity
                .max(1)
                .saturating_mul(8)
                .max(64)
                .max(sample_size_floor),
            TinyLfuCounterKind::Packed4Bit,
            false,
        )
    }

    pub fn with_doorkeeper(capacity: usize, min_width: usize, sample_size_floor: usize) -> Self {
        Self::with_options(
            capacity,
            min_width,
            capacity
                .max(1)
                .saturating_mul(8)
                .max(64)
                .max(sample_size_floor),
            TinyLfuCounterKind::U8,
            true,
        )
    }

    pub fn with_two_counter_decay(capacity: usize, min_width: usize, sample_size_floor: usize) -> Self {
        Self::with_options(
            capacity,
            min_width,
            capacity
                .max(1)
                .saturating_mul(8)
                .max(64)
                .max(sample_size_floor),
            TinyLfuCounterKind::TwoCounter,
            false,
        )
    }

    #[cfg(test)]
    fn with_sample_size(capacity: usize, sample_size: usize) -> Self {
        Self::with_options(capacity, 64, sample_size, TinyLfuCounterKind::U8, false)
    }

    fn with_options(
        capacity: usize,
        min_width: usize,
        sample_size: usize,
        counter_kind: TinyLfuCounterKind,
        doorkeeper_enabled: bool,
    ) -> Self {
        let width = capacity
            .max(16)
            .saturating_mul(4)
            .next_power_of_two()
            .max(64)
            .max(min_width.max(1).next_power_of_two());
        let counter_count = width.saturating_mul(DEPTH);
        Self {
            width,
            mask: width - 1,
            sample_size: sample_size.max(1),
            samples: AtomicUsize::new(0),
            counters: TinyLfuCounters::new(counter_kind, counter_count),
            doorkeeper: doorkeeper_enabled.then(|| DoorKeeper::with_capacity(width)),
        }
    }

    pub fn increment(&self, hash: u64) -> u8 {
        if let Some(doorkeeper) = &self.doorkeeper
            && !doorkeeper.check_and_set(hash)
        {
            self.record_sample();
            return self.estimate(hash);
        }

        for row in 0..DEPTH {
            self.counters.saturating_increment(self.index(hash, row));
        }

        self.record_sample();
        self.estimate(hash)
    }

    pub fn estimate(&self, hash: u64) -> u8 {
        let mut min = u8::MAX;
        for row in 0..DEPTH {
            min = min.min(self.counters.load(self.index(hash, row)));
        }
        min
    }

    fn record_sample(&self) {
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

    #[inline]
    fn index(&self, hash: u64, row: usize) -> usize {
        let mixed = mix_tiny_lfu_hash(hash, row as u64);
        row * self.width + ((mixed as usize) & self.mask)
    }

    fn age(&self) {
        self.counters.age();
        if let Some(doorkeeper) = &self.doorkeeper {
            doorkeeper.clear();
        }
    }

    #[cfg(test)]
    pub(crate) fn width_for_test(&self) -> usize {
        self.width
    }

    #[cfg(test)]
    pub(crate) fn sample_size_for_test(&self) -> usize {
        self.sample_size
    }

    #[cfg(test)]
    pub(crate) fn counter_kind_for_test(&self) -> TinyLfuCounterKind {
        self.counters.kind()
    }

    #[cfg(test)]
    pub(crate) fn doorkeeper_contains_for_test(&self, hash: u64) -> bool {
        self.doorkeeper
            .as_ref()
            .is_some_and(|doorkeeper| doorkeeper.contains(hash))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TinyLfuCounterKind {
    U8,
    Packed4Bit,
    TwoCounter,
}

enum TinyLfuCounters {
    U8(Box<[AtomicU8]>),
    Packed4Bit(Packed4BitCounters),
    TwoCounter(DualSketchCounters),
}

impl TinyLfuCounters {
    fn new(kind: TinyLfuCounterKind, len: usize) -> Self {
        match kind {
            TinyLfuCounterKind::U8 => Self::U8(
                (0..len)
                    .map(|_| AtomicU8::new(0))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            TinyLfuCounterKind::Packed4Bit => Self::Packed4Bit(Packed4BitCounters::new(len)),
            TinyLfuCounterKind::TwoCounter => Self::TwoCounter(DualSketchCounters::new(len)),
        }
    }

    #[cfg(test)]
    fn kind(&self) -> TinyLfuCounterKind {
        match self {
            Self::U8(_) => TinyLfuCounterKind::U8,
            Self::Packed4Bit(_) => TinyLfuCounterKind::Packed4Bit,
            Self::TwoCounter(_) => TinyLfuCounterKind::TwoCounter,
        }
    }

    fn load(&self, index: usize) -> u8 {
        match self {
            Self::U8(counters) => counters[index].load(Ordering::Relaxed),
            Self::Packed4Bit(counters) => counters.load(index),
            Self::TwoCounter(counters) => counters.load(index),
        }
    }

    fn saturating_increment(&self, index: usize) {
        match self {
            Self::U8(counters) => saturating_increment(&counters[index]),
            Self::Packed4Bit(counters) => counters.saturating_increment(index),
            Self::TwoCounter(counters) => counters.saturating_increment(index),
        }
    }

    fn age(&self) {
        match self {
            Self::U8(counters) => {
                for counter in counters.iter() {
                    counter
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                            Some(value >> 1)
                        })
                        .ok();
                }
            }
            Self::Packed4Bit(counters) => counters.age(),
            Self::TwoCounter(counters) => counters.age(),
        }
    }
}

struct Packed4BitCounters {
    bytes: Box<[AtomicU8]>,
}

impl Packed4BitCounters {
    fn new(counter_len: usize) -> Self {
        let byte_len = counter_len.div_ceil(2);
        Self {
            bytes: (0..byte_len)
                .map(|_| AtomicU8::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn load(&self, index: usize) -> u8 {
        let byte = self.bytes[index / 2].load(Ordering::Relaxed);
        if index & 1 == 0 {
            byte & 0x0F
        } else {
            byte >> 4
        }
    }

    fn saturating_increment(&self, index: usize) {
        let byte = &self.bytes[index / 2];
        let shift = if index & 1 == 0 { 0 } else { 4 };
        let mask = 0x0F_u8 << shift;
        byte.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            let current = (value & mask) >> shift;
            if current == 15 {
                None
            } else {
                Some((value & !mask) | ((current + 1) << shift))
            }
        })
        .ok();
    }

    fn age(&self) {
        for byte in self.bytes.iter() {
            byte.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                let low = (value & 0x0F) >> 1;
                let high = ((value >> 4) >> 1) << 4;
                Some(low | high)
            })
            .ok();
        }
    }
}

struct DualSketchCounters {
    a: Packed4BitCounters,
    b: Packed4BitCounters,
    active_is_a: AtomicBool,
}

impl DualSketchCounters {
    fn new(counter_len: usize) -> Self {
        Self {
            a: Packed4BitCounters::new(counter_len),
            b: Packed4BitCounters::new(counter_len),
            active_is_a: AtomicBool::new(true),
        }
    }

    fn load(&self, index: usize) -> u8 {
        let a_val = self.a.load(index);
        let b_val = self.b.load(index);
        a_val.max(b_val)
    }

    fn saturating_increment(&self, index: usize) {
        if self.active_is_a.load(Ordering::Relaxed) {
            self.a.saturating_increment(index);
        } else {
            self.b.saturating_increment(index);
        }
    }

    fn age(&self) {
        if self.active_is_a.load(Ordering::Relaxed) {
            self.b.age();
            self.active_is_a.store(false, Ordering::Relaxed);
        } else {
            self.a.age();
            self.active_is_a.store(true, Ordering::Relaxed);
        }
    }
}

struct DoorKeeper {
    mask: usize,
    bits: Box<[AtomicU8]>,
}

impl DoorKeeper {
    fn with_capacity(width: usize) -> Self {
        let bit_count = width.max(8).saturating_mul(8).next_power_of_two().max(64);
        let byte_count = bit_count / 8;
        Self {
            mask: bit_count - 1,
            bits: (0..byte_count)
                .map(|_| AtomicU8::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn check_and_set(&self, hash: u64) -> bool {
        let mut all_present = true;
        for row in 0..DEPTH {
            all_present &= self.set_bit(self.index(hash, row));
        }
        all_present
    }

    #[cfg(test)]
    fn contains(&self, hash: u64) -> bool {
        (0..DEPTH).all(|row| self.bit_is_set(self.index(hash, row)))
    }

    fn clear(&self) {
        for byte in self.bits.iter() {
            byte.store(0, Ordering::Relaxed);
        }
    }

    fn index(&self, hash: u64, row: usize) -> usize {
        (mix_tiny_lfu_hash(hash, row as u64) as usize) & self.mask
    }

    fn set_bit(&self, bit: usize) -> bool {
        let byte = &self.bits[bit / 8];
        let mask = 1_u8 << (bit & 7);
        byte.fetch_or(mask, Ordering::Relaxed) & mask != 0
    }

    #[cfg(test)]
    fn bit_is_set(&self, bit: usize) -> bool {
        let byte = self.bits[bit / 8].load(Ordering::Relaxed);
        let mask = 1_u8 << (bit & 7);
        byte & mask != 0
    }
}

#[inline]
fn saturating_increment(counter: &AtomicU8) {
    let mut current = counter.load(Ordering::Relaxed);
    while current < u8::MAX {
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
fn mix_tiny_lfu_hash(hash: u64, row: u64) -> u64 {
    let mut mixed = hash ^ row.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    mixed ^= mixed >> 33;
    mixed = mixed.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    mixed ^ (mixed >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_and_estimate_track_frequency() {
        let sketch = TinyLfu::with_capacity(64);
        let hash = 42;
        assert_eq!(sketch.estimate(hash), 0);
        assert_eq!(sketch.increment(hash), 1);
        assert_eq!(sketch.increment(hash), 2);
        assert!(sketch.estimate(hash) >= 2);
        assert_eq!(sketch.estimate(999), 0);
    }

    #[test]
    fn aging_reduces_old_frequency() {
        let sketch = TinyLfu::with_sample_size(4, 4);
        let hash = 7;
        for _ in 0..3 {
            sketch.increment(hash);
        }
        let before = sketch.estimate(hash);
        sketch.increment(100);
        let after = sketch.estimate(hash);
        assert!(after < before);
    }

    #[test]
    fn counters_saturate() {
        let sketch = TinyLfu::with_sample_size(4, usize::MAX);
        let hash = 123;
        for _ in 0..300 {
            sketch.increment(hash);
        }
        assert_eq!(sketch.estimate(hash), u8::MAX);
    }

    #[test]
    fn packed_4bit_counters_saturate_at_fifteen() {
        let sketch = TinyLfu::with_packed_4bit(4, 64, usize::MAX);
        let hash = 123;
        for _ in 0..300 {
            sketch.increment(hash);
        }
        assert_eq!(sketch.estimate(hash), 15);
    }

    #[test]
    fn packed_4bit_adjacent_counters_are_independent() {
        let counters = Packed4BitCounters::new(2);
        counters.saturating_increment(0);
        counters.saturating_increment(0);
        assert_eq!(counters.load(0), 2);
        assert_eq!(counters.load(1), 0);

        counters.saturating_increment(1);
        assert_eq!(counters.load(0), 2);
        assert_eq!(counters.load(1), 1);
    }

    #[test]
    fn packed_4bit_aging_halves_each_nibble() {
        let counters = Packed4BitCounters::new(2);
        for _ in 0..15 {
            counters.saturating_increment(0);
        }
        for _ in 0..8 {
            counters.saturating_increment(1);
        }

        counters.age();

        assert_eq!(counters.load(0), 7);
        assert_eq!(counters.load(1), 4);
    }

    #[test]
    fn doorkeeper_holds_first_touch_and_resets_on_age() {
        let sketch = TinyLfu::with_doorkeeper(4, 64, usize::MAX);
        let hash = 777;

        assert_eq!(sketch.increment(hash), 0);
        assert!(sketch.doorkeeper_contains_for_test(hash));
        assert_eq!(sketch.estimate(hash), 0);

        assert_eq!(sketch.increment(hash), 1);
        assert_eq!(sketch.estimate(hash), 1);

        sketch.age();
        assert!(!sketch.doorkeeper_contains_for_test(hash));
        assert_eq!(sketch.increment(hash), 0);
    }

    #[test]
    fn dual_sketch_swap_preserves_recent_frequency() {
        let counters = DualSketchCounters::new(2);
        counters.a.saturating_increment(0);
        counters.a.saturating_increment(0);
        counters.a.saturating_increment(0);

        assert_eq!(counters.load(0), 3);

        counters.age();

        assert_eq!(counters.load(0), 3);
    }

    #[test]
    fn dual_sketch_age_clears_standby() {
        let counters = DualSketchCounters::new(2);

        counters.saturating_increment(0);
        counters.saturating_increment(0);
        counters.saturating_increment(0);

        assert_eq!(counters.load(0), 3);

        counters.age();

        assert_eq!(counters.load(0), 3);

        counters.saturating_increment(0);
        assert_eq!(counters.load(0), 3);
    }

    #[test]
    fn dual_sketch_independent_counters() {
        let counters = DualSketchCounters::new(4);

        counters.saturating_increment(0);
        counters.saturating_increment(0);
        counters.saturating_increment(1);

        assert_eq!(counters.load(0), 2);
        assert_eq!(counters.load(1), 1);
        assert_eq!(counters.load(2), 0);

        counters.age();

        assert_eq!(counters.load(0), 2);
        assert_eq!(counters.load(1), 1);
        assert_eq!(counters.load(2), 0);
    }
}
