use mfs_core::atomic_writeback::AtomicWriteBehindCache;
use mfs_core::slot_writeback::SlotWriteBehindCache;
use mfs_core::writeback::{WriteBehindCache, WriteBehindConfig};
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const COUNT: u64 = 100_000;
const TRIALS: usize = 5;

type Blob = [u8; 128];

struct Stats {
    label: &'static str,
    count: u64,
    min: Duration,
    median: Duration,
    max: Duration,
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "{:<32} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op (peak ops/s={:.0})",
            self.label,
            self.count,
            TRIALS,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min),
        );
    }
}

fn measure<F>(label: &'static str, count: u64, mut body: F) -> Stats
where
    F: FnMut() -> u64,
{
    let mut samples = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let acc = body();
        let elapsed = start.elapsed();
        black_box(acc);
        samples.push(elapsed);
    }
    samples.sort();
    Stats {
        label,
        count,
        min: samples[0],
        median: samples[TRIALS / 2],
        max: samples[TRIALS - 1],
    }
}

fn blob(seed: u64) -> Blob {
    let mut out = [0u8; 128];
    for (i, b) in out.iter_mut().enumerate() {
        *b = seed.wrapping_add(i as u64) as u8;
    }
    out
}

fn config() -> WriteBehindConfig {
    WriteBehindConfig {
        initial_capacity: COUNT as usize,
        dirty_queue_capacity: COUNT as usize,
        ..WriteBehindConfig::default()
    }
}

fn populate_boxed() -> WriteBehindCache<u64, Blob> {
    let cache = WriteBehindCache::<u64, Blob>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(i, blob(i));
    }
    drop(p);
    cache
}

fn populate_slot() -> SlotWriteBehindCache<u64, Blob> {
    let cache = SlotWriteBehindCache::<u64, Blob>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(i, blob(i));
    }
    drop(p);
    cache
}

fn populate_atomic() -> AtomicWriteBehindCache<u64, Blob> {
    let cache = AtomicWriteBehindCache::<u64, Blob>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(i, blob(i));
    }
    drop(p);
    cache
}

fn arc_values() -> Vec<Arc<Blob>> {
    (0..COUNT)
        .map(|i| Arc::new(blob(i.wrapping_mul(17))))
        .collect()
}

fn main() {
    println!("=== slot write-behind vs boxed WriteBehindCache (COUNT={COUNT}, Blob=[u8;128]) ===");

    let boxed = populate_boxed();
    measure("boxed_writeback_read_with", COUNT, || {
        let mut checksum = 0u64;
        let p = boxed.pin();
        for i in 0..COUNT {
            checksum ^= p.read_with(&i, |v| v[0] as u64).expect("loaded key");
        }
        checksum
    })
    .print();
    measure("boxed_writeback_put", COUNT, || {
        let p = boxed.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(blob(i.wrapping_mul(11))));
        }
        boxed.len() as u64
    })
    .print();
    let boxed_arc = populate_boxed();
    let boxed_arcs = arc_values();
    measure("boxed_writeback_put_arc", COUNT, || {
        let p = boxed_arc.pin();
        for i in 0..COUNT {
            p.put_arc(black_box(i), Arc::clone(black_box(&boxed_arcs[i as usize])));
        }
        boxed_arc.len() as u64
    })
    .print();

    let slot = populate_slot();
    measure("slot_writeback_read_with", COUNT, || {
        let mut checksum = 0u64;
        let p = slot.pin();
        for i in 0..COUNT {
            checksum ^= p.read_with(&i, |v| v[0] as u64).expect("loaded key");
        }
        checksum
    })
    .print();
    measure("slot_writeback_put", COUNT, || {
        let p = slot.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(blob(i.wrapping_mul(11))));
        }
        slot.len() as u64
    })
    .print();
    measure("slot_writeback_put_dirty", COUNT, || {
        let p = slot.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(blob(i.wrapping_mul(13))));
        }
        slot.len() as u64
    })
    .print();
    let slot_arc = populate_slot();
    let arcs = arc_values();
    measure("slot_writeback_put_arc", COUNT, || {
        let p = slot_arc.pin();
        for i in 0..COUNT {
            p.put_arc(black_box(i), Arc::clone(black_box(&arcs[i as usize])));
        }
        slot_arc.len() as u64
    })
    .print();

    let atomic = populate_atomic();
    measure("atomic_writeback_read_with", COUNT, || {
        let mut checksum = 0u64;
        let p = atomic.pin();
        for i in 0..COUNT {
            checksum ^= p.read_with(&i, |v| v[0] as u64).expect("loaded key");
        }
        checksum
    })
    .print();
    measure("atomic_writeback_put", COUNT, || {
        let p = atomic.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(blob(i.wrapping_mul(11))));
        }
        atomic.len() as u64
    })
    .print();
}
