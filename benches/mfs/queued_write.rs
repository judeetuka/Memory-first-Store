use mfs_neural::dense_writeback_map::DenseWriteBehindMap;
use mfs_neural::queued_dense_writeback::QueuedDenseWriteBehindMap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const COUNT: u64 = 100_000;
const TRIALS: usize = 5;

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
            "{:<30} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op (peak ops/s={:.0})",
            self.label,
            self.count,
            TRIALS,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min)
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

fn main() {
    println!("=== queued dense write-behind (COUNT={COUNT}, trials={TRIALS}) ===");

    let eager = DenseWriteBehindMap::<u64, u64>::with_capacity(COUNT as usize);
    measure("eager_dense_put", COUNT, || {
        let p = eager.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(i.wrapping_mul(3)));
        }
        eager.len() as u64
    })
    .print();

    measure("eager_dense_replace", COUNT, || {
        let p = eager.pin();
        for i in 0..COUNT {
            p.put(black_box(i), black_box(i.wrapping_mul(7)));
        }
        eager.len() as u64
    })
    .print();

    let queued = QueuedDenseWriteBehindMap::<u64, u64>::with_capacity(COUNT as usize);
    measure("queued_put_enqueue", COUNT, || {
        let mut last = None;
        for i in 0..COUNT {
            last = Some(
                queued
                    .put_async(black_box(i), black_box(i.wrapping_mul(3)))
                    .unwrap(),
            );
        }
        last.unwrap().wait_applied();
        queued.len() as u64
    })
    .print();

    measure("queued_replace_enqueue", COUNT, || {
        let mut last = None;
        for i in 0..COUNT {
            last = Some(
                queued
                    .put_async(black_box(i), black_box(i.wrapping_mul(7)))
                    .unwrap(),
            );
        }
        last.unwrap().wait_applied();
        queued.len() as u64
    })
    .print();

    measure("queued_barrier_all", COUNT, || {
        for i in 0..COUNT {
            queued
                .put_async(black_box(i), black_box(i.wrapping_mul(11)))
                .unwrap();
        }
        queued.barrier_all().unwrap();
        queued.len() as u64
    })
    .print();

    queued.shutdown().unwrap();
}
