//! Isolation bench: figure out where the WriteBehindCache regression
//! comes from.

use mfs_core::FastBuildHasher;
use mfs_core::writeback::WriteBehindCache;
use papaya::HashMap as PapayaMap;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

#[allow(dead_code)]
struct Probe {
    value: Option<Arc<u64>>,
    version: u64,
    last_touch: AtomicU64,
}

fn measure<F>(label: &str, count: u64, mut body: F)
where
    F: FnMut() -> u64,
{
    let mut samples = Vec::new();
    for _ in 0..5 {
        let start = Instant::now();
        let v = body();
        let elapsed = start.elapsed();
        black_box(v);
        samples.push(elapsed);
    }
    samples.sort();
    let ns = samples[0].as_nanos() as f64 / count as f64;
    println!("{label:<58} min={ns:.2} ns/op");
}

fn main() {
    let count = 1_000_000u64;

    // Variant A: papaya<u64, u64> with FastBuildHasher, insert-populated.
    let m_a: PapayaMap<u64, u64, FastBuildHasher> = PapayaMap::builder()
        .capacity(count as usize)
        .hasher(FastBuildHasher::default())
        .build();
    {
        let p = m_a.pin();
        for i in 0..count {
            p.insert(i, i * 2);
        }
    }
    measure("A: papaya<u64,u64> Fx insert-populated", count, || {
        let mut sum = 0u64;
        let p = m_a.pin();
        for i in 0..count {
            sum ^= *p.get(black_box(&i)).expect("k");
        }
        sum
    });

    // Variant B: papaya<u64, u64> with FastBuildHasher, compute-populated.
    let m_b: PapayaMap<u64, u64, FastBuildHasher> = PapayaMap::builder()
        .capacity(count as usize)
        .hasher(FastBuildHasher::default())
        .build();
    for i in 0..count {
        m_b.pin().compute(i, |existing| {
            let v = existing.map(|(_, x)| *x + 1).unwrap_or(i * 2);
            papaya::Operation::Insert::<u64, ()>(v)
        });
    }
    measure("B: papaya<u64,u64> Fx compute-populated", count, || {
        let mut sum = 0u64;
        let p = m_b.pin();
        for i in 0..count {
            sum ^= *p.get(black_box(&i)).expect("k");
        }
        sum
    });

    // Variant C: papaya<u64, Probe> Fx compute-populated.
    let m_c: PapayaMap<u64, Probe, FastBuildHasher> = PapayaMap::builder()
        .capacity(count as usize)
        .hasher(FastBuildHasher::default())
        .build();
    for i in 0..count {
        m_c.pin().compute(i, |_existing| {
            papaya::Operation::Insert::<Probe, ()>(Probe {
                value: Some(Arc::new(i * 2)),
                version: 1,
                last_touch: AtomicU64::new(0),
            })
        });
    }
    measure("C: papaya<u64,Probe> Fx compute-populated", count, || {
        let mut sum = 0u64;
        let p = m_c.pin();
        for i in 0..count {
            let r = p.get(black_box(&i)).expect("k");
            sum ^= *r.value.as_ref().expect("v").as_ref();
        }
        sum
    });

    // Variant D: WriteBehindCache built with the OLD pre-feat-branch
    // default of 1024 (the historical capacity-trap demo). Kept as a
    // regression check.
    //
    // History: when the cache was backed by papaya (resizable), this
    // variant demonstrated a ~30x read regression on Skylake from
    // resize-induced heap fragmentation across 1M inserts.
    //
    // Current behaviour: the cache is now backed by `ConcurrentMap`,
    // which is **fixed-capacity**. Inserts past the load-factor
    // limit fail silently (`InsertOutcome::Full`), so loading 1M
    // entries into a cap-1024 cache leaves ~99% of the keys absent.
    // We iterate reads via `get(...).map(...)` (no `.expect`) so the
    // bench measures read-cost-against-an-undersized-table without
    // panicking. The cost is still dominated by the probe walk: a
    // saturated table means each get traverses the full probe chain
    // before failing — that's the regression this variant still
    // proves.
    let cache = WriteBehindCache::<u64, u64>::with_config(mfs_core::writeback::WriteBehindConfig {
        dirty_shards: 16,
        initial_capacity: 1024,
        dirty_queue_capacity: 16 * 1024,
    });
    for i in 0..count {
        let _ = cache.try_load_clean(i, i * 2);
    }
    measure("D: WriteBehindCache (under-sized to 1024)", count, || {
        let mut sum = 0u64;
        let p = cache.pin();
        for i in 0..count {
            if let Some(v) = p.get(black_box(&i)) {
                sum ^= *v;
            }
        }
        sum
    });

    // Variant E: WriteBehindCache pre-allocated to count.
    let cache2 =
        WriteBehindCache::<u64, u64>::with_config(mfs_core::writeback::WriteBehindConfig {
            dirty_shards: 16,
            initial_capacity: count as usize,
            dirty_queue_capacity: 16 * 1024,
        });
    for i in 0..count {
        cache2.load_clean(i, i * 2);
    }
    measure("E: WriteBehindCache (pre-sized cap)", count, || {
        let mut sum = 0u64;
        let p = cache2.pin();
        for i in 0..count {
            sum ^= *p.get(black_box(&i)).expect("k");
        }
        sum
    });

    // Variant F: WriteBehindCache pre-sized, read_with (no Arc::clone).
    measure("F: WriteBehindCache pre-sized read_with", count, || {
        let mut sum = 0u64;
        let p = cache2.pin();
        for i in 0..count {
            sum ^= p.read_with(black_box(&i), |v| *v).expect("k");
        }
        sum
    });
}
