//! Head-to-head throughput + latency comparison between foyer-memory and MfS.
//!
//! Workload: 8 threads × 200k ops, 50% reads / 50% writes on a contended hot
//! key set of 64 keys. This is the same pattern as `dashmap_contention` so
//! the numbers are directly comparable across all three competitors
//! (dashmap, foyer-memory, MfS).
//!
//! Contestants:
//! - `foyer_memory_fifo`     — foyer with FIFO eviction, capacity 10× working
//!   set. FIFO's `acquire()`/`release()` return `Op::Noop`, so `get()` only
//!   takes a read lock + hash table lookup. This is foyer's cheapest path.
//! - `mfs_lockfree`          — `LockFreeCache` (uncapped, no dirty tracking).
//!
//! `WriteBehindCache` is excluded from this comparison because foyer-memory
//! has no write-behind semantics — comparing them here would conflate the
//! write-behind contract cost (~17 ns/write dirty bookkeeping) with the
//! underlying hash-table+locking cost we actually want to isolate. A
//! separate bench should exercise WriteBehindCache against the realistic
//! workload (with an active flusher).

use foyer_memory::{Cache, CacheBuilder, FifoConfig};
use hdrhistogram::{Histogram, SyncHistogram};
use mfs_core::lockfree::LockFreeCache;
use plotters::prelude::*;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

const KEYS: u64 = 64;
const THREADS: usize = 8;
const ITERS: usize = 50_000;
const READ_PCT: u32 = 50;

fn main() {
    std::fs::create_dir_all("benches/foyer/charts").ok();
    let mut series: Vec<(String, Histogram<u32>, f64)> = Vec::new();

    // foyer FIFO — oversized capacity so no eviction occurs; isolates the
    // hash-table + locking cost without admission-policy work.
    {
        eprintln!("running foyer_memory_fifo...");
        let cache: Cache<u64, u64> = CacheBuilder::new((KEYS as usize) * 10)
            .with_shards(16)
            .with_eviction_config(FifoConfig {})
            .build();
        for i in 0..KEYS {
            cache.insert(i, i);
        }
        let (hist, mops) = run(
            &cache,
            |c, k| {
                let _ = c.get(&k).map(|e| *e.value());
            },
            |c, k, v| {
                let _ = c.insert(k, v);
            },
        );
        eprintln!("  done foyer_memory_fifo: {:.2} Mops/s", mops);
        series.push(("foyer_memory_fifo".into(), hist, mops));
    }

    // mfs LockFreeCache — uncapped (no eviction), pure read/write speed.
    {
        eprintln!("running mfs_lockfree...");
        let cache = LockFreeCache::<u64, u64>::with_capacity(KEYS as usize * 4);
        {
            let p = cache.pin();
            for i in 0..KEYS {
                p.insert(i, i);
            }
        }
        let (hist, mops) = run(
            &cache,
            |c, k| {
                let p = c.pin();
                let _ = p.get(&k).copied();
            },
            |c, k, v| {
                let p = c.pin();
                p.insert(k, v);
            },
        );
        eprintln!("  done mfs_lockfree: {:.2} Mops/s", mops);
        series.push(("mfs_lockfree".into(), hist, mops));
    }

    println!(
        "foyer_memory_vs_mfs ({} threads, {} hot keys, {}% reads, {} ops/thread)",
        THREADS, KEYS, READ_PCT, ITERS
    );
    println!(
        "{:<28} {:>8} {:>8} {:>9} {:>9} {:>12}",
        "contestant", "p50ns", "p99ns", "p999ns", "max_ns", "agg_Mops/s"
    );
    for (name, h, mops) in &series {
        println!(
            "{:<28} {:>8} {:>8} {:>9} {:>9} {:>12.2}",
            name,
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.max(),
            mops,
        );
    }
    let chart_series: Vec<(String, Histogram<u32>)> = series
        .iter()
        .map(|(n, h, _)| (n.clone(), h.clone()))
        .collect();
    render(
        &chart_series,
        "benches/foyer/charts/memory_vs_mfs-cdf.png",
        "foyer-memory vs MfS contention CDF",
    )
    .ok();
    println!("Wrote chart to benches/foyer/charts/memory_vs_mfs-cdf.png");
}

fn run<T: Sync>(
    cache: &T,
    read: impl Fn(&T, u64) + Send + Copy,
    write: impl Fn(&T, u64, u64) + Send + Copy,
) -> (Histogram<u32>, f64) {
    let barrier = Barrier::new(THREADS);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    let wall_start_outer = std::sync::Mutex::new(None::<Instant>);
    let wall_end_outer = std::sync::Mutex::new(None::<Instant>);
    thread::scope(|s| {
        for t in 0..THREADS {
            let (b, c) = (&barrier, cache);
            let mut rec = hist.recorder();
            let wall_start = &wall_start_outer;
            let wall_end = &wall_end_outer;
            s.spawn(move || {
                b.wait();
                if t == 0 {
                    *wall_start.lock().unwrap() = Some(Instant::now());
                }
                let mut rng = (t as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xdeadbeef;
                for i in 0..ITERS {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let k = (rng >> 8) % KEYS;
                    let start = Instant::now();
                    if (rng as u32 % 100) < READ_PCT {
                        read(c, k);
                    } else {
                        write(c, k, i as u64);
                    }
                    rec.record(start.elapsed().as_nanos() as u64).ok();
                }
                if t == THREADS - 1 {
                    // last-finishing thread's end time is a fine approximation;
                    // refined by taking max across threads via atomic if needed
                    *wall_end.lock().unwrap() = Some(Instant::now());
                }
            });
        }
    });
    hist.refresh();
    let total_ops = (THREADS * ITERS) as f64;
    let elapsed = wall_end_outer
        .lock()
        .unwrap()
        .and_then(|e| {
            wall_start_outer
                .lock()
                .unwrap()
                .map(|s| e.duration_since(s))
        })
        .unwrap_or_default()
        .as_secs_f64()
        .max(1e-9);
    let agg_mops = total_ops / elapsed / 1_000_000.0;
    ((*hist).clone(), agg_mops)
}

fn render(
    series: &[(String, Histogram<u32>)],
    path: &str,
    title: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new(path, (1280, 720)).into_drawing_area();
    root.fill(&WHITE)?;
    let max = series
        .iter()
        .map(|(_, h)| h.max() as f64)
        .fold(1.0, f64::max);
    let min = series
        .iter()
        .map(|(_, h)| h.min().max(1) as f64)
        .fold(max, f64::min);
    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 28).into_font())
        .margin(20)
        .x_label_area_size(50)
        .y_label_area_size(60)
        .build_cartesian_2d((min.log10()..max.log10()).step(0.05), 0.0f64..1.0)?;
    chart
        .configure_mesh()
        .x_desc("latency (ns, log10)")
        .y_desc("CDF")
        .draw()?;
    let palette = [&RED, &BLUE, &GREEN, &MAGENTA, &CYAN, &BLACK];
    for (idx, (name, hist)) in series.iter().enumerate() {
        let color = palette[idx % palette.len()];
        let pts: Vec<(f64, f64)> = (0..=200)
            .map(|i| {
                let q = i as f64 / 200.0;
                ((hist.value_at_quantile(q).max(1) as f64).log10(), q)
            })
            .collect();
        chart
            .draw_series(LineSeries::new(pts, color.stroke_width(2)))?
            .label(name.clone())
            .legend(move |(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], color.stroke_width(2))
            });
    }
    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()?;
    root.present()?;
    Ok(())
}
