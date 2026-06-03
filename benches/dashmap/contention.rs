use crossbeam_utils::CachePadded;
use hdrhistogram::{Histogram, SyncHistogram};
use plotters::prelude::*;
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::Instant;

const THREADS: usize = 8;
const ITERS: usize = 100_000;
const SERIAL_LOCKS: usize = 64;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    keys: u64,
    read_pct: u32,
}

impl Scenario {
    const fn new(name: &'static str, keys: u64, read_pct: u32) -> Self {
        Self {
            name,
            keys,
            read_pct,
        }
    }
}

fn main() {
    std::fs::create_dir_all("benches/dashmap/charts").ok();
    let scenarios = [
        Scenario::new("single_key_read", 1, 100),
        Scenario::new("single_key_write", 1, 0),
        Scenario::new("single_key_mixed", 1, 50),
        Scenario::new("hot64_read", 64, 100),
        Scenario::new("hot64_write", 64, 0),
        Scenario::new("hot64_mixed", 64, 50),
        Scenario::new("warm1024_mixed", 1024, 50),
    ];

    for scenario in scenarios {
        let series = run_scenario(scenario);
        print_scenario(scenario, &series);
        render(
            &series,
            &format!("benches/dashmap/charts/{}.png", scenario.name),
            &format!("dashmap contention CDF ({})", scenario.name),
        )
        .ok();
        if scenario.name == "hot64_mixed" {
            render(
                &series,
                "benches/dashmap/charts/contention-cdf.png",
                "dashmap contention CDF",
            )
            .ok();
        }
    }
    println!("Wrote charts to benches/dashmap/charts/");
}

fn run_scenario(scenario: Scenario) -> Vec<(String, Histogram<u32>)> {
    let mut series = Vec::new();

    let dm: dashmap::DashMap<u64, u64> = dashmap::DashMap::with_capacity(scenario.keys as usize);
    for i in 0..scenario.keys {
        dm.insert(i, i);
    }
    series.push(("dashmap".into(), run_dashmap(&dm, scenario)));

    let lf = mfs_core::lockfree::LockFreeCache::<u64, u64>::with_capacity(scenario.keys as usize);
    {
        let p = lf.pin();
        for i in 0..scenario.keys {
            p.insert(i, i);
        }
    }
    series.push((
        "mfs_lockfree_oneshot_pin".into(),
        run_mfs_lockfree_oneshot(&lf, scenario),
    ));
    series.push((
        "mfs_lockfree_thread_pin".into(),
        run_mfs_lockfree_thread_pin(&lf, scenario),
    ));
    series.push((
        "mfs_lockfree_serial_writes".into(),
        run_mfs_lockfree_serial_writes(&lf, scenario),
    ));

    let inline = mfs_core::inline_map::InlineU64Map::with_capacity(scenario.keys as usize);
    for i in 0..scenario.keys {
        inline.insert(i, i);
    }
    series.push((
        "mfs_inline_u64".into(),
        run_mfs_inline_u64(&inline, scenario),
    ));

    let plf = mfs_core::partitioned_lockfree::PartitionedLockFreeCache::<u64, u64>::with_capacity_and_partitions(
        scenario.keys as usize,
        THREADS,
    );
    {
        let p = plf.pin();
        for i in 0..scenario.keys {
            p.insert(i, i);
        }
    }
    series.push((
        "mfs_partitioned_thread_pin".into(),
        run_mfs_partitioned_thread_pin(&plf, scenario),
    ));

    series
}

fn print_scenario(scenario: Scenario, series: &[(String, Histogram<u32>)]) {
    println!(
        "dashmap contention scenario={} ({} threads, {} hot keys, {}% reads, {} ops/thread)",
        scenario.name, THREADS, scenario.keys, scenario.read_pct, ITERS
    );
    for (name, h) in series {
        println!(
            "{name:<30} p50={:>5}ns p99={:>6}ns p999={:>7}ns max={:>8}ns",
            h.value_at_quantile(0.50),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.max(),
        );
    }
}

fn run_dashmap(cache: &dashmap::DashMap<u64, u64>, scenario: Scenario) -> Histogram<u32> {
    run_threads(scenario, |k, v| {
        if let Some(v) = v {
            cache.insert(k, v);
        } else {
            cache.get(&k).map(|value| *value);
        }
    })
}

fn run_mfs_lockfree_oneshot(
    cache: &mfs_core::lockfree::LockFreeCache<u64, u64>,
    scenario: Scenario,
) -> Histogram<u32> {
    run_threads(scenario, |k, v| {
        let p = cache.pin();
        if let Some(v) = v {
            p.insert(k, v);
        } else {
            let _ = p.get(&k).copied();
        }
    })
}

fn run_mfs_lockfree_thread_pin(
    cache: &mfs_core::lockfree::LockFreeCache<u64, u64>,
    scenario: Scenario,
) -> Histogram<u32> {
    let barrier = Barrier::new(THREADS);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..THREADS {
            let (barrier, cache) = (&barrier, cache);
            let mut recorder = hist.recorder();
            scope.spawn(move || {
                let pinned = cache.pin();
                barrier.wait();
                let mut rng = seed_rng(thread_idx);
                for op_idx in 0..ITERS {
                    let (key, is_read) = next_op(&mut rng, scenario);
                    let start = Instant::now();
                    if is_read {
                        let _ = pinned.get(&key).copied();
                    } else {
                        pinned.insert(key, op_idx as u64);
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_mfs_lockfree_serial_writes(
    cache: &mfs_core::lockfree::LockFreeCache<u64, u64>,
    scenario: Scenario,
) -> Histogram<u32> {
    let locks = (0..SERIAL_LOCKS)
        .map(|_| CachePadded::new(Mutex::new(())))
        .collect::<Vec<_>>();
    let barrier = Barrier::new(THREADS);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..THREADS {
            let (barrier, cache, locks) = (&barrier, cache, &locks);
            let mut recorder = hist.recorder();
            scope.spawn(move || {
                let pinned = cache.pin();
                barrier.wait();
                let mut rng = seed_rng(thread_idx);
                for op_idx in 0..ITERS {
                    let (key, is_read) = next_op(&mut rng, scenario);
                    let start = Instant::now();
                    if is_read {
                        let _ = pinned.get(&key).copied();
                    } else {
                        let _guard = locks[serial_lock_idx(key)].lock().expect("serial lock");
                        pinned.insert(key, op_idx as u64);
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_mfs_inline_u64(
    cache: &mfs_core::inline_map::InlineU64Map,
    scenario: Scenario,
) -> Histogram<u32> {
    run_threads(scenario, |k, v| {
        if let Some(v) = v {
            cache.insert(k, v);
        } else {
            let _ = cache.get(k);
        }
    })
}

fn run_mfs_partitioned_thread_pin(
    cache: &mfs_core::partitioned_lockfree::PartitionedLockFreeCache<u64, u64>,
    scenario: Scenario,
) -> Histogram<u32> {
    let barrier = Barrier::new(THREADS);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..THREADS {
            let (barrier, cache) = (&barrier, cache);
            let mut recorder = hist.recorder();
            scope.spawn(move || {
                let pinned = cache.pin();
                barrier.wait();
                let mut rng = seed_rng(thread_idx);
                for op_idx in 0..ITERS {
                    let (key, is_read) = next_op(&mut rng, scenario);
                    let start = Instant::now();
                    if is_read {
                        let _ = pinned.get(&key).copied();
                    } else {
                        pinned.insert(key, op_idx as u64);
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_threads<F>(scenario: Scenario, op: F) -> Histogram<u32>
where
    F: Fn(u64, Option<u64>) + Send + Sync + Copy,
{
    let barrier = Barrier::new(THREADS);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..THREADS {
            let barrier = &barrier;
            let mut recorder = hist.recorder();
            scope.spawn(move || {
                barrier.wait();
                let mut rng = seed_rng(thread_idx);
                for op_idx in 0..ITERS {
                    let (key, is_read) = next_op(&mut rng, scenario);
                    let start = Instant::now();
                    if is_read {
                        op(key, None);
                    } else {
                        op(key, Some(op_idx as u64));
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

#[inline]
fn seed_rng(thread_idx: usize) -> u64 {
    (thread_idx as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xdeadbeef
}

#[inline]
fn next_op(rng: &mut u64, scenario: Scenario) -> (u64, bool) {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    let key = (*rng >> 8) % scenario.keys;
    let is_read = (*rng as u32 % 100) < scenario.read_pct;
    (key, is_read)
}

#[inline]
fn serial_lock_idx(key: u64) -> usize {
    (key as usize) & (SERIAL_LOCKS - 1)
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
    let palette = [&RED, &BLUE, &GREEN, &MAGENTA, &CYAN, &YELLOW];
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
