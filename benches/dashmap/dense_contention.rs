use hdrhistogram::{Histogram, SyncHistogram};
use plotters::prelude::*;
use std::env;
use std::sync::Barrier;
use std::thread;
use std::time::Instant;

const THREAD_COUNTS: [usize; 4] = [1, 2, 4, 8];
const ITERS: usize = 100_000;

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
        Scenario::new("single_key_write", 1, 0),
        Scenario::new("single_key_mixed", 1, 50),
        Scenario::new("hot64_write", 64, 0),
        Scenario::new("hot64_mixed", 64, 50),
        Scenario::new("warm1024_mixed", 1024, 50),
    ];
    let scenarios = selected_scenarios(&scenarios);
    let thread_counts = selected_thread_counts();
    let repeats = env_usize("MFS_DENSE_CONTENTION_REPEATS", 1).max(1);

    for threads in thread_counts {
        for &scenario in &scenarios {
            let mut repeated = Vec::with_capacity(repeats);
            for _ in 0..repeats {
                repeated.push(run_scenario(scenario, threads));
            }
            if repeats == 1 {
                print_scenario(scenario, threads, &repeated[0]);
            } else {
                print_repeat_summary(scenario, threads, &repeated);
            }
            let series = repeated.last().expect("at least one repeat");
            render(
                series,
                &format!(
                    "benches/dashmap/charts/dense-{}t-{}.png",
                    threads, scenario.name
                ),
                &format!(
                    "dense hot-write CDF ({} threads, {})",
                    threads, scenario.name
                ),
            )
            .ok();
        }
    }
    println!("Wrote charts to benches/dashmap/charts/dense-*t-*.png");
}

fn run_scenario(scenario: Scenario, threads: usize) -> Vec<(String, Histogram<u32>)> {
    let mut series = Vec::new();

    let dm: dashmap::DashMap<u64, u64> = dashmap::DashMap::with_capacity(scenario.keys as usize);
    for i in 0..scenario.keys {
        dm.insert(i, i);
    }
    series.push(("dashmap".into(), run_dashmap(&dm, scenario, threads)));

    let inline = mfs_core::inline_map::InlineU64Map::with_capacity(scenario.keys as usize);
    for i in 0..scenario.keys {
        inline.insert(i, i);
    }
    series.push((
        "mfs_inline_u64".into(),
        run_inline(&inline, scenario, threads),
    ));

    let dense_kv =
        mfs_neural::dense_kv::DenseKvMap::<u64, [u8; 8]>::with_capacity(scenario.keys as u32);
    {
        let pinned = dense_kv.pin();
        for i in 0..scenario.keys {
            pinned.put(i, i.to_le_bytes()).expect("preload dense kv");
        }
    }
    series.push((
        "mfs_dense_kv".into(),
        run_dense_kv(&dense_kv, scenario, threads),
    ));

    let dense_u64 =
        mfs_neural::dense_writeback::DenseWriteBehindU64::with_capacity(scenario.keys as usize);
    {
        let pinned = dense_u64.pin();
        for i in 0..scenario.keys {
            pinned.load_clean(i, i);
        }
    }
    series.push((
        "mfs_dense_writeback_u64".into(),
        run_dense_u64(&dense_u64, scenario, threads),
    ));

    let concurrent_dense = mfs_neural::concurrent_dense_writeback_map::ConcurrentDenseWriteBehindMap::<u64, [u8; 8]>::with_capacity(
        scenario.keys as usize,
    );
    {
        let pinned = concurrent_dense.pin();
        for i in 0..scenario.keys {
            pinned.load_clean(i, i.to_le_bytes());
        }
    }
    series.push((
        "mfs_concurrent_dense_wb".into(),
        run_concurrent_dense_map(&concurrent_dense, scenario, threads),
    ));

    let dense = mfs_neural::dense_writeback_map::DenseWriteBehindMap::<u64, [u8; 8]>::with_capacity(
        scenario.keys as usize,
    );
    {
        let pinned = dense.pin();
        for i in 0..scenario.keys {
            pinned.load_clean(i, i.to_le_bytes());
        }
    }
    series.push((
        "mfs_dense_writeback_map".into(),
        run_dense_map(&dense, scenario, threads),
    ));

    series
}

fn selected_scenarios(defaults: &[Scenario]) -> Vec<Scenario> {
    let Some(filter) = env::var("MFS_DENSE_CONTENTION_SCENARIOS").ok() else {
        return defaults.to_vec();
    };
    let requested = filter
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    let selected = defaults
        .iter()
        .copied()
        .filter(|scenario| requested.contains(&scenario.name))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        defaults.to_vec()
    } else {
        selected
    }
}

fn selected_thread_counts() -> Vec<usize> {
    let Some(raw) = env::var("MFS_DENSE_CONTENTION_THREADS").ok() else {
        return THREAD_COUNTS.to_vec();
    };
    let selected = raw
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|&value| value > 0)
        .collect::<Vec<_>>();
    if selected.is_empty() {
        THREAD_COUNTS.to_vec()
    } else {
        selected
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn print_scenario(scenario: Scenario, threads: usize, series: &[(String, Histogram<u32>)]) {
    println!(
        "dense contention scenario={} ({} threads, {} hot keys, {}% reads, {} ops/thread)",
        scenario.name, threads, scenario.keys, scenario.read_pct, ITERS
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

fn print_repeat_summary(
    scenario: Scenario,
    threads: usize,
    repeated: &[Vec<(String, Histogram<u32>)>],
) {
    println!(
        "dense contention diagnostic scenario={} ({} threads, {} hot keys, {}% reads, {} ops/thread, {} repeats)",
        scenario.name,
        threads,
        scenario.keys,
        scenario.read_pct,
        ITERS,
        repeated.len(),
    );
    let contestants = repeated[0].len();
    for idx in 0..contestants {
        let name = &repeated[0][idx].0;
        let p99 = repeated
            .iter()
            .map(|series| series[idx].1.value_at_quantile(0.99))
            .collect::<Vec<_>>();
        let p999 = repeated
            .iter()
            .map(|series| series[idx].1.value_at_quantile(0.999))
            .collect::<Vec<_>>();
        let max = repeated
            .iter()
            .map(|series| series[idx].1.max())
            .collect::<Vec<_>>();
        let p99 = summarize(&p99);
        let p999 = summarize(&p999);
        let max = summarize(&max);
        println!(
            "{name:<30} p99(min/med/max/cv)={:>5}/{:>5}/{:>5}ns/{:>5.1}% p999={:>6}/{:>6}/{:>6}ns/{:>5.1}% max_med={:>8}ns",
            p99.min,
            p99.median,
            p99.max,
            p99.cv_percent,
            p999.min,
            p999.median,
            p999.max,
            p999.cv_percent,
            max.median,
        );
    }
}

struct Summary {
    min: u64,
    median: u64,
    max: u64,
    cv_percent: f64,
}

fn summarize(values: &[u64]) -> Summary {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mean = sorted.iter().map(|&value| value as f64).sum::<f64>() / sorted.len() as f64;
    let variance = sorted
        .iter()
        .map(|&value| {
            let diff = value as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / sorted.len() as f64;
    let cv_percent = if mean == 0.0 {
        0.0
    } else {
        100.0 * variance.sqrt() / mean
    };
    Summary {
        min: sorted[0],
        median: sorted[sorted.len() / 2],
        max: *sorted.last().expect("non-empty values"),
        cv_percent,
    }
}

fn run_dashmap(
    cache: &dashmap::DashMap<u64, u64>,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    run_threads(scenario, threads, |key, value| {
        if let Some(value) = value {
            cache.insert(key, value);
        } else {
            let _ = cache.get(&key).map(|value| *value);
        }
    })
}

fn run_inline(
    cache: &mfs_core::inline_map::InlineU64Map,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    run_threads(scenario, threads, |key, value| {
        if let Some(value) = value {
            cache.insert(key, value);
        } else {
            let _ = cache.get(key);
        }
    })
}

fn run_dense_kv(
    cache: &mfs_neural::dense_kv::DenseKvMap<u64, [u8; 8]>,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    let barrier = Barrier::new(threads);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..threads {
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
                        let _ = pinned.get(&key);
                    } else {
                        pinned.put(key, (op_idx as u64).to_le_bytes()).ok();
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_dense_u64(
    cache: &mfs_neural::dense_writeback::DenseWriteBehindU64,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    let barrier = Barrier::new(threads);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..threads {
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
                        let _ = pinned.get(&key);
                    } else {
                        pinned.put(key, op_idx as u64);
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_concurrent_dense_map(
    cache: &mfs_neural::concurrent_dense_writeback_map::ConcurrentDenseWriteBehindMap<u64, [u8; 8]>,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    let barrier = Barrier::new(threads);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..threads {
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
                        let _ = pinned.get(&key);
                    } else {
                        pinned.put(key, (op_idx as u64).to_le_bytes());
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_dense_map(
    cache: &mfs_neural::dense_writeback_map::DenseWriteBehindMap<u64, [u8; 8]>,
    scenario: Scenario,
    threads: usize,
) -> Histogram<u32> {
    let barrier = Barrier::new(threads);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..threads {
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
                        let _ = pinned.get(&key);
                    } else {
                        pinned.put(key, (op_idx as u64).to_le_bytes());
                    }
                    recorder.record(start.elapsed().as_nanos() as u64).ok();
                }
            });
        }
    });
    hist.refresh();
    (*hist).clone()
}

fn run_threads<F>(scenario: Scenario, threads: usize, op: F) -> Histogram<u32>
where
    F: Fn(u64, Option<u64>) + Send + Sync + Copy,
{
    let barrier = Barrier::new(threads);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(3).unwrap());
    thread::scope(|scope| {
        for thread_idx in 0..threads {
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
