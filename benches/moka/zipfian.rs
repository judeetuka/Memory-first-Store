use hdrhistogram::Histogram;
use plotters::prelude::*;
use std::fs::File;
use std::time::Instant;

use base64::engine::general_purpose::STANDARD;
use base64::write::EncoderWriter;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};

const WORKING_SET: u64 = 100_000;
const CACHE_CAP: u64 = 10_000;
const OPS: usize = 1_000_000;
const ZIPF_ALPHA: f64 = 1.1;

fn main() {
    std::fs::create_dir_all("benches/moka/charts").ok();
    let keys = zipfian_sequence(OPS, WORKING_SET, ZIPF_ALPHA);

    let mut series: Vec<(String, Histogram<u32>, f64)> = Vec::new();

    let mk: moka::sync::Cache<u64, u64> =
        moka::sync::Cache::builder().max_capacity(CACHE_CAP).build();
    let (hits, hist) = measure(&keys, |k| {
        if let Some(v) = mk.get(&k) {
            Some(v)
        } else {
            mk.insert(k, k);
            None
        }
    });
    let hr = 100.0 * hits as f64 / OPS as f64;
    write_hist("benches/moka/moka_sync.hist", &hist);
    series.push(("moka_sync".into(), hist, hr));

    let lf = mfs_core::lockfree::LockFreeCache::<u64, u64>::with_capacity(WORKING_SET as usize);
    let (hits, hist) = measure(&keys, |k| {
        let p = lf.pin();
        if let Some(v) = p.get(&k).copied() {
            Some(v)
        } else {
            p.insert(k, k);
            None
        }
    });
    let hr = 100.0 * hits as f64 / OPS as f64;
    write_hist("benches/moka/mfs_lockfree_shadow.hist", &hist);
    series.push(("mfs_lockfree_shadow".into(), hist, hr));

    let s3 = mfs_core::s3fifo::S3FifoCache::<u64, u64>::with_capacity(CACHE_CAP as usize);
    let (hits, hist) = measure(&keys, |k| {
        if let Some(v) = s3.get(&k).map(|v| *v) {
            Some(v)
        } else {
            s3.insert(k, k);
            None
        }
    });
    let hr = 100.0 * hits as f64 / OPS as f64;
    write_hist("benches/moka/mfs_s3fifo.hist", &hist);
    series.push(("mfs_s3fifo".into(), hist, hr));

    println!(
        "moka zipfian (alpha={ZIPF_ALPHA}, working_set={WORKING_SET}, cap={CACHE_CAP}, ops={OPS})"
    );
    for (name, hist, hr) in &series {
        println!(
            "{name:<24} hit_ratio={hr:>6.3}% p50={:>5}ns p99={:>6}ns p999={:>7}ns max={:>7}ns",
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.max(),
        );
    }

    let histograms: Vec<(String, Histogram<u32>)> =
        series.into_iter().map(|(n, h, _)| (n, h)).collect();
    render(
        &histograms,
        "benches/moka/charts/zipfian-cdf.png",
        "moka zipfian CDF",
    )
    .ok();
    println!("Wrote chart to benches/moka/charts/zipfian-cdf.png");
}

fn measure(keys: &[u64], mut op: impl FnMut(u64) -> Option<u64>) -> (usize, Histogram<u32>) {
    let mut hist = Histogram::<u32>::new(3).unwrap();
    let mut hits = 0usize;
    for &k in keys {
        let t = Instant::now();
        if op(k).is_some() {
            hits += 1;
        }
        hist.record(t.elapsed().as_nanos() as u64).ok();
    }
    (hits, hist)
}

fn zipfian_sequence(n: usize, support: u64, alpha: f64) -> Vec<u64> {
    let support_usize = support as usize;
    let mut cum = Vec::with_capacity(support_usize);
    let mut acc = 0.0f64;
    for i in 1..=support {
        acc += 1.0 / (i as f64).powf(alpha);
        cum.push(acc);
    }
    let harmonic = acc;
    let mut rng = 0xdeadbeefu64;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let u = (rng as f64 / u64::MAX as f64).clamp(1e-12, 1.0 - 1e-12);
        let target = u * harmonic;
        let idx = cum.partition_point(|&w| w < target).min(support_usize - 1);
        out.push(idx as u64);
    }
    out
}

fn write_hist(path: &str, hist: &Histogram<u32>) {
    if let Ok(mut f) = File::create(path) {
        let mut s = V2DeflateSerializer::new();
        let _ = s.serialize(hist, &mut EncoderWriter::new(&mut f, &STANDARD));
    }
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
    let palette = [&RED, &BLUE, &GREEN, &MAGENTA];
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
