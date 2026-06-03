use std::fs::File;
use std::sync::Barrier;
use std::thread;

use base64::engine::general_purpose::STANDARD;
use base64::write::EncoderWriter;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use hdrhistogram::{Histogram, SyncHistogram};
use plotters::prelude::*;

fn main() {
    std::fs::create_dir_all("benches/papaya/charts").ok();
    println!("=== papaya (incremental) ===");
    p99_insert(papaya::HashMap::new(), |map, i| {
        map.pin().insert(i, ());
    });
    p99_concurrent_insert("papaya", papaya::HashMap::new(), |map, i| {
        map.pin().insert(i, ());
    });

    println!("=== papaya (blocking) ===");
    let map = papaya::HashMap::builder()
        .resize_mode(papaya::ResizeMode::Blocking)
        .build();

    p99_insert(map.clone(), |map, i| {
        map.pin().insert(i, ());
    });
    p99_concurrent_insert("papaya-blocking", map, |map, i| {
        map.pin().insert(i, ());
    });

    println!("=== dashmap ===");
    p99_insert(dashmap::DashMap::new(), |map, i| {
        map.insert(i, ());
    });
    p99_concurrent_insert("dashmap", dashmap::DashMap::new(), |map, i| {
        map.insert(i, ());
    });
}

fn p99_insert<T>(map: T, insert: impl Fn(&T, usize)) {
    const ITEMS: usize = 10_000_000;

    let mut max = None;

    for i in 0..ITEMS {
        let now = std::time::Instant::now();
        insert(&map, i);
        let elapsed = now.elapsed();

        if max.map(|max| elapsed > max).unwrap_or(true) {
            max = Some(elapsed);
        }
    }

    println!("p99 insert: {}ms", max.unwrap().as_millis());
}

fn p99_concurrent_insert<T: Sync>(name: &str, map: T, insert: impl Fn(&T, usize) + Send + Copy) {
    const ITEMS: usize = 1_000_000;

    let barrier = Barrier::new(8);
    let mut hist = SyncHistogram::<u32>::from(Histogram::new(1).unwrap());

    thread::scope(|s| {
        for t in 0..8 {
            let (barrier, map) = (&barrier, &map);
            let mut hist = hist.recorder();
            s.spawn(move || {
                barrier.wait();

                let mut max = None;
                for i in 0..ITEMS {
                    let i = (t + 1) * i;

                    let now = std::time::Instant::now();
                    insert(map, i);
                    let elapsed = now.elapsed();

                    if max.map(|max| elapsed > max).unwrap_or(true) {
                        max = Some(elapsed);
                    }

                    hist.record(elapsed.as_micros().try_into().unwrap())
                        .unwrap();
                }

                println!("p99 concurrent insert: {}ms", max.unwrap().as_millis());
            });
        }
    });

    hist.refresh();

    let mut f = File::create(format!("benches/papaya/{name}.hist")).unwrap();
    let mut s = V2DeflateSerializer::new();
    s.serialize(&hist, &mut EncoderWriter::new(&mut f, &STANDARD))
        .unwrap();
    let _ = render_cdf(
        &hist,
        &format!("benches/papaya/charts/{name}-cdf.png"),
        &format!("{name} concurrent insert CDF"),
    );
}

fn render_cdf(
    hist: &Histogram<u32>,
    path: &str,
    title: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new(path, (1280, 720)).into_drawing_area();
    root.fill(&WHITE)?;
    let max = (hist.max() as f64).max(1.0);
    let min = (hist.min().max(1)) as f64;
    let mut chart = ChartBuilder::on(&root)
        .caption(title, ("sans-serif", 28).into_font())
        .margin(20)
        .x_label_area_size(50)
        .y_label_area_size(60)
        .build_cartesian_2d((min.log10()..max.log10()).step(0.05), 0.0f64..1.0)?;
    chart
        .configure_mesh()
        .x_desc("latency (µs, log10)")
        .y_desc("CDF")
        .draw()?;
    let pts: Vec<(f64, f64)> = (0..=200)
        .map(|i| {
            let q = i as f64 / 200.0;
            ((hist.value_at_quantile(q).max(1) as f64).log10(), q)
        })
        .collect();
    chart.draw_series(LineSeries::new(pts, RED.stroke_width(2)))?;
    root.present()?;
    Ok(())
}
