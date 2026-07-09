//! Extract URL→plain-text pairs from Common Crawl WET files into JSON Lines.
//!
//! Reads .warc.wet.gz files (concatenated gzip members, each gzipped WARC record
//! is an independent member), parses WARC headers, and writes one JSON object
//! per line to stdout.
//!
//! Usage:
//!   cargo run -p mfs-db --release --example cc_wet_extract -- \
//!       /tmp/mfs-bench/data/ > /tmp/mfs-bench/extracted.jsonl
//!
//! Or write directly to a file:
//!   cargo run -p mfs-db --release --example cc_wet_extract -- \
//!       /tmp/mfs-bench/data/ /tmp/mfs-bench/extracted.jsonl
//!
//! Output format (JSON Lines):
//!   {"url":"http://...","content":"page plain text..."}

use flate2::read::MultiGzDecoder;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Clone)]
struct WetRecord {
    url: String,
    content: Vec<u8>,
}

/// Parse a .warc.wet.gz file using MultiGzDecoder (handles concatenated gzip
/// members, which is the standard Common Crawl WARC/WET format).
fn parse_wet_gz(path: &str) -> std::io::Result<Vec<WetRecord>> {
    let file = fs::File::open(path)?;
    // MultiGzDecoder: decompresses ALL concatenated gzip members
    let decoder = MultiGzDecoder::new(file);
    let mut reader = BufReader::with_capacity(512 * 1024, decoder);
    let mut records = Vec::new();
    let mut line = String::with_capacity(4096);

    loop {
        // Skip blank lines between records
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                return Ok(records);
            }
            if !line.trim().is_empty() {
                break;
            }
        }

        if !line.starts_with("WARC/1.0") {
            continue;
        }

        // Parse WARC headers
        let mut url = String::new();
        let mut content_length: usize = 0;
        let mut is_conversion = false;
        let headers_found = loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                return Ok(records);
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                break true;
            }

            if let Some(u) = trimmed.strip_prefix("WARC-Target-URI: ") {
                url = u.to_string();
            }
            if trimmed == "WARC-Type: conversion" {
                is_conversion = true;
            }
            if let Some(cl) = trimmed.strip_prefix("Content-Length: ") {
                content_length = cl.parse().unwrap_or(0);
            }
        };

        if !headers_found {
            break;
        }

        // Read content body
        let mut content = vec![0u8; content_length];
        if content_length > 0 {
            if let Err(e) = reader.read_exact(&mut content) {
                eprintln!("  [warn] truncated record (content_length={}): {e}", content_length);
                break;
            }
        }

        // Only keep conversion records with actual content
        if is_conversion && !url.is_empty() && content_length > 0 {
            records.push(WetRecord { url, content });
        }

        if records.len() % 5000 == 0 {
            eprint!("\r  parsed {} records...", records.len());
        }
    }

    Ok(records)
}

fn format_duration(d: std::time::Duration) -> String {
    if d.as_secs() > 60 {
        format!("{:.1}m {}s", d.as_secs_f64() / 60.0, d.as_secs() % 60)
    } else if d.as_secs() > 0 {
        format!("{:.1}s", d.as_secs_f64())
    } else if d.as_millis() > 0 {
        format!("{} ms", d.as_millis())
    } else if d.as_micros() > 0 {
        format!("{} µs", d.as_micros())
    } else {
        format!("{} ns", d.as_nanos())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run -p mfs-db --release --example cc_wet_extract -- <data_dir> [output_file]");
        eprintln!();
        eprintln!("Extracts URL→content pairs from Common Crawl WET files into JSON Lines.");
        eprintln!("If no output_file is given, writes to stdout.");
        std::process::exit(1);
    }

    let data_dir = &args[1];
    let output: Box<dyn Write> = if args.len() > 2 {
        Box::new(fs::File::create(&args[2])?)
    } else {
        Box::new(std::io::stdout())
    };
    let mut writer = std::io::BufWriter::new(output);

    // Discover WET files
    let mut wet_files: Vec<PathBuf> = fs::read_dir(data_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("gz"))
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.contains("warc.wet"))
                .unwrap_or(false)
        })
        .collect();
    wet_files.sort();

    if wet_files.is_empty() {
        return Err(format!("no .warc.wet.gz files found in {data_dir}").into());
    }

    eprintln!("Extracting {} WET files from {data_dir}", wet_files.len());
    eprintln!();

    let total_start = Instant::now();
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;

    for (i, path) in wet_files.iter().enumerate() {
        let path_str = path.to_string_lossy();
        let file_start = Instant::now();
        eprintln!("[{}/{}] parsing {} ...",
            i + 1, wet_files.len(),
            path.file_name().unwrap().to_string_lossy());

        let records = parse_wet_gz(&path_str)?;

        // Write JSON Lines
        let mut file_bytes = 0u64;
        for rec in &records {
            file_bytes += rec.content.len() as u64;
            writeln!(writer,
                "{{\"url\":{url},\"content\":{content}}}",
                url = serde_json::to_string(&rec.url)?,
                content = serde_json::to_string(&std::str::from_utf8(&rec.content).unwrap_or(""))?,
            )?;
        }
        total_bytes += file_bytes;

        let elapsed = file_start.elapsed();
        eprintln!("  → {} records, {:.1} MB in {} ({:.0} rec/s)",
            records.len(),
            file_bytes as f64 / 1_048_576.0,
            format_duration(elapsed),
            records.len() as f64 / elapsed.as_secs_f64(),
        );
        total_records += records.len() as u64;
    }

    writer.flush()?;

    let total_elapsed = total_start.elapsed();
    eprintln!();
    eprintln!("Done: {total_records} records, {:.1} MB total ({:.0} rec/s)",
        total_bytes as f64 / 1_048_576.0,
        total_records as f64 / total_elapsed.as_secs_f64(),
    );

    Ok(())
}
