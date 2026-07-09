//! Multi-engine search benchmark: Common Crawl WET dataset.
//!
//! Compares mfs_db, SQLite (in-memory), redb, and fjall on the same 141K-record
//! URL->content dataset from CC-MAIN-2025-33, measuring point-lookup throughput
//! across 4 access patterns: sequential, random, 2-thread, 4-thread.
//!
//! Run:
//!   cargo run -p mfs-db --release --example cc_wet_bench -- /tmp/mfs-bench/extracted.jsonl
//!
//! Self-contained (download + extract + bench):
//!   cargo run -p mfs-db --release --example cc_wet_bench -- --crawl CC-MAIN-2025-33 /tmp/mfs-bench/
//!
//! Hardware: i5-6300U (Skylake, 2c/4t, L1 64KB, L2 512KB, L3 3MB)
//! Dataset:  ~1.2 GB, 141,858 URL->content records, avg 8.5 KB per value

use std::fs;
use std::hint::black_box;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------
// Record type + JSONL loader
// ---------------------------------------------------------------

#[derive(Debug, Clone)]
struct WetRecord {
    url: String,
    content: Vec<u8>,
}

fn read_jsonl(path: &str) -> std::io::Result<Vec<WetRecord>> {
    let file = fs::File::open(path)?;
    let reader = BufReader::with_capacity(512 * 1024, file);
    let mut records = Vec::new();

    for res in reader.lines() {
        let line = res?;
        if line.is_empty() {
            continue;
        }
        let url = parse_jsonl_field(&line, "\"url\":\"")
            .or_else(|| parse_jsonl_field(&line, "\"url\": \""))
            .unwrap_or_default();
        let content = parse_jsonl_field(&line, "\"content\":\"")
            .or_else(|| parse_jsonl_field(&line, "\"content\": \""))
            .unwrap_or_default();
        if !url.is_empty() && !content.is_empty() {
            records.push(WetRecord {
                url: unescape_json(url),
                content: content.as_bytes().to_vec(),
            });
        }
        if records.len() % 10000 == 0 {
            eprint!("\r  read {} records...", records.len());
        }
    }
    eprintln!("\r  read {} records (done)", records.len());
    Ok(records)
}

fn parse_jsonl_field<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let start = line.find(prefix)?;
    let vs = start + prefix.len();
    let mut escaped = false;
    for (i, ch) in line[vs..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(&line[vs..vs + i]);
        }
    }
    None
}

fn unescape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some(c) => { out.push('\\'); out.push(c); }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

// -----------------------------------------------------------
// Auto-download logic
// -----------------------------------------------------------

const CRAWL_BASE: &str = "https://data.commoncrawl.org";
const SEGMENT_COUNT: usize = 6;

fn crawl_segment_paths(crawl_id: &str) -> std::io::Result<Vec<String>> {
    let url = format!("{CRAWL_BASE}/crawl-data/{crawl_id}/wet.paths.gz");
    let mut resp = ureq::get(&url).call()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("HTTP {e}")))?;
    let data = resp.body_mut().read_to_vec()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("body: {e}")))?;
    let mut decoder = flate2::read::GzDecoder::new(data.as_slice());
    let mut text = String::new();
    decoder.read_to_string(&mut text)?;
    Ok(text.lines().take(SEGMENT_COUNT).map(|s| s.to_string()).collect())
}

/// Download WET segments and extract to JSONL. Returns path to the JSONL file.
fn download_and_extract(crawl_id: &str, output_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let jsonl_path = output_dir.join("extracted.jsonl");
    if jsonl_path.exists() {
        eprintln!("  JSONL already exists at {}", jsonl_path.display());
        return Ok(jsonl_path.to_string_lossy().to_string());
    }
    let wet_dir = output_dir.join("data");
    fs::create_dir_all(&wet_dir)?;
    let paths = crawl_segment_paths(crawl_id)?;
    eprintln!("  Downloading {} WET segments from {crawl_id}...", paths.len());
    for (i, rel_path) in paths.iter().enumerate() {
        let url = format!("{CRAWL_BASE}/{rel_path}");
        let filename = Path::new(rel_path).file_name().unwrap().to_string_lossy();
        let out_path = wet_dir.join(filename.as_ref());
        if out_path.exists() {
            eprintln!("  [{}/{}] {} already cached", i + 1, paths.len(), filename);
            continue;
        }
        eprintln!("  [{}/{}] downloading {} ...", i + 1, paths.len(), filename);
        let mut resp = ureq::get(&url).call()
            .map_err(|e| format!("HTTP error downloading {}: {e}", url))?;
        let body = resp.body_mut().with_config().limit(u64::MAX).read_to_vec()
            .map_err(|e| format!("body read error: {e}"))?;
        fs::write(&out_path, &body)?;
        eprintln!("         {} MB downloaded", body.len() / 1_048_576);
    }
    eprintln!("\n  Extracting WET files to JSONL...");
    let writer = fs::File::create(&jsonl_path)?;
    let mut writer = std::io::BufWriter::new(writer);
    let mut wet_files: Vec<PathBuf> = fs::read_dir(&wet_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("gz"))
        .filter(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.contains("warc.wet")).unwrap_or(false))
        .collect();
    wet_files.sort();
    let mut total = 0u64;
    for (i, path) in wet_files.iter().enumerate() {
        let path_str = path.to_string_lossy();
        eprintln!("  [{}/{}] extracting {} ...", i + 1, wet_files.len(), path.file_name().unwrap().to_string_lossy());
        let records = parse_wet_gz(&path_str)?;
        for rec in &records {
            writeln!(writer,
                "{{\"url\":{url},\"content\":{content}}}",
                url = serde_json::to_string(&rec.url)?,
                content = serde_json::to_string(&std::str::from_utf8(&rec.content).unwrap_or(""))?,
            )?;
        }
        total += records.len() as u64;
        eprintln!("         -> {} records", records.len());
    }
    writer.flush()?;
    eprintln!("  Total: {total} records written to {}", jsonl_path.display());
    Ok(jsonl_path.to_string_lossy().to_string())
}

fn parse_wet_gz(path: &str) -> std::io::Result<Vec<WetRecord>> {
    let file = fs::File::open(path)?;
    let decoder = flate2::read::MultiGzDecoder::new(file);
    let mut reader = BufReader::with_capacity(512 * 1024, decoder);
    let mut records = Vec::new();
    let mut line = String::with_capacity(4096);
    loop {
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 { return Ok(records); }
            if !line.trim().is_empty() { break; }
        }
        if !line.starts_with("WARC/1.0") { continue; }
        let mut url = String::new();
        let mut content_length: usize = 0;
        let mut is_conversion = false;
        let headers_found = loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 { return Ok(records); }
            let trimmed = line.trim();
            if trimmed.is_empty() { break true; }
            if let Some(u) = trimmed.strip_prefix("WARC-Target-URI: ") { url = u.to_string(); }
            if trimmed == "WARC-Type: conversion" { is_conversion = true; }
            if let Some(cl) = trimmed.strip_prefix("Content-Length: ") { content_length = cl.parse().unwrap_or(0); }
        };
        if !headers_found { break; }
        let mut content = vec![0u8; content_length];
        if content_length > 0 {
            if let Err(e) = reader.read_exact(&mut content) { eprintln!("  [warn] truncated record: {e}"); break; }
        }
        if is_conversion && !url.is_empty() && content_length > 0 {
            records.push(WetRecord { url, content });
        }
        if records.len() % 5000 == 0 { eprint!("\r  parsed {} records...", records.len()); }
    }
    Ok(records)
}

// -----------------------------------------------------------
// Harness
// -----------------------------------------------------------

const TRIALS: usize = 5;

struct BenchStats {
    label: String,
    count: u64,
    min: Duration,
    median: Duration,
    max: Duration,
}

impl BenchStats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "  {:<50} {:>8} keys  min={:>8.2} ns  median={:>8.2} ns  max={:>8.2} ns  ({:>10.0} keys/s)",
            self.label, self.count, ns(self.min), ns(self.median), ns(self.max), ops(self.min),
        );
    }
}

fn measure<F>(label: String, count: u64, mut body: F) -> BenchStats
where F: FnMut() -> u64,
{
    let mut samples: Vec<Duration> = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let acc = body();
        let elapsed = start.elapsed();
        black_box(acc);
        samples.push(elapsed);
    }
    samples.sort();
    BenchStats { label, count, min: samples[0], median: samples[TRIALS / 2], max: samples[TRIALS - 1] }
}

fn format_duration(d: Duration) -> String {
    if d.as_secs() > 60 { format!("{:.1}m {}s", d.as_secs_f64() / 60.0, d.as_secs() % 60) }
    else if d.as_secs() > 0 { format!("{:.1}s", d.as_secs_f64()) }
    else if d.as_millis() > 0 { format!("{} ms", d.as_millis()) }
    else if d.as_micros() > 0 { format!("{} us", d.as_micros()) }
    else { format!("{} ns", d.as_nanos()) }
}

// -----------------------------------------------------------
// Engine trait
// -----------------------------------------------------------

trait EngineRunner: Send + Sync {
    fn load(&mut self, records: &[WetRecord]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>>;
}

// ---- MfS DB ----
struct MfsEngine(Option<mfs_db::engine::NoSqlEngine>);

impl EngineRunner for MfsEngine {
    fn load(&mut self, records: &[WetRecord]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        use mfs_db::engine::*;
        let cap = (records.len() * 2).max(1_000_000);
        let engine = NoSqlEngine::open_memory(EngineConfig { raw_initial_capacity: cap, ..EngineConfig::default() })?;
        engine.create_raw_collection("cc")?;
        let keys: Vec<Vec<u8>> = records.iter().map(|r| r.url.as_bytes().to_vec()).collect();
        for (i, rec) in records.iter().enumerate() {
            engine.put_raw("cc", RawKey::from(rec.url.as_bytes()), RawValue::from(rec.content.as_slice()), WriteOptions::default())?;
            if i > 0 && i % 10000 == 0 { eprint!("\r  mfs_db: ingesting {}/{}", i, records.len()); }
        }
        self.0 = Some(engine);
        eprintln!("\r  mfs_db: ingesting {}/{}", records.len(), records.len());
        Ok(keys)
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        use mfs_db::engine::*;
        let eng = self.0.as_ref().unwrap();
        match eng.get_raw("cc", &RawKey::from(key), ReadOptions::default())? {
            Some(r) => Ok(Some(r.value.as_bytes().to_vec())),
            None => Ok(None),
        }
    }
}

// ---- SQLite (Mutex-wrapped for Sync) ----
struct SqliteEngine(Option<Mutex<rusqlite::Connection>>);

impl EngineRunner for SqliteEngine {
    fn load(&mut self, records: &[WetRecord]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.execute_batch(
            "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-200000; PRAGMA temp_store=MEMORY;"
        )?;
        conn.execute("CREATE TABLE cc (url TEXT PRIMARY KEY, content BLOB) WITHOUT ROWID", [])?;
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO cc (url, content) VALUES (?1, ?2)")?;
            for (i, rec) in records.iter().enumerate() {
                stmt.execute(rusqlite::params![rec.url, rec.content])?;
                if i > 0 && i % 10000 == 0 { eprint!("\r  sqlite: ingesting {}/{}", i, records.len()); }
            }
        }
        tx.commit()?;
        eprintln!("\r  sqlite: ingesting {}/{}", records.len(), records.len());
        self.0 = Some(Mutex::new(conn));
        Ok(records.iter().map(|r| r.url.as_bytes().to_vec()).collect())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let url = std::str::from_utf8(key)?;
        let conn = self.0.as_ref().unwrap().lock().unwrap();
        let mut stmt = conn.prepare_cached("SELECT content FROM cc WHERE url = ?1")?;
        let mut rows = stmt.query(rusqlite::params![url])?;
        match rows.next()? {
            Some(row) => Ok(Some(row.get::<_, Vec<u8>>(0)?)),
            None => Ok(None),
        }
    }
}

// ---- redb ----
struct RedbEngine(Option<redb::Database>);
use redb::{ReadableDatabase, TableDefinition};
static CC_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("cc");

impl EngineRunner for RedbEngine {
    fn load(&mut self, records: &[WetRecord]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let tmp = tempfile::NamedTempFile::new()?;
        let db = redb::Database::create(tmp.path())?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(CC_TABLE)?;
            for (i, rec) in records.iter().enumerate() {
                table.insert(rec.url.as_str(), rec.content.as_slice())?;
                if i > 0 && i % 10000 == 0 { eprint!("\r  redb: ingesting {}/{}", i, records.len()); }
            }
        }
        txn.commit()?;
        self.0 = Some(db);
        eprintln!("\r  redb: ingesting {}/{}", records.len(), records.len());
        Ok(records.iter().map(|r| r.url.as_bytes().to_vec()).collect())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let db = self.0.as_ref().unwrap();
        let txn = db.begin_read()?;
        let table = txn.open_table(CC_TABLE)?;
        let url = std::str::from_utf8(key)?;
        match table.get(url)? {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }
}

// ---- fjall (LSM-tree) ----
struct FjallEngine {
    db: Option<fjall::Database>,
    tree: Option<fjall::Keyspace>,
}

impl EngineRunner for FjallEngine {
    fn load(&mut self, records: &[WetRecord]) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let db = fjall::Database::builder(tmp.path()).open()?;
        let tree = db.keyspace("cc", || fjall::KeyspaceCreateOptions::default())?;
        for (i, rec) in records.iter().enumerate() {
            tree.insert(rec.url.as_bytes(), &rec.content)?;
            if i > 0 && i % 10000 == 0 { eprint!("\r  fjall: ingesting {}/{}", i, records.len()); }
        }
        self.db = Some(db);
        self.tree = Some(tree);
        eprintln!("\r  fjall: ingesting {}/{}", records.len(), records.len());
        Ok(records.iter().map(|r| r.url.as_bytes().to_vec()).collect())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let tree = self.tree.as_ref().unwrap();
        match tree.get(key)? {
            Some(v) => Ok(Some(v.to_vec())),
            None => Ok(None),
        }
    }
}

// -----------------------------------------------------------
// Run engines
// -----------------------------------------------------------

fn bench_mfs(records: &[WetRecord], total_data_gb: f64) -> Result<(), Box<dyn std::error::Error>> {
    let mut eng = MfsEngine(None);
    let keys = eng.load(records)?;
    run_all_patterns("mfs_db (ConcurrentMap)", eng, &keys, records, total_data_gb)
}

fn bench_sqlite(records: &[WetRecord], total_data_gb: f64) -> Result<(), Box<dyn std::error::Error>> {
    let mut eng = SqliteEngine(None);
    let keys = eng.load(records)?;
    run_all_patterns("SQLite (in-memory)", eng, &keys, records, total_data_gb)
}

fn bench_redb(records: &[WetRecord], total_data_gb: f64) -> Result<(), Box<dyn std::error::Error>> {
    let mut eng = RedbEngine(None);
    let keys = eng.load(records)?;
    run_all_patterns("redb", eng, &keys, records, total_data_gb)
}

fn bench_fjall(records: &[WetRecord], total_data_gb: f64) -> Result<(), Box<dyn std::error::Error>> {
    let mut eng = FjallEngine { db: None, tree: None };
    let keys = eng.load(records)?;
    run_all_patterns("fjall (LSM-tree)", eng, &keys, records, total_data_gb)
}

fn run_all_patterns(
    name: &'static str,
    eng: impl EngineRunner + 'static,
    keys: &[Vec<u8>],
    records: &[WetRecord],
    total_data_gb: f64,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = keys.len() as u64;
    println!("\n  --- {} ---", name);
    println!("  Data: {:.3} GB, {} keys", total_data_gb, count);

    let eng = Arc::new(eng);
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

    // Sequential
    measure(format!("{} sequential", name), count, || {
        let mut sum: u64 = 0;
        for row in 0..count as usize {
            if let Ok(Some(v)) = eng.get(key_refs[row]) {
                sum ^= v.first().copied().unwrap_or(0) as u64;
            }
        }
        sum
    })
    .print();
    drop(key_refs);

    // Random
    {
        use rand::rngs::SmallRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut shuffled: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        shuffled.shuffle(&mut rng);
        measure(format!("{} random", name), count, || {
            let mut sum: u64 = 0;
            for k in &shuffled { if let Ok(Some(v)) = eng.get(k) { sum ^= v.first().copied().unwrap_or(0) as u64; } }
            sum
        })
        .print();
    }

    // 2 threads
    let all_keys: Vec<Vec<u8>> = records.iter().map(|r| r.url.as_bytes().to_vec()).collect();
    {
        use rand::rngs::SmallRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut shuffled = all_keys.clone();
        shuffled.shuffle(&mut rng);
        let cs = shuffled.len() / 2;
        let chunks = [shuffled[..cs].to_vec(), shuffled[cs..].to_vec()];
        let eng = eng.clone();
        measure(format!("{} 2-thread random", name), count, || {
            let sum = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for chunk in &chunks {
                let c = chunk.clone();
                let e = eng.clone();
                let s = sum.clone();
                handles.push(thread::spawn(move || {
                    let mut local: u64 = 0;
                    for k in &c { if let Ok(Some(v)) = e.get(k) { local ^= v.first().copied().unwrap_or(0) as u64; } }
                    s.fetch_add(local as usize, Ordering::Relaxed);
                }));
            }
            for h in handles { h.join().unwrap(); }
            sum.load(Ordering::Relaxed) as u64
        }).print();
    }

    // 4 threads
    {
        use rand::rngs::SmallRng;
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
        let mut rng = SmallRng::seed_from_u64(42);
        let mut shuffled = all_keys;
        shuffled.shuffle(&mut rng);
        let cs = shuffled.len() / 4;
        let chunks = [shuffled[..cs].to_vec(), shuffled[cs..cs*2].to_vec(), shuffled[cs*2..cs*3].to_vec(), shuffled[cs*3..].to_vec()];
        let eng = eng.clone();
        measure(format!("{} 4-thread random", name), count, || {
            let sum = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::new();
            for chunk in &chunks {
                let c = chunk.clone();
                let e = eng.clone();
                let s = sum.clone();
                handles.push(thread::spawn(move || {
                    let mut local: u64 = 0;
                    for k in &c { if let Ok(Some(v)) = e.get(k) { local ^= v.first().copied().unwrap_or(0) as u64; } }
                    s.fetch_add(local as usize, Ordering::Relaxed);
                }));
            }
            for h in handles { h.join().unwrap(); }
            sum.load(Ordering::Relaxed) as u64
        }).print();
    }
    Ok(())
}

// -----------------------------------------------------------
// Main
// -----------------------------------------------------------

fn run_benchmark(jsonl_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("+---------------------------------------------------------------+");
    println!("| Multi-Engine KV Search Benchmark: Common Crawl WET              |");
    println!("+---------------------------------------------------------------+");
    println!();

    let load_start = Instant::now();
    let all_records = read_jsonl(jsonl_path)?;
    let load_elapsed = load_start.elapsed();
    let total_value_bytes: usize = all_records.iter().map(|r| r.content.len()).sum();
    let total_key_bytes: usize = all_records.iter().map(|r| r.url.len()).sum();
    let total_data_gb = (total_value_bytes + total_key_bytes) as f64 / 1_000_000_000.0;

    println!("Records:          {}", all_records.len());
    println!("Total data:       {:.3} GB", total_data_gb);
    println!("Avg value size:   {:.0} bytes", total_value_bytes as f64 / all_records.len() as f64);
    println!("Load time:        {}", format_duration(load_elapsed));
    println!();
    println!("Phase 2: Search benchmarks ({} trials, min/median/max)", TRIALS);

    bench_mfs(&all_records, total_data_gb)?;
    bench_sqlite(&all_records, total_data_gb)?;
    bench_redb(&all_records, total_data_gb)?;
    bench_fjall(&all_records, total_data_gb)?;

    println!("\nDone.");
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage:");
        eprintln!("  {} <jsonl_path>                  direct bench", args[0]);
        eprintln!("  {} --crawl CC-MAIN-2025-33 <dir>  download + extract + bench", args[0]);
        std::process::exit(1);
    }
    let jsonl_path = if args[1] == "--crawl" {
        if args.len() < 4 { eprintln!("Usage: {} --crawl CC-MAIN-YYYY-NN <output_dir>", args[0]); std::process::exit(1); }
        match download_and_extract(&args[2], &PathBuf::from(&args[3])) {
            Ok(p) => p,
            Err(e) => { eprintln!("Download/extract failed: {e}"); std::process::exit(1); }
        }
    } else {
        args[1].clone()
    };
    if let Err(e) = run_benchmark(&jsonl_path) { eprintln!("Error: {e}"); std::process::exit(1); }
}