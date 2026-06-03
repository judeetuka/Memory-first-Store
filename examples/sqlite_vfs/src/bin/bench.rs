use mfs_sqlite_vfs_example::{register_mfs_vfs, unique_vfs_name};
use rusqlite::{Connection, OpenFlags, params};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct BenchResult {
    label: &'static str,
    rows: usize,
    elapsed: Duration,
    count: i64,
}

impl BenchResult {
    fn print(&self) {
        let ops = self.rows as f64 / self.elapsed.as_secs_f64();
        println!(
            "{:<30} rows={} count={} elapsed={:.3}s ops/sec={:.0}",
            self.label,
            self.rows,
            self.count,
            self.elapsed.as_secs_f64(),
            ops,
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows = env_usize("MFS_SQLITE_ROWS", 100_000);
    let autocommit_rows = env_usize("MFS_SQLITE_AUTOCOMMIT_ROWS", rows.min(10_000));
    let writers = env_usize("MFS_SQLITE_WRITERS", 4).max(1);
    let writer_rows = env_usize("MFS_SQLITE_WRITER_ROWS", 2_000);
    let payload_bytes = env_usize("MFS_SQLITE_PAYLOAD_BYTES", 32);
    let payload = vec![7u8; payload_bytes];

    println!(
        "=== SQLite VFS insert benchmark rows={} autocommit_rows={} payload={}B ===",
        rows, autocommit_rows, payload_bytes
    );
    println!("PRAGMAs: journal_mode=MEMORY, synchronous=OFF, temp_store=MEMORY");

    let mut stock_memory = Connection::open_in_memory()?;
    run_batched("stock_memory_tx", &mut stock_memory, rows, &payload)?.print();

    let mut mfs = open_mfs_connection("mfs-bench")?;
    run_batched("mfs_vfs_tx", &mut mfs, rows, &payload)?.print();

    let stock_path = temp_db_path("mfs-sqlite-stock");
    let mut stock_file = Connection::open(&stock_path)?;
    run_batched("stock_file_tx", &mut stock_file, rows, &payload)?.print();
    drop(stock_file);
    std::fs::remove_file(&stock_path).ok();

    let mut stock_memory_auto = Connection::open_in_memory()?;
    run_autocommit(
        "stock_memory_autocommit",
        &mut stock_memory_auto,
        autocommit_rows,
        &payload,
    )?
    .print();

    let mut mfs_auto = open_mfs_connection("mfs-autocommit")?;
    run_autocommit(
        "mfs_vfs_autocommit",
        &mut mfs_auto,
        autocommit_rows,
        &payload,
    )?
    .print();

    run_multi_writer_stock_file(writers, writer_rows, &payload)?.print();
    run_multi_writer_mfs(writers, writer_rows, &payload)?.print();

    Ok(())
}

fn open_mfs_connection(prefix: &str) -> Result<Connection, Box<dyn std::error::Error>> {
    let vfs = unique_vfs_name(prefix);
    let _logger = register_mfs_vfs(&vfs).map_err(|code| format!("vfs register failed: {code}"))?;
    let conn = Connection::open_with_flags_and_vfs(
        "bench.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs.as_str(),
    )?;
    Ok(conn)
}

fn configure(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = MEMORY;
         PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;
         DROP TABLE IF EXISTS kv;
         CREATE TABLE kv (id INTEGER PRIMARY KEY, payload BLOB NOT NULL);",
    )
}

fn run_batched(
    label: &'static str,
    conn: &mut Connection,
    rows: usize,
    payload: &[u8],
) -> rusqlite::Result<BenchResult> {
    configure(conn)?;
    let start = Instant::now();
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached("INSERT INTO kv (id, payload) VALUES (?1, ?2)")?;
        for row in 0..rows {
            stmt.execute(params![row as i64, payload])?;
        }
    }
    tx.commit()?;
    let elapsed = start.elapsed();
    let count = row_count(conn)?;
    Ok(BenchResult {
        label,
        rows,
        elapsed,
        count,
    })
}

fn run_autocommit(
    label: &'static str,
    conn: &mut Connection,
    rows: usize,
    payload: &[u8],
) -> rusqlite::Result<BenchResult> {
    configure(conn)?;
    let start = Instant::now();
    {
        let mut stmt = conn.prepare_cached("INSERT INTO kv (id, payload) VALUES (?1, ?2)")?;
        for row in 0..rows {
            stmt.execute(params![row as i64, payload])?;
        }
    }
    let elapsed = start.elapsed();
    let count = row_count(conn)?;
    Ok(BenchResult {
        label,
        rows,
        elapsed,
        count,
    })
}

fn row_count(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
}

fn setup_multi(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = MEMORY;
         PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;
         DROP TABLE IF EXISTS kv;
         CREATE TABLE kv (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, payload BLOB NOT NULL);",
    )
}

fn configure_worker(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.execute_batch(
        "PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;",
    )
}

fn run_worker_tx(
    mut conn: Connection,
    worker: usize,
    rows: usize,
    payload: Vec<u8>,
) -> Result<usize, String> {
    configure_worker(&conn).map_err(|err| err.to_string())?;
    let tx = conn.transaction().map_err(|err| err.to_string())?;
    {
        let mut stmt = tx
            .prepare_cached("INSERT INTO kv (id, worker, payload) VALUES (?1, ?2, ?3)")
            .map_err(|err| err.to_string())?;
        let base = worker * rows;
        for row in 0..rows {
            stmt.execute(params![
                (base + row) as i64,
                worker as i64,
                payload.as_slice()
            ])
            .map_err(|err| err.to_string())?;
        }
    }
    tx.commit().map_err(|err| err.to_string())?;
    Ok(rows)
}

fn run_multi_writer_stock_file(
    writers: usize,
    rows_per_writer: usize,
    payload: &[u8],
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let path = temp_db_path("mfs-sqlite-stock-multi");
    let conn = Connection::open(&path)?;
    setup_multi(&conn)?;
    drop(conn);

    let barrier = Arc::new(Barrier::new(writers));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(writers);
    for worker in 0..writers {
        let path = path.clone();
        let payload = payload.to_vec();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = Connection::open(path).map_err(|err| err.to_string())?;
            barrier.wait();
            run_worker_tx(conn, worker, rows_per_writer, payload)
        }));
    }

    let mut inserted = 0usize;
    for handle in handles {
        inserted += handle.join().map_err(|_| "stock worker panicked")??;
    }
    let elapsed = start.elapsed();
    let conn = Connection::open(&path)?;
    let count = row_count(&conn)?;
    drop(conn);
    std::fs::remove_file(&path).ok();
    Ok(BenchResult {
        label: "stock_file_multi_writer",
        rows: inserted,
        elapsed,
        count,
    })
}

fn run_multi_writer_mfs(
    writers: usize,
    rows_per_writer: usize,
    payload: &[u8],
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let vfs = unique_vfs_name("mfs-multi");
    let _logger = register_mfs_vfs(&vfs).map_err(|code| format!("vfs register failed: {code}"))?;
    let conn = Connection::open_with_flags_and_vfs(
        "multi.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs.as_str(),
    )?;
    setup_multi(&conn)?;
    drop(conn);

    let barrier = Arc::new(Barrier::new(writers));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(writers);
    for worker in 0..writers {
        let vfs = vfs.clone();
        let payload = payload.to_vec();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let conn = Connection::open_with_flags_and_vfs(
                "multi.db",
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                vfs.as_str(),
            )
            .map_err(|err| err.to_string())?;
            barrier.wait();
            run_worker_tx(conn, worker, rows_per_writer, payload)
        }));
    }

    let mut inserted = 0usize;
    for handle in handles {
        inserted += handle.join().map_err(|_| "mfs worker panicked")??;
    }
    let elapsed = start.elapsed();
    let conn = Connection::open_with_flags_and_vfs(
        "multi.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs.as_str(),
    )?;
    let count = row_count(&conn)?;
    Ok(BenchResult {
        label: "mfs_vfs_multi_writer",
        rows: inserted,
        elapsed,
        count,
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn temp_db_path(prefix: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{}-{stamp}.db", std::process::id()))
}
