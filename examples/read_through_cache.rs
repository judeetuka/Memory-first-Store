//! Read-through cache backed by a slow "database".
//!
//! This is the canonical Redis-replacement pattern:
//!
//! 1. App calls `get_user(id)`.
//! 2. Cache hit ⇒ return immediately, no DB load.
//! 3. Cache miss ⇒ load from DB, populate the cache as `load_clean`
//!    (clean: this came from the source of truth, no need to flush back),
//!    return.
//! 4. App calls `update_user(id, ...)` ⇒ writes to cache via `put`,
//!    which marks the entry dirty.
//! 5. A background flusher periodically drains dirty entries to the DB.
//!
//! ## Caveat: read-after-delete
//!
//! `WriteBehindCache::get` returns `None` both for "key was never present"
//! and "key was deleted but the tombstone hasn't flushed yet". In a naïve
//! read-through pattern, a `get` immediately after a `delete` would hit
//! `None`, fall through to the DB, find the still-undeleted row there,
//! and `load_clean` it back into the cache — silently undoing the delete.
//!
//! This example sidesteps the race by waiting for the flusher to drain
//! the delete tombstone before reading again. In production you'd want
//! one of: synchronous `flush_idle` after a delete, an
//! application-level "pending deletes" set, or a tri-state cache API
//! that exposes tombstones explicitly.
//!
//! Run with `cargo run --release --example read_through_cache`.

use mfs_core::writeback::WriteBehindCache;
use mfs_core::{FlushBackend, FlushRecord, Operation};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct UserRecord {
    id: u64,
    name: String,
    email: String,
}

/// Stand-in for the real DB. In a real app this is Postgres / SQLite /
/// Mongo / whatever — anywhere `flush()` writes to the durable side.
#[derive(Default)]
struct MockDb {
    rows: HashMap<u64, UserRecord>,
    read_count: usize,
    write_count: usize,
    delete_count: usize,
}

impl MockDb {
    fn select(&mut self, id: u64) -> Option<UserRecord> {
        self.read_count += 1;
        // simulate the latency of a DB round-trip
        thread::sleep(Duration::from_micros(200));
        self.rows.get(&id).cloned()
    }
}

/// FlushBackend wraps the DB so the cache can persist dirty rows in
/// batches. Idempotent: if the same record is replayed, the upsert is
/// equivalent.
struct DbBackend {
    db: Arc<Mutex<MockDb>>,
}

impl FlushBackend<u64, UserRecord> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, UserRecord>]) -> Result<(), ()> {
        let mut db = self.db.lock().unwrap();
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put) => {
                    db.rows.insert(r.key, v.as_ref().clone());
                    db.write_count += 1;
                }
                (None, Operation::Delete) => {
                    db.rows.remove(&r.key);
                    db.delete_count += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// The cached repository. The app talks to this, not the cache or DB
/// directly.
struct UserRepo {
    cache: Arc<WriteBehindCache<u64, UserRecord>>,
    db: Arc<Mutex<MockDb>>,
}

impl UserRepo {
    fn get(&self, id: u64) -> Option<UserRecord> {
        // Fast path: already in cache.
        if let Some(rec) = self.cache.get(&id) {
            return Some(rec.as_ref().clone());
        }

        // Slow path: pull from the DB and populate the cache as CLEAN
        // so the loaded record doesn't immediately flush back.
        let row = self.db.lock().unwrap().select(id)?;
        self.cache.load_clean(id, row.clone());
        Some(row)
    }

    fn update(&self, id: u64, name: &str, email: &str) {
        // `put` marks dirty; the flusher will eventually persist this.
        self.cache.put(
            id,
            UserRecord {
                id,
                name: name.to_string(),
                email: email.to_string(),
            },
        );
    }

    fn delete(&self, id: u64) {
        self.cache.delete(id);
    }
}

fn spawn_flusher(
    cache: Arc<WriteBehindCache<u64, UserRecord>>,
    db: Arc<Mutex<MockDb>>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut backend = DbBackend { db };
        // Tick every 100 ms, drain up to 10k records each time, treat
        // entries as flushable after 32 logical clock ticks of idleness.
        while !stop.load(Ordering::Relaxed) {
            let _ = cache.flush_idle(&mut backend, 32, 10_000);
            thread::sleep(Duration::from_millis(100));
        }
        // Best-effort final drain on shutdown.
        for _ in 0..16 {
            let n = cache.flush_idle(&mut backend, 0, 50_000).unwrap_or(0);
            if n == 0 {
                break;
            }
        }
    })
}

fn main() {
    let cache = Arc::new(WriteBehindCache::<u64, UserRecord>::with_capacity(10_000));
    let db = Arc::new(Mutex::new(MockDb::default()));

    // Seed the DB with some pre-existing rows.
    {
        let mut g = db.lock().unwrap();
        for i in 0..50u64 {
            g.rows.insert(
                i,
                UserRecord {
                    id: i,
                    name: format!("preloaded user {i}"),
                    email: format!("user{i}@example.com"),
                },
            );
        }
    }

    let repo = UserRepo {
        cache: Arc::clone(&cache),
        db: Arc::clone(&db),
    };
    let stop = Arc::new(AtomicBool::new(false));
    let flusher = spawn_flusher(Arc::clone(&cache), Arc::clone(&db), Arc::clone(&stop));

    // First read: cache miss ⇒ DB hit ⇒ cache populated.
    println!("first get(7):  {:?}", repo.get(7));

    // Second read: cache hit, no DB traffic.
    println!("second get(7): {:?}", repo.get(7));

    // Update goes to cache; flusher will persist it later.
    repo.update(7, "renamed user 7", "new7@example.com");
    println!("after update:  {:?}", repo.get(7));

    // Delete goes to cache as a tombstone; persisted on next flush.
    repo.delete(42);
    println!("delete(42) issued — waiting for flusher to persist the tombstone");
    println!("(reading via the read-through `get` before flush would race");
    println!(" the tombstone against the DB select; see the doc-comment caveat)");

    // Wait for the flusher to drain. After this returns, the delete is in
    // the DB and the cache tombstone has been cleaned up.
    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Relaxed);
    flusher.join().unwrap();
    println!("post-flush get(42): {:?}", repo.get(42));

    let g = db.lock().unwrap();
    println!();
    println!("DB activity:");
    println!("  selects: {}", g.read_count);
    println!("  writes:  {}", g.write_count);
    println!("  deletes: {}", g.delete_count);
    println!("  rows:    {}", g.rows.len());
    println!();
    println!("user 7 in DB now: {:?}", g.rows.get(&7));
    println!("user 42 in DB now: {:?}", g.rows.get(&42));
}
