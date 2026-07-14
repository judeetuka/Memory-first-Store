//! Crash recovery for Redis-like object-store values.
//!
//! This is the object-store version of `wal_recovery`: replay the WAL into a
//! fresh `MfsObjectStore`, mutate Redis-like values in memory, flush through the
//! WAL, then `sync_now()` as the durable boundary.
//!
//! Run with `cargo run -p mfs-compat --release --example object_store_wal`. Run it twice and
//! the second invocation will recover values written by the first.

use mfs_compat::object_store::MfsObjectStore;
use mfs_core::Operation;
use mfs_core::durability::{WalBackend, WalConfig};
use mfs_store::value::{MfsValue, MfsValueCodec};
use std::path::Path;

fn replay_object_store(path: &Path, store: &MfsObjectStore) -> std::io::Result<usize> {
    WalBackend::<Vec<u8>, MfsValue, MfsValueCodec>::replay(path, &MfsValueCodec, |record| {
        match record.op {
            Operation::Put => {
                if let Some(value) = record.value {
                    store.load_clean(record.key, value);
                }
            }
            Operation::Delete => {
                store.delete(record.key);
            }
        }
    })
}

fn text(value: Option<Vec<u8>>) -> String {
    value
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|| "<missing>".to_string())
}

fn main() -> std::io::Result<()> {
    let wal_path = Path::new("/tmp/mfs_object_store.wal");

    let store = MfsObjectStore::with_capacity(10_000);
    let recovered = replay_object_store(wal_path, &store)?;
    println!(
        "recovered {} object records from {}",
        recovered,
        wal_path.display()
    );

    if recovered > 0 {
        let name = store.get_string(b"user:latest:name").expect("string key");
        let email = store
            .hash_get(b"user:latest:profile", b"email")
            .expect("hash key");
        println!("  recovered user: name={:?}, email={}", name, text(email));
    }

    let mut wal = WalBackend::open(wal_path, MfsValueCodec, WalConfig::default())?;
    let run = recovered as u64;

    store.set_string(b"user:latest:name".to_vec(), format!("Ada #{run}"));
    store
        .hash_set(
            b"user:latest:profile".to_vec(),
            b"email".to_vec(),
            format!("ada{run}@example.com").into_bytes(),
        )
        .expect("profile is a hash");
    store
        .hash_set(
            b"user:latest:profile".to_vec(),
            b"role".to_vec(),
            b"engineer".to_vec(),
        )
        .expect("profile is a hash");
    store
        .list_push(
            b"user:latest:events".to_vec(),
            format!("login:{run}").into_bytes(),
        )
        .expect("events is a list");
    store
        .zadd(
            b"leaderboard".to_vec(),
            run as f64,
            format!("user:{run}").into_bytes(),
        )
        .expect("leaderboard is a sorted set");
    store.set_json_bytes(b"user:latest:json".to_vec(), br#"{"active":true}"#.to_vec());

    let written = store.flush_idle(
        &mut wal, /*idle_ticks=*/ 0, /*max_records=*/ 10_000,
    )?;
    wal.sync_now()?;
    println!("flushed {written} dirty object records and fsync'd the WAL");

    let leaderboard = store
        .zrange(b"leaderboard", 0, -1)
        .expect("leaderboard zset");
    println!(
        "  leaderboard members: {:?}",
        leaderboard
            .into_iter()
            .map(|member| String::from_utf8_lossy(&member).into_owned())
            .collect::<Vec<_>>()
    );

    println!();
    println!("Run this example again to replay {}.", wal_path.display());
    println!("Delete it to start fresh: rm {}", wal_path.display());

    Ok(())
}
