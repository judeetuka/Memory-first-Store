//! Crash recovery via the bundled write-ahead log.
//!
//! Demonstrates the full durability lifecycle:
//!
//! 1. Open or create a WAL file.
//! 2. Replay any existing WAL contents into a fresh `MemoryFirstStore`
//!    (this is what you do on startup — it gives you back the state the
//!    process had when it last crashed or shut down).
//! 3. Run normally: writes go to the cache, the flusher drains them
//!    through the WAL.
//! 4. `sync_now()` is the durability barrier — call it before you
//!    acknowledge a write to a client, otherwise the loss window is
//!    bounded only by the WAL config.
//!
//! Run with `cargo run -p mfs-core --release --example wal_recovery`. Run it twice
//! and you'll see the second invocation pick up state from the first
//! through the on-disk WAL.

use mfs_core::MemoryFirstStore;
use mfs_core::durability::{U64Codec, WalBackend, WalConfig, replay_into_u64_store};
use std::path::Path;

fn main() -> std::io::Result<()> {
    let wal_path = Path::new("/tmp/mfs_example.wal");

    // 1. Replay anything that was on disk into the in-memory store.
    let store = MemoryFirstStore::<u64, u64>::new();
    let recovered = replay_into_u64_store(wal_path, &store)?;
    println!(
        "recovered {} records from {}",
        recovered,
        wal_path.display()
    );
    if recovered > 0 {
        println!(
            "  store now has {} live entries (sample: key=1 -> {:?})",
            store.stats().len,
            store.get(&1).map(|v| *v),
        );
    }

    // 2. Open the WAL backend for ongoing writes.
    let mut wal = WalBackend::open(wal_path, U64Codec, WalConfig::default())?;

    // 3. Mutate the cache. These are dirty until flushed.
    let next_seed = recovered as u64;
    for i in 0..10u64 {
        let key = next_seed + i;
        store.put(key, key.wrapping_mul(7));
    }
    println!(
        "wrote 10 new records with keys {}..{}",
        next_seed,
        next_seed + 10
    );

    // 4. Persist via the WAL. After sync_now() the data survives kill -9
    // and power loss.
    let written = store.flush_idle(&mut wal, /*idle_ticks=*/ 1, /*max=*/ 10_000)?;
    wal.sync_now()?;
    println!("flushed {written} dirty records and fsync'd the WAL");

    println!();
    println!("Run this example again to see the next invocation pick up");
    println!("everything from {}.", wal_path.display());
    println!("Delete the file to start fresh: rm {}", wal_path.display());

    Ok(())
}
