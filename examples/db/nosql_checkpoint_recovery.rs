//! Raw NoSQL checkpoint plus WAL suffix recovery.
//!
//! Run with `cargo run -p mfs-db --release --example nosql_checkpoint_recovery`.

use mfs_db::engine::{
    EngineConfig, Lsn, NoSqlEngine, RawKey, RawValue, RawWalSegmentWriter, ReadOptions,
    WriteOptions, recover_raw_checkpoint_then_wal, write_raw_checkpoint_to_dir,
};

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = std::env::temp_dir().join("mfs_nosql_checkpoint_recovery_example");
    let checkpoint_dir = root.join("checkpoints");
    let wal_path = root.join("raw.wal");
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&checkpoint_dir)?;

    let config = EngineConfig {
        raw_initial_capacity: 16,
        ..EngineConfig::default()
    };
    let engine = NoSqlEngine::open_memory(config.clone())?;
    engine.create_raw_collection("raw_users")?;

    let key_a = RawKey::from(&b"user:1"[..]);
    let key_b = RawKey::from(&b"user:2"[..]);
    let key_c = RawKey::from(&b"user:3"[..]);

    {
        let mut wal = RawWalSegmentWriter::open(&wal_path)?;
        wal.append_put("raw_users", &key_a, &RawValue::from(&b"ada"[..]))?;
        engine.put_raw(
            "raw_users",
            key_a.clone(),
            RawValue::from(&b"ada"[..]),
            WriteOptions::default(),
        )?;
        wal.append_put("raw_users", &key_b, &RawValue::from(&b"grace"[..]))?;
        engine.put_raw(
            "raw_users",
            key_b.clone(),
            RawValue::from(&b"grace"[..]),
            WriteOptions::default(),
        )?;
        wal.sync_now()?;

        let checkpoint = write_raw_checkpoint_to_dir(&checkpoint_dir, &engine, Lsn::new(2))?;
        println!(
            "checkpoint wrote {} records at LSN {}",
            checkpoint.record_count,
            checkpoint.checkpoint_lsn.get()
        );

        wal.append_put("raw_users", &key_a, &RawValue::from(&b"ada-updated"[..]))?;
        wal.append_delete("raw_users", &key_b)?;
        wal.append_put("raw_users", &key_c, &RawValue::from(&b"katherine"[..]))?;
        wal.sync_now()?;
        println!(
            "WAL suffix synced through LSN 5 at {}",
            wal.path().display()
        );
    }

    let recovery = recover_raw_checkpoint_then_wal(&checkpoint_dir, &wal_path, config)?;
    let checkpoint = recovery.checkpoint.expect("checkpoint is present");
    assert_eq!(checkpoint.metadata.checkpoint_lsn, Lsn::new(2));
    assert_eq!(recovery.wal.records, 3);
    assert_eq!(recovery.wal.last_lsn, Lsn::new(5));

    let read_a = recovery
        .engine
        .get_raw("raw_users", &key_a, ReadOptions::default())?
        .expect("user:1 recovered");
    assert_eq!(read_a.value.as_bytes(), b"ada-updated");
    let read_c = recovery
        .engine
        .get_raw("raw_users", &key_c, ReadOptions::default())?
        .expect("user:3 recovered");
    assert!(
        recovery
            .engine
            .get_raw("raw_users", &key_b, ReadOptions::default())?
            .is_none(),
        "user:2 delete from the WAL suffix is replayed"
    );

    println!(
        "recovered checkpoint LSN {} plus {} WAL suffix records; user:1 v{} = {}, user:3 v{} = {}",
        checkpoint.metadata.checkpoint_lsn.get(),
        recovery.wal.records,
        read_a.version.get(),
        text(read_a.value.as_bytes()),
        read_c.version.get(),
        text(read_c.value.as_bytes())
    );

    std::fs::remove_dir_all(&root).ok();
    Ok(())
}
