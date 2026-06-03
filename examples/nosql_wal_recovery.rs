//! Raw NoSQL engine WAL recovery.
//!
//! Run with `cargo run --release --example nosql_wal_recovery`.

use mfs_db::engine::{
    DocumentVersion, EngineConfig, Lsn, NoSqlEngine, RawKey, RawValue, RawWalSegmentWriter,
    ReadOptions, replay_raw_wal,
};

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let wal_path = std::env::temp_dir().join("mfs_nosql_wal_recovery.wal");
    std::fs::remove_file(&wal_path).ok();

    let key_a = RawKey::from(&b"user:1"[..]);
    let key_b = RawKey::from(&b"user:2"[..]);

    {
        let mut wal = RawWalSegmentWriter::open(&wal_path)?;
        assert_eq!(
            wal.append_put("raw_users", &key_a, &RawValue::from(&b"ada"[..]))?,
            Lsn::new(1)
        );
        assert_eq!(
            wal.append_put("raw_users", &key_b, &RawValue::from(&b"grace"[..]))?,
            Lsn::new(2)
        );
        assert_eq!(wal.append_delete("raw_users", &key_a)?, Lsn::new(3));
        wal.sync_now()?;
        println!("wrote and synced WAL at {}", wal.path().display());
    }

    let recovered = NoSqlEngine::open_memory(EngineConfig {
        raw_initial_capacity: 16,
        ..EngineConfig::default()
    })?;
    let stats = replay_raw_wal(&wal_path, &recovered)?;
    assert_eq!(stats.records, 3);
    assert_eq!(stats.last_lsn, Lsn::new(3));

    assert!(
        recovered
            .get_raw("raw_users", &key_a, ReadOptions::default())?
            .is_none(),
        "deleted key should replay as absent"
    );
    let read_b = recovered
        .get_raw("raw_users", &key_b, ReadOptions::default())?
        .expect("live key is recovered");
    assert_eq!(read_b.version, DocumentVersion::new(1));
    assert_eq!(read_b.value.as_bytes(), b"grace");
    println!(
        "replayed {} records through LSN {}; recovered user:2 v{} = {}",
        stats.records,
        stats.last_lsn.get(),
        read_b.version.get(),
        text(read_b.value.as_bytes())
    );

    std::fs::remove_file(&wal_path).ok();
    Ok(())
}
