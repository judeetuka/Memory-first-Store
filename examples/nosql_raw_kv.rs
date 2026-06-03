//! Raw key/value operations on the NoSQL engine.
//!
//! Run with `cargo run --release --example nosql_raw_kv`.

use mfs_db::engine::{
    DocumentVersion, EngineConfig, EngineError, NoSqlEngine, RawKey, RawValue, ReadOptions,
    WriteOptions,
};

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let engine = NoSqlEngine::open_memory(EngineConfig {
        raw_initial_capacity: 16,
        ..EngineConfig::default()
    })?;
    let collection_id = engine.create_raw_collection("sessions")?;
    println!(
        "created raw collection `sessions` as id {}",
        collection_id.get()
    );

    let key = RawKey::from(&b"session:42"[..]);
    let first = engine.put_raw(
        "sessions",
        key.clone(),
        RawValue::from(&b"state=created"[..]),
        WriteOptions::default(),
    )?;
    assert_eq!(first.version, DocumentVersion::new(1));

    let read = engine
        .get_raw("sessions", &key, ReadOptions::default())?
        .expect("value is present after put");
    assert_eq!(read.version, first.version);
    println!(
        "put/get round trip: v{} = {}",
        read.version.get(),
        text(read.value.as_bytes())
    );

    let second = engine.compare_put_raw(
        "sessions",
        key.clone(),
        RawValue::from(&b"state=active"[..]),
        first.version,
    )?;
    assert_eq!(second.version, DocumentVersion::new(2));
    println!("expected-version put advanced to v{}", second.version.get());

    match engine.compare_put_raw(
        "sessions",
        key.clone(),
        RawValue::from(&b"state=stale"[..]),
        first.version,
    ) {
        Err(EngineError::Conflict {
            expected, actual, ..
        }) => println!(
            "stale write rejected: expected v{}, actual v{}",
            expected.get(),
            actual.get()
        ),
        other => panic!("expected conflict from stale version, got {other:?}"),
    }

    let delete = engine.delete_raw(
        "sessions",
        key.clone(),
        WriteOptions {
            expected_version: Some(second.version),
            ..WriteOptions::default()
        },
    )?;
    assert_eq!(delete.version, DocumentVersion::new(3));
    assert!(
        engine
            .get_raw("sessions", &key, ReadOptions::default())?
            .is_none(),
        "deleted keys read as absent"
    );
    println!(
        "delete advanced to v{} and tombstoned the key",
        delete.version.get()
    );

    Ok(())
}
