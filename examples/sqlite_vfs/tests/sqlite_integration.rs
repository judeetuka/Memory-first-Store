use mfs_sqlite_vfs_example::{register_mfs_vfs, unique_vfs_name};
use rusqlite::{Connection, OpenFlags};

fn open_conn() -> Connection {
    let vfs = unique_vfs_name("mfs-test");
    let _logger = register_mfs_vfs(&vfs).expect("register mfs vfs");
    let conn = Connection::open_with_flags_and_vfs(
        "main.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs.as_str(),
    )
    .expect("open sqlite connection with mfs vfs");
    conn.execute_batch("PRAGMA journal_mode = MEMORY;").unwrap();
    conn
}

#[test]
fn create_insert_select_round_trip() {
    let conn = open_conn();
    conn.execute_batch(
        "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO kv (k, v) VALUES ('a', 'one'), ('b', 'two');",
    )
    .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
        .unwrap();
    let value: String = conn
        .query_row("SELECT v FROM kv WHERE k = 'b'", [], |row| row.get(0))
        .unwrap();

    assert_eq!(count, 2);
    assert_eq!(value, "two");
}

#[test]
fn transaction_commit_and_rollback_work() {
    let conn = open_conn();
    conn.execute_batch("CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT NOT NULL);")
        .unwrap();

    conn.execute_batch("BEGIN; INSERT INTO events (label) VALUES ('committed'); COMMIT;")
        .unwrap();
    conn.execute_batch("BEGIN; INSERT INTO events (label) VALUES ('rolled-back'); ROLLBACK;")
        .unwrap();

    let labels = conn
        .prepare("SELECT label FROM events ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(labels, vec!["committed".to_string()]);
}

#[test]
fn blob_io_uses_sqlite_pager() {
    let conn = open_conn();
    conn.execute_batch("CREATE TABLE blobs (data BLOB NOT NULL);")
        .unwrap();
    conn.execute("INSERT INTO blobs (data) VALUES (zeroblob(8192))", [])
        .unwrap();
    let rowid = conn.last_insert_rowid();

    let mut blob = conn
        .blob_open(rusqlite::MAIN_DB, "blobs", "data", rowid, false)
        .unwrap();
    blob.write_at(b"hello", 4096).unwrap();
    drop(blob);

    let data: Vec<u8> = conn
        .query_row("SELECT data FROM blobs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(&data[4096..4101], b"hello");
}
