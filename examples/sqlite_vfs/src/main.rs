use mfs_sqlite_vfs_example::{register_mfs_vfs, unique_vfs_name};
use rusqlite::{Connection, OpenFlags};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vfs = unique_vfs_name("mfs-example");
    let _logger =
        register_mfs_vfs(&vfs).map_err(|code| format!("sqlite vfs register failed: {code}"))?;

    let conn = Connection::open_with_flags_and_vfs(
        "main.db",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        vfs.as_str(),
    )?;

    conn.execute_batch(
        "PRAGMA journal_mode = MEMORY;
         CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
         INSERT INTO kv (k, v) VALUES ('hello', 'world'), ('mfs', 'sqlite');",
    )?;

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))?;
    let value: String = conn.query_row("SELECT v FROM kv WHERE k = 'mfs'", [], |row| row.get(0))?;

    println!("rows={count}, mfs={value}");
    Ok(())
}
