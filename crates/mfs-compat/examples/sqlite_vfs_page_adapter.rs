use mfs_compat::page_store::{InMemoryPageStore, LockMode, PageStoreResult};
use mfs_compat::page_vfs::MfsPageVfs;

fn main() -> PageStoreResult<()> {
    let store = InMemoryPageStore::new();
    let first = MfsPageVfs::new(store.clone());
    let second = first.connection(store.connection());

    let first_file = first.x_open("main.db")?;
    let second_file = second.x_open("main.db")?;
    assert_eq!(first_file.file_id(), second_file.file_id());

    first.x_write(&first_file, 0, b"sqlite")?;
    first.x_sync(&first_file)?;

    let mut buf = [0; 6];
    second.x_read(&second_file, 0, &mut buf)?;
    assert_eq!(&buf, b"sqlite");

    first.x_lock(&first_file, LockMode::Shared)?;
    first.x_unlock(&first_file)?;

    println!(
        "sqlite-shaped page adapter round trip: file={} size={}",
        first_file.name(),
        first.x_file_size(&first_file)?
    );
    Ok(())
}
