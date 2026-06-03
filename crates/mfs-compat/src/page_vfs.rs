//! Safe SQLite-shaped adapter over [`crate::page_store`].
//!
//! This module mirrors the SQLite VFS file methods in pure Rust. It does not
//! register a C VFS yet; it gives us a testable adapter surface first.

use crate::page_store::{FileId, LockMode, MfsPageStore, PageStoreResult};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

pub type PageVfsResult<T> = PageStoreResult<T>;

/// Handle returned by [`MfsPageVfs::x_open`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageVfsFile {
    file: FileId,
    name: String,
}

impl PageVfsFile {
    pub fn file_id(&self) -> FileId {
        self.file
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Pure Rust adapter matching SQLite's VFS operation shape.
#[derive(Clone)]
pub struct MfsPageVfs<S> {
    store: S,
    namespace: Arc<Mutex<Namespace>>,
}

impl<S> MfsPageVfs<S>
where
    S: MfsPageStore + Clone,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            namespace: Arc::new(Mutex::new(Namespace::default())),
        }
    }

    /// Create another VFS connection sharing file-name mappings.
    ///
    /// Pass a page-store handle with the desired lock owner. For
    /// [`crate::page_store::InMemoryPageStore`], use `store.connection()` for a
    /// distinct connection and `store.clone()` for the same owner.
    pub fn connection(&self, store: S) -> Self {
        Self {
            store,
            namespace: Arc::clone(&self.namespace),
        }
    }

    /// Open or create a logical SQLite file by name.
    pub fn x_open(&self, name: impl AsRef<str>) -> PageVfsResult<PageVfsFile> {
        let name = name.as_ref();
        let mut namespace = self.namespace.lock();
        let file = namespace.open(name);
        Ok(PageVfsFile {
            file,
            name: name.to_string(),
        })
    }

    pub fn x_read(&self, file: &PageVfsFile, offset: u64, buf: &mut [u8]) -> PageVfsResult<()> {
        self.store.read_at(file.file, offset, buf)
    }

    pub fn x_write(&self, file: &PageVfsFile, offset: u64, bytes: &[u8]) -> PageVfsResult<()> {
        self.store.write_at(file.file, offset, bytes)
    }

    pub fn x_sync(&self, file: &PageVfsFile) -> PageVfsResult<()> {
        self.store.sync(file.file)
    }

    pub fn x_file_size(&self, file: &PageVfsFile) -> PageVfsResult<u64> {
        self.store.file_size(file.file)
    }

    pub fn x_truncate(&self, file: &PageVfsFile, len: u64) -> PageVfsResult<()> {
        self.store.truncate(file.file, len)
    }

    pub fn x_lock(&self, file: &PageVfsFile, mode: LockMode) -> PageVfsResult<()> {
        self.store.lock(file.file, mode)
    }

    pub fn x_unlock(&self, file: &PageVfsFile) -> PageVfsResult<()> {
        self.store.unlock(file.file)
    }
}

#[derive(Default)]
struct Namespace {
    files: BTreeMap<String, FileId>,
    next_file: u64,
}

impl Namespace {
    fn open(&mut self, name: &str) -> FileId {
        if let Some(file) = self.files.get(name) {
            return *file;
        }
        let file = FileId::new(self.next_file);
        self.next_file = self.next_file.saturating_add(1);
        self.files.insert(name.to_string(), file);
        file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::{InMemoryPageStore, PageStoreError};

    #[test]
    fn open_write_read_sync_and_size_round_trip() {
        let store = InMemoryPageStore::new();
        let vfs = MfsPageVfs::new(store);
        let file = vfs.x_open("main.db").unwrap();

        assert_eq!(file.name(), "main.db");
        assert_eq!(vfs.x_file_size(&file), Ok(0));
        assert_eq!(vfs.x_write(&file, 4, b"sqlite"), Ok(()));
        assert_eq!(vfs.x_file_size(&file), Ok(10));

        let mut buf = [0xff; 10];
        assert_eq!(vfs.x_read(&file, 0, &mut buf), Ok(()));
        assert_eq!(&buf, b"\0\0\0\0sqlite");
        assert_eq!(vfs.x_sync(&file), Ok(()));
    }

    #[test]
    fn same_name_reopens_same_file_across_connections() {
        let store = InMemoryPageStore::new();
        let first = MfsPageVfs::new(store.clone());
        let second = first.connection(store.connection());

        let first_file = first.x_open("main.db").unwrap();
        let second_file = second.x_open("main.db").unwrap();
        assert_eq!(first_file.file_id(), second_file.file_id());

        assert_eq!(first.x_write(&first_file, 0, b"page"), Ok(()));
        let mut buf = [0; 4];
        assert_eq!(second.x_read(&second_file, 0, &mut buf), Ok(()));
        assert_eq!(&buf, b"page");
    }

    #[test]
    fn different_names_open_distinct_files() {
        let store = InMemoryPageStore::new();
        let vfs = MfsPageVfs::new(store);

        let main = vfs.x_open("main.db").unwrap();
        let journal = vfs.x_open("main.db-journal").unwrap();
        assert_ne!(main.file_id(), journal.file_id());

        assert_eq!(vfs.x_write(&main, 0, b"main"), Ok(()));
        assert_eq!(vfs.x_write(&journal, 0, b"jnl"), Ok(()));

        let mut main_buf = [0; 4];
        let mut journal_buf = [0; 3];
        assert_eq!(vfs.x_read(&main, 0, &mut main_buf), Ok(()));
        assert_eq!(vfs.x_read(&journal, 0, &mut journal_buf), Ok(()));
        assert_eq!(&main_buf, b"main");
        assert_eq!(&journal_buf, b"jnl");
    }

    #[test]
    fn short_read_zero_fills_through_adapter() {
        let store = InMemoryPageStore::new();
        let vfs = MfsPageVfs::new(store);
        let file = vfs.x_open("main.db").unwrap();

        assert_eq!(vfs.x_write(&file, 0, b"abc"), Ok(()));
        let mut buf = [0xff; 6];
        assert_eq!(
            vfs.x_read(&file, 0, &mut buf),
            Err(PageStoreError::ShortRead { available: 3 })
        );
        assert_eq!(&buf, b"abc\0\0\0");
    }

    #[test]
    fn truncate_routes_to_page_store() {
        let store = InMemoryPageStore::new();
        let vfs = MfsPageVfs::new(store);
        let file = vfs.x_open("main.db").unwrap();

        assert_eq!(vfs.x_write(&file, 0, b"abcdef"), Ok(()));
        assert_eq!(vfs.x_truncate(&file, 3), Ok(()));
        assert_eq!(vfs.x_file_size(&file), Ok(3));

        let mut buf = [0xff; 5];
        assert_eq!(
            vfs.x_read(&file, 0, &mut buf),
            Err(PageStoreError::ShortRead { available: 3 })
        );
        assert_eq!(&buf, b"abc\0\0");
    }

    #[test]
    fn locks_are_connection_scoped() {
        let store = InMemoryPageStore::new();
        let first = MfsPageVfs::new(store.clone());
        let second = first.connection(store.connection());
        let file_a = first.x_open("main.db").unwrap();
        let file_b = second.x_open("main.db").unwrap();

        assert_eq!(first.x_lock(&file_a, LockMode::Shared), Ok(()));
        assert_eq!(second.x_lock(&file_b, LockMode::Shared), Ok(()));
        assert_eq!(
            second.x_lock(&file_b, LockMode::Exclusive),
            Err(PageStoreError::LockConflict {
                file: file_b.file_id(),
                requested: LockMode::Exclusive,
            })
        );

        assert_eq!(first.x_unlock(&file_a), Ok(()));
        assert_eq!(second.x_lock(&file_b, LockMode::Exclusive), Ok(()));
    }
}
