//! Page/file-oriented storage core for future database adapters.
//!
//! The object store exposes Redis-like values. This module is deliberately a
//! lower-level byte store: database adapters see files, offsets, byte ranges,
//! sync boundaries, and advisory locks.

use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Identifier for a logical database file inside a page store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(u64);

impl FileId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for FileId {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

/// Advisory lock modes used by page/file adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LockMode {
    Shared,
    Reserved,
    Pending,
    Exclusive,
}

/// Errors produced by page-store operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageStoreError {
    /// `offset + len` overflowed or does not fit this platform's address space.
    RangeTooLarge,
    /// Fewer bytes were available than requested. The unread part of the
    /// caller's buffer has already been zero-filled.
    ShortRead { available: u64 },
    /// The requested lock conflicts with another connection's lock.
    LockConflict { file: FileId, requested: LockMode },
}

pub type PageStoreResult<T> = Result<T, PageStoreError>;

/// Byte-addressable page/file store shared by future DB adapters.
pub trait MfsPageStore {
    /// Read exactly `buf.len()` bytes from `file` at `offset`.
    ///
    /// If the read reaches a hole or EOF, implementations must copy any
    /// available bytes, zero-fill the rest of `buf`, and return
    /// [`PageStoreError::ShortRead`]. This matches the contract a future SQLite
    /// VFS adapter needs for `xRead`.
    fn read_at(&self, file: FileId, offset: u64, buf: &mut [u8]) -> PageStoreResult<()>;

    /// Write all bytes at `offset`, growing the file and zero-filling gaps.
    fn write_at(&self, file: FileId, offset: u64, bytes: &[u8]) -> PageStoreResult<()>;

    /// Make prior writes durable. In-memory implementations may treat this as a
    /// no-op durability boundary.
    fn sync(&self, file: FileId) -> PageStoreResult<()>;

    /// Resize the file. Extending a file fills new bytes with zeroes.
    fn truncate(&self, file: FileId, len: u64) -> PageStoreResult<()>;
    fn file_size(&self, file: FileId) -> PageStoreResult<u64>;

    /// Acquire or upgrade an advisory file lock for this connection.
    fn lock(&self, file: FileId, mode: LockMode) -> PageStoreResult<()>;

    /// Release all advisory locks held by this connection for `file`.
    fn unlock(&self, file: FileId) -> PageStoreResult<()>;
}

/// In-memory implementation of [`MfsPageStore`].
///
/// Cloning this value preserves the same connection identity. Use
/// [`Self::connection`] to create another handle with a distinct lock owner.
#[derive(Clone)]
pub struct InMemoryPageStore {
    inner: Arc<Mutex<Inner>>,
    owner: u64,
}

impl InMemoryPageStore {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(_files: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                files: BTreeMap::new(),
                locks: BTreeMap::new(),
                next_owner: 2,
            })),
            owner: 1,
        }
    }

    pub fn connection(&self) -> Self {
        let mut inner = self.inner.lock();
        let owner = inner.next_owner;
        inner.next_owner = inner.next_owner.saturating_add(1).max(2);
        Self {
            inner: Arc::clone(&self.inner),
            owner,
        }
    }
}

impl Default for InMemoryPageStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MfsPageStore for InMemoryPageStore {
    fn read_at(&self, file: FileId, offset: u64, buf: &mut [u8]) -> PageStoreResult<()> {
        if buf.is_empty() {
            return Ok(());
        }

        let start = checked_pos(offset)?;
        let end = checked_end(offset, buf.len())?;
        buf.fill(0);

        let inner = self.inner.lock();
        let Some(bytes) = inner.files.get(&file) else {
            return Err(PageStoreError::ShortRead { available: 0 });
        };
        if start >= bytes.len() {
            return Err(PageStoreError::ShortRead { available: 0 });
        }

        let copy_end = end.min(bytes.len());
        let copied = copy_end - start;
        buf[..copied].copy_from_slice(&bytes[start..copy_end]);
        if copied < buf.len() {
            return Err(PageStoreError::ShortRead {
                available: copied as u64,
            });
        }
        Ok(())
    }

    fn write_at(&self, file: FileId, offset: u64, bytes: &[u8]) -> PageStoreResult<()> {
        if bytes.is_empty() {
            return Ok(());
        }

        let start = checked_pos(offset)?;
        let end = checked_end(offset, bytes.len())?;
        let mut inner = self.inner.lock();
        let file_bytes = inner.files.entry(file).or_default();
        if file_bytes.len() < end {
            file_bytes.resize(end, 0);
        }
        file_bytes[start..end].copy_from_slice(bytes);
        Ok(())
    }

    fn sync(&self, _file: FileId) -> PageStoreResult<()> {
        Ok(())
    }

    fn truncate(&self, file: FileId, len: u64) -> PageStoreResult<()> {
        let len = checked_pos(len)?;
        let mut inner = self.inner.lock();
        inner.files.entry(file).or_default().resize(len, 0);
        Ok(())
    }

    fn file_size(&self, file: FileId) -> PageStoreResult<u64> {
        let inner = self.inner.lock();
        Ok(inner.files.get(&file).map_or(0, |bytes| bytes.len() as u64))
    }

    fn lock(&self, file: FileId, mode: LockMode) -> PageStoreResult<()> {
        let mut inner = self.inner.lock();
        let owner = self.owner;
        let state = inner.locks.entry(file).or_default();
        let accepted = match mode {
            LockMode::Shared => state.lock_shared(owner),
            LockMode::Reserved => state.lock_reserved(owner),
            LockMode::Pending => state.lock_pending(owner),
            LockMode::Exclusive => state.lock_exclusive(owner),
        };
        if accepted {
            Ok(())
        } else {
            Err(PageStoreError::LockConflict {
                file,
                requested: mode,
            })
        }
    }

    fn unlock(&self, file: FileId) -> PageStoreResult<()> {
        let mut inner = self.inner.lock();
        let Some(state) = inner.locks.get_mut(&file) else {
            return Ok(());
        };
        state.unlock(self.owner);
        if state.is_empty() {
            inner.locks.remove(&file);
        }
        Ok(())
    }
}

struct Inner {
    files: BTreeMap<FileId, Vec<u8>>,
    locks: BTreeMap<FileId, LockState>,
    next_owner: u64,
}

#[derive(Default)]
struct LockState {
    shared: BTreeSet<u64>,
    reserved: Option<u64>,
    pending: Option<u64>,
    exclusive: Option<u64>,
}

impl LockState {
    fn lock_shared(&mut self, owner: u64) -> bool {
        if self.exclusive.is_some_and(|held| held != owner)
            || self.pending.is_some_and(|held| held != owner)
        {
            return false;
        }
        self.shared.insert(owner);
        true
    }

    fn lock_reserved(&mut self, owner: u64) -> bool {
        if self.exclusive.is_some_and(|held| held != owner)
            || self.reserved.is_some_and(|held| held != owner)
        {
            return false;
        }
        self.shared.insert(owner);
        self.reserved = Some(owner);
        true
    }

    fn lock_pending(&mut self, owner: u64) -> bool {
        if self.exclusive.is_some_and(|held| held != owner)
            || self.pending.is_some_and(|held| held != owner)
            || self.reserved.is_some_and(|held| held != owner)
        {
            return false;
        }
        self.shared.insert(owner);
        self.reserved = Some(owner);
        self.pending = Some(owner);
        true
    }

    fn lock_exclusive(&mut self, owner: u64) -> bool {
        if self.exclusive.is_some_and(|held| held != owner)
            || self.pending.is_some_and(|held| held != owner)
            || self.reserved.is_some_and(|held| held != owner)
            || self.shared.iter().any(|held| *held != owner)
        {
            return false;
        }
        self.shared.insert(owner);
        self.reserved = Some(owner);
        self.pending = Some(owner);
        self.exclusive = Some(owner);
        true
    }

    fn unlock(&mut self, owner: u64) {
        self.shared.remove(&owner);
        if self.reserved == Some(owner) {
            self.reserved = None;
        }
        if self.pending == Some(owner) {
            self.pending = None;
        }
        if self.exclusive == Some(owner) {
            self.exclusive = None;
        }
    }

    fn is_empty(&self) -> bool {
        self.shared.is_empty()
            && self.reserved.is_none()
            && self.pending.is_none()
            && self.exclusive.is_none()
    }
}

fn checked_pos(pos: u64) -> PageStoreResult<usize> {
    usize::try_from(pos).map_err(|_| PageStoreError::RangeTooLarge)
}

fn checked_end(offset: u64, len: usize) -> PageStoreResult<usize> {
    let len = u64::try_from(len).map_err(|_| PageStoreError::RangeTooLarge)?;
    let end = offset
        .checked_add(len)
        .ok_or(PageStoreError::RangeTooLarge)?;
    checked_pos(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_grows_file_and_reads_zero_filled_holes() {
        let store = InMemoryPageStore::new();
        let file = FileId::new(7);

        let mut missing = [0xff; 4];
        assert_eq!(
            store.read_at(file, 0, &mut missing),
            Err(PageStoreError::ShortRead { available: 0 })
        );
        assert_eq!(missing, [0; 4]);

        store.write_at(file, 4, b"abc").unwrap();
        assert_eq!(store.file_size(file), Ok(7));

        let mut full = [0xff; 7];
        assert_eq!(store.read_at(file, 0, &mut full), Ok(()));
        assert_eq!(&full, b"\0\0\0\0abc");

        let mut short = [0xff; 5];
        assert_eq!(
            store.read_at(file, 5, &mut short),
            Err(PageStoreError::ShortRead { available: 2 })
        );
        assert_eq!(&short, b"bc\0\0\0");
    }

    #[test]
    fn truncate_shrinks_and_extends_with_zeroes() {
        let store = InMemoryPageStore::new();
        let file = FileId::new(1);

        store.write_at(file, 0, b"abcdef").unwrap();
        store.truncate(file, 3).unwrap();
        assert_eq!(store.file_size(file), Ok(3));
        let mut shrunk = [0xff; 5];
        assert_eq!(
            store.read_at(file, 0, &mut shrunk),
            Err(PageStoreError::ShortRead { available: 3 })
        );
        assert_eq!(&shrunk, b"abc\0\0");

        store.truncate(file, 6).unwrap();
        assert_eq!(store.file_size(file), Ok(6));
        let mut extended = [0xff; 6];
        assert_eq!(store.read_at(file, 0, &mut extended), Ok(()));
        assert_eq!(&extended, b"abc\0\0\0");
    }

    #[test]
    fn sync_and_zero_length_operations_are_noops() {
        let store = InMemoryPageStore::new();
        let file = FileId::new(3);
        let mut empty = [];

        assert_eq!(store.read_at(file, u64::MAX, &mut empty), Ok(()));
        assert_eq!(store.write_at(file, 1024, &[]), Ok(()));
        assert_eq!(store.file_size(file), Ok(0));
        assert_eq!(store.sync(file), Ok(()));
    }

    #[test]
    fn locks_enforce_connection_compatibility() {
        let first = InMemoryPageStore::new();
        let second = first.connection();
        let file = FileId::new(9);

        assert_eq!(first.lock(file, LockMode::Shared), Ok(()));
        assert_eq!(second.lock(file, LockMode::Shared), Ok(()));
        assert_eq!(
            second.lock(file, LockMode::Exclusive),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Exclusive,
            })
        );

        assert_eq!(first.unlock(file), Ok(()));
        assert_eq!(second.lock(file, LockMode::Exclusive), Ok(()));
        assert_eq!(
            first.lock(file, LockMode::Shared),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Shared,
            })
        );

        assert_eq!(second.unlock(file), Ok(()));
        assert_eq!(first.lock(file, LockMode::Reserved), Ok(()));
        assert_eq!(
            second.lock(file, LockMode::Reserved),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Reserved,
            })
        );
    }

    #[test]
    fn pending_lock_blocks_new_shared_readers() {
        let first = InMemoryPageStore::new();
        let second = first.connection();
        let third = first.connection();
        let file = FileId::new(10);

        assert_eq!(first.lock(file, LockMode::Shared), Ok(()));
        assert_eq!(second.lock(file, LockMode::Shared), Ok(()));
        assert_eq!(first.lock(file, LockMode::Pending), Ok(()));
        assert_eq!(
            third.lock(file, LockMode::Shared),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Shared,
            })
        );
        assert_eq!(
            first.lock(file, LockMode::Exclusive),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Exclusive,
            })
        );

        assert_eq!(second.unlock(file), Ok(()));
        assert_eq!(first.lock(file, LockMode::Exclusive), Ok(()));
    }

    #[test]
    fn clone_preserves_owner_connection_creates_distinct_owner() {
        let original = InMemoryPageStore::new();
        let clone = original.clone();
        let other = original.connection();
        let file = FileId::new(11);

        assert_eq!(original.lock(file, LockMode::Exclusive), Ok(()));
        assert_eq!(clone.lock(file, LockMode::Shared), Ok(()));
        assert_eq!(
            other.lock(file, LockMode::Shared),
            Err(PageStoreError::LockConflict {
                file,
                requested: LockMode::Shared,
            })
        );

        assert_eq!(clone.unlock(file), Ok(()));
        assert_eq!(other.lock(file, LockMode::Shared), Ok(()));
    }

    #[test]
    fn range_overflow_is_rejected() {
        let store = InMemoryPageStore::new();
        let file = FileId::new(1);
        let mut byte = [0];

        assert_eq!(
            store.read_at(file, u64::MAX, &mut byte),
            Err(PageStoreError::RangeTooLarge)
        );
        assert_eq!(
            store.write_at(file, u64::MAX, &[1]),
            Err(PageStoreError::RangeTooLarge)
        );
    }
}
