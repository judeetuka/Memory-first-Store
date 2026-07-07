# SQLite VFS: Page Store and VFS Adapter

The page store and VFS adapter provide a SQLite-shaped I/O surface over
in-memory (or future on-disk) byte storage. This is the lowest-level
compat layer: database adapters see files, offsets, byte ranges, sync
boundaries, and advisory locks.

## Architecture

```
MfsPageVfs (SQLite VFS shape)
    |
    v
MfsPageStore (byte-addressable file trait)
    |
    v
InMemoryPageStore (in-memory implementation)
```

`MfsPageStore` is the trait. `InMemoryPageStore` is the reference
implementation. `MfsPageVfs` wraps a page store and adds a name-to-FileId
namespace, matching the shape of SQLite's VFS file operations.

## MfsPageStore trait

The core abstraction: byte-addressable files with advisory locking.

```rust
pub trait MfsPageStore {
    fn read_at(&self, file: FileId, offset: u64, buf: &mut [u8]) -> PageStoreResult<()>;
    fn write_at(&self, file: FileId, offset: u64, bytes: &[u8]) -> PageStoreResult<()>;
    fn sync(&self, file: FileId) -> PageStoreResult<()>;
    fn truncate(&self, file: FileId, len: u64) -> PageStoreResult<()>;
    fn file_size(&self, file: FileId) -> PageStoreResult<u64>;
    fn lock(&self, file: FileId, mode: LockMode) -> PageStoreResult<()>;
    fn unlock(&self, file: FileId) -> PageStoreResult<()>;
}
```

### read_at

Reads exactly `buf.len()` bytes from `file` at `offset`. If the read
reaches a hole or EOF, implementations copy any available bytes,
zero-fill the rest of `buf`, and return `PageStoreError::ShortRead {
available }`. This matches the contract SQLite's `xRead` expects.

### write_at

Writes all bytes at `offset`, growing the file and zero-filling gaps.

### sync

Makes prior writes durable. In-memory implementations treat this as a
no-op durability boundary.

### truncate

Resizes the file. Extending fills new bytes with zeroes.

### lock / unlock

Advisory file locks with four modes matching SQLite's locking protocol:

```rust
pub enum LockMode {
    Shared,     // Multiple readers, no writers.
    Reserved,   // Single writer intent, readers still allowed.
    Pending,    // Waiting for exclusive. New shared readers blocked.
    Exclusive,  // Single writer, no readers.
}
```

Locks are scoped to a connection (owner ID). Cloning an
`InMemoryPageStore` preserves the same owner. Use `connection()` to
create a distinct owner.

## InMemoryPageStore

```rust
use mfs_compat::page_store::{InMemoryPageStore, FileId, LockMode};

let store = InMemoryPageStore::new();

// Write to a file.
let file = FileId::new(1);
store.write_at(file, 0, b"hello")?;

// Read back.
let mut buf = [0u8; 5];
store.read_at(file, 0, &mut buf)?;
assert_eq!(&buf, b"hello");

// File size.
assert_eq!(store.file_size(file)?, 5);

// Truncate.
store.truncate(file, 3)?;
assert_eq!(store.file_size(file)?, 3);

// Sync (no-op for in-memory).
store.sync(file)?;
```

### Connections and locking

```rust
let first = InMemoryPageStore::new();
let second = first.connection(); // distinct lock owner

let file = FileId::new(1);

// Both can hold shared locks.
first.lock(file, LockMode::Shared)?;
second.lock(file, LockMode::Shared)?;

// Exclusive lock conflicts with the other connection's shared lock.
assert!(second.lock(file, LockMode::Exclusive).is_err());

// Release and re-acquire.
first.unlock(file)?;
second.lock(file, LockMode::Exclusive)?;
```

### Clone vs. connection

- `store.clone()` creates a handle with the **same** lock owner. Both
  handles share locks.
- `store.connection()` creates a handle with a **new** lock owner. Locks
  are independent.

## MfsPageVfs

Wraps a `MfsPageStore` and adds a name-to-FileId namespace. Method names
match SQLite's VFS operations (`x_open`, `x_read`, `x_write`, etc.).

```rust
use mfs_compat::page_store::InMemoryPageStore;
use mfs_compat::page_vfs::MfsPageVfs;

let store = InMemoryPageStore::new();
let vfs = MfsPageVfs::new(store.clone());

// Open a file by name.
let file = vfs.x_open("main.db")?;

// Write and read.
vfs.x_write(&file, 0, b"sqlite")?;
let mut buf = [0u8; 6];
vfs.x_read(&file, 0, &mut buf)?;
assert_eq!(&buf, b"sqlite");

// File size.
assert_eq!(vfs.x_file_size(&file)?, 6);

// Sync.
vfs.x_sync(&file)?;

// Truncate.
vfs.x_truncate(&file, 3)?;
assert_eq!(vfs.x_file_size(&file)?, 3);

// Locking.
vfs.x_lock(&file, LockMode::Shared)?;
vfs.x_unlock(&file)?;
```

### Connections

```rust
let first = MfsPageVfs::new(store.clone());
let second = first.connection(store.connection());

// Same name maps to the same FileId across connections.
let file_a = first.x_open("main.db")?;
let file_b = second.x_open("main.db")?;
assert_eq!(file_a.file_id(), file_b.file_id());

// Writes from one connection are visible from the other.
first.x_write(&file_a, 0, b"data")?;
let mut buf = [0u8; 4];
second.x_read(&file_b, 0, &mut buf)?;
assert_eq!(&buf, b"data");

// But locks are connection-scoped.
first.x_lock(&file_a, LockMode::Shared)?;
assert!(second.x_lock(&file_b, LockMode::Exclusive).is_err());
```

### PageVfsFile

```rust
pub struct PageVfsFile {
    // opaque
}

impl PageVfsFile {
    pub fn file_id(&self) -> FileId;
    pub fn name(&self) -> &str;
}
```

## Error handling

```rust
pub enum PageStoreError {
    RangeTooLarge,
    ShortRead { available: u64 },
    LockConflict { file: FileId, requested: LockMode },
}
```

- `RangeTooLarge`: offset + len overflowed or doesn't fit the platform's
  address space.
- `ShortRead`: fewer bytes available than requested. The unread part of
  the buffer is zero-filled.
- `LockConflict`: the requested lock conflicts with another connection's
  lock.

## Example: multi-connection SQLite-shaped storage

```rust
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
```

Run with:

```bash
cargo run -p mfs-compat --release --example sqlite_vfs_page_adapter
```

## When to use this

- You're building a database adapter that needs SQLite-shaped file I/O.
- You want to control the storage layer yourself (in-memory now, on-disk
  later).
- You need advisory locking with SQLite's four-mode protocol.

## When to skip it

- You need key-value or document storage. Use `MfsObjectStore` or
  `SchemaStore`.
- You need a full SQLite integration. This is a testable adapter surface,
  not a registered C VFS yet.

## Cross-links

- [Object Store](object-store.md) for Redis-like key-value storage.
- [Mutable Object Store](mutable-object-store.md) for growable storage with TTL.
- [Schema Store](schema-store.md) for schema-validated documents.
- [mfs-core](../core/overview.md) for the underlying cache primitives.
