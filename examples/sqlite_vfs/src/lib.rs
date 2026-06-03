use mfs_compat::page_store::{FileId, InMemoryPageStore, LockMode, MfsPageStore, PageStoreError};
use parking_lot::Mutex;
use sqlite_plugin::flags::{AccessFlags, CreateMode, LockLevel, OpenMode, OpenOpts};
use sqlite_plugin::logger::SqliteLogger;
use sqlite_plugin::vars;
use sqlite_plugin::vfs::{RegisterOpts, Vfs, VfsHandle, VfsResult, register_static};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct MfsSqliteHandle {
    file: FileId,
    path: String,
    readonly: bool,
    in_memory: bool,
    delete_on_close: bool,
    current_lock: LockLevel,
    store: InMemoryPageStore,
}

impl VfsHandle for MfsSqliteHandle {
    fn readonly(&self) -> bool {
        self.readonly
    }

    fn in_memory(&self) -> bool {
        self.in_memory
    }
}

#[derive(Default)]
struct Namespace {
    files: BTreeMap<String, FileId>,
    next_file: u64,
    next_temp: u64,
}

impl Namespace {
    fn open(&mut self, path: Option<&str>) -> (String, FileId, bool) {
        let (path, in_memory) = match path {
            Some(path) => (path.to_string(), path == ":memory:"),
            None => {
                let temp = self.next_temp;
                self.next_temp = self.next_temp.saturating_add(1);
                (format!("mfs-temp-{temp}"), true)
            }
        };

        if let Some(file) = self.files.get(&path) {
            return (path, *file, in_memory);
        }

        let file = FileId::new(self.next_file);
        self.next_file = self.next_file.saturating_add(1).max(1);
        self.files.insert(path.clone(), file);
        (path, file, in_memory)
    }
}

pub struct MfsSqliteVfs {
    store: InMemoryPageStore,
    namespace: Mutex<Namespace>,
}

impl MfsSqliteVfs {
    pub fn new() -> Self {
        Self {
            store: InMemoryPageStore::new(),
            namespace: Mutex::new(Namespace::default()),
        }
    }
}

impl Default for MfsSqliteVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for MfsSqliteVfs {
    type Handle = MfsSqliteHandle;

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        let mut namespace = self.namespace.lock();
        let mode = opts.mode();
        let create = matches!(
            mode,
            OpenMode::ReadWrite {
                create: CreateMode::Create | CreateMode::MustCreate
            }
        );
        let exists = path
            .and_then(|path| namespace.files.get(path).copied())
            .is_some();

        if !create && !exists && !mode.is_readonly() {
            return Err(vars::SQLITE_CANTOPEN);
        }
        if mode.must_create() && exists {
            return Err(vars::SQLITE_CANTOPEN);
        }

        let (path, file, in_memory) = namespace.open(path);
        Ok(MfsSqliteHandle {
            file,
            path,
            readonly: mode.is_readonly(),
            in_memory,
            delete_on_close: opts.delete_on_close(),
            current_lock: LockLevel::Unlocked,
            store: self.store.connection(),
        })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        let file = self.namespace.lock().files.remove(path);
        if let Some(file) = file {
            self.store.truncate(file, 0).map_err(map_write_error)?;
        }
        Ok(())
    }

    fn access(&self, path: &str, _flags: AccessFlags) -> VfsResult<bool> {
        Ok(self.namespace.lock().files.contains_key(path))
    }

    fn file_size(&self, handle: &mut Self::Handle) -> VfsResult<usize> {
        let len = handle
            .store
            .file_size(handle.file)
            .map_err(map_read_error)?;
        usize::try_from(len).map_err(|_| vars::SQLITE_IOERR_FSTAT)
    }

    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        handle
            .store
            .truncate(handle.file, size as u64)
            .map_err(map_write_error)
    }

    fn write(&self, handle: &mut Self::Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        if handle.readonly {
            return Err(vars::SQLITE_READONLY);
        }
        handle
            .store
            .write_at(handle.file, offset as u64, data)
            .map_err(map_write_error)?;
        Ok(data.len())
    }

    fn read(&self, handle: &mut Self::Handle, offset: usize, data: &mut [u8]) -> VfsResult<usize> {
        match handle.store.read_at(handle.file, offset as u64, data) {
            Ok(()) => Ok(data.len()),
            Err(PageStoreError::ShortRead { available }) => {
                usize::try_from(available).map_err(|_| vars::SQLITE_IOERR_READ)
            }
            Err(error) => Err(map_read_error(error)),
        }
    }

    fn lock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        if level <= handle.current_lock {
            return Ok(());
        }
        let mode = sqlite_to_mfs_lock(level)?;
        handle
            .store
            .lock(handle.file, mode)
            .map_err(map_lock_error)?;
        handle.current_lock = level;
        Ok(())
    }

    fn unlock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        if level >= handle.current_lock {
            return Ok(());
        }
        handle.store.unlock(handle.file).map_err(map_lock_error)?;
        if level != LockLevel::Unlocked {
            let mode = sqlite_to_mfs_lock(level)?;
            handle
                .store
                .lock(handle.file, mode)
                .map_err(map_lock_error)?;
        }
        handle.current_lock = level;
        Ok(())
    }

    fn check_reserved_lock(&self, handle: &mut Self::Handle) -> VfsResult<bool> {
        Ok(matches!(
            handle.current_lock,
            LockLevel::Reserved | LockLevel::Pending | LockLevel::Exclusive
        ))
    }

    fn sync(&self, handle: &mut Self::Handle) -> VfsResult<()> {
        handle.store.sync(handle.file).map_err(map_write_error)
    }

    fn close(&self, handle: Self::Handle) -> VfsResult<()> {
        if handle.delete_on_close {
            self.delete(&handle.path)?;
        }
        Ok(())
    }
}

pub fn register_mfs_vfs(name: &str) -> VfsResult<SqliteLogger> {
    let name = CString::new(name).map_err(|_| vars::SQLITE_MISUSE)?;
    register_static(
        name,
        MfsSqliteVfs::new(),
        RegisterOpts {
            make_default: false,
        },
    )
}

pub fn unique_vfs_name(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}

fn sqlite_to_mfs_lock(level: LockLevel) -> VfsResult<LockMode> {
    match level {
        LockLevel::Unlocked => Err(vars::SQLITE_IOERR_UNLOCK),
        LockLevel::Shared => Ok(LockMode::Shared),
        LockLevel::Reserved => Ok(LockMode::Reserved),
        LockLevel::Pending => Ok(LockMode::Pending),
        LockLevel::Exclusive => Ok(LockMode::Exclusive),
    }
}

fn map_read_error(error: PageStoreError) -> i32 {
    match error {
        PageStoreError::ShortRead { .. } => vars::SQLITE_IOERR_SHORT_READ,
        PageStoreError::RangeTooLarge => vars::SQLITE_IOERR_READ,
        PageStoreError::LockConflict { .. } => vars::SQLITE_BUSY,
    }
}

fn map_write_error(error: PageStoreError) -> i32 {
    match error {
        PageStoreError::ShortRead { .. } | PageStoreError::RangeTooLarge => {
            vars::SQLITE_IOERR_WRITE
        }
        PageStoreError::LockConflict { .. } => vars::SQLITE_BUSY,
    }
}

fn map_lock_error(error: PageStoreError) -> i32 {
    match error {
        PageStoreError::LockConflict { .. } => vars::SQLITE_BUSY,
        PageStoreError::RangeTooLarge | PageStoreError::ShortRead { .. } => vars::SQLITE_IOERR_LOCK,
    }
}
