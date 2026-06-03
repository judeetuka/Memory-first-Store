//! Minimum-viable write-ahead log (WAL) backend.
//!
//! This is a reference implementation that turns the [`FlushBackend`] hook
//! into a crash-recoverable persistent log. It is intentionally simple: the
//! goal is to give callers something that survives `kill -9` and power loss
//! without dragging in serde, async runtimes, or external dependencies.
//!
//! ## On-disk format
//!
//! The log is an append-only stream of length-prefixed records with a trailing
//! checksum. Recovery scans from the start, validating each record, and stops
//! at the first invalid record (truncation, torn write, or corruption).
//!
//! ```text
//! record := [u32 magic = 0x4D46_5750]   ("MFWP" little-endian)
//!           [u32 payload_len]
//!           [payload_len bytes]
//!           [u32 crc32c]
//!
//! payload := [u8 op]                   (0 = put, 1 = delete)
//!            [u64 version]
//!            [u32 key_len]   [key_len bytes]
//!            [u32 value_len] [value_len bytes]   (omitted if op == delete)
//! ```
//!
//! The checksum is the **CRC-32C (Castagnoli) polynomial 0x1EDC6F41**,
//! computed over `magic ++ payload_len ++ payload`. The [`crc32c`] crate
//! uses the SSE4.2 `CRC32` instruction on x86_64 and the equivalent NEON
//! `CRC32CB`/`CRC32CW`/`CRC32CX` on aarch64, hitting multiple GB/s of
//! checksum bandwidth. Same polynomial used by iSCSI, Btrfs, RocksDB.
//!
//! ## Durability model
//!
//! `flush` appends each record to a [`BufWriter`] and increments a byte
//! counter. When the counter exceeds [`WalConfig::sync_threshold_bytes`]
//! _or_ a record count exceeds [`WalConfig::sync_threshold_records`], the
//! backend issues `flush()` followed by `File::sync_data()` (`fdatasync`
//! on Linux). Records are not durable until the next sync. Callers who
//! need bounded loss should call [`WalBackend::sync_now`] explicitly after
//! a batch they care about, or set both thresholds low.
//!
//! ## Recovery
//!
//! Use [`WalBackend::replay`] to walk the log on startup and rehydrate the
//! in-memory store. The function streams records from the file and invokes
//! a caller-supplied closure for each valid record; invalid trailing bytes
//! are silently truncated (the standard "stop-at-first-bad-record" policy).
//!
//! ## What this is NOT
//!
//! - There is no log rotation, segment management, or compaction. After a
//!   long run the WAL grows unbounded; you should checkpoint state
//!   externally and start a fresh log, or layer your own segment scheme on
//!   top.
//! - The synchronous [`WalBackend`] is intentionally simple: buffer + threshold
//!   sync. [`AsyncWalBackend`] and [`GroupCommitWalBackend`] layer dedicated
//!   writer threads on top when callers want enqueue latency or durable group
//!   acknowledgements.
//! - There is no integrity verification beyond the per-record checksum.
//!   File-system corruption beyond a truncated tail is not handled.
//!
//! Treat this module as the _smallest correct_ WAL: a foundation, not a
//! finished product.

use crate::{FlushBackend, FlushRecord, Operation};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;

const RECORD_MAGIC: u32 = 0x4D46_5750; // "MFWP"
const MAX_REPLAY_PAYLOAD_BYTES: u32 = 128 * 1024 * 1024;

/// Pluggable byte codec for keys and values.
///
/// The WAL is type-agnostic; the user provides a codec that converts to and
/// from raw bytes. A blanket impl exists for `(u64, u64)` so the type
/// commonly used in benchmarks works out of the box.
pub trait WalCodec<K, V> {
    fn encode_key(&self, key: &K, out: &mut Vec<u8>);
    fn encode_value(&self, value: &V, out: &mut Vec<u8>);
    fn decode_key(&self, bytes: &[u8]) -> io::Result<K>;
    fn decode_value(&self, bytes: &[u8]) -> io::Result<V>;
}

/// Default codec for `u64` keys and values, fixed-width little-endian.
#[derive(Debug, Default, Clone, Copy)]
pub struct U64Codec;

impl WalCodec<u64, u64> for U64Codec {
    fn encode_key(&self, key: &u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&key.to_le_bytes());
    }
    fn encode_value(&self, value: &u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.to_le_bytes());
    }
    fn decode_key(&self, bytes: &[u8]) -> io::Result<u64> {
        if bytes.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "u64 key must be exactly 8 bytes",
            ));
        }
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }
    fn decode_value(&self, bytes: &[u8]) -> io::Result<u64> {
        if bytes.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "u64 value must be exactly 8 bytes",
            ));
        }
        Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WalConfig {
    pub sync_threshold_bytes: usize,
    pub sync_threshold_records: usize,
    pub buffer_capacity_bytes: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            sync_threshold_bytes: 64 * 1024,
            sync_threshold_records: 256,
            buffer_capacity_bytes: 64 * 1024,
        }
    }
}

pub struct WalBackend<K, V, C: WalCodec<K, V>> {
    path: PathBuf,
    writer: BufWriter<File>,
    codec: C,
    config: WalConfig,
    pending_bytes: usize,
    pending_records: usize,
    scratch: Vec<u8>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, C: WalCodec<K, V>> WalBackend<K, V, C> {
    pub fn open<P: AsRef<Path>>(path: P, codec: C, config: WalConfig) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(false)
            .open(&path)?;
        let writer = BufWriter::with_capacity(config.buffer_capacity_bytes, file);
        Ok(Self {
            path,
            writer,
            codec,
            config,
            pending_bytes: 0,
            pending_records: 0,
            scratch: Vec::with_capacity(256),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sync_now(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.pending_bytes = 0;
        self.pending_records = 0;
        Ok(())
    }

    fn maybe_sync(&mut self) -> io::Result<()> {
        if self.pending_bytes >= self.config.sync_threshold_bytes
            || self.pending_records >= self.config.sync_threshold_records
        {
            self.sync_now()?;
        }
        Ok(())
    }

    fn write_record(&mut self, record: &FlushRecord<K, V>) -> io::Result<()> {
        let payload_buf = &mut self.scratch;
        payload_buf.clear();

        match record.op {
            Operation::Put => payload_buf.push(0),
            Operation::Delete => payload_buf.push(1),
        }
        payload_buf.extend_from_slice(&record.version.to_le_bytes());

        let key_start = payload_buf.len();
        payload_buf.extend_from_slice(&[0u8; 4]);
        self.codec.encode_key(&record.key, payload_buf);
        let key_len = (payload_buf.len() - key_start - 4) as u32;
        payload_buf[key_start..key_start + 4].copy_from_slice(&key_len.to_le_bytes());

        if let Some(value) = &record.value {
            let val_start = payload_buf.len();
            payload_buf.extend_from_slice(&[0u8; 4]);
            self.codec.encode_value(value.as_ref(), payload_buf);
            let val_len = (payload_buf.len() - val_start - 4) as u32;
            payload_buf[val_start..val_start + 4].copy_from_slice(&val_len.to_le_bytes());
        } else {
            payload_buf.extend_from_slice(&0u32.to_le_bytes());
        }

        let payload_len = payload_buf.len() as u32;
        let mut crc = crc32c::crc32c(&RECORD_MAGIC.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &payload_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, payload_buf);

        self.writer.write_all(&RECORD_MAGIC.to_le_bytes())?;
        self.writer.write_all(&payload_len.to_le_bytes())?;
        self.writer.write_all(payload_buf)?;
        self.writer.write_all(&crc.to_le_bytes())?;

        self.pending_bytes += 4 + 4 + payload_buf.len() + 4;
        self.pending_records += 1;
        Ok(())
    }

    fn write_records(&mut self, records: &[FlushRecord<K, V>]) -> io::Result<()> {
        for record in records {
            self.write_record(record)?;
        }
        Ok(())
    }
}

impl<K, V, C> FlushBackend<K, V> for WalBackend<K, V, C>
where
    C: WalCodec<K, V>,
{
    type Error = io::Error;

    fn flush(&mut self, records: &[FlushRecord<K, V>]) -> io::Result<()> {
        self.write_records(records)?;
        self.maybe_sync()
    }
}

/// Configuration for [`AsyncWalBackend`].
#[derive(Debug, Clone, Copy)]
pub struct AsyncWalConfig {
    /// Maximum number of flush batches waiting in the WAL queue.
    ///
    /// A bounded queue is intentional: if storage cannot keep up, callers
    /// eventually apply backpressure instead of accumulating unbounded dirty
    /// state in RAM. Each queued item is one `Vec<FlushRecord<..>>`, not one
    /// individual record.
    pub queue_capacity: usize,
}

/// Configuration for [`GroupCommitWalBackend`].
#[derive(Debug, Clone, Copy)]
pub struct GroupCommitWalConfig {
    /// Maximum number of flush commands waiting for the WAL writer.
    pub queue_capacity: usize,
    /// Maximum number of records to sync in one durable group.
    pub max_group_records: usize,
}

impl Default for GroupCommitWalConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_group_records: 4096,
        }
    }
}

impl Default for AsyncWalConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
        }
    }
}

enum WalCommand<K, V> {
    Flush(Vec<FlushRecord<K, V>>),
    Sync(mpsc::SyncSender<io::Result<()>>),
    Shutdown(mpsc::SyncSender<io::Result<()>>),
}

enum GroupWalCommand<K, V> {
    Flush {
        records: Vec<FlushRecord<K, V>>,
        ack: mpsc::SyncSender<io::Result<()>>,
    },
    Barrier(mpsc::SyncSender<io::Result<()>>),
    Shutdown(mpsc::SyncSender<io::Result<()>>),
}

/// Dedicated-thread WAL backend.
///
/// `AsyncWalBackend` implements [`FlushBackend`] by enqueueing flushed record
/// batches into a bounded channel. A dedicated writer thread owns the real
/// [`WalBackend`], drains queued batches, serializes records, and performs the
/// configured `sync_data` policy.
///
/// ## Durability semantics
///
/// [`FlushBackend::flush`] returning `Ok(())` means the batch was accepted by
/// the in-process WAL queue — **not** that it reached disk. Call
/// [`sync_barrier`](Self::sync_barrier) to wait until all previously accepted
/// batches have been written and `sync_data`'d. [`shutdown`](Self::shutdown)
/// also performs a final durability barrier before joining the writer thread.
///
/// This split is deliberate: hot flushers pay queue latency, while callers who
/// need strong durability can opt into an explicit barrier.
pub struct AsyncWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    tx: mpsc::SyncSender<WalCommand<K, V>>,
    handle: Option<thread::JoinHandle<io::Result<()>>>,
    _marker: std::marker::PhantomData<C>,
}

impl<K, V, C> AsyncWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    pub fn open<P: AsRef<Path>>(
        path: P,
        codec: C,
        wal_config: WalConfig,
        async_config: AsyncWalConfig,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let (tx, rx) = mpsc::sync_channel(async_config.queue_capacity.max(1));
        let handle = thread::spawn(move || run_async_wal_thread(path, codec, wal_config, rx));
        Ok(Self {
            tx,
            handle: Some(handle),
            _marker: std::marker::PhantomData,
        })
    }

    /// Wait until all batches accepted before this call are durable.
    pub fn sync_barrier(&self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx
            .send(WalCommand::Sync(tx))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "async WAL thread stopped"))?;
        rx.recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "async WAL thread stopped"))?
    }

    /// Enqueue a batch into the dedicated WAL writer thread.
    ///
    /// Returning `Ok(())` means the batch was accepted into the bounded
    /// in-process queue. It is not durable until [`sync_barrier`](Self::sync_barrier)
    /// or [`shutdown`](Self::shutdown) completes.
    pub fn enqueue(&self, records: &[FlushRecord<K, V>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.tx
            .send(WalCommand::Flush(records.to_vec()))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "async WAL thread stopped"))
    }

    /// Drain queued batches, sync the WAL, and join the writer thread.
    pub fn shutdown(mut self) -> io::Result<()> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(0);
        let send_result = self.tx.send(WalCommand::Shutdown(tx));
        let barrier_result = match send_result {
            Ok(()) => rx.recv().map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "async WAL thread stopped")
            })?,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "async WAL thread stopped",
            )),
        };
        let join_result = match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(result) => result,
                Err(_) => Err(io::Error::other("async WAL thread panicked")),
            },
            None => Ok(()),
        };
        barrier_result.and(join_result)
    }
}

impl<K, V, C> FlushBackend<K, V> for AsyncWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    type Error = io::Error;

    fn flush(&mut self, records: &[FlushRecord<K, V>]) -> io::Result<()> {
        self.enqueue(records)
    }
}

impl<K, V, C> Drop for AsyncWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

/// Durable group-commit WAL backend.
///
/// Unlike [`AsyncWalBackend`], a [`GroupCommitWalHandle`] implements
/// [`FlushBackend`] with a durable acknowledgment: `flush()` returns only after
/// the writer thread has appended the group containing the batch and
/// [`File::sync_data`] has completed via [`WalBackend::sync_now`]. Concurrent
/// handles can submit batches in parallel; the writer drains available batches
/// and amortizes one sync across the group.
pub struct GroupCommitWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    tx: mpsc::SyncSender<GroupWalCommand<K, V>>,
    handle: Option<thread::JoinHandle<io::Result<()>>>,
    _marker: std::marker::PhantomData<C>,
}

pub struct GroupCommitWalHandle<K, V>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    tx: mpsc::SyncSender<GroupWalCommand<K, V>>,
}

impl<K, V> Clone for GroupCommitWalHandle<K, V>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<K, V, C> GroupCommitWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    pub fn open<P: AsRef<Path>>(
        path: P,
        codec: C,
        wal_config: WalConfig,
        group_config: GroupCommitWalConfig,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let (tx, rx) = mpsc::sync_channel(group_config.queue_capacity.max(1));
        let max_group_records = group_config.max_group_records.max(1);
        let handle = thread::spawn(move || {
            run_group_commit_wal_thread(path, codec, wal_config, max_group_records, rx)
        });
        Ok(Self {
            tx,
            handle: Some(handle),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn handle(&self) -> GroupCommitWalHandle<K, V> {
        GroupCommitWalHandle {
            tx: self.tx.clone(),
        }
    }

    /// Wait until all previously submitted durable groups are synced.
    pub fn sync_barrier(&self) -> io::Result<()> {
        self.handle().sync_barrier()
    }

    pub fn shutdown(mut self) -> io::Result<()> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(0);
        let send_result = self.tx.send(GroupWalCommand::Shutdown(tx));
        let barrier_result = match send_result {
            Ok(()) => rx.recv().map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "group WAL thread stopped")
            })?,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "group WAL thread stopped",
            )),
        };
        let join_result = match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(result) => result,
                Err(_) => Err(io::Error::other("group WAL thread panicked")),
            },
            None => Ok(()),
        };
        barrier_result.and(join_result)
    }
}

impl<K, V, C> Drop for GroupCommitWalBackend<K, V, C>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    fn drop(&mut self) {
        let _ = self.shutdown_inner();
    }
}

impl<K, V> GroupCommitWalHandle<K, V>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    pub fn sync_barrier(&self) -> io::Result<()> {
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx
            .send(GroupWalCommand::Barrier(tx))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "group WAL thread stopped"))?;
        rx.recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "group WAL thread stopped"))?
    }
}

impl<K, V> FlushBackend<K, V> for GroupCommitWalHandle<K, V>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
{
    type Error = io::Error;

    fn flush(&mut self, records: &[FlushRecord<K, V>]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let (tx, rx) = mpsc::sync_channel(0);
        self.tx
            .send(GroupWalCommand::Flush {
                records: records.to_vec(),
                ack: tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "group WAL thread stopped"))?;
        rx.recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "group WAL thread stopped"))?
    }
}

fn run_async_wal_thread<K, V, C>(
    path: PathBuf,
    codec: C,
    wal_config: WalConfig,
    rx: mpsc::Receiver<WalCommand<K, V>>,
) -> io::Result<()>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    let mut wal = WalBackend::open(path, codec, wal_config)?;
    while let Ok(cmd) = rx.recv() {
        match cmd {
            WalCommand::Flush(records) => {
                let mut batch = records;
                let mut next_control = None;
                loop {
                    match rx.try_recv() {
                        Ok(WalCommand::Flush(records)) => batch.extend(records),
                        Ok(cmd) => {
                            next_control = Some(cmd);
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                wal.flush(&batch)?;
                if let Some(cmd) = next_control {
                    match cmd {
                        WalCommand::Flush(_) => unreachable!("flush commands are drained above"),
                        WalCommand::Sync(done) => {
                            let result = wal.sync_now();
                            let failed = result.is_err();
                            let _ = done.send(result);
                            if failed {
                                return Err(io::Error::other("async WAL sync failed"));
                            }
                        }
                        WalCommand::Shutdown(done) => {
                            let result = wal.sync_now();
                            let failed = result.is_err();
                            let _ = done.send(result);
                            if failed {
                                return Err(io::Error::other("async WAL shutdown sync failed"));
                            }
                            return Ok(());
                        }
                    }
                }
            }
            WalCommand::Sync(done) => {
                let result = wal.sync_now();
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    return Err(io::Error::other("async WAL sync failed"));
                }
            }
            WalCommand::Shutdown(done) => {
                let result = wal.sync_now();
                let failed = result.is_err();
                let _ = done.send(result);
                if failed {
                    return Err(io::Error::other("async WAL shutdown sync failed"));
                }
                return Ok(());
            }
        }
    }
    wal.sync_now()
}

fn run_group_commit_wal_thread<K, V, C>(
    path: PathBuf,
    codec: C,
    wal_config: WalConfig,
    max_group_records: usize,
    rx: mpsc::Receiver<GroupWalCommand<K, V>>,
) -> io::Result<()>
where
    K: Clone + Send + 'static,
    V: Send + Sync + 'static,
    C: WalCodec<K, V> + Send + 'static,
{
    let mut wal = WalBackend::open(path, codec, wal_config)?;
    while let Ok(cmd) = rx.recv() {
        match cmd {
            GroupWalCommand::Flush { records, ack } => {
                let mut batch = records;
                let mut acks = vec![ack];
                let mut next_control = None;
                while batch.len() < max_group_records {
                    match rx.try_recv() {
                        Ok(GroupWalCommand::Flush { records, ack }) => {
                            batch.extend(records);
                            acks.push(ack);
                        }
                        Ok(cmd) => {
                            next_control = Some(cmd);
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                send_group_result(
                    &mut acks,
                    wal.write_records(&batch).and_then(|_| wal.sync_now()),
                )?;
                if let Some(cmd) = next_control
                    && handle_group_control(&mut wal, cmd)?
                {
                    return Ok(());
                }
            }
            cmd => {
                if handle_group_control(&mut wal, cmd)? {
                    return Ok(());
                }
            }
        }
    }
    wal.sync_now()
}

fn handle_group_control<K, V, C>(
    wal: &mut WalBackend<K, V, C>,
    cmd: GroupWalCommand<K, V>,
) -> io::Result<bool>
where
    C: WalCodec<K, V>,
{
    match cmd {
        GroupWalCommand::Flush { .. } => {
            unreachable!("flush commands are handled by the group loop")
        }
        GroupWalCommand::Barrier(done) => {
            send_single_result(done, wal.sync_now())?;
            Ok(false)
        }
        GroupWalCommand::Shutdown(done) => {
            send_single_result(done, wal.sync_now())?;
            Ok(true)
        }
    }
}

fn send_single_result(
    done: mpsc::SyncSender<io::Result<()>>,
    result: io::Result<()>,
) -> io::Result<()> {
    match result {
        Ok(()) => {
            let _ = done.send(Ok(()));
            Ok(())
        }
        Err(e) => {
            let kind = e.kind();
            let message = e.to_string();
            let _ = done.send(Err(io::Error::new(kind, message)));
            Err(e)
        }
    }
}

fn send_group_result(
    acks: &mut Vec<mpsc::SyncSender<io::Result<()>>>,
    result: io::Result<()>,
) -> io::Result<()> {
    match result {
        Ok(()) => {
            for ack in acks.drain(..) {
                let _ = ack.send(Ok(()));
            }
            Ok(())
        }
        Err(e) => {
            let kind = e.kind();
            let message = e.to_string();
            for ack in acks.drain(..) {
                let _ = ack.send(Err(io::Error::new(kind, message.clone())));
            }
            Err(e)
        }
    }
}

/// A single decoded record produced by [`WalBackend::replay`].
#[derive(Debug)]
pub struct ReplayRecord<K, V> {
    pub key: K,
    pub value: Option<V>,
    pub version: u64,
    pub op: Operation,
}

impl<K, V, C: WalCodec<K, V>> WalBackend<K, V, C> {
    /// Stream records from the WAL at `path`, invoking `f` for each valid
    /// record in append order. Stops at end-of-file or first invalid record
    /// (truncation, torn write, magic mismatch, checksum mismatch). The
    /// number of records successfully replayed is returned.
    pub fn replay<P, F>(path: P, codec: &C, mut f: F) -> io::Result<usize>
    where
        P: AsRef<Path>,
        F: FnMut(ReplayRecord<K, V>),
    {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(0);
        }
        let file = File::open(path)?;
        let total_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut count = 0usize;

        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let magic = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
            if magic != RECORD_MAGIC {
                break;
            }
            let payload_len = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
            if payload_len > MAX_REPLAY_PAYLOAD_BYTES {
                break;
            }

            let pos = reader.stream_position()?;
            let needed = payload_len as u64 + 4;
            if pos + needed > total_len {
                // truncated tail; stop cleanly
                reader.seek(SeekFrom::Start(pos - 8))?;
                break;
            }

            let mut payload = vec![0u8; payload_len as usize];
            reader.read_exact(&mut payload)?;
            let mut checksum_buf = [0u8; 4];
            reader.read_exact(&mut checksum_buf)?;
            let stored_crc = u32::from_le_bytes(checksum_buf);

            let mut crc = crc32c::crc32c(&magic.to_le_bytes());
            crc = crc32c::crc32c_append(crc, &payload_len.to_le_bytes());
            crc = crc32c::crc32c_append(crc, &payload);
            if crc != stored_crc {
                break;
            }

            let Some(decoded) = decode_payload::<K, V, C>(codec, &payload) else {
                break;
            };
            f(decoded);
            count += 1;
        }
        Ok(count)
    }
}

fn decode_payload<K, V, C: WalCodec<K, V>>(
    codec: &C,
    payload: &[u8],
) -> Option<ReplayRecord<K, V>> {
    let mut cursor = 0usize;
    if payload.len() < 1 + 8 + 4 {
        return None;
    }
    let op_byte = payload[cursor];
    cursor += 1;
    let op = match op_byte {
        0 => Operation::Put,
        1 => Operation::Delete,
        _ => return None,
    };
    let version = u64::from_le_bytes(payload[cursor..cursor + 8].try_into().ok()?);
    cursor += 8;
    let key_len = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    if cursor + key_len > payload.len() {
        return None;
    }
    let key = codec.decode_key(&payload[cursor..cursor + key_len]).ok()?;
    cursor += key_len;
    if cursor + 4 > payload.len() {
        return None;
    }
    let val_len = u32::from_le_bytes(payload[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    let value = if val_len == 0 {
        None
    } else {
        if cursor + val_len > payload.len() {
            return None;
        }
        let v = codec
            .decode_value(&payload[cursor..cursor + val_len])
            .ok()?;
        cursor += val_len;
        Some(v)
    };
    if cursor != payload.len() {
        return None;
    }
    Some(ReplayRecord {
        key,
        value,
        version,
        op,
    })
}

/// Convenience: rebuild a `MemoryFirstStore<u64, u64>` from a WAL on disk.
/// Records are applied in order; later versions of a key supersede earlier
/// ones, matching the original write order.
pub fn replay_into_u64_store(
    path: impl AsRef<Path>,
    store: &crate::MemoryFirstStore<u64, u64>,
) -> io::Result<usize> {
    let codec = U64Codec;
    WalBackend::<u64, u64, U64Codec>::replay(path, &codec, |rec| match rec.op {
        Operation::Put => {
            if let Some(v) = rec.value {
                store.load_clean(rec.key, v);
            }
        }
        Operation::Delete => {
            store.delete(rec.key);
        }
    })
}

#[doc(hidden)]
pub fn _construct_replay_record<K, V>(
    key: K,
    value: Option<V>,
    version: u64,
    op: Operation,
) -> ReplayRecord<K, V> {
    ReplayRecord {
        key,
        value,
        version,
        op,
    }
}

// Convenience: re-export Arc so docs/examples are self-contained.
#[doc(hidden)]
pub use std::sync::Arc as _Arc;

// Silence unused-Arc-import lints in users that don't need it.
#[allow(dead_code)]
fn _arc_link<V>(_: Option<Arc<V>>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlushBackend, FlushRecord, MemoryFirstStore, Operation};
    use std::sync::Arc;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("mfs_wal_{name}_{pid}_{ts}.log"));
        p
    }

    fn put_record(key: u64, value: u64, version: u64) -> FlushRecord<u64, u64> {
        FlushRecord {
            key,
            value: Some(Arc::new(value)),
            version,
            op: Operation::Put,
        }
    }

    fn delete_record(key: u64, version: u64) -> FlushRecord<u64, u64> {
        FlushRecord {
            key,
            value: None,
            version,
            op: Operation::Delete,
        }
    }

    #[test]
    fn write_and_replay_round_trip() {
        let path = tmp_path("round_trip");
        {
            let mut wal =
                WalBackend::open(&path, U64Codec, WalConfig::default()).expect("open wal");
            wal.flush(&[put_record(1, 100, 1), put_record(2, 200, 2)])
                .unwrap();
            wal.flush(&[delete_record(1, 3)]).unwrap();
            wal.sync_now().unwrap();
        }

        let mut seen: Vec<(u64, Option<u64>, u64, Operation)> = Vec::new();
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |rec| {
            seen.push((rec.key, rec.value, rec.version, rec.op));
        })
        .expect("replay");
        assert_eq!(count, 3);
        assert_eq!(seen[0], (1, Some(100), 1, Operation::Put));
        assert_eq!(seen[1], (2, Some(200), 2, Operation::Put));
        assert_eq!(seen[2], (1, None, 3, Operation::Delete));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn async_wal_barrier_makes_records_replayable() {
        let path = tmp_path("async_barrier");
        {
            let mut wal = AsyncWalBackend::open(
                &path,
                U64Codec,
                WalConfig::default(),
                AsyncWalConfig { queue_capacity: 4 },
            )
            .expect("open async wal");
            wal.flush(&[put_record(1, 100, 1), put_record(2, 200, 2)])
                .unwrap();
            wal.flush(&[delete_record(1, 3)]).unwrap();
            wal.sync_barrier().unwrap();
            wal.shutdown().unwrap();
        }

        let mut seen: Vec<(u64, Option<u64>, u64, Operation)> = Vec::new();
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |rec| {
            seen.push((rec.key, rec.value, rec.version, rec.op));
        })
        .expect("replay");
        assert_eq!(count, 3);
        assert_eq!(seen[0], (1, Some(100), 1, Operation::Put));
        assert_eq!(seen[1], (2, Some(200), 2, Operation::Put));
        assert_eq!(seen[2], (1, None, 3, Operation::Delete));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn async_wal_shutdown_drains_without_explicit_barrier() {
        let path = tmp_path("async_shutdown");
        {
            let mut wal = AsyncWalBackend::open(
                &path,
                U64Codec,
                WalConfig::default(),
                AsyncWalConfig { queue_capacity: 2 },
            )
            .expect("open async wal");
            wal.flush(&[put_record(7, 70, 1)]).unwrap();
            wal.flush(&[put_record(8, 80, 2)]).unwrap();
            wal.shutdown().unwrap();
        }

        let mut seen = Vec::new();
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |rec| {
            seen.push((rec.key, rec.value, rec.version, rec.op));
        })
        .expect("replay");
        assert_eq!(count, 2);
        assert_eq!(seen[0], (7, Some(70), 1, Operation::Put));
        assert_eq!(seen[1], (8, Some(80), 2, Operation::Put));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn group_commit_flush_makes_records_replayable() {
        let path = tmp_path("group_commit_flush");
        {
            let wal = GroupCommitWalBackend::open(
                &path,
                U64Codec,
                WalConfig::default(),
                GroupCommitWalConfig {
                    queue_capacity: 4,
                    max_group_records: 16,
                },
            )
            .expect("open group wal");
            let mut handle = wal.handle();
            handle
                .flush(&[put_record(1, 100, 1), put_record(2, 200, 2)])
                .unwrap();
            handle.flush(&[delete_record(1, 3)]).unwrap();
            wal.shutdown().unwrap();
        }

        let mut seen: Vec<(u64, Option<u64>, u64, Operation)> = Vec::new();
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |rec| {
            seen.push((rec.key, rec.value, rec.version, rec.op));
        })
        .expect("replay");
        assert_eq!(count, 3);
        assert_eq!(seen[0], (1, Some(100), 1, Operation::Put));
        assert_eq!(seen[1], (2, Some(200), 2, Operation::Put));
        assert_eq!(seen[2], (1, None, 3, Operation::Delete));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_stops_at_truncated_tail() {
        let path = tmp_path("truncated");
        {
            let mut wal =
                WalBackend::open(&path, U64Codec, WalConfig::default()).expect("open wal");
            wal.flush(&[put_record(7, 77, 1), put_record(8, 88, 2)])
                .unwrap();
            wal.sync_now().unwrap();
        }

        // Truncate the last 4 bytes of the file to simulate a torn write.
        let len = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len.saturating_sub(4)).unwrap();
        drop(f);

        let mut seen = Vec::new();
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |rec| {
            seen.push((rec.key, rec.value, rec.version, rec.op));
        })
        .expect("replay");
        assert_eq!(count, 1);
        assert_eq!(seen[0], (7, Some(77), 1, Operation::Put));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_into_store_rehydrates_state() {
        let path = tmp_path("rehydrate");
        {
            let mut wal =
                WalBackend::open(&path, U64Codec, WalConfig::default()).expect("open wal");
            wal.flush(&[put_record(1, 10, 1), put_record(2, 20, 1)])
                .unwrap();
            wal.flush(&[put_record(1, 11, 2)]).unwrap();
            wal.flush(&[delete_record(2, 2)]).unwrap();
            wal.sync_now().unwrap();
        }

        let store = MemoryFirstStore::<u64, u64>::new();
        let n = replay_into_u64_store(&path, &store).expect("replay into store");
        assert_eq!(n, 4);
        assert_eq!(store.get(&1).map(|v| *v), Some(11));
        assert!(store.get(&2).is_none());

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_detects_corrupted_checksum() {
        let path = tmp_path("corrupt");
        {
            let mut wal =
                WalBackend::open(&path, U64Codec, WalConfig::default()).expect("open wal");
            wal.flush(&[put_record(1, 100, 1), put_record(2, 200, 2)])
                .unwrap();
            wal.sync_now().unwrap();
        }

        // Flip a byte inside the first record's key payload to break checksum.
        // The first record starts at offset 0; magic(4)+len(4)+op(1)+ver(8)+keylen(4) = 21
        // so byte 21 is the first byte of the encoded key.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.seek(SeekFrom::Start(21)).unwrap();
            let mut b = [0u8; 1];
            f.read_exact(&mut b).unwrap();
            b[0] ^= 0xFF;
            f.seek(SeekFrom::Start(21)).unwrap();
            f.write_all(&b).unwrap();
            f.sync_data().unwrap();
        }

        let mut seen = 0usize;
        let codec = U64Codec;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &codec, |_| {
            seen += 1;
        })
        .expect("replay");
        // Corrupted first record means no records replayed.
        assert_eq!(count, 0);
        assert_eq!(seen, 0);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replay_stops_before_allocating_oversized_payload() {
        let path = tmp_path("oversized_payload");
        {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap();
            file.write_all(&RECORD_MAGIC.to_le_bytes()).unwrap();
            file.write_all(&(MAX_REPLAY_PAYLOAD_BYTES + 1).to_le_bytes())
                .unwrap();
            file.sync_all().unwrap();
        }

        let mut seen = 0usize;
        let count = WalBackend::<u64, u64, U64Codec>::replay(&path, &U64Codec, |_| {
            seen += 1;
        })
        .expect("replay oversized payload");
        assert_eq!(count, 0);
        assert_eq!(seen, 0);

        std::fs::remove_file(&path).ok();
    }
}
