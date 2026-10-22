//! Batched flush worker for the SQLite query copy.
//!
//! The hot layer answers reads from memory. The *query copy* is the durable,
//! SQL-shaped view of the same data, and it is written by this worker:
//! mutations are queued, drained in batches, and committed one transaction
//! per batch.
//!
//! The discipline comes from the store's own write-behind contract:
//!
//! * A batch is never dropped. A failed batch is put back at the front of
//!   the queue, in order, and retried with an exponential backoff. A dropped
//!   batch is silent data loss.
//! * The barrier is answered only after everything queued before it has
//!   committed. That is what makes a graceful shutdown safe.
//! * The worker is loud. A flush worker that goes quiet must always be
//!   noticeable, so startup and every failure are reported on stderr --
//!   the only channel available without pulling in a logging dependency.

use crate::schema_flush::{SchemaFlushBackend, SchemaFlushRecord};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread::JoinHandle;
use std::time::Duration;

/// Largest number of records written in one transaction.
pub const DEFAULT_BATCH_MAX: usize = 500;
/// Longest the worker waits for new work before looking again.
pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);
/// Longest the worker waits after a failed batch.
pub const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Longest a shutdown barrier waits for the queue to drain.
pub const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_secs(10);

/// Tuning knobs for the flush worker.
#[derive(Debug, Clone, Copy)]
pub struct FlushConfig {
    /// Maximum records per transaction.
    pub batch_max: usize,
    /// Idle wait between polls of the queue.
    pub interval: Duration,
    /// Upper bound of the exponential backoff after a failed batch.
    pub backoff_max: Duration,
    /// Upper bound of a shutdown barrier wait.
    pub barrier_timeout: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            batch_max: DEFAULT_BATCH_MAX,
            interval: DEFAULT_INTERVAL,
            backoff_max: DEFAULT_BACKOFF_MAX,
            barrier_timeout: DEFAULT_BARRIER_TIMEOUT,
        }
    }
}

/// Work item handed to the worker thread.
enum FlushJob {
    Record(SchemaFlushRecord),
    /// Asks the worker to answer once everything queued before it committed.
    Barrier { ack: Sender<()> },
}

/// Why a flush worker operation failed.
#[derive(Debug)]
pub enum FlushWorkerError {
    /// The worker thread is gone; nothing will be flushed.
    WorkerStopped,
    /// The worker dropped the barrier without answering it.
    WorkerDropped,
    /// The barrier was not answered within the timeout.
    BarrierTimeout { timeout: Duration },
    /// The worker thread could not be started.
    ThreadSpawn { error: std::io::Error },
}

impl fmt::Display for FlushWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WorkerStopped => write!(f, "flush worker is not running"),
            Self::WorkerDropped => write!(f, "flush worker dropped the barrier"),
            Self::BarrierTimeout { timeout } => {
                write!(f, "flush barrier timed out after {timeout:?}")
            }
            Self::ThreadSpawn { error } => write!(f, "flush worker thread failed to start: {error}"),
        }
    }
}

impl std::error::Error for FlushWorkerError {}

/// Background writer that turns queued mutations into a durable query copy.
///
/// The backend is shared: the worker writes through it, and readers read the
/// committed copy through the same handle.
pub struct FlushWorker<B> {
    backend: Arc<Mutex<B>>,
    sender: Arc<Mutex<Option<Sender<FlushJob>>>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    queued: Arc<AtomicUsize>,
    config: FlushConfig,
}

impl<B> FlushWorker<B>
where
    B: SchemaFlushBackend + Send + 'static,
    B::Error: fmt::Display,
{
    /// Start a worker with the default batching.
    pub fn spawn(backend: Arc<Mutex<B>>) -> Result<Self, FlushWorkerError> {
        Self::spawn_with_config(backend, FlushConfig::default())
    }

    /// Start a worker with explicit batching.
    pub fn spawn_with_config(
        backend: Arc<Mutex<B>>,
        config: FlushConfig,
    ) -> Result<Self, FlushWorkerError> {
        let (sender, receiver) = mpsc::channel::<FlushJob>();
        let queued = Arc::new(AtomicUsize::new(0));
        let worker_backend = Arc::clone(&backend);
        let worker_queued = Arc::clone(&queued);
        let handle = std::thread::Builder::new()
            .name("mfs-flush".to_string())
            .spawn(move || run_worker(worker_backend, receiver, worker_queued, config))
            .map_err(|error| FlushWorkerError::ThreadSpawn { error })?;
        eprintln!(
            "[mfs-flush] worker alive: batch_max={} interval_ms={} backoff_max_ms={}",
            config.batch_max,
            config.interval.as_millis(),
            config.backoff_max.as_millis()
        );
        Ok(Self {
            backend,
            sender: Arc::new(Mutex::new(Some(sender))),
            handle: Mutex::new(Some(handle)),
            queued,
            config,
        })
    }

    /// The shared backend, for readers of the committed copy.
    pub fn backend(&self) -> &Arc<Mutex<B>> {
        &self.backend
    }

    /// Records queued but not yet committed.
    pub fn queued_records(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Queue one mutation for the copy.
    ///
    /// This never blocks on the database: the hot path must not wait for
    /// SQLite.
    pub fn enqueue(&self, record: SchemaFlushRecord) -> Result<(), FlushWorkerError> {
        let guard = self.sender.lock();
        let sender = guard.as_ref().ok_or(FlushWorkerError::WorkerStopped)?;
        sender
            .send(FlushJob::Record(record))
            .map_err(|_| FlushWorkerError::WorkerStopped)?;
        self.queued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Wait until everything queued before this call is committed.
    ///
    /// The barrier is answered only after a successful commit, so a caller
    /// that returns from `barrier` knows the copy holds every record it
    /// queued before the barrier.
    pub fn barrier(&self) -> Result<(), FlushWorkerError> {
        self.barrier_with_timeout(self.config.barrier_timeout)
    }

    /// [`Self::barrier`] with an explicit timeout.
    pub fn barrier_with_timeout(&self, timeout: Duration) -> Result<(), FlushWorkerError> {
        let (ack_tx, ack_rx) = mpsc::channel::<()>();
        {
            let guard = self.sender.lock();
            let sender = guard.as_ref().ok_or(FlushWorkerError::WorkerStopped)?;
            sender
                .send(FlushJob::Barrier { ack: ack_tx })
                .map_err(|_| FlushWorkerError::WorkerStopped)?;
        }
        match ack_rx.recv_timeout(timeout) {
            Ok(()) => Ok(()),
            Err(RecvTimeoutError::Timeout) => Err(FlushWorkerError::BarrierTimeout { timeout }),
            Err(RecvTimeoutError::Disconnected) => Err(FlushWorkerError::WorkerDropped),
        }
    }

    /// Drain the queue and stop the worker.
    ///
    /// The barrier comes first: the copy is only allowed to fall behind after
    /// the process is gone.
    pub fn shutdown(&self) -> Result<(), FlushWorkerError> {
        let drained = self.barrier();
        self.stop();
        drained
    }

    /// Close the queue and wait for the worker thread to finish.
    fn stop(&self) {
        // Dropping the sender closes the queue. The worker flushes what is
        // left, then exits.
        self.sender.lock().take();
        if let Some(handle) = self.handle.lock().take() {
            if handle.join().is_err() {
                eprintln!("[mfs-flush] worker thread panicked");
            }
        }
    }
}

impl<B> Drop for FlushWorker<B> {
    fn drop(&mut self) {
        // Best-effort drain: closing the queue lets the worker flush what is
        // left and exit. The thread is not joined here -- a failed batch
        // backs off for as long as the backoff allows, and a destructor must
        // not block on that.
        self.sender.lock().take();
    }
}

fn run_worker<B>(
    backend: Arc<Mutex<B>>,
    receiver: Receiver<FlushJob>,
    queued: Arc<AtomicUsize>,
    config: FlushConfig,
) where
    B: SchemaFlushBackend + Send + 'static,
    B::Error: fmt::Display,
{
    let mut pending: VecDeque<SchemaFlushRecord> = VecDeque::new();
    let mut barrier: Option<Sender<()>> = None;
    let mut backoff = config.interval;
    let mut closed = false;

    loop {
        // Collect a batch, stopping at a barrier. Everything queued before
        // the barrier has to be committed before the barrier is answered.
        while pending.len() < config.batch_max && barrier.is_none() {
            match receiver.try_recv() {
                Ok(FlushJob::Record(record)) => pending.push_back(record),
                Ok(FlushJob::Barrier { ack }) => barrier = Some(ack),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }

        if pending.is_empty() && barrier.is_none() {
            if closed {
                break;
            }
            match receiver.recv_timeout(config.interval) {
                Ok(FlushJob::Record(record)) => pending.push_back(record),
                Ok(FlushJob::Barrier { ack }) => barrier = Some(ack),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => closed = true,
            }
            continue;
        }

        if !pending.is_empty() {
            let batch: Vec<SchemaFlushRecord> = pending.drain(..).collect();
            let outcome = {
                let mut guard = backend.lock();
                guard.flush_records(&batch)
            };
            if let Err(error) = outcome {
                // Never drop a batch. Put it back at the front so the order
                // is preserved, and back off so a broken database is not
                // hammered.
                eprintln!(
                    "[mfs-flush] flush batch failed ({} record(s)): {error}; requeueing",
                    batch.len()
                );
                for record in batch.into_iter().rev() {
                    pending.push_front(record);
                }
                std::thread::sleep(backoff);
                backoff = backoff.saturating_mul(2).min(config.backoff_max);
                // The barrier is deliberately not answered: it waits for a
                // successful commit of everything queued before it.
                continue;
            }
            queued.fetch_sub(batch.len(), Ordering::Relaxed);
            backoff = config.interval;
        }

        if let Some(ack) = barrier.take() {
            let _ = ack.send(());
        }

        if closed {
            break;
        }
    }
}
