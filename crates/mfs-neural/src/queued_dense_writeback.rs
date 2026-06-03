//! Queued write lane for [`crate::dense_writeback_map::DenseWriteBehindMap`].
//!
//! Existing eager write APIs stay unchanged. This type adds explicit eventual
//! writes: `put_async` accepts work into a bounded per-shard queue and returns a
//! [`WriteTicket`]. The value becomes visible only after the ticket is applied
//! (or after `barrier_all`).

use crate::DenseValue;
use crate::dense_writeback_map::{DenseWriteBehindMap, DenseWriteBehindStats};
use crossbeam_utils::CachePadded;
use mfs_core::writeback::WriteBehindConfig;
use mfs_core::{FastBuildHasher, FlushBackend};
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedWriteError {
    Closed,
    Full,
}

pub struct WriteTicket {
    seq: u64,
    applied: Arc<AtomicU64>,
}

impl WriteTicket {
    #[inline]
    pub fn is_applied(&self) -> bool {
        self.applied.load(Ordering::Acquire) >= self.seq
    }

    pub fn wait_applied(&self) {
        while !self.is_applied() {
            std::thread::yield_now();
        }
    }
}

enum Command<K, V> {
    Put { seq: u64, key: K, value: V },
    Delete { seq: u64, key: K },
    Barrier { seq: u64, done: SyncSender<()> },
    Shutdown { done: SyncSender<()> },
}

pub struct QueuedDenseWriteBehindMap<K, V, S = FastBuildHasher>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    inner: Arc<DenseWriteBehindMap<K, V, S>>,
    senders: Box<[SyncSender<Command<K, V>>]>,
    next_seq: Box<[CachePadded<AtomicU64>]>,
    applied: Box<[Arc<AtomicU64>]>,
    handles: Vec<thread::JoinHandle<()>>,
    shard_mask: usize,
    hash_builder: S,
}

impl<K, V> QueuedDenseWriteBehindMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
{
    pub fn with_capacity(expected_entries: usize) -> Self {
        Self::with_config(WriteBehindConfig {
            initial_capacity: expected_entries,
            ..WriteBehindConfig::default()
        })
    }

    pub fn with_config(config: WriteBehindConfig) -> Self {
        Self::with_hasher_and_config(FastBuildHasher::default(), config)
    }
}

impl<K, V, S> QueuedDenseWriteBehindMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher + Clone + Send + Sync + 'static,
{
    pub fn with_hasher_and_config(hash_builder: S, config: WriteBehindConfig) -> Self {
        let shard_count = config.dirty_shards.max(1).next_power_of_two();
        let queue_cap = config.dirty_queue_capacity.max(64);
        let inner = Arc::new(DenseWriteBehindMap::with_hasher_and_config(
            hash_builder.clone(),
            config,
        ));
        let mut senders = Vec::with_capacity(shard_count);
        let mut next_seq = Vec::with_capacity(shard_count);
        let mut applied = Vec::with_capacity(shard_count);
        let mut handles = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = mpsc::sync_channel(queue_cap);
            let map = Arc::clone(&inner);
            let applied_seq = Arc::new(AtomicU64::new(0));
            let applied_for_thread = Arc::clone(&applied_seq);
            handles.push(thread::spawn(move || {
                while let Ok(cmd) = rx.recv() {
                    match cmd {
                        Command::Put { seq, key, value } => {
                            map.put(key, value);
                            applied_for_thread.store(seq, Ordering::Release);
                        }
                        Command::Delete { seq, key } => {
                            map.delete(key);
                            applied_for_thread.store(seq, Ordering::Release);
                        }
                        Command::Barrier { seq, done } => {
                            applied_for_thread.store(seq, Ordering::Release);
                            let _ = done.send(());
                        }
                        Command::Shutdown { done } => {
                            let _ = done.send(());
                            break;
                        }
                    }
                }
            }));
            senders.push(tx);
            next_seq.push(CachePadded::new(AtomicU64::new(0)));
            applied.push(applied_seq);
        }
        Self {
            inner,
            senders: senders.into_boxed_slice(),
            next_seq: next_seq.into_boxed_slice(),
            applied: applied.into_boxed_slice(),
            handles,
            shard_mask: shard_count - 1,
            hash_builder,
        }
    }

    #[inline]
    fn shard_idx(&self, key: &K) -> usize {
        (self.hash_builder.hash_one(key).rotate_right(7) as usize) & self.shard_mask
    }

    fn next_ticket(&self, shard: usize) -> (u64, WriteTicket) {
        let seq = self.next_seq[shard].fetch_add(1, Ordering::Relaxed) + 1;
        (
            seq,
            WriteTicket {
                seq,
                applied: Arc::clone(&self.applied[shard]),
            },
        )
    }

    pub fn put_async(&self, key: K, value: V) -> Result<WriteTicket, QueuedWriteError> {
        let shard = self.shard_idx(&key);
        let (seq, ticket) = self.next_ticket(shard);
        self.senders[shard]
            .send(Command::Put { seq, key, value })
            .map_err(|_| QueuedWriteError::Closed)?;
        Ok(ticket)
    }

    pub fn try_put_async(&self, key: K, value: V) -> Result<WriteTicket, QueuedWriteError> {
        let shard = self.shard_idx(&key);
        let (seq, ticket) = self.next_ticket(shard);
        match self.senders[shard].try_send(Command::Put { seq, key, value }) {
            Ok(()) => Ok(ticket),
            Err(TrySendError::Full(_)) => Err(QueuedWriteError::Full),
            Err(TrySendError::Disconnected(_)) => Err(QueuedWriteError::Closed),
        }
    }

    pub fn delete_async(&self, key: K) -> Result<WriteTicket, QueuedWriteError> {
        let shard = self.shard_idx(&key);
        let (seq, ticket) = self.next_ticket(shard);
        self.senders[shard]
            .send(Command::Delete { seq, key })
            .map_err(|_| QueuedWriteError::Closed)?;
        Ok(ticket)
    }

    pub fn try_delete_async(&self, key: K) -> Result<WriteTicket, QueuedWriteError> {
        let shard = self.shard_idx(&key);
        let (seq, ticket) = self.next_ticket(shard);
        match self.senders[shard].try_send(Command::Delete { seq, key }) {
            Ok(()) => Ok(ticket),
            Err(TrySendError::Full(_)) => Err(QueuedWriteError::Full),
            Err(TrySendError::Disconnected(_)) => Err(QueuedWriteError::Closed),
        }
    }

    pub fn barrier_all(&self) -> Result<(), QueuedWriteError> {
        for shard in 0..self.senders.len() {
            let seq = self.next_seq[shard].fetch_add(1, Ordering::Relaxed) + 1;
            let (tx, rx) = mpsc::sync_channel(0);
            self.senders[shard]
                .send(Command::Barrier { seq, done: tx })
                .map_err(|_| QueuedWriteError::Closed)?;
            rx.recv().map_err(|_| QueuedWriteError::Closed)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn stats(&self) -> DenseWriteBehindStats {
        self.inner.stats()
    }

    pub fn flush_idle<B>(
        &self,
        backend: &mut B,
        idle_ticks: u64,
        max_records: usize,
    ) -> Result<usize, B::Error>
    where
        B: FlushBackend<K, V>,
    {
        let _ = self.barrier_all();
        self.inner.flush_idle(backend, idle_ticks, max_records)
    }

    pub fn shutdown(mut self) -> Result<(), QueuedWriteError> {
        self.shutdown_inner()
    }

    fn shutdown_inner(&mut self) -> Result<(), QueuedWriteError> {
        for sender in self.senders.iter() {
            let (tx, rx) = mpsc::sync_channel(0);
            sender
                .send(Command::Shutdown { done: tx })
                .map_err(|_| QueuedWriteError::Closed)?;
            rx.recv().map_err(|_| QueuedWriteError::Closed)?;
        }
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl<K, V, S> Drop for QueuedDenseWriteBehindMap<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: DenseValue,
    S: BuildHasher,
{
    fn drop(&mut self) {
        for sender in self.senders.iter() {
            let (tx, _rx) = mpsc::sync_channel(0);
            let _ = sender.send(Command::Shutdown { done: tx });
        }
        while let Some(handle) = self.handles.pop() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_controls_visibility() {
        let map = QueuedDenseWriteBehindMap::<u64, u64>::with_capacity(64);
        let ticket = map.put_async(1, 10).unwrap();
        ticket.wait_applied();
        assert_eq!(map.get(&1), Some(10));
    }

    #[test]
    fn barrier_all_applies_all_shards() {
        let map = QueuedDenseWriteBehindMap::<u64, u64>::with_capacity(1024);
        for i in 0..256u64 {
            map.put_async(i, i * 2).unwrap();
        }
        map.barrier_all().unwrap();
        for i in 0..256u64 {
            assert_eq!(map.get(&i), Some(i * 2));
        }
    }

    #[test]
    fn flush_waits_for_queued_writes() {
        #[derive(Default)]
        struct Backend(Vec<mfs_core::FlushRecord<u64, u64>>);
        impl FlushBackend<u64, u64> for Backend {
            type Error = ();
            fn flush(
                &mut self,
                records: &[mfs_core::FlushRecord<u64, u64>],
            ) -> Result<(), Self::Error> {
                self.0.extend_from_slice(records);
                Ok(())
            }
        }

        let map = QueuedDenseWriteBehindMap::<u64, u64>::with_capacity(64);
        map.put_async(1, 10).unwrap();
        let mut backend = Backend::default();
        assert_eq!(map.flush_idle(&mut backend, 0, 1024).unwrap(), 1);
        assert_eq!(backend.0[0].key, 1);
        assert_eq!(backend.0[0].value.as_deref().copied(), Some(10));
    }

    #[test]
    fn same_key_fifo_ordering() {
        let map = QueuedDenseWriteBehindMap::<u64, u64>::with_capacity(64);
        let mut last = None;
        for i in 0..128u64 {
            last = Some(map.put_async(1, i).unwrap());
        }
        last.unwrap().wait_applied();
        assert_eq!(map.get(&1), Some(127));
        map.shutdown().unwrap();
    }
}
