//! Bounded-tail memory reclamation primitives, opt-in alternative to the
//! default crossbeam-epoch reclamation used by
//! [`crate::concurrent_map::ConcurrentMap`].
//!
//! Available only with the `bounded-reclaim` feature flag.
//!
//! ## Why a separate path
//!
//! `crossbeam-epoch` minimises mean latency but has an unbounded p99.9
//! tail: a single preempted reader holding an epoch pin blocks
//! reclamation across all writers in the same epoch. Measured tails
//! under hot-key write contention reach 82.8 µs on T460 and 42.2 µs vs
//! foyer-memory's 10.3 µs in the head-to-head bench from
//! `benches/foyer/memory_vs_mfs.rs`.
//!
//! This module uses the **hyaline** reclamation scheme via the
//! [`seize`](https://docs.rs/seize) crate. Hyaline reference-counts
//! retired-object batches, not individual reads — read overhead is a
//! single atomic load (no SeqCst fence), and memory is bounded like
//! hazard pointers.
//!
//! ## When to use this
//!
//! Hot-key workloads with strict p99.9 SLAs. The default crossbeam-epoch
//! path stays the recommended choice for everything else; this module
//! adds a *parallel* implementation, it does not replace the default.
//!
//! ## API
//!
//! [`BoundedCell<T>`] is a single atomic pointer slot whose stored value
//! is reclaimed via seize when no guard references it. [`BoundedCell::guard`]
//! returns a [`BoundedGuard`] that derefs to `&T`. Writers call
//! [`BoundedCell::swap`] to install a new value and defer the old
//! value's reclamation.
//!
//! ```
//! # #[cfg(feature = "bounded-reclaim")]
//! # fn main() {
//! use mfs_core::bounded_reclaim::BoundedCell;
//!
//! let cell = BoundedCell::new(42u64);
//! {
//!     let g = cell.guard().expect("cell is non-empty");
//!     assert_eq!(*g, 42);
//! }
//! cell.swap(99);
//! {
//!     let g = cell.guard().expect("cell is non-empty");
//!     assert_eq!(*g, 99);
//! }
//! # }
//! # #[cfg(not(feature = "bounded-reclaim"))]
//! # fn main() {}
//! ```
//!
//! ## Scope of this module
//!
//! This is the foundation primitive only — a full `ConcurrentMapBounded`
//! type that mirrors [`crate::concurrent_map::ConcurrentMap`] on top of
//! [`seize::Collector`] is the next-session deliverable per
//! `Changelog/memory-first-store/phase4-hazard-pointers-plan.md`.

use seize::{Collector, Guard, LocalGuard, reclaim};
use std::ops::Deref;
use std::sync::atomic::{AtomicPtr, Ordering};

/// A single atomic pointer slot with hyaline-protected reads and
/// retire-based writes.
///
/// Each `BoundedCell` owns its own [`Collector`]; retire batches are
/// isolated to the cell. For data structures with many cells that
/// should share a reclamation domain (and therefore amortise retire
/// batches), construct a single `Collector` and use the lower-level
/// `seize` API directly.
///
/// `T: Send + Sync + 'static` because the retired pointer may be
/// reclaimed from any thread that holds the last reference to its
/// retire batch.
pub struct BoundedCell<T>
where
    T: Send + Sync + 'static,
{
    collector: Collector,
    ptr: AtomicPtr<T>,
}

impl<T> BoundedCell<T>
where
    T: Send + Sync + 'static,
{
    /// Construct a cell initialised with `value`.
    pub fn new(value: T) -> Self {
        Self {
            collector: Collector::new(),
            ptr: AtomicPtr::new(Box::into_raw(Box::new(value))),
        }
    }

    /// Acquire a guarded read. Returns `None` if the cell is empty
    /// (only possible after [`take`](Self::take) — the safe
    /// constructors and [`swap`](Self::swap) always leave a value).
    ///
    /// The returned guard holds a [`LocalGuard`] that keeps the
    /// current thread marked active until the guard drops. Subsequent
    /// retires on this cell's collector will not free the protected
    /// allocation until this guard (and all other concurrent guards)
    /// are released.
    pub fn guard(&self) -> Option<BoundedGuard<'_, T>> {
        let guard = self.collector.enter();
        let ptr = guard.protect(&self.ptr, Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }
        Some(BoundedGuard {
            _guard: guard,
            value: unsafe { &*ptr },
        })
    }

    /// Replace the cell's value with `new` and defer reclamation of
    /// the old value. The old value will be freed by `seize` once no
    /// guard in this collector references it.
    pub fn swap(&self, new: T) {
        let new_ptr = Box::into_raw(Box::new(new));
        let guard = self.collector.enter();
        let old = self.ptr.swap(new_ptr, Ordering::AcqRel);
        if !old.is_null() {
            unsafe { guard.defer_retire(old, reclaim::boxed::<T>) };
        }
    }

    /// Atomically take the value out of the cell, leaving it empty.
    /// Subsequent [`guard`](Self::guard) calls return `None` until the
    /// next [`swap`](Self::swap). The taken value is dropped once no
    /// guard references it.
    pub fn take(&self) {
        let guard = self.collector.enter();
        let old = self.ptr.swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !old.is_null() {
            unsafe { guard.defer_retire(old, reclaim::boxed::<T>) };
        }
    }
}

impl<T> Drop for BoundedCell<T>
where
    T: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let raw = *self.ptr.get_mut();
        if !raw.is_null() {
            unsafe {
                let _ = Box::from_raw(raw);
            }
        }
    }
}

// Safety: BoundedCell owns its Collector and its AtomicPtr; all
// operations on the cell are routed through atomic loads/stores
// through the collector's reclamation discipline. T: Send + Sync is
// the same bound `Arc<T>` uses for sharing across threads.
unsafe impl<T> Send for BoundedCell<T> where T: Send + Sync + 'static {}
unsafe impl<T> Sync for BoundedCell<T> where T: Send + Sync + 'static {}

/// Guard returned by [`BoundedCell::guard`]. Derefs to `&T`. While the
/// guard is alive, the underlying value cannot be reclaimed.
pub struct BoundedGuard<'cell, T>
where
    T: Send + Sync + 'static,
{
    _guard: LocalGuard<'cell>,
    value: &'cell T,
}

impl<'cell, T> Deref for BoundedGuard<'cell, T>
where
    T: Send + Sync + 'static,
{
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn new_and_guard_returns_value() {
        let cell = BoundedCell::new(42u64);
        let g = cell.guard().expect("non-empty");
        assert_eq!(*g, 42);
    }

    #[test]
    fn swap_replaces_value() {
        let cell = BoundedCell::new(1u64);
        cell.swap(2);
        let g = cell.guard().expect("non-empty");
        assert_eq!(*g, 2);
    }

    #[test]
    fn old_guard_remains_valid_after_swap() {
        let cell = BoundedCell::new(100u64);
        let g_old = cell.guard().expect("non-empty");
        cell.swap(200);
        assert_eq!(*g_old, 100, "old guard still sees old value");
        let g_new = cell.guard().expect("non-empty");
        assert_eq!(*g_new, 200, "new guard sees new value");
    }

    #[test]
    fn take_leaves_cell_empty() {
        let cell = BoundedCell::new(7u64);
        cell.take();
        assert!(cell.guard().is_none(), "cell is empty after take");
        cell.swap(8);
        let g = cell.guard().expect("non-empty after re-swap");
        assert_eq!(*g, 8);
    }

    #[test]
    fn concurrent_readers_and_writer_safe() {
        let cell = Arc::new(BoundedCell::new(0u64));
        let stop = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let c = Arc::clone(&cell);
            let s = Arc::clone(&stop);
            readers.push(thread::spawn(move || {
                let mut max_seen = 0u64;
                while s.load(Ordering::Relaxed) == 0 {
                    let g = c.guard().expect("non-empty");
                    let v = *g;
                    if v > max_seen {
                        max_seen = v;
                    }
                }
                max_seen
            }));
        }
        let writer_cell = Arc::clone(&cell);
        let writer_stop = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            for i in 1..1000u64 {
                writer_cell.swap(i);
            }
            writer_stop.store(1, Ordering::Relaxed);
        });
        writer.join().unwrap();
        for r in readers {
            let v = r.join().unwrap();
            assert!(v <= 999, "reader observed impossible value");
        }
        let final_g = cell.guard().expect("non-empty");
        assert_eq!(*final_g, 999, "writer's last value visible");
    }

    #[test]
    fn drop_does_not_leak() {
        let cell = BoundedCell::new(vec![1u64, 2, 3]);
        cell.swap(vec![4, 5, 6]);
        drop(cell);
    }
}
