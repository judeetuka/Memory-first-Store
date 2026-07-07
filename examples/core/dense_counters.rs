//! Dense numeric lane: per-key atomic counters at L1 latency.
//!
//! Demonstrates the patterns the dense lane was built for — feature
//! flags, leaderboards, real-time analytics counters, neuron-state
//! arrays for SNN/GNN workloads. Every read is a single atomic load
//! (sub-nanosecond on modern CPUs); every increment is one CAS-loop
//! that also flips the dirty bit packed into bit 63 of the value.
//!
//! Run with `cargo run -p mfs-core --release --example dense_counters`.

use mfs_core::DenseU64Lane;
use std::sync::Arc;
use std::thread;

fn main() {
    // 1 million per-key counters. Indexed by u64 key, capped at
    // DenseU64Lane::with_len(N).
    let lane = Arc::new(DenseU64Lane::with_len(1_000_000));

    // Eight worker threads each bumping a shared counter at index 42.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let lane = Arc::clone(&lane);
        handles.push(thread::spawn(move || {
            for _ in 0..1_000_000 {
                lane.fetch_add(42, 1);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let v = lane.load(42);
    println!("counter at key 42 = {v} (expected 8_000_000)");
    assert_eq!(v, 8_000_000);

    // The dirty bit is set automatically by store/fetch_add. A flusher
    // (or a checkpoint job) can scan the dirty entries and persist them.
    lane.store(0, 1234);
    lane.store(7, 5678);
    let dirty = lane.dirty_values(usize::MAX);
    println!("dirty entries (sample): {} total", dirty.len());
    for (idx, val) in dirty.iter().take(5) {
        println!("  index={idx} value={val}");
    }

    // After persisting, mark them clean.
    let indices: Vec<usize> = dirty.iter().map(|(i, _)| *i).collect();
    lane.mark_many_clean(indices.iter().copied());
    println!(
        "after mark_clean: {} dirty entries left",
        lane.dirty_values(usize::MAX).len()
    );

    // The values remain readable; only the dirty flag was cleared.
    println!("counter at key 42 still = {}", lane.load(42));
}
