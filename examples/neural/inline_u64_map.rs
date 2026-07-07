//! Inline u64-to-u64 map: seqlock-based, no per-write allocation.
//!
//! Run with: cargo run -p mfs-core --release --example inline_u64_map

use mfs_core::inline_map::{InlineU64Map, InsertOutcome};

fn main() {
    let map = InlineU64Map::with_capacity(1024);

    // Insert (first time)
    let outcome = map.insert(1, 100);
    println!("insert(1, 100): {:?}", outcome);
    assert_eq!(outcome, InsertOutcome::Inserted);

    // Insert again (replaces existing)
    let outcome = map.insert(1, 200);
    println!("insert(1, 200): {:?}", outcome);
    assert_eq!(outcome, InsertOutcome::Replaced);

    // Read back
    assert_eq!(map.get(1), Some(200));

    // Update existing in-place, returns old value
    assert_eq!(map.update(1, 999), Some(200));
    assert_eq!(map.get(1), Some(999));

    // Remove
    assert_eq!(map.remove(1), Some(999));
    assert!(map.get(1).is_none());

    println!("map len={}, capacity={}", map.len(), map.capacity());
    println!("InlineU64Map example complete.");
}
