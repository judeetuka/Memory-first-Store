//! Dense key-value map: inline 8-byte values at L1 latency.
//!
//! Run with: cargo run -p mfs-neural --release --example dense_kv_map

use mfs_neural::dense_kv::DenseKvMap;

fn main() {
    let map = DenseKvMap::<u64, u64>::with_capacity(1024);

    // Insert key-value pairs (put returns Result<(), V>)
    map.put(1, 100).unwrap();
    map.put(2, 200).unwrap();
    map.put(3, 300).unwrap();

    // Read back
    assert_eq!(map.get(&1), Some(100));
    assert_eq!(map.get(&2), Some(200));

    // Update in-place (existing key, no new allocation)
    map.put(1, 999).unwrap();
    assert_eq!(map.get(&1), Some(999));

    // read_with avoids the copy
    let doubled = map.read_with(&3, |v| v * 2);
    assert_eq!(doubled, Some(600));

    // Remove
    assert_eq!(map.remove(&2), Some(200));
    assert!(map.get(&2).is_none());

    println!("map len={}, capacity={}", map.len(), map.capacity());
    println!("DenseKvMap example complete.");
}
