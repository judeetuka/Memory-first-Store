//! Dense write-behind map: 8-byte values with write-behind durability.
//!
//! Run with: cargo run -p mfs-neural --release --example dense_write_behind

use mfs_core::{FlushBackend, FlushRecord};
use mfs_neural::dense_writeback_map::DenseWriteBehindMap;

struct CountingBackend {
    flushed: usize,
}

impl FlushBackend<u64, u64> for CountingBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, u64>]) -> Result<(), ()> {
        self.flushed += records.len();
        Ok(())
    }
}

fn main() {
    let map = DenseWriteBehindMap::<u64, u64>::with_capacity(1024);

    // Write data (marks entries as dirty, returns version)
    map.put(1, 100);
    map.put(2, 200);
    map.load_clean(3, 300); // loaded as clean (won't flush)

    // Read back
    assert_eq!(map.get(&1), Some(100));
    assert_eq!(map.get(&3), Some(300));

    // Flush dirty entries through the backend
    let mut backend = CountingBackend { flushed: 0 };
    let flushed = map.flush_idle(&mut backend, 0, 10).unwrap();
    println!("Flushed {flushed} records ({} total)", backend.flushed);

    // Stats
    let stats = map.stats();
    println!(
        "map len={}, dirty={}, clock={}",
        stats.len, stats.dirty, stats.logical_clock
    );
    println!("DenseWriteBehindMap example complete.");
}
