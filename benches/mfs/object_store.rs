use mfs_compat::object_store::{MfsMutableObjectStore, MfsObjectStore};
use mfs_compat::object_store_durability::MutableObjectStoreBundle;
use mfs_core::atomic_writeback::AtomicWriteBehindCache;
use mfs_core::durability::{WalBackend, WalConfig};
use mfs_core::slot_writeback::SlotWriteBehindCache;
use mfs_core::writeback::{WriteBehindCache, WriteBehindConfig};
use mfs_core::{FlushBackend, FlushRecord};
use mfs_db::value::{MfsValue, MfsValueCodec};
use std::collections::BTreeMap;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const COUNT: u64 = 10_000;
const HELPER_COUNT: u64 = 1_000;
const TRIALS: usize = 5;

struct Stats {
    label: &'static str,
    count: u64,
    min: Duration,
    median: Duration,
    max: Duration,
}

#[derive(Default)]
struct CountingBackend {
    records: u64,
}

impl FlushBackend<Vec<u8>, MfsValue> for CountingBackend {
    type Error = ();

    fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
        self.records += records.len() as u64;
        Ok(())
    }
}

impl Stats {
    fn print(&self) {
        let ns = |d: Duration| d.as_nanos() as f64 / self.count as f64;
        let ops = |d: Duration| self.count as f64 / d.as_secs_f64();
        println!(
            "{:<36} count={} trials={} min={:.2} ns/op median={:.2} ns/op max={:.2} ns/op (peak ops/s={:.0})",
            self.label,
            self.count,
            TRIALS,
            ns(self.min),
            ns(self.median),
            ns(self.max),
            ops(self.min),
        );
    }
}

fn measure<F>(label: &'static str, count: u64, mut body: F) -> Stats
where
    F: FnMut() -> u64,
{
    let mut samples = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let acc = body();
        let elapsed = start.elapsed();
        black_box(acc);
        samples.push(elapsed);
    }
    samples.sort();
    Stats {
        label,
        count,
        min: samples[0],
        median: samples[TRIALS / 2],
        max: samples[TRIALS - 1],
    }
}

fn config() -> WriteBehindConfig {
    WriteBehindConfig {
        initial_capacity: COUNT as usize,
        dirty_queue_capacity: COUNT as usize,
        ..WriteBehindConfig::default()
    }
}

fn key(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

fn bytes_value(seed: u64) -> MfsValue {
    let mut out = Vec::with_capacity(128);
    for i in 0..128u64 {
        out.push(seed.wrapping_add(i) as u8);
    }
    MfsValue::Bytes(out)
}

fn hash_value(seed: u64) -> MfsValue {
    let mut fields = BTreeMap::new();
    for i in 0..4u64 {
        fields.insert(
            format!("field_{i}").into_bytes(),
            seed.wrapping_add(i).to_le_bytes().to_vec(),
        );
    }
    MfsValue::Hash(fields)
}

fn temp_path(label: &str, extension: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_nanos();
    path.push(format!(
        "mfs_object_store_bench_{label}_{}_{}.{}",
        std::process::id(),
        unique,
        extension
    ));
    path
}

fn arc_bytes_values() -> Vec<Arc<MfsValue>> {
    (0..COUNT)
        .map(|i| Arc::new(bytes_value(i.wrapping_mul(17))))
        .collect()
}

fn populate_boxed() -> WriteBehindCache<Vec<u8>, MfsValue> {
    let cache = WriteBehindCache::<Vec<u8>, MfsValue>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(key(i), bytes_value(i));
    }
    drop(p);
    cache
}

fn populate_atomic() -> AtomicWriteBehindCache<Vec<u8>, MfsValue> {
    let cache = AtomicWriteBehindCache::<Vec<u8>, MfsValue>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(key(i), bytes_value(i));
    }
    drop(p);
    cache
}

fn populate_slot() -> SlotWriteBehindCache<Vec<u8>, MfsValue> {
    let cache = SlotWriteBehindCache::<Vec<u8>, MfsValue>::with_config(config());
    let p = cache.pin();
    for i in 0..COUNT {
        p.load_clean(key(i), bytes_value(i));
    }
    drop(p);
    cache
}

fn populate_object_strings() -> MfsObjectStore {
    let store = MfsObjectStore::with_config(config());
    for i in 0..COUNT {
        store.load_clean(key(i), MfsValue::String(format!("value-{i}")));
    }
    store
}

fn populate_mutable_strings() -> MfsMutableObjectStore {
    let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
    for i in 0..COUNT {
        store.load_clean(key(i), MfsValue::String(format!("value-{i}")));
    }
    store
}

fn main() {
    println!("=== MfsValue writer comparison (COUNT={COUNT}, trials={TRIALS}) ===");

    let boxed = populate_boxed();
    measure("boxed_value_read_with", COUNT, || {
        let p = boxed.pin();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= p
                .read_with(&key(i), |value| match value {
                    MfsValue::Bytes(bytes) => bytes[0] as u64,
                    _ => 0,
                })
                .expect("loaded key");
        }
        checksum
    })
    .print();
    measure("boxed_value_put_bytes", COUNT, || {
        let p = boxed.pin();
        for i in 0..COUNT {
            p.put(
                black_box(key(i)),
                black_box(bytes_value(i.wrapping_mul(11))),
            );
        }
        boxed.len() as u64
    })
    .print();

    let boxed_arc = populate_boxed();
    let boxed_arcs = arc_bytes_values();
    measure("boxed_value_put_arc_bytes", COUNT, || {
        let p = boxed_arc.pin();
        for i in 0..COUNT {
            p.put_arc(
                black_box(key(i)),
                Arc::clone(black_box(&boxed_arcs[i as usize])),
            );
        }
        boxed_arc.len() as u64
    })
    .print();

    let boxed_hash = populate_boxed();
    measure("boxed_value_put_hash", COUNT, || {
        let p = boxed_hash.pin();
        for i in 0..COUNT {
            p.put(black_box(key(i)), black_box(hash_value(i.wrapping_mul(13))));
        }
        boxed_hash.len() as u64
    })
    .print();

    let atomic = populate_atomic();
    measure("atomic_value_read_with", COUNT, || {
        let p = atomic.pin();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= p
                .read_with(&key(i), |value| match value {
                    MfsValue::Bytes(bytes) => bytes[0] as u64,
                    _ => 0,
                })
                .expect("loaded key");
        }
        checksum
    })
    .print();
    measure("atomic_value_put_bytes", COUNT, || {
        let p = atomic.pin();
        for i in 0..COUNT {
            p.put(
                black_box(key(i)),
                black_box(bytes_value(i.wrapping_mul(11))),
            );
        }
        atomic.len() as u64
    })
    .print();

    let atomic_hash = populate_atomic();
    measure("atomic_value_put_hash", COUNT, || {
        let p = atomic_hash.pin();
        for i in 0..COUNT {
            p.put(black_box(key(i)), black_box(hash_value(i.wrapping_mul(13))));
        }
        atomic_hash.len() as u64
    })
    .print();

    let slot = populate_slot();
    measure("slot_value_read_with", COUNT, || {
        let p = slot.pin();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= p
                .read_with(&key(i), |value| match value {
                    MfsValue::Bytes(bytes) => bytes[0] as u64,
                    _ => 0,
                })
                .expect("loaded key");
        }
        checksum
    })
    .print();
    measure("slot_value_put_bytes", COUNT, || {
        let p = slot.pin();
        for i in 0..COUNT {
            p.put(
                black_box(key(i)),
                black_box(bytes_value(i.wrapping_mul(11))),
            );
        }
        slot.len() as u64
    })
    .print();

    let slot_arc = populate_slot();
    let slot_arcs = arc_bytes_values();
    measure("slot_value_put_arc_bytes", COUNT, || {
        let p = slot_arc.pin();
        for i in 0..COUNT {
            p.put_arc(
                black_box(key(i)),
                Arc::clone(black_box(&slot_arcs[i as usize])),
            );
        }
        slot_arc.len() as u64
    })
    .print();

    let slot_hash = populate_slot();
    measure("slot_value_put_hash", COUNT, || {
        let p = slot_hash.pin();
        for i in 0..COUNT {
            p.put(black_box(key(i)), black_box(hash_value(i.wrapping_mul(13))));
        }
        slot_hash.len() as u64
    })
    .print();

    println!("=== MfsObjectStore helper mutations (COUNT={HELPER_COUNT}, trials={TRIALS}) ===");
    measure("object_append_bytes", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store
                .append_bytes(black_box(b"bytes".to_vec()), black_box([i as u8]))
                .expect("append bytes");
        }
        store
            .get_bytes(b"bytes")
            .expect("typed bytes read")
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0)
    })
    .print();

    measure("mutable_object_append_bytes", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store
                .append_bytes(black_box(b"bytes".to_vec()), black_box([i as u8]))
                .expect("mutable append bytes");
        }
        store
            .get_bytes(b"bytes")
            .expect("mutable typed bytes read")
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0)
    })
    .print();

    measure("object_incr_by", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        let mut value = 0i64;
        for _ in 0..HELPER_COUNT {
            value = store
                .incr_by(black_box(b"counter".to_vec()), black_box(1))
                .expect("increment integer");
        }
        value as u64
    })
    .print();

    measure("mutable_object_incr_by", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        let mut value = 0i64;
        for _ in 0..HELPER_COUNT {
            value = store
                .incr_by(black_box(b"counter".to_vec()), black_box(1))
                .expect("mutable increment integer");
        }
        value as u64
    })
    .print();

    measure("object_get_string", COUNT, || {
        let store = populate_object_strings();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= store
                .get_string(black_box(&key(i)))
                .expect("object string read")
                .map(|value| value.len() as u64)
                .unwrap_or(0);
        }
        checksum
    })
    .print();

    measure("mutable_object_get_string", COUNT, || {
        let store = populate_mutable_strings();
        let mut checksum = 0u64;
        for i in 0..COUNT {
            checksum ^= store
                .get_string(black_box(&key(i)))
                .expect("mutable string read")
                .map(|value| value.len() as u64)
                .unwrap_or(0);
        }
        checksum
    })
    .print();

    measure("object_delete_existing", COUNT, || {
        let store = populate_object_strings();
        for i in 0..COUNT {
            store.delete(black_box(key(i)));
        }
        store.stats().dirty as u64
    })
    .print();

    measure("mutable_object_delete_existing", COUNT, || {
        let store = populate_mutable_strings();
        for i in 0..COUNT {
            store.delete(black_box(key(i)));
        }
        store.stats().dirty as u64
    })
    .print();

    measure("mutable_object_grow_strings", COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(1);
        for i in 0..COUNT {
            store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
        }
        store.len() as u64
    })
    .print();

    measure("object_list_push", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store
                .list_push(black_box(b"list".to_vec()), black_box(key(i)))
                .expect("list push");
        }
        store.list_len(b"list").expect("list length") as u64
    })
    .print();

    measure("mutable_object_list_push", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store
                .list_push(black_box(b"list".to_vec()), black_box(key(i)))
                .expect("mutable list push");
        }
        store.list_len(b"list").expect("mutable list length") as u64
    })
    .print();

    measure("object_list_extend_1k", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        store
            .list_extend(black_box(b"list".to_vec()), (0..HELPER_COUNT).map(key))
            .expect("list extend");
        store.list_len(b"list").expect("list length") as u64
    })
    .print();

    measure("mutable_object_list_extend_1k", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        store
            .list_extend(black_box(b"list".to_vec()), (0..HELPER_COUNT).map(key))
            .expect("mutable list extend");
        store.list_len(b"list").expect("mutable list length") as u64
    })
    .print();

    measure("object_hash_set", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store
                .hash_set(
                    black_box(b"hash".to_vec()),
                    black_box(key(i)),
                    black_box(key(i + 1)),
                )
                .expect("hash set");
        }
        store.hash_len(b"hash").expect("hash length") as u64
    })
    .print();

    measure("mutable_object_hash_set", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store
                .hash_set(
                    black_box(b"hash".to_vec()),
                    black_box(key(i)),
                    black_box(key(i + 1)),
                )
                .expect("mutable hash set");
        }
        store.hash_len(b"hash").expect("mutable hash length") as u64
    })
    .print();

    measure("object_hash_set_many_1k", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        store
            .hash_set_many(
                black_box(b"hash".to_vec()),
                (0..HELPER_COUNT).map(|i| (key(i), key(i + 1))),
            )
            .expect("hash set many");
        store.hash_len(b"hash").expect("hash length") as u64
    })
    .print();

    measure("mutable_object_hash_set_many_1k", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        store
            .hash_set_many(
                black_box(b"hash".to_vec()),
                (0..HELPER_COUNT).map(|i| (key(i), key(i + 1))),
            )
            .expect("mutable hash set many");
        store.hash_len(b"hash").expect("mutable hash length") as u64
    })
    .print();

    measure("object_set_add", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store
                .set_add(black_box(b"set".to_vec()), black_box(key(i)))
                .expect("set add");
        }
        store.set_len(b"set").expect("set length") as u64
    })
    .print();

    measure("mutable_object_set_add", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store
                .set_add(black_box(b"set".to_vec()), black_box(key(i)))
                .expect("mutable set add");
        }
        store.set_len(b"set").expect("mutable set length") as u64
    })
    .print();

    measure("object_set_add_many_1k", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        store
            .set_add_many(black_box(b"set".to_vec()), (0..HELPER_COUNT).map(key))
            .expect("set add many");
        store.set_len(b"set").expect("set length") as u64
    })
    .print();

    measure("mutable_object_set_add_many_1k", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        store
            .set_add_many(black_box(b"set".to_vec()), (0..HELPER_COUNT).map(key))
            .expect("mutable set add many");
        store.set_len(b"set").expect("mutable set length") as u64
    })
    .print();

    measure("object_zadd", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store
                .zadd(
                    black_box(b"zset".to_vec()),
                    black_box(i as f64),
                    black_box(key(i)),
                )
                .expect("zadd");
        }
        store.zlen(b"zset").expect("sorted set length") as u64
    })
    .print();

    measure("mutable_object_zadd", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store
                .zadd(
                    black_box(b"zset".to_vec()),
                    black_box(i as f64),
                    black_box(key(i)),
                )
                .expect("mutable zadd");
        }
        store.zlen(b"zset").expect("mutable sorted set length") as u64
    })
    .print();

    measure("object_zadd_many_1k", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        store
            .zadd_many(
                black_box(b"zset".to_vec()),
                (0..HELPER_COUNT).map(|i| (i as f64, key(i))),
            )
            .expect("zadd many");
        store.zlen(b"zset").expect("sorted set length") as u64
    })
    .print();

    measure("mutable_object_zadd_many_1k", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        store
            .zadd_many(
                black_box(b"zset".to_vec()),
                (0..HELPER_COUNT).map(|i| (i as f64, key(i))),
            )
            .expect("mutable zadd many");
        store.zlen(b"zset").expect("mutable sorted set length") as u64
    })
    .print();

    measure("object_flush_counting_1k", HELPER_COUNT, || {
        let store = MfsObjectStore::with_config(config());
        for i in 0..HELPER_COUNT {
            store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
        }
        let mut backend = CountingBackend::default();
        store
            .flush_idle(&mut backend, 0, usize::MAX)
            .expect("object counting flush");
        backend.records
    })
    .print();

    measure("mutable_object_flush_counting_1k", HELPER_COUNT, || {
        let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
        for i in 0..HELPER_COUNT {
            store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
        }
        let mut backend = CountingBackend::default();
        store
            .flush_idle(&mut backend, 0, usize::MAX)
            .expect("mutable counting flush");
        backend.records
    })
    .print();

    measure("object_flush_wal_1k", HELPER_COUNT, || {
        let path = temp_path("object_flush", "mfswal");
        let records = (|| -> std::io::Result<u64> {
            let store = MfsObjectStore::with_config(config());
            for i in 0..HELPER_COUNT {
                store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
            }
            let mut wal = WalBackend::open(&path, MfsValueCodec, WalConfig::default())?;
            let records = store.flush_idle(&mut wal, 0, usize::MAX)? as u64;
            wal.sync_now()?;
            Ok(records)
        })()
        .expect("object WAL flush");
        let _ = fs::remove_file(&path);
        records
    })
    .print();

    measure("mutable_object_flush_wal_1k", HELPER_COUNT, || {
        let path = temp_path("mutable_flush", "mfswal");
        let records = (|| -> std::io::Result<u64> {
            let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
            for i in 0..HELPER_COUNT {
                store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
            }
            let mut wal = WalBackend::open(&path, MfsValueCodec, WalConfig::default())?;
            let records = store.flush_idle(&mut wal, 0, usize::MAX)? as u64;
            wal.sync_now()?;
            Ok(records)
        })()
        .expect("mutable WAL flush");
        let _ = fs::remove_file(&path);
        records
    })
    .print();

    measure("mutable_object_checkpoint_recover_1k", HELPER_COUNT, || {
        let path = temp_path("mutable_checkpoint", "mfs");
        let records = (|| -> std::io::Result<u64> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
            for i in 0..HELPER_COUNT {
                store.load_clean(key(i), MfsValue::String(format!("value-{i}")));
            }
            let checkpoint = bundle.write_checkpoint(&store)?;
            let recovered = bundle.recover(config().initial_capacity)?;
            assert_eq!(recovered.store.len(), HELPER_COUNT as usize);
            Ok(checkpoint.records as u64)
        })()
        .expect("mutable checkpoint recover");
        let _ = fs::remove_dir_all(&path);
        records
    })
    .print();

    measure("mutable_object_cold_read_through_1k", HELPER_COUNT, || {
        let path = temp_path("mutable_cold_read", "mfs");
        let hits = (|| -> std::io::Result<u64> {
            let bundle = MutableObjectStoreBundle::open(&path)?;
            let store = MfsMutableObjectStore::with_capacity(config().initial_capacity);
            for i in 0..HELPER_COUNT {
                store.set_string(black_box(key(i)), black_box(format!("value-{i}")));
            }
            assert_eq!(bundle.demote_all_to_cold(&store)?, HELPER_COUNT as usize);
            let mut hits = 0u64;
            for i in 0..HELPER_COUNT {
                if bundle
                    .get_with_cold_promotion(&store, black_box(&key(i)))?
                    .is_some()
                {
                    hits += 1;
                }
            }
            Ok(hits)
        })()
        .expect("mutable cold read-through");
        let _ = fs::remove_dir_all(&path);
        hits
    })
    .print();
}
