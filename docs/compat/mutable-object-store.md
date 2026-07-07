# MfsMutableObjectStore: Growable Object Store with TTL and Cold Tier

`MfsMutableObjectStore` is the opt-in growable alternative to
`MfsObjectStore`. Where the boxed store uses a fixed-capacity
`ConcurrentMap`, the mutable store uses sharded growable `HashMap`s behind
per-shard mutexes. This trades lock-free reads for unlimited key capacity,
in-place mutation of composite values, and TTL/TTI expiry.

## What it is

A sharded, growable object store that supports:

- All the same Redis-like data types as `MfsObjectStore` (strings, lists,
  hashes, sets, sorted sets).
- In-place mutation of list/hash/set/sorted-set elements without cloning
  the entire collection on every write.
- Per-key TTL (time-to-live) and TTI (time-to-idle) expiry.
- Cold-tier demotion: idle keys get written to on-disk generations and
  evicted from the hot shard. Reads promote them back on demand.
- WAL + checkpoint persistence through `MutableObjectStorePersistence`.

## When to use it

- Your key count grows beyond what you can pre-allocate.
- You need per-key expiry (TTL or TTI).
- You do heavy mutation on lists, hashes, or sorted sets and want in-place
  updates instead of clone-and-replace.
- You want automatic cold-tier demotion for idle keys.

## When to stick with MfsObjectStore

- Your working set is bounded and fits in a pre-allocated capacity.
- You need lock-free reads (the mutable store takes a per-shard mutex on
  every read).
- You don't need expiry or cold tier.

## Construction

```rust
use mfs_compat::object_store::MfsMutableObjectStore;

let store = MfsMutableObjectStore::with_capacity(100_000);
```

The capacity is a hint for initial shard sizing. The store grows as needed.

## Scalar operations

The scalar API mirrors `MfsObjectStore`:

```rust
store.set_string(b"name".to_vec(), "Ada");
store.set_integer(b"counter".to_vec(), 42);
store.set_bytes(b"blob".to_vec(), vec![0xDE, 0xAD]);
store.set_json_bytes(b"doc".to_vec(), br#"{"key":"value"}"#);

let name = store.get_string(b"name").unwrap();
let count = store.get_integer(b"counter").unwrap();
```

### TTL and TTI

```rust
// Key expires 1000 ticks after creation, regardless of access.
store.put_with_ttl_ticks(b"session".to_vec(), MfsValue::String("abc".into()), 1000)?;

// Key expires 500 ticks after last access.
store.put_with_tti_ticks(b"cache".to_vec(), MfsValue::String("xyz".into()), 500)?;

// Both: expires at min(creation + ttl, last_touch + tti).
store.put_with_expiry_ticks(b"combo".to_vec(), MfsValue::Integer(1), 1000, 500)?;
```

Expiry is checked lazily on read and eagerly during flush and tiering
operations. Call `store.expire()` to sweep all expired keys.

### Expiry metadata

```rust
use mfs_compat::object_store::MutableObjectExpiryMeta;

let meta: Option<MutableObjectExpiryMeta> = store.expiry_meta(b"session");
// meta.version, meta.last_touch, meta.expires_at, meta.tti_ticks
```

## List, hash, set, and sorted-set operations

The mutable store supports the same mutation API as `MfsObjectStore`, but
mutates in place rather than cloning the entire collection:

```rust
// Lists: push, extend, pop, range, index, len
store.list_push(b"tasks".to_vec(), b"first".to_vec()).unwrap();
store.list_extend(b"tasks".to_vec(), vec![b"second", b"third"]).unwrap();
let front = store.list_pop_front(b"tasks".to_vec()).unwrap();
let slice = store.list_range(b"tasks", 0, -1).unwrap();

// Hashes: set, set_many, get, del, len, get_all, exists
store.hash_set(b"user:1".to_vec(), b"name".to_vec(), b"Ada".to_vec()).unwrap();
let name = store.hash_get(b"user:1", b"name").unwrap();

// Sets: add, add_many, remove, contains, len, members
store.set_add(b"online".to_vec(), b"alice".to_vec()).unwrap();
let has = store.set_contains(b"online", b"alice").unwrap();

// Sorted sets: zadd, zadd_many, zscore, zrange, zrem, zlen
store.zadd(b"board".to_vec(), 100.0, b"alice".to_vec()).unwrap();
let score = store.zscore(b"board", b"alice").unwrap();
```

The key difference: `list_push` on `MfsMutableObjectStore` mutates the
`VecDeque` in place. On `MfsObjectStore`, it clones the entire `Vec`,
appends, and re-inserts.

## Load-clean and eviction

```rust
// Load without marking dirty (won't flush back).
store.load_clean(b"key".to_vec(), MfsValue::String("from-db".into()));

// Versioned clean load (for recovery).
store.load_clean_versioned(b"key".to_vec(), MfsValue::Integer(1), 42);

// Clean delete (for recovery).
store.load_clean_delete(b"key".to_vec());

// Evict a clean key from the hot shard (for cold-tier demotion).
let evicted = store.evict_clean(b"key");
```

## Flush

```rust
use mfs_core::{FlushBackend, FlushRecord};

struct MyBackend;
impl FlushBackend<Vec<u8>, MfsValue> for MyBackend {
    type Error = std::io::Error;
    fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
        Ok(())
    }
}

let mut backend = MyBackend;
let flushed = store.flush_idle(&mut backend, 32, 10_000)?;
```

The flush path drains per-shard dirty queues, version-checks each entry,
skips entries that haven't been idle long enough, and re-queues on backend
error. Delete tombstones are cleaned up after successful flush.

## Persistence: MutableObjectStorePersistence

The persistence bundle wraps a WAL, checkpoint directory, and cold-tier
storage into a single object:

```rust
use mfs_compat::object_store_durability::{
    MutableObjectStorePersistence, MutableObjectStoreBundle,
    TieringPolicy, MutableObjectStoreBundleOptions,
};

// Open or create a persistence bundle.
let mut persistence = MutableObjectStorePersistence::open("/var/data/mfs-objects")?;

// Flush idle entries to the WAL.
persistence.flush_idle(&store, 32, 10_000)?;

// Force sync.
persistence.sync_now()?;

// Checkpoint: snapshot all live keys, reset the WAL.
let checkpoint = persistence.checkpoint_and_reset_wal(&store)?;

// Recover from disk.
let recovery = persistence.recover(100_000)?;
let store = recovery.store;
// recovery.checkpoint — the checkpoint that was loaded (if any).
// recovery.wal_records — number of WAL records replayed on top.
```

### Recovery flow

1. Load the latest checkpoint (if any) into a fresh store.
2. Replay WAL records with version > checkpoint LSN.
3. Expiry metadata is stored alongside objects in the WAL using a
   `\0mfs:object-meta:v1\0` key prefix.

### Cold tier

```rust
use mfs_compat::object_store_durability::TieringPolicy;

let policy = TieringPolicy {
    idle_threshold_ticks: 1024,    // key must be idle for this many ticks
    max_records: 128,              // max records to demote per call
    hot_capacity_soft_limit: None, // optional: only demote if hot_len > limit
    min_clean_age_ticks: 1,        // key must be clean for at least this many ticks
};

let report = persistence.demote_by_policy(&store, policy)?;
// report.demoted       — keys written to cold generation and evicted
// report.skipped_dirty — keys with pending dirty entries
// report.skipped_recent — keys not idle long enough
// report.skipped_capacity — keys above the selection limit
```

Cold data is stored in append-only generation files under
`tiers/cold/`. Each generation has a data file (`.mfsobj`) and an index
file (`.mfsidx`). A cold manifest tracks generations and tombstones.

### Cold promotion on read

```rust
// Read-through: check hot store first, then promote from cold tier.
let value = persistence.get_value_with_cold_promotion(&store, b"key")?;

// Typed variant.
let string = persistence.get_string_with_cold_promotion(&store, b"key")?;
```

### Cold GC

```rust
let gc_report = persistence.bundle().gc_cold_tier(&store)?;
// gc_report.generations_removed — old generations compacted away
// gc_report.records_dropped_expired — expired records dropped
// gc_report.bytes_freed — disk space reclaimed
```

GC scans all cold generations newest-first, keeps the first live version
of each key, drops expired records, compacts survivors into a new
generation, and removes old generation files.

## Tiering policy vs. boxed store

`MfsObjectStore` has no tiering. All keys live in the fixed-capacity
`ConcurrentMap` until explicitly deleted or flushed to a backend.

`MfsMutableObjectStore` adds:

- **Idle-based demotion**: keys not touched for `idle_threshold_ticks` are
  candidates for cold storage.
- **Clean-age guard**: keys must be clean (no pending dirty entries) for
  `min_clean_age_ticks` before demotion.
- **Capacity-aware selection**: if `hot_capacity_soft_limit` is set,
  demotion only happens when the hot store exceeds the limit.
- **Dirty-key exclusion**: keys with unflushed mutations are never
  demoted.

## Stats

```rust
let stats = store.stats();
// stats.len           — number of live keys in hot shards
// stats.dirty         — number of pending dirty entries
// stats.logical_clock — current logical clock tick

let len = store.len();
let empty = store.is_empty();
let hwm = store.durable_high_water_mark();
```

## Cross-links

- [Object Store](object-store.md) for the fixed-capacity, lock-free-read variant.
- [Schema Store](schema-store.md) for schema-validated documents.
- [SQLite VFS](sqlite-vfs.md) for the page store and VFS adapter.
