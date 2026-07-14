# MfsObjectStore: Redis-like Facade

`MfsObjectStore` wraps a `WriteBehindCache<Vec<u8>, MfsValue>` and exposes a
Redis-shaped API: typed getters and setters for strings, integers, bytes, and
JSON, plus mutation operations for lists, hashes, sets, and sorted sets.

## What it is

A fixed-capacity, lock-free-read object store. Under the hood, reads go
through `WriteBehindCache` (which uses the in-house `ConcurrentMap`), so
they're lock-free and return `Arc<MfsValue>` without cloning the inner data.
Writes acquire a per-key striped mutex, then call `try_put` on the underlying
cache. Dirty entries flush through any `FlushBackend<Vec<u8>, MfsValue>` you
provide.

## When to use it

- You want Redis data structures in-process, with no network round-trip.
- Your working set fits in a pre-allocated capacity.
- You need write-behind persistence to a custom backend (WAL, database,
  remote service).
- Read-heavy workloads where lock-free reads matter.

## When to use MfsMutableObjectStore instead

- You need growable key capacity beyond the initial allocation.
- You need TTL or TTI expiry on keys.
- You need in-place mutation of list/hash/set/sorted-set elements without
  cloning the entire collection on every write.
- You need cold-tier demotion and promotion.

See [Mutable Object Store](mutable-object-store.md).

## Construction

```rust
use mfs_compat::object_store::MfsObjectStore;

// Fixed capacity, default hasher.
let store = MfsObjectStore::with_capacity(100_000);

// Custom config.
use mfs_core::writeback::WriteBehindConfig;
let store = MfsObjectStore::with_config(WriteBehindConfig {
    initial_capacity: 50_000,
    ..WriteBehindConfig::default()
});
```

## Scalar operations

### Set and get typed values

```rust
// Strings
store.set_string(b"user:name".to_vec(), "Ada");
let name: Option<String> = store.get_string(b"user:name").unwrap();

// Integers
store.set_integer(b"counter".to_vec(), 42);
let count: Option<i64> = store.get_integer(b"counter").unwrap();

// Raw bytes
store.set_bytes(b"blob".to_vec(), vec![0xDE, 0xAD]);
let blob: Option<Vec<u8>> = store.get_bytes(b"blob").unwrap();

// JSON (stored as raw bytes)
store.set_json_bytes(b"doc".to_vec(), br#"{"key":"value"}"#);
```

### Generic put and get

```rust
use mfs_store::value::MfsValue;

store.put(b"key".to_vec(), MfsValue::String("hello".into()));
let value: Option<Arc<MfsValue>> = store.get(b"key");

// Zero-copy read without Arc clone:
let len = store.read_with(b"key", |v| match v {
    MfsValue::String(s) => s.len(),
    _ => 0,
});
```

### Delete and load_clean

```rust
store.delete(b"key".to_vec());

// Load from a backend without marking dirty (won't flush back).
store.load_clean(b"key".to_vec(), MfsValue::String("from-db".into()));
```

### Atomic increment and append

```rust
// Atomic increment. Returns the new value.
let new_count = store.incr_by(b"counter".to_vec(), 5).unwrap();

// Append bytes to an existing bytes value (or create if missing).
store.append_bytes(b"log".to_vec(), b"entry\n").unwrap();
```

Both operations are serialized per-key through the striped mutation locks.

## List operations

```rust
store.set_list(b"tasks".to_vec(), vec![b"a".to_vec(), b"b".to_vec()]);

store.list_push(b"tasks".to_vec(), b"c".to_vec()).unwrap();
store.list_extend(b"tasks".to_vec(), vec![b"d", b"e"]).unwrap();

let front = store.list_pop_front(b"tasks".to_vec()).unwrap();
let back  = store.list_pop_back(b"tasks".to_vec()).unwrap();

let len   = store.list_len(b"tasks").unwrap();
let slice = store.list_range(b"tasks", 0, -1).unwrap();
let item  = store.list_index(b"tasks", 0).unwrap();
```

Negative indexes work like Redis: `-1` is the last element.

## Hash operations

```rust
use std::collections::BTreeMap;

let mut fields = BTreeMap::new();
fields.insert(b"name".to_vec(), b"Ada".to_vec());
fields.insert(b"age".to_vec(), b"37".to_vec());
store.set_hash(b"user:1".to_vec(), fields);

store.hash_set(b"user:1".to_vec(), b"email".to_vec(), b"ada@example.com".to_vec()).unwrap();
store.hash_set_many(b"user:1".to_vec(), [(b"city".to_vec(), b"London".to_vec())]).unwrap();

let name    = store.hash_get(b"user:1", b"name").unwrap();
let all     = store.hash_get_all(b"user:1").unwrap();
let exists  = store.hash_exists(b"user:1", b"email").unwrap();
let len     = store.hash_len(b"user:1").unwrap();
let removed = store.hash_del(b"user:1".to_vec(), b"city".to_vec()).unwrap();
```

## Set operations

```rust
use std::collections::BTreeSet;

let mut members = BTreeSet::new();
members.insert(b"alice".to_vec());
members.insert(b"bob".to_vec());
store.set_set(b"online".to_vec(), members);

store.set_add(b"online".to_vec(), b"carol".to_vec()).unwrap();
store.set_add_many(b"online".to_vec(), vec![b"dave", b"eve"]).unwrap();

let has    = store.set_contains(b"online", b"alice").unwrap();
let len    = store.set_len(b"online").unwrap();
let all    = store.set_members(b"online").unwrap();
let removed = store.set_remove(b"online".to_vec(), b"bob".to_vec()).unwrap();
```

## Sorted set operations

```rust
use mfs_store::value::SortedSetEntry;

store.set_sorted_set(b"board".to_vec(), vec![
    SortedSetEntry { score: 100.0, member: b"alice".to_vec() },
    SortedSetEntry { score: 200.0, member: b"bob".to_vec() },
]).unwrap();

store.zadd(b"board".to_vec(), 150.0, b"carol".to_vec()).unwrap();
store.zadd_many(b"board".to_vec(), [(300.0, b"dave".to_vec())]).unwrap();

let score   = store.zscore(b"board", b"alice").unwrap();
let members = store.zrange(b"board", 0, -1).unwrap();
let len     = store.zlen(b"board").unwrap();
let removed = store.zrem(b"board".to_vec(), b"bob".to_vec()).unwrap();
```

Scores must be finite (`f64::NAN` and `f64::INFINITY` are rejected).

## Write-behind flush

```rust
use mfs_core::{FlushBackend, FlushRecord};

struct MyBackend { /* your persistence target */ }

impl FlushBackend<Vec<u8>, MfsValue> for MyBackend {
    type Error = std::io::Error;
    fn flush(&mut self, records: &[FlushRecord<Vec<u8>, MfsValue>]) -> Result<(), Self::Error> {
        // Persist each record to your backend.
        Ok(())
    }
}

let mut backend = MyBackend { /* ... */ };
let flushed = store.flush_idle(&mut backend, 32, 10_000)?;
```

`flush_idle` drains entries that have been idle for at least `idle_ticks`
logical clock ticks and haven't been modified since they were enqueued.

### Auto-flusher

```rust
use mfs_core::writeback::AutoFlusherConfig;

let auto_flusher = store.spawn_auto_flusher(
    |_batch_size| MyBackend { /* fresh backend per invocation */ },
    AutoFlusherConfig::default(),
);
// Runs on a background thread. Drop to stop.
```

## Error handling

All mutation operations return `Result<_, ObjectStoreError>`:

```rust
pub enum ObjectStoreError {
    WrongType { expected: &'static str, actual: ValueTag },
    InvalidValue(&'static str),
    CapacityFull,
}
```

- `WrongType`: you called a list operation on a string key, or similar.
- `InvalidValue`: sorted set score is NaN or infinity, integer overflow.
- `CapacityFull`: the underlying `ConcurrentMap` is at capacity.

## Stats

```rust
let stats = store.stats();
// stats.len    — number of live keys
// stats.dirty  — number of entries waiting to flush
// stats.logical_clock — current logical clock tick
```

## Cross-links

- [Mutable Object Store](mutable-object-store.md) for the growable variant
  with TTL/TTI and cold tier.
- [Schema Store](schema-store.md) for schema-validated documents.
- [mfs-core: WriteBehindCache](../core/overview.md) for the underlying cache.
