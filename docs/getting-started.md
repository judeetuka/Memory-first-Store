# Getting Started

> See the [README](../README.md) for a full description of the project
> and the [Benchmarks](benchmarks.md) for performance numbers.

## Usage

See [Examples](examples/index.md) for all runnable examples.

Runnable examples are registered through workspace crates:

MfS is Core-first. Start with `mfs-core`, add `mfs-neural` for hot-path
dense numeric layers, and add `mfs-store` when you need the optional
durable hot storage layer. Use `mfs-compat` only for compatibility and legacy
adapters, including SQLite-facing pieces.

Use workspace crates in this order:

1. `mfs-core`: cache/store primitives, write-behind, and WAL.
2. `mfs-neural`: dense numeric layers built on core.
3. `mfs-store`: optional durable hot storage layer built on core.
4. `mfs-compat`: compatibility and legacy adapters.

Durable hot storage examples:

```bash
cargo run -p mfs-store --release --example nosql_raw_kv        # raw bytes, versions, conflicts, delete
cargo run -p mfs-store --release --example nosql_schema_mode   # schema validation plus put/get
cargo run -p mfs-store --release --example nosql_wal_recovery  # raw WAL sync and replay
cargo run -p mfs-store --release --example nosql_checkpoint_recovery  # checkpoint plus WAL suffix replay
```

SQLite remains as a compatibility path:

```bash
cargo run -p mfs-compat --release --example schema_sqlite_flush  # schema flush SQL generation
cargo run -p mfs-compat --release --example mfs_database         # SQLite-backed schema store demo
cargo run -p mfs-compat --release --example sqlite_vfs_page_adapter  # page-store VFS adapter demo
(cd examples/sqlite_vfs && cargo run --release)                  # standalone SQLite VFS crate
```

Other cache and lane examples:

```bash
cargo run -p mfs-core --release --example read_through_cache  # read-through cache pattern
cargo run -p mfs-core --release --example wal_recovery        # crash recovery
cargo run -p mfs-core --release --example dense_counters      # atomic counters at L1 latency
```

Start with `nosql_raw_kv` for the raw hot storage path or
`nosql_schema_mode` for schema-aware documents. Use the SQLite examples
only when checking SQL persistence or the VFS compatibility layer.

### 1. Read-through cache backed by a database

The pattern: app talks to a `UserRepo`, which checks the cache first and
only falls through to the slow DB on miss. Mutations write to the cache
and a background flusher persists them in batches. Full code at
`examples/read_through_cache.rs`. The skeleton:

```rust
use memory_first_store::writeback::WriteBehindCache;
use memory_first_store::{FlushBackend, FlushRecord, Operation};
use std::sync::{Arc, Mutex};

struct UserRecord { /* … your record type … */ }

struct DbBackend { db: Arc<Mutex<MyDb>> }

impl FlushBackend<u64, UserRecord> for DbBackend {
    type Error = ();
    fn flush(&mut self, records: &[FlushRecord<u64, UserRecord>]) -> Result<(), ()> {
        // Idempotent batch upsert / delete against your real database.
        let mut db = self.db.lock().unwrap();
        for r in records {
            match (&r.value, r.op) {
                (Some(v), Operation::Put)    => db.upsert(r.key, v.as_ref().clone()),
                (None,    Operation::Delete) => db.delete(r.key),
                _ => {}
            }
        }
        Ok(())
    }
}

let cache = Arc::new(WriteBehindCache::<u64, UserRecord>::with_capacity(100_000));

// Read path: cache hit ⇒ fast; miss ⇒ DB load + load_clean.
fn get(cache: &WriteBehindCache<u64, UserRecord>, db: &Mutex<MyDb>, id: u64)
    -> Option<UserRecord>
{
    if let Some(rec) = cache.get(&id) { return Some(rec.as_ref().clone()); }
    let row = db.lock().unwrap().select(id)?;
    cache.load_clean(id, row.clone());     // populated as CLEAN — won't flush back
    Some(row)
}

// Write path: cache.put / cache.delete; the flusher persists later.
cache.put(user_id, UserRecord { /* … */ });
cache.delete(other_id);

// Background flusher (run on its own thread):
loop {
    cache.flush_idle(&mut backend, /*idle_ticks=*/ 32, /*max=*/ 10_000)?;
    std::thread::sleep(std::time::Duration::from_millis(100));
}
```

> **Caveat: read-after-delete.** A `cache.get(&id)` immediately after a
> `cache.delete(&id)` returns `None` (the entry is a tombstone), so a
> naïve read-through falls through to the DB, finds the row still
> there (delete hasn't flushed yet), and `load_clean`s it back —
> silently undoing the delete. In a real app, either flush
> synchronously after a delete, keep an application-level
> "delete-pending" set, or change the read-through to bypass the cache
> for `id`s known to be in flight. The example demonstrates the safe
> wait-for-flush variant.

### 2. WAL-based crash recovery

```rust
use memory_first_store::durability::{WalBackend, WalConfig, U64Codec, replay_into_u64_store};
use memory_first_store::MemoryFirstStore;

// On startup: rebuild the in-memory state from disk.
let store = MemoryFirstStore::<u64, u64>::new();
let recovered = replay_into_u64_store("data.wal", &store)?;
println!("recovered {recovered} records");

// During operation: the WAL is your FlushBackend.
let mut wal = WalBackend::open("data.wal", U64Codec, WalConfig::default())?;
store.flush_idle(&mut wal, /*idle_ticks=*/ 1, /*max=*/ 10_000)?;
wal.sync_now()?;   // fsync — data now survives kill -9 / power loss
```

For non-`u64` value types implement `WalCodec<K, V>` (encode/decode to
bytes); the rest of the WAL machinery (length-prefixed records,
hardware CRC32C, torn-write-tolerant replay) is type-agnostic.

### 3. Dense numeric lane

For per-key atomic counters / SNN / GNN state where each key is a
small contiguous integer:

```rust
use memory_first_store::DenseU64Lane;
use std::sync::Arc;

let lane = Arc::new(DenseU64Lane::with_len(1_000_000));
lane.store(42, 99);              // single atomic write, dirty bit set in MSB
let v = lane.load(42);           // ~0.5 ns on Zen 3 — at the L1 floor
lane.fetch_add(42, 1);           // CAS loop over the packed word

// Periodically scan and persist dirty entries:
for (idx, val) in lane.dirty_values(usize::MAX) {
    persist(idx, val)?;
    lane.mark_clean(idx);
}
```

Tradeoff: values are 63-bit (bit 63 is reserved as the dirty flag).
`DENSE_VALUE_MAX` is exposed as a constant.

## Performance notes

### Pre-size your cache

`ConcurrentMap` is **fixed-capacity** — no live resize. The default
`with_capacity(1_000_000)` is sized for the steady-state working set; if
you need substantially more, pass an explicit capacity at construction.
Historical regressions when the underlying map (then `papaya`) was
under-sized still apply: the diagnostic in `benches/probe.rs` attempts a
large preload into a cache built for 1024 entries and measures the
miss-heavy saturated-table probe cost. Use `LockFreeCache::with_capacity` and
`WriteBehindCache::with_capacity` whenever you have any estimate of the
working set. `ConcurrentMap::rebuild_with_capacity` can build a larger
snapshot for caller-controlled maintenance swaps, but it is not live resize;
writers should be quiesced if you need an exact handoff.

For Redis-like object workloads that need growable key capacity and mutable
list/hash/set/zset operations, use the opt-in `mfs_compat::object_store::
MfsMutableObjectStore` path. It avoids the fixed-capacity `ConcurrentMap` choke
point by using sharded growable maps, while the core lock-free cache lanes stay
fixed-capacity for predictable hot-path probes. Papaya-style incremental live
resizing for `ConcurrentMap` itself remains a separate design problem.

### Hold a pin guard

`cache.pin()` returns a `Pinned<'_, ...>` guard that owns a
`seize::LocalGuard`. Constructing the guard is the dominant per-call
cost; in tight read loops, hold one guard and run many reads against
it, then drop. The convenience `cache.get(&key)` exists but pins
internally on every call.

### Use `read_with` over `get` when possible

`get` returns `Arc<V>` and pays an `Arc::clone` (atomic refcount RMW).
`read_with(&key, |&V| -> R)` runs your closure inside the guard's
lifetime and skips the clone. On Skylake the difference is 36 ns vs
148 ns; on Zen 3 it's 18 ns vs 25 ns.

### Don't `get_batch` on small working sets

`get_batch` pre-hashes all keys and prefetches shard headers. On
Zen 3 with 1 M u64 keys it's slower than scalar `get` because the
working set fits in L3 and the prefetch overhead dominates. Useful for
DRAM-bound random access on bigger working sets.

## Building

```bash
make build              # compile all crates
make test               # run all tests
make fmt                # format code
make lint               # clippy lint
make doc                # build documentation
```

Benchmark harnesses are available in the development repository (not included
in this public release). See [contributing.md](contributing.md) for details.
