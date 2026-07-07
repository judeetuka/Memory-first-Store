# S3FifoCache

Bounded in-process cache with an S3-FIFO-style admission and eviction policy.

## What It Is

A separate policy-bearing cache type that implements the S3-FIFO eviction algorithm. It intentionally does **not** modify [`LockFreeCache`](lockfree-cache.md) or [`WriteBehindCache`](writebehind-cache.md), because every admission policy has read/write overhead and the raw hot path must stay policy-free.

## When to Use

- You need a bounded cache with better hit ratios than simple LRU or FIFO under scan-heavy workloads.
- You want admission control to reject one-hit wonders and protect hot entries from cold scans.
- You're willing to trade some raw throughput for better cache efficiency.

**Don't use when:**
- You need the absolute fastest reads (use `LockFreeCache` or `ConcurrentMap`).
- You need write-behind semantics (use `WriteBehindCache`).
- Your workload is purely hot-key with no scan pressure.

## Key Design

### Three FIFO Queues

- **Cold FIFO**: receives all new entries.
- **Hot FIFO**: receives entries that were referenced while in cold, or found in the ghost filter.
- **Ghost FIFO**: stores only hashes of recently evicted cold entries (no values).

### Eviction Flow

1. New entries land in the cold FIFO.
2. When cold evicts, entries with `referenced >= small_to_main_threshold` (or high frequency) are promoted to hot.
3. Entries with `referenced == 0` are evicted. Their hash goes into the ghost FIFO.
4. When a new entry's hash matches the ghost filter, it lands directly in hot (it was recently evicted and is now being re-accessed).
5. Hot entries with `referenced > 0` cycle to the back of the hot queue. Entries with `referenced == 0` are evicted.

### Admission Filter (Optional)

When `admission_enabled = true`, a TinyLFU-style frequency sketch gates admission. A newcomer must have higher estimated frequency than the eviction victim, or it's rejected outright.

**Default**: admission filter is **off**. The default `ghost25` configuration (ghost capacity = 25% of total capacity) provides the best hit-ratio-vs-speed tradeoff for most workloads.

### Admission Experiments

Five experiment variants are available for tuning admission behavior:

| Variant | Description |
|---------|-------------|
| `CapacityGate { maximum_frequency }` | Reject newcomers when cache is full and their prior frequency is below the gate |
| `WideSketch { min_width, sample_size_floor }` | Wider frequency sketch with configurable floor |
| `Packed4Bit { min_width, sample_size_floor }` | 4-bit packed counters (2 counters per byte) |
| `Doorkeeper { min_width, sample_size_floor }` | Bloom-filter doorkeeper gates first touch before backing sketch |
| `TwoCounterDecay { min_width, sample_size_floor }` | Two-counter decay sketch for frequency estimation |

These are opt-in via `S3FifoAdmissionExperiment`. The default ghost25 configuration is recommended unless you have specific tuning needs.

### Per-Shard RwLock

Each shard has its own `RwLock` so reads can run concurrently inside a shard. Reads only bump a tiny saturated reference counter; they do not mutate FIFO lists.

## Public API

### Construction

```rust
use mfs_core::s3fifo::{S3FifoCache, S3FifoConfig};

// Simple capacity-based construction
let cache = S3FifoCache::<u64, String>::with_capacity(100_000);

// Full configuration
let config = S3FifoConfig::new(100_000)
    .with_shards(16)                          // default: available_parallelism * 4
    .with_hot_ratio_percent(90)               // default: 90%
    .with_ghost_ratio_percent(25)             // default: 25%
    .with_small_to_main_threshold(1)          // default: 1
    .with_admission_filter(false);            // default: off

let cache = S3FifoCache::with_config(config);
```

### Basic Operations

```rust
// Insert
cache.insert(key: K, value: V) -> Option<Arc<V>>;

// Get
cache.get(key: &K) -> Option<Arc<V>>;

// Read with closure (avoids Arc::clone)
cache.read_with(key: &K, |&V| -> R) -> Option<R>;

// Remove
cache.remove(key: &K) -> Option<Arc<V>>;

// Metadata
cache.len() -> usize;
cache.is_empty() -> bool;
```

### Diagnostics

```rust
// Insert with timing breakdown
let result = cache.insert_diagnostics(key, value);
// result.previous: Option<Arc<V>>
// result.metrics: S3FifoOpDiagnostics

// Read with timing breakdown
let result = cache.read_with_diagnostics(key, |v| v.clone());
// result.result: Option<R>
// result.metrics: S3FifoOpDiagnostics
```

### S3FifoOpDiagnostics

```rust
struct S3FifoOpDiagnostics {
    pub rwlock_read_acquire: Duration,
    pub rwlock_write_acquire: Duration,
    pub map_lookup_read_closure: Duration,
    pub fifo_maintenance: Duration,
    pub ghost_bookkeeping: Duration,
    pub admission_bookkeeping: Duration,
}
```

## Configuration Knobs

| Knob | Default | Effect |
|------|---------|--------|
| `capacity` | (required) | Maximum entries in the cache |
| `shards` | `available_parallelism * 4` | Number of shards (rounded to power of 2) |
| `hot_ratio_percent` | 90 | Percentage of capacity allocated to hot FIFO |
| `ghost_ratio_percent` | 25 | Percentage of capacity for ghost filter |
| `small_to_main_threshold` | 1 | Reference count needed to promote cold -> hot |
| `admission_enabled` | false | Enable TinyLFU admission filter |
| `admission_experiment` | None | Select admission variant (see table above) |

## Code Example

```rust
use mfs_core::s3fifo::{S3FifoCache, S3FifoConfig};

// Create a cache with 100k capacity, 90% hot, 25% ghost
let cache = S3FifoCache::<u64, String>::with_config(
    S3FifoConfig::new(100_000)
        .with_hot_ratio_percent(90)
        .with_ghost_ratio_percent(25)
);

// Insert entries
for i in 0..50_000 {
    cache.insert(i, format!("value-{i}"));
}

// Hot items survive scans better than cold
for _ in 0..4 {
    let _ = cache.get(&1);  // bump reference count
}

// Simulate a scan (cold entries)
for i in 50_000..90_000 {
    cache.insert(i, format!("scan-{i}"));
}

// Hot entry still present
assert!(cache.get(&1).is_some());

// Optional: enable admission filter for better hit ratio under scan
let cache_with_admission = S3FifoCache::<u64, String>::with_config(
    S3FifoConfig::new(100_000)
        .with_admission_filter(true)
);
```

## Performance Notes

- Reads are `RwLock::read()` + hash map lookup + reference counter bump. No FIFO mutation on the read path.
- Writes are `RwLock::write()` + hash map insert + potential eviction. Eviction cost depends on how many cold entries need to be scanned before finding an unreferenced victim.
- The admission filter adds overhead on every insert (frequency sketch update + estimation). Only enable it if your workload has significant scan pressure and you need better hit ratios.

## Cross-Links

- [LockFreeCache](lockfree-cache.md) — the policy-free lock-free cache (faster reads, no admission control)
- [WriteBehindCache](writebehind-cache.md) — lock-free cache with write-behind persistence
- [ConcurrentMap](concurrent-map.md) — the underlying lock-free hash table
- [Architecture](../architecture.md) — how S3-FIFO fits in the crate layering
- Source: `crates/mfs-core/src/s3fifo.rs`
