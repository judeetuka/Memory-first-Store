# Durability: Write-Ahead Log (WAL)

Minimum-viable write-ahead log backend for crash recovery.

## What It Is

A reference WAL implementation that turns the [`FlushBackend`](flush-backend.md) hook into a crash-recoverable persistent log. Append-only file with length-prefixed records, hardware-accelerated CRC32C integrity, and configurable sync thresholds.

Survives `kill -9` and power loss without dragging in serde, async runtimes, or external dependencies.

## When to Use

- You need durability for your in-memory cache without building a full database.
- You want crash recovery on startup: replay the WAL to rebuild state.
- You need a simple, correct foundation to layer your own segment rotation or compaction on top.

**Don't use when:**
- You need log rotation, segment management, or compaction (layer your own scheme on top).
- You need group commit across threads (use `GroupCommitWalBackend` instead).
- You need NVRAM support or async enqueue semantics (use `AsyncWalBackend`).

## On-Disk Format

The log is an append-only stream of length-prefixed records with a trailing checksum. Recovery scans from the start, validating each record, and stops at the first invalid record (truncation, torn write, or corruption).

```text
record := [u32 magic = 0x4D46_5750]   ("MFWP" little-endian)
          [u32 payload_len]
          [payload_len bytes]
          [u32 crc32c]

payload := [u8 op]                   (0 = put, 1 = delete)
           [u64 version]
           [u32 key_len]   [key_len bytes]
           [u32 value_len] [value_len bytes]   (omitted if op == delete)
```

The checksum is the **CRC-32C (Castagnoli) polynomial 0x1EDC6F41**, computed over `magic ++ payload_len ++ payload`. The `crc32c` crate uses the SSE4.2 `CRC32` instruction on x86_64 and the equivalent NEON `CRC32CB`/`CRC32CW`/`CRC32CX` on aarch64, hitting multiple GB/s of checksum bandwidth. Same polynomial used by iSCSI, Btrfs, RocksDB.

## Durability Model

`flush` appends each record to a `BufWriter` and increments a byte counter. When the counter exceeds `sync_threshold_bytes` or the record count exceeds `sync_threshold_records`, the backend issues `flush()` followed by `File::sync_data()` (`fdatasync` on Linux).

Records are not durable until the next sync. Callers who need bounded loss should call `sync_now()` explicitly after a batch they care about, or set both thresholds low.

## Recovery

Use `WalBackend::replay` to walk the log on startup and rehydrate the in-memory store. The function streams records from the file and invokes a caller-supplied closure for each valid record. Invalid trailing bytes are silently truncated (the standard "stop-at-first-bad-record" policy).

## Public API

### WalBackend (synchronous)

```rust
use mfs_core::durability::{WalBackend, WalConfig, U64Codec};

// Open or create the WAL
let mut wal = WalBackend::open("data.wal", U64Codec, WalConfig::default())?;

// Flush records (implements FlushBackend trait)
wal.flush(&[record1, record2])?;

// Force sync to disk
wal.sync_now()?;

// Replay on startup
let count = WalBackend::<u64, u64, U64Codec>::replay("data.wal", &U64Codec, |rec| {
    match rec.op {
        Operation::Put => store.load_clean(rec.key, rec.value.unwrap()),
        Operation::Delete => store.delete(rec.key),
    }
})?;
```

### AsyncWalBackend (dedicated writer thread)

```rust
use mfs_core::durability::{AsyncWalBackend, AsyncWalConfig};

let mut wal = AsyncWalBackend::open(
    "data.wal",
    U64Codec,
    WalConfig::default(),
    AsyncWalConfig::default(),
)?;

// Enqueue is non-blocking (bounded channel)
wal.flush(&[record1, record2])?;

// Wait until all previously accepted batches are durable
wal.sync_barrier()?;

// Drain and shutdown
wal.shutdown()?;
```

### GroupCommitWalBackend (durable group acknowledgments)

```rust
use mfs_core::durability::{GroupCommitWalBackend, GroupCommitWalConfig};

let wal = GroupCommitWalBackend::open(
    "data.wal",
    U64Codec,
    WalConfig::default(),
    GroupCommitWalConfig::default(),
)?;

// Get a cloneable handle
let mut handle = wal.handle();

// flush() returns only after the group is synced to disk
handle.flush(&[record1, record2])?;

wal.shutdown()?;
```

### WalConfig

```rust
struct WalConfig {
    pub sync_threshold_bytes: usize,    // default: 64 KB
    pub sync_threshold_records: usize,  // default: 256
    pub buffer_capacity_bytes: usize,   // default: 64 KB
}
```

### WalCodec trait

```rust
pub trait WalCodec<K, V> {
    fn encode_key(&self, key: &K, out: &mut Vec<u8>);
    fn encode_value(&self, value: &V, out: &mut Vec<u8>);
    fn decode_key(&self, bytes: &[u8]) -> io::Result<K>;
    fn decode_value(&self, bytes: &[u8]) -> io::Result<V>;
}
```

`U64Codec` is provided for `(u64, u64)` keys and values. Implement `WalCodec` for custom types.

## Code Example

```rust
use mfs_core::durability::{WalBackend, WalConfig, U64Codec, replay_into_u64_store};
use mfs_core::MemoryFirstStore;

// On startup: rebuild the in-memory state from disk.
let store = MemoryFirstStore::<u64, u64>::new();
let recovered = replay_into_u64_store("data.wal", &store)?;
println!("recovered {recovered} records");

// During operation: the WAL is your FlushBackend.
let mut wal = WalBackend::open("data.wal", U64Codec, WalConfig::default())?;
store.flush_idle(&mut wal, /*idle_ticks=*/ 1, /*max=*/ 10_000)?;
wal.sync_now()?;   // fsync — data now survives kill -9 / power loss
```

For non-`u64` value types, implement `WalCodec<K, V>` (encode/decode to bytes). The rest of the WAL machinery (length-prefixed records, hardware CRC32C, torn-write-tolerant replay) is type-agnostic.

## What This Is NOT

- **No log rotation or segment management.** After a long run the WAL grows unbounded. Checkpoint state externally and start a fresh log, or layer your own segment scheme on top.
- **No group commit in the synchronous backend.** `WalBackend` is buffer + threshold sync. `AsyncWalBackend` and `GroupCommitWalBackend` layer dedicated writer threads on top when callers want enqueue latency or durable group acknowledgements.
- **No integrity verification beyond per-record checksum.** File-system corruption beyond a truncated tail is not handled.

Treat this module as the smallest correct WAL: a foundation, not a finished product.

## Cross-Links

- [FlushBackend](flush-backend.md) — the trait this WAL implements
- [WriteBehindCache](writebehind-cache.md) — the cache that uses `FlushBackend` for persistence
- [Architecture](../architecture.md) — how durability fits in the crate layering
- Source: `crates/mfs-core/src/durability.rs`
