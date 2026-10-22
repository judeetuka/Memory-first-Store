# SQLite Flush: Query Copy

`schema_flush_sqlite` is the concrete backend that executes the SQL the
`schema_flush` planner generates. The hot layer answers reads from memory; this
module mirrors those writes into a real SQLite file so a report, an ad-hoc
`sqlite3` session, or a second process can query them.

```rust
use std::sync::Arc;
use parking_lot::Mutex;
use mfs_compat::flush_worker::{FlushConfig, FlushWorker};
use mfs_compat::schema_flush::SchemaFlushRecord;
use mfs_compat::schema_flush_sqlite::SqliteFlushBackend;

let backend = Arc::new(Mutex::new(SqliteFlushBackend::open("query.db")?));
backend.lock().ensure_schema(&schema)?;
let worker = FlushWorker::spawn_with_config(Arc::clone(&backend), FlushConfig::default())?;

// On the write path: build a record, hand it to the worker, move on.
let record = SchemaFlushRecord::from_schema_write(
    "users", &schema, &primary, Some(document), result.lsn.expect("wal lsn").get(),
)?;
worker.enqueue(record)?;

// Before a read that must see everything:
worker.barrier()?;
let doc = backend.lock().document_by_key("users", &key)?;

// On shutdown:
worker.shutdown()?;
```

## Placement

The backend lives in `mfs-compat`, beside the planner it executes. It cannot
live in `mfs-store`: `mfs-compat` already depends on `mfs-store`, so hosting it
in the store would create the cycle `mfs-compat → mfs-store → mfs-compat`. The
hot store's own semantics -- WAL, checkpoints, recovery -- are untouched. This
is an addition, not a change to the write path.

`rusqlite` is a real dependency of `mfs-compat` for this reason.

## What gets written

Every `Put` record writes two things:

- **Typed columns**, one per schema field, so SQL can filter and join on
  individual fields without decoding a document.
- **`doc BLOB`**, the document encoded with `encode_schema_value` -- the same
  codec the store's WAL uses.

The `doc` column is the part worth calling out. The reference implementation's
generic path wrote only `(mfs_key, mfs_lsn)` and dropped the value, so a
collection with no bespoke writer flushed a row with no content. Here the
record carries `document: Option<SchemaValue>` and the planner encodes it, so
any collection flushes its value. `doc` is reserved: a schema field that maps
to it is rejected rather than silently overwritten.

Foreign keys are **not** enforced. The copy is written in flush order, which is
not dependency order, so enforcing `REFERENCES` would make a valid flush order
fail. Declarations are kept for documentation and for readers.

## Durability split

Two durability domains, deliberately separate:

| | Owns | Crash costs |
|---|---|---|
| Hot layer | Its WAL and checkpoints | Whatever the WAL had not synced |
| Query copy | The SQLite file | Whatever had not been flushed |

A crash of the copy costs only records that had not been flushed, because the
hot layer still has them. The copy is never the source of truth.

`SqliteFlushBackend` configures the copy with `journal_mode = WAL`,
`synchronous = NORMAL`, `busy_timeout = 5000`, `wal_autocheckpoint = 1000`,
`cache_size = -32000`, and `temp_store = MEMORY`.

## The LSN watermark

The copy tracks the highest LSN it has committed in `mfs_flush_state`, in the
same transaction as the batch -- so a reader can never see a watermark that
promises more than the copy holds.

The LSN is the **store-wide WAL LSN** from `WriteResult::lsn`, not a
per-document `DocumentVersion`. A per-document version would make a store-wide
watermark meaningless.

The watermark is a **high-water mark of committed records, not a contiguous
prefix**. It says how far the copy has seen, not that every LSN below it is
present. A collection that was never flushed contributes nothing while still
raising the watermark when other collections advance past it. It only ever
moves forward: a late batch with a lower LSN cannot drag the copy backwards.

## Single owner, many readers

The process that flushes owns the write connection. One connection, one
writer. Other processes open the file with their own SQLite client:

```rust
let reader = SqliteFlushBackend::open_reader("query.db")?;
```

`open_reader` is read-only and independent -- it takes no part in the flush.

## The flush worker

`flush_worker` drives the backend on a background thread. It is generic over
`B: SchemaFlushBackend + Send + 'static` with `B::Error: Display`, so tests can
inject a failing backend.

| Constant | Value | Meaning |
|---|---|---|
| `DEFAULT_BATCH_MAX` | 500 | Largest number of records in one transaction |
| `DEFAULT_INTERVAL` | 250 ms | Longest idle wait before polling again |
| `DEFAULT_BACKOFF_MAX` | 30 s | Upper bound of the backoff after a failed batch |
| `DEFAULT_BARRIER_TIMEOUT` | 10 s | Upper bound of a shutdown barrier wait |

Guarantees:

- **A batch is never dropped.** A failed batch goes back at the *front* of the
  queue, in order, and is retried with exponential backoff. A dropped batch is
  silent data loss, so the failure is reported loudly on stderr.
- **A batch is atomic.** Every record lands or none does, in one transaction,
  with the watermark moved in the same transaction.
- **The barrier is answered only after a successful commit** of everything
  queued before it. That is what makes a graceful shutdown safe. A barrier is
  never answered after a failed batch.
- **The worker is loud.** Startup and every failure go to stderr -- the only
  channel available without pulling in a logging dependency. A flush worker
  that goes quiet must always be noticeable.

`shutdown()` drains the queue first, then stops. `Drop` closes the queue and
lets the worker flush what is left, but does not join: a failed batch backs off
for as long as the backoff allows, and a destructor must not block on that.

## Cost on the hot path

Measured on a 14-core machine, release build, 5 interleaved rounds of 20,000
writes to a fresh WAL-backed store each round, fastest median per scenario:

| | ns/write |
|---|---|
| Build the flush record | 360 |
| Enqueue to a live worker | 4 |
| **Total added to the hot path** | **~358 (12.6% of a 2851 ns hot write)** |

A bare `mpsc` send on the same machine is 52 ns, so the enqueue is within noise
of the floor: the queue charges almost nothing. A worker flushing to disk on
another core is indistinguishable, on the writer's thread, from one that writes
nothing.

Nearly all of the cost is building the record -- encoding the document for the
`doc` column. That is the honest price of requirement two: the document is
written, so it must be encoded.

Reproduce with `cargo run --release -p mfs-compat --example flush_hot_path_cost`.

## Loss and duplication

What the copy can and cannot lose:

- **Records not yet flushed are lost from the copy** on a crash. They are not
  lost from the hot layer, which still holds them.
- **A committed batch is not lost.** It is in the SQLite file with
  `synchronous = NORMAL`.
- **A duplicate flush is possible.** A batch committed, then the process
  crashed before the queue acknowledged it, will be flushed again on restart.
  Every record is an `INSERT ... ON CONFLICT ... DO UPDATE` guarded by
  `mfs_lsn`, so a duplicate at the same or lower LSN is a no-op. Re-flushing is
  idempotent, which is why this is safe.
- **The copy can fall behind** by an arbitrary amount if the backend is broken,
  because the worker backs off instead of dropping. `queued_records()` reports
  how far behind. A barrier will not return while the backlog is uncommitted.
- **Deletes can leave rows behind** if the delete record is never flushed. The
  copy keeps the last `Put` and its document.

## Testing

| File | Covers |
|---|---|
| `tests/sqlite_flush.rs` | Hot write visible, document round-trip, barrier drains, watermark advances, delete leaves the copy |
| `tests/sqlite_flush_failure.rs` | Failed batch requeued with nothing lost, aborted batch atomic, stale write does not move the copy backwards, hot write survives a flushing failure |
| `tests/schema_flush_plan.rs` | Planner SQL and value generation, including the `doc` column |

## Examples

- `schema_sqlite_flush` -- the full flow: open, ensure schema, spawn, write,
  barrier, read back a join, delete, shutdown.
- `flush_hot_path_cost` -- the measurement above.
