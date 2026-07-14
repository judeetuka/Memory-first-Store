# mfs-compat: Compatibility and Legacy Adapters

`mfs-compat` sits on top of `mfs-core` and `mfs-store`, providing higher-level
APIs that mimic familiar external systems. If your application already thinks
in Redis data structures, SQL tables, or SQLite page layouts, this crate lets
you keep that mental model while running entirely in-process.

## What's inside

| Module | Type | Purpose |
|---|---|---|
| [`object_store`](object-store.md) | `MfsObjectStore` | Redis-like facade over `WriteBehindCache`. Fixed capacity, lock-free reads, write-behind flush. |
| [`object_store`](mutable-object-store.md) | `MfsMutableObjectStore` | Growable variant with TTL/TTI expiry, in-place mutation of lists/hashes/sets/sorted sets, and cold-tier demotion. |
| [`object_store_durability`](mutable-object-store.md) | `MutableObjectStorePersistence` | WAL + checkpoint + cold-tier persistence bundle for `MfsMutableObjectStore`. |
| [`schema_store`](schema-store.md) | `SchemaStore` | Schema-validated document store with secondary indexes, unique constraints, and foreign-key references. |
| [`schema_flush`](schema-store.md) | `SchemaFlushBackend` | SQL generation helpers that turn schema flush records into CREATE TABLE, UPSERT, and DELETE statements. |
| [`page_store`](sqlite-vfs.md) | `MfsPageStore` / `InMemoryPageStore` | Byte-addressable file store with advisory locking. The low-level surface database adapters plug into. |
| [`page_vfs`](sqlite-vfs.md) | `MfsPageVfs` | SQLite VFS-shaped adapter over `MfsPageStore`. Name-to-FileId namespace, connection-scoped locks. |

## When to reach for mfs-compat

You're building an in-process cache or store and one of these applies:

- You need Redis data structures (strings, lists, hashes, sets, sorted sets)
  without running a Redis server.
- You want schema-validated documents with secondary indexes, but you don't
  need a full SQL engine.
- You're embedding SQLite-shaped storage and want to control the page I/O
  layer yourself.
- You need to flush in-memory state to SQL tables with LSN-guarded upserts.

## When to skip it

- You only need raw concurrent maps or lock-free caches. Stay in
  [`mfs-core`](../core/overview.md).
- You need dense 8-byte numeric lanes. Go to
  [`mfs-neural`](../neural/overview.md).
- You want the raw hot storage layer without schema validation. Use
  [`mfs-store`](../db/overview.md) directly.

## Crate dependency order

```
mfs-core          (caches, write-behind, WAL)
    |
mfs-store            (value types, schema definitions)
    |
mfs-compat        (this crate: object stores, schema store, page store, VFS)
```

## Feature flags

`MfsMutableObjectStore` and its persistence layer are available without any
feature flag. The core `MfsObjectStore` is always available.

## Cross-links

- [Object Store](object-store.md) for the fixed-capacity Redis-like facade.
- [Mutable Object Store](mutable-object-store.md) for the growable variant
  with TTL, cold tier, and persistence.
- [Schema Store](schema-store.md) for schema-validated documents and SQL flush.
- [SQLite VFS](sqlite-vfs.md) for the page store and VFS adapter.
