# memory-first-store — make targets

CARGO  ?= cargo
RUSTUP ?= rustup
BENCH  ?= $(CARGO) bench --all-features

.PHONY: help build test test-release fmt fmt-check lint clippy doc clean \
        bench bench-hot bench-wal bench-slot-writeback bench-s3fifo-tuning bench-queued-write bench-object-store bench-schema-store bench-local-db bench-nosql-engine bench-object-realistic bench-realistic bench-realistic-stable bench-probe bench-criterion bench-competitors bench-criterion-report \
        all check ci

help:
	@echo "memory-first-store — common commands"
	@echo ""
	@echo "  make build              cargo build --workspace"
	@echo "  make test               cargo test --workspace --all-features"
	@echo "  make test-release       cargo test --workspace --all-features --release"
	@echo "  make fmt                rustfmt all sources"
	@echo "  make fmt-check          verify formatting (CI-style)"
	@echo "  make clippy             clippy with -D warnings"
	@echo "  make doc                cargo doc --open"
	@echo ""
	@echo "Benchmarks (release profile, fat LTO):"
	@echo "  make bench                  run registered MfS benches with package-correct commands"
	@echo "  make bench-hot              microbenches: raw read/write hot paths"
	@echo "  make bench-wal              direct vs async WAL enqueue/durability bench"
	@echo "  make bench-slot-writeback   boxed vs slot-index write-behind bench"
	@echo "  make bench-s3fifo-tuning    MfS S3FIFO knob sweep"
	@echo "  make bench-queued-write     queued dense write-behind bench"
	@echo "  make bench-object-store     object-value writer comparison bench"
	@echo "  make bench-schema-store     schema store CRUD/index/include/WAL/SQL bench"
	@echo "  make bench-local-db         library-only SQLite/redb/fjall KV comparison"
	@echo "  make bench-nosql-engine     NoSqlEngine lane harness (raw/schema/WAL/checkpoint/replay)"
	@echo "  make bench-realistic        mixed workload (Redis-replacement profile)"
	@echo "  make bench-realistic-stable same as above with MFS_RUNS=10 distribution"
	@echo "  make bench-criterion        criterion-driven microbenches"
	@echo "                              (with confidence intervals + HTML report)"
	@echo "  make bench-competitors      criterion head-to-head vs Rust competitors"
	@echo "  make bench-criterion-report open the criterion HTML report"
	@echo "  make bench-probe            capacity-fragmentation diagnostic"
	@echo ""
	@echo "Realistic bench env vars (override on command line):"
	@echo "  MFS_DURATION_SECS=5      wall-clock seconds per run"
	@echo "  MFS_RUNS=1               number of independent runs"
	@echo "                           (>1 prints distribution summary)"
	@echo "  MFS_THREADS=N            worker threads (default min(num_cpus, 8))"
	@echo "  MFS_KEYS=100000          key universe size"
	@echo "  MFS_VALUE_BYTES=128      metadata blob size per record"
	@echo "  MFS_READ_PCT=95          read percentage of mix"
	@echo "  MFS_WRITE_PCT=4          write percentage (delete = 100-read-write)"
	@echo "  MFS_HOT_PCT=80           % of accesses targeting hot 20% of keys"
	@echo "  MFS_FLUSH_INTERVAL_MS=10 flusher tick interval"
	@echo "  MFS_SAMPLE_RATE=64       1-in-N latency timing sample rate"
	@echo ""
	@echo "  make ci                 fmt-check + clippy + test (no bench)"
	@echo "  make all                fmt + clippy + test + bench"
	@echo "  make clean              cargo clean"

build:
	$(CARGO) build --workspace --all-features

check:
	$(CARGO) check --workspace --all-targets --all-features

test:
	$(CARGO) test --workspace --all-features

test-release:
	$(CARGO) test --workspace --all-features --release

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

lint: clippy

doc:
	$(CARGO) doc --workspace --all-features --no-deps --open

bench: bench-hot bench-wal bench-slot-writeback bench-s3fifo-tuning bench-queued-write bench-object-store bench-schema-store bench-local-db bench-nosql-engine bench-object-realistic bench-realistic bench-probe

bench-hot:
	$(BENCH) -p mfs-core --bench mfs_hot_path

bench-wal:
	$(BENCH) -p mfs-core --bench mfs_wal_async

bench-slot-writeback:
	$(BENCH) -p mfs-core --bench mfs_slot_writeback

bench-s3fifo-tuning:
	$(BENCH) -p mfs-core --bench mfs_s3fifo_tuning

bench-queued-write:
	$(BENCH) -p mfs-neural --bench mfs_queued_write

bench-object-store:
	$(BENCH) -p mfs-compat --bench mfs_object_store

bench-schema-store:
	$(BENCH) -p mfs-compat --bench mfs_schema_store

bench-local-db:
	$(BENCH) -p mfs-compat --bench local_db_sqlite_kv

bench-nosql-engine:
	$(BENCH) -p mfs-db --bench mfs_nosql_engine

bench-nosql-query:
	$(BENCH) -p mfs-db --bench mfs_nosql_query

bench-object-realistic:
	$(BENCH) -p mfs-compat --bench mfs_object_realistic

bench-realistic:
	$(BENCH) -p mfs-core --bench mfs_realistic

bench-realistic-stable:
	MFS_RUNS=10 MFS_DURATION_SECS=5 $(BENCH) -p mfs-core --bench mfs_realistic

bench-probe:
	$(BENCH) -p mfs-core --bench mfs_probe

bench-criterion:
	$(BENCH) -p mfs-core --bench mfs_criterion

bench-competitors:
	$(BENCH) -p mfs-core --bench dashmap_contention
	$(BENCH) -p mfs-core --bench moka_zipfian
	$(BENCH) -p mfs-core --bench papaya_latency
	$(BENCH) -p mfs-core --bench papaya_single_thread
	$(BENCH) -p mfs-core --bench scc_hash_map
	$(BENCH) -p mfs-core --bench scc_hash_index
	$(BENCH) -p mfs-core --bench scc_hash_cache
	$(BENCH) -p mfs-core --bench scc_tree_index
	$(BENCH) -p mfs-core --bench quick_cache_benchmarks
	$(BENCH) -p mfs-core --bench tinyufo_bench_perf
	$(BENCH) -p mfs-core --bench tinyufo_bench_hit_ratio
	$(BENCH) -p mfs-core --bench foyer_bench_hit_ratio
	$(BENCH) -p mfs-core --bench foyer_bench_dynamic_dispatch
	$(BENCH) -p mfs-core --bench foyer_memory_vs_mfs

# Open the criterion HTML report (run bench-criterion first).
bench-criterion-report:
	xdg-open target/criterion/report/index.html 2>/dev/null \
		|| open target/criterion/report/index.html 2>/dev/null \
		|| echo "open target/criterion/report/index.html in your browser"

ci: fmt-check clippy test

all: fmt clippy test bench

clean:
	$(CARGO) clean
