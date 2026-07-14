# Contributing

Thanks for your interest in MfS. This page covers the commands you need
for day-to-day development.

## Prerequisites

- Rust toolchain (stable). The project targets Linux x86_64 and aarch64.
- `cargo` (ships with rustup).
- `make` (optional, wraps common workflows).

## Building

```bash
cargo build --workspace --all-features
```
Or via Make:

```bash
make build
```

This compiles all workspace crates (`mfs-core`, `mfs-neural`, `mfs-db`,
`mfs-compat`) with every feature flag enabled.

## Testing

```bash
cargo test --workspace --all-features
```
Or via Make:

```bash
make test
```

For release-mode tests (closer to benchmark conditions):

```bash
cargo test --workspace --all-features --release
```

## Formatting

```bash
cargo fmt --all
```

To check formatting without modifying files (used in CI):

```bash
cargo fmt --all -- --check
```

## Linting

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
```
Or via Make:

```bash
make clippy
```

All clippy warnings are treated as errors. Fix them before submitting.

## CI Check

The full CI pipeline runs format check, clippy, and tests in sequence:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Or using the Makefile:

```bash
make ci
```

## Benchmarking

Benchmark harnesses are available in the development repository (not included
in this public release). See the [README](../README.md) for methodology and
key numbers.

## Getting Help

- **Questions**: Open a discussion or issue on the repository.
- **Bugs**: File an issue with a minimal reproduction. Include your Rust
  version (`rustc --version`), platform (`uname -a`), and the commit
  hash you're running.
- **Performance regressions**: Include benchmark output with `MFS_RUNS=10`
  so we can see the distribution and CV%.

## Filing Issues

When filing an issue, please include:

1. **What you did**: the code or command that triggered the problem.
2. **What you expected**: the behaviour you expected.
3. **What actually happened**: the actual behaviour, including any error
   messages or panics.
4. **Environment**: Rust version, OS, CPU, and MfS commit hash.

For performance issues, also include:

- Benchmark command and environment variables.
- At least 3 runs so we can see variance.
- Whether you're running on Skylake, Zen 3, or another microarchitecture.

## Code Style

- Follow standard Rust conventions (`rustfmt` + `clippy`).
- Write doc comments on public items.
- Keep the hot path allocation-free where possible.
- Pre-size `ConcurrentMap` for your working set; it's fixed-capacity.
- Don't introduce third-party concurrent-map dependencies without
  discussion. The in-house `ConcurrentMap` and `InlineU64Map` are
  deliberate design choices.

## Project Layout

```
crates/
  mfs-core/        Foundation: concurrent maps, caches, WAL, S3-FIFO
  mfs-neural/      Dense numeric layers (DenseKvMap, DenseWriteBehindMap)
  mfs-db/          NoSQL engine: raw KV, schema mode, checkpoint recovery
  mfs-compat/      Compatibility: object store, schema store, SQLite VFS
examples/
  core/            Core crate examples
  db/              NoSQL engine examples
  compat/          Compatibility layer examples
  neural/          Dense numeric layer examples
docs/              Documentation (this directory)
```

## License

MfS is dual-licensed under MIT and Apache-2.0. By contributing, you
agree to license your contributions under the same terms.
