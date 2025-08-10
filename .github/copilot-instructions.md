# Copilot Instructions for Feldera

## Repository Overview

**Feldera** is a fast query engine for **incremental computation** that can evaluate arbitrary SQL programs incrementally. It is more powerful, expressive, and performant than existing alternatives like batch engines, warehouses, stream processors, or streaming databases.

### Repository Statistics
- **Primary Languages**: Rust (core engine), Java (SQL compiler), TypeScript/Svelte (web console), Python (SDK)
- **Size**: Large multi-language repository with 14 Rust crates, Java compiler, web console, Python SDK
- **License**: MIT OR Apache-2.0
- **MSRV**: Rust 1.87.0

### Key Components
- **DBSP Engine**: Core incremental computation engine (Rust)
- **SQL Compiler**: Converts SQL to DBSP (Java, based on Apache Calcite)
- **Pipeline Manager**: REST API server (Rust)
- **Web Console**: Browser-based UI (TypeScript/Svelte)
- **Python SDK**: Python bindings and client library
- **Adapters**: Connectors for Kafka, HTTP, S3, databases, etc.

## Build & Development Setup

### Required Dependencies
Always install ALL dependencies before building:

```bash
# System dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y cmake libssl-dev libsasl2-dev

# Rust toolchain (required version 1.87.0)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default 1.87.0

# Java (JDK 19 or newer required)
# Install OpenJDK 21 or newer

# Maven (for Java compilation)
# Install Maven 3.6+

# Bun (for web console)
# curl -fsSL https://bun.sh/install | bash

# Python 3 (for Python SDK)
# Install Python 3.8+
```

### Build Process

1. **Build SQL Compiler**:
```bash
cd sql-to-dbsp-compiler
./build.sh
```

Note: The SQL compiler should be built before running the pipeline-manager.

2. **Build Rust Components**:
```bash
# From repository root
cargo build --all-targets
# Build only the CLI tool
cargo build --package fda
```

3. **Build Web Console** (if needed):
```bash
cd web-console
bun install
bun run build
```

4. **Build Python SDK** (if needed):
```bash
cd python
uv pip install .
```

### Running Feldera

To start the system:
```bash
# From repository root, after building SQL compiler
cargo run --bin=pipeline-manager
# Visit http://localhost:8080 for web console
```

## Testing

### Test Execution Order
Run tests in this specific order to avoid failures:

1. **Rust unit tests**:
```bash
# Run doc tests
export WEBCONSOLE_BUILD_DIR="$(mktemp -d)"
touch $WEBCONSOLE_BUILD_DIR/index.html
cargo test --doc --workspace -- --test-threads 18
# Run all unit tests
cargo test
# Run manager tests
cargo test -p pipeline-manager
# Run a specific test
cargo test -p pipeline-manager support_data_collector::tests
```

2. **Java tests**:
```bash
cd sql-to-dbsp-compiler
mvn test
```

Note: This takes a very long time, best to run indidvidual tests.

3. **Integration tests** (requires Docker):
```bash
cargo run --bin=pipeline-manager
# Run some python integration test, e.g.:
cd python
uv run python -m pytest tests/test_shared_pipeline.py::TestPipeline::test_support_bundle_with_selectors -v
```

### Common Test Issues

- **Memory issues**: Limit test threads: `-- --test-threads 10`
- **SQL compiler not found**: Build `sql-to-dbsp-compiler` first
- **Web console**: Run `bun install` before testing

## Linting & Code Quality

### Pre-commit checks

```bash
# Install pre-commit hooks (runs on every commit)
uv pip install pre-commit
```

Run pre-commit hook manually after changes:

```
pre-commit run --show-diff-on-failure --color=always --all-files
```

### Manual Linting
```bash
# Rust formatting and linting
cargo fmt --all
cargo clippy --no-deps -- -D warnings

# Python formatting
cd python
uv tool install ruff
uv run ruff format
uv run ruff check --fix

# Web console formatting
cd web-console
bun run format
bun run lint
```

### Updating OpenAPI

When modifying REST API:

```bash
cargo run --bin=pipeline-manager -- --dump-openapi
```

## Project Layout & Architecture

### Core Rust Crates (`/crates/`)
- **`dbsp/`**: Core incremental computation engine
- **`pipeline-manager/`**: REST API server, main binary
- **`feldera-types/`**: Common type definitions
- **`adapters/`**: Input/output connectors (Kafka, HTTP, S3, etc.)
- **`sqllib/`**: SQL standard library functions
- **`storage/`**: Persistent storage layer
- **`fda/`**: CLI tool for managing pipelines

### Key Directories
- **`sql-to-dbsp-compiler/`**: Java-based SQL to DBSP compiler (Apache Calcite)
- **`web-console/`**: Svelte/TypeScript web UI
- **`python/`**: Python SDK and bindings
- **`deploy/`**: Docker build
- **`.github/workflows/`**: CI/CD pipelines

### Configuration Files
- **`Cargo.toml`**: Rust workspace configuration
- **`openapi.json`**: REST API specification (auto-generated)

## CI/CD Pipeline

### Workflow Dependencies

The CI runs these jobs in dependency order:

1. `build-rust` + `build-java` (parallel)
2. `test-unit` (needs both builds)
3. `test-adapters` (needs rust build)
4. `build-docker` (needs both builds)
5. `test-integration` (needs docker)
6. `test-java` (needs java build)

### Environment Variables

- **`RUST_BACKTRACE=1`**: Enable backtraces for debugging
- **`FELDERA_HOST`**: Host for pipeline-manager (e.g., `http://localhost:8080`)

## Common Issues & Solutions

### Build Failures
- **"cargo-machete not found"**: Install with `cargo install cargo-machete`
- **Rust compilation OOM**: Reduce parallelism: `cargo build -j 4`
- **Missing SQL compiler**: Run `./sql-to-dbsp-compiler/build.sh` first

## Development Workflow

### Key Commands Summary
```bash
# Complete build from scratch
cd sql-to-dbsp-compiler && ./build.sh
cd .. && cargo build --release --locked

# Run development server (builds web-console)
cargo run --bin=pipeline-manager

# Test suite
cargo test --doc --workspace -- --test-threads 10
cargo test

# Code quality
cargo fmt --all && cargo clippy --no-deps -- -D warnings
```
## Performance Notes

- **SQL compiler build**: 5 minutes (caches Calcite build)
- **Rust build**: 5-10 minutes (with sccache)
- **Full CI pipeline**: ~45-60 minutes
- **Container build**: ~20 minutes

**Trust these instructions** - they are comprehensive and tested. Only explore further if you encounter issues not covered above or if the instructions appear outdated.
