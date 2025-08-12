# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the DBSP (Database Stream Processor) crate.

## Key Development Commands

### Building and Testing

```bash
# Build the DBSP crate
cargo build -p dbsp

# Run tests
cargo test -p dbsp

# Run specific test
cargo test -p dbsp test_name

# Run benchmarks
cargo bench -p dbsp

# Run examples
cargo run --example degrees
cargo run --example tutorial1
```

### Development Tools

```bash
# Check with clippy
cargo clippy -p dbsp

# Format code
cargo fmt -p dbsp

# Generate documentation
cargo doc -p dbsp --open

# Run with memory sanitizer (requires nightly)
cargo +nightly test -p dbsp --target x86_64-unknown-linux-gnu -Zbuild-std --features=sanitizer
```

## Architecture Overview

### Technology Stack

- **Language**: Rust with advanced type system features
- **Concurrency**: Multi-threaded execution with worker pools
- **Memory Management**: Custom allocators with mimalloc integration
- **Serialization**: rkyv for zero-copy serialization
- **Testing**: Extensive property-based testing with proptest

### Core Concepts

DBSP is a computational engine for **incremental computation** on changing datasets:

- **Incremental Processing**: Changes propagate in time proportional to change size, not dataset size
- **Stream Processing**: Continuous analysis of changing data
- **Differential Computation**: Maintains differences between consecutive states
- **Circuit Model**: Computation expressed as circuits of operators

### Project Structure

#### Core Directories

- `src/circuit/` - Circuit infrastructure and runtime
- `src/operator/` - Computational operators (map, filter, join, aggregate, etc.)
- `src/trace/` - Data structures for storing and indexing collections
- `src/algebra/` - Mathematical abstractions (lattices, groups, etc.)
- `src/dynamic/` - Dynamic typing system for runtime flexibility
- `src/storage/` - Persistent storage backend
- `src/time/` - Timestamp and ordering abstractions

#### Key Components

- **Runtime**: Circuit execution engine with scheduling
- **Operators**: Computational primitives (50+ operators)
- **Traces**: Indexed data structures for efficient querying
- **Handles**: Input/output interfaces for circuits

## Important Implementation Details

### Circuit Model

```rust
use dbsp::{Runtime, OutputHandle, IndexedZSet};

// Create circuit with 2 worker threads
let (mut circuit, handles) = Runtime::init_circuit(2, |circuit| {
    let (input, handle) = circuit.add_input_zset::<(String, i32), isize>();
    let output = input.map(|(k, v)| (k.clone(), v * 2));
    Ok((handle, output.output()))
})?;

// Execute one step
circuit.step()?;
```

### Operator Categories

#### Core Operators
- **Map/Filter**: Element-wise transformations
- **Aggregation**: Group-by and windowing operations
- **Join**: Various join algorithms (hash, indexed, asof)
- **Time Series**: Windowing and temporal operations

#### Advanced Operators
- **Recursive**: Fixed-point computations
- **Dynamic**: Runtime-configurable operators
- **Communication**: Multi-worker coordination
- **Storage**: Persistent state management

### Memory Management

- **Custom Allocators**: mimalloc for performance
- **Zero-Copy**: rkyv serialization avoids allocations
- **Batch Processing**: Amortized allocation costs
- **Garbage Collection**: Automatic cleanup of unused data

## Development Workflow

### For Operator Development

1. Define operator in `src/operator/`
2. Implement required traits (`Operator`, `StrictOperator`, etc.)
3. Add comprehensive tests in module
4. Add property tests with proptest
5. Update documentation and examples

### For Algorithm Implementation

1. Study existing patterns in `src/operator/`
2. Consider incremental vs non-incremental versions
3. Implement both typed and dynamic variants if needed
4. Add benchmarks for performance validation
5. Test with various data distributions

### Testing Strategy

#### Unit Tests
- Located alongside implementation code
- Test both correctness and incremental behavior
- Use `proptest` for property-based testing

#### Integration Tests
- Complex multi-operator scenarios
- End-to-end pipeline testing
- Performance regression detection

#### Benchmarks
- Located in `benches/` directory
- Real-world datasets (fraud detection, social networks)
- Performance tracking across versions

### Performance Considerations

#### Optimization Techniques
- **Batch Processing**: Process multiple elements together
- **Index Selection**: Choose appropriate data structures
- **Memory Layout**: Optimize for cache performance
- **Parallelization**: Leverage multi-core execution

#### Profiling Tools
- Built-in performance monitoring
- CPU profiling integration
- Memory usage tracking
- Visual circuit graph generation

### Configuration Files

- `Cargo.toml` - Package configuration with extensive feature flags
- `Makefile.toml` - Build automation scripts
- `benches/` - Performance benchmark suite
- `proptest-regressions/` - Property test regression data

### Key Features and Flags

- `default` - Standard feature set
- `with-serde` - Serialization support
- `persistence` - Persistent storage backend
- `mimalloc` - High-performance allocator
- `tokio` - Async runtime integration

### Dependencies

#### Core Dependencies
- `rkyv` - Zero-copy serialization
- `crossbeam` - Lock-free data structures
- `hashbrown` - Fast hash maps
- `num-traits` - Numeric abstractions

#### Development Dependencies
- `proptest` - Property-based testing
- `criterion` - Benchmarking framework
- `tempfile` - Temporary file management

### Advanced Features

#### Dynamic Typing
- Runtime type flexibility
- Operator composition at runtime
- Schema evolution support

#### Storage Backend
- Persistent state management
- Checkpoint/recovery mechanisms
- File-based and cloud storage options

#### Multi-threading
- Automatic work distribution
- Lock-free data structures
- NUMA-aware scheduling

### Best Practices

- **Incremental First**: Always consider incremental behavior
- **Type Safety**: Leverage Rust's type system for correctness
- **Performance**: Profile before optimizing
- **Testing**: Property tests for complex invariants
- **Documentation**: Comprehensive examples and tutorials