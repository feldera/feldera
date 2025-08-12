# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the DBSP source code.

## Overview

The `src/` directory contains the core implementation of DBSP (Database Stream Processor), a computational engine for incremental computation on changing datasets. DBSP enables processing changes in time proportional to the size of changes rather than the entire dataset, making it ideal for continuous analysis of large, frequently-changing data.

## Architecture Overview

### **Computational Model**

DBSP is based on a formal theoretical foundation that provides:

1. **Semantics**: Formal language of streaming operators with precise stream transformation specifications
2. **Algorithm**: Incremental dataflow program generation that processes input events proportional to input size rather than database state size

### **Core Design Principles**

- **Incremental Processing**: Changes propagate through circuits in time proportional to change size, not dataset size
- **Stream Processing**: Continuous analysis of changing data through operator circuits
- **Differential Computation**: Maintains differences between consecutive states using mathematical foundations
- **Circuit Model**: Computation expressed as circuits of interconnected operators with formal semantics

## Directory Structure Overview

### **Direct Subdirectories of `dbsp/src/`**

#### **`algebra/`**
Mathematical foundations and algebraic structures underlying DBSP's incremental computation model. Contains abstract algebraic concepts including monoids, groups, rings, and lattices that provide the theoretical basis for change propagation and differential computation. The `zset/` subdirectory implements Z-sets (collections with multiplicities) which are fundamental to representing insertions and deletions in incremental computation. Includes specialized number types (`checked_int.rs`, `floats.rs`) and ordering abstractions (`order.rs`, `lattice.rs`) essential for maintaining mathematical correctness in streaming operations.

#### **`circuit/`**
Core circuit infrastructure providing the runtime execution engine for DBSP computations. Contains circuit construction (`circuit_builder.rs`), execution control (`runtime.rs`), and scheduling (`schedule/`) components. The `dbsp_handle.rs` provides the main API for multi-worker circuit execution, while `checkpointer.rs` handles state persistence for fault tolerance. Includes performance monitoring (`metrics.rs`), circuit introspection (`trace.rs`), and integration with async runtimes (`tokio.rs`). The scheduler supports dynamic work distribution with NUMA-aware thread management.

#### **`dynamic/`**
Dynamic typing system that enables runtime flexibility while maintaining performance. Implements trait object architecture to avoid excessive monomorphization during compilation. Contains core dynamic types (`data.rs`, `pair.rs`, `vec.rs`), serialization support (`rkyv.rs`), and factory patterns (`factory.rs`) for creating trait objects. The `erase.rs` module handles type erasure, while `downcast.rs` provides safe downcasting mechanisms. Essential for SQL compiler integration where concrete types are not known at compile time.

#### **`monitor/`**
Circuit monitoring and visualization tools for debugging and performance analysis. Provides circuit graph generation (`circuit_graph.rs`) for visual representation of operator connectivity and data flow. The `visual_graph.rs` module creates GraphViz-compatible output for circuit visualization. Essential for understanding complex circuit behavior, debugging performance bottlenecks, and validating circuit construction correctness during development.

#### **`operator/`**
Complete implementation of DBSP's operator library containing 50+ streaming operators. Organized into basic operators (`apply.rs`, `filter_map.rs`, `plus.rs`), aggregation operators (`aggregate.rs`, `group/`), join operators (`join.rs`, `asof_join.rs`, `semijoin.rs`), and specialized operators (`time_series/`, `recursive.rs`). The `dynamic/` subdirectory provides dynamically-typed versions of all operators for runtime flexibility. Communication operators (`communication/`) handle multi-worker data distribution. Input/output handling (`input.rs`, `output.rs`) provides type-safe interfaces for data ingestion and emission.

#### **`profile/`**
Performance profiling and monitoring infrastructure for circuit execution analysis. Contains CPU profiling support (`cpu.rs`) with integration to standard Rust profiling tools. Provides DBSP-specific performance counters, memory usage tracking, and execution time analysis. Essential for performance optimization, identifying bottlenecks, and ensuring efficient resource utilization in production deployments.

#### **`storage/`**
Multi-tier persistent storage system supporting various backend implementations. The `backend/` directory contains storage abstractions with memory (`memory_impl.rs`) and POSIX file system (`posixio_impl.rs`) implementations. Buffer caching (`buffer_cache/`) provides intelligent memory management with LRU eviction policies. File format handling (`file/`) implements zero-copy serialization with rkyv, while `dirlock/` provides file system synchronization. Supports both memory-optimized and disk-based storage for different deployment scenarios.

#### **`time/`**
Temporal abstractions and time-based computation support for streaming operations. Implements timestamp types and ordering relationships (`antichain.rs`, `product.rs`) essential for maintaining temporal consistency in incremental computation. Provides the foundation for time-based operators, windowing operations, and watermark handling. Critical for ensuring correct temporal semantics in streaming analytics and maintaining consistency across distributed computations.

#### **`trace/`**
Core data structures for storing and indexing collections with efficient access patterns. Contains batch and trace implementations (`ord/`) supporting both in-memory and file-based storage. The `cursor/` subdirectory provides iteration interfaces, while `layers/` implements hierarchical data organization. Spine-based temporal organization (`spine_async/`) enables efficient merge operations for time-ordered data. Supports various specialized trace types including key-value batches, weighted sets, and indexed collections optimized for different query patterns.

#### **`utils/`**
Utility functions and helper modules supporting core DBSP functionality. Contains sorting algorithms (`sort.rs`, `unstable_sort.rs`), data consolidation (`consolidation/`), and specialized data structures. Tuple generation (`tuple/`) provides compile-time tuple creation, while `vec_ext.rs` extends vector functionality. The `sample.rs` module implements sampling algorithms for statistical operations. Includes property-based testing utilities and performance optimization helpers.

## Module Architecture

### **Core Infrastructure Layers**

#### **1. Dynamic Dispatch System** (`dynamic/`)
**Purpose**: Provides dynamic typing to limit monomorphization and balance compilation speed with runtime performance.

**Key Components**:
```rust
// Core trait hierarchy for dynamic dispatch
trait Data: Clone + Eq + Ord + Hash + SizeOf + Send + Sync + Debug + 'static {}
trait DataTrait: DowncastTrait + ClonableTrait + ArchiveTrait + Data {}

// Factory pattern for creating trait objects
trait Factory<T> {
    fn default_box(&self) -> Box<T>;
    fn default_ref(&self) -> &mut T;
}
```

**Dynamic Type System**:
- `DynData`: Dynamically typed data with concrete type erasure
- `DynPair`: Dynamic tuples for key-value relationships
- `DynVec`: Dynamic vectors with efficient batch operations
- `DynWeightedPairs`: Collections of weighted data for incremental computation

**Safety Considerations**: Uses unsafe code for performance by eliding TypeId checks in release builds while maintaining type safety through careful API design.

#### **2. Algebraic Foundations** (`algebra/`)
**Purpose**: Mathematical abstractions underlying incremental computation.

**Core Algebraic Structures**:
```rust
// Trait hierarchy for mathematical structures
trait SemigroupValue: Clone + Eq + SizeOf + AddByRef + 'static {}
trait MonoidValue: SemigroupValue + HasZero {}
trait GroupValue: MonoidValue + Neg<Output = Self> + NegByRef {}
trait RingValue: GroupValue + Mul<Output = Self> + MulByRef<Output = Self> + HasOne {}
```

**Z-Set Implementation** (`algebra/zset/`):
- **ZSet**: Collections with multiplicities (positive/negative weights)
- **IndexedZSet**: Indexed collections for efficient lookups and joins
- **Mathematical Operations**: Addition, subtraction, multiplication following group theory
- **Change Propagation**: Differential computation using +/- weights for insertions/deletions

#### **3. Trace System** (`trace/`)
**Purpose**: Core data structures for storing and indexing collections with time-based organization.

**Batch and Trace Abstractions**:
```rust
trait BatchReader {
    type Key: DBData;
    type Val: DBData;
    type Time: Timestamp;
    type Diff: MonoidValue;

    fn cursor(&self) -> Self::Cursor;
}

trait Trace: BatchReader {
    fn append_batch(&mut self, batch: &Self::Batch);
    fn map_batches<F>(&self, f: F);
}
```

**Storage Implementations**:
- **In-Memory**: `VecBatch`, `OrdBatch` for fast access
- **File-Based**: `FileBatch` for persistent storage with memory efficiency
- **Fallback**: `FallbackBatch` combining memory and file storage
- **Spine**: Efficient merge trees for temporal data organization

#### **4. Circuit Infrastructure** (`circuit/`)
**Purpose**: Runtime execution engine with scheduling, state management, and multi-threading.

**Circuit Execution Model**:
```rust
trait Operator {
    fn eval(&mut self);
    fn commit(&mut self);
    fn get_metadata(&self) -> OperatorMeta;
}

// Circuit construction and execution
let (circuit, handles) = Runtime::init_circuit(workers, |circuit| {
    let input = circuit.add_input_zset::<Record, Weight>();
    let output = input.map(|r| transform(r));
    Ok((input_handle, output.output()))
})?;
```

**Key Circuit Components**:
- **Runtime**: Multi-worker execution with NUMA-aware scheduling
- **Scheduler**: Dynamic scheduling with work-stealing parallelization
- **Handles**: Type-safe input/output interfaces (`InputHandle`, `OutputHandle`)
- **Checkpointing**: State persistence for fault tolerance
- **Metrics**: Performance monitoring and circuit introspection

## Operator System

### **Operator Categories and Implementations**

#### **Core Transformation Operators** (`operator/`)

**Basic Operators**:
```rust
// Element-wise transformations
.map(|x| f(x))           // Map operator
.filter(|x| p(x))        // Filter operator
.filter_map(|x| f(x))    // Combined filter and map

// Arithmetic operations
.plus(&other)            // Set union with weight addition
.minus(&other)           // Set difference with weight subtraction
```

**Aggregation Operators**:
```rust
// Group-by aggregation with incremental maintenance
.aggregate_generic::<K, V, R>(
    |k, v| key_func(k, v),          // Key extraction
    |acc, k, v, w| agg_func(acc, k, v, w), // Aggregation function
)

// Window aggregation
.window_aggregate(window_spec, agg_func)
```

**Join Operators**:
```rust
// Hash join with incremental updates
.join::<K, V1, V2>(&other, |k, v1, v2| result(k, v1, v2))

// Index join for efficient lookups
.index_join(&indexed_stream, join_key)

// As-of join for temporal relationships
.asof_join(&other, time_key, join_condition)
```

#### **Advanced Operators**

**Recursive Computation**:
```rust
// Fixed-point iteration for recursive queries
circuit.recursive(|child| {
    let (feedback, output) = child.add_feedback(z_set_factory);
    let result = base_case.plus(&feedback.delay().recursive_step());
    feedback.connect(&result);
    Ok(output)
})
```

**Time Series Operators** (`operator/time_series/`):
- **Windowing**: Time-based and row-based windows with expiration
- **Watermarks**: Late data handling and progress tracking
- **Rolling Aggregates**: Efficient sliding window computations
- **Range Queries**: Time range-based data retrieval

**Communication Operators** (`operator/communication/`):
- **Exchange**: Data redistribution across workers
- **Gather**: Collecting distributed results
- **Shard**: Partitioning data for parallel processing

### **Dynamic vs Static APIs**

#### **Static API** (Top-level `operator/` modules)
```rust
// Type-safe, compile-time checked operations
let result: Stream<_, OrdZSet<(String, i32), isize>> = input
    .map(|(k, v)| (k.to_uppercase(), v * 2))
    .filter(|(_, v)| *v > 0);
```

#### **Dynamic API** (`operator/dynamic/`)
```rust
// Runtime-flexible operations with dynamic typing
let result = input_dyn
    .map_generic(&map_factory, &output_factory)
    .filter_generic(&filter_factory);
```

**Trade-offs**:
- **Static**: Better performance, compile-time type checking, less flexibility
- **Dynamic**: Faster compilation, runtime flexibility, SQL compiler integration

## Storage Architecture

### **Multi-Tier Storage System** (`storage/`)

#### **Storage Backends** (`storage/backend/`)
```rust
trait StorageBackend {
    fn create_file(&self, path: &StoragePath) -> Result<Self::File, StorageError>;
    fn open_file(&self, path: &StoragePath) -> Result<Self::File, StorageError>;
}

// Implementations:
// - MemoryBackend: In-memory for testing and small datasets
// - PosixBackend: File system storage for production
```

#### **Buffer Cache System** (`storage/buffer_cache/`)
- **LRU Caching**: Intelligent buffer management with configurable cache sizes
- **Async I/O**: Non-blocking file operations with efficient prefetching
- **Memory Mapping**: Direct memory access for large files when beneficial
- **Cache Statistics**: Performance monitoring and cache hit rate tracking

#### **File Format and Serialization** (`storage/file/`)
```rust
// Zero-copy serialization with rkyv
trait Serializable {
    fn serialize<S: Serializer>(&self, serializer: &mut S) -> Result<(), S::Error>;
}

// File-based batch implementations
FileBatch {
    reader: FileReader<K, V, T, R>,
    metadata: BatchMetadata,
    cache_stats: CacheStats,
}
```

## Performance Architecture

### **Multi-Threading and Parallelization**

#### **Worker-Based Execution Model**:
```rust
// Multi-worker runtime with work-stealing scheduler
let workers = 8;
let (circuit, handles) = Runtime::init_circuit(workers, circuit_constructor)?;

// Automatic work distribution across operators
// NUMA-aware memory allocation
// Lock-free data structures for synchronization
```

#### **Operator Parallelization**:
- **Embarrassingly Parallel**: Map, filter operations across workers
- **Hash-Based Partitioning**: Join operations with consistent hashing
- **Pipeline Parallelism**: Different operators executing concurrently
- **Batch Processing**: Amortized costs through efficient batching

### **Memory Management Optimization**

#### **Custom Allocation Strategies**:
```rust
// mimalloc integration for high-performance allocation
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
```

#### **Zero-Copy Techniques**:
- **rkyv Serialization**: Zero-copy deserialization for file I/O
- **Reference Sharing**: `Rc`/`Arc` for immutable data sharing
- **In-Place Updates**: Mutation when safe for performance
- **Batch Reuse**: Buffer recycling to minimize allocations

### **Optimization Techniques**

#### **Data Structure Selection**:
- **Indexed Collections**: B-trees for sorted data with range queries
- **Hash Tables**: Hash maps for point lookups and equi-joins
- **Vectors**: Sequential access patterns with cache efficiency
- **Sparse Representations**: Efficient storage for sparse datasets

#### **Algorithm Optimization**:
- **Incremental Algorithms**: Differential computation for all operators
- **Index Selection**: Automatic index creation for query optimization
- **Lazy Evaluation**: Defer computation until results are needed
- **Batch Consolidation**: Merge operations for efficiency

## Testing and Validation

### **Testing Architecture**

#### **Property-Based Testing**:
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_incremental_correctness(
        initial_data in vec((any::<String>(), any::<i32>()), 0..100),
        updates in vec((any::<String>(), any::<i32>()), 0..50)
    ) {
        // Verify incremental result matches batch computation
        let incremental_result = incremental_computation(&initial_data, &updates);
        let batch_result = batch_computation(&[&initial_data[..], &updates[..]].concat());
        prop_assert_eq!(incremental_result, batch_result);
    }
}
```

#### **Integration Testing**:
- **End-to-End Pipelines**: Complete circuit execution validation
- **Correctness Verification**: Incremental vs batch result comparison
- **Performance Regression**: Benchmark tracking across versions
- **Fault Tolerance**: Checkpoint/recovery scenario testing

### **Debugging and Profiling**

#### **Circuit Introspection** (`monitor/`):
```rust
// Visual circuit graph generation
circuit.generate_graphviz(&mut output);

// Performance metrics collection
let metrics = circuit.gather_metrics();
println!("Operator throughput: {} records/sec", metrics.throughput);
```

#### **Profiling Integration** (`profile/`):
- **CPU Profiling**: Integration with standard Rust profiling tools
- **Memory Analysis**: Allocation tracking and memory usage patterns
- **Custom Metrics**: DBSP-specific performance counters
- **Tracing Support**: Distributed tracing for complex circuits

## Development Patterns and Best Practices

### **Operator Development Guidelines**

#### **Implementing New Operators**:
1. **Define Traits**: Specify operator behavior through trait definitions
2. **Static Implementation**: Create type-safe static version first
3. **Dynamic Wrapper**: Add dynamic dispatch for SQL compiler integration
4. **Testing**: Comprehensive unit tests with property-based validation
5. **Documentation**: Examples and performance characteristics

#### **Performance Optimization**:
- **Profile First**: Use built-in profiling before optimizing
- **Batch Processing**: Prefer batch operations over single-record processing
- **Memory Layout**: Consider cache-friendly data arrangements
- **Incremental Logic**: Ensure algorithms process only changes when possible

### **Integration Points**

#### **SQL Compiler Integration**:
- Dynamic operators provide runtime flexibility for compiled SQL
- Factory pattern enables type-erased circuit construction
- Metadata system supports query optimization and introspection

#### **Storage System Integration**:
- Pluggable storage backends for different deployment scenarios
- Checkpointing support for fault-tolerant long-running computations
- File-based batches for memory-efficient large dataset processing

### **Error Handling Patterns**

#### **Structured Error Types**:
```rust
#[derive(Debug, Error)]
pub enum Error {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Runtime error: {0}")]
    Runtime(#[from] RuntimeError),

    #[error("Scheduler error: {0}")]
    Scheduler(#[from] SchedulerError),
}
```

#### **Recovery Strategies**:
- **Graceful Degradation**: Continue processing when possible
- **State Restoration**: Checkpoint-based recovery for critical errors
- **Error Propagation**: Structured error context through computation stack

This DBSP source code represents a sophisticated computational engine that successfully implements incremental computation theory in a high-performance, production-ready system. The modular architecture, extensive use of Rust's type system, and careful performance optimization create a powerful foundation for streaming analytics and incremental view maintenance.