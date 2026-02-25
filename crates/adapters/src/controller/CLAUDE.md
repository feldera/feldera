# Controller Architecture

The controller module implements the centralized orchestration layer for Feldera's I/O adapter system. It coordinates circuit execution, manages data flow, implements fault tolerance, and provides runtime statistics.

## Core Purpose

The controller serves as the **central control plane** that:
- Coordinates DBSP circuit execution with I/O endpoints
- Implements runtime flow control and backpressure management
- Manages pipeline state transitions and lifecycle
- Provides fault tolerance through journaling and checkpointing
- Collects and reports performance statistics
- Orchestrates streaming data ingestion and output processing

## Architecture Overview

### Multi-Threaded Design

The controller employs a **three-thread architecture** for optimal performance:

1. **Circuit Thread** (`CircuitThread`) - The main orchestrator that:
   - Owns the DBSP circuit handle and executes `circuit.step()`
   - Coordinates with input/output endpoints
   - Manages circuit state transitions (Running/Paused/Terminated)
   - Handles commands (checkpointing, profiling, suspension)
   - Implements fault-tolerant journaling and replay logic

2. **Backpressure Thread** (`BackpressureThread`) - Flow control manager that:
   - Monitors input buffer levels across all endpoints
   - Pauses/resumes input endpoints based on buffer thresholds
   - Prevents memory exhaustion during high-throughput scenarios
   - Implements user-requested connector state changes

3. **Statistics Thread** (`StatisticsThread`) - Performance monitoring that:
   - Collects metrics every second (memory, storage, processed records)
   - Maintains a 60-second rolling window of time series data
   - Notifies subscribers of new metrics via broadcast channels
   - Provides data for real-time monitoring and alerting

### Key Components

#### Controller (`Controller`)
- **Public API** for pipeline management and state control
- **Thread-safe wrapper** around `ControllerInner` with proper lifecycle management
- **Command interface** for asynchronous operations (checkpointing, profiling)

#### ControllerInner (`ControllerInner`)
- **Shared state** accessible by all threads
- **Input/Output endpoint management** with dynamic reconfiguration
- **Circuit catalog integration** for schema-aware processing
- **Configuration management** and validation

#### ControllerStatus (`ControllerStatus`)
- **Lock-free performance counters** using atomics for minimal overhead
- **Statistical aggregation** across all endpoints and circuit operations
- **Time series broadcasting** for real-time monitoring clients
- **Flow control metrics** for backpressure management

## File Organization

### Core Files

- **`mod.rs`** - Main controller implementation with multi-threaded orchestration
- **`stats.rs`** - Performance monitoring, metrics collection, and statistics reporting
- **`error.rs`** - Error handling abstractions (re-exports from adapterlib)

### Specialized Modules

- **`checkpoint.rs`** - Fault tolerance through persistent checkpointing
  - Circuit state serialization and recovery
  - Input endpoint offset tracking
  - Configuration preservation across restarts

- **`journal.rs`** - Transaction log for exactly-once processing
  - Step-by-step metadata recording
  - Replay mechanism for fault recovery
  - Storage-agnostic persistence layer

- **`sync.rs`** - Enterprise checkpoint synchronization (S3/cloud storage)
  - Distributed checkpoint management
  - Cross-instance state coordination
  - Activation markers for standby pipelines

- **`validate.rs`** - Configuration validation and dependency analysis
  - Connector dependency cycle detection
  - Pipeline configuration consistency checks
  - Input/output endpoint validation

- **`test.rs`** - Comprehensive testing utilities and integration tests

## Key Design Patterns

### 1. Lock-Free Statistics
```rust
// Atomic counters prevent blocking the datapath
pub total_input_records: AtomicU64,
pub total_processed_records: AtomicU64,
// Broadcast channels for real-time streaming
pub time_series_notifier: broadcast::Sender<SampleStatistics>,
```

**Benefits:**
- Zero-overhead metrics collection
- Non-blocking circuit execution
- Real-time monitoring without performance impact

### 2. Thread Coordination
```rust
struct CircuitThread {
    controller: Arc<ControllerInner>,      // Shared state
    circuit: DBSPHandle,                   // Circuit ownership
    backpressure_thread: BackpressureThread, // Flow control
    _statistics_thread: StatisticsThread,    // Metrics
}
```

**Benefits:**
- Clear separation of concerns
- Independent thread lifecycles
- Coordinated shutdown without deadlocks

### 3. Command Pattern for Async Operations
```rust
enum Command {
    GraphProfile(GraphProfileCallbackFn),
    Checkpoint(CheckpointCallbackFn),
    Suspend(SuspendCallbackFn),
}
```

**Benefits:**
- Non-blocking public API
- Type-safe callback handling
- Graceful error propagation

### 4. State Machine Pipeline Management
```rust
pub enum PipelineState {
    Paused,    // Endpoints paused, draining existing data
    Running,   // Active ingestion and processing
    Terminated, // Permanent shutdown
}
```

**Benefits:**
- Predictable state transitions
- Clean resource management
- User-controlled pipeline lifecycle

## Fault Tolerance Architecture

### Journaling System
- **Step-by-step logging** of all input processing
- **Metadata preservation** for exact replay scenarios
- **Storage-agnostic** design supporting file systems and cloud storage

### Checkpointing Integration
- **Circuit state snapshots** at configurable intervals
- **Input offset tracking** for consistent recovery points
- **Configuration preservation** across pipeline restarts

### Error Handling Strategy
- **Transient vs Permanent errors** with different recovery strategies
- **Graceful degradation** rather than complete failure
- **Detailed error context** for debugging and monitoring

## Performance Characteristics

### Throughput Optimization
- **Lock-free statistics** prevent contention on hot paths
- **Batched circuit steps** amortize processing overhead
- **Parallel I/O processing** with independent endpoint threads

### Memory Management
- **Bounded buffers** prevent unlimited memory growth
- **Backpressure coordination** maintains system stability
- **Efficient broadcast channels** for multiple monitoring clients

### Latency Minimization
- **Direct circuit coupling** avoids unnecessary queuing
- **Atomic metric updates** enable real-time monitoring
- **Thread affinity** for CPU cache optimization

## Integration Points

### DBSP Circuit Integration
- Direct `DBSPHandle` ownership and step execution
- Schema-aware input/output catalog management
- Performance profile collection and analysis

### Transport Layer Integration
- Dynamic endpoint creation and lifecycle management
- Pluggable transport implementations (Kafka, HTTP, Files)
- Format-agnostic data processing pipeline

### Storage System Integration
- Backend-agnostic checkpoint and journal persistence
- Cloud storage synchronization for distributed deployments
- Efficient binary serialization for compact storage

## Development Patterns

### Adding New Metrics
1. Define atomic counters in `ControllerStatus`
2. Update collection logic in `StatisticsThread`
3. Include in time series broadcasting
4. Add appropriate accessor methods

### Extending State Management
1. Modify `PipelineState` enum if needed
2. Update state transition logic in circuit thread
3. Add appropriate synchronization primitives
4. Update error handling and recovery paths

### Implementing New Commands
1. Add command variant to `Command` enum
2. Implement callback-based processing
3. Add public API method with proper error handling
4. Ensure graceful shutdown behavior

## Best Practices

### Thread Safety
- Use `Arc<AtomicBool>` for cancellation flags
- Employ `crossbeam` primitives for lock-free data structures
- Minimize shared mutable state through message passing

### Resource Management
- Implement proper `Drop` traits for cleanup
- Use `JoinHandle` for thread lifecycle management
- Employ timeouts for external operations

### Error Propagation
- Distinguish between recoverable and permanent errors
- Provide detailed error context for debugging
- Use structured error types with clear categorization

### Performance Monitoring
- Collect metrics without impacting datapath performance
- Use efficient serialization for network transmission
- Implement configurable aggregation windows

This controller architecture enables Feldera to achieve high-performance stream processing while maintaining strong consistency guarantees and providing comprehensive observability into pipeline operations.