# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the transport module in the adapters crate.

## Overview

The transport module implements Feldera's I/O abstraction layer, providing unified access to diverse external systems for DBSP circuits. It enables high-performance, fault-tolerant data ingestion and emission across transports including Kafka, HTTP, files, databases, and cloud storage systems, while maintaining strong consistency guarantees for incremental computation.

## Architecture Overview

### **Dual Transport Abstraction Model**

The transport system uses two complementary abstraction patterns:

#### **Regular Transports** (Transport + Format Separation)
```rust
TransportInputEndpoint → InputReader → Parser → InputConsumer
```
- **Transport**: Handles data movement and connection management
- **Format**: Separate parser handles data serialization/deserialization
- **Examples**: HTTP, Kafka, File, URL, S3, PubSub, Redis

#### **Integrated Transports** (Transport + Format Combined)
```rust
IntegratedInputEndpoint → InputReader → InputConsumer
```
- **Unified**: Transport and format tightly coupled for efficiency
- **Examples**: PostgreSQL, Delta Lake, Iceberg (database-specific optimizations)

### **Core Abstractions and Traits**

#### Primary Transport Traits
```rust
pub trait InputEndpoint {
    fn endpoint_name(&self) -> &str;
    fn supports_fault_tolerance(&self) -> bool;
}

pub trait TransportInputEndpoint: InputEndpoint {
    fn open(&self, consumer: Box<dyn InputConsumer>,
           parser: Box<dyn Parser>, ...) -> Box<dyn InputReader>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(self: Box<Self>, input_handle: &InputCollectionHandle,
           ...) -> Box<dyn InputReader>;
}
```

#### Input Reader Interface
```rust
pub trait InputReader: Send {
    fn seek(&mut self, position: JsonValue) -> AnyResult<()>;
    fn request(&mut self, command: InputReaderCommand);
    fn is_closed(&self) -> bool;
}
```

**Command-Driven State Management**:
```rust
pub enum InputReaderCommand {
    Queue,      // Start queuing data
    Extend,     // Resume normal processing
    Pause,      // Pause data ingestion
    Replay { seek, queue, hash }, // Replay from checkpoint
    Disconnect, // Graceful shutdown
}
```

## Transport Implementations

### **HTTP Transport** (`http/`)

**Key Features**:
- **Real-time Streaming**: Chunked transfer encoding with configurable timeouts
- **Exactly-Once Support**: Fault tolerance with seek/replay capabilities
- **State Management**: Atomic operations for thread-safe state transitions
- **Flexible Endpoints**: GET, POST, and streaming endpoint support

**Architecture Pattern**:
```rust
// Background worker thread pattern
thread::spawn(move || {
    while let Some(command) = command_receiver.recv() {
        match command {
            Extend => start_streaming_data(),
            Pause => pause_and_queue(),
            Disconnect => graceful_shutdown(),
        }
    }
});
```

**Configuration**:
```rust
HttpInputConfig {
    path: String,           // HTTP endpoint URL
    method: HttpMethod,     // GET/POST request method
    timeout_ms: Option<u64>, // Request timeout
    headers: BTreeMap<String, String>, // Custom headers
}
```

### **Kafka Transport** (`kafka/`)

**Dual Implementation Strategy**:

#### **Fault-Tolerant Kafka** (`ft/`):
- **Exactly-Once Semantics**: Transaction-based processing with commit coordination
- **Complex State Management**: Offset tracking with replay capabilities
- **Memory Monitoring**: Real-time memory usage reporting for large datasets
- **Error Handling**: Sophisticated retry logic with exponential backoff

#### **Non-Fault-Tolerant Kafka** (`nonft/`):
- **High Performance**: Simplified processing for maximum throughput
- **At-Least-Once**: Basic reliability without exact replay guarantees
- **Reduced Overhead**: Minimal state tracking for performance-critical scenarios

**Key Implementation Details**:
```rust
// Kafka-specific error refinement
fn refine_kafka_error<C>(client: &KafkaClient<C>, e: KafkaError) -> (bool, AnyError) {
    // Converts librdkafka errors into actionable Anyhow errors
    // Determines fatality for proper error handling strategy
}

// Authentication integration
KafkaAuthConfig::Aws { region } => {
    // AWS MSK IAM authentication with credential chain
}
```

### **File Transport** (`file.rs`)

**File Processing Features**:
- **Follow Mode**: Tail-like behavior for continuously growing files
- **Seek Support**: Resume from specific file positions using byte offsets
- **Buffer Management**: Configurable read buffers for memory efficiency
- **Testing Integration**: Barrier synchronization for deterministic test execution

**Seek Implementation**:
```rust
impl InputReader for FileInputReader {
    fn seek(&mut self, position: JsonValue) -> AnyResult<()> {
        if let JsonValue::Number(n) = position {
            self.file.seek(SeekFrom::Start(n.as_u64().unwrap()))?;
        }
    }
}
```

### **URL Transport** (`url.rs`)

**Advanced HTTP Features**:
- **Range Requests**: HTTP Range header support for resumable downloads
- **Connection Management**: Automatic reconnection with configurable timeouts
- **Pause/Resume**: Sophisticated state management with timeout handling
- **Error Recovery**: Retry logic with exponential backoff and circuit breaking

**Resume Capability**:
```rust
// HTTP Range request for resumption
let range_header = format!("bytes={}-", current_position);
request.headers.insert("Range", range_header);
```

### **Specialized Transports**

#### **S3 Transport** (`s3.rs`):
- **AWS SDK Integration**: Native AWS authentication and region support
- **Object Streaming**: Efficient streaming of large S3 objects
- **Prefix Filtering**: Support for processing multiple objects with key patterns

#### **Redis Transport** (`redis.rs`):
- **Multiple Data Structures**: Support for streams, lists, and pub/sub patterns
- **Connection Pooling**: Efficient connection reuse and management
- **Auth Support**: Redis AUTH and ACL integration

#### **Ad Hoc Transport** (`adhoc.rs`):
- **Direct Data Injection**: Programmatic data insertion for testing and development
- **Schema Flexibility**: Support for arbitrary data structures
- **Development Support**: Simplified data injection for rapid prototyping

## Error Handling Architecture

### **Hierarchical Error Classification**

**Error Severity Levels**:
```rust
// Fatal errors require complete restart
if is_fatal_error(&error) {
    consumer.error(true, error); // Signal fatal condition
    break; // Terminate processing loop
}

// Non-fatal errors allow recovery
consumer.error(false, error); // Continue processing
```

**Error Context Preservation**:
- **Rich Error Messages**: Actionable information with suggested fixes
- **Source Location**: Precise error location tracking for debugging
- **Error Chains**: Maintain full error context through transformation layers

### **Async Error Handling**

**Out-of-Band Error Reporting**:
```rust
// Background thread error callback
let error_callback = Arc::clone(&consumer);
tokio::spawn(async move {
    if let Err(e) = async_operation().await {
        error_callback.error(false, e.into());
    }
});
```

## Concurrency and Threading Patterns

### **Thread-per-Transport Architecture**

Most transports follow a consistent threading pattern:

```rust
let (command_sender, command_receiver) = unbounded();
let worker_thread = thread::Builder::new()
    .name(format!("{}-worker", transport_name))
    .spawn(move || {
        transport_worker_loop(command_receiver, consumer)
    })?;

// InputReader implementation delegates to worker thread
struct TransportInputReader {
    command_sender: UnboundedSender<InputReaderCommand>,
    worker_handle: JoinHandle<()>,
}
```

### **Channel-Based Communication**

**Command Dispatch Pattern**:
- **Unbounded Channels**: `UnboundedSender<InputReaderCommand>` for command dispatch
- **Backpressure Handling**: Optional bounded channels for flow control
- **Error Propagation**: Separate error channels for out-of-band error reporting

### **Mixed Async/Sync Design**

**Tokio Integration Strategy**:
```rust
// Background thread with Tokio runtime for async operations
let rt = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;

rt.block_on(async {
    // Async HTTP client operations
    let response = http_client.get(url).send().await?;
    // Process streaming response
});
```

## Fault Tolerance and State Management

### **Three-Tier Fault Tolerance Model**

#### **Level 1: None**
- No persistence or recovery capabilities
- Suitable for non-critical or easily reproducible data sources
- Example: Development/testing scenarios

#### **Level 2: At-Least-Once**
- Can resume from checkpoints but may have duplicates
- Checkpoint-based recovery with seek capability
- Example: File reading with position tracking

#### **Level 3: Exactly-Once**
- Can replay exact data with hash verification
- Strong consistency guarantees for critical data processing
- Example: Kafka transactions with offset management

### **Resume/Replay Implementation**

```rust
pub enum Resume {
    Barrier,                              // Cannot resume - start from beginning
    Seek { seek: JsonValue },            // Resume from checkpoint position
    Replay { seek: JsonValue, queue: Vec<ParsedStream>, hash: u64 }, // Exact replay
}

impl InputReader for FaultTolerantReader {
    fn request(&mut self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Replay { seek, queue, hash } => {
                self.seek(seek)?;
                self.replay_queue(queue, hash)?; // Exact data replay
                self.resume_normal_processing();
            }
        }
    }
}
```

### **Journaling and Checkpointing**

**Metadata Persistence**:
- Position tracking for seekable transports
- Queue snapshots for exact replay scenarios
- Hash verification for data integrity validation

## Configuration and Factory Patterns

### **Unified Configuration System**

All transport configurations are centralized in `feldera-types/src/transport/`:

```rust
#[derive(Deserialize, Serialize, Clone)]
pub enum TransportConfig {
    Kafka(KafkaInputConfig),
    Http(HttpInputConfig),
    File(FileInputConfig),
    // ... other transport configs
}
```

### **Factory Pattern Implementation**

```rust
pub fn input_transport_config_to_endpoint(
    config: &TransportConfig,
    endpoint_name: &str,
    secrets_dir: &Path,
) -> AnyResult<Option<Box<dyn TransportInputEndpoint>>> {

    match config {
        TransportConfig::Kafka(kafka_config) => {
            let endpoint = KafkaInputEndpoint::new(kafka_config, secrets_dir)?;
            Ok(Some(Box::new(endpoint)))
        }
        TransportConfig::Http(http_config) => {
            let endpoint = HttpInputEndpoint::new(http_config)?;
            Ok(Some(Box::new(endpoint)))
        }
        // ... other transport factories
    }
}
```

### **Secret Management Integration**

**Secure Credential Handling**:
```rust
// Resolve secrets from files or environment
let resolved_config = config.resolve_secrets(secrets_dir)?;
let credentials = extract_credentials(&resolved_config)?;
```

## Testing Infrastructure and Patterns

### **Barrier-Based Test Synchronization**

**Deterministic Test Execution**:
```rust
#[cfg(test)]
static BARRIERS: Mutex<BTreeMap<String, usize>> = Mutex::new(BTreeMap::new());

pub fn set_barrier(name: &str, value: usize) {
    BARRIERS.lock().unwrap().insert(name.into(), value);
}

pub fn barrier_wait(name: &str) {
    // Synchronization point for deterministic test execution
    while BARRIERS.lock().unwrap().get(name).copied().unwrap_or(0) > 0 {
        thread::sleep(Duration::from_millis(10));
    }
}
```

### **Mock Transport Implementations**

**Test Transport Pattern**:
```rust
struct MockInputReader {
    data: Vec<ParsedStream>,
    position: usize,
    consumer: Box<dyn InputConsumer>,
}

impl InputReader for MockInputReader {
    fn request(&mut self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Extend => self.send_next_batch(),
            InputReaderCommand::Pause => self.pause_processing(),
        }
    }
}
```

### **Integration Testing**

**Real External Service Testing**:
- Docker-based test environments for Kafka, PostgreSQL, Redis
- Testcontainers integration for isolated testing
- Property-based testing for fault tolerance scenarios

## Performance Optimization Patterns

### **Batching and Buffering**

**Efficient Data Processing**:
```rust
// Configurable batch sizes for optimal throughput
const DEFAULT_BATCH_SIZE: usize = 1000;

// Buffer management for memory efficiency
struct BufferedReader {
    buffer: Vec<u8>,
    batch_size: usize,
    max_buffer_size: usize,
}
```

### **Memory Management**

**Resource Monitoring**:
```rust
// Memory usage tracking for large datasets
pub fn report_memory_usage(&self) -> MemoryUsage {
    MemoryUsage {
        buffered_bytes: self.buffer.len(),
        queued_records: self.queue.len(),
        peak_memory: self.peak_memory.load(Ordering::Relaxed),
    }
}
```

### **Connection Pooling and Reuse**

**Efficient Resource Utilization**:
- HTTP connection pooling with keep-alive
- Kafka connection reuse across multiple topics
- Database connection pooling for integrated transports

## Development Best Practices

### **Adding New Transport Implementation**

1. **Define Configuration**: Add transport config to `TransportConfig` enum
2. **Implement Traits**: Create `TransportInputEndpoint` and `InputReader` implementations
3. **Error Handling**: Implement proper error classification and recovery
4. **Threading**: Follow thread-per-transport pattern with command channels
5. **Testing**: Add comprehensive unit and integration tests
6. **Documentation**: Update transport documentation and examples

### **Debugging Transport Issues**

**Diagnostic Tools**:
- **Logging**: Comprehensive tracing at debug/trace levels
- **Metrics**: Built-in performance and error rate monitoring
- **State Inspection**: Runtime state examination capabilities
- **Error Analysis**: Rich error context with actionable information

### **Performance Tuning**

**Optimization Areas**:
- **Buffer Sizes**: Tune based on data characteristics and memory constraints
- **Thread Configuration**: Optimize worker thread count for I/O patterns
- **Connection Pooling**: Configure pool sizes based on workload patterns
- **Batching**: Optimize batch sizes for throughput vs. latency trade-offs

This transport layer provides Feldera with a robust, high-performance, and extensible I/O foundation that enables reliable data processing at scale while maintaining the strong consistency guarantees required for incremental computation.