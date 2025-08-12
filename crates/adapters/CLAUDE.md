# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Adapters crate.

## Key Development Commands

### Building and Testing

```bash
# Build the adapters crate
cargo build -p adapters

# Run tests
cargo test -p adapters

# Run tests with required dependencies (Kafka, Redis, etc.)
cargo test -p adapters --features=with-kafka

# Build with all features
cargo build -p adapters --all-features
```

### Running Examples

```bash
# Run server demo with Kafka support
cargo run --example server --features="with-kafka server"

# Test specific transport integrations
cargo test -p adapters test_kafka
cargo test -p adapters test_http
```

### Development Environment

```bash
# Start required services for testing
docker run -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v24.2.4 redpanda start --smp 2

# Install system dependencies
sudo apt install cmake
```

## Architecture Overview

### Technology Stack

- **I/O Framework**: Async I/O with tokio runtime
- **Serialization**: Multiple format support (JSON, CSV, Avro, Parquet)
- **Transport Layer**: Kafka, HTTP, File, Redis, S3 integrations
- **Database Integration**: PostgreSQL and Delta Lake connectors
- **Testing**: Comprehensive integration testing with external services

### Core Concepts

The Adapters crate provides a **unified I/O framework** for DBSP circuits:

- **Input Adapters**: Ingest data from external sources into DBSP circuits
- **Output Adapters**: Stream circuit outputs to external consumers
- **Transport Layer**: Pluggable transport mechanisms
- **Format Layer**: Serialization/deserialization for different data formats
- **Controller**: Orchestrates circuit execution with I/O adapters

### Project Structure

#### Core Directories

- `src/adhoc/` - Implementation for the ad-hoc queries that uses Apache DataFusion batch query engine
- `src/format/` - Data format handlers (JSON, CSV, Avro, Parquet)
- `src/transport/` - Transport implementations (Kafka, HTTP, File, etc.)
- `src/integrated/` - Integrated connectors (Postgres, Delta Lake)
- `src/controller/` - Circuit controller and lifecycle management
- `src/static_compile/` - Compile-time adapter generation
- `src/test/` - Testing utilities and mock implementations

#### Key Components

- **Transport Adapters**: Protocol-specific I/O implementations
- **Format Processors**: Data serialization/deserialization
- **Controller**: Circuit execution coordinator
- **Catalog**: Runtime circuit introspection and control

## Important Implementation Details

### Transport Layer

#### Supported Transports

**Kafka Integration**
```rust
// Fault-tolerant Kafka producer
use adapters::transport::kafka::ft::KafkaOutputTransport;

// Non-fault-tolerant (higher performance)
use adapters::transport::kafka::nonft::KafkaOutputTransport;
```

**HTTP REST API**
```rust
use adapters::transport::http::{HttpInputTransport, HttpOutputTransport};
```

**File I/O**
```rust
use adapters::transport::file::{FileInputTransport, FileOutputTransport};
```

**Redis Streams**
```rust
use adapters::transport::redis::RedisOutputTransport;
```

### Format Layer

#### Supported Formats

- **CSV**: Delimited text with schema inference
- **JSON**: Nested data structures with type coercion
- **Avro**: Schema-based binary serialization
- **Parquet**: Columnar data format
- **Raw**: Unprocessed byte streams

#### Format Pipeline

1. **Deserialization**: Convert external format to internal representation
2. **Type Coercion**: Map external types to DBSP types
3. **Validation**: Ensure data integrity and constraints
4. **Batch Processing**: Optimize throughput with batching

### Controller Architecture

```rust
use adapters::controller::Controller;

// Create controller with circuit
let controller = Controller::new(circuit, adapters)?;

// Start data processing
controller.start().await?;

// Monitor and control
controller.step().await?;
controller.pause().await?;
```

#### Controller Features

- **Lifecycle Management**: Start, pause, stop, checkpoint
- **Error Handling**: Graceful degradation and recovery
- **Statistics**: Performance monitoring and metrics
- **Flow Control**: Backpressure and rate limiting

### Integrated Connectors

#### PostgreSQL CDC
```rust
use adapters::integrated::postgres::PostgresInputAdapter;

// Real-time change data capture
let adapter = PostgresInputAdapter::new(config).await?;
```

#### Delta Lake
```rust
use adapters::integrated::delta_table::DeltaTableOutputAdapter;

// Write to Delta Lake format
let adapter = DeltaTableOutputAdapter::new(config).await?;
```

## Development Workflow

### For New Transport Implementation

1. Create module in `src/transport/`
2. Implement `InputTransport` and/or `OutputTransport` traits
3. Add configuration types in `feldera-types`
4. Implement error handling and reconnection logic
5. Add comprehensive tests with real service
6. Update feature flags in `Cargo.toml`

### For New Format Support

1. Add format module in `src/format/`
2. Implement `Deserializer` and/or `Serializer` traits
3. Add schema inference and validation
4. Handle type coercion edge cases
5. Add performance benchmarks
6. Test with various data patterns

### Testing Strategy

#### Unit Tests
- Mock implementations for isolated testing
- Format conversion correctness
- Error handling scenarios

#### Integration Tests
- Real external service dependencies
- End-to-end data flow validation
- Performance and reliability testing

#### Test Dependencies

The test suite requires external services:

- **Kafka/Redpanda**: Message queue testing
- **PostgreSQL**: Database connector testing
- **Redis**: Stream testing
- **S3-compatible**: Object storage testing

### Performance Optimization

#### Throughput Optimization
- **Batch Processing**: Amortize per-record overhead
- **Connection Pooling**: Reuse network connections
- **Parallel Processing**: Multi-threaded I/O operations
- **Zero-Copy**: Minimize memory allocations

#### Memory Management
- **Streaming Processing**: Bounded memory usage
- **Backpressure**: Flow control mechanisms
- **Buffer Management**: Optimal buffer sizing
- **Garbage Collection**: Efficient cleanup

### Configuration Files

- `Cargo.toml` - Feature flags and dependencies
- `lsan.supp` - Leak sanitizer suppressions
- Test data files in various formats

### Key Features

- **Fault Tolerance**: Automatic reconnection and retry logic
- **Schema Evolution**: Handle schema changes gracefully
- **Monitoring**: Built-in metrics and observability
- **Multi-format**: Support for diverse data formats

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime
- `serde` - Serialization framework
- `anyhow` - Error handling
- `tracing` - Structured logging

#### Transport Dependencies
- `rdkafka` - Kafka client
- `reqwest` - HTTP client
- `redis` - Redis client
- `rusoto_s3` - AWS S3 client

#### Format Dependencies
- `csv` - CSV processing
- `serde_json` - JSON processing
- `apache-avro` - Avro format support
- `parquet` - Columnar data format

### Error Handling Patterns

- **Transient Errors**: Retry with exponential backoff
- **Permanent Errors**: Fail fast with detailed diagnostics
- **Schema Errors**: Graceful degradation with data preservation
- **Network Errors**: Connection management and recovery

### Security Considerations

- **Authentication**: Support for various auth mechanisms
- **Encryption**: TLS/SSL for network transports
- **Access Control**: Fine-grained permissions
- **Data Privacy**: PII handling and sanitization

### Monitoring and Observability

- **Metrics**: Throughput, latency, error rates
- **Tracing**: Distributed request tracing
- **Logging**: Structured log output
- **Health Checks**: Service health monitoring

### Best Practices

- **Async Design**: Non-blocking I/O operations
- **Resource Management**: Proper cleanup and lifecycle
- **Error Propagation**: Structured error handling
- **Testing**: Real service integration tests
- **Documentation**: Comprehensive examples and guides