# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Adapter Library crate.

## Key Development Commands

### Building and Testing

```bash
# Build the adapterlib crate
cargo build -p adapterlib

# Run tests
cargo test -p adapterlib

# Check documentation
cargo doc -p adapterlib --open
```

## Architecture Overview

### Technology Stack

- **Transport Abstraction**: Generic transport layer interface
- **Format Processing**: Data format abstraction and utilities
- **Catalog System**: Runtime circuit introspection
- **Error Handling**: Comprehensive error types for I/O operations

### Core Purpose

Adapter Library provides **foundational abstractions** for building I/O adapters:

- **Transport Traits**: Generic interfaces for input/output transports
- **Format Abstractions**: Data serialization/deserialization framework
- **Catalog Interface**: Runtime circuit inspection and control
- **Utility Functions**: Common functionality for adapter implementations

### Project Structure

#### Core Modules

- `src/transport.rs` - Transport layer abstractions and traits
- `src/format.rs` - Data format processing abstractions
- `src/catalog.rs` - Circuit catalog and introspection
- `src/errors/` - Error types for adapter operations
- `src/utils/` - Utility functions and helpers

## Important Implementation Details

### Transport Abstraction

#### Core Transport Traits
```rust
pub trait InputTransport {
    type Error;

    async fn start(&mut self) -> Result<(), Self::Error>;
    async fn read_batch(&mut self) -> Result<Vec<ParsedEvent>, Self::Error>;
    async fn stop(&mut self) -> Result<(), Self::Error>;
}

pub trait OutputTransport {
    type Error;

    async fn start(&mut self) -> Result<(), Self::Error>;
    async fn write_batch(&mut self, data: &[OutputEvent]) -> Result<(), Self::Error>;
    async fn stop(&mut self) -> Result<(), Self::Error>;
}
```

#### Transport Features
- **Async Interface**: Non-blocking I/O operations
- **Batch Processing**: Efficient bulk data transfer
- **Error Handling**: Transport-specific error types
- **Lifecycle Management**: Start/stop semantics

### Format Processing

#### Format Traits
```rust
pub trait Deserializer {
    type Error;
    type Output;

    fn deserialize(&mut self, data: &[u8]) -> Result<Self::Output, Self::Error>;
}

pub trait Serializer {
    type Error;
    type Input;

    fn serialize(&mut self, data: &Self::Input) -> Result<Vec<u8>, Self::Error>;
}
```

#### Format Features
- **Type Safety**: Strongly typed serialization
- **Error Recovery**: Graceful handling of malformed data
- **Schema Evolution**: Handle schema changes
- **Performance**: Zero-copy where possible

### Catalog System

#### Circuit Introspection
```rust
pub trait Catalog {
    fn input_handles(&self) -> Vec<InputHandle>;
    fn output_handles(&self) -> Vec<OutputHandle>;
    fn get_input_handle(&self, name: &str) -> Option<InputHandle>;
    fn get_output_handle(&self, name: &str) -> Option<OutputHandle>;
}
```

#### Runtime Control
- **Handle Discovery**: Find inputs and outputs by name
- **Schema Inspection**: Query table and view schemas
- **Type Information**: Runtime type metadata
- **Statistics**: Performance and throughput metrics

### Error Handling Framework

#### Error Categories
```rust
pub enum AdapterError {
    TransportError(TransportError),
    FormatError(FormatError),
    ConfigurationError(String),
    RuntimeError(String),
}
```

#### Error Features
- **Structured Errors**: Rich error information with context
- **Error Chaining**: Preserve error causation chains
- **Recovery Hints**: Actionable error messages
- **Logging Integration**: Structured logging support

### Utility Functions

#### Common Patterns
- **Retry Logic**: Configurable retry mechanisms
- **Rate Limiting**: Throughput control utilities
- **Buffer Management**: Efficient buffer handling
- **Connection Pooling**: Resource management helpers

#### Data Processing Utilities
- **Type Conversion**: Between different data representations
- **Validation**: Data integrity checking
- **Transformation**: Common data transformations
- **Batching**: Batch size optimization

## Development Workflow

### For Transport Implementation

1. Implement `InputTransport` or `OutputTransport` trait
2. Define transport-specific error types
3. Add configuration types for transport parameters
4. Implement lifecycle methods (start/stop)
5. Add comprehensive error handling
6. Test with various failure scenarios

### For Format Support

1. Implement `Deserializer` or `Serializer` trait
2. Handle schema validation and evolution
3. Add performance optimizations
4. Test with malformed data
5. Document format-specific behavior
6. Add benchmarks for performance validation

### Testing Strategy

#### Unit Tests
- **Interface Compliance**: Trait implementation correctness
- **Error Handling**: Comprehensive error scenario testing
- **Edge Cases**: Boundary conditions and limits
- **Resource Management**: Proper cleanup and lifecycle

#### Integration Tests
- **End-to-End**: Complete adapter pipeline testing
- **Performance**: Throughput and latency validation
- **Reliability**: Failure recovery and resilience
- **Compatibility**: Cross-format and cross-transport testing

### Design Patterns

#### Async Patterns
```rust
// Async transport implementation
impl InputTransport for MyTransport {
    async fn read_batch(&mut self) -> Result<Vec<ParsedEvent>, Self::Error> {
        // Non-blocking batch read
        let data = self.connection.read_batch().await?;
        Ok(self.format.deserialize_batch(&data)?)
    }
}
```

#### Error Handling Patterns
```rust
// Comprehensive error handling
match transport.read_batch().await {
    Ok(events) => process_events(events),
    Err(TransportError::ConnectionLost) => reconnect_and_retry(),
    Err(TransportError::Timeout) => continue_with_empty_batch(),
    Err(e) => return Err(e.into()),
}
```

### Performance Considerations

#### Batch Processing
- **Optimal Batch Size**: Balance latency vs throughput
- **Memory Usage**: Avoid excessive buffering
- **CPU Utilization**: Efficient serialization/deserialization
- **Network I/O**: Minimize network round trips

#### Resource Management
- **Connection Pooling**: Reuse expensive resources
- **Buffer Reuse**: Minimize allocations
- **Async Efficiency**: Non-blocking operations
- **Cleanup**: Proper resource disposal

### Configuration Files

- `Cargo.toml` - Core dependencies for transport and format abstractions
- Minimal dependencies to avoid bloat
- Feature flags for optional functionality

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime
- `serde` - Serialization framework
- `anyhow` - Error handling
- `tracing` - Structured logging

#### Optional Dependencies
- `datafusion` - SQL query engine integration
- Various format-specific libraries

### Best Practices

#### API Design
- **Trait-Based**: Use traits for extensibility
- **Async First**: Design for async from the start
- **Error Rich**: Provide detailed error information
- **Performance Aware**: Consider performance implications

#### Implementation Patterns
- **Resource Safety**: Proper RAII patterns
- **Error Propagation**: Use `?` operator consistently
- **Logging**: Comprehensive structured logging
- **Testing**: Test both success and failure paths

#### Documentation
- **Trait Documentation**: Clear trait contract specification
- **Examples**: Comprehensive usage examples
- **Error Scenarios**: Document error conditions
- **Performance Notes**: Performance characteristics

This crate provides the foundational abstractions that make it easy to implement new I/O adapters while maintaining consistency and performance across the Feldera platform.