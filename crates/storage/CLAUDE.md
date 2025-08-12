# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Storage crate.

## Key Development Commands

### Building and Testing

```bash
# Build the storage crate
cargo build -p storage

# Run tests
cargo test -p storage

# Run tests with specific backend
cargo test -p storage --features=test_posixio
cargo test -p storage --features=test_memory

# Benchmark storage operations
cargo bench -p storage
```

## Architecture Overview

### Technology Stack

- **Storage Backends**: Pluggable storage implementations
- **Async I/O**: Non-blocking file operations with tokio
- **Error Handling**: Comprehensive storage error types
- **Testing**: Mock and real backend testing

### Core Purpose

Storage provides **persistent storage abstractions** for DBSP:

- **Backend Abstraction**: Pluggable storage implementations
- **Async Interface**: Non-blocking storage operations
- **Error Recovery**: Robust error handling and retry logic
- **Performance**: Optimized for DBSP's access patterns

### Project Structure

#### Core Modules

- `src/backend/` - Storage backend implementations
- `src/file.rs` - File-based storage interface
- `src/block.rs` - Block-level storage operations
- `src/fbuf.rs` - File buffer management
- `src/error.rs` - Storage error types

## Important Implementation Details

### Storage Backend Trait

#### Core Interface
```rust
pub trait StorageBackend: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn read(&self, path: &str, offset: u64, length: usize) -> Result<Vec<u8>, Self::Error>;
    async fn write(&self, path: &str, offset: u64, data: &[u8]) -> Result<(), Self::Error>;
    async fn delete(&self, path: &str) -> Result<(), Self::Error>;
    async fn exists(&self, path: &str) -> Result<bool, Self::Error>;
}
```

#### Backend Implementations

**Memory Backend**
- In-memory storage for testing
- Fast access, no persistence
- Used for unit tests and development

**POSIX I/O Backend**
- Standard filesystem operations
- Production storage backend
- Supports all POSIX-compliant filesystems

### File Interface

#### Async File Operations
```rust
pub struct AsyncFile {
    backend: Arc<dyn StorageBackend>,
    path: String,
}

impl AsyncFile {
    pub async fn read_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError>;
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), StorageError>;
    pub async fn sync(&self) -> Result<(), StorageError>;
}
```

#### File Features
- **Random Access**: Read/write at arbitrary offsets
- **Async Operations**: Non-blocking I/O
- **Error Handling**: Robust error recovery
- **Sync Operations**: Force data to storage

### Block-Level Operations

#### Block Interface
```rust
pub struct Block {
    data: Vec<u8>,
    offset: u64,
    dirty: bool,
}

impl Block {
    pub fn read(&self, offset: usize, length: usize) -> &[u8];
    pub fn write(&mut self, offset: usize, data: &[u8]);
    pub fn is_dirty(&self) -> bool;
}
```

#### Block Features
- **Caching**: In-memory block caching
- **Dirty Tracking**: Optimize write operations
- **Batch Operations**: Efficient bulk I/O
- **Alignment**: Optimal block alignment

### File Buffer Management

#### Buffer Pool
```rust
pub struct FileBuffer {
    blocks: HashMap<u64, Block>,
    capacity: usize,
    backend: Arc<dyn StorageBackend>,
}

impl FileBuffer {
    pub async fn read_block(&mut self, offset: u64) -> Result<&Block, StorageError>;
    pub async fn write_block(&mut self, offset: u64, data: &[u8]) -> Result<(), StorageError>;
    pub async fn flush(&mut self) -> Result<(), StorageError>;
}
```

#### Buffer Features
- **LRU Eviction**: Least Recently Used block eviction
- **Write Coalescing**: Batch multiple writes
- **Read Ahead**: Predictive block loading
- **Memory Limits**: Bounded memory usage

## Development Workflow

### For New Storage Backend

1. Implement `StorageBackend` trait
2. Define backend-specific error types
3. Add configuration for backend parameters
4. Implement all required async methods
5. Add comprehensive error handling
6. Test with various failure scenarios
7. Add performance benchmarks

### For Storage Optimization

1. Profile existing storage patterns
2. Identify bottlenecks in I/O operations
3. Implement caching or batching optimizations
4. Test performance improvements
5. Validate correctness with stress tests
6. Document performance characteristics

### Testing Strategy

#### Unit Tests
- **Backend Interface**: Test all backend implementations
- **Error Conditions**: Network failures, disk full, permission errors
- **Concurrency**: Multiple simultaneous operations
- **Data Integrity**: Verify data consistency

#### Integration Tests
- **Real Storage**: Test with actual filesystems
- **Performance**: Throughput and latency measurement
- **Reliability**: Long-running stability tests
- **Recovery**: Error recovery and retry logic

### Error Handling

#### Error Categories
```rust
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Backend error: {0}")]
    Backend(Box<dyn std::error::Error + Send + Sync>),

    #[error("Configuration error: {0}")]
    Config(String),
}
```

#### Error Handling Patterns
- **Retry Logic**: Automatic retry with exponential backoff
- **Circuit Breaker**: Fail fast when backend is unavailable
- **Graceful Degradation**: Continue with reduced functionality
- **Error Logging**: Structured error logging for debugging

### Performance Optimization

#### I/O Patterns
- **Sequential Access**: Optimize for sequential reads/writes
- **Batch Operations**: Group multiple operations
- **Alignment**: Align I/O to block boundaries
- **Prefetching**: Read-ahead for predictable patterns

#### Memory Management
- **Buffer Pooling**: Reuse memory buffers
- **Zero-Copy**: Minimize data copying
- **Compression**: Optional data compression
- **Memory Mapping**: Use mmap for large files

### Configuration

#### Backend Configuration
```rust
pub struct StorageConfig {
    pub backend: BackendType,
    pub cache_size: usize,
    pub block_size: usize,
    pub max_concurrent_ops: usize,
}
```

#### Tuning Parameters
- **Cache Size**: Memory allocated for caching
- **Block Size**: I/O operation granularity
- **Concurrency**: Maximum parallel operations
- **Retry Policy**: Error retry configuration

### Configuration Files

- `Cargo.toml` - Storage backend dependencies and feature flags
- Feature flags for different backends and testing modes

### Dependencies

#### Core Dependencies
- `tokio` - Async runtime and I/O
- `thiserror` - Error handling
- `futures` - Async utilities
- `tracing` - Structured logging

#### Backend Dependencies
- `libc` - POSIX I/O operations
- `memmap2` - Memory mapping
- `lz4` - Optional compression

### Best Practices

#### API Design
- **Async First**: All operations are async by default
- **Error Rich**: Detailed error information with context
- **Resource Safe**: Proper cleanup and resource management
- **Performance Aware**: Design for high-performance patterns

#### Implementation
- **Zero-Copy**: Minimize data copying where possible
- **Batch Operations**: Group operations for efficiency
- **Error Recovery**: Robust error handling and retry logic
- **Memory Bounds**: Prevent unbounded memory usage

#### Testing
- **Real Backends**: Test with actual storage systems
- **Failure Injection**: Test error conditions systematically
- **Performance Tests**: Validate performance characteristics
- **Concurrency Tests**: Test thread safety and race conditions

This crate provides the storage foundation that enables DBSP to persist state and handle large datasets that don't fit in memory.