# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Feldera Types crate.

## Key Development Commands

### Building and Testing

```bash
# Build the feldera-types crate
cargo build -p feldera-types

# Run tests
cargo test -p feldera-types

# Check documentation
cargo doc -p feldera-types --open

# Run with all features
cargo build -p feldera-types --all-features
```

## Architecture Overview

### Technology Stack

- **Serialization**: serde with JSON support
- **Type System**: Rust's type system with trait-based abstractions
- **Configuration**: Structured configuration types
- **Error Handling**: Comprehensive error types and conversions

### Core Purpose

Feldera Types provides **shared type definitions** and **configuration structures** used across the entire Feldera platform:

- **Configuration Types**: Pipeline, connector, and transport configurations
- **Data Format Types**: Schema definitions for supported data formats
- **Error Types**: Standardized error handling across components
- **Transport Types**: Configuration for various transport mechanisms

### Project Structure

#### Core Modules

- `src/config.rs` - Pipeline and runtime configuration
- `src/transport/` - Transport-specific configuration types
- `src/format/` - Data format configuration and schemas
- `src/error.rs` - Error types and conversions
- `src/query.rs` - Query and program schema definitions

## Important Implementation Details

### Configuration Architecture

#### Pipeline Configuration
```rust
use feldera_types::config::PipelineConfig;

let config = PipelineConfig {
    workers: Some(4),
    storage: Some(storage_config),
    resources: Some(resource_limits),
    ..Default::default()
};
```

#### Transport Configuration
```rust
use feldera_types::transport::{KafkaInputConfig, HttpOutputConfig};

// Kafka input configuration
let kafka_config = KafkaInputConfig {
    brokers: vec!["localhost:9092".to_string()],
    topic: "input_topic".to_string(),
    group_id: Some("consumer_group".to_string()),
    ..Default::default()
};
```

### Data Format Types

#### Format Configuration
- **CSV**: Field delimiters, headers, escaping rules
- **JSON**: Schema validation, type coercion settings
- **Avro**: Schema registry integration, evolution policies
- **Parquet**: Compression, column selection, batch sizing

#### Schema Definitions
```rust
use feldera_types::program_schema::{Field, SqlType, Relation};

let table_schema = Relation {
    name: "users".to_string(),
    fields: vec![
        Field {
            name: "id".to_string(),
            columntype: SqlType::Integer { nullable: false },
            case_sensitive: false,
        },
        Field {
            name: "name".to_string(),
            columntype: SqlType::Varchar { nullable: true, precision: None },
            case_sensitive: false,
        },
    ],
    ..Default::default()
};
```

### Transport Types

#### Supported Transports
- **Kafka**: Broker configuration, topic settings, consumer groups
- **HTTP**: Endpoint URLs, authentication, rate limiting
- **File**: Path specifications, file formats, polling intervals
- **PostgreSQL**: Connection strings, table mappings, CDC settings
- **Delta Lake**: Storage locations, partition schemes, versioning

### Error Handling

#### Error Categories
- **Configuration Errors**: Invalid settings, missing required fields
- **Validation Errors**: Schema mismatches, type conflicts
- **Runtime Errors**: Transport failures, format conversion errors
- **System Errors**: Resource exhaustion, permission issues

```rust
use feldera_types::error::DetailedError;

// Structured error with context
let error = DetailedError::invalid_configuration(
    "Invalid Kafka broker configuration",
    Some("brokers field cannot be empty"),
);
```

## Development Workflow

### For New Configuration Types

1. Add configuration struct in appropriate module
2. Implement `Default`, `Serialize`, `Deserialize` traits
3. Add validation logic with `Validate` trait
4. Add comprehensive documentation with examples
5. Add unit tests for serialization/deserialization
6. Update dependent crates to use new configuration

### For New Data Types

1. Define type in appropriate module
2. Implement required traits (Clone, Debug, etc.)
3. Add serde support for JSON serialization
4. Add conversion methods to/from other representations
5. Add validation logic if needed
6. Test edge cases and error conditions

### Testing Strategy

#### Unit Tests
- Serialization round-trip testing
- Configuration validation testing
- Error message formatting
- Default value behavior

#### Integration Tests
- Cross-crate compatibility
- Real configuration file parsing
- Error propagation across boundaries

### Validation Framework

The crate includes a validation framework for configuration:

```rust
use feldera_types::config::Validate;

impl Validate for MyConfig {
    fn validate(&self) -> Result<(), DetailedError> {
        if self.workers == 0 {
            return Err(DetailedError::invalid_configuration(
                "workers must be greater than 0",
                None,
            ));
        }
        Ok(())
    }
}
```

### Serialization Patterns

#### JSON Serialization
- **Snake Case**: Field names use snake_case convention
- **Optional Fields**: Use `Option<T>` for optional configuration
- **Default Values**: Implement sensible defaults
- **Validation**: Validate after deserialization

#### Custom Serialization
```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", content = "config")]
pub enum TransportConfig {
    Kafka(KafkaConfig),
    Http(HttpConfig),
    File(FileConfig),
}
```

### Configuration Files

- `Cargo.toml` - Minimal dependencies for type definitions
- Feature flags for optional functionality
- Version compatibility across Feldera components

### Key Design Principles

- **Backward Compatibility**: Schema evolution without breaking changes
- **Type Safety**: Leverage Rust's type system for correctness
- **Documentation**: Comprehensive field documentation
- **Validation**: Runtime validation with helpful error messages
- **Modularity**: Separate concerns by transport/format type

### Dependencies

#### Core Dependencies
- `serde` - Serialization framework
- `serde_json` - JSON format support
- `chrono` - Date/time types
- `uuid` - UUID generation

#### Optional Dependencies
- `url` - URL parsing and validation
- `regex` - Pattern matching for validation
- `base64` - Encoding/decoding support

### Best Practices

#### Configuration Design
- **Sensible Defaults**: Most fields should have reasonable defaults
- **Clear Naming**: Field names should be self-documenting
- **Validation**: Validate configuration at construction time
- **Documentation**: Include examples in field documentation

#### Error Handling
- **Structured Errors**: Use DetailedError for rich error information
- **Context**: Provide helpful context in error messages
- **Recovery**: Design errors to be actionable
- **Consistency**: Use consistent error patterns across types

#### Type Design
- **Composability**: Types should compose well together
- **Extensibility**: Design for future extension
- **Performance**: Avoid unnecessary allocations
- **Testing**: Include comprehensive test coverage

### Usage Patterns

#### Configuration Loading
```rust
use feldera_types::config::PipelineConfig;

// Load from JSON file
let config: PipelineConfig = serde_json::from_str(&json_content)?;
config.validate()?;

// Merge with defaults
let final_config = PipelineConfig {
    workers: config.workers.or(Some(1)),
    ..config
};
```

#### Schema Validation
```rust
use feldera_types::program_schema::ProgramSchema;

// Validate program schema
let schema: ProgramSchema = serde_json::from_str(&schema_json)?;
schema.validate_consistency()?;
```

This crate is foundational to the Feldera platform, providing the type system backbone for configuration, data formats, and error handling across all components.