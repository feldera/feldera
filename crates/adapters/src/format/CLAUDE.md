# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the format module in the adapters crate.

## Overview

The format module implements Feldera's unified data serialization/deserialization layer, providing extensible support for multiple data formats (JSON, CSV, Avro, Parquet, Raw) while integrating seamlessly with DBSP circuits. It enables high-performance data parsing and generation across diverse external systems while maintaining strong type safety and comprehensive error handling.

## Architecture Overview

### **Plugin-Based Factory Pattern**

The format system uses a registry-based architecture with runtime format discovery:

```rust
// Global format registries for input/output operations
static INPUT_FORMATS: LazyLock<IndexMap<String, Box<dyn InputFormat>>> = LazyLock::new(|| {
    let mut formats = IndexMap::new();
    formats.insert("json".to_string(), Box::new(JsonInputFormat) as _);
    formats.insert("csv".to_string(), Box::new(CsvInputFormat) as _);
    formats.insert("avro".to_string(), Box::new(AvroInputFormat) as _);
    formats.insert("parquet".to_string(), Box::new(ParquetInputFormat) as _);
    formats.insert("raw".to_string(), Box::new(RawInputFormat) as _);
    formats
});
```

### **Three-Layer Architecture**

#### **1. Format Factory Layer**
```rust
trait InputFormat: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn new_parser(&self, endpoint_name: &str, input_stream: &InputCollectionHandle,
                  config: &YamlValue) -> Result<Box<dyn Parser>, ControllerError>;
}

trait OutputFormat: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn new_encoder(&self, endpoint_name: &str, config: &ConnectorConfig,
                   key_schema: &Option<Relation>, value_schema: &Relation,
                   consumer: Box<dyn OutputConsumer>) -> Result<Box<dyn Encoder>, ControllerError>;
}
```

#### **2. Stream Processing Layer**
```rust
trait Parser: Send + Sync {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>);
    fn splitter(&self) -> Box<dyn Splitter>;
    fn fork(&self) -> Box<dyn Parser>;
}

trait Encoder: Send {
    fn consumer(&mut self) -> &mut dyn OutputConsumer;
    fn encode(&mut self, batch: &dyn SerBatch) -> AnyResult<()>;
    fn fork(&self) -> Box<dyn Encoder>;
}
```

#### **3. Data Abstraction Layer**
```rust
trait InputBuffer: Send + Sync {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError>;
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer>;
    fn hash(&self) -> u64;
    fn len(&self) -> usize;
}
```

## Format Implementations

### **JSON Format** (`json/`)

**Most Feature-Rich Implementation** supporting diverse update patterns:

#### **Update Format Variants**
```rust
pub enum JsonUpdateFormat {
    Raw,                    // Direct record insertion: {"field1": "value1", "field2": "value2"}
    InsertDelete,          // Explicit ops: {"insert": {record}, "delete": {record}}
    Debezium,              // CDC: {"payload": {"op": "c|u|d", "before": {..}, "after": {..}}}
    Snowflake,             // Streaming: {"action": "INSERT", "data": {record}}
    Redis,                 // Key-value: {"key": "value", "op": "SET"}
}
```

#### **Advanced JSON Processing**
- **JSON Splitter**: Sophisticated state machine handling nested objects and arrays
- **Schema Generation**: Kafka Connect JSON schema generation for Debezium integration
- **Error Recovery**: Field-level error attribution with suggested fixes
- **Array Support**: Both single objects and object arrays

**JSON Splitter State Machine**:
```rust
enum JsonSplitterState {
    Start,              // Looking for object/array start
    InObject(u32),      // Inside object, tracking nesting depth
    InArray(u32),       // Inside array, tracking nesting depth
    InString,           // Inside quoted string
    Escape,             // Handling escape sequences
}
```

### **CSV Format** (`csv/`)

**High-Performance Text Processing** with custom deserializer:

#### **Key Features**
- **Custom Deserializer**: Forked from `csv` crate to expose low-level `Deserializer`
- **Streaming Processing**: Line-by-line processing with configurable delimiters
- **Quote Handling**: Proper CSV quote escaping and multi-line field support
- **Header Support**: Optional header row processing and field mapping

#### **Configuration Options**
```rust
pub struct CsvParserConfig {
    pub delimiter: u8,              // Field delimiter (default: comma)
    pub quote: u8,                  // Quote character (default: double quote)
    pub escape: Option<u8>,         // Escape character
    pub has_headers: bool,          // First row contains headers
    pub flexible: bool,             // Allow variable field counts
    pub comment: Option<u8>,        // Comment line prefix
}
```

### **Avro Format** (`avro/`)

**Enterprise Integration Focus** with schema registry support:

#### **Schema Registry Integration**
```rust
pub struct AvroConfig {
    pub registry_url: Option<String>,           // Confluent Schema Registry URL
    pub authentication: SchemaRegistryAuth,     // Authentication configuration
    pub schema: Option<String>,                 // Inline schema definition
    pub schema_id: Option<u32>,                // Registry schema ID
    pub proxy: Option<String>,                 // HTTP proxy configuration
    pub timeout_ms: Option<u64>,               // Request timeout
}

pub enum SchemaRegistryAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}
```

#### **Binary Processing**
- **Efficient Deserialization**: Direct Avro binary format processing
- **Schema Evolution**: Automatic handling of schema changes
- **Type Safety**: Strong typing with Rust's type system
- **Connection Pooling**: Reuse HTTP connections for schema registry access

### **Parquet Format** (`parquet/`)

**Analytics-Optimized Columnar Processing** with Arrow integration:

#### **Arrow Integration**
```rust
// Feldera SQL types → Arrow schema conversion
fn sql_to_arrow_type(sql_type: &SqlType) -> ArrowResult<DataType> {
    match sql_type {
        SqlType::Boolean => Ok(DataType::Boolean),
        SqlType::TinyInt => Ok(DataType::Int8),
        SqlType::SmallInt => Ok(DataType::Int16),
        SqlType::Integer => Ok(DataType::Int32),
        SqlType::BigInt => Ok(DataType::Int64),
        SqlType::Decimal(precision, scale) => Ok(DataType::Decimal128(*precision as u8, *scale as i8)),
        // ... comprehensive type mapping
    }
}
```

#### **Batch Processing Architecture**
- **Columnar Efficiency**: Leverages Arrow's vectorized operations
- **Large Dataset Support**: Streaming processing of multi-GB files
- **Delta Lake Integration**: Special handling for Databricks Delta Lake format
- **Type Preservation**: Maintains SQL type semantics through Arrow conversion

### **Raw Format** (`raw.rs`)

**Minimal Overhead Processing** for binary data:

#### **Processing Modes**
```rust
pub enum RawReaderMode {
    Lines,      // Split input by newlines, each line becomes a record
    Blob,       // Entire input becomes a single record
}
```

#### **Use Cases**
- **Binary Data**: Direct processing of binary streams
- **Log Processing**: Line-based log file ingestion
- **Custom Protocols**: Raw byte stream handling for specialized formats
- **Zero Parsing**: Minimal CPU overhead for high-throughput scenarios

## Stream Processing Architecture

### **Data Flow Pipeline**

```
Raw Bytes → Splitter → Parser → InputBuffer → DBSP Circuit
    ↑           ↑        ↑         ↑           ↑
    |           |        |         |           |
Transport   Boundary   Record   Batched    Database
Layer       Detection  Parsing   Records    Operations
```

### **Stream Splitting Strategies**

#### **Format-Specific Splitters**
```rust
trait Splitter: Send {
    fn input(&mut self, data: &[u8]) -> Option<usize>;  // Returns boundary position
    fn clear(&mut self);                                // Reset internal state
}

// Different splitting strategies:
pub struct LineSplitter;        // Newline-based splitting (CSV, Raw)
pub struct JsonSplitter;        // JSON object/array boundary detection
pub struct Sponge;              // No splitting - consume entire input (Parquet)
```

#### **Boundary Detection**
- **Incremental Processing**: Handle partial records across buffer boundaries
- **State Preservation**: Maintain parsing state between buffer chunks
- **Memory Efficiency**: Process large streams without loading entire content

### **Buffer Management**

#### **InputBuffer Trait Implementation**
```rust
trait InputBuffer: Send + Sync {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError>;
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer>;
    fn hash(&self) -> u64;                              // For replay consistency
    fn len(&self) -> usize;
}
```

#### **Advanced Buffer Features**
- **Partial Consumption**: `take_some()` for controlled batch processing
- **Replay Verification**: `hash()` for deterministic fault tolerance
- **Memory Management**: Efficient buffer allocation and reuse
- **Backpressure Handling**: Buffer size limits with overflow management

## Error Handling and Recovery

### **Structured Error System**

```rust
pub struct ParseError(Box<ParseErrorInner>);

struct ParseErrorInner {
    description: String,                    // Human-readable error description
    event_number: Option<u64>,             // Stream position for debugging
    field: Option<String>,                 // Failed field name
    invalid_bytes: Option<Vec<u8>>,        // Binary data that failed parsing
    invalid_text: Option<String>,          // Text data that failed parsing
    suggestion: Option<Cow<'static, str>>, // Suggested fix
}
```

### **Error Recovery Strategies**

#### **Graceful Degradation**
```rust
impl Parser for JsonParser {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        let mut buffer = JsonInputBuffer::new();
        let mut errors = Vec::new();

        // Continue processing after individual record failures
        for record in self.split_records(data) {
            match self.parse_record(record) {
                Ok(parsed) => buffer.push(parsed),
                Err(e) => {
                    errors.push(e);
                    // Continue with next record
                }
            }
        }

        (Some(Box::new(buffer)), errors)
    }
}
```

#### **Error Classification**
- **Field-Level Errors**: Specific field parsing failures with context
- **Record-Level Errors**: Entire record rejection with recovery suggestions
- **Batch-Level Errors**: Array parsing failures requiring batch rollback
- **Format-Level Errors**: Configuration or schema errors

## Type System Integration

### **Schema Conversion Pipeline**

#### **Unified Type Mapping**
```rust
// SQL types → Format-specific schema conversion
const JSON_SERDE_CONFIG: SqlSerdeConfig = SqlSerdeConfig {
    timestamp_format: TimestampFormat::String("%Y-%m-%d %H:%M:%S%.f".to_string()),
    date_format: DateFormat::String("%Y-%m-%d".to_string()),
    time_format: TimeFormat::String("%H:%M:%S%.f".to_string()),
    decimal_format: DecimalFormat::String,
    json_flavor: JsonFlavor::Default,
};
```

#### **Format-Specific Configurations**
- **JSON**: Flexible type coercion with null handling
- **CSV**: String-based parsing with configurable type inference
- **Avro**: Strong typing with schema evolution support
- **Parquet**: Arrow type system with SQL semantics preservation

### **Null Safety and Three-Valued Logic**

**Comprehensive Null Handling**:
```rust
// SQL NULL propagation through format layers
match field_value {
    Some(value) => parse_typed_value(value, field_type)?,
    None => Ok(SqlValue::Null),  // Preserve SQL null semantics
}
```

## Performance Optimization Patterns

### **Memory Management**

#### **Zero-Copy Processing**
```rust
// Direct byte slice processing where possible
fn parse_string_field(input: &[u8]) -> Result<&str, ParseError> {
    std::str::from_utf8(input).map_err(|e| ParseError::invalid_utf8(e))
}
```

#### **Buffer Reuse Patterns**
```rust
// Efficient buffer recycling
impl InputBuffer for JsonInputBuffer {
    fn take_some(&mut self, n: usize) -> Box<dyn InputBuffer> {
        let mut taken = Self::with_capacity(n);
        taken.records = self.records.drain(..n.min(self.records.len())).collect();
        Box::new(taken)
    }
}
```

### **Batch Processing Optimization**

#### **Configurable Batch Sizes**
- **Memory Constraints**: Limit batch sizes based on available memory
- **Latency Requirements**: Smaller batches for low-latency scenarios
- **Throughput Optimization**: Larger batches for maximum throughput

#### **Vectorized Operations**
- **Arrow Integration**: SIMD-optimized columnar operations for Parquet
- **Batch Type Conversion**: Vectorized SQL type conversions
- **Parallel Processing**: Multi-threaded parsing with `fork()` support

### **Network and I/O Optimization**

#### **Schema Registry Optimization**
```rust
// Connection pooling and caching
struct SchemaRegistryClient {
    client: Arc<reqwest::Client>,           // Reuse HTTP connections
    schema_cache: RwLock<HashMap<u32, Schema>>, // Cache schemas
    auth_token: Option<Arc<str>>,           // Cached authentication
}
```

## Testing Infrastructure

### **Comprehensive Test Strategy**

#### **Property-Based Testing**
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn json_parser_handles_arbitrary_input(input in ".*") {
        let parser = JsonParser::new(config);
        let (buffer, errors) = parser.parse(input.as_bytes());
        // Verify parsing never panics and errors are structured
        assert!(buffer.is_some() || !errors.is_empty());
    }
}
```

#### **Mock Infrastructure**
```rust
// Isolated testing with mock consumers
struct MockOutputConsumer {
    records: Vec<MockUpdate>,
    errors: Vec<ParseError>,
}

impl OutputConsumer for MockOutputConsumer {
    fn batch_end(&mut self) -> AnyResult<()> {
        // Capture batch boundaries for testing
    }
}
```

### **Format Compatibility Testing**

#### **Round-Trip Verification**
```rust
#[test]
fn test_json_roundtrip() {
    let original_data = generate_test_records();

    // Serialize with encoder
    let encoded = JsonEncoder::encode(&original_data)?;

    // Parse with parser
    let (parsed_buffer, errors) = JsonParser::parse(&encoded)?;

    // Verify data integrity
    assert_eq!(original_data, parsed_buffer.records);
    assert!(errors.is_empty());
}
```

## Configuration and Setup Patterns

### **Hierarchical Configuration System**

#### **Multi-Source Configuration**
```rust
// HTTP Request → Format Configuration
fn config_from_http_request(&self, endpoint_name: &str, request: &HttpRequest)
    -> Result<Box<dyn ErasedSerialize>, ControllerError> {

    // Extract format-specific configuration from HTTP headers/body
    let config = self.extract_config_from_request(request)?;
    self.validate_config(&config)?;
    Ok(Box::new(config))
}

// YAML Configuration → Parser Instance
fn new_parser(&self, endpoint_name: &str, input_stream: &InputCollectionHandle,
              config: &YamlValue) -> Result<Box<dyn Parser>, ControllerError> {

    let parsed_config = self.parse_yaml_config(config)?;
    self.validate_parser_config(&parsed_config)?;
    Ok(Box::new(self.create_parser(parsed_config)))
}
```

#### **Configuration Validation**
- **Schema Validation**: Early validation of format configurations
- **Transport Compatibility**: Format-transport compatibility verification
- **Resource Constraints**: Memory and performance limit validation
- **Security Validation**: Authentication and authorization checks

## Integration Patterns

### **Transport Layer Integration**

#### **Transport-Agnostic Design**
The format layer maintains complete independence from transport specifics:
- **Abstract Interfaces**: `InputConsumer`/`OutputConsumer` for transport integration
- **Configuration Extraction**: HTTP, YAML, and programmatic configuration sources
- **Buffer Negotiation**: Transport-specific buffer size and batching limits

### **DBSP Circuit Integration**

#### **Streaming Data Flow**
```rust
// Direct integration with DBSP circuits
impl InputBuffer for FormatInputBuffer {
    fn flush(&mut self, handle: &InputCollectionHandle) -> Vec<ParseError> {
        let mut errors = Vec::new();

        // Stream records directly to DBSP circuit
        for record in self.records.drain(..) {
            match handle.insert(record) {
                Ok(_) => {},
                Err(e) => errors.push(ParseError::circuit_error(e)),
            }
        }

        errors
    }
}
```

## Development Best Practices

### **Adding New Format Support**

1. **Create Format Module**: Add new module under `format/`
2. **Implement Core Traits**: `InputFormat`, `OutputFormat`, `Parser`, `Encoder`
3. **Add Stream Splitter**: Implement boundary detection logic
4. **Type System Integration**: Map format types to SQL types
5. **Configuration Support**: Add YAML and HTTP configuration parsing
6. **Comprehensive Testing**: Unit tests, integration tests, property tests
7. **Documentation**: Update format documentation and examples
8. **Registry Integration**: Add to `INPUT_FORMATS`/`OUTPUT_FORMATS` registries

### **Performance Tuning Guidelines**

#### **Memory Optimization**
- **Profile Memory Usage**: Use `cargo bench` and memory profiling tools
- **Optimize Hot Paths**: Focus on frequently executed parsing code
- **Buffer Size Tuning**: Configure optimal buffer sizes for workload
- **Connection Pooling**: Reuse network connections where possible

#### **Throughput Optimization**
- **Batch Size Tuning**: Balance latency vs throughput requirements
- **Parallel Processing**: Leverage multi-core processing with `fork()`
- **Zero-Copy Patterns**: Avoid unnecessary data copying
- **SIMD Integration**: Use vectorized operations for columnar formats

### **Error Handling Best Practices**

#### **User-Friendly Errors**
- **Precise Error Messages**: Include field names, positions, and suggested fixes
- **Error Attribution**: Link errors back to original data sources
- **Graceful Recovery**: Continue processing after recoverable errors
- **Rich Context**: Provide sufficient context for debugging

This format module represents a sophisticated, production-ready data processing system that successfully abstracts the complexity of multiple data formats while providing high performance, reliability, and extensibility for the Feldera streaming analytics platform.