# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Integrated Connectors module in the Adapters crate.

## Key Development Commands

### Building and Testing

```bash
# Build with all integrated connector features
cargo build -p adapters --features="with-deltalake,with-iceberg"

# Run PostgreSQL connector tests
cargo test -p adapters postgres

# Run Delta Lake connector tests (requires AWS credentials)
cargo test -p adapters delta_table

# Run Iceberg connector tests
cargo test -p adapters --features="iceberg-tests-fs" iceberg

# Build specific integrated connector
cargo build -p adapters --features="with-deltalake"
```

### Feature Flags

```toml
# Available integrated connector features
with-deltalake = ["deltalake"]        # Delta Lake support
with-iceberg = ["feldera-iceberg"]    # Apache Iceberg support
# PostgreSQL support is always enabled via tokio-postgres
```

## Architecture Overview

### Technology Stack

- **Database Integration**: PostgreSQL with tokio-postgres
- **Data Lake Integration**: Delta Lake with deltalake crate, Apache Iceberg
- **Query Engine**: DataFusion for Delta Lake filtering and projection
- **Storage Backends**: S3, Azure Blob, Google Cloud Storage
- **Data Formats**: Parquet for Delta Lake, custom serialization for PostgreSQL
- **Async Runtime**: Tokio for non-blocking I/O operations

### Core Purpose

The Integrated Connectors module provides **tightly-coupled transport and format implementations** for external data systems:

- **Single-Purpose Connectors**: Transport protocol and data format are integrated into one component
- **Database CDC**: Real-time change data capture from PostgreSQL
- **Data Lake Integration**: Batch and streaming access to Delta Lake and Iceberg tables
- **Format Optimization**: Native format handling without separate format layer
- **Schema Evolution**: Support for evolving schemas in data lake formats

## Project Structure

### Core Components

#### Main Module (`mod.rs`)
- Factory functions for creating integrated endpoints
- Trait definitions for `IntegratedInputEndpoint` and `IntegratedOutputEndpoint`
- Feature-gated module imports

#### PostgreSQL Integration (`postgres/`)
- **Input**: SQL query execution and result streaming
- **Output**: INSERT, UPSERT, DELETE operations with prepared statements
- **Features**: Type mapping, connection pooling, error retry logic

#### Delta Lake Integration (`delta_table/`)
- **Input**: Table scanning, incremental reads, change data feed (CDF)
- **Output**: Parquet file writing with Delta Lake transaction log
- **Features**: S3/Azure/GCS storage, schema evolution, time travel

#### Apache Iceberg Integration
- **External crate**: `feldera-iceberg` (referenced but implementation in separate crate)
- **Features**: Schema evolution, partition management, catalog integration

## Implementation Details

### Integrated Endpoint Architecture

#### Core Traits
```rust
// From mod.rs - integrated output endpoint combining transport and encoding
pub trait IntegratedOutputEndpoint: OutputEndpoint + Encoder {
    fn into_encoder(self: Box<Self>) -> Box<dyn Encoder>;
    fn as_endpoint(&mut self) -> &mut dyn OutputEndpoint;
}

// Factory function for creating integrated endpoints
pub fn create_integrated_output_endpoint(
    endpoint_id: EndpointId,
    endpoint_name: &str,
    connector_config: &ConnectorConfig,
    key_schema: &Option<Relation>,
    schema: &Relation,
    controller: Weak<ControllerInner>,
) -> Result<Box<dyn IntegratedOutputEndpoint>, ControllerError>
```

#### Format Integration Validation
```rust
// Integrated connectors don't allow separate format specification
if connector_config.format.is_some() {
    return Err(ControllerError::invalid_parser_configuration(
        endpoint_name,
        &format!("{} transport does not allow 'format' specification",
                connector_config.transport.name())
    ));
}
```

### PostgreSQL Integration

#### Input Connector - Query Execution
```rust
// From postgres/input.rs - SQL query execution and streaming
pub struct PostgresInputEndpoint {
    inner: Arc<PostgresInputEndpointInner>,
}

impl PostgresInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresReaderConfig,    // Contains SQL query and connection info
        consumer: Box<dyn InputConsumer>,
    ) -> Self
}
```

#### Connection Management
```rust
// From postgres/input.rs - PostgreSQL connection handling
async fn connect_to_postgres(&self)
    -> Result<(Client, Connection<Socket, NoTlsStream>), ControllerError> {

    let (client, connection) = tokio_postgres::connect(self.config.uri.as_str(), NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok((client, connection))
}
```

#### Type Mapping Implementation
```rust
// From postgres/input.rs - comprehensive PostgreSQL to JSON type mapping
let value = match *col_type {
    Type::BOOL => {
        let v: Option<bool> = row.get(col_idx);
        json!(v)
    }
    Type::VARCHAR | Type::TEXT => {
        let v: Option<String> = row.get(col_idx);
        json!(v)
    }
    Type::INT4 => {
        let v: Option<i32> = row.get(col_idx);
        json!(v)
    }
    Type::TIMESTAMP => {
        let v: Option<chrono::NaiveDateTime> = row.get(col_idx);
        let vutc = v.as_ref().map(|v| Utc.from_utc_datetime(v).to_rfc3339());
        json!(vutc)
    }
    Type::NUMERIC => {
        let v: Option<Decimal> = row.get(col_idx);
        json!(v)
    }
    // Support for arrays, UUIDs, and other PostgreSQL types...
};
```

#### Output Connector - Prepared Statements
```rust
// From postgres/output.rs - prepared statement management
struct PreparedStatements {
    insert: Statement,
    upsert: Statement,    // INSERT ... ON CONFLICT DO UPDATE
    delete: Statement,
}

// Error classification for retry logic
enum BackoffError {
    Temporary(anyhow::Error),    // Retry with backoff
    Permanent(anyhow::Error),    // Fail immediately
}

impl From<postgres::Error> for BackoffError {
    fn from(value: postgres::Error) -> Self {
        // Classify connection errors as temporary, SQL errors as permanent
        if value.is_closed() || matches!(value.code(), Some(SqlState::CONNECTION_FAILURE)) {
            Self::Temporary(anyhow!("failed to connect to postgres: {value}"))
        } else {
            Self::Permanent(anyhow!("postgres error: permanent: {value}"))
        }
    }
}
```

### Delta Lake Integration

#### Input Connector - Table Scanning
```rust
// From delta_table/input.rs - Delta Lake table scanning
pub struct DeltaTableInputEndpoint {
    inner: Arc<DeltaTableInputEndpointInner>,
}

impl DeltaTableInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &DeltaTableReaderConfig,  // Table URI, query filters, etc.
        consumer: Box<dyn InputConsumer>,
    ) -> Self
}
```

#### Storage Handler Registration
```rust
// From delta_table/mod.rs - cloud storage integration
static REGISTER_STORAGE_HANDLERS: Once = Once::new();

pub fn register_storage_handlers() {
    REGISTER_STORAGE_HANDLERS.call_once(|| {
        deltalake::aws::register_handlers(None);      // S3 support
        deltalake::azure::register_handlers(None);    // Azure Blob support
        deltalake::gcp::register_handlers(None);      // Google Cloud Storage
    });
}
```

#### Delta Lake-Specific Serialization
```rust
// From delta_table/mod.rs - Delta Lake serialization configuration
pub fn delta_input_serde_config() -> SqlSerdeConfig {
    SqlSerdeConfig::default()
        // Delta uses microsecond timestamps in Parquet
        .with_timestamp_format(TimestampFormat::String("%Y-%m-%dT%H:%M:%S%.f%Z"))
        .with_date_format(DateFormat::DaysSinceEpoch)
        .with_decimal_format(DecimalFormat::String)
        .with_uuid_format(UuidFormat::String)  // UUIDs as strings in Delta
}
```

#### DataFusion Integration for Query Pushdown
```rust
// From delta_table/input.rs - DataFusion integration for filtering
use datafusion::prelude::{SessionContext, DataFrame};
use deltalake::datafusion::logical_expr::Expr;

// Create DataFusion context for Delta Lake
let session_ctx = SessionContext::new();
let delta_table = DeltaTableBuilder::from_uri(&config.uri).build().await?;

// Apply filtering at storage layer for performance
if let Some(filter_expr) = &config.filter {
    let logical_plan = session_ctx
        .read_table(Arc::new(delta_table))?
        .filter(filter_expr.clone())?
        .logical_plan().clone();
}
```

### Multi-Format Support Patterns

#### Feature-Gated Endpoint Creation
```rust
// From mod.rs - conditional compilation based on features
pub fn create_integrated_input_endpoint(
    endpoint_name: &str,
    config: &ConnectorConfig,
    consumer: Box<dyn InputConsumer>,
) -> Result<Box<dyn IntegratedInputEndpoint>, ControllerError> {

    let ep: Box<dyn IntegratedInputEndpoint> = match &config.transport {
        #[cfg(feature = "with-deltalake")]
        DeltaTableInput(config) => Box::new(DeltaTableInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        #[cfg(feature = "with-iceberg")]
        IcebergInput(config) => Box::new(feldera_iceberg::IcebergInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        PostgresInput(config) => Box::new(PostgresInputEndpoint::new(
            endpoint_name, config, consumer
        )),

        transport => Err(ControllerError::unknown_input_transport(
            endpoint_name, &transport.name()
        ))?,
    };

    Ok(ep)
}
```

## Key Features and Capabilities

### PostgreSQL Integration
- **Query-based Input**: Execute arbitrary SQL queries and stream results
- **Comprehensive Type Support**: All PostgreSQL types including arrays and UUIDs
- **Change Streaming**: Real-time data ingestion from query results
- **Prepared Statements**: Optimized INSERT/UPSERT/DELETE operations
- **Connection Management**: Automatic reconnection and error recovery
- **Transaction Safety**: ACID compliance for output operations

### Delta Lake Integration
- **Schema Evolution**: Handle schema changes across table versions
- **Time Travel**: Read data from specific table versions or timestamps
- **Change Data Feed**: Incremental processing of table changes
- **Multi-Cloud Storage**: S3, Azure Blob, Google Cloud Storage support
- **Partition Pruning**: Efficient reading through partition elimination
- **DataFusion Optimization**: Query pushdown and column projection

### Apache Iceberg Integration
- **Catalog Integration**: Support for Hive, Glue, REST catalogs
- **Schema Evolution**: Advanced schema evolution capabilities
- **Partition Management**: Automatic partition lifecycle management
- **Multi-Engine Compatibility**: Compatible with Spark, Flink, Trino

## Configuration and Usage

### PostgreSQL Configuration
```yaml
# Input connector - execute query and stream results
transport:
  name: postgres_input
  config:
    uri: "postgresql://user:pass@localhost:5432/database"
    query: "SELECT id, name, created_at FROM users WHERE created_at > NOW() - INTERVAL '1 day'"

# Output connector - write to table
transport:
  name: postgres_output
  config:
    uri: "postgresql://user:pass@localhost:5432/database"
    table: "processed_events"
    mode: "upsert"  # insert, upsert, or append
```

### Delta Lake Configuration
```yaml
# Input connector - read from Delta Lake table
transport:
  name: delta_table_input
  config:
    uri: "s3://my-bucket/delta-tables/events/"
    mode: "snapshot"  # snapshot or follow
    version: 42  # optional: read specific version
    timestamp: "2024-01-15T10:00:00Z"  # optional: read at timestamp

# Output connector - write to Delta Lake
transport:
  name: delta_table_output
  config:
    uri: "s3://my-bucket/delta-tables/processed/"
    mode: "append"  # append or overwrite
```

## Development Workflow

### Adding New Integrated Connector

1. **Create connector module** in `src/integrated/my_connector/`
2. **Implement required traits**:
   - `IntegratedInputEndpoint` for input connectors
   - `IntegratedOutputEndpoint` for output connectors
3. **Add feature flag** in `Cargo.toml`
4. **Update factory functions** in `mod.rs`
5. **Add configuration types** in `feldera-types`
6. **Implement comprehensive tests** with real external services

### Testing Strategy

#### Unit Tests
- Type conversion and serialization correctness
- Error handling and retry logic
- Configuration validation

#### Integration Tests
- Real database/storage system connectivity
- End-to-end data flow validation
- Performance and reliability testing
- Schema evolution scenarios

### Performance Optimization

#### Connection Management
- **Connection Pooling**: Reuse database connections
- **Async I/O**: Non-blocking operations throughout
- **Batch Processing**: Optimize throughput with batching
- **Retry Logic**: Exponential backoff for transient failures

#### Data Processing
- **Schema Caching**: Cache schema information to avoid repeated lookups
- **Columnar Processing**: Leverage Arrow/Parquet columnar formats
- **Query Pushdown**: Push filters and projections to storage layer
- **Parallel Processing**: Multi-threaded data processing where possible

## Dependencies and Integration

### Core Dependencies
- **tokio-postgres** - PostgreSQL async client
- **deltalake** - Delta Lake Rust implementation with DataFusion
- **feldera-iceberg** - Apache Iceberg integration (separate crate)
- **datafusion** - Query engine for Delta Lake optimization
- **arrow** - Columnar data processing

### Cloud Storage Dependencies
- **AWS SDK** - S3 integration for Delta Lake
- **Azure SDK** - Azure Blob Storage support
- **Google Cloud SDK** - Google Cloud Storage support

### Error Handling Patterns
- **Classified Errors**: Distinguish between temporary and permanent failures
- **Retry Logic**: Exponential backoff for transient errors
- **Circuit Breakers**: Prevent cascade failures
- **Graceful Degradation**: Continue operation when possible

### Security Considerations
- **Connection Encryption**: TLS for database connections
- **Cloud Authentication**: IAM roles and service accounts
- **Credential Management**: Secure credential storage and rotation
- **Access Control**: Fine-grained permissions for data access

## Best Practices

### Configuration Design
- **Sensible Defaults**: Provide reasonable default values
- **Validation**: Validate configuration at startup
- **Documentation**: Clear documentation for all configuration options
- **Feature Flags**: Use feature flags for optional dependencies

### Error Handling
- **Structured Errors**: Use detailed error types with context
- **User-Friendly Messages**: Provide actionable error messages
- **Logging**: Comprehensive logging for debugging
- **Metrics**: Export metrics for monitoring and alerting

### Performance Monitoring
- **Throughput Metrics**: Track records/second processed
- **Latency Metrics**: Monitor end-to-end processing latency
- **Error Rates**: Track and alert on error rates
- **Resource Usage**: Monitor CPU, memory, and network usage

This module enables Feldera to integrate seamlessly with external data systems while maintaining high performance and reliability through native format handling and optimized data processing pipelines.