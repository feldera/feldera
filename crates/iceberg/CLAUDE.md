# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Iceberg crate.

## Key Development Commands

### Building and Testing

```bash
# Build the iceberg crate
cargo build -p iceberg

# Run tests (requires test environment)
cargo test -p iceberg

# Set up test environment
cd src/test
python create_test_table_s3.py

# Install test dependencies
pip install -r requirements.txt
```

## Architecture Overview

### Technology Stack

- **Apache Iceberg**: Open table format for large analytic datasets
- **S3 Integration**: AWS S3 and S3-compatible storage
- **Async I/O**: Non-blocking I/O operations with tokio
- **Python Integration**: Test utilities with Python ecosystem

### Core Purpose

Iceberg provides **Apache Iceberg table format support** for Feldera:

- **Table Format**: Support for Iceberg's open table format
- **Cloud Storage**: Integration with S3 and cloud storage
- **Schema Evolution**: Handle schema changes over time
- **Time Travel**: Support for historical data queries

### Project Structure

#### Core Modules

- `src/lib.rs` - Core Iceberg functionality
- `src/input.rs` - Iceberg table input adapter
- `src/test/` - Test utilities and setup scripts

## Important Implementation Details

### Iceberg Integration

#### Table Format Support
```rust
pub struct IcebergTable {
    pub metadata: TableMetadata,
    pub schema: Schema,
    pub partition_spec: PartitionSpec,
}

impl IcebergTable {
    pub async fn load_from_catalog(&self, path: &str) -> Result<IcebergTable, IcebergError>;
    pub async fn scan(&self, filter: Option<Expression>) -> Result<FileScan, IcebergError>;
}
```

#### Features
- **Metadata Management**: Handle Iceberg metadata files
- **Schema Evolution**: Support schema changes over time
- **Partitioning**: Efficient data partitioning schemes
- **File Management**: Track data files and manifests

### Input Adapter

#### Data Ingestion
```rust
pub struct IcebergInputAdapter {
    table: IcebergTable,
    scan: FileScan,
    reader: ParquetReader,
}

impl InputAdapter for IcebergInputAdapter {
    async fn read_batch(&mut self) -> Result<RecordBatch, InputError> {
        let files = self.scan.next_files().await?;
        let batch = self.reader.read_files(files).await?;
        Ok(batch)
    }
}
```

#### Adapter Features
- **Incremental Reading**: Read only new/changed data
- **Parallel Processing**: Concurrent file reading
- **Format Support**: Parquet and other Iceberg-supported formats
- **Filter Pushdown**: Push filters to storage layer

### Cloud Storage Integration

#### S3 Support
```rust
pub struct S3IcebergCatalog {
    client: S3Client,
    warehouse_location: String,
}

impl IcebergCatalog for S3IcebergCatalog {
    async fn load_table(&self, name: &str) -> Result<IcebergTable, CatalogError>;
    async fn create_table(&self, name: &str, schema: Schema) -> Result<(), CatalogError>;
}
```

#### Storage Features
- **Multi-Cloud**: Support for AWS S3, Azure, GCS
- **Authentication**: Handle cloud credentials securely
- **Performance**: Optimized for cloud storage patterns
- **Cost Optimization**: Minimize storage operations

### Test Infrastructure

#### Python Test Setup
```python
# create_test_table_s3.py
import pyiceberg
from pyiceberg.catalog import load_catalog

def create_test_table():
    catalog = load_catalog("test")
    schema = pyiceberg.schema.Schema([
        pyiceberg.types.NestedField(1, "id", pyiceberg.types.LongType()),
        pyiceberg.types.NestedField(2, "name", pyiceberg.types.StringType()),
    ])

    catalog.create_table("test.table", schema)
```

#### Test Features
- **Realistic Data**: Generate realistic test datasets
- **Schema Variations**: Test various schema configurations
- **Performance Testing**: Measure ingestion performance
- **Integration Testing**: End-to-end pipeline testing

## Development Workflow

### For Iceberg Features

1. Study Apache Iceberg specification
2. Implement feature following Iceberg standards
3. Add comprehensive tests with real data
4. Test with various storage backends
5. Validate performance characteristics
6. Document compatibility and limitations

### For Storage Integration

1. Implement storage backend interface
2. Add authentication and configuration
3. Test with real cloud storage
4. Optimize for performance and cost
5. Add error handling and retry logic
6. Document setup and configuration

### Testing Strategy

#### Unit Tests
- **Metadata Parsing**: Test Iceberg metadata handling
- **Schema Evolution**: Test schema change scenarios
- **Partitioning**: Test partition pruning logic
- **Error Handling**: Test various error conditions

#### Integration Tests
- **Real Storage**: Test with actual S3 buckets
- **Large Data**: Test with realistic data sizes
- **Concurrent Access**: Test parallel reading
- **Schema Evolution**: Test with evolving schemas

### Configuration

#### Iceberg Configuration
```rust
pub struct IcebergConfig {
    pub catalog_type: CatalogType,
    pub warehouse_location: String,
    pub s3_endpoint: Option<String>,
    pub credentials: CredentialsConfig,
}

pub enum CatalogType {
    Hive,
    Hadoop,
    S3,
    Custom(Box<dyn IcebergCatalog>),
}
```

#### Storage Configuration
- **Credentials**: AWS credentials, IAM roles, access keys
- **Endpoints**: S3 endpoints, regions, custom endpoints
- **Performance**: Connection pooling, retry policies
- **Security**: Encryption, access control

### Performance Optimization

#### Read Optimization
- **Parallel Reading**: Read multiple files concurrently
- **Filter Pushdown**: Apply filters at storage level
- **Column Pruning**: Read only required columns
- **Vectorized Processing**: Efficient data processing

#### Memory Management
- **Streaming**: Stream large datasets without loading entirely
- **Buffer Management**: Optimize memory usage
- **Resource Control**: Limit concurrent operations
- **Garbage Collection**: Efficient memory cleanup

### Configuration Files

- `Cargo.toml` - Iceberg and cloud storage dependencies
- `src/test/requirements.txt` - Python test dependencies
- `src/test/requirements.ci.txt` - CI-specific Python dependencies

### Dependencies

#### Core Dependencies
- `iceberg-rs` - Rust Iceberg implementation
- `tokio` - Async runtime
- `aws-sdk-s3` - AWS S3 integration
- `parquet` - Parquet file format support

#### Test Dependencies
- `tempfile` - Temporary test files
- `uuid` - Test data generation
- Python ecosystem for test data setup

### Best Practices

#### Iceberg Usage
- **Standards Compliance**: Follow Apache Iceberg specification
- **Schema Design**: Design schemas for evolution
- **Partitioning**: Choose appropriate partitioning strategies
- **Metadata Management**: Handle metadata efficiently

#### Cloud Integration
- **Cost Awareness**: Minimize cloud storage costs
- **Performance**: Optimize for cloud storage patterns
- **Security**: Follow cloud security best practices
- **Reliability**: Handle transient cloud failures

#### Error Handling
- **Transient Errors**: Retry transient cloud failures
- **Schema Errors**: Handle schema incompatibilities gracefully
- **Resource Errors**: Handle resource exhaustion
- **Data Errors**: Handle corrupted or missing data

### Usage Examples

#### Basic Table Access
```rust
use iceberg::{IcebergTable, S3IcebergCatalog};

let catalog = S3IcebergCatalog::new(s3_config);
let table = catalog.load_table("warehouse.orders").await?;

let scan = table.scan(None).await?;
let batches = scan.collect().await?;
```

#### Filtered Reading
```rust
use iceberg::expressions::Expression;

let filter = Expression::gt("order_date", "2023-01-01");
let scan = table.scan(Some(filter)).await?;

for batch in scan {
    process_batch(batch?).await?;
}
```

This crate enables Feldera to work with modern data lake architectures using the Apache Iceberg table format for large-scale analytics.