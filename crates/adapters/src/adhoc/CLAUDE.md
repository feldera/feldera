# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Ad-hoc Query module in the Adapters crate.

## Key Development Commands

### Building and Testing

```bash
# Build the adhoc module as part of adapters crate
cargo build -p adapters

# Run tests for the adhoc module
cargo test -p adapters adhoc

# Run tests with DataFusion features enabled
cargo test -p adapters --features="with-datafusion"

# Check documentation
cargo doc -p adapters --open
```

### Running Ad-hoc Queries

```bash
# Via CLI
fda exec pipeline-name "SELECT * FROM materialized_view"

# Interactive shell
fda shell pipeline-name

# Via HTTP API
curl "http://localhost:8080/v0/pipelines/my-pipeline/query?sql=SELECT%20*%20FROM%20users&format=json"
```

## Architecture Overview

### Technology Stack

- **Query Engine**: Apache DataFusion for SQL query execution
- **Output Formats**: Text tables, JSON, Parquet, Arrow IPC
- **Transport**: WebSocket and HTTP streaming
- **Runtime**: Multi-threaded execution using DBSP's Tokio runtime
- **Storage**: Direct access to DBSP's materialized state

### Core Purpose

The Ad-hoc Query module provides **batch SQL query capabilities** alongside Feldera's core incremental processing:

- **Interactive Queries**: Real-time SQL queries against current pipeline state
- **Multiple Formats**: Support for various output formats optimized for different use cases
- **Streaming Results**: Memory-efficient streaming of large result sets
- **DataFusion Integration**: Full SQL compatibility through Apache DataFusion engine

This is the primary module that is responsible for ad-hoc queries backend functionality.

### Project Structure

#### Core Files

- `mod.rs` - Main module with WebSocket handling and session context creation
- `executor.rs` - Query execution and result streaming implementations
- `format.rs` - Text table formatting utilities
- `table.rs` - DataFusion table provider and INSERT operation support

## Implementation Details

### DataFusion Session Configuration

#### Session Context Setup
```rust
// From mod.rs - actual DataFusion configuration
pub(crate) fn create_session_context(config: &PipelineConfig) -> Result<SessionContext> {
    const SORT_IN_PLACE_THRESHOLD_BYTES: usize = 64 * 1024 * 1024;
    const SORT_SPILL_RESERVATION_BYTES: usize = 64 * 1024 * 1024;

    let session_config = SessionConfig::new()
        .with_target_partitions(config.global.workers as usize)
        .with_sort_in_place_threshold_bytes(SORT_IN_PLACE_THRESHOLD_BYTES)
        .with_sort_spill_reservation_bytes(SORT_SPILL_RESERVATION_BYTES);

    // Memory pool configuration for large queries
    let mut runtime_env_builder = RuntimeEnvBuilder::new();
    if let Some(memory_mb_max) = config.global.resources.memory_mb_max {
        let memory_bytes_max = memory_mb_max * 1024 * 1024;
        runtime_env_builder = runtime_env_builder
            .with_memory_pool(Arc::new(FairSpillPool::new(memory_bytes_max as usize)));
    }

    // Spill-to-disk directory for temp files during large operations
    if let Some(storage) = &config.storage_config {
        let path = PathBuf::from(storage.path.clone()).join("adhoc-tmp");
        runtime_env_builder = runtime_env_builder.with_temp_file_path(path);
    }
}
```

### Query Execution Pipeline

#### Multi-Threaded Execution Strategy
```rust
// From executor.rs - execution in DBSP runtime for parallelization
fn execute_stream(df: DataFrame) -> Receiver<DFResult<SendableRecordBatchStream>> {
    let (tx, rx) = oneshot::channel();
    // Execute in DBSP's multi-threaded runtime, not actix-web's single-threaded runtime
    dbsp::circuit::tokio::TOKIO.spawn(async move {
        let _r = tx.send(df.execute_stream().await);
    });
    rx
}
```

#### Result Streaming Implementation
```rust
// Four different streaming formats implemented:

// 1. Text format - human-readable tables
pub(crate) fn stream_text_query(df: DataFrame)
    -> impl Stream<Item = Result<ByteString, PipelineError>>

// 2. JSON format - line-delimited JSON records (deprecated)
pub(crate) fn stream_json_query(df: DataFrame)
    -> impl Stream<Item = Result<ByteString, PipelineError>>

// 3. Arrow IPC format - high-performance binary streaming
pub(crate) fn stream_arrow_query(df: DataFrame)
    -> impl Stream<Item = Result<Bytes, DataFusionError>>

// 4. Parquet format - columnar data with compression
pub(crate) fn stream_parquet_query(df: DataFrame)
    -> impl Stream<Item = Result<Bytes, DataFusionError>>
```

### WebSocket Handler Implementation

#### Real-time Query Processing
```rust
// From mod.rs - WebSocket message handling
pub async fn adhoc_websocket(
    df_session: SessionContext,
    req: HttpRequest,
    stream: Payload,
) -> Result<HttpResponse, PipelineError> {
    let (res, mut ws_session, stream) = actix_ws::handle(&req, stream)?;
    let mut stream = stream
        .max_frame_size(MAX_WS_FRAME_SIZE)  // 2MB frame limit
        .aggregate_continuations()
        .max_continuation_size(4 * MAX_WS_FRAME_SIZE);

    // Process WebSocket messages
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(AggregatedMessage::Text(text)) => {
                let args = serde_json::from_str::<AdhocQueryArgs>(&text)?;
                let df = df_session
                    .sql_with_options(&args.sql, SQLOptions::new().with_allow_ddl(false))
                    .await?;
                adhoc_query_handler(df, ws_session.clone(), args).await?;
            }
            // Handle other message types...
        }
    }
}
```

### Table Provider Implementation

#### DBSP State Integration
```rust
// From table.rs - AdHocTable provides DataFusion access to DBSP materialized state
pub struct AdHocTable {
    controller: Weak<ControllerInner>,           // Weak ref to avoid cycles
    input_handle: Option<Box<dyn DeCollectionHandle>>,  // For INSERT operations
    name: SqlIdentifier,
    materialized: bool,                          // Only materialized tables are queryable
    schema: Arc<Schema>,
    snapshots: ConsistentSnapshots,              // Current state snapshots
}

#[async_trait]
impl TableProvider for AdHocTable {
    async fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>)
        -> Result<Arc<dyn ExecutionPlan>> {

        // Validate materialization requirement
        if !self.materialized {
            return Err(DataFusionError::Execution(
                format!("Make sure `{}` is configured as materialized: \
                use `with ('materialized' = 'true')` for tables, or `create materialized view` for views",
                self.name)
            ));
        }

        // Create execution plan
        Ok(Arc::new(AdHocQueryExecution::new(/* ... */)))
    }
}
```

#### INSERT Operation Support
```rust
// From table.rs - INSERT INTO materialized tables
async fn insert_into(&self, input: Arc<dyn ExecutionPlan>, overwrite: InsertOp)
    -> Result<Arc<dyn ExecutionPlan>> {

    match &self.input_handle {
        Some(ih) => {
            // Create temporary adhoc input endpoint
            let endpoint_name = format!("adhoc-ingress-{}-{}", self.name.name(), Uuid::new_v4());
            let sink = Arc::new(AdHocTableSink::new(/* ... */));
            Ok(Arc::new(DataSinkExec::new(input, sink, None)))
        }
        None => exec_err!("Called insert_into on a view")
    }
}
```

### Query Execution Details

#### Record Batch Processing
```rust
// From table.rs - efficient batch processing from DBSP cursors
let mut cursor = batch_reader.cursor(RecordFormat::Parquet(
    output_adhoc_arrow_serde_config().clone()
))?;

const MAX_BATCH_SIZE: usize = 256;  // Optimized for latency vs throughput
let mut cur_batch_size = 0;

while cursor.key_valid() {
    if cursor.weight() > 0 {  // Skip deleted records
        cursor.serialize_key_to_arrow(&mut insert_builder.builder)?;
        cur_batch_size += 1;

        if cur_batch_size >= MAX_BATCH_SIZE {
            let batch = insert_builder.builder.to_record_batch()?;
            send_batch(&tx, &projection, batch).await?;
            cur_batch_size = 0;
        }
    }
    cursor.step_key();
}
```

### Output Format Implementations

#### Text Table Formatting
```rust
// From format.rs - human-readable table output
pub(crate) fn create_table(results: &[RecordBatch]) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");  // comfy-table preset

    // Add headers from schema
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }

    // Process each row with cell length limiting
    const CELL_MAX_LENGTH: usize = 64;
    for formatter in &formatters {
        let mut content = formatter.value(row).to_string();
        if content.len() > CELL_MAX_LENGTH {
            content.truncate(CELL_MAX_LENGTH);
            content.push_str("...");
        }
    }
}
```

#### Parquet Streaming
```rust
// From executor.rs - efficient Parquet streaming with compression
const PARQUET_CHUNK_SIZE: usize = MAX_WS_FRAME_SIZE / 2;  // 1MB chunks

let mut writer = AsyncArrowWriter::try_new(
    ChannelWriter::new(tx),
    schema,
    Some(WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()),
)?;

// Stream with controlled chunk sizes
while let Some(batch) = stream.next().await.transpose()? {
    writer.write(&batch).await?;
    if writer.in_progress_size() > PARQUET_CHUNK_SIZE {
        writer.flush().await?;  // Send chunk to client
    }
}
```

## Key Features and Limitations

### Supported Operations
- **SELECT queries** from materialized tables and views
- **Complex queries** with joins, aggregations, window functions
- **INSERT statements** into materialized tables
- **Multiple output formats** for different use cases
- **Streaming results** for memory efficiency

### Critical Limitations
- **Materialization Required**: Only `WITH ('materialized' = 'true')` tables/views are queryable
- **No DDL Operations**: CREATE, ALTER, DROP are explicitly disabled
- **SQL Dialect Differences**: DataFusion SQL vs Feldera SQL (Calcite-based) differences
- **Resource Sharing**: Shares CPU/memory with main pipeline processing
- **WebSocket Frame Limits**: 2MB maximum frame size

### Configuration Requirements

#### Pipeline Configuration
```sql
-- Tables must be explicitly materialized
CREATE TABLE users (id INT, name STRING) WITH ('materialized' = 'true');

-- Or use materialized views
CREATE MATERIALIZED VIEW user_stats AS
  SELECT COUNT(*) as count, AVG(age) as avg_age FROM users;
```

#### Memory and Threading
- Uses pipeline's configured worker thread count
- Memory limits configurable via pipeline resources
- Spill-to-disk support for large operations
- Automatic parallelization across available cores

## Development Workflow

### Adding New Output Formats

1. Add format enum variant to `AdHocResultFormat` in feldera-types
2. Implement streaming function in `executor.rs`
3. Add format handling in `adhoc_query_handler()` in `mod.rs`
4. Add HTTP response handling in `stream_adhoc_result()`
5. Update client SDKs and documentation

### Performance Optimization

#### Memory Management
- Configure appropriate memory pools for large queries
- Use spill-to-disk for operations exceeding memory limits
- Optimize batch sizes for latency vs throughput trade-offs
- Monitor WebSocket frame sizes to prevent timeouts

#### Query Performance
- Leverage DataFusion's query optimizer
- Use projection pushdown when possible
- Consider materialized view design for common queries
- Monitor resource usage impact on main pipeline

### Error Handling Patterns

#### Materialization Validation
```rust
if !self.materialized {
    return Err(DataFusionError::Execution(
        format!("Tried to SELECT from a non-materialized source. Make sure `{}` is configured as materialized",
                self.name)
    ));
}
```

#### Resource Management Errors
- Memory limit exceeded → automatic spill to disk
- WebSocket connection lost → graceful cleanup
- Query timeout → configurable limits with clear messages
- Invalid SQL → DataFusion parser error propagation

## Dependencies and Integration

### Core Dependencies
- **datafusion** - SQL query engine and execution
- **arrow** - Columnar data processing and formats
- **actix-web** - WebSocket and HTTP handling
- **tokio** - Async runtime and streaming
- **parquet** - Columnar file format support

### Integration Points
- **Controller**: Access to circuit state and snapshots
- **Transport**: AdHoc input endpoints for INSERT operations
- **Storage**: Direct access to materialized table storage
- **Types**: Shared configuration and error types

### Performance Characteristics
- **Throughput**: Optimized for interactive query latency
- **Memory Usage**: Bounded by configuration with spill support
- **Parallelization**: Automatic multi-threading via DataFusion
- **Format Efficiency**: Binary formats (Arrow, Parquet) for high throughput

This module bridges the gap between Feldera's incremental streaming processing and traditional batch SQL analytics, providing essential inspection and analysis capabilities for developers and operators.