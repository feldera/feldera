use crate::transport::{
    InputEndpoint, InputQueue, InputReaderCommand, IntegratedInputEndpoint, NonFtInputReaderCommand,
};
use crate::{ControllerError, InputConsumer, InputReader, PipelineState, RecordFormat};
use anyhow::{Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use etl::config::{
    BatchConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig, PgConnectionConfig,
    PipelineConfig, TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
};
use etl::destination::Destination;
use etl::destination::async_result::{
    TruncateTableResult, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::EtlResult;
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use etl::types::{ArrayCell, Cell, Event, TableId, TableRow};
use feldera_adapterlib::catalog::{DeCollectionStream, InputCollectionHandle};
use feldera_adapterlib::format::ParseError;
use feldera_adapterlib::transport::{Resume, Watermark};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::format::json::JsonFlavor;
use feldera_types::transport::postgres::PostgresCdcReaderConfig;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch::{Receiver, Sender, channel};
use tracing::{debug, error, info, warn};
use url::Url;
use xxhash_rust::xxh3::xxh3_64;

/// Deferred async result senders waiting for step completion.
type DeferredSenders = Vec<WriteEventsResult<()>>;

/// Integrated input connector that reads from Postgres via logical replication (CDC).
pub struct PostgresCdcInputEndpoint {
    inner: Arc<PostgresCdcInputInner>,
}

impl PostgresCdcInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        Self {
            inner: Arc::new(PostgresCdcInputInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        }
    }
}

impl InputEndpoint for PostgresCdcInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::AtLeastOnce)
    }
}

impl IntegratedInputEndpoint for PostgresCdcInputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PostgresCdcInputReader::new(
            &self.inner,
            input_handle,
        )?))
    }
}

struct PostgresCdcInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<PostgresCdcInputInner>,
}

impl PostgresCdcInputReader {
    fn new(
        endpoint: &Arc<PostgresCdcInputInner>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let endpoint_clone = endpoint.clone();

        let (init_status_sender, init_status_receiver) =
            tokio::sync::oneshot::channel::<Result<(), ControllerError>>();

        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))?;

        thread::Builder::new()
            .name("postgres-cdc-input-tokio-wrapper".to_string())
            .spawn(move || {
                TOKIO.block_on(async {
                    let _ = endpoint_clone
                        .worker_task(input_stream, receiver, init_status_sender)
                        .await;
                })
            })
            .expect("failed to create Postgres CDC input connector thread");

        init_status_receiver.blocking_recv().map_err(|_| {
            ControllerError::input_transport_error(
                &endpoint.endpoint_name,
                true,
                anyhow!("worker thread terminated unexpectedly during initialization"),
            )
        })??;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
        })
    }
}

impl InputReader for PostgresCdcInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => {
                // Flush queue to circuit, collecting timestamps for watermarks.
                let (buffer_size, _hasher, flushed) = self.inner.queue.flush_with_aux();

                let watermarks: Vec<Watermark> = flushed
                    .iter()
                    .map(|(ts, _)| Watermark::new(*ts, None))
                    .collect();

                // Build resume metadata so Feldera can checkpoint our position.
                // The actual resume state is managed by etl's PostgresStore;
                // we just need a stable identifier so the controller knows we
                // support resumption.
                let resume_metadata = json!({
                    "pipeline_id": self.inner.pipeline_id,
                });
                let resume = Resume::Seek {
                    seek: resume_metadata,
                };

                // Report data to controller with resume metadata (must be
                // called exactly once per Queue command).
                self.inner
                    .consumer
                    .extended(buffer_size, Some(resume), watermarks);

                // Take any deferred senders that write_events stored.
                let senders: DeferredSenders =
                    std::mem::take(&mut *self.inner.pending_senders.lock().unwrap());

                if !senders.is_empty() {
                    if let Some(tx) = self.inner.completion_tx.as_ref() {
                        // Snapshot total_completed_steps AFTER flush.  The
                        // data will land in the next step (>completed), so
                        // this value is the correct lower bound for both
                        // fast mode (fire when completed_steps > this) and
                        // strict mode (fire when checkpointed_steps > this,
                        // per the `total_checkpointed_steps >= n` semantics).
                        let step_at_flush = self
                            .inner
                            .completion_rx
                            .as_ref()
                            .map(|rx| rx.borrow().total_completed_steps)
                            .unwrap_or(0);
                        let _ = tx.send((step_at_flush, senders));
                    } else {
                        // No completion tracking — fire immediately.
                        for sender in senders {
                            sender.send(Ok(()));
                        }
                    }
                }
            }
            NonFtInputReaderCommand::Transition(state) => drop(self.sender.send_replace(state)),
        }
    }

    fn is_closed(&self) -> bool {
        self.inner.queue.is_empty() && self.sender.is_closed()
    }
}

impl Drop for PostgresCdcInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

struct PostgresCdcInputInner {
    endpoint_name: String,
    config: PostgresCdcReaderConfig,
    consumer: Box<dyn InputConsumer>,
    queue: Arc<InputQueue>,
    /// Deterministic pipeline ID used for replication slot naming and resume.
    pipeline_id: u64,
    /// Deferred async result senders from `write_events`, waiting to be paired
    /// with a step number during the next `Queue` command.
    pending_senders: Arc<Mutex<DeferredSenders>>,
    /// Watch receiver for step completion — used to snapshot `step_at_flush`
    /// in the Queue handler.  Always tracks `total_completed_steps`.
    completion_rx: Option<tokio::sync::watch::Receiver<Completion>>,
    /// Watcher source for the background task.  Taken once by `worker_task_inner`.
    /// `Strict` when fault tolerance is enabled (gates slot on checkpoint);
    /// `Fast` otherwise (gates slot on step completion).
    watcher_rx: Mutex<Option<WatcherReceiver>>,
    /// Sender for passing (step_at_flush, senders) to the background task.
    /// Created once at construction time if completion tracking is available.
    completion_tx: Option<mpsc::UnboundedSender<(u64, DeferredSenders)>>,
    /// Receiver half, taken once by worker_task_inner to spawn the background task.
    completion_task_rx: Mutex<Option<mpsc::UnboundedReceiver<(u64, DeferredSenders)>>>,
}

impl PostgresCdcInputInner {
    fn new(
        endpoint_name: &str,
        config: PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let completion_rx = consumer.completion_watcher();

        // Deterministic pipeline ID from config — stable across Rust versions (xxh3).
        // Excludes password and other volatile fields so that rotating the password
        // doesn't change the pipeline_id (which would orphan the replication slot
        // and stored etl state, forcing a full re-snapshot).
        let pipeline_id = {
            let identity =
                stable_connection_identity(&config.uri, &config.publication, &config.source_table);
            xxh3_64(identity.as_bytes())
        };

        // Use strict mode (gate slot on checkpoint) when fault tolerance is enabled;
        // fast mode (gate slot on step completion) otherwise.
        let watcher_rx = match consumer.checkpoint_watcher() {
            Some(rx) => Some(WatcherReceiver::Strict(rx)),
            None => completion_rx.clone().map(WatcherReceiver::Fast),
        };

        let (completion_tx, completion_task_rx) = if watcher_rx.is_some() {
            let (tx, rx) = mpsc::unbounded_channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        Self {
            endpoint_name: endpoint_name.to_string(),
            config,
            consumer,
            queue,
            pipeline_id,
            pending_senders: Arc::new(Mutex::new(Vec::new())),
            completion_rx,
            watcher_rx: Mutex::new(watcher_rx),
            completion_tx,
            completion_task_rx: Mutex::new(completion_task_rx),
        }
    }

    async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
    ) {
        let mut receiver_clone = receiver.clone();
        select! {
            _ = self.clone().worker_task_inner(input_stream, receiver, init_status_sender) => {
                debug!("postgres_cdc {}: worker task terminated", &self.endpoint_name);
            }
            _ = receiver_clone.wait_for(|state| state == &PipelineState::Terminated) => {
                debug!("postgres_cdc {}: received termination command; worker task canceled",
                    &self.endpoint_name);
            }
        }
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        mut receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
    ) {
        let pg_conn = match parse_pg_uri(&self.config.uri) {
            Ok(conn) => conn,
            Err(e) => {
                let _ =
                    init_status_sender.send(Err(ControllerError::invalid_transport_configuration(
                        &self.endpoint_name,
                        &format!("failed to parse Postgres URI: {e}"),
                    )));
                return;
            }
        };

        let pipeline_config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.config.publication.clone(),
            pg_connection: pg_conn.clone(),
            batch: BatchConfig::default(),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(MemoryBackpressureConfig::default()),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
        };

        // Use PostgresStore to persist table replication phases across restarts.
        // This allows etl to resume from the replication slot position instead of
        // re-snapshotting the entire table on restart.
        let store = match PostgresStore::new(self.pipeline_id, pg_conn).await {
            Ok(store) => store,
            Err(e) => {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to initialize PostgresStore: {e}"),
                )));
                return;
            }
        };

        // Spawn the completion watcher background task if tracking is available.
        // The watcher and the channel were created in new(); we take them here.
        let completion_handle = match (
            self.watcher_rx.lock().unwrap().take(),
            self.completion_task_rx.lock().unwrap().take(),
        ) {
            (Some(watcher), Some(rx)) => Some(tokio::spawn(completion_watcher_task(
                watcher,
                rx,
                self.endpoint_name.clone(),
            ))),
            _ => None,
        };

        let pending_senders = if self.completion_rx.is_some() {
            Some(Arc::clone(&self.pending_senders))
        } else {
            None
        };

        let destination = FelderaDestination {
            input_stream: Arc::new(Mutex::new(input_stream)),
            queue: Arc::clone(&self.queue),
            source_table: self.config.source_table.clone(),
            endpoint_name: self.endpoint_name.clone(),
            relation_cache: Arc::new(Mutex::new(HashMap::new())),
            pending_senders,
        };

        let mut pipeline = Pipeline::new(pipeline_config, store, destination);

        match pipeline.start().await {
            Ok(()) => {
                info!(
                    "postgres_cdc {}: etl pipeline started for publication '{}', table '{}'",
                    &self.endpoint_name, &self.config.publication, &self.config.source_table,
                );
                let _ = init_status_sender.send(Ok(()));
            }
            Err(e) => {
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to start etl pipeline: {e}"),
                )));
                return;
            }
        }

        wait_running(&mut receiver).await;

        if let Err(e) = pipeline.wait().await {
            error!(
                "postgres_cdc {}: etl pipeline error: {e}",
                &self.endpoint_name
            );
            self.consumer.error(true, anyhow!(e), None);
        }

        // Shut down the completion watcher task.
        if let Some(handle) = completion_handle {
            handle.abort();
            let _ = handle.await;
        }

        self.consumer.eoi();
    }
}

/// Relation metadata cached from WAL Relation events.
#[derive(Clone, Debug)]
struct RelationInfo {
    table_name: String,
    schema_name: String,
    column_names: Vec<String>,
}

/// etl Destination implementation that pushes data into a Feldera DeCollectionStream.
#[derive(Clone)]
struct FelderaDestination {
    input_stream: Arc<Mutex<Box<dyn DeCollectionStream>>>,
    queue: Arc<InputQueue>,
    source_table: String,
    endpoint_name: String,
    relation_cache: Arc<Mutex<HashMap<u32, RelationInfo>>>,
    /// Deferred async result senders. If `Some`, write_events stores senders here
    /// instead of firing them immediately. The Queue handler picks them up.
    pending_senders: Option<Arc<Mutex<DeferredSenders>>>,
}

impl Destination for FelderaDestination {
    fn name() -> &'static str {
        "feldera"
    }

    async fn truncate_table(
        &self,
        table_id: TableId,
        async_result: TruncateTableResult<()>,
    ) -> EtlResult<()> {
        warn!(
            "postgres_cdc {}: truncate_table called for table_id={}, ignoring",
            &self.endpoint_name, table_id
        );
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult<()>,
    ) -> EtlResult<()> {
        let is_target = {
            let cache = self.relation_cache.lock().unwrap();
            if let Some(info) = cache.get(&u32::from(table_id)) {
                self.is_target_table(&info.schema_name, &info.table_name)
            } else {
                // During snapshot, we may not have relation info yet.
                // Accept rows and let the data flow through.
                true
            }
        };

        if !is_target {
            return Ok(());
        }

        let column_names: Vec<String> = {
            let cache = self.relation_cache.lock().unwrap();
            cache
                .get(&u32::from(table_id))
                .map(|info| info.column_names.clone())
                .unwrap_or_else(|| {
                    // During snapshot without relation info, generate fallback names
                    // based on the first row's column count.
                    let n = table_rows.first().map(|r| r.values().len()).unwrap_or(0);
                    (0..n).map(|i| format!("col_{i}")).collect()
                })
        };

        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for row in &table_rows {
            let cells = row.values();
            let json_value = row_to_json(cells, &column_names);

            let json_str = json_value.to_string();
            if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                errors.push(ParseError::text_event_error(
                    "Failed to deserialize CDC snapshot row",
                    e,
                    0,
                    Some(&json_str),
                    None,
                ));
            }
            bytes += json_str.len();

            if bytes >= 2 * 1024 * 1024 {
                self.queue.push((stream.take_all(), errors), timestamp);
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.queue.push((stream.take_all(), errors), timestamp);
        }

        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        async_result: WriteEventsResult<()>,
    ) -> EtlResult<()> {
        let mut stream = self.input_stream.lock().unwrap();
        let mut bytes = 0;
        let mut errors = Vec::new();
        let timestamp = Utc::now();

        for event in &events {
            match event {
                Event::Relation(rel) => {
                    let table_schema = &rel.table_schema;
                    let info = RelationInfo {
                        table_name: table_schema.name.name.clone(),
                        schema_name: table_schema.name.schema.clone(),
                        column_names: table_schema
                            .column_schemas
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    };
                    debug!(
                        "postgres_cdc {}: relation event for {}.{} (id={})",
                        &self.endpoint_name, &info.schema_name, &info.table_name, table_schema.id,
                    );
                    self.relation_cache
                        .lock()
                        .unwrap()
                        .insert(u32::from(table_schema.id), info);
                }
                Event::Insert(insert) => {
                    if !self.is_target_table_by_id(u32::from(insert.table_id)) {
                        continue;
                    }
                    if let Some(cols) = self.get_column_names(u32::from(insert.table_id)) {
                        let json_value = row_to_json(insert.table_row.values(), &cols);
                        let json_str = json_value.to_string();
                        if let Err(e) = stream.insert(json_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC insert",
                                e,
                                0,
                                Some(&json_str),
                                None,
                            ));
                        }
                        bytes += json_str.len();
                    }
                }
                Event::Update(update) => {
                    if !self.is_target_table_by_id(u32::from(update.table_id)) {
                        continue;
                    }
                    if let Some(cols) = self.get_column_names(u32::from(update.table_id)) {
                        // Delete old row if available
                        if let Some((_full, old_row)) = &update.old_table_row {
                            let old_json = row_to_json(old_row.values(), &cols);
                            let old_str = old_json.to_string();
                            if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                                errors.push(ParseError::text_event_error(
                                    "Failed to deserialize CDC update (old)",
                                    e,
                                    0,
                                    Some(&old_str),
                                    None,
                                ));
                            }
                            bytes += old_str.len();
                        }
                        // Insert new row
                        let new_json = row_to_json(update.table_row.values(), &cols);
                        let new_str = new_json.to_string();
                        if let Err(e) = stream.insert(new_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC update (new)",
                                e,
                                0,
                                Some(&new_str),
                                None,
                            ));
                        }
                        bytes += new_str.len();
                    }
                }
                Event::Delete(delete) => {
                    if !self.is_target_table_by_id(u32::from(delete.table_id)) {
                        continue;
                    }
                    if let Some(cols) = self.get_column_names(u32::from(delete.table_id))
                        && let Some((_full, old_row)) = &delete.old_table_row
                    {
                        let old_json = row_to_json(old_row.values(), &cols);
                        let old_str = old_json.to_string();
                        if let Err(e) = stream.delete(old_str.as_bytes(), &None) {
                            errors.push(ParseError::text_event_error(
                                "Failed to deserialize CDC delete",
                                e,
                                0,
                                Some(&old_str),
                                None,
                            ));
                        }
                        bytes += old_str.len();
                    }
                }
                Event::Truncate(_) => {
                    warn!(
                        "postgres_cdc {}: received TRUNCATE event, ignoring",
                        &self.endpoint_name
                    );
                }
                Event::Begin(_) | Event::Commit(_) | Event::Unsupported => {}
            }

            if bytes >= 2 * 1024 * 1024 {
                self.queue.push((stream.take_all(), errors), timestamp);
                bytes = 0;
                errors = Vec::new();
            }
        }

        if bytes > 0 || !errors.is_empty() {
            self.queue.push((stream.take_all(), errors), timestamp);
        }

        // Defer or fire the async result.
        if let Some(ref pending) = self.pending_senders {
            pending.lock().unwrap().push(async_result);
        } else {
            async_result.send(Ok(()));
        }

        Ok(())
    }
}

impl FelderaDestination {
    fn is_target_table(&self, schema_name: &str, table_name: &str) -> bool {
        let qualified = format!("{schema_name}.{table_name}");
        self.source_table == qualified
            || self.source_table == table_name
            || self.source_table == format!("\"{schema_name}\".\"{table_name}\"")
    }

    fn is_target_table_by_id(&self, table_id: u32) -> bool {
        let cache = self.relation_cache.lock().unwrap();
        if let Some(info) = cache.get(&table_id) {
            self.is_target_table(&info.schema_name, &info.table_name)
        } else {
            false
        }
    }

    fn get_column_names(&self, table_id: u32) -> Option<Vec<String>> {
        let cache = self.relation_cache.lock().unwrap();
        cache.get(&table_id).map(|info| info.column_names.clone())
    }
}

/// Convert a row of cells to a JSON object using the given column names.
fn row_to_json(cells: &[Cell], column_names: &[String]) -> Value {
    let mut map = serde_json::Map::new();
    for (i, cell) in cells.iter().enumerate() {
        let col_name = column_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("col_{i}"));
        map.insert(col_name, cell_to_json(cell));
    }
    Value::Object(map)
}

/// Convert an etl Cell to a serde_json Value.
fn cell_to_json(cell: &Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => json!(b),
        Cell::String(s) => json!(s),
        Cell::I16(n) => json!(n),
        Cell::I32(n) => json!(n),
        Cell::U32(n) => json!(n),
        Cell::I64(n) => json!(n),
        Cell::F32(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::F64(f) => {
            if f.is_nan() || f.is_infinite() {
                Value::Null
            } else {
                json!(f)
            }
        }
        Cell::Numeric(n) => {
            // Preserve precision by encoding as string.
            json!(n.to_string())
        }
        Cell::Date(d) => json!(d.to_string()),
        Cell::Time(t) => json!(t.to_string()),
        Cell::Timestamp(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
        Cell::TimestampTz(ts) => json!(ts.to_rfc3339()),
        Cell::Uuid(u) => json!(u.to_string()),
        Cell::Json(j) => j.clone(),
        Cell::Bytes(b) => {
            // Encode as byte array to match Datagen's BinaryFormat::Array.
            json!(b)
        }
        Cell::Array(arr) => array_cell_to_json(arr),
    }
}

/// Convert an etl ArrayCell to a JSON array.
fn array_cell_to_json(arr: &ArrayCell) -> Value {
    match arr {
        ArrayCell::Bool(v) => json!(v),
        ArrayCell::String(v) => json!(v),
        ArrayCell::I16(v) => json!(v),
        ArrayCell::I32(v) => json!(v),
        ArrayCell::U32(v) => json!(v),
        ArrayCell::I64(v) => json!(v),
        ArrayCell::F32(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::F64(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(f) if f.is_nan() || f.is_infinite() => Value::Null,
                    Some(f) => json!(f),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Numeric(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(n) => json!(n.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Date(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(d) => json!(d.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Time(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(t) => json!(t.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Timestamp(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => json!(ts.format("%Y-%m-%dT%H:%M:%S%.f").to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::TimestampTz(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(ts) => json!(ts.to_rfc3339()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Uuid(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(u) => json!(u.to_string()),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Json(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(j) => j.clone(),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
        ArrayCell::Bytes(v) => {
            let vals: Vec<Value> = v
                .iter()
                .map(|opt| match opt {
                    Some(b) => json!(b),
                    None => Value::Null,
                })
                .collect();
            Value::Array(vals)
        }
    }
}

/// Typed watch receiver used by the completion watcher background task.
///
/// `Fast` waits for step completion (`total_completed_steps`); used when fault
/// tolerance is not enabled.  `Strict` waits for checkpoint completion; used
/// when fault tolerance is enabled so the replication slot only advances past
/// the last durable checkpoint, preserving at-least-once correctness for
/// stateful circuits after a crash.
enum WatcherReceiver {
    Fast(tokio::sync::watch::Receiver<Completion>),
    Strict(tokio::sync::watch::Receiver<u64>),
}

impl WatcherReceiver {
    async fn changed(&mut self) -> Result<(), tokio::sync::watch::error::RecvError> {
        match self {
            Self::Fast(rx) => rx.changed().await,
            Self::Strict(rx) => rx.changed().await,
        }
    }

    fn frontier(&self) -> u64 {
        match self {
            Self::Fast(rx) => rx.borrow().total_completed_steps,
            Self::Strict(rx) => *rx.borrow(),
        }
    }
}

/// Background task that fires deferred ETL async result senders when the
/// completion frontier passes the step recorded at Queue time.
///
/// Each entry is `(step_at_flush, senders)` where `step_at_flush` is the
/// value of `total_completed_steps` at the time the data was flushed to the
/// circuit.  The data lands in the next step, so we fire when the frontier
/// strictly exceeds `step_at_flush`.
async fn completion_watcher_task(
    mut watcher: WatcherReceiver,
    mut pending_rx: mpsc::UnboundedReceiver<(u64, DeferredSenders)>,
    endpoint_name: String,
) {
    let mut waiting: Vec<(u64, DeferredSenders)> = Vec::new();

    loop {
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    break; // Sender dropped (pipeline shutting down)
                }
                let f = watcher.frontier();
                fire_completed(&mut waiting, f);
            }
            maybe_entry = pending_rx.recv() => {
                match maybe_entry {
                    Some((step_at_flush, senders)) => {
                        let f = watcher.frontier();
                        if f > step_at_flush {
                            // Already past the threshold — fire immediately.
                            for sender in senders {
                                sender.send(Ok(()));
                            }
                        } else {
                            waiting.push((step_at_flush, senders));
                        }
                    }
                    None => break, // Channel closed
                }
            }
        }
    }

    // On shutdown, remaining senders are dropped. AsyncResult's Drop impl
    // sends an error to the ETL side, causing it to shut down gracefully.
    debug!(
        "postgres_cdc {endpoint_name}: completion watcher exiting with {} pending entries",
        waiting.len()
    );
}

/// Fires deferred senders whose data has been fully processed.
fn fire_completed(waiting: &mut Vec<(u64, DeferredSenders)>, completed_steps: u64) {
    waiting.retain_mut(|(step_at_flush, senders)| {
        if completed_steps > *step_at_flush {
            for sender in senders.drain(..) {
                sender.send(Ok(()));
            }
            false
        } else {
            true
        }
    });
}

/// Build a stable identity string for pipeline_id hashing.
///
/// Extracts host/port/database from the URI (excludes password, username,
/// and query parameters) combined with publication and source_table.
/// Falls back to the raw URI if parsing fails — the pipeline will likely
/// fail startup shortly after anyway with a clearer error.
fn stable_connection_identity(uri: &str, publication: &str, source_table: &str) -> String {
    let (host, port, db) = match Url::parse(uri) {
        Ok(url) => {
            let host = url.host_str().unwrap_or("").to_string();
            let port = url.port().unwrap_or(5432);
            let db = url.path().trim_start_matches('/').to_string();
            (host, port, db)
        }
        Err(_) => return format!("{uri}\0{publication}\0{source_table}"),
    };
    format!("{host}:{port}/{db}\0{publication}\0{source_table}")
}

/// Parse a Postgres URI into etl's PgConnectionConfig.
fn parse_pg_uri(uri: &str) -> AnyResult<PgConnectionConfig> {
    let url = Url::parse(uri)?;

    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("missing host in URI"))?
        .to_string();
    let port = url.port().unwrap_or(5432);
    let username = url.username().to_string();
    if username.is_empty() {
        return Err(anyhow!("missing username in URI"));
    }
    let password = url.password().map(|p| p.to_string().into());
    let name = url.path().trim_start_matches('/').to_string();
    if name.is_empty() {
        return Err(anyhow!("missing database name in URI"));
    }

    Ok(PgConnectionConfig {
        host,
        port,
        name,
        username,
        password,
        tls: TlsConfig::disabled(),
        keepalive: TcpKeepaliveConfig::default(),
    })
}

/// Block until the state is `Running`.
async fn wait_running(receiver: &mut Receiver<PipelineState>) {
    let _ = receiver
        .wait_for(|state| state == &PipelineState::Running)
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
    use etl::types::PgNumeric;
    use serde_json::json;
    use std::str::FromStr;

    // -----------------------------------------------------------------------
    // cell_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cell_null() {
        assert_eq!(cell_to_json(&Cell::Null), Value::Null);
    }

    #[test]
    fn test_cell_bool() {
        assert_eq!(cell_to_json(&Cell::Bool(true)), json!(true));
        assert_eq!(cell_to_json(&Cell::Bool(false)), json!(false));
    }

    #[test]
    fn test_cell_string() {
        assert_eq!(cell_to_json(&Cell::String("hello".into())), json!("hello"));
        assert_eq!(cell_to_json(&Cell::String("".into())), json!(""));
        // Unicode
        assert_eq!(
            cell_to_json(&Cell::String("caf\u{00e9}".into())),
            json!("caf\u{00e9}")
        );
    }

    #[test]
    fn test_cell_integers() {
        assert_eq!(cell_to_json(&Cell::I16(42)), json!(42));
        assert_eq!(cell_to_json(&Cell::I16(-1)), json!(-1));
        assert_eq!(cell_to_json(&Cell::I32(100_000)), json!(100_000));
        assert_eq!(
            cell_to_json(&Cell::U32(4_000_000_000)),
            json!(4_000_000_000u64)
        );
        assert_eq!(cell_to_json(&Cell::I64(i64::MAX)), json!(i64::MAX));
        assert_eq!(cell_to_json(&Cell::I64(i64::MIN)), json!(i64::MIN));
    }

    #[test]
    fn test_cell_f32() {
        assert_eq!(cell_to_json(&Cell::F32(3.14)), json!(3.14f32));
        // NaN and infinity produce null
        assert_eq!(cell_to_json(&Cell::F32(f32::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F32(f32::NEG_INFINITY)), Value::Null);
    }

    #[test]
    fn test_cell_f64() {
        assert_eq!(cell_to_json(&Cell::F64(2.718)), json!(2.718f64));
        assert_eq!(cell_to_json(&Cell::F64(f64::NAN)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::INFINITY)), Value::Null);
        assert_eq!(cell_to_json(&Cell::F64(f64::NEG_INFINITY)), Value::Null);
    }

    #[test]
    fn test_cell_numeric() {
        let n = PgNumeric::from_str("123.456").unwrap();
        let v = cell_to_json(&Cell::Numeric(n));
        assert_eq!(v, json!("123.456"));
    }

    #[test]
    fn test_cell_date() {
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let v = cell_to_json(&Cell::Date(d));
        assert_eq!(v, json!("2024-06-15"));
    }

    #[test]
    fn test_cell_time() {
        let t = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        let v = cell_to_json(&Cell::Time(t));
        assert_eq!(v, json!("14:30:00"));
    }

    #[test]
    fn test_cell_timestamp() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        );
        let v = cell_to_json(&Cell::Timestamp(dt));
        assert_eq!(v, json!("2024-01-01T12:00:00"));
    }

    #[test]
    fn test_cell_timestamptz() {
        let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let v = cell_to_json(&Cell::TimestampTz(dt));
        // RFC 3339 format
        assert_eq!(v, json!("2024-01-01T12:00:00+00:00"));
    }

    #[test]
    fn test_cell_uuid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let v = cell_to_json(&Cell::Uuid(u));
        assert_eq!(v, json!("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_cell_json() {
        let j = json!({"key": "value", "num": 42});
        let v = cell_to_json(&Cell::Json(j.clone()));
        assert_eq!(v, j);
    }

    #[test]
    fn test_cell_bytes() {
        let v = cell_to_json(&Cell::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(v, json!([0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_cell_bytes_empty() {
        let v = cell_to_json(&Cell::Bytes(vec![]));
        assert_eq!(v, json!([]));
    }

    // -----------------------------------------------------------------------
    // array_cell_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_array_bool() {
        let arr = ArrayCell::Bool(vec![Some(true), Some(false), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([true, false, null]));
    }

    #[test]
    fn test_array_string() {
        let arr = ArrayCell::String(vec![Some("a".into()), None, Some("b".into())]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["a", null, "b"]));
    }

    #[test]
    fn test_array_i16() {
        let arr = ArrayCell::I16(vec![Some(1), Some(-2), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([1, -2, null]));
    }

    #[test]
    fn test_array_i32() {
        let arr = ArrayCell::I32(vec![Some(100), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([100, null]));
    }

    #[test]
    fn test_array_i64() {
        let arr = ArrayCell::I64(vec![Some(i64::MAX), None, Some(0)]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([i64::MAX, null, 0]));
    }

    #[test]
    fn test_array_f32_with_special() {
        let arr = ArrayCell::F32(vec![Some(1.5), None, Some(f32::NAN), Some(f32::INFINITY)]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([1.5f32, null, null, null]));
    }

    #[test]
    fn test_array_f64_with_special() {
        let arr = ArrayCell::F64(vec![Some(2.5), Some(f64::NEG_INFINITY), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([2.5f64, null, null]));
    }

    #[test]
    fn test_array_numeric() {
        let n = PgNumeric::from_str("99.99").unwrap();
        let arr = ArrayCell::Numeric(vec![Some(n), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["99.99", null]));
    }

    #[test]
    fn test_array_date() {
        let d = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
        let arr = ArrayCell::Date(vec![Some(d), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-12-25", null]));
    }

    #[test]
    fn test_array_time() {
        let t = NaiveTime::from_hms_opt(8, 30, 0).unwrap();
        let arr = ArrayCell::Time(vec![Some(t), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["08:30:00", null]));
    }

    #[test]
    fn test_array_timestamp() {
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        let arr = ArrayCell::Timestamp(vec![Some(dt), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-06-01T00:00:00", null]));
    }

    #[test]
    fn test_array_timestamptz() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let arr = ArrayCell::TimestampTz(vec![Some(dt), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["2024-06-01T00:00:00+00:00", null]));
    }

    #[test]
    fn test_array_uuid() {
        let u = uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let arr = ArrayCell::Uuid(vec![Some(u), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!(["12345678-1234-1234-1234-123456789abc", null]));
    }

    #[test]
    fn test_array_json() {
        let j = json!({"a": 1});
        let arr = ArrayCell::Json(vec![Some(j.clone()), None]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([{"a": 1}, null]));
    }

    #[test]
    fn test_array_bytes() {
        let arr = ArrayCell::Bytes(vec![Some(vec![0xca, 0xfe]), None, Some(vec![])]);
        let v = array_cell_to_json(&arr);
        assert_eq!(v, json!([[0xca, 0xfe], null, []]));
    }

    // -----------------------------------------------------------------------
    // row_to_json unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_to_json_basic() {
        let cells = vec![Cell::I32(1), Cell::String("hello".into()), Cell::Bool(true)];
        let cols = vec!["id".into(), "name".into(), "active".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"id": 1, "name": "hello", "active": true}));
    }

    #[test]
    fn test_row_to_json_with_null() {
        let cells = vec![Cell::I32(42), Cell::Null];
        let cols = vec!["id".into(), "value".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"id": 42, "value": null}));
    }

    #[test]
    fn test_row_to_json_more_cells_than_columns() {
        // Extra cells get auto-generated column names
        let cells = vec![Cell::I32(1), Cell::I32(2), Cell::I32(3)];
        let cols = vec!["a".into(), "b".into()];
        let v = row_to_json(&cells, &cols);
        assert_eq!(v, json!({"a": 1, "b": 2, "col_2": 3}));
    }

    #[test]
    fn test_row_to_json_all_types() {
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        let t = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
        let ts = NaiveDateTime::new(d, t);
        let tstz = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let n = PgNumeric::from_str("123.45").unwrap();

        let cells = vec![
            Cell::Bool(true),
            Cell::I16(16),
            Cell::I32(32),
            Cell::U32(4_000_000_000),
            Cell::I64(64),
            Cell::F32(1.5),
            Cell::F64(2.5),
            Cell::Numeric(n),
            Cell::String("text".into()),
            Cell::Date(d),
            Cell::Time(t),
            Cell::Timestamp(ts),
            Cell::TimestampTz(tstz),
            Cell::Uuid(u),
            Cell::Json(json!({"key": "val"})),
            Cell::Bytes(vec![0xab, 0xcd]),
            Cell::Null,
            Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), None])),
        ];
        let cols: Vec<String> = vec![
            "bool_col",
            "i16_col",
            "i32_col",
            "u32_col",
            "i64_col",
            "f32_col",
            "f64_col",
            "numeric_col",
            "text_col",
            "date_col",
            "time_col",
            "ts_col",
            "tstz_col",
            "uuid_col",
            "json_col",
            "bytes_col",
            "null_col",
            "arr_col",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let v = row_to_json(&cells, &cols);
        assert_eq!(v["bool_col"], json!(true));
        assert_eq!(v["i16_col"], json!(16));
        assert_eq!(v["i32_col"], json!(32));
        assert_eq!(v["u32_col"], json!(4_000_000_000u64));
        assert_eq!(v["i64_col"], json!(64));
        assert_eq!(v["f32_col"], json!(1.5f32));
        assert_eq!(v["f64_col"], json!(2.5f64));
        assert_eq!(v["numeric_col"], json!("123.45"));
        assert_eq!(v["text_col"], json!("text"));
        assert_eq!(v["date_col"], json!("2024-03-15"));
        assert_eq!(v["time_col"], json!("10:30:00"));
        assert_eq!(v["ts_col"], json!("2024-03-15T10:30:00"));
        assert_eq!(v["tstz_col"], json!("2024-03-15T10:30:00+00:00"));
        assert_eq!(v["uuid_col"], json!("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(v["json_col"], json!({"key": "val"}));
        assert_eq!(v["bytes_col"], json!([0xab, 0xcd]));
        assert_eq!(v["null_col"], Value::Null);
        assert_eq!(v["arr_col"], json!([1, 2, null]));
    }

    // -----------------------------------------------------------------------
    // stable_connection_identity unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_stable_identity_excludes_password() {
        let id1 = stable_connection_identity(
            "postgres://user:oldpass@localhost:5432/db",
            "pub",
            "public.tbl",
        );
        let id2 = stable_connection_identity(
            "postgres://user:newpass@localhost:5432/db",
            "pub",
            "public.tbl",
        );
        assert_eq!(
            id1, id2,
            "rotating the password should not change the stable identity"
        );
    }

    #[test]
    fn test_stable_identity_excludes_username() {
        let id1 =
            stable_connection_identity("postgres://alice@localhost:5432/db", "pub", "public.tbl");
        let id2 =
            stable_connection_identity("postgres://bob@localhost:5432/db", "pub", "public.tbl");
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_stable_identity_changes_with_host() {
        let id1 = stable_connection_identity("postgres://u:p@host1:5432/db", "pub", "public.tbl");
        let id2 = stable_connection_identity("postgres://u:p@host2:5432/db", "pub", "public.tbl");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_stable_identity_changes_with_publication() {
        let id1 = stable_connection_identity("postgres://u:p@host:5432/db", "pub1", "public.tbl");
        let id2 = stable_connection_identity("postgres://u:p@host:5432/db", "pub2", "public.tbl");
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_stable_identity_invalid_uri_falls_back() {
        // Parse failure falls back to using the raw URI — identity is still
        // deterministic, just less robust to URI format variations.
        let id = stable_connection_identity("not a valid uri", "pub", "tbl");
        assert!(id.contains("not a valid uri"));
    }

    // -----------------------------------------------------------------------
    // parse_pg_uri unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_pg_uri_basic() {
        let config = parse_pg_uri("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.username, "user");
        assert!(config.password.is_some());
        assert_eq!(config.name, "mydb");
    }

    #[test]
    fn test_parse_pg_uri_default_port() {
        let config = parse_pg_uri("postgres://user:pass@host.example.com/testdb").unwrap();
        assert_eq!(config.port, 5432);
        assert_eq!(config.host, "host.example.com");
    }

    #[test]
    fn test_parse_pg_uri_no_password() {
        let config = parse_pg_uri("postgres://user@localhost/mydb").unwrap();
        assert!(config.password.is_none());
    }

    #[test]
    fn test_parse_pg_uri_missing_username() {
        let result = parse_pg_uri("postgres://localhost/mydb");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pg_uri_missing_database() {
        let result = parse_pg_uri("postgres://user:pass@localhost");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_pg_uri_custom_port() {
        let config = parse_pg_uri("postgres://user:pass@db.host:15432/mydb").unwrap();
        assert_eq!(config.port, 15432);
    }

    #[test]
    fn test_parse_pg_uri_invalid_scheme() {
        let result = parse_pg_uri("not_a_uri");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // RelationInfo / target table matching tests (direct struct construction)
    // -----------------------------------------------------------------------

    #[test]
    fn test_relation_info_clone() {
        let info = RelationInfo {
            table_name: "orders".to_string(),
            schema_name: "public".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
        };
        let cloned = info.clone();
        assert_eq!(cloned.table_name, "orders");
        assert_eq!(cloned.schema_name, "public");
        assert_eq!(cloned.column_names.len(), 2);
    }

    /// Test the is_target_table logic extracted for direct verification.
    /// This mirrors FelderaDestination::is_target_table without needing to
    /// construct the full struct.
    fn target_table_matches(source_table: &str, schema_name: &str, table_name: &str) -> bool {
        let qualified = format!("{schema_name}.{table_name}");
        source_table == qualified
            || source_table == table_name
            || source_table == format!("\"{schema_name}\".\"{table_name}\"")
    }

    #[test]
    fn test_target_table_unqualified() {
        assert!(target_table_matches("orders", "public", "orders"));
        assert!(!target_table_matches("orders", "public", "users"));
    }

    #[test]
    fn test_target_table_qualified() {
        assert!(target_table_matches("public.orders", "public", "orders"));
        assert!(!target_table_matches("other.orders", "public", "orders"));
    }

    #[test]
    fn test_target_table_quoted() {
        assert!(target_table_matches(
            "\"public\".\"orders\"",
            "public",
            "orders"
        ));
        assert!(!target_table_matches(
            "\"other\".\"orders\"",
            "public",
            "orders"
        ));
    }

    #[test]
    fn test_target_table_different_schema() {
        assert!(!target_table_matches("myschema.orders", "public", "orders"));
        assert!(target_table_matches(
            "myschema.orders",
            "myschema",
            "orders"
        ));
    }

    #[test]
    fn test_relation_cache_lookup() {
        let cache: HashMap<u32, RelationInfo> = HashMap::from([(
            42,
            RelationInfo {
                table_name: "orders".to_string(),
                schema_name: "public".to_string(),
                column_names: vec!["id".to_string(), "amount".to_string()],
            },
        )]);
        // Simulate is_target_table_by_id
        let source_table = "public.orders";
        let info42 = cache.get(&42).unwrap();
        assert!(target_table_matches(
            source_table,
            &info42.schema_name,
            &info42.table_name,
        ));
        assert!(cache.get(&99).is_none());

        // Simulate get_column_names
        let cols = cache.get(&42).map(|info| info.column_names.clone());
        assert_eq!(cols, Some(vec!["id".to_string(), "amount".to_string()]));
        assert_eq!(cache.get(&99).map(|info| info.column_names.clone()), None);
    }

    // -----------------------------------------------------------------------
    // fire_completed unit tests
    // -----------------------------------------------------------------------

    /// Creates an `AsyncResult` sender and a future that resolves to `true` when
    /// the sender is fired with `Ok(())`.  Works in both sync and async contexts
    /// because the pending side is wrapped in a tokio oneshot that the caller
    /// can `.await`.
    fn make_async_sender() -> (
        WriteEventsResult<()>,
        impl std::future::Future<Output = bool>,
    ) {
        let (async_result, pending) = etl::destination::async_result::AsyncResult::<()>::new(());
        let fut = async move { pending.await.into_result().is_ok() };
        (async_result, fut)
    }

    /// Sync-only helper: spawns a background OS thread so `fire_completed`
    /// (which is sync) can be tested without a tokio runtime on the test thread.
    fn make_tracked_sender_sync() -> (WriteEventsResult<()>, std::sync::mpsc::Receiver<bool>) {
        let (flag_tx, flag_rx) = std::sync::mpsc::channel();
        let (async_result, pending) = etl::destination::async_result::AsyncResult::<()>::new(());
        std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async move {
                    let result = pending.await;
                    let _ = flag_tx.send(result.into_result().is_ok());
                });
        });
        (async_result, flag_rx)
    }

    #[test]
    fn test_fire_completed_fires_when_past_frontier() {
        let (sender, flag_rx) = make_tracked_sender_sync();
        let mut waiting: Vec<(u64, DeferredSenders)> = vec![(5, vec![sender])];

        // completed_steps=5 is NOT strictly greater than step_at_flush=5 — should not fire.
        fire_completed(&mut waiting, 5);
        assert_eq!(waiting.len(), 1, "should still be waiting at step=5");

        // completed_steps=6 is strictly greater — should fire.
        fire_completed(&mut waiting, 6);
        assert!(waiting.is_empty(), "entry should have been removed");

        let fired = flag_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert!(fired, "sender should have been fired with Ok(())");
    }

    #[test]
    fn test_fire_completed_does_not_fire_at_equal_frontier() {
        let (sender, _flag_rx) = make_tracked_sender_sync();
        let mut waiting: Vec<(u64, DeferredSenders)> = vec![(10, vec![sender])];
        fire_completed(&mut waiting, 10);
        assert_eq!(
            waiting.len(),
            1,
            "step_at_flush==completed_steps must not fire"
        );
    }

    #[test]
    fn test_fire_completed_fires_only_ready_entries() {
        let (s1, rx1) = make_tracked_sender_sync();
        let (s2, _rx2) = make_tracked_sender_sync();
        let mut waiting: Vec<(u64, DeferredSenders)> = vec![
            (3, vec![s1]), // fires at completed=4
            (7, vec![s2]), // stays waiting at completed=4
        ];
        fire_completed(&mut waiting, 4);
        assert_eq!(waiting.len(), 1, "only the step=7 entry should remain");
        assert_eq!(waiting[0].0, 7);
        let fired = rx1.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert!(fired);
    }

    #[test]
    fn test_fire_completed_empty_waiting() {
        let mut waiting: Vec<(u64, DeferredSenders)> = vec![];
        fire_completed(&mut waiting, 100);
        assert!(waiting.is_empty());
    }

    // -----------------------------------------------------------------------
    // completion_watcher_task unit tests
    // -----------------------------------------------------------------------

    fn make_completion(completed: u64) -> Completion {
        Completion {
            total_completed_steps: completed,
        }
    }

    const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

    #[tokio::test]
    async fn test_watcher_fast_mode_fires_on_completed_steps() {
        let (completion_tx, completion_rx) = tokio::sync::watch::channel(make_completion(0));
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Fast(completion_rx),
            pending_rx,
            "test-fast".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((2, vec![sender])).unwrap();

        // Advance completed_steps past step_at_flush=2.
        completion_tx.send(make_completion(3)).unwrap();

        let fired = tokio::time::timeout(TIMEOUT, fired_fut)
            .await
            .expect("timed out waiting for fast-mode fire");
        assert!(
            fired,
            "fast mode must fire when total_completed_steps > step_at_flush"
        );
    }

    #[tokio::test]
    async fn test_watcher_strict_mode_does_not_fire_on_completed_steps_only() {
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::watch::channel(0u64);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Strict(checkpoint_rx),
            pending_rx,
            "test-strict".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((2, vec![sender])).unwrap();

        // Only advance the checkpoint notifier to 0 (no change) — must not fire.
        let _ = checkpoint_tx.send(0);
        tokio::task::yield_now().await;

        // The future must NOT resolve — a very short timeout should expire.
        let did_fire = tokio::time::timeout(std::time::Duration::from_millis(50), fired_fut).await;
        assert!(
            did_fire.is_err(),
            "strict mode must NOT fire when checkpoint frontier has not advanced past step_at_flush"
        );
    }

    #[tokio::test]
    async fn test_watcher_strict_mode_fires_on_checkpointed_steps() {
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::watch::channel(0u64);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Strict(checkpoint_rx),
            pending_rx,
            "test-strict-fires".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((2, vec![sender])).unwrap();

        // Checkpoint step advances past step_at_flush=2 — must fire.
        checkpoint_tx.send(3).unwrap();

        let fired = tokio::time::timeout(TIMEOUT, fired_fut)
            .await
            .expect("timed out waiting for strict-mode fire");
        assert!(
            fired,
            "strict mode must fire when checkpointed step > step_at_flush"
        );
    }

    #[tokio::test]
    async fn test_watcher_fires_immediately_when_already_past_frontier() {
        // completed_steps already beyond step_at_flush at task start.
        let (_completion_tx, completion_rx) = tokio::sync::watch::channel(make_completion(10));
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Fast(completion_rx),
            pending_rx,
            "test-already-done".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((5, vec![sender])).unwrap();

        let fired = tokio::time::timeout(TIMEOUT, fired_fut)
            .await
            .expect("timed out waiting for immediate fire");
        assert!(
            fired,
            "must fire immediately when frontier already exceeds step_at_flush"
        );
    }

    #[tokio::test]
    async fn test_watcher_strict_fires_immediately_when_checkpointed_past_frontier() {
        let (_checkpoint_tx, checkpoint_rx) = tokio::sync::watch::channel(10u64);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Strict(checkpoint_rx),
            pending_rx,
            "test-strict-already-done".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((5, vec![sender])).unwrap();

        let fired = tokio::time::timeout(TIMEOUT, fired_fut)
            .await
            .expect("timed out waiting for immediate strict-mode fire");
        assert!(
            fired,
            "strict mode must fire immediately when checkpointed step already exceeds step_at_flush"
        );
    }

    /// Regression: at Queue time the checkpointed step (2) lags the flush
    /// frontier (5).  A partial checkpoint advancing to 4 does not cover the
    /// step containing the new data, so strict mode must NOT fire.
    #[tokio::test]
    async fn test_watcher_strict_does_not_fire_on_partial_checkpoint() {
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::watch::channel(2u64);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Strict(checkpoint_rx),
            pending_rx,
            "test-strict-partial".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        // step_at_flush = 5.
        pending_tx.send((5, vec![sender])).unwrap();

        // Partial checkpoint advances to 4 — still below step_at_flush=5.
        checkpoint_tx.send(4).unwrap();
        tokio::task::yield_now().await;

        let did_fire = tokio::time::timeout(std::time::Duration::from_millis(50), fired_fut).await;
        assert!(
            did_fire.is_err(),
            "strict mode must NOT fire when checkpoint (4) does not exceed step_at_flush (5)"
        );
    }

    /// Regression: after a partial checkpoint stalls, a full checkpoint that
    /// passes step_at_flush must fire.
    #[tokio::test]
    async fn test_watcher_strict_fires_when_checkpoint_passes_step_at_flush() {
        let (checkpoint_tx, checkpoint_rx) = tokio::sync::watch::channel(2u64);
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Strict(checkpoint_rx),
            pending_rx,
            "test-strict-passes".to_string(),
        ));

        let (sender, fired_fut) = make_async_sender();
        pending_tx.send((5, vec![sender])).unwrap();

        // Full checkpoint advances past step_at_flush=5 — must fire.
        checkpoint_tx.send(6).unwrap();

        let fired = tokio::time::timeout(TIMEOUT, fired_fut)
            .await
            .expect("timed out waiting for strict fire after lagging checkpoint");
        assert!(
            fired,
            "strict mode must fire when checkpointed step (6) > step_at_flush (5)"
        );
    }

    #[tokio::test]
    async fn test_watcher_multiple_batches_different_thresholds() {
        let (completion_tx, completion_rx) = tokio::sync::watch::channel(make_completion(0));
        let (pending_tx, pending_rx) = mpsc::unbounded_channel::<(u64, DeferredSenders)>();

        tokio::spawn(completion_watcher_task(
            WatcherReceiver::Fast(completion_rx),
            pending_rx,
            "test-multi".to_string(),
        ));

        let (s1, f1) = make_async_sender(); // fires at completed > 1
        let (s2, f2) = make_async_sender(); // fires at completed > 4
        let (s3, f3) = make_async_sender(); // fires at completed > 7

        pending_tx.send((1, vec![s1])).unwrap();
        pending_tx.send((4, vec![s2])).unwrap();
        pending_tx.send((7, vec![s3])).unwrap();

        // Advance to 2 — only s1 fires.
        completion_tx.send(make_completion(2)).unwrap();
        assert!(
            tokio::time::timeout(TIMEOUT, f1)
                .await
                .expect("s1 timed out")
        );

        // Advance to 5 — s2 fires.
        completion_tx.send(make_completion(5)).unwrap();
        assert!(
            tokio::time::timeout(TIMEOUT, f2)
                .await
                .expect("s2 timed out")
        );

        // Advance to 8 — s3 fires.
        completion_tx.send(make_completion(8)).unwrap();
        assert!(
            tokio::time::timeout(TIMEOUT, f3)
                .await
                .expect("s3 timed out")
        );
    }
}
