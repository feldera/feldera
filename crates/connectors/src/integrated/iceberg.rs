//! Iceberg output connector: one snapshot of CDC rows per output batch.

use crate::catalog::{SerBatchReader, SerKeyRange};
use crate::controller::{ControllerInner, EndpointId};
use crate::integrated::change_log::{ChangeLog, FillError};
use crate::transport::Step;
use crate::{
    ControllerError, Encoder, OutputConsumer, OutputEndpoint, OutputEndpointCallbacks, RecordFormat,
};
use anyhow::{Result as AnyResult, anyhow};
use chrono::Utc;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_iceberg::{
    DataFile, IcebergSink, Layout, PendingCommit, SinkError, iceberg_output_serde_config,
};
use feldera_storage::tokio::TOKIO;
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::program_schema::Relation;
use feldera_types::transport::iceberg::IcebergWriterConfig;
use std::cmp::min;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

/// Rows that each thread encodes at a time.
const CHUNK_SIZE: usize = 100_000;

/// Open retries, and time, when `max_retries` is unset. Bounded: connector health is not
/// visible while the pipeline initializes.
const DEFAULT_OPEN_RETRIES: u32 = 10;
const DEFAULT_OPEN_TIME: Duration = Duration::from_secs(5 * 60);

const MAX_BACKOFF: Duration = Duration::from_secs(10);

struct Inner {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: IcebergWriterConfig,
    sink: IcebergSink,
    key_schema: Option<Relation>,
    value_schema: Relation,
    is_index: bool,
    controller: Weak<ControllerInner>,
    /// Progress metric: rows written in the current batch.
    records_written: Arc<AtomicU64>,
    /// Cancels writes, commits, and retry waits.
    shutdown: CancellationToken,
    /// Uniqueness violations reported, for tests.
    #[cfg(any(test, feature = "testing"))]
    violations: AtomicU64,
}

pub struct IcebergOutputEndpoint {
    inner: Arc<Inner>,
    threads: usize,
}

impl IcebergOutputEndpoint {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &IcebergWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        shutdown: CancellationToken,
        continue_previous_state: bool,
        is_index: bool,
        output_buffered: bool,
    ) -> Result<Self, ControllerError> {
        Self::check_config(endpoint_name, config, is_index)?;
        if !output_buffered {
            warn!(
                "iceberg {endpoint_name}: without 'enable_output_buffer', each step with output \
                 adds a snapshot, and a table with many snapshots is slow to read and to commit \
                 to. See https://docs.feldera.com/connectors#configuring-the-output-buffer"
            );
        }
        // iceberg-rust never renews `rest.token`, even with `rest.credential`.
        if config.rest_catalog_config.token.is_some() {
            warn!(
                "iceberg {endpoint_name}: 'rest.token' is never renewed, so the connector stops \
                 writing when it expires. Use 'rest.credential' without 'rest.token' instead"
            );
        }

        // Panic safety: block_on() panics if called from a tokio async context.
        // new() is called from sync controller code (connect_output), so this is fine.
        let sink = TOKIO
            .block_on(open(
                endpoint_name,
                config,
                value_schema,
                continue_previous_state,
                &shutdown,
            ))
            .map_err(|e| {
                ControllerError::output_transport_error(
                    endpoint_name,
                    true,
                    anyhow!(
                        "error creating or opening Iceberg table '{}': {e:#}",
                        config.table_name
                    ),
                )
            })?;

        Ok(Self::with_sink(
            endpoint_id,
            endpoint_name,
            config,
            key_schema,
            value_schema,
            controller,
            shutdown,
            is_index,
            sink,
        ))
    }

    fn check_config(
        endpoint_name: &str,
        config: &IcebergWriterConfig,
        is_index: bool,
    ) -> Result<(), ControllerError> {
        config
            .validate()
            .map_err(|e| ControllerError::invalid_transport_configuration(endpoint_name, &e))?;
        if config.threads.unwrap_or(1) > 1 && !is_index {
            return Err(ControllerError::invalid_transport_configuration(
                endpoint_name,
                "Parallel writes (threads > 1) require the view to have a unique key to \
                 ensure correct ordering of inserts and deletes. Please specify the `index` \
                 property in the connector configuration. For more details, see: \
                 https://docs.feldera.com/connectors/unique_keys",
            ));
        }
        Ok(())
    }

    /// Endpoint over an open `sink`.
    #[allow(clippy::too_many_arguments)]
    fn with_sink(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &IcebergWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        shutdown: CancellationToken,
        is_index: bool,
        sink: IcebergSink,
    ) -> Self {
        let inner = Arc::new(Inner {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            sink,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            is_index,
            controller,
            records_written: Arc::new(AtomicU64::new(0)),
            shutdown,
            #[cfg(any(test, feature = "testing"))]
            violations: AtomicU64::new(0),
        });
        // add_output() has already been called, so the metrics slot exists.
        if let Some(controller) = inner.controller.upgrade() {
            controller
                .status
                .register_batch_progress_counter(&inner.endpoint_id, inner.records_written.clone());
        }
        Self {
            threads: config.threads.unwrap_or(1),
            inner,
        }
    }
}

/// Opens or creates the table, retrying transient errors.
async fn open(
    endpoint_name: &str,
    config: &IcebergWriterConfig,
    relation: &Relation,
    resume: bool,
    shutdown: &CancellationToken,
) -> AnyResult<IcebergSink> {
    let max_retries = config.max_retries.unwrap_or(DEFAULT_OPEN_RETRIES);
    let mut retries = 0;
    let mut last_error = None;
    // One id for all attempts, so that an attempt finds a table that an earlier one created.
    let create_id = uuid::Uuid::new_v4().to_string();
    let attempts = async {
        let mut backoff = Duration::from_secs(1);
        loop {
            match IcebergSink::open(config, relation, resume, &create_id).await {
                Ok(sink) => {
                    info!(
                        "iceberg {endpoint_name}: opened Iceberg table '{}'",
                        config.table_name
                    );
                    return Ok(sink);
                }
                Err(SinkError::Transient(e)) if retries < max_retries => {
                    retries += 1;
                    warn!(
                        "iceberg {endpoint_name}: error creating or opening Iceberg table '{}' \
                         (attempt {retries}, retrying in {backoff:?}): {e:#}",
                        config.table_name
                    );
                    last_error = Some(e);
                    sleep(backoff).await;
                    backoff = min(backoff * 2, MAX_BACKOFF);
                }
                Err(e) => return Err(e.into_inner()),
            }
        }
    };
    // An attempt that the catalog never answers takes the full catalog request timeout.
    let bounded = async {
        match config.max_retries {
            Some(_) => Ok(attempts.await),
            None => timeout(DEFAULT_OPEN_TIME, attempts).await,
        }
    };
    match shutdown.run_until_cancelled(bounded).await {
        None => Err(anyhow!("the pipeline is shutting down")),
        Some(Ok(result)) => result,
        Some(Err(_)) => Err(anyhow!(
            "the table did not open in {DEFAULT_OPEN_TIME:?}: {:#}",
            last_error.unwrap_or_else(|| anyhow!("the catalog did not answer"))
        )),
    }
}

impl Inner {
    fn set_health(&self, health: ConnectorHealth) {
        if let Some(controller) = self.controller.upgrade() {
            controller.update_output_connector_health(self.endpoint_id, health);
        }
    }

    /// Retries transient errors, up to `max_retries`. The connector is unhealthy meanwhile.
    async fn retry<T>(
        &self,
        description: &str,
        mut operation: impl AsyncFnMut() -> Result<T, SinkError>,
    ) -> Result<T, SinkError> {
        let mut retries = 0;
        let mut backoff = Duration::from_secs(1);
        loop {
            match operation().await {
                Ok(result) => {
                    self.set_health(ConnectorHealth::healthy());
                    if retries > 0 {
                        info!(
                            "iceberg {}: {description} succeeded after {retries} retries",
                            self.endpoint_name
                        );
                    }
                    return Ok(result);
                }
                Err(SinkError::Transient(e))
                    if self.config.max_retries.is_none_or(|max| retries < max) =>
                {
                    retries += 1;
                    let message = format!(
                        "{description} failed (attempt {retries}, retrying in {backoff:?}): {e:#}"
                    );
                    self.set_health(ConnectorHealth::unhealthy(&message));
                    warn!("iceberg {}: {message}", self.endpoint_name);
                    sleep(backoff).await;
                    backoff = min(backoff * 2, MAX_BACKOFF);
                }
                Err(SinkError::Transient(e)) => {
                    return Err(SinkError::Transient(anyhow!(
                        "{description} failed after {retries} retries: {e:#}"
                    )));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Writes and commits `batch`. The caller owns `tasks`, to stop them on shutdown.
    async fn write_and_commit(
        self: &Arc<Self>,
        tasks: &mut RangeTasks,
        batch: Arc<dyn SerBatchReader>,
        threads: usize,
        micros: i64,
    ) -> AnyResult<()> {
        let mut earlier: Option<PendingCommit> = None;
        let mut rewrites = 0;
        // Report uniqueness violations until a write of the batch completes. A write that
        // failed part way can report one twice, but none is lost.
        let mut reported = false;
        loop {
            let written = self
                .retry("writing Iceberg data files", async || {
                    let written = self.write(tasks, &batch, threads, micros, !reported).await;
                    reported |= written.is_ok();
                    written
                })
                .await
                .map_err(SinkError::into_inner)?;
            let Some((layout, files, num_rows)) = written else {
                return Ok(());
            };
            let mut pending = match earlier.take() {
                Some(earlier) => earlier.rewritten(&layout, files),
                None => self.sink.prepare(&layout, files),
            };
            let num_bytes = pending.num_bytes() as usize;

            let committed = self
                .retry("committing to the Iceberg table", async || {
                    self.sink.commit(&mut pending).await
                })
                .await;
            match committed {
                Ok(()) => {
                    if let Some(controller) = self.controller.upgrade() {
                        controller
                            .status
                            .output_buffer(self.endpoint_id, num_bytes, num_rows);
                    }
                    return Ok(());
                }
                // This attempt's files are orphans. A rewrite counts as a retry.
                Err(SinkError::TableChanged(e))
                    if self.config.max_retries.is_none_or(|max| rewrites < max) =>
                {
                    rewrites += 1;
                    warn!(
                        "iceberg {}: {e:#}. Writing the batch again.",
                        self.endpoint_name
                    );
                    earlier = Some(pending);
                }
                // Out of retries. An earlier attempt can still land.
                Err(e) => {
                    if let Some(controller) = self.controller.upgrade() {
                        controller.output_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            false,
                            e.into_inner().context("gave up the batch"),
                            Some("iceberg_commit"),
                        );
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Writes `batch` for the table as it is now. `None` if the batch has no rows.
    async fn write(
        self: &Arc<Self>,
        tasks: &mut RangeTasks,
        batch: &Arc<dyn SerBatchReader>,
        threads: usize,
        micros: i64,
        report: bool,
    ) -> Result<Option<(Arc<Layout>, Vec<DataFile>, usize)>, SinkError> {
        // A retry writes every row again.
        self.records_written.store(0, Ordering::Relaxed);
        let ranges = batch
            .clone()
            .split(
                threads,
                RecordFormat::Parquet(iceberg_output_serde_config()),
            )
            .map_err(SinkError::Permanent)?;
        if ranges.is_empty() {
            return Ok(None);
        }

        // The table's owner can make the table fit the view.
        let layout = self.sink.begin().await.map_err(|e| match e {
            SinkError::Unfit(e) => SinkError::Transient(e),
            e => e,
        })?;

        for range in ranges {
            let task = write_range(range, self.clone(), layout.clone(), micros, report);
            tasks.spawn(task.in_current_span());
        }
        let mut files = Vec::new();
        let mut num_rows = 0;
        while let Some(result) = tasks.join_next().await {
            let result = result
                .unwrap_or_else(|e| Err(SinkError::Permanent(anyhow!("write task panicked: {e}"))));
            match result {
                Ok((mut range_files, range_rows)) => {
                    files.append(&mut range_files);
                    num_rows += range_rows;
                }
                Err(e) => {
                    // Stop the other ranges before the retry. Their files are orphans.
                    tasks.shutdown().await;
                    return Err(e);
                }
            }
        }
        if files.is_empty() {
            return Ok(None);
        }
        Ok(Some((layout, files, num_rows)))
    }
}

/// Tasks that write the key ranges of one batch.
type RangeTasks = JoinSet<Result<(Vec<DataFile>, usize), SinkError>>;

/// Writes one key range. `report` reports uniqueness violations.
async fn write_range(
    range: Box<dyn SerKeyRange>,
    inner: Arc<Inner>,
    layout: Arc<Layout>,
    micros: i64,
    report: bool,
) -> Result<(Vec<DataFile>, usize), SinkError> {
    let mut writer = layout.writer().await?;
    let cursor = range.cursor().map_err(SinkError::Transient)?;
    let mut rows = cursor
        .arrow_sink(layout.row_schema())
        .map_err(SinkError::Permanent)?;
    let mut change_log = ChangeLog::new(
        cursor,
        &inner.value_schema.name,
        inner.key_schema.as_ref(),
        inner.is_index,
    );
    let mut report_violation = |e| {
        if !report {
            return;
        }
        #[cfg(any(test, feature = "testing"))]
        inner.violations.fetch_add(1, Ordering::Relaxed);
        if let Some(controller) = inner.controller.upgrade() {
            controller.output_transport_error(
                inner.endpoint_id,
                &inner.endpoint_name,
                false,
                e,
                Some("iceberg_uniqueness_violation"),
            );
        }
    };
    let mut ops = Vec::new();
    let mut num_rows = 0;

    loop {
        let more = change_log
            .fill(rows.as_mut(), &mut ops, CHUNK_SIZE, &mut report_violation)
            .map_err(|e| match e {
                FillError::Rows(e) => SinkError::Permanent(e),
                FillError::Read(e) => SinkError::Transient(e),
            })?;
        if !ops.is_empty() {
            let batch = rows
                .finish()
                .map_err(|e| SinkError::Permanent(anyhow!("error generating arrow arrays: {e}")))?;
            writer.write(&batch, &ops, micros).await?;
            num_rows += ops.len();
            inner
                .records_written
                .fetch_add(ops.len() as u64, Ordering::Relaxed);
            ops.clear();
        }
        if !more {
            break;
        }
    }
    Ok((writer.close().await?, num_rows))
}

impl Encoder for IcebergOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    /// Commits here, not in `batch_end`: a table change needs the batch to write it again.
    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let inner = &self.inner;
        let span = info_span!(
            "iceberg_output",
            endpoint = &*inner.endpoint_name,
            table = &*inner.config.table_name,
        );
        let micros = Utc::now().timestamp_micros();
        let mut tasks = RangeTasks::new();
        // Panic safety: block_on() panics if called from a tokio async context.
        // encode() is called from the dedicated output thread (output_thread_func).
        TOKIO
            .block_on(async {
                let work = inner
                    .write_and_commit(&mut tasks, batch, self.threads, micros)
                    .instrument(span);
                let result = inner.shutdown.run_until_cancelled(work).await;
                // A shutdown can leave range tasks running.
                tasks.shutdown().await;
                result
            })
            .unwrap_or_else(|| {
                Err(anyhow!(
                    "writing to the Iceberg table was aborted: the pipeline is shutting down"
                ))
            })
    }
}

impl OutputConsumer for IcebergOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.inner.records_written.store(0, Ordering::Relaxed);
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        // As in Delta, the progress count lasts until the batch ends.
        self.inner.records_written.store(0, Ordering::Relaxed);
    }
}

impl OutputEndpoint for IcebergOutputEndpoint {
    fn connect(&mut self, _callbacks: Arc<dyn OutputEndpointCallbacks>) -> AnyResult<()> {
        unreachable!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        // A restart writes the steps since the checkpoint again.
        false
    }
}

/// Tests on a memory catalog over a temporary directory.
#[cfg(any(test, feature = "testing"))]
pub mod tests {
    use super::*;
    use crate::test::{engine_test, published_schema, relations, rows};
    use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, UInt32Array};
    use arrow::compute::{cast, concat_batches, sort_to_indices, take_record_batch};
    use arrow::datatypes::Int64Type;
    use feldera_adapterlib::test::{ConnectorTestEngine, Updates};
    use feldera_iceberg::test_support::{
        ErrorKind, Failure, Faults, FlakyCatalog, PrimitiveType, TestWarehouse,
    };
    use feldera_iceberg::{OP_COLUMN, TS_COLUMN};
    use feldera_types::program_schema::{ColumnType, Field};
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::time::Instant;

    /// View with a column of every SQL type.
    pub fn value_relation() -> Relation {
        relations::every_type("iceberg_output")
    }

    /// The index of [`value_relation`] on `bigint`.
    pub fn key_relation() -> Relation {
        key_relation_of(&value_relation())
    }

    /// Index of `relation` on `bigint`, named after it: the fixture finds indexes by name.
    pub fn key_relation_of(relation: &Relation) -> Relation {
        Relation::new(
            format!("{}_key", relation.name.name()).into(),
            vec![Field::new("bigint".into(), ColumnType::bigint(false))],
            false,
            BTreeMap::new(),
        )
    }

    /// A view with no `timestamptz` column, which the Glue client can write.
    pub fn basic_relation() -> Relation {
        relations::basic("iceberg_output_basic")
    }

    /// [`value_relation`] with every column but the key `bigint` nullable.
    pub fn nullable_value_relation() -> Relation {
        let mut relation = relations::every_type("iceberg_output_nullable");
        for field in &mut relation.fields {
            field.columntype.nullable = field.name.name() != "bigint";
        }
        relation
    }

    /// `n` rows of [`value_relation`].
    fn sample_rows(n: usize) -> RecordBatch {
        rows_of(&value_relation(), n)
    }

    /// Returns `n` rows of `relation`, with `bigint` from 0 up.
    fn rows_of(relation: &Relation, n: usize) -> RecordBatch {
        let rows = proptest::collection::vec(rows::row(relation), n)
            .new_tree(&mut TestRunner::deterministic())
            .unwrap()
            .current();
        let rows: Vec<String> = rows
            .into_iter()
            .enumerate()
            .map(|(bigint, mut row)| {
                row.insert("bigint".to_string(), json!(bigint));
                serde_json::Value::Object(row).to_string()
            })
            .collect();
        let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
        Updates::from_json(published_schema(relation), &rows)
            .unwrap()
            .rows()
            .clone()
    }

    /// Endpoint of table `t` in `warehouse`, with `faults` injected into its catalog.
    fn endpoint_with(
        warehouse: &TestWarehouse,
        threads: usize,
        indexed: bool,
        max_retries: Option<u32>,
        faults: Faults,
        shutdown: CancellationToken,
    ) -> (IcebergOutputEndpoint, Arc<FlakyCatalog>) {
        let relation = value_relation();
        endpoint_for(
            &relation,
            warehouse,
            threads,
            indexed,
            max_retries,
            faults,
            shutdown,
        )
    }

    /// [`endpoint_with`] for a view `relation` with the key `bigint`.
    fn endpoint_for(
        relation: &Relation,
        warehouse: &TestWarehouse,
        threads: usize,
        indexed: bool,
        max_retries: Option<u32>,
        faults: Faults,
        shutdown: CancellationToken,
    ) -> (IcebergOutputEndpoint, Arc<FlakyCatalog>) {
        let (sink, catalog) = TOKIO.block_on(warehouse.flaky_sink("t", relation, faults));
        let endpoint = IcebergOutputEndpoint::with_sink(
            EndpointId::default(),
            "test_endpoint",
            &config(threads, max_retries),
            &indexed.then(|| key_relation_of(relation)),
            relation,
            Weak::new(),
            shutdown,
            indexed,
            sink,
        );
        (endpoint, catalog)
    }

    fn config(threads: usize, max_retries: Option<u32>) -> IcebergWriterConfig {
        serde_json::from_value(json!({
            "catalog_type": "rest",
            "rest.uri": "http://localhost:1",
            "table_name": "ns.t",
            "threads": threads,
            "max_retries": max_retries,
        }))
        .unwrap()
    }

    fn endpoint(warehouse: &TestWarehouse, threads: usize, indexed: bool) -> IcebergOutputEndpoint {
        endpoint_with(
            warehouse,
            threads,
            indexed,
            Some(0),
            Faults::default(),
            CancellationToken::new(),
        )
        .0
    }

    fn encode(
        endpoint: &mut IcebergOutputEndpoint,
        batch: Arc<dyn SerBatchReader>,
    ) -> AnyResult<()> {
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        let result = endpoint.encode(batch);
        endpoint.consumer().batch_end();
        result
    }

    fn indexed_batch(
        engine: &dyn ConnectorTestEngine,
        rows: &RecordBatch,
        weights: &[i64],
    ) -> Arc<dyn SerBatchReader> {
        engine
            .indexed_output_batch(&key_relation(), &value_relation(), rows, weights)
            .unwrap()
    }

    fn inserts(engine: &dyn ConnectorTestEngine, rows: &RecordBatch) -> Arc<dyn SerBatchReader> {
        indexed_batch(engine, rows, &vec![1; rows.num_rows()])
    }

    /// The rows of table `t`.
    fn table(warehouse: &TestWarehouse) -> Option<RecordBatch> {
        let batches = TOKIO.block_on(warehouse.read("t"));
        let schema = batches.first()?.schema();
        Some(concat_batches(&schema, &batches).unwrap())
    }

    /// The `bigint`, the operation, and the `string` of each row of table `t`, sorted.
    fn changes(warehouse: &TestWarehouse) -> Vec<(i64, String, String)> {
        let Some(table) = table(warehouse) else {
            return Vec::new();
        };
        let column = |name: &str| table.column_by_name(name).unwrap().clone();
        let (bigint, op, string) = (column("bigint"), column(OP_COLUMN), column("string"));
        let mut changes: Vec<_> = (0..table.num_rows())
            .map(|row| {
                (
                    bigint.as_primitive::<Int64Type>().value(row),
                    op.as_string::<i32>().value(row).to_string(),
                    string.as_string::<i32>().value(row).to_string(),
                )
            })
            .collect();
        changes.sort();
        changes
    }

    /// What [`changes`] returns for `rows`, each with operation `op`.
    fn expected(rows: &RecordBatch, op: &str) -> Vec<(i64, String, String)> {
        let column = |name: &str| rows.column_by_name(name).unwrap().clone();
        let (bigint, string) = (column("bigint"), column("string"));
        (0..rows.num_rows())
            .map(|row| {
                (
                    bigint.as_primitive::<Int64Type>().value(row),
                    op.to_string(),
                    string.as_string::<i32>().value(row).to_string(),
                )
            })
            .collect()
    }

    fn snapshots(warehouse: &TestWarehouse) -> usize {
        TOKIO.block_on(warehouse.snapshots("t"))
    }

    fn inserts_arrive(engine: &dyn ConnectorTestEngine, threads: usize) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let mut endpoint = endpoint(&warehouse, threads, true);
        let rows = sample_rows(1000);

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();

        assert_eq!(changes(&warehouse), expected(&rows, "i"));
        // All ranges commit together in one snapshot.
        assert_eq!(snapshots(&warehouse), 1);
    }

    #[engine_test]
    pub fn inserts_arrive_single_thread(engine: &dyn ConnectorTestEngine) {
        inserts_arrive(engine, 1);
    }

    #[engine_test]
    pub fn inserts_arrive_multi_thread(engine: &dyn ConnectorTestEngine) {
        inserts_arrive(engine, 4);
    }

    /// Each value reads back unchanged, in the type of the table.
    #[engine_test]
    pub fn every_type_arrives(engine: &dyn ConnectorTestEngine) {
        every_type_arrives_for(engine, &value_relation(), 5);
    }

    /// Each nullable type, with NULL in about half of the rows.
    #[engine_test]
    pub fn every_nullable_type_arrives(engine: &dyn ConnectorTestEngine) {
        every_type_arrives_for(engine, &nullable_value_relation(), 20);
    }

    fn every_type_arrives_for(engine: &dyn ConnectorTestEngine, relation: &Relation, n: usize) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let shutdown = CancellationToken::new();
        let faults = Faults::default();
        let (mut endpoint, _) =
            endpoint_for(relation, &warehouse, 1, true, Some(0), faults, shutdown);
        let rows = rows_of(relation, n);
        for field in relation
            .fields
            .iter()
            .filter(|f| f.columntype.nullable && !f.unused)
        {
            let name = field.name.name();
            let column = rows.column_by_name(&name).unwrap();
            assert!(column.null_count() > 0, "no NULL in column '{name}'");
        }
        let batch = engine
            .indexed_output_batch(&key_relation_of(relation), relation, &rows, &vec![1; n])
            .unwrap();

        encode(&mut endpoint, batch).unwrap();

        // Sort by `bigint` to match the order of `rows`.
        let table = table(&warehouse).unwrap();
        let order = sort_to_indices(table.column_by_name("bigint").unwrap(), None, None).unwrap();
        let table = take_record_batch(&table, &order).unwrap();
        for field in rows.schema().fields() {
            let name = field.name();
            let read = table.column_by_name(name).unwrap();
            let wrote = rows.column_by_name(name).unwrap();
            for row in 0..rows.num_rows() {
                if wrote.is_null(row) {
                    assert!(read.is_null(row), "column '{name}', row {row}");
                    continue;
                }
                match name.as_str() {
                    // UUID is text in the view and 16 bytes in the table.
                    "uuid" => {
                        let uuid = uuid::Uuid::parse_str(wrote.as_string::<i32>().value(row));
                        assert_eq!(
                            read.as_fixed_size_binary().value(row),
                            uuid.unwrap().as_bytes()
                        );
                    }
                    // VARIANT is JSON text in the table.
                    "variant" => {
                        let json = |array: &ArrayRef| {
                            let text = array.as_string::<i32>().value(row);
                            serde_json::from_str::<serde_json::Value>(text).unwrap()
                        };
                        assert_eq!(json(read), json(wrote));
                    }
                    _ => {}
                }
            }
            if !["uuid", "variant"].contains(&name.as_str()) {
                // Compare in the Arrow type that the engine publishes.
                let read = cast(read, wrote.data_type())
                    .unwrap_or_else(|e| panic!("column '{name}': {e}"));
                assert_eq!(&read, wrote, "column '{name}'");
            }
        }
        assert!(table.column_by_name(TS_COLUMN).unwrap().null_count() == 0);
    }

    /// With an index, an update is one `u` row with the new values, a delete one `d` row.
    #[engine_test]
    pub fn updates_and_deletes_are_rows_of_the_change_log(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let mut endpoint = endpoint(&warehouse, 1, true);
        // Row 4 is a new version of row 1.
        let rows = sample_rows(5);
        let bigint = rows.schema().index_of("bigint").unwrap();
        let mut columns = rows.columns().to_vec();
        columns[bigint] = Arc::new(Int64Array::from(vec![0, 1, 2, 3, 1]));
        let rows = RecordBatch::try_new(rows.schema(), columns).unwrap();
        let first = rows.slice(0, 4);
        let pick =
            |indices: Vec<u32>| take_record_batch(&rows, &UInt32Array::from(indices)).unwrap();

        encode(&mut endpoint, inserts(engine, &first)).unwrap();
        let second = indexed_batch(engine, &pick(vec![1, 4, 2]), &[-1, 1, -1]);
        encode(&mut endpoint, second).unwrap();
        // Update back: the engine orders a key's versions by value, so both orders occur.
        let third = indexed_batch(engine, &pick(vec![4, 1]), &[-1, 1]);
        encode(&mut endpoint, third).unwrap();

        let mut all = expected(&first, "i");
        all.extend(expected(&pick(vec![4]), "u"));
        all.extend(expected(&pick(vec![2]), "d"));
        all.extend(expected(&pick(vec![1]), "u"));
        all.sort();
        assert_eq!(changes(&warehouse), all);
        assert_eq!(snapshots(&warehouse), 3);
    }

    /// Updates and deletes that several threads write give the same change log as one thread.
    #[engine_test]
    pub fn updates_and_deletes_arrive_multi_thread(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let mut endpoint = endpoint(&warehouse, 4, true);
        let old = sample_rows(1000);
        encode(&mut endpoint, inserts(engine, &old)).unwrap();
        // New versions of the odd keys, taken from other rows.
        let newer = sample_rows(2000).slice(1000, 1000);
        let bigint = newer.schema().index_of("bigint").unwrap();
        let mut columns = newer.columns().to_vec();
        columns[bigint] = Arc::new(Int64Array::from_iter_values(0..1000));
        let newer = RecordBatch::try_new(newer.schema(), columns).unwrap();
        let odd = take_record_batch(&newer, &UInt32Array::from_iter_values((1..1000).step_by(2)));
        let odd = odd.unwrap();
        let even = take_record_batch(&old, &UInt32Array::from_iter_values((0..1000).step_by(2)));
        let files = || TOKIO.block_on(warehouse.data_files("t")).1;
        let files_before = files();

        // Delete the even keys, update the odd keys.
        let rows = concat_batches(&old.schema(), [&old, &odd]).unwrap();
        let mut weights = vec![-1; 1000];
        weights.resize(1500, 1);
        encode(&mut endpoint, indexed_batch(engine, &rows, &weights)).unwrap();

        let mut all = expected(&old, "i");
        all.extend(expected(&even.unwrap(), "d"));
        all.extend(expected(&odd, "u"));
        all.sort();
        assert_eq!(changes(&warehouse), all);
        // More than one key range wrote the batch.
        let written = files() - files_before;
        assert!(written > 1, "{written} files");
    }

    /// Without an index, a row repeats once per unit of weight.
    #[engine_test]
    pub fn rows_without_an_index_follow_their_weights(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let mut endpoint = endpoint(&warehouse, 1, false);
        let rows = sample_rows(2);

        let batch = engine
            .output_batch(&value_relation(), &rows, &[2, -1])
            .unwrap();
        encode(&mut endpoint, batch).unwrap();

        let mut all = expected(&rows.slice(0, 1), "i");
        all.extend(expected(&rows.slice(0, 1), "i"));
        all.extend(expected(&rows.slice(1, 1), "d"));
        assert_eq!(changes(&warehouse), all);
    }

    #[engine_test]
    pub fn an_empty_batch_adds_no_snapshot(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let mut endpoint = endpoint(&warehouse, 1, true);

        encode(&mut endpoint, inserts(engine, &sample_rows(0))).unwrap();

        assert_eq!(snapshots(&warehouse), 0);
    }

    #[engine_test]
    pub fn a_commit_that_fails_is_tried_again(engine: &dyn ConnectorTestEngine) {
        for threads in [1, 4] {
            let warehouse = TOKIO.block_on(TestWarehouse::new());
            let faults = Faults {
                commits: [Failure::Lost].into(),
                ..Faults::default()
            };
            let shutdown = CancellationToken::new();
            let (mut endpoint, _) =
                endpoint_with(&warehouse, threads, true, None, faults, shutdown);
            let rows = sample_rows(100);

            encode(&mut endpoint, inserts(engine, &rows)).unwrap();

            assert_eq!(
                changes(&warehouse),
                expected(&rows, "i"),
                "{threads} threads"
            );
            assert_eq!(snapshots(&warehouse), 1, "{threads} threads");
        }
    }

    /// The commit lands, and its response is lost. The retry finds the snapshot.
    #[engine_test]
    pub fn a_commit_whose_answer_is_lost_is_not_repeated(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let faults = Faults {
            commits: [Failure::ResponseLost].into(),
            ..Faults::default()
        };
        let (mut endpoint, _) =
            endpoint_with(&warehouse, 1, true, None, faults, CancellationToken::new());
        let rows = sample_rows(10);

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();

        assert_eq!(changes(&warehouse), expected(&rows, "i"));
        assert_eq!(snapshots(&warehouse), 1);
    }

    /// After `max_retries`, the batch is dropped, and the next batch commits.
    #[engine_test]
    pub fn a_batch_is_given_up_when_the_retries_run_out(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let faults = Faults {
            commits: [Failure::Lost, Failure::Lost].into(),
            ..Faults::default()
        };
        let (mut endpoint, catalog) = endpoint_with(
            &warehouse,
            1,
            true,
            Some(1),
            faults,
            CancellationToken::new(),
        );
        let rows = sample_rows(4);

        encode(&mut endpoint, inserts(engine, &rows.slice(0, 2))).unwrap();
        assert_eq!(changes(&warehouse), []);
        assert!(catalog.faults.lock().unwrap().commits.is_empty());

        encode(&mut endpoint, inserts(engine, &rows.slice(2, 2))).unwrap();
        assert_eq!(changes(&warehouse), expected(&rows.slice(2, 2), "i"));
    }

    /// The schema changes between write and commit, so the batch is written again.
    #[engine_test]
    pub fn a_table_that_changes_under_a_batch_gets_the_batch_again(
        engine: &dyn ConnectorTestEngine,
    ) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let (mut endpoint, catalog) = endpoint_with(
            &warehouse,
            1,
            true,
            Some(1),
            Faults::default(),
            CancellationToken::new(),
        );
        let rows = sample_rows(10);
        // Load 1 is for the write, load 2 for the commit.
        catalog.faults.lock().unwrap().add_column_at_load = Some(2);

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();

        assert_eq!(changes(&warehouse), expected(&rows, "i"));
        let table = table(&warehouse).unwrap();
        assert_eq!(table.column_by_name("added").unwrap().null_count(), 10);
        // The files of the first write stay on disk as orphans.
        assert_eq!(TOKIO.block_on(warehouse.data_files("t")), (2, 1));
    }

    /// A failed write is retried, then dropped with an error, and nothing is committed.
    #[engine_test]
    pub fn a_write_that_fails_commits_nothing(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let (mut endpoint, _) = endpoint_with(
            &warehouse,
            4,
            true,
            Some(1),
            Faults::default(),
            CancellationToken::new(),
        );
        let rows = sample_rows(100);
        // A file in place of the data directory fails the write, even as root.
        let data = TOKIO.block_on(warehouse.data_dir("t"));
        std::fs::write(&data, b"").unwrap();
        let error = encode(&mut endpoint, inserts(engine, &rows)).unwrap_err();
        std::fs::remove_file(&data).unwrap();
        let error = format!("{error:#}");
        assert!(error.contains("failed after 1 retries"), "{error}");
        assert_eq!(snapshots(&warehouse), 0);

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();
        assert_eq!(changes(&warehouse), expected(&rows, "i"));
    }

    /// A late first commit, after a schema change made the batch be written again, stays the
    /// only commit of the batch.
    #[engine_test]
    pub fn a_late_commit_is_not_repeated_by_the_batch_written_again(
        engine: &dyn ConnectorTestEngine,
    ) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let (mut endpoint, catalog) = endpoint_with(
            &warehouse,
            1,
            true,
            None,
            Faults::default(),
            CancellationToken::new(),
        );
        let rows = sample_rows(10);
        {
            // Loads: 1 write, 2 first commit, 3 its retry.
            let mut faults = catalog.faults.lock().unwrap();
            faults.commits = [Failure::Late].into();
            faults.add_column_at_load = Some(3);
        }

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();

        assert_eq!(changes(&warehouse), expected(&rows, "i"));
    }

    /// A shutdown ends unlimited commit retries.
    #[engine_test]
    pub fn a_shutdown_ends_the_retries(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let faults = Faults {
            commits: vec![Failure::Lost; 1000].into(),
            ..Faults::default()
        };
        let shutdown = CancellationToken::new();
        let (mut endpoint, catalog) =
            endpoint_with(&warehouse, 1, true, None, faults, shutdown.clone());

        // Cancel during the backoff after the first failed commit.
        let canceller = std::thread::spawn(move || {
            let start = Instant::now();
            while catalog.faults.lock().unwrap().commits.len() == 1000
                && start.elapsed() < std::time::Duration::from_secs(30)
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            shutdown.cancel();
        });
        let error = encode(&mut endpoint, inserts(engine, &sample_rows(2))).unwrap_err();
        canceller.join().unwrap();

        assert!(error.to_string().contains("shutting down"), "{error}");
        assert_eq!(changes(&warehouse), []);
    }

    /// The progress count is the batch's rows until `batch_end`, also after a rewrite.
    #[engine_test]
    pub fn the_progress_count_is_the_rows_of_the_batch(engine: &dyn ConnectorTestEngine) {
        for threads in [1, 4] {
            let warehouse = TOKIO.block_on(TestWarehouse::new());
            let (mut endpoint, catalog) = endpoint_with(
                &warehouse,
                threads,
                true,
                Some(1),
                Faults::default(),
                CancellationToken::new(),
            );
            // Load 1 is for the write, load 2 for the commit.
            catalog.faults.lock().unwrap().add_column_at_load = Some(2);
            let written = |endpoint: &IcebergOutputEndpoint| {
                endpoint.inner.records_written.load(Ordering::Relaxed)
            };

            endpoint.consumer().batch_start(0, OutputBatchType::Delta);
            endpoint.encode(inserts(engine, &sample_rows(100))).unwrap();
            assert_eq!(written(&endpoint), 100, "{threads} threads");
            endpoint.consumer().batch_end();
            assert_eq!(written(&endpoint), 0, "{threads} threads");
        }
    }

    /// A key inserted twice is reported once. A first write that fails before it reaches the
    /// key does not lose the report, and a rewrite for a table change does not repeat it.
    #[engine_test]
    pub fn a_uniqueness_violation_is_reported_once_per_batch(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        // The first write fails to load the table. Of the loads that work, 1 is for the
        // write, 2 for the commit.
        let faults = Faults {
            loads: 1,
            add_column_at_load: Some(2),
            ..Faults::default()
        };
        let shutdown = CancellationToken::new();
        let (mut endpoint, _) = endpoint_with(&warehouse, 1, true, Some(1), faults, shutdown);
        let rows = sample_rows(2);

        encode(&mut endpoint, indexed_batch(engine, &rows, &[2, 1])).unwrap();

        assert_eq!(endpoint.inner.violations.load(Ordering::Relaxed), 1);
        assert_eq!(changes(&warehouse), expected(&rows.slice(1, 1), "i"));
    }

    /// A table that stops fitting the view fails the batch, and the next batch is written once
    /// the table fits again.
    #[engine_test]
    pub fn a_table_that_stops_fitting_the_view_is_written_once_it_fits(
        engine: &dyn ConnectorTestEngine,
    ) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let shutdown = CancellationToken::new();
        let faults = Faults::default();
        let (mut endpoint, _) = endpoint_with(&warehouse, 1, true, Some(1), faults, shutdown);
        let rows = sample_rows(10);
        TOKIO.block_on(warehouse.drop_column("t", "string"));

        let error = format!(
            "{:#}",
            encode(&mut endpoint, inserts(engine, &rows)).unwrap_err()
        );
        assert!(error.contains("does not fit the view"), "{error}");
        assert!(error.contains("failed after 1 retries"), "{error}");
        assert_eq!(snapshots(&warehouse), 0);

        TOKIO.block_on(warehouse.add_column("t", "string", PrimitiveType::String));
        encode(&mut endpoint, inserts(engine, &rows)).unwrap();
        assert_eq!(changes(&warehouse), expected(&rows, "i"));
    }

    /// A dropped table waits for its owner to create it again.
    #[engine_test]
    pub fn a_dropped_table_is_retried(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let shutdown = CancellationToken::new();
        let faults = Faults::default();
        let (mut endpoint, _) = endpoint_with(&warehouse, 1, true, Some(1), faults, shutdown);
        TOKIO.block_on(warehouse.drop_table("t"));

        let error = format!(
            "{:#}",
            encode(&mut endpoint, inserts(engine, &sample_rows(2))).unwrap_err()
        );

        assert!(error.contains("failed after 1 retries"), "{error}");
    }

    /// The REST client returns `DataInvalid` when its token endpoint fails with an Iceberg
    /// error body. The connector retries the table load, and keeps the batch.
    #[engine_test]
    pub fn a_load_error_from_a_token_endpoint_is_retried(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let faults = Faults {
            loads: 1,
            load_kind: Some(ErrorKind::DataInvalid),
            ..Faults::default()
        };
        let shutdown = CancellationToken::new();
        let (mut endpoint, _) = endpoint_with(&warehouse, 1, true, Some(1), faults, shutdown);
        let rows = sample_rows(10);

        encode(&mut endpoint, inserts(engine, &rows)).unwrap();

        assert_eq!(changes(&warehouse), expected(&rows, "i"));
    }

    /// A table that no change can make fit fails the batch at once, with no retry.
    #[engine_test]
    pub fn a_table_that_cannot_be_made_to_fit_fails_at_once(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let relation = basic_relation();
        let sink = TOKIO.block_on(warehouse.glue_sink("t", &relation));
        let mut endpoint = IcebergOutputEndpoint::with_sink(
            EndpointId::default(),
            "test_endpoint",
            &config(1, Some(1)),
            &None,
            &relation,
            Weak::new(),
            CancellationToken::new(),
            false,
            sink,
        );
        // The Glue client converts every schema of the table, and fails on this column.
        TOKIO.block_on(warehouse.add_column("t", "at", PrimitiveType::Timestamptz));
        let row = r#"{"id": 1, "b": true, "i": null, "s": "a"}"#;
        let rows = Updates::from_json(published_schema(&relation), &[(1, row)]).unwrap();
        let batch = engine.output_batch(&relation, rows.rows(), &[1]).unwrap();

        let error = format!("{:#}", encode(&mut endpoint, batch).unwrap_err());

        assert!(error.contains("timestamptz"), "{error}");
        assert!(!error.contains("retries"), "{error}");
    }

    /// A table that changes before every commit makes the connector give the batch up after
    /// `max_retries` rewrites.
    #[engine_test]
    pub fn rewrites_count_against_max_retries(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let (mut endpoint, catalog) = endpoint_with(
            &warehouse,
            1,
            true,
            Some(1),
            Faults::default(),
            CancellationToken::new(),
        );
        // Two loads per write, so 3 or more writes would commit.
        catalog.faults.lock().unwrap().schema_changes = 5;

        encode(&mut endpoint, inserts(engine, &sample_rows(10))).unwrap();

        assert_eq!(snapshots(&warehouse), 0);
    }

    /// A row whose weight spans several `fill` calls is written once per unit of weight.
    #[engine_test]
    pub fn a_weight_can_span_chunks(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let relation = value_relation();
        let (sink, _) = TOKIO.block_on(warehouse.flaky_sink("t", &relation, Faults::default()));
        let layout = TOKIO.block_on(sink.begin()).unwrap();
        let batch = engine
            .output_batch(&relation, &sample_rows(2), &[3, -2])
            .unwrap();
        let format = RecordFormat::Parquet(iceberg_output_serde_config());
        let range = batch.split(1, format).unwrap().remove(0);
        let cursor = range.cursor().unwrap();
        let mut rows = cursor.arrow_sink(layout.row_schema()).unwrap();
        let mut change_log = ChangeLog::new(cursor, &relation.name, None, false);

        // One operation per call.
        let mut all = Vec::new();
        for _ in 0..10 {
            let mut ops = Vec::new();
            let more = change_log.fill(rows.as_mut(), &mut ops, 1, &mut |e| panic!("{e}"));
            assert!(ops.len() <= 1);
            all.extend(ops);
            if !matches!(more, Ok(true)) {
                break;
            }
        }
        all.sort();
        assert_eq!(all, ["d", "d", "i", "i", "i"]);
    }

    /// With an index, each key is one operation, also when each `fill` call holds only one.
    #[engine_test]
    pub fn indexed_changes_can_span_chunks(engine: &dyn ConnectorTestEngine) {
        let warehouse = TOKIO.block_on(TestWarehouse::new());
        let relation = value_relation();
        let key = key_relation();
        let (sink, _) = TOKIO.block_on(warehouse.flaky_sink("t", &relation, Faults::default()));
        let layout = TOKIO.block_on(sink.begin()).unwrap();
        // Key 0 is inserted, key 1 is updated (rows 1 and 3), and key 2 is deleted.
        let rows = sample_rows(4);
        let bigint = rows.schema().index_of("bigint").unwrap();
        let mut columns = rows.columns().to_vec();
        columns[bigint] = Arc::new(Int64Array::from(vec![0, 1, 2, 1]));
        let rows = RecordBatch::try_new(rows.schema(), columns).unwrap();
        let batch = engine
            .indexed_output_batch(&key, &relation, &rows, &[1, -1, -1, 1])
            .unwrap();
        let format = RecordFormat::Parquet(iceberg_output_serde_config());
        let range = batch.split(1, format).unwrap().remove(0);
        let cursor = range.cursor().unwrap();
        let mut rows = cursor.arrow_sink(layout.row_schema()).unwrap();
        let mut change_log = ChangeLog::new(cursor, &relation.name, Some(&key), true);

        // One operation per call.
        let mut all = Vec::new();
        for _ in 0..10 {
            let mut ops = Vec::new();
            let more = change_log.fill(rows.as_mut(), &mut ops, 1, &mut |e| panic!("{e}"));
            assert!(ops.len() <= 1);
            all.extend(ops);
            if !matches!(more, Ok(true)) {
                break;
            }
        }
        assert_eq!(all, ["i", "u", "d"]);
        assert_eq!(rows.finish().unwrap().num_rows(), 3);
    }

    /// A catalog that never answers fails the open after [DEFAULT_OPEN_TIME], not after every
    /// retry has timed out.
    #[cfg(test)]
    #[tokio::test(start_paused = true)]
    async fn an_open_that_the_catalog_never_answers_is_bounded() {
        // Takes connections, and never answers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let config: IcebergWriterConfig = serde_json::from_value(json!({
            "catalog_type": "rest",
            "rest.uri": format!("http://{}", listener.local_addr().unwrap()),
            "table_name": "ns.t",
        }))
        .unwrap();
        let shutdown = CancellationToken::new();

        let error = open("test", &config, &value_relation(), false, &shutdown)
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("did not open"), "{error:#}");
    }

    /// A token endpoint that fails with an Iceberg error body does not stop the open: the
    /// connector tries again.
    #[cfg(test)]
    #[tokio::test]
    async fn an_open_retries_a_token_endpoint_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const TOKENS: &str = "/v1/oauth/tokens";
        let server = MockServer::start().await;
        let body = json!({
            "error": {"message": "try later", "type": "ServiceUnavailableException", "code": 503}
        });
        Mock::given(method("POST"))
            .and(path(TOKENS))
            .respond_with(ResponseTemplate::new(503).set_body_json(body))
            .mount(&server)
            .await;
        let config: IcebergWriterConfig = serde_json::from_value(json!({
            "catalog_type": "rest",
            "rest.uri": server.uri(),
            "rest.credential": "client:secret",
            "table_name": "ns.t",
            "max_retries": 1,
        }))
        .unwrap();
        let shutdown = CancellationToken::new();

        let error = open("test", &config, &value_relation(), false, &shutdown)
            .await
            .err()
            .unwrap();

        assert!(format!("{error:#}").contains("try later"), "{error:#}");
        let requests = server.received_requests().await.unwrap();
        let tokens = requests.iter().filter(|r| r.url.path() == TOKENS).count();
        // The first try and one retry.
        assert_eq!(tokens, 2, "{error:#}");
    }

    #[test]
    fn threads_need_an_index() {
        let config: IcebergWriterConfig = serde_json::from_value(json!({
            "catalog_type": "rest",
            "rest.uri": "http://localhost:1",
            "table_name": "ns.t",
            "threads": 2,
        }))
        .unwrap();
        let error = IcebergOutputEndpoint::check_config("test_endpoint", &config, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unique key"), "{error}");
        IcebergOutputEndpoint::check_config("test_endpoint", &config, true).unwrap();
    }
}
