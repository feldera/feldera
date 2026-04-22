use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::{io::Write, str::FromStr, sync::Weak, time::Duration};

use super::{
    error::BackoffError, prepared_statements::PreparedStatements, tls::make_tls_connector,
};
use crate::{ControllerError, util::indexed_operation_type};
use crate::{
    buffer_op,
    catalog::{RecordFormat, SerBatchReader, SerCursor},
    controller::{ControllerInner, EndpointId},
    flush_op,
    format::{Encoder, OutputConsumer},
    transport::OutputEndpoint,
    util::IndexedOperationType,
};
use anyhow::{Context, Result as AnyResult, anyhow, bail};
use feldera_adapterlib::catalog::SplitCursorBuilder;
use feldera_adapterlib::transport::{AsyncErrorCallback, CommandHandler, OutputBatchType, Step};
use feldera_types::{
    format::json::JsonFlavor,
    program_schema::{Relation, SqlIdentifier},
    transport::postgres::{PostgresWriteMode, PostgresWriterConfig},
};
use parking_lot::RwLock;
use postgres::{Client, NoTls, Statement};
use serde::Deserialize;

/// Commands sent to all workers at once.
#[derive(Clone, Copy)]
enum BroadcastCommand {
    BatchStart,
    BatchEnd,
    Shutdown,
}

enum WorkerCommand {
    Broadcast(BroadcastCommand),
    Encode(SplitCursorBuilder),
}

enum WorkerResult {
    Ok { num_bytes: usize, num_rows: usize },
    Err(anyhow::Error),
}

/// A single postgres worker that owns a connection and runs on a dedicated thread.
struct PostgresWorker {
    worker_idx: usize,
    endpoint_id: EndpointId,
    endpoint_name: String,
    table: String,
    client: postgres::Client,
    config: PostgresWriterConfig,
    extra_columns: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    transaction: Option<postgres::Transaction<'static>>,
    prepared_statements: PreparedStatements,
    insert_buf: Vec<u8>,
    upsert_buf: Vec<u8>,
    delete_buf: Vec<u8>,
    inserts: usize,
    upserts: usize,
    deletes: usize,
    key_schema: Relation,
    value_schema: Relation,
    controller: Weak<ControllerInner>,
    num_bytes: usize,
    num_rows: usize,
}

impl Drop for PostgresWorker {
    fn drop(&mut self) {
        self.transaction = None;
    }
}

const PG_CONNECTION_VALIDITY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn connect(config: &PostgresWriterConfig, endpoint_name: &str) -> Result<Client, BackoffError> {
    let pgcnf = postgres::Config::from_str(&config.uri).map_err(|e| {
        BackoffError::Permanent(anyhow!("error parsing postgres connection string: {e}"))
    })?;

    let client = match make_tls_connector(&config.tls, endpoint_name) {
        Ok(Some(connector)) => {
            tracing::debug!("CA certificate provided, connecting to postgres with TLS");
            pgcnf.connect(connector)
        }
        Ok(None) => {
            tracing::debug!("no CA certificates provided, connecting to postgres without TLS");
            pgcnf.connect(NoTls)
        }
        Err(e) => return Err(BackoffError::Permanent(e)),
    }?;

    Ok(client)
}

impl PostgresWorker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        idx: usize,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &PostgresWriterConfig,
        extra_columns: Arc<RwLock<BTreeMap<String, Option<String>>>>,
        key_schema: &Relation,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, BackoffError> {
        let table = config.table.to_owned();
        let mut client = connect(config, endpoint_name)?;

        let prepared_statements =
            PreparedStatements::new(key_schema, value_schema, config, &mut client)?;

        Ok(Self {
            worker_idx: idx,
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            controller,
            table,
            config: config.clone(),
            extra_columns,
            client,
            transaction: None,
            prepared_statements,
            key_schema: key_schema.clone(),
            num_rows: 0,
            num_bytes: 0,
            inserts: 0,
            upserts: 0,
            deletes: 0,
            insert_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            upsert_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            delete_buf: Vec::with_capacity(config.max_buffer_size_bytes),
            value_schema: value_schema.to_owned(),
        })
    }

    fn is_cdc(&self) -> bool {
        matches!(self.config.mode, PostgresWriteMode::Cdc)
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.value_schema.name
    }

    fn index_name(&self) -> &SqlIdentifier {
        &self.key_schema.name
    }

    fn transaction(&mut self) -> AnyResult<&mut postgres::Transaction<'static>> {
        self.transaction.as_mut().ok_or(anyhow!(
            "postgres: attempting to perform a transaction that hasn't been created yet"
        ))
    }

    fn exec_statement(&mut self, stmt: Statement, mut value: Vec<u8>, name: &str) {
        loop {
            match self.exec_statement_inner(stmt.clone(), &mut value, name) {
                Ok(_) => return,
                Err(e) => {
                    let retry = e.should_retry();
                    let Some(controller) = self.controller.upgrade() else {
                        tracing::warn!("controller is shutting down: aborting");
                        return;
                    };
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        e.inner(),
                        Some("pg_exec"),
                    );
                    if !retry {
                        return;
                    }
                    self.retry_connecting_with_backoff();
                }
            }
        }
    }

    fn exec_statement_inner(
        &mut self,
        stmt: Statement,
        value: &mut Vec<u8>,
        name: &str,
    ) -> Result<(), BackoffError> {
        if value.last() != Some(&b']') {
            value.push(b']');
        }

        if value.len() <= 2 {
            return Ok(());
        }

        self.num_bytes += value.len();

        let v: &str = std::str::from_utf8(value.as_slice()).map_err(|e| {
            BackoffError::Permanent(anyhow!("record contains non utf-8 characters: {e}"))
        })?;

        self.transaction()
            .map_err(BackoffError::Permanent)?
            .execute(&stmt, &[&v])
            .map_err(|e| {
                BackoffError::from(e).context(format!("while executing {name} statement: {v}"))
            })?;

        value.clear();
        value.push(b'[');

        Ok(())
    }

    fn flush_insert(&mut self) {
        flush_op!(
            self,
            buf = insert_buf,
            counter = inserts,
            stmt = insert,
            name = "insert"
        );
    }

    fn insert(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = insert_buf,
            counter = inserts,
            stmt = insert,
            flush_fn = flush_insert,
            name = "insert",
            value = value
        );
    }

    fn flush_upsert(&mut self) {
        flush_op!(
            self,
            buf = upsert_buf,
            counter = upserts,
            stmt = upsert,
            name = "upsert"
        );
    }

    fn upsert(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = upsert_buf,
            counter = upserts,
            stmt = upsert,
            flush_fn = flush_upsert,
            name = "upsert",
            value = value
        );
    }

    fn flush_delete(&mut self) {
        flush_op!(
            self,
            buf = delete_buf,
            counter = deletes,
            stmt = delete,
            name = "delete"
        );
    }

    fn delete(&mut self, mut value: Vec<u8>) {
        buffer_op!(
            self,
            buf = delete_buf,
            counter = deletes,
            stmt = delete,
            flush_fn = flush_delete,
            name = "delete",
            value = value
        );
    }

    fn flush(&mut self) {
        self.flush_delete();
        self.flush_insert();
        self.flush_upsert();
    }

    fn retry_connecting(&mut self) -> Result<(), BackoffError> {
        if self.controller.upgrade().is_none() {
            tracing::warn!("controller is shutting down: aborting");
            return Ok(());
        };
        if self.client.is_valid(PG_CONNECTION_VALIDITY_TIMEOUT).is_ok() {
            return Ok(());
        };

        self.transaction = None;
        self.client = connect(&self.config, &self.endpoint_name)?;

        self.prepared_statements = PreparedStatements::new(
            &self.key_schema,
            &self.value_schema,
            &self.config,
            &mut self.client,
        )
        .map_err(|e| {
            e.context(format!(
                "postgres: error preparing statements after reconnecting to postgres
These statements were successfully prepared before reconnecting. Does the table {} still exist?",
                self.table
            ))
        })?;

        tracing::info!(
            "postgres: worker-thread-{} successfully reconnected to postgres",
            self.worker_idx
        );

        Ok(())
    }

    fn retry_connecting_with_backoff(&mut self) {
        let backoff = 1000;
        let mut n_retries = 1;

        let Some(controller) = self.controller.upgrade() else {
            tracing::warn!("controller is shutting down: aborting");
            return;
        };

        loop {
            tracing::info!(
                "worker-thread-{} retrying to connect to postgres",
                self.worker_idx
            );
            match self.retry_connecting() {
                Ok(_) => return,
                Err(e) => {
                    let retry = e.should_retry();
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        e.inner(),
                        Some("pg_conn_retry"),
                    );
                    if !retry {
                        return;
                    }
                }
            }

            std::thread::sleep(
                Duration::from_millis(backoff * n_retries).min(Duration::from_secs(60)),
            );
            n_retries += 1;
        }
    }

    fn batch_start_inner(&mut self) -> Result<(), BackoffError> {
        // Skip the controller check in test/bench mode.
        #[cfg(not(any(test, feature = "bench-mode")))]
        if self.controller.upgrade().is_none() {
            tracing::warn!("controller is shutting down: aborting");
            return Ok(());
        }

        let txn = self.client.transaction()?;

        // SAFETY: The transaction borrows `self.client`. Both live on this
        // worker's dedicated thread and never move. The transaction is committed
        // or rolled back in `batch_end_inner` before the next batch.
        let transaction: postgres::Transaction<'static> = unsafe { std::mem::transmute(txn) };
        self.transaction = Some(transaction);

        Ok(())
    }

    /// Flush remaining buffers, commit the transaction, and return (bytes, rows) written.
    fn batch_end_inner(&mut self) -> Result<(usize, usize), BackoffError> {
        self.flush();

        let transaction = self
            .transaction
            .take()
            .ok_or(BackoffError::Permanent(anyhow!(
                "postgres: attempted to commit a transaction that hasn't been started"
            )))?;

        transaction.commit()?;

        let num_bytes = std::mem::take(&mut self.num_bytes);
        let num_rows = std::mem::take(&mut self.num_rows);

        Ok((num_bytes, num_rows))
    }

    /// Encode records from the cursor into postgres within the current transaction.
    fn encode_cursor(
        &mut self,
        cursor: &mut dyn SerCursor,
        extra_columns: BTreeMap<String, Option<String>>,
    ) -> anyhow::Result<()> {
        while cursor.key_valid() {
            if let Some(op) = indexed_operation_type(self.view_name(), self.index_name(), cursor)? {
                cursor.rewind_vals();
                match op {
                    IndexedOperationType::Insert => {
                        let mut buf = Vec::new();
                        cursor.serialize_val(&mut buf)?;
                        self.serialize_extra_columns(&mut buf, &extra_columns)?;

                        if self.is_cdc() {
                            self.serialize_cdc_fields(op, &mut buf)?;
                        }
                        self.insert(buf);
                    }
                    IndexedOperationType::Delete => {
                        let mut buf: Vec<u8> = Vec::new();

                        if self.is_cdc() {
                            cursor.serialize_val(&mut buf)?;
                            self.serialize_extra_columns(&mut buf, &extra_columns)?;
                            self.serialize_cdc_fields(op, &mut buf)?;
                        } else {
                            cursor.serialize_key(&mut buf)?;
                        }

                        self.delete(buf);
                    }
                    IndexedOperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }

                        let mut buf: Vec<u8> = Vec::new();
                        cursor.serialize_val(&mut buf)?;
                        self.serialize_extra_columns(&mut buf, &extra_columns)?;
                        if self.is_cdc() {
                            self.serialize_cdc_fields(op, &mut buf)?;
                        }
                        self.upsert(buf);
                    }
                };

                self.num_rows += 1;
            }

            cursor.step_key();
        }

        Ok(())
    }

    fn serialize_cdc_fields(
        &self,
        op: IndexedOperationType,
        buf: &mut Vec<u8>,
    ) -> Result<(), anyhow::Error> {
        let op = match op {
            IndexedOperationType::Insert => "i",
            IndexedOperationType::Delete => "d",
            IndexedOperationType::Upsert => "u",
        };

        buf.pop(); // Remove the trailing '}'

        // Insert CDC fields
        write!(buf, r#","{}":"{}""#, self.config.cdc_op_column, op)
            .context("failed when encoding CDC op field")?;
        write!(
            buf,
            r#","{}":{}"#,
            self.config.cdc_ts_column,
            chrono::Utc::now().timestamp_micros()
        )
        .context("failed when encoding CDC TS field")?;

        buf.push(b'}'); // Re-add the trailing '}'

        Ok(())
    }

    fn serialize_extra_columns(
        &self,
        buf: &mut Vec<u8>,
        extra_columns: &BTreeMap<String, Option<String>>,
    ) -> Result<(), anyhow::Error> {
        if extra_columns.is_empty() {
            return Ok(());
        }

        let brace = buf.pop(); // Remove the trailing '}'

        debug_assert_eq!(brace, Some(b'}'));

        for (key, value) in extra_columns {
            write!(
                buf,
                r#",{}:{}"#,
                serde_json::to_string(key).unwrap(),
                serde_json::to_string(value).unwrap()
            )
            .context("failed when encoding extra columns")?;
        }
        buf.push(b'}'); // Re-add the trailing '}'
        Ok(())
    }
}

impl PostgresWorker {
    fn run(
        mut self,
        cmd_rx: crossbeam::channel::Receiver<WorkerCommand>,
        result_tx: crossbeam::channel::Sender<WorkerResult>,
    ) {
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                WorkerCommand::Broadcast(BroadcastCommand::BatchStart) => loop {
                    match self.batch_start_inner() {
                        Ok(()) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes: 0,
                                num_rows: 0,
                            });
                            break;
                        }
                        Err(e) => {
                            if e.should_retry() {
                                tracing::error!(
                                    "error when trying to start transaction, retrying with backoff: {}",
                                    e.inner()
                                );
                                self.retry_connecting_with_backoff();
                                continue;
                            }
                            let _ = result_tx.send(WorkerResult::Err(e.inner()));
                            break;
                        }
                    }
                },
                WorkerCommand::Encode(cursor_builder) => {
                    let mut cursor = cursor_builder.build();
                    let extra_columns = self.extra_columns.read().clone();
                    match self.encode_cursor(&mut cursor, extra_columns) {
                        Ok(()) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes: 0,
                                num_rows: 0,
                            });
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e));
                        }
                    }
                }
                WorkerCommand::Broadcast(BroadcastCommand::BatchEnd) => loop {
                    match self.batch_end_inner() {
                        Ok((num_bytes, num_rows)) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes,
                                num_rows,
                            });
                            break;
                        }
                        Err(e) => {
                            if e.should_retry() {
                                tracing::error!(
                                    "error when trying to commit transaction, retrying with backoff: {}",
                                    e.inner()
                                );
                                self.retry_connecting_with_backoff();
                                continue;
                            }
                            let _ = result_tx.send(WorkerResult::Err(e.inner()));
                            break;
                        }
                    }
                },
                WorkerCommand::Broadcast(BroadcastCommand::Shutdown) => break,
            }
        }
    }
}

struct WorkerHandle {
    cmd_tx: crossbeam::channel::Sender<WorkerCommand>,
    result_rx: crossbeam::channel::Receiver<WorkerResult>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Coordinates N parallel `PostgresWorker`s, each on its own thread.
///
/// Incoming batches are partitioned by key range so each worker handles
/// a disjoint slice of the data.
pub struct PostgresOutputEndpoint {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: PostgresWriterConfig,
    extra_columns: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    controller: Weak<ControllerInner>,
    handles: Vec<WorkerHandle>,
    txn_start: std::time::Instant,
    num_bytes: usize,
    num_rows: usize,
}

/// Commands supported by the Postgres output connector.
#[derive(Deserialize)]
enum PostgresOutputEndpointCommand {
    /// Set the values of a subset of extra columns.
    #[serde(rename = "set_extra_columns")]
    SetExtraColumns(BTreeMap<String, Option<String>>),
}

struct PostgresOutputEndpointCommandHandler {
    extra_columns: Arc<RwLock<BTreeMap<String, Option<String>>>>,
    allowed_extra_column_names: HashSet<String>,
}

impl PostgresOutputEndpointCommandHandler {
    fn new(
        extra_columns: Arc<RwLock<BTreeMap<String, Option<String>>>>,
        config: &PostgresWriterConfig,
    ) -> Self {
        Self {
            extra_columns,
            allowed_extra_column_names: config.extra_columns.iter().cloned().collect(),
        }
    }
}

impl CommandHandler for PostgresOutputEndpointCommandHandler {
    fn command(&self, command: serde_json::Value) -> AnyResult<serde_json::Value> {
        let command = serde_json::from_value::<PostgresOutputEndpointCommand>(command.clone())
            .map_err(|e| anyhow!("Postgres output connector failed to parse command '{command}' with the following error: {e}"))?;

        match command {
            PostgresOutputEndpointCommand::SetExtraColumns(extra_columns) => {
                let unknown: Vec<&str> = extra_columns
                    .keys()
                    .filter(|k| !self.allowed_extra_column_names.contains(*k))
                    .map(|s| s.as_str())
                    .collect();
                if !unknown.is_empty() {
                    let mut allowed: Vec<&str> = self
                        .allowed_extra_column_names
                        .iter()
                        .map(|s| s.as_str())
                        .collect();
                    allowed.sort_unstable();
                    bail!(
                        "set_extra_columns: unknown column name(s) {unknown:?}; allowed names from connector config field `extra_columns` are {allowed:?}"
                    );
                }

                let mut configured_extra_columns = self.extra_columns.write();
                for (key, value) in extra_columns {
                    configured_extra_columns.insert(key, value);
                }
                Ok(serde_json::to_value(&*configured_extra_columns)
                    .expect("Postgres output connector failed to serialize response"))
            }
        }
    }
}

impl Drop for PostgresOutputEndpoint {
    fn drop(&mut self) {
        for handle in &self.handles {
            let _ = handle
                .cmd_tx
                .send(WorkerCommand::Broadcast(BroadcastCommand::Shutdown));
        }
        for handle in &mut self.handles {
            if let Some(thread) = handle.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

/// Ensures `config.extra_columns` has no duplicates and no name matches a column in `value_schema`.
fn validate_extra_columns_against_value_schema(
    endpoint_name: &str,
    config: &PostgresWriterConfig,
    value_schema: &Relation,
) -> Result<(), ControllerError> {
    let value_sql_names: HashSet<String> = value_schema
        .fields
        .iter()
        .map(|f| f.name.name().to_lowercase())
        .collect();

    let mut seen = HashSet::new();
    for name in &config.extra_columns {
        if !seen.insert(name) {
            return Err(ControllerError::invalid_transport_configuration(
                endpoint_name,
                &format!("duplicate name in connector config field 'extra_columns': {name:?}"),
            ));
        }
        if value_sql_names.contains(&name.to_lowercase()) {
            return Err(ControllerError::invalid_transport_configuration(
                endpoint_name,
                &format!(
                    "connector config 'extra_columns' includes {name:?}, which is already a column in the output view; \
                     extra columns must not duplicate view columns"
                ),
            ));
        }
    }
    Ok(())
}

impl PostgresOutputEndpoint {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &PostgresWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        validate_extra_columns_against_value_schema(endpoint_name, config, value_schema)?;

        let key_schema = key_schema
            .to_owned()
            .ok_or(ControllerError::not_supported(
                "Postgres output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys"
            ))?;

        let num_threads = config.threads;
        let mut handles = Vec::with_capacity(num_threads);

        // Extra columns are not set initially.
        let extra_columns = Arc::new(RwLock::new(BTreeMap::new()));

        for i in 0..num_threads {
            let worker = PostgresWorker::new(
                i,
                endpoint_id,
                endpoint_name,
                config,
                extra_columns.clone(),
                &key_schema,
                value_schema,
                controller.clone(),
            )
            .map_err(|e| ControllerError::output_transport_error(endpoint_name, true, e.inner()))?;

            let (cmd_tx, cmd_rx) = crossbeam::channel::bounded(1);
            let (result_tx, result_rx) = crossbeam::channel::bounded(1);

            let thread_name = format!("pg-output-{endpoint_name}-{i}");
            let thread = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker.run(cmd_rx, result_tx))
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to spawn worker thread: {e}"),
                    )
                })?;

            handles.push(WorkerHandle {
                cmd_tx,
                result_rx,
                thread: Some(thread),
            });
        }

        Ok(Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_owned(),
            config: config.clone(),
            extra_columns,
            controller,
            handles,
            txn_start: std::time::Instant::now(),
            num_bytes: 0,
            num_rows: 0,
        })
    }

    /// Send `cmd` to every worker, wait for all replies, and accumulate byte/row counts.
    fn broadcast_and_collect(&mut self, cmd: BroadcastCommand) -> Result<(), anyhow::Error> {
        for handle in &self.handles {
            let _ = handle.cmd_tx.send(WorkerCommand::Broadcast(cmd));
        }

        let mut errors: Vec<anyhow::Error> = Vec::new();

        for handle in &self.handles {
            match handle.result_rx.recv() {
                Ok(WorkerResult::Ok {
                    num_bytes,
                    num_rows,
                }) => {
                    self.num_bytes += num_bytes;
                    self.num_rows += num_rows;
                }
                Ok(WorkerResult::Err(e)) => {
                    errors.push(e);
                }
                Err(_) => {
                    errors.push(anyhow!("worker thread disconnected"));
                }
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} worker(s) failed: {msg}", errors.len());
        }

        Ok(())
    }
}

impl OutputConsumer for PostgresOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.txn_start = std::time::Instant::now();

        match self.broadcast_and_collect(BroadcastCommand::BatchStart) {
            Ok(()) => (),
            Err(err) => {
                let Some(controller) = self.controller.upgrade() else {
                    tracing::warn!("controller is shutting down: aborting");
                    return;
                };
                controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to start Postgres transaction(s): {err:#}"),
                    Some("pg_batch_start"),
                );
            }
        }
    }

    fn push_buffer(&mut self, _: &[u8], _: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        match self.broadcast_and_collect(BroadcastCommand::BatchEnd) {
            Ok(()) => {
                let elapsed = self.txn_start.elapsed();
                let num_bytes = std::mem::take(&mut self.num_bytes);
                let num_rows = std::mem::take(&mut self.num_rows);
                tracing::debug!(
                    "postgres: flushed {num_rows} rows and {num_bytes} bytes in {elapsed:?}",
                );

                if let Some(controller) = self.controller.upgrade() {
                    controller
                        .status
                        .output_buffer(self.endpoint_id, num_bytes, num_rows);
                };
            }
            Err(err) => {
                let Some(controller) = self.controller.upgrade() else {
                    tracing::warn!("controller is shutting down: aborting");
                    return;
                };
                controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    true,
                    anyhow!("failed to commit changes to Postgres: {err:#}"),
                    Some("pg_batch_end"),
                );
            }
        }
    }
}

impl Encoder for PostgresOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> anyhow::Result<()> {
        let num_workers = self.handles.len();

        // Split the batch into key-range partitions, one per worker.
        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(num_workers, &mut *bounds);

        let mut workers_dispatched = 0;

        for i in 0..=bounds.len() {
            let Some(cursor_builder) = SplitCursorBuilder::from_bounds(
                batch.clone(),
                &*bounds,
                i,
                RecordFormat::Json(JsonFlavor::Postgres),
            ) else {
                continue;
            };

            assert!(
                workers_dispatched <= num_workers,
                "unreachable: attempting to dispatch more workers than `num_workers` {num_workers}"
            );

            self.handles[workers_dispatched]
                .cmd_tx
                .send(WorkerCommand::Encode(cursor_builder))
                .map_err(|_| anyhow!("worker thread disconnected"))?;
            workers_dispatched += 1;
        }

        // Wait for all dispatched workers to finish.
        let mut errors: Vec<anyhow::Error> = Vec::new();
        for i in 0..workers_dispatched {
            match self.handles[i].result_rx.recv() {
                Ok(WorkerResult::Ok { .. }) => {}
                Ok(WorkerResult::Err(e)) => errors.push(e),
                Err(_) => errors.push(anyhow!("worker thread disconnected")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} worker(s) failed: {msg}", errors.len());
        }

        Ok(())
    }
}

impl OutputEndpoint for PostgresOutputEndpoint {
    fn command_handler(&self) -> Option<Arc<dyn CommandHandler>> {
        Some(Arc::new(PostgresOutputEndpointCommandHandler::new(
            self.extra_columns.clone(),
            &self.config,
        )))
    }

    fn connect(&mut self, _: AsyncErrorCallback) -> anyhow::Result<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        todo!()
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> anyhow::Result<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> anyhow::Result<()> {
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        todo!()
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Weak;

    use feldera_adapterlib::errors::journal::ControllerError;
    use feldera_types::{
        program_schema::{ColumnType, Field, Relation, SqlType},
        transport::postgres::{PostgresTlsConfig, PostgresWriteMode, PostgresWriterConfig},
    };
    use postgres::NoTls;

    use super::PostgresOutputEndpoint;
    use crate::controller::EndpointId;

    fn int_field(name: &str) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ: SqlType::Int,
                nullable: true,
                precision: None,
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )
    }

    fn varchar_field(name: &str) -> Field {
        Field::new(
            name.into(),
            ColumnType {
                typ: SqlType::Varchar,
                nullable: true,
                precision: Some(-1),
                scale: None,
                component: None,
                fields: None,
                key: None,
                value: None,
            },
        )
    }

    fn relation(name: &str, fields: Vec<Field>, materialized: bool) -> Relation {
        Relation {
            name: name.into(),
            fields,
            materialized,
            properties: Default::default(),
        }
    }

    fn postgres_url() -> String {
        std::env::var("POSTGRES_URL")
            .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
    }

    fn make_config(table: &str) -> PostgresWriterConfig {
        PostgresWriterConfig {
            uri: postgres_url(),
            table: table.into(),
            tls: PostgresTlsConfig::default(),
            max_records_in_buffer: None,
            max_buffer_size_bytes: usize::pow(2, 20),
            on_conflict_do_nothing: false,
            mode: PostgresWriteMode::Materialized,
            cdc_op_column: "__feldera_op".to_owned(),
            cdc_ts_column: "__feldera_ts".to_owned(),
            extra_columns: Vec::new(),
            threads: 4,
        }
    }

    fn create_endpoint(
        table: &str,
        idx_rel: Option<Relation>,
        main_rel: Relation,
    ) -> Result<PostgresOutputEndpoint, ControllerError> {
        PostgresOutputEndpoint::new(
            EndpointId::default(),
            "blah",
            &make_config(table),
            &idx_rel,
            &main_rel,
            Weak::new(),
        )
    }

    fn postgres_client() -> postgres::Client {
        postgres::Client::connect(&postgres_url(), NoTls).expect("failed to connect to postgres")
    }

    fn truncate_table(client: &mut postgres::Client, table: &str) {
        client
            .execute(&format!(r#"TRUNCATE TABLE "{table}" "#), &[])
            .expect("failed to drop table");
    }

    fn drop_table(client: &mut postgres::Client, table: &str) {
        client
            .execute(&format!(r#"DROP TABLE "{table}" "#), &[])
            .expect("failed to drop table");
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_table_name() {
        let mut client = postgres_client();
        let table = "01JWRRNVP4CGER2E3SQQKCZFNQ";

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, s varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation("v1", vec![int_field("id"), varchar_field("s")], true);
        create_endpoint(table, Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation("v1", vec![int_field("id"), varchar_field(field)], true);
        create_endpoint(table, Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_key_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation(
            "v1_idx",
            vec![
                int_field("id"),
                varchar_field(r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#),
            ],
            false,
        );
        let main = relation(
            "v1",
            vec![
                int_field("id"),
                varchar_field(r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#),
            ],
            true,
        );
        create_endpoint("02JWRRNVP4CGER2E3SQQKCZFNQ", Some(idx), main).unwrap();

        drop_table(&mut client, table);
    }

    #[test]
    #[serial_test::serial]
    fn test_postgres_bad_field_name() {
        let mut client = postgres_client();
        let table = "02JWRRNVP4CGER2E3SQQKCZFNQ";
        let field = r#""01JWRRNVP4CGER2E3SQQKCZFNQ""#;

        client
            .execute(
                &format!(r#"CREATE TABLE "{table}" (id int primary key, {field} varchar)"#),
                &[],
            )
            .expect("failed to create test table in postgres");

        truncate_table(&mut client, table);

        let idx = relation("v1_idx", vec![int_field("id")], false);
        let main = relation(
            "v1",
            vec![int_field("id"), varchar_field("01JWRRNVP4CGER2E3SQQKCZFNQ")],
            true,
        );
        let err = create_endpoint("02JWRRNVP4CGER2E3SQQKCZFNQ", Some(idx), main)
            .err()
            .unwrap();
        assert!(err.to_string().contains("Please ensure all field"));

        drop_table(&mut client, table);
    }

    mod parallel {
        use std::collections::BTreeMap;
        use std::sync::{Arc, Weak};

        use dbsp::OrdIndexedZSet;
        use dbsp::utils::Tup2;
        use feldera_adapterlib::transport::OutputEndpoint;
        use feldera_macros::IsNone;
        use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
        use feldera_types::{deserialize_without_context, serialize_struct};
        use postgres::NoTls;
        use size_of::SizeOf;

        use crate::catalog::SerBatch;
        use crate::controller::EndpointId;
        use crate::format::Encoder;
        use crate::static_compile::seroutput::SerBatchImpl;
        use feldera_adapterlib::transport::OutputBatchType;

        use super::super::PostgresOutputEndpoint;
        use feldera_types::transport::postgres::{
            PostgresTlsConfig, PostgresWriteMode, PostgresWriterConfig,
        };

        // Test record types

        #[derive(
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
            Clone,
            Hash,
            SizeOf,
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            IsNone,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        struct TestRecord {
            id: i32,
            b: bool,
            i: Option<i64>,
            s: String,
        }

        deserialize_without_context!(TestRecord);

        serialize_struct!(TestRecord()[4]{
            id["id"]: i32,
            b["b"]: bool,
            i["i"]: Option<i64>,
            s["s"]: String
        });

        #[derive(
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
            Clone,
            Hash,
            SizeOf,
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            IsNone,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        struct TestKey {
            id: i32,
        }

        deserialize_without_context!(TestKey);

        serialize_struct!(TestKey()[1]{
            id["id"]: i32
        });

        #[derive(
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            serde::Serialize,
            serde::Deserialize,
            Clone,
            Hash,
            SizeOf,
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            IsNone,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        struct TestRecordWithExtraColumns {
            id: i32,
            b: bool,
            i: Option<i64>,
            s: String,
            #[serde(rename = "EXTRA.COLUMN1")]
            extra_column1: Option<String>,
            extra_column2: Option<String>,
        }

        deserialize_without_context!(TestRecordWithExtraColumns);

        // Helpers

        fn key_relation() -> Relation {
            Relation {
                name: SqlIdentifier::new("test_idx", false),
                fields: vec![Field::new("id".into(), ColumnType::int(false))],
                materialized: false,
                properties: BTreeMap::new(),
            }
        }

        fn value_relation() -> Relation {
            Relation {
                name: SqlIdentifier::new("test_view", false),
                fields: vec![
                    Field::new("id".into(), ColumnType::int(false)),
                    Field::new("b".into(), ColumnType::boolean(false)),
                    Field::new("i".into(), ColumnType::bigint(true)),
                    Field::new("s".into(), ColumnType::varchar(false)),
                ],
                materialized: true,
                properties: BTreeMap::new(),
            }
        }

        fn postgres_url() -> String {
            std::env::var("POSTGRES_URL")
                .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
        }

        fn pg_client() -> postgres::Client {
            postgres::Client::connect(&postgres_url(), NoTls)
                .expect("failed to connect to postgres")
        }

        const TEST_TABLE: &str = "test_parallel_pg_output";

        fn setup_table(client: &mut postgres::Client) {
            client
                .execute(
                    &format!(
                        r#"CREATE TABLE IF NOT EXISTS "{TEST_TABLE}" (
                            id int PRIMARY KEY,
                            b bool NOT NULL,
                            i int8,
                            s varchar NOT NULL
                        )"#
                    ),
                    &[],
                )
                .expect("failed to create test table");
            client
                .execute(&format!(r#"TRUNCATE "{TEST_TABLE}""#), &[])
                .expect("failed to truncate test table");
        }

        fn setup_table_with_extra_columns(client: &mut postgres::Client) {
            client
                .execute(&format!(r#"DROP TABLE IF EXISTS "{TEST_TABLE}""#), &[])
                .expect("failed to drop test table");

            client
                .execute(
                    &format!(
                        r#"
                        CREATE TABLE "{TEST_TABLE}" (
                            id int PRIMARY KEY,
                            b bool NOT NULL,
                            i int8,
                            s varchar NOT NULL,
                            "EXTRA.COLUMN1" varchar,
                            extra_column2 varchar
                        )"#
                    ),
                    &[],
                )
                .expect("failed to create test table");
        }

        fn make_config_with_extra_columns(
            threads: usize,
            extra_columns: Vec<String>,
        ) -> PostgresWriterConfig {
            PostgresWriterConfig {
                uri: postgres_url(),
                table: TEST_TABLE.to_string(),
                tls: PostgresTlsConfig::default(),
                max_records_in_buffer: None,
                max_buffer_size_bytes: usize::pow(2, 20),
                on_conflict_do_nothing: false,
                mode: PostgresWriteMode::Materialized,
                cdc_op_column: "__feldera_op".to_owned(),
                cdc_ts_column: "__feldera_ts".to_owned(),
                threads,
                extra_columns,
            }
        }

        fn make_endpoint_with_extra_columns(
            threads: usize,
            extra_columns: Vec<String>,
        ) -> PostgresOutputEndpoint {
            PostgresOutputEndpoint::new(
                EndpointId::default(),
                "test_endpoint",
                &make_config_with_extra_columns(threads, extra_columns),
                &Some(key_relation()),
                &value_relation(),
                Weak::new(),
            )
            .expect("failed to create endpoint")
        }

        fn make_endpoint(threads: usize) -> PostgresOutputEndpoint {
            make_endpoint_with_extra_columns(threads, Vec::new())
        }

        fn build_insert_batch(records: &[TestRecord]) -> Arc<dyn SerBatch> {
            let tuples: Vec<_> = records
                .iter()
                .map(|r| Tup2(Tup2(TestKey { id: r.id }, r.clone()), 1i64))
                .collect();
            let zset = OrdIndexedZSet::from_tuples((), tuples);
            Arc::new(SerBatchImpl::<_, TestKey, TestRecord>::new(zset))
        }

        fn build_delete_batch(records: &[TestRecord]) -> Arc<dyn SerBatch> {
            let tuples: Vec<_> = records
                .iter()
                .map(|r| Tup2(Tup2(TestKey { id: r.id }, r.clone()), -1i64))
                .collect();
            let zset = OrdIndexedZSet::from_tuples((), tuples);
            Arc::new(SerBatchImpl::<_, TestKey, TestRecord>::new(zset))
        }

        /// Build an upsert batch: weight -1 for the old value, +1 for the new.
        fn build_upsert_batch(updates: &[(TestRecord, TestRecord)]) -> Arc<dyn SerBatch> {
            let mut tuples = Vec::new();
            for (old, new) in updates {
                assert_eq!(old.id, new.id);
                tuples.push(Tup2(Tup2(TestKey { id: old.id }, old.clone()), -1i64));
                tuples.push(Tup2(Tup2(TestKey { id: new.id }, new.clone()), 1i64));
            }
            let zset = OrdIndexedZSet::from_tuples((), tuples);
            Arc::new(SerBatchImpl::<_, TestKey, TestRecord>::new(zset))
        }

        fn encode_batch(endpoint: &mut PostgresOutputEndpoint, batch: &Arc<dyn SerBatch>) {
            endpoint.consumer().batch_start(0, OutputBatchType::Delta);
            endpoint
                .encode(batch.clone().arc_as_batch_reader())
                .unwrap();
            endpoint.consumer().batch_end();
        }

        fn query_all(client: &mut postgres::Client) -> Vec<TestRecord> {
            let rows = client
                .query(
                    &format!(r#"SELECT id, b, i, s FROM "{TEST_TABLE}" ORDER BY id"#),
                    &[],
                )
                .expect("failed to query");
            rows.into_iter()
                .map(|row| TestRecord {
                    id: row.get(0),
                    b: row.get(1),
                    i: row.get(2),
                    s: row.get(3),
                })
                .collect()
        }

        fn query_all_with_extra_columns(
            client: &mut postgres::Client,
        ) -> Vec<TestRecordWithExtraColumns> {
            let rows = client
                .query(
                    &format!(r#"SELECT id, b, i, s, "EXTRA.COLUMN1", extra_column2 FROM "{TEST_TABLE}" ORDER BY id"#),
                    &[],
                )
                .expect("failed to query");
            rows.into_iter()
                .map(|row| TestRecordWithExtraColumns {
                    id: row.get(0),
                    b: row.get(1),
                    i: row.get(2),
                    s: row.get(3),
                    extra_column1: row.get(4),
                    extra_column2: row.get(5),
                })
                .collect()
        }

        fn make_records(n: usize) -> Vec<TestRecord> {
            (0..n)
                .map(|i| TestRecord {
                    id: i as i32,
                    b: i % 2 == 0,
                    i: if i % 3 == 0 {
                        None
                    } else {
                        Some(i as i64 * 10)
                    },
                    s: format!("record_{i}"),
                })
                .collect()
        }

        // Tests

        fn insert_test(threads: usize) {
            let mut client = pg_client();
            setup_table(&mut client);

            let records = make_records(100);
            let batch = build_insert_batch(&records);
            let mut endpoint = make_endpoint(threads);

            encode_batch(&mut endpoint, &batch);

            let got = query_all(&mut client);
            assert_eq!(got, records);
        }

        #[test]
        #[serial_test::serial]
        fn test_insert_single_thread() {
            insert_test(1);
        }

        #[test]
        #[serial_test::serial]
        fn test_insert_multi_thread() {
            insert_test(4);
        }

        fn upsert_test(threads: usize) {
            let mut client = pg_client();
            setup_table(&mut client);

            let records = make_records(100);
            let batch = build_insert_batch(&records);
            let mut endpoint = make_endpoint(threads);

            encode_batch(&mut endpoint, &batch);

            // Update even-id records
            let updates: Vec<_> = records
                .iter()
                .filter(|r| r.id % 2 == 0)
                .map(|r| {
                    let mut new = r.clone();
                    new.s = format!("updated_{}", r.id);
                    (r.clone(), new)
                })
                .collect();
            let upsert_batch = build_upsert_batch(&updates);

            encode_batch(&mut endpoint, &upsert_batch);

            let got = query_all(&mut client);
            let expected: Vec<_> = records
                .iter()
                .map(|r| {
                    if r.id % 2 == 0 {
                        let mut new = r.clone();
                        new.s = format!("updated_{}", r.id);
                        new
                    } else {
                        r.clone()
                    }
                })
                .collect();
            assert_eq!(got, expected);
        }

        #[test]
        #[serial_test::serial]
        fn extra_columns_test() {
            let mut client = pg_client();
            setup_table_with_extra_columns(&mut client);

            let records = make_records(100);
            let batch1 = build_insert_batch(&records[..25]);
            let batch2 = build_insert_batch(&records[25..50]);
            let batch3 = build_insert_batch(&records[50..75]);
            let batch4 = build_insert_batch(&records[75..100]);
            let mut endpoint = make_endpoint_with_extra_columns(
                2,
                vec!["EXTRA.COLUMN1".to_string(), "extra_column2".to_string()],
            );

            encode_batch(&mut endpoint, &batch1);

            endpoint
                .command_handler()
                .unwrap()
                .command(serde_json::json!({
                    "set_extra_columns": {
                        "EXTRA.COLUMN1": "foo1",
                        "extra_column2": "bar1",
                    }
                }))
                .unwrap();

            encode_batch(&mut endpoint, &batch2);

            endpoint
                .command_handler()
                .unwrap()
                .command(serde_json::json!({
                    "set_extra_columns": {
                        "EXTRA.COLUMN1": "foo2",
                        "extra_column2": "bar2",
                    }
                }))
                .unwrap();

            encode_batch(&mut endpoint, &batch3);

            endpoint
                .command_handler()
                .unwrap()
                .command(serde_json::json!({
                    "set_extra_columns": {
                        "EXTRA.COLUMN1": null,
                        "extra_column2": null,
                    }
                }))
                .unwrap();

            encode_batch(&mut endpoint, &batch4);

            let got = query_all_with_extra_columns(&mut client);
            let expected1: Vec<_> = records[..25]
                .iter()
                .map(|r| TestRecordWithExtraColumns {
                    id: r.id,
                    b: r.b,
                    i: r.i,
                    s: r.s.clone(),
                    extra_column1: None,
                    extra_column2: None,
                })
                .collect();
            let expected2: Vec<_> = records[25..50]
                .iter()
                .map(|r| TestRecordWithExtraColumns {
                    id: r.id,
                    b: r.b,
                    i: r.i,
                    s: r.s.clone(),
                    extra_column1: Some("foo1".to_string()),
                    extra_column2: Some("bar1".to_string()),
                })
                .collect();
            let expected3: Vec<_> = records[50..75]
                .iter()
                .map(|r| TestRecordWithExtraColumns {
                    id: r.id,
                    b: r.b,
                    i: r.i,
                    s: r.s.clone(),
                    extra_column1: Some("foo2".to_string()),
                    extra_column2: Some("bar2".to_string()),
                })
                .collect();
            let expected4: Vec<_> = records[75..100]
                .iter()
                .map(|r| TestRecordWithExtraColumns {
                    id: r.id,
                    b: r.b,
                    i: r.i,
                    s: r.s.clone(),
                    extra_column1: None,
                    extra_column2: None,
                })
                .collect();
            let expected: Vec<_> = expected1
                .into_iter()
                .chain(expected2)
                .chain(expected3)
                .chain(expected4)
                .collect();
            assert_eq!(got, expected);

            // Update records
            let updates: Vec<_> = records
                .iter()
                .map(|r| {
                    let mut new = r.clone();
                    new.s = format!("updated_{}", r.id);
                    (r.clone(), new)
                })
                .collect();
            let upsert_batch = build_upsert_batch(&updates);

            endpoint
                .command_handler()
                .unwrap()
                .command(serde_json::json!({
                    "set_extra_columns": {
                        "EXTRA.COLUMN1": "foo3",
                        "extra_column2": "bar3",
                    }
                }))
                .unwrap();

            encode_batch(&mut endpoint, &upsert_batch);

            let got = query_all_with_extra_columns(&mut client);
            let expected: Vec<_> = expected
                .into_iter()
                .map(|r| {
                    let mut new = r.clone();
                    new.s = format!("updated_{}", r.id);
                    new.extra_column1 = Some("foo3".to_string());
                    new.extra_column2 = Some("bar3".to_string());
                    new
                })
                .collect();
            assert_eq!(got, expected);
        }

        #[test]
        fn extra_columns_duplicate_names_in_config_rejected() {
            let cfg = make_config_with_extra_columns(1, vec!["a".into(), "a".into()]);
            let err = match PostgresOutputEndpoint::new(
                EndpointId::default(),
                "test_endpoint",
                &cfg,
                &Some(key_relation()),
                &value_relation(),
                Weak::new(),
            ) {
                Ok(_) => panic!("expected duplicate extra_columns to be rejected"),
                Err(e) => e,
            };
            assert!(
                err.to_string().contains("duplicate"),
                "unexpected error: {err:?}"
            );
        }

        #[test]
        fn extra_columns_clash_with_view_column_rejected() {
            let cfg = make_config_with_extra_columns(1, vec!["id".into()]);
            let err = match PostgresOutputEndpoint::new(
                EndpointId::default(),
                "test_endpoint",
                &cfg,
                &Some(key_relation()),
                &value_relation(),
                Weak::new(),
            ) {
                Ok(_) => panic!("expected extra column name clash with view to be rejected"),
                Err(e) => e,
            };
            let msg = err.to_string();
            assert!(
                msg.contains("extra_columns") && msg.contains("output view"),
                "unexpected error: {err:?}"
            );
        }

        #[test]
        #[serial_test::serial]
        fn test_upsert_single_thread() {
            upsert_test(1);
        }

        #[test]
        #[serial_test::serial]
        fn test_upsert_multi_thread() {
            upsert_test(4);
        }

        fn delete_test(threads: usize) {
            let mut client = pg_client();
            setup_table(&mut client);

            let records = make_records(100);
            let batch = build_insert_batch(&records);
            let mut endpoint = make_endpoint(threads);

            encode_batch(&mut endpoint, &batch);

            // Delete odd-id records
            let to_delete: Vec<_> = records.iter().filter(|r| r.id % 2 == 1).cloned().collect();
            let delete_batch = build_delete_batch(&to_delete);

            encode_batch(&mut endpoint, &delete_batch);

            let got = query_all(&mut client);
            let expected: Vec<_> = records.into_iter().filter(|r| r.id % 2 == 0).collect();
            assert_eq!(got, expected);
        }

        #[test]
        #[serial_test::serial]
        fn test_delete_single_thread() {
            delete_test(1);
        }

        #[test]
        #[serial_test::serial]
        fn test_delete_multi_thread() {
            delete_test(4);
        }

        fn empty_batch_test(threads: usize) {
            let mut client = pg_client();
            setup_table(&mut client);

            let batch = build_insert_batch(&[]);
            let mut endpoint = make_endpoint(threads);

            encode_batch(&mut endpoint, &batch);

            let got = query_all(&mut client);
            assert!(got.is_empty());
        }

        #[test]
        #[serial_test::serial]
        fn test_empty_batch_single_thread() {
            empty_batch_test(1);
        }

        #[test]
        #[serial_test::serial]
        fn test_empty_batch_multi_thread() {
            empty_batch_test(4);
        }

        #[test]
        #[serial_test::serial]
        fn test_single_record_multi_thread() {
            let mut client = pg_client();
            setup_table(&mut client);

            let records = make_records(1);
            let batch = build_insert_batch(&records);
            let mut endpoint = make_endpoint(4);

            encode_batch(&mut endpoint, &batch);

            let got = query_all(&mut client);
            assert_eq!(got, records);
        }

        fn multiple_batches_test(threads: usize) {
            let mut client = pg_client();
            setup_table(&mut client);

            let mut endpoint = make_endpoint(threads);

            // Batch 1: insert 50 records
            let records1 = make_records(50);
            let batch1 = build_insert_batch(&records1);
            encode_batch(&mut endpoint, &batch1);

            let got = query_all(&mut client);
            assert_eq!(got, records1);

            // Batch 2: insert 50 more records (ids 50..100)
            let records2: Vec<_> = (50..100)
                .map(|i| TestRecord {
                    id: i,
                    b: i % 2 == 0,
                    i: if i % 3 == 0 {
                        None
                    } else {
                        Some(i as i64 * 10)
                    },
                    s: format!("record_{i}"),
                })
                .collect();
            let batch2 = build_insert_batch(&records2);
            encode_batch(&mut endpoint, &batch2);

            let got = query_all(&mut client);
            let mut all_records = records1;
            all_records.extend(records2);
            assert_eq!(got, all_records);

            // Batch 3: update records 0..25
            let updates: Vec<_> = all_records[..25]
                .iter()
                .map(|r| {
                    let mut new = r.clone();
                    new.s = format!("batch3_{}", r.id);
                    (r.clone(), new)
                })
                .collect();
            let batch3 = build_upsert_batch(&updates);
            encode_batch(&mut endpoint, &batch3);

            let got = query_all(&mut client);
            let expected: Vec<_> = all_records
                .iter()
                .map(|r| {
                    if r.id < 25 {
                        let mut new = r.clone();
                        new.s = format!("batch3_{}", r.id);
                        new
                    } else {
                        r.clone()
                    }
                })
                .collect();
            assert_eq!(got, expected);

            // Batch 4: delete records 75..100
            let to_delete: Vec<_> = expected[75..].to_vec();
            let batch4 = build_delete_batch(&to_delete);
            encode_batch(&mut endpoint, &batch4);

            let got = query_all(&mut client);
            assert_eq!(got.len(), 75);
            assert_eq!(got, expected[..75]);
        }

        #[test]
        #[serial_test::serial]
        fn test_multiple_batches_single_thread() {
            multiple_batches_test(1);
        }

        #[test]
        #[serial_test::serial]
        fn test_multiple_batches_multi_thread() {
            multiple_batches_test(4);
        }
    }
}
