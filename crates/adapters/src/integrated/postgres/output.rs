use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io::Write, str::FromStr, sync::Weak, time::Duration};

use super::{
    error::BackoffError, prepared_statements::PreparedStatements, tls::make_tls_connector,
};
use crate::{ControllerError, util::indexed_operation_type};
use crate::{
    buffer_op,
    catalog::{RecordFormat, SerBatchReader, SerCursor},
    controller::{ControllerInner, EndpointId, MAX_CONNECTOR_ERROR_LEN},
    flush_op,
    format::{Encoder, OutputConsumer},
    transport::OutputEndpoint,
    util::{IndexedOperationType, truncate_ellipse},
};
use anyhow::{Context, Result as AnyResult, anyhow, bail};
use feldera_adapterlib::catalog::SerKeyRange;
use feldera_adapterlib::format::MAX_RECORD_LEN_IN_ERRMSG;
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
    Encode(Box<dyn SerKeyRange>),
}

enum WorkerResult {
    Ok { num_bytes: usize, num_rows: usize },
    Err(anyhow::Error),
}

/// Maximum number of times a worker writes one batch.
///
/// This bounds repeated failures once a connection has come back; waiting for
/// postgres to return happens inside each attempt, in
/// [`PostgresWorker::retry_connecting_with_backoff`], and is not bounded.
const MAX_BATCH_WRITE_ATTEMPTS: usize = 10;

/// The partition of a batch assigned to a worker, retained until the batch
/// commits so that the worker can write it again.
///
/// A [`SerKeyRange`] holds the batch and hands out a fresh cursor per call,
/// and the controller keeps the batch alive across
/// `batch_start`/`encode`/`batch_end` regardless, so retaining this costs a
/// reference count rather than a copy of the data.
struct PendingBatch {
    range: Box<dyn SerKeyRange>,
    /// The extra column values captured when the batch was first encoded.
    /// Writing it again reuses them, so a rewrite produces the same rows even if
    /// the configured values have changed since.
    extra_columns: BTreeMap<String, Option<serde_json::Value>>,
}

/// What has become of the transaction a worker is writing its batch into.
///
/// Postgres aborts a transaction on any statement error and answers a subsequent
/// `COMMIT` with `ROLLBACK` rather than an error, so a commit reporting success
/// proves nothing on its own: only this tells the worker whether the rows it
/// counted reached the table.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    /// Every statement has succeeded, so the transaction can commit.
    Open,
    /// The connection dropped, taking the transaction and every row it held.
    ///
    /// Any rows written as part of this transaction are lost and must be written again.
    Lost,
    /// Postgres rejected a statement and aborted the transaction. Writing the
    /// batch again would hit the same rejection.
    Rejected,
}

/// A single postgres worker that owns a connection and runs on a dedicated thread.
struct PostgresWorker {
    worker_idx: usize,
    endpoint_id: EndpointId,
    endpoint_name: String,
    table: String,
    client: postgres::Client,
    config: PostgresWriterConfig,
    extra_columns: Arc<RwLock<BTreeMap<String, Option<serde_json::Value>>>>,
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
    /// Bytes written by statements in the current transaction.
    ///
    /// Counted only after `execute` succeeds, and reported to the endpoint only
    /// if the transaction then commits cleanly, so that it measures what reached
    /// the table rather than what the worker attempted.
    num_bytes: usize,
    /// Rows written by statements in the current transaction, counted and
    /// reported on the same terms as [`Self::num_bytes`].
    num_rows: usize,
    /// Rows encoded for the current batch, counted as they are buffered rather
    /// than as statements succeed, so that a failed batch can report how many
    /// rows never reached the table. Unlike [`Self::num_rows`], this count
    /// covers the rows whose statements never ran because an earlier failure had
    /// already aborted the transaction.
    batch_rows: usize,
    /// What has become of [`Self::transaction`], which decides whether the `pending_batch`
    /// commits, is written again, or is dropped.
    transaction_state: TransactionState,
    /// The batch being written, kept for as long as it may need writing again.
    pending_batch: Option<PendingBatch>,
    /// Shared counter of records sent to postgres in the current batch.
    /// Updated atomically after each successful `execute()` call across all
    /// workers; reset to 0 by the endpoint at batch boundaries.
    records_written: Arc<AtomicU64>,
}

/// Ends any open transaction while the client it borrows is still there.
///
/// `transaction` borrows `client` behind the transmute in `batch_start_inner`,
/// which hides the borrow from the compiler, and struct fields drop in
/// declaration order, `client` first. So this is not the tidying it looks like:
/// without it, dropping a worker that still holds a transaction would leave
/// `Transaction::drop` rolling back over a client that is already gone.
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
    }
    .map_err(BackoffError::connecting)?;

    Ok(client)
}

impl PostgresWorker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        idx: usize,
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &PostgresWriterConfig,
        extra_columns: Arc<RwLock<BTreeMap<String, Option<serde_json::Value>>>>,
        key_schema: &Relation,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        records_written: Arc<AtomicU64>,
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
            batch_rows: 0,
            transaction_state: TransactionState::Open,
            pending_batch: None,
            inserts: 0,
            upserts: 0,
            deletes: 0,
            insert_buf: Vec::with_capacity(config.max_buffer_size.as_usize()),
            upsert_buf: Vec::with_capacity(config.max_buffer_size.as_usize()),
            delete_buf: Vec::with_capacity(config.max_buffer_size.as_usize()),
            value_schema: value_schema.to_owned(),
            records_written,
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

    fn exec_statement(
        &mut self,
        stmt: Statement,
        mut value: Vec<u8>,
        name: &str,
        num_records: usize,
    ) {
        // Nothing more can reach the table through this transaction, so pushing
        // the rest of the batch at it only produces more failures to report.
        // The whole transaction will eventually be retried.
        if self.transaction_state != TransactionState::Open {
            return;
        }

        let Err(e) = self.exec_statement_inner(stmt, &mut value, name, num_records) else {
            return;
        };

        // Retrying the statement here cannot work: recovering takes a new
        // transaction and a fresh encode, which `write_batch` drives.
        let recoverable = e.should_retry();

        let Some(controller) = self.controller.upgrade() else {
            tracing::warn!("controller is shutting down: aborting");
            return;
        };
        controller.output_transport_error(
            self.endpoint_id,
            &self.endpoint_name,
            !recoverable,
            e.inner(),
            Some("pg_exec"),
        );
    }

    fn exec_statement_inner(
        &mut self,
        stmt: Statement,
        value: &mut Vec<u8>,
        name: &str,
        num_records: usize,
    ) -> Result<(), BackoffError> {
        if value.last() != Some(&b']') {
            value.push(b']');
        }

        if value.len() <= 2 {
            return Ok(());
        }

        let v: &str = std::str::from_utf8(value.as_slice()).map_err(|e| {
            BackoffError::Permanent(anyhow!("record contains non utf-8 characters: {e}"))
        })?;

        let result = self
            .transaction()
            .map_err(BackoffError::Permanent)?
            .execute(&stmt, &[&v]);

        if let Err(e) = result {
            // Report only a prefix of the payload: it holds a whole batch of
            // records, up to `max_buffer_size`, which is far too large
            // to keep in the error list served by `/stats`. The postgres error
            // below the context embeds the failing row, so a fixed prefix
            // bound can push the whole message past MAX_CONNECTOR_ERROR_LEN;
            // give the prefix only the room the error chain leaves over.
            const ERRMSG_HEADROOM_BYTES: usize = 512;
            let error = BackoffError::from(e);
            let payload_prefix_budget = MAX_CONNECTOR_ERROR_LEN
                .saturating_sub(error.message_len() + ERRMSG_HEADROOM_BYTES)
                .min(MAX_RECORD_LEN_IN_ERRMSG);
            let error = error.context(format!(
                "while executing {name} statement for {num_records} record(s) ({} bytes), which start with: {}",
                v.len(),
                truncate_ellipse(v, payload_prefix_budget, "...")
            ));

            // Transaction state is decided by whether the error is retry-able.
            self.transaction_state = if error.should_retry() {
                TransactionState::Lost
            } else {
                TransactionState::Rejected
            };

            return Err(error);
        }

        // Count what this statement wrote. The transaction still has to commit
        // for any of it to survive, which `batch_end_inner` decides.
        self.num_bytes += v.len();
        self.num_rows += num_records;

        // Report progress: these records have now been sent to postgres within
        // the open transaction.  The endpoint resets the counter to 0 at batch
        // boundaries (start, successful commit, failed commit).
        self.records_written
            .fetch_add(num_records as u64, Ordering::Relaxed);

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

        // Losing the connection rolled back the open transaction, so drop what
        // it had written from the count. `batch_end_inner` fails for the rest of
        // this batch, since there is no transaction left to commit.
        self.transaction = None;
        self.num_bytes = 0;
        self.num_rows = 0;
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

        loop {
            if self.shutting_down() {
                tracing::warn!("controller is shutting down: aborting");
                return;
            }

            tracing::info!(
                "worker-thread-{} retrying to connect to postgres",
                self.worker_idx
            );
            match self.retry_connecting() {
                Ok(_) => return,
                Err(e) => {
                    // Only a failure that ends the loop leaves the endpoint
                    // unable to reach postgres.
                    let retry = e.should_retry();
                    if let Some(controller) = self.controller.upgrade() {
                        controller.output_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            !retry,
                            e.inner(),
                            Some("pg_conn_retry"),
                        );
                    }
                    if !retry {
                        return;
                    }
                }
            }

            if !self.backoff_unless_shutdown(
                Duration::from_millis(backoff * n_retries).min(Duration::from_secs(60)),
            ) {
                tracing::warn!("controller is shutting down: aborting");
                return;
            }
            n_retries += 1;
        }
    }

    /// Whether the pipeline has been asked to stop.
    ///
    /// The endpoint runs on the controller's output thread and blocks there
    /// for this worker's reply, and that output thread is a DBSP aux thread
    /// that `RuntimeHandle::kill` joins.  So this has to report a kill of the
    /// runtime as well as a `Terminated` controller, which is what
    /// [ControllerInner::shutdown_token] does: a checkpoint that fails in a worker
    /// kills the circuit from the circuit thread, the thread that would
    /// otherwise set `Terminated`, and the reconnect loop this gates is
    /// unbounded.
    fn shutting_down(&self) -> bool {
        self.controller
            .upgrade()
            .is_none_or(|controller| controller.shutdown_token().is_cancelled())
    }

    /// Waits `duration` before the next connection attempt, giving up as soon as
    /// the pipeline is asked to stop. Returns whether the wait ran to completion.
    fn backoff_unless_shutdown(&self, duration: Duration) -> bool {
        const SLICE: Duration = Duration::from_millis(100);

        let mut remaining = duration;
        while !remaining.is_zero() {
            if self.shutting_down() {
                return false;
            }
            let slice = remaining.min(SLICE);
            std::thread::sleep(slice);
            remaining -= slice;
        }
        true
    }

    fn batch_start_inner(&mut self) -> Result<(), BackoffError> {
        // Skip the controller check in test/bench mode.
        #[cfg(not(any(test, feature = "bench-mode")))]
        if self.controller.upgrade().is_none() {
            tracing::warn!("controller is shutting down: aborting");
            return Ok(());
        }

        // Roll back and release anything a previous batch left open before
        // borrowing the client again. The transmute below hides that borrow from
        // the compiler, so it is on this function to keep two of them from
        // overlapping.
        self.transaction = None;

        let txn = self.client.transaction()?;

        // SAFETY: The transaction borrows `self.client`. Both live on this
        // worker's dedicated thread and never move. The borrow ends before the
        // next one begins: `batch_end_inner` commits or rolls the transaction
        // back, and the line above releases it on the paths that do not reach
        // `batch_end_inner`.
        let transaction: postgres::Transaction<'static> = unsafe { std::mem::transmute(txn) };
        self.transaction = Some(transaction);

        // Start counting afresh: whatever the previous transaction wrote was
        // either reported at its commit or lost with it. Writing a batch again
        // encodes it from the start, so its rows are counted from the start too.
        self.num_bytes = 0;
        self.num_rows = 0;
        self.batch_rows = 0;
        self.transaction_state = TransactionState::Open;

        Ok(())
    }

    /// Flush remaining buffers, commit the transaction, and return (bytes, rows) written.
    fn batch_end_inner(&mut self) -> Result<(usize, usize), BackoffError> {
        self.flush();

        // Do not try to commit a transaction the server has already discarded:
        // report a retryable failure so that the batch is written again.
        if self.transaction_state == TransactionState::Lost {
            self.transaction = None;
            self.num_bytes = 0;
            self.num_rows = 0;
            return Err(BackoffError::Temporary(anyhow!(
                "the connection to PostgreSQL dropped before the rows for table {:?} \
                 could be committed",
                self.table
            )));
        }

        let transaction = self
            .transaction
            .take()
            .ok_or(BackoffError::Permanent(anyhow!(
                "postgres: attempted to commit a transaction that hasn't been started"
            )))?;

        // Roll back explicitly rather than committing: Postgres would answer the
        // commit with `ROLLBACK` and no error, which would report the rows this
        // transaction wrote as written when they were all discarded.
        if self.transaction_state == TransactionState::Rejected {
            let rollback = match transaction.rollback() {
                Ok(()) => String::new(),
                Err(e) => format!(" Rolling the transaction back also failed: {e}."),
            };
            return Err(BackoffError::Permanent(anyhow!(
                "PostgreSQL rejected one of the statements writing to table {:?} and \
                 aborted the transaction, so {} row(s) from the current batch were \
                 dropped. The rejected statement is reported as a separate \
                 error.{rollback}",
                self.table,
                self.batch_rows,
            )));
        }

        transaction.commit()?;

        let num_bytes = std::mem::take(&mut self.num_bytes);
        let num_rows = std::mem::take(&mut self.num_rows);

        Ok((num_bytes, num_rows))
    }

    /// Write the current batch, and write it again from the start if the
    /// connection drops partway.
    ///
    /// Losing the connection rolls the transaction back, so the rows it held are
    /// gone and the payloads that carried them have been drained from the
    /// buffers. Writing the batch again from [`Self::pending_batch`] is what
    /// keeps it from being lost, and is safe precisely because the rollback
    /// returned the table to its pre-batch state.
    ///
    /// A failure that is not retryable ends the batch: rewriting it would hit
    /// the same error, so the caller reports it instead.
    ///
    /// One case stays ambiguous. When the commit itself fails on a lost
    /// connection, whether the server applied it is unknowable, and this writes
    /// the batch again. In `materialized` mode that is harmless, since an insert
    /// resolves conflicts and updates and deletes are idempotent. In `cdc` mode
    /// the target is append-only, so a batch that did commit gains a duplicate
    /// set of rows. Duplicates are the lesser evil for an event log, and this
    /// connector does not claim to be fault tolerant either way.
    fn write_batch(&mut self) -> AnyResult<(usize, usize)> {
        let mut last_error = None;

        for attempt in 0..MAX_BATCH_WRITE_ATTEMPTS {
            if attempt > 0 {
                self.retry_connecting_with_backoff();

                // The connection can drop again while restarting, which is just
                // another attempt rather than the end of the batch.
                match self.restart_batch() {
                    Ok(()) => (),
                    Err(e) if e.should_retry() => {
                        last_error = Some(e.inner());
                        continue;
                    }
                    Err(e) => return Err(e.inner()),
                }
            }

            match self.batch_end_inner() {
                Ok(totals) => return Ok(totals),
                Err(e) if e.should_retry() => {
                    let error = e.inner();
                    tracing::error!(
                        "postgres: worker-thread-{} lost its connection while writing a batch \
                         (attempt {} of {MAX_BATCH_WRITE_ATTEMPTS}), writing it again: {error}",
                        self.worker_idx,
                        attempt + 1,
                    );
                    last_error = Some(error);
                }
                Err(e) => return Err(e.inner()),
            }
        }

        Err(anyhow!(
            "gave up writing to table {:?} after {MAX_BATCH_WRITE_ATTEMPTS} attempts, so \
             these rows were dropped; the last failure was: {}",
            self.table,
            last_error.expect("the loop records a failure before it gives up")
        ))
    }

    /// Open a fresh transaction and encode the retained batch into it again.
    fn restart_batch(&mut self) -> Result<(), BackoffError> {
        // The buffers may hold a partly built payload from the failed attempt.
        // `batch_start_inner` resets the counts and the transaction state.
        self.insert_buf.clear();
        self.upsert_buf.clear();
        self.delete_buf.clear();
        self.inserts = 0;
        self.upserts = 0;
        self.deletes = 0;

        self.batch_start_inner()?;

        // A worker that was handed no partition of this batch has nothing to
        // write again; its transaction is simply empty.
        let Some(pending) = self.pending_batch.take() else {
            return Ok(());
        };
        let result = match pending.range.cursor() {
            Ok(mut cursor) => self.encode_cursor(cursor.as_mut(), pending.extra_columns.clone()),
            Err(e) => Err(e),
        };
        self.pending_batch = Some(pending);

        // Encoding fails on serialization, which will fail the same way
        // however many times the batch is written again, or on reading the
        // batch, which the worker does not retry either.
        if let Err(e) = result {
            // This ends the batch, and no one downstream commits or rolls back
            // the transaction just opened, so release it here rather than leave
            // the session idle in a transaction until the next batch starts.
            self.transaction = None;
            return Err(BackoffError::Permanent(e));
        }

        Ok(())
    }

    /// Encode records from the cursor into postgres within the current transaction.
    fn encode_cursor(
        &mut self,
        cursor: &mut dyn SerCursor,
        extra_columns: BTreeMap<String, Option<serde_json::Value>>,
    ) -> anyhow::Result<()> {
        while cursor.key_valid() {
            let op = match indexed_operation_type(self.view_name(), self.index_name(), cursor) {
                Ok(op) => op,
                Err(e) => {
                    if let Some(controller) = self.controller.upgrade() {
                        controller.output_transport_error(
                            self.endpoint_id,
                            &self.endpoint_name,
                            false,
                            e,
                            Some("pg_uniqueness_violation"),
                        );
                    }
                    None
                }
            };
            if let Some(op) = op {
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

                // One row buffered, counted whether or not its statement runs.
                self.batch_rows += 1;
            }

            cursor.step_key()?;
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
        extra_columns: &BTreeMap<String, Option<serde_json::Value>>,
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
                        // A pipeline that is stopping has no use for a
                        // transaction, and `retry_connecting_with_backoff`
                        // declines to reconnect once it is, so retrying would
                        // spin. Answer the endpoint, which is waiting for a
                        // reply, and go back to the command queue for the
                        // shutdown already in it. This arm comes first because
                        // the one below would otherwise take these errors.
                        Err(e) if self.shutting_down() => {
                            tracing::warn!(
                                "controller is shutting down: abandoning the transaction that \
                                 worker-thread-{} could not start: {}",
                                self.worker_idx,
                                e.inner()
                            );
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes: 0,
                                num_rows: 0,
                            });
                            break;
                        }
                        Err(e) if e.should_retry() => {
                            tracing::error!(
                                "error when trying to start transaction, retrying with backoff: {}",
                                e.inner()
                            );
                            self.retry_connecting_with_backoff();
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e.inner()));
                            break;
                        }
                    }
                },
                WorkerCommand::Encode(range) => {
                    let extra_columns = self.extra_columns.read().clone();
                    let result = match range.cursor() {
                        Ok(mut cursor) => {
                            self.encode_cursor(cursor.as_mut(), extra_columns.clone())
                        }
                        Err(e) => Err(e),
                    };
                    // Hold on to the batch: if the connection drops before the
                    // transaction commits, `write_batch` encodes it again.
                    self.pending_batch = Some(PendingBatch {
                        range,
                        extra_columns,
                    });
                    let _ = match result {
                        Ok(()) => result_tx.send(WorkerResult::Ok {
                            num_bytes: 0,
                            num_rows: 0,
                        }),
                        Err(e) => result_tx.send(WorkerResult::Err(e)),
                    };
                }
                WorkerCommand::Broadcast(BroadcastCommand::BatchEnd) => {
                    let _ = match self.write_batch() {
                        Ok((num_bytes, num_rows)) => result_tx.send(WorkerResult::Ok {
                            num_bytes,
                            num_rows,
                        }),
                        Err(e) => result_tx.send(WorkerResult::Err(e)),
                    };
                    // Release the batch, and with it the worker's reference to
                    // the data the controller handed it.
                    self.pending_batch = None;

                    // The next batch borrows the client again behind a
                    // transmute, so no transaction may outlive this one. Every
                    // test that writes a batch checks this.
                    debug_assert!(
                        self.transaction.is_none(),
                        "worker-thread-{} finished a batch with its transaction still open",
                        self.worker_idx
                    );
                }
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
    extra_columns: Arc<RwLock<BTreeMap<String, Option<serde_json::Value>>>>,
    controller: Weak<ControllerInner>,
    handles: Vec<WorkerHandle>,
    txn_start: std::time::Instant,
    num_bytes: usize,
    num_rows: usize,
    /// Running count of records sent to postgres in the current batch, shared
    /// across all worker threads.  Registered with the controller's metrics
    /// during construction; reset to 0 at batch boundaries.
    records_written: Arc<AtomicU64>,
}

/// Commands supported by the Postgres output connector.
#[derive(Deserialize)]
enum PostgresOutputEndpointCommand {
    /// Set the values of a subset of extra columns.
    #[serde(rename = "set_extra_columns")]
    SetExtraColumns(BTreeMap<String, Option<serde_json::Value>>),
}

struct PostgresOutputEndpointCommandHandler {
    extra_columns: Arc<RwLock<BTreeMap<String, Option<serde_json::Value>>>>,
    allowed_extra_column_names: HashSet<String>,
}

impl PostgresOutputEndpointCommandHandler {
    fn new(
        extra_columns: Arc<RwLock<BTreeMap<String, Option<serde_json::Value>>>>,
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
            .map_err(|e| {
                let command = truncate_ellipse(
                    &command.to_string(),
                    MAX_RECORD_LEN_IN_ERRMSG,
                    "...",
                )
                .into_owned();
                anyhow!("Postgres output connector failed to parse command '{command}' with the following error: {e}")
            })?;

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
        is_index: bool,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        validate_extra_columns_against_value_schema(endpoint_name, config, value_schema)?;

        if !is_index || key_schema.is_none() {
            return Err(ControllerError::not_supported(
                "Postgres output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys",
            ));
        }

        let key_schema = key_schema.as_ref().unwrap().clone();

        let num_threads = config.threads;
        let mut handles = Vec::with_capacity(num_threads);

        // Extra columns are not set initially.
        let extra_columns = Arc::new(RwLock::new(BTreeMap::new()));

        // Shared progress counter.  Register with the controller so the metric
        // is exposed; `add_output()` has already been called by this point so
        // the metrics slot exists.
        let records_written = Arc::new(AtomicU64::new(0));
        if let Some(c) = controller.upgrade() {
            c.status
                .register_batch_progress_counter(&endpoint_id, records_written.clone());
        }

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
                records_written.clone(),
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
            records_written,
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
            // With one writer thread, which is the default, naming the count
            // adds nothing to what the failure itself already says.
            if errors.len() == 1 {
                bail!("{msg}");
            }
            bail!(
                "{} of the connector's writer threads failed: {msg}",
                errors.len()
            );
        }

        Ok(())
    }
}

impl OutputConsumer for PostgresOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size.as_usize()
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.txn_start = std::time::Instant::now();
        self.records_written.store(0, Ordering::Relaxed);

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
        let result = self.broadcast_and_collect(BroadcastCommand::BatchEnd);
        // Reset progress on every batch_end, regardless of outcome: on success
        // the records have been committed; on failure the counter would
        // otherwise leak across batches.
        self.records_written.store(0, Ordering::Relaxed);

        // Report what committed even when a worker failed. Each worker commits
        // its own transaction, so a batch that failed in one worker still wrote
        // the rows the others committed, and `broadcast_and_collect` accumulated
        // only those. Taking the totals here, rather than on the success path
        // alone, also keeps a failed batch from carrying them into the next one.
        let elapsed = self.txn_start.elapsed();
        let num_bytes = std::mem::take(&mut self.num_bytes);
        let num_rows = std::mem::take(&mut self.num_rows);
        let Some(controller) = self.controller.upgrade() else {
            tracing::warn!("controller is shutting down: aborting");
            return;
        };
        controller
            .status
            .output_buffer(self.endpoint_id, num_bytes, num_rows);

        match result {
            Ok(()) => {
                tracing::debug!(
                    "postgres: flushed {num_rows} rows and {num_bytes} bytes in {elapsed:?}",
                );
            }
            Err(err) => {
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

        let mut workers_dispatched = 0;

        // Split the batch into key ranges, at most one per worker.
        for range in batch.split(num_workers, RecordFormat::Json(JsonFlavor::Postgres))? {
            assert!(
                workers_dispatched <= num_workers,
                "unreachable: attempting to dispatch more workers than `num_workers` {num_workers}"
            );

            self.handles[workers_dispatched]
                .cmd_tx
                .send(WorkerCommand::Encode(range))
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
            // With one writer thread, which is the default, naming the count
            // adds nothing to what the failure itself already says.
            if errors.len() == 1 {
                bail!("{msg}");
            }
            bail!(
                "{} of the connector's writer threads failed: {msg}",
                errors.len()
            );
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

#[cfg(any(test, feature = "testing"))]
pub mod endpoint_tests;

#[cfg(test)]
mod tests {
    use feldera_types::size::ConfigSize;
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
        Relation::new(name.into(), fields, materialized, Default::default())
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
            max_buffer_size: ConfigSize::from_mib(1),
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
            true,
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

    /// Tests of the writer that reach no engine. The `postgres_output` connector suite covers
    /// the writer with an engine.
    mod parallel {
        use std::sync::Weak;
        use std::sync::atomic::Ordering;

        use feldera_adapterlib::transport::OutputBatchType;

        use super::super::PostgresOutputEndpoint;
        use super::super::endpoint_tests::{
            key_relation, make_config_with_extra_columns, make_endpoint, pg_client,
            records_written, setup_table, value_relation,
        };
        use crate::controller::EndpointId;
        use crate::format::Encoder;

        /// A worker that cannot open a transaction while the pipeline is
        /// stopping must answer the endpoint instead of retrying.
        ///
        /// It used to retry either way. `retry_connecting_with_backoff` returns
        /// at once once the pipeline is stopping, so the retry loop spun on a
        /// core, never went back to the command queue where the shutdown was
        /// waiting, and never answered `batch_start`. The endpoint's `Drop`
        /// joins its workers, so the pipeline hung rather than stopped.
        ///
        /// `Weak::new()` stands in for a controller that is gone, one of the two
        /// states `shutting_down` reports.
        #[test]
        #[serial_test::serial]
        fn batch_start_gives_up_while_the_pipeline_is_stopping() {
            let mut client = pg_client();
            setup_table(&mut client);

            let mut endpoint = make_endpoint(1);

            // The worker connected while the endpoint was built. Drop that
            // connection so the transaction it opens next fails. These tests are
            // serial, so the only other backends on this database are its own.
            let terminated: i64 = client
                .query_one(
                    "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) \
                     FROM pg_stat_activity WHERE pid <> pg_backend_pid() \
                     AND datname = current_database()) t",
                    &[],
                )
                .expect("failed to terminate the worker's backend")
                .get(0);
            assert!(terminated > 0, "found no backend to terminate");

            // On a thread of its own, so that a regression fails this test
            // rather than hanging the whole run.
            let (returned_tx, returned) = crossbeam::channel::bounded(1);
            std::thread::spawn(move || {
                endpoint.consumer().batch_start(0, OutputBatchType::Delta);
                let _ = returned_tx.send(());
            });

            assert!(
                returned
                    .recv_timeout(std::time::Duration::from_secs(20))
                    .is_ok(),
                "batch_start never returned after the worker lost its connection"
            );
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
                true,
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
                true,
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
        fn test_progress_counter_batch_start_resets() {
            let mut client = pg_client();
            setup_table(&mut client);

            let mut endpoint = make_endpoint(1);

            // Simulate a stale counter value from an earlier (presumably
            // crashed) batch and verify that batch_start clears it.
            endpoint.records_written.store(99, Ordering::Relaxed);
            endpoint.consumer().batch_start(0, OutputBatchType::Delta);
            assert_eq!(records_written(&endpoint), 0);

            // Close out cleanly so the worker transaction doesn't leak.
            endpoint.consumer().batch_end();
        }
    }
}
