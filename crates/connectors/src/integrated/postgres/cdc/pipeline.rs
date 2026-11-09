use super::*;

/// The etl pipeline the connector runs: Postgres-backed state, Feldera sink.
pub(super) type EtlPipeline = Pipeline<PostgresStore, FelderaDestination>;

/// Table errors a connector older than the `drain_on_shutdown` pin left behind
/// when Feldera stopped it mid-write: the refusal it recorded itself, and the
/// error etl records for a result whose sender was dropped. etl persisted both
/// as `ManualRetry` table errors, which it never retries on its own, so a
/// start after an upgrade rolls them back (see [`reconcile_table_state`]).
/// A stop records neither any more: etl now abandons the pending write, and
/// the connector parks the results it cannot answer instead of dropping them.
/// The second text is etl's, from `destination/async_result.rs`, for a result
/// sender dropped unanswered, which only a connector bug does now.
pub(super) const LEGACY_STOP_ERRORS: [&str; 2] = [
    "Postgres CDC input connector terminated before accepting batch",
    "Async result channel closed before sending",
];

/// Whether a stored table error is one an older connector left at a stop.
/// Such writes were never acknowledged, so replaying them is safe.
pub(super) fn is_legacy_stop_error(reason: &str) -> bool {
    LEGACY_STOP_ERRORS
        .iter()
        .any(|marker| reason.contains(marker))
}

/// etl persists table failures as text, so use the same diagnostics for startup
/// errors and failures reported later by the table monitor. Keep unrelated
/// errors free of setup advice and retain the original PostgreSQL diagnostic.
/// Return a single-layer error so the controller's root-cause formatter shows
/// the complete message and hint without repeating etl's embedded diagnostics.
pub(super) fn with_setup_hint(error: anyhow::Error, run_source_migrations: bool) -> anyhow::Error {
    let message = error.to_string();
    let diagnostic = message.to_ascii_lowercase();
    let permission_denied = [
        "permission denied",
        "must be superuser",
        "must be a superuser",
        "must be owner",
    ]
    .iter()
    .any(|text| diagnostic.contains(text));
    let missing_helper = diagnostic.contains("does not exist")
        && ["etl.describe_table_schema(", "etl.describe_table_identity("]
            .iter()
            .any(|function| diagnostic.contains(function));
    if !permission_denied && !missing_helper {
        return error;
    }

    let hint = if run_source_migrations {
        "With run_source_migrations=true, connect as a superuser to install etl's source \
         objects. To use a non-superuser role instead, have an administrator install the \
         source objects and apply the required runtime grants before setting \
         run_source_migrations=false."
    } else {
        "With run_source_migrations=false, have an administrator install or update etl's \
         source objects and grant EXECUTE on its schema helper functions, SELECT on every \
         table in the publication and USAGE on their schemas, plus the required database, \
         etl schema, and state-table privileges. The runtime role also needs the REPLICATION \
         attribute."
    };
    let message = message.trim_end();
    let separator = if message.ends_with('.') { " " } else { ". " };
    anyhow!(
        "{message}{separator}Hint: {hint} State-store migrations still run regardless of this flag. \
         See 'Running as a non-superuser': {NON_SUPERUSER_DOCS}"
    )
}

pub(super) const NON_SUPERUSER_DOCS: &str =
    "https://docs.feldera.com/connectors/sources/postgresql-cdc#running-as-a-non-superuser";

/// The `ddl_command_end` event trigger that etl's source migrations install.
pub(super) const DDL_EVENT_TRIGGER: &str = "supabase_etl_ddl_message_trigger";

/// Fail when etl's DDL event trigger is missing or disabled. Without it the
/// source emits no schema-change messages: a dropped column or a changed
/// column type would then be decoded silently against the stale schema.
/// `pg_event_trigger` is readable by every role, so the check needs no grant.
pub(super) async fn verify_ddl_event_trigger(client: &tokio_postgres::Client) -> AnyResult<()> {
    let row = client
        .query_opt(
            "select evtenabled::text from pg_event_trigger where evtname = $1",
            &[&DDL_EVENT_TRIGGER],
        )
        .await?;
    let problem = match row {
        None => "does not exist in the source database",
        Some(row) if row.get::<_, String>(0) == "D" => "is disabled in the source database",
        Some(_) => return Ok(()),
    };
    Err(anyhow!(
        "etl's event trigger '{DDL_EVENT_TRIGGER}' {problem}, so schema changes to the \
         published tables would go unnoticed. Have an administrator recreate or enable it \
         (see 'Running as a non-superuser': {NON_SUPERUSER_DOCS})"
    ))
}

/// Integrated input connector that reads from Postgres via logical replication (CDC).
pub struct PostgresCdcInputEndpoint {
    inner: Arc<PostgresCdcInputInner>,
}

impl PostgresCdcInputEndpoint {
    pub fn new(
        endpoint_name: &str,
        config: &PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Result<Self, ControllerError> {
        config
            .validate()
            .map_err(|e| ControllerError::invalid_transport_configuration(endpoint_name, &e))?;
        // A URI that does not parse fails here, where the other configuration
        // errors do, rather than once the reader starts.
        parse_pg_uri(&config.uri, &config.tls, endpoint_name).map_err(|e| {
            ControllerError::invalid_transport_configuration(
                endpoint_name,
                &format!("failed to parse Postgres URI: {e}"),
            )
        })?;

        Ok(Self {
            inner: Arc::new(PostgresCdcInputInner::new(
                endpoint_name,
                config.clone(),
                consumer,
            )),
        })
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
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(PostgresCdcInputReader::new(
            &self.inner,
            input_handle,
            resume_info,
        )?))
    }
}

pub(super) struct PostgresCdcInputInner {
    pub(super) endpoint_name: String,
    config: PostgresCdcReaderConfig,
    pub(super) consumer: Box<dyn InputConsumer>,
    pub(super) queue: Arc<InputQueue<QueueAux>>,
    /// Deterministic pipeline ID used for replication slot naming and resume.
    pub(super) pipeline_id: u64,
    /// Watcher source for the background task.  Taken once by `worker_task_inner`.
    /// `Strict` when fault tolerance is enabled (gates slot on checkpoint);
    /// `Fast` otherwise (gates slot on step completion).
    watcher_rx: Mutex<Option<WatcherReceiver>>,
    /// Sender passing acknowledgments and the step that holds their rows to the
    /// background task. Created at construction if completion tracking exists.
    completion_task_tx: Option<mpsc::UnboundedSender<HeldAcks>>,
    /// Receiver half, taken once by worker_task_inner to spawn the background task.
    pub(super) completion_task_rx: Mutex<Option<mpsc::UnboundedReceiver<HeldAcks>>>,
    /// The checkpoint barrier of the initial copy, which the destination, this
    /// reader and [`TableErrorMonitor`] share; see [`CopyBarrier`].
    pub(super) copy: Arc<CopyBarrier>,
    /// Fault tolerance is enabled: the slot advances only past checkpoints.
    pub(super) strict: bool,
    /// The connector is shutting etl down. A write that starts afterwards
    /// queues nothing and parks its result; see
    /// [`FelderaDestination::wait_unpaused`].
    stopping: Arc<AtomicBool>,
    /// Results the stop could not answer; see [`ParkedResults`].
    parked: Arc<ParkedResults>,
}

impl PostgresCdcInputInner {
    pub(super) fn new(
        endpoint_name: &str,
        config: PostgresCdcReaderConfig,
        consumer: Box<dyn InputConsumer>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let completion_rx = consumer.completion_watcher();

        let pipeline_id = pipeline_id(&config.uri, &config.publication, &config.source_table);

        // Use strict mode (gate slot on checkpoint) when fault tolerance is enabled;
        // fast mode (gate slot on step completion) otherwise.
        let watcher_rx = match consumer.checkpoint_watcher() {
            Some(rx) => Some(WatcherReceiver::Strict(rx)),
            None => completion_rx.map(WatcherReceiver::Fast),
        };
        let strict = matches!(watcher_rx, Some(WatcherReceiver::Strict(_)));

        let (completion_task_tx, completion_task_rx) = if watcher_rx.is_some() {
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
            watcher_rx: Mutex::new(watcher_rx),
            completion_task_tx,
            completion_task_rx: Mutex::new(completion_task_rx),
            copy: Arc::new(CopyBarrier::new()),
            strict,
            stopping: Arc::new(AtomicBool::new(false)),
            parked: Arc::new(ParkedResults::default()),
        }
    }

    /// Answer etl for the writes a flush completed, once the step that holds
    /// their rows is durable.
    ///
    /// The stamp is the step the flush fed, and the watcher fires when the
    /// frontier passes it: the checkpoint frontier under fault tolerance, the
    /// step-completion frontier otherwise.
    pub(super) fn acknowledge_after_step(&self, acks: DeferredAcks) {
        let Some(tx) = self.completion_task_tx.as_ref() else {
            // Nothing tracks steps, so there is nothing to wait for.
            for ack in acks {
                ack();
            }
            return;
        };
        let Some(flush_step) = self.consumer.current_step() else {
            // A consumer that reports step progress must say which step it is
            // feeding. Falling back to the count of steps every output
            // connector has finished would stamp these writes with a step
            // whose checkpoint predates their rows, and a crash after the
            // answer would lose them (#7122). Fail the endpoint and drop the
            // answers instead: etl reads those rows again on the next start.
            self.consumer.error(
                true,
                anyhow!(
                    "the pipeline reports step progress but not the step it is feeding, so the \
                     connector cannot tell PostgreSQL which changes are safely stored. This is a \
                     bug, please report it to Feldera developers: \
                     https://github.com/feldera/feldera/issues/"
                ),
                None,
            );
            return;
        };
        if let Err(send_error) = tx.send(HeldAcks { flush_step, acks }) {
            // The completion watcher has already stopped, so nothing will pick
            // these up. A flush that lands while the stop is draining the
            // channel is as durable as one that landed before it, so answer
            // the writes the frontier has passed by the same rule the watcher
            // used, and park the rest: etl reads their rows again on the next
            // start.
            let HeldAcks { flush_step, acks } = send_error.0;
            answer_durable_after_stop(&self.watcher_rx, &self.parked, flush_step, acks);
        }
    }

    pub(super) async fn worker_task(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        feldera_required_columns: Vec<String>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
        redo_snapshot: bool,
        resumed_copy: ResumedCopy,
    ) {
        self.clone()
            .worker_task_inner(
                input_stream,
                feldera_required_columns,
                receiver,
                init_status_sender,
                redo_snapshot,
                resumed_copy,
            )
            .await;
        debug!(
            "postgres_cdc {}: worker task terminated",
            &self.endpoint_name
        );
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        input_stream: Box<dyn DeCollectionStream>,
        feldera_required_columns: Vec<String>,
        receiver: Receiver<PipelineState>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
        redo_snapshot: bool,
        resumed_copy: ResumedCopy,
    ) {
        let pg_conn = match parse_pg_uri(&self.config.uri, &self.config.tls, &self.endpoint_name) {
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

        // A `source_table` the publication does not carry is a configuration
        // error, and the most likely one after an unqualified name came to
        // mean `public`. Say so now, with the publication's table list, rather
        // than ingest nothing until the stall monitor speaks.
        let catalog = connect_pg(&self.config.uri, &self.config.tls, &self.endpoint_name).await;
        match &catalog {
            Ok(client) => match publication_tables(client, &self.config.publication).await {
                Ok(tables) => {
                    if let Some(problem) = source_table_missing_from(
                        &self.config.source_table,
                        &self.config.publication,
                        &tables,
                    ) {
                        let _ =
                            init_status_sender.send(Err(ControllerError::input_transport_error(
                                &self.endpoint_name,
                                true,
                                anyhow!(problem),
                            )));
                        return;
                    }
                }
                Err(e) => self.warn_publication_unlisted(&e),
            },
            Err(e) => self.warn_publication_unlisted(e),
        }

        // Skipped source migrations leave the event trigger to an
        // administrator, so check it before touching the state store: a
        // never-bootstrapped database then stays untouched. With migrations
        // enabled the trigger is checked after etl installs it. A failed
        // catalog connection is left for the state store to report.
        if let (false, Ok(client)) = (self.config.run_source_migrations, &catalog)
            && let Err(e) = verify_ddl_event_trigger(client).await
        {
            let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                &self.endpoint_name,
                true,
                e,
            )));
            return;
        }

        let pipeline_config = PipelineConfig {
            id: self.pipeline_id,
            publication_name: self.config.publication.clone(),
            pg_connection: pg_conn.clone(),
            // etl stores its replication state in the source database itself, so
            // the state store reuses the source connection.
            store_pg_connection: None,
            // etl names its replication slots after the pipeline id alone, and
            // failover slots need PostgreSQL 17 or later, so the connector
            // keeps the default slot configuration.
            replication_slot: Default::default(),
            batch: batch_config_from(&self.config.batch),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(memory_backpressure_config_from(
                &self.config.memory_backpressure,
            )),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            // The initial copy needs etl's schema helpers. When migrations are
            // disabled, an administrator must have installed them beforehand.
            run_source_migrations: self.config.run_source_migrations,
            replication_lag_refresh_interval_ms:
                PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
            settle_durable_interval_ms: None,
            // A write becomes durable only when the circuit commits a step,
            // and the circuit stops stepping once the connector is told to
            // stop. Draining would wait for an answer that never comes, so
            // etl abandons the pending write and the buffered batch instead;
            // PostgreSQL offers them again on the next start.
            drain_on_shutdown: false,
        };

        // Use PostgresStore to persist table replication phases across restarts.
        // This allows etl to resume from the replication slot position instead of
        // re-snapshotting the entire table on restart.
        let store = match PostgresStore::new(self.pipeline_id, pg_conn).await {
            Ok(store) => store,
            Err(e) => {
                let message = format!("failed to initialize PostgresStore: {e}");
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    with_setup_hint(
                        anyhow!(e).context(message),
                        self.config.run_source_migrations,
                    ),
                )));
                return;
            }
        };

        match reconcile_table_state(
            &store,
            &self.config.source_table,
            redo_snapshot,
            resumed_copy,
            &self.endpoint_name,
        )
        .await
        {
            Ok(reconciled) => {
                if let Some(lsn) = resumed_copy.lsn().max(reconciled.sync_done_lsn) {
                    self.copy.note_sync_done_lsn(lsn);
                }
                self.copy.set_copy_open(reconciled.copy_outstanding);
            }
            Err(e) => {
                let message = format!(
                    "could not reconcile the replication state of table '{}' before \
                     starting: {e}. The connector keeps its resume position in schema 'etl' \
                     of the source database and must read and update it at startup; fix the \
                     reported problem, then restart the pipeline",
                    self.config.source_table
                );
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &self.endpoint_name,
                    true,
                    with_setup_hint(
                        anyhow!(e).context(message),
                        self.config.run_source_migrations,
                    ),
                )));
                return;
            }
        }

        let destination = FelderaDestination {
            input_stream: Arc::new(Mutex::new(input_stream)),
            queue: Arc::clone(&self.queue),
            source_table: self.config.source_table.clone(),
            endpoint_name: self.endpoint_name.clone(),
            feldera_required_columns,
            // One predicate decides deferral for the destination and the
            // reader alike, so neither can answer etl on its own terms.
            defer_acks: self.completion_task_tx.is_some(),
            pipeline_state_rx: receiver.clone(),
            copy: Arc::clone(&self.copy),
            consumer: self.consumer.clone(),
            stopping: Arc::clone(&self.stopping),
            parked: Arc::clone(&self.parked),
        };

        let table_error_monitor = TableErrorMonitor {
            endpoint_name: self.endpoint_name.clone(),
            run_source_migrations: self.config.run_source_migrations,
            consumer: self.consumer.clone(),
            store: store.clone(),
            source_table: self.config.source_table.clone(),
            publication: self.config.publication.clone(),
            copy: Arc::clone(&self.copy),
            pipeline_state_rx: receiver.clone(),
        };
        // Stopping the pipeline needs the pipeline itself, so a stop that
        // arrives while `start` runs takes effect once it returns: the
        // termination branch below fires at once.
        let mut pipeline = Pipeline::new(pipeline_config, store, destination);

        let started = match pipeline.start().await {
            Ok(()) if self.config.run_source_migrations => match &catalog {
                // The migrations have installed the trigger by now; an
                // administrator may still have disabled it since.
                Ok(client) => verify_ddl_event_trigger(client).await,
                Err(_) => Ok(()),
            },
            Ok(()) => Ok(()),
            Err(e) => {
                let message = format!("failed to start etl pipeline: {e}");
                Err(with_setup_hint(
                    anyhow!(e).context(message),
                    self.config.run_source_migrations,
                ))
            }
        };
        drop(catalog);
        match started {
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
                    e,
                )));
                self.shutdown_etl_pipeline(Some(&pipeline));
                return;
            }
        }

        // Spawn the completion watcher background task if tracking is available.
        // The watcher and the channel were created in new(); we take the channel
        // here after etl has started so startup failures do not leave a task
        // behind. The watcher is cloned rather than taken: the reader keeps its
        // own to read the frontier after the watcher stops, so a flush that
        // races the stop can still tell the durable writes from the rest.
        let mut completion_handle = match (
            self.watcher_rx.lock().unwrap().as_ref().cloned(),
            self.completion_task_rx.lock().unwrap().take(),
        ) {
            (Some(watcher), Some(rx)) => {
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                Some(CompletionWatcher {
                    shutdown_tx,
                    handle: tokio::spawn(completion_watcher_task(
                        watcher,
                        rx,
                        shutdown_rx,
                        Arc::clone(&self.parked),
                        self.endpoint_name.clone(),
                    )),
                })
            }
            _ => None,
        };

        // Run the pipeline alongside a watcher for non-retriable per-table
        // errors. etl marks a table errored (e.g. on a source schema change)
        // without failing the whole pipeline, so `pipeline.wait` would block
        // forever while the input silently stalls; the watcher reports such an
        // error so the controller fails the endpoint instead.
        let mut receiver_clone = receiver.clone();
        let pipeline_wait = pipeline.wait();
        tokio::pin!(pipeline_wait);
        let (pipeline_result, report_error) = select! {
            result = &mut pipeline_wait => (result, true),
            // Drop the watch guard inside the block: holding it while awaiting
            // `pipeline_wait` blocks etl tasks that read the same watch in
            // `wait_unpaused`, once the reader's `Drop` queues another write.
            _ = async {
                let _ = receiver_clone
                    .wait_for(|state| state == &PipelineState::Terminated)
                    .await;
            } => {
                debug!(
                    "postgres_cdc {}: received termination command; shutting down etl pipeline",
                    &self.endpoint_name
                );
                self.shutdown_etl_pipeline(Some(&pipeline));
                stop_completion_watcher(&mut completion_handle, &self.endpoint_name).await;
                (self.await_etl_stop(&mut pipeline_wait).await, false)
            }
            _ = table_error_monitor.run() => {
                self.shutdown_etl_pipeline(Some(&pipeline));
                stop_completion_watcher(&mut completion_handle, &self.endpoint_name).await;
                (self.await_etl_stop(&mut pipeline_wait).await, false)
            }
        };

        if let Err(e) = pipeline_result {
            if report_error && *receiver.borrow() != PipelineState::Terminated {
                error!(
                    "postgres_cdc {}: etl pipeline error: {e}",
                    &self.endpoint_name
                );
                self.consumer.error(
                    true,
                    with_setup_hint(anyhow!(e), self.config.run_source_migrations),
                    None,
                );
            } else {
                debug!(
                    "postgres_cdc {}: etl pipeline stopped during shutdown: {e}",
                    &self.endpoint_name
                );
            }
        }

        stop_completion_watcher(&mut completion_handle, &self.endpoint_name).await;

        self.consumer.eoi();
    }

    fn warn_publication_unlisted(&self, error: &anyhow::Error) {
        warn!(
            "postgres_cdc {}: could not list the tables of publication '{}' ({error}); a \
             source table the publication does not carry is reported by the stall \
             monitor instead",
            &self.endpoint_name, &self.config.publication
        );
    }

    /// Wait for etl to stop after [`Self::shutdown_etl_pipeline`], for at
    /// most [`ETL_STOP_GRACE`].
    ///
    /// The stop reaches this wait once the completion watcher has stopped,
    /// which takes at most [`WATCHER_SHUTDOWN_TIMEOUT`], and the runtime
    /// shutdown after it takes at most [`RUNTIME_SHUTDOWN_TIMEOUT`]. The three
    /// together bound how long a stop holds the replication slots.
    ///
    /// etl closes its replication connections when its workers return, and
    /// PostgreSQL releases the slots then. A worker that does not return
    /// would keep its slot active for as long as it lives, so the wait is
    /// bounded: past the grace the connector gives up on etl, and the runtime
    /// etl runs on is shut down once the worker task returns, which closes
    /// the connections in its stead.
    async fn await_etl_stop(
        &self,
        pipeline_wait: impl Future<Output = EtlResult<()>>,
    ) -> EtlResult<()> {
        match tokio::time::timeout(ETL_STOP_GRACE, pipeline_wait).await {
            Ok(result) => result,
            Err(_) => {
                STOPPED_PAST_GRACE.lock().unwrap().insert(self.pipeline_id);
                warn!(
                    "postgres_cdc {}: etl did not stop within {ETL_STOP_GRACE:?}, so its \
                     replication connections are closed by force. PostgreSQL offers the changes \
                     they had not confirmed again on the next start",
                    &self.endpoint_name
                );
                Ok(())
            }
        }
    }

    /// Stops etl. With draining off, etl abandons the write it has pending
    /// and the batch it has buffered, so nothing holds the replication slot
    /// open; PostgreSQL offers those changes again on the next start. The
    /// answers still queued stay queued: a flush after the stop answers into
    /// a channel nobody reads, and the rest drop with the queue, after etl
    /// has stopped.
    ///
    /// `pipeline` is `None` once the worker that owned it has returned, when
    /// only a held copy barrier is left to park.
    fn shutdown_etl_pipeline(&self, pipeline: Option<&EtlPipeline>) {
        if let Some(pipeline) = pipeline {
            pipeline.shutdown();
        }
        // etl is asked to stop first; the flag then turns away the writes it
        // still issues before it sees the request.
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(terminal) = self.copy.take_terminal() {
            self.parked.park(terminal);
        }
    }
}

impl Drop for PostgresCdcInputInner {
    fn drop(&mut self) {
        self.shutdown_etl_pipeline(None);
    }
}

/// Tables `publication` carries, as `(schema, table)`, read from
/// `pg_publication_tables` over a plain connection to the source.
/// A plain (non-replication) connection for catalog queries at startup.
pub(super) async fn connect_pg(
    uri: &str,
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<tokio_postgres::Client> {
    let config: tokio_postgres::Config = uri.parse()?;
    Ok(match make_tls_connector(tls, endpoint_name)? {
        Some(connector) => {
            let (client, connection) = config.connect(connector).await?;
            tokio::spawn(connection);
            client
        }
        None => {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(connection);
            client
        }
    })
}

pub(super) async fn publication_tables(
    client: &tokio_postgres::Client,
    publication: &str,
) -> AnyResult<Vec<(String, String)>> {
    let rows = client
        .query(
            "select schemaname, tablename from pg_publication_tables where pubname = $1",
            &[&publication],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect())
}

/// Why the connector could never read `source_table` from `publication`,
/// given the tables the publication carries, or `None` when it carries the
/// table.
pub(super) fn source_table_missing_from(
    source_table: &str,
    publication: &str,
    tables: &[(String, String)],
) -> Option<String> {
    if tables
        .iter()
        .any(|(schema, table)| is_target_table(source_table, schema, table))
    {
        return None;
    }
    let carried = if tables.is_empty() {
        "no tables".to_string()
    } else {
        tables
            .iter()
            .map(|(schema, table)| format!("{schema}.{table}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Some(format!(
        "table '{source_table}' is not in publication '{publication}' (an unqualified name \
         refers to schema 'public'); the publication carries {carried}. Add the table to the \
         publication, or name one the publication carries, then restart the pipeline"
    ))
}

/// Whether `schema_name.table_name` is the connector's source table. An
/// unqualified `source_table` names a table in `public`, as the connector
/// documentation says; matching a bare name in any schema would let a
/// publication that carries `sales.orders` and `archive.orders` merge both
/// into one Feldera table (#6126).
pub(super) fn is_target_table(source_table: &str, schema_name: &str, table_name: &str) -> bool {
    let qualified = format!("{schema_name}.{table_name}");
    source_table == qualified
        || source_table == format!("\"{schema_name}\".\"{table_name}\"")
        || (schema_name == "public" && source_table == table_name)
}

/// Whether etl has completed the initial table sync for a table in `state`.
///
/// etl excludes `FinishedCopy` on purpose: a copy that finished without a
/// durable catchup handoff is read again on the next start, so the table is not
/// done with its first read.
pub(super) fn sync_completed(state: &TableState) -> bool {
    TableStateType::from(state).has_completed_table_sync()
}

/// Postgres OID of `source_table`, if etl has stored its schema.
///
/// etl keeps one schema row per version of a table and never prunes the rows
/// of a table that was dropped, so a source table that was dropped and
/// recreated leaves several ids under one name. etl keeps replication state
/// only for the table it replicates now, so that state picks the current one.
pub(super) async fn target_table_id(
    store: &PostgresStore,
    source_table: &str,
) -> EtlResult<Option<TableId>> {
    let ids: Vec<TableId> = store
        .get_table_schemas()
        .await?
        .iter()
        .filter(|schema| is_target_table(source_table, &schema.name.schema, &schema.name.name))
        .map(|schema| schema.id)
        .collect();
    let mut candidates = Vec::with_capacity(ids.len());
    for id in ids {
        candidates.push((id, store.get_table_state(id).await?.is_some()));
    }
    Ok(current_table_id(candidates))
}

/// Among the ids that share the source table's name, each paired with whether
/// etl keeps replication state for it, the table etl replicates now: the
/// newest id with state, else the newest id.
pub(super) fn current_table_id(mut candidates: Vec<(TableId, bool)>) -> Option<TableId> {
    candidates.sort();
    candidates.dedup();
    candidates
        .iter()
        .rev()
        .find(|(_, has_state)| *has_state)
        .or(candidates.last())
        .map(|(id, _)| *id)
}

/// How long a stop waits for the watcher to answer its durable writes.
pub(crate) const WATCHER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a stop waits for etl to close its replication connections on its
/// own; see [`PostgresCdcInputInner::await_etl_stop`].
pub(crate) const ETL_STOP_GRACE: Duration = Duration::from_secs(5);

/// How long the connector's runtime waits for the tasks it still hosts once
/// the worker task has returned, before it drops them. Dropping them is what
/// closes the connections of an etl worker that outlived the grace.
pub(crate) const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Run `future` to completion on `runtime`, then shut the runtime down within
/// [`RUNTIME_SHUTDOWN_TIMEOUT`], whether the future returned or panicked.
///
/// Dropping a runtime waits without bound for the work it hosts to stop, so
/// a panic that unwound past the shutdown would turn the bounded stop into a
/// hang on this thread, with etl's connections and slots held meanwhile. The
/// panic is resumed once the runtime is gone.
pub(super) fn block_on_then_shut_down<F: Future>(
    runtime: tokio::runtime::Runtime,
    future: F,
) -> F::Output {
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| runtime.block_on(future)));
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    match result {
        Ok(output) => output,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

/// The etl pipelines, by [`pipeline_id`], whose stop ran past
/// [`ETL_STOP_GRACE`] and closed etl's connections by force. Such a stop
/// releases its slots late. A test of prompt release checks that its own
/// pipelines are absent; keying by pipeline keeps the stragglers of other
/// tests, whose wrapper threads outlive their bodies, out of its verdict.
pub(crate) static STOPPED_PAST_GRACE: Mutex<BTreeSet<u64>> = Mutex::new(BTreeSet::new());

/// Worker threads of the runtime a connector runs etl on. The most parallel
/// work etl does for one connector is the initial copy, which reads
/// `max_copy_connections_per_table` connections at once, so one worker per
/// connection keeps the copy as parallel as it was on the shared runtime.
pub(super) const RUNTIME_WORKER_THREADS: usize =
    PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE as usize;

/// Stack size of the threads etl runs on, the shared runtime's. A stack
/// overflow in a task takes the whole process down, so the runtime that
/// replaced the shared one must not shrink the stacks etl grew up with.
pub(super) const RUNTIME_THREAD_STACK_SIZE: usize = 6 * 1024 * 1024;

/// Stop the completion watcher, letting it answer the writes the frontier has
/// already passed. Calling it again finds nothing to stop.
pub(super) async fn stop_completion_watcher(
    watcher: &mut Option<CompletionWatcher>,
    endpoint_name: &str,
) {
    let Some(CompletionWatcher {
        shutdown_tx,
        mut handle,
    }) = watcher.take()
    else {
        return;
    };
    // An error means the task has already returned on its own.
    let _ = shutdown_tx.send(());
    // The task answers without awaiting, so the timeout is a backstop that
    // keeps a stop bounded should an answer ever block.
    if tokio::time::timeout(WATCHER_SHUTDOWN_TIMEOUT, &mut handle)
        .await
        .is_err()
    {
        warn!(
            "postgres_cdc {endpoint_name}: the completion watcher did not stop within \
             {WATCHER_SHUTDOWN_TIMEOUT:?}, so it is being aborted. The writes it had not yet \
             answered stay unacknowledged, so PostgreSQL offers their changes again on the \
             next start"
        );
        handle.abort();
        let _ = handle.await;
    }
}

/// Deterministic pipeline ID derived from the connection config.
///
/// Stable across Rust versions (xxh3) and across password rotations: the
/// identity string excludes the password and other volatile fields, so
/// rotating the password does not change the ID — which would otherwise
/// orphan the replication slot and stored etl state and force a full
/// re-snapshot. etl names its replication slots after this ID (e.g.
/// `supabase_etl_apply_<id>`), so tests reconstruct it to clean up slots.
pub(crate) fn pipeline_id(uri: &str, publication: &str, source_table: &str) -> u64 {
    xxh3_64(stable_connection_identity(uri, publication, source_table).as_bytes())
}

/// Build a stable identity string for pipeline_id hashing.
///
/// Extracts host/port/database from the URI (excludes password, username,
/// and query parameters) combined with publication and source_table.
/// Falls back to the raw URI if parsing fails — the pipeline will likely
/// fail startup shortly after anyway with a clearer error.
pub(super) fn stable_connection_identity(
    uri: &str,
    publication: &str,
    source_table: &str,
) -> String {
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
pub(super) fn parse_pg_uri(
    uri: &str,
    tls: &PostgresTlsConfig,
    endpoint_name: &str,
) -> AnyResult<PgConnectionConfig> {
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
        // No separate numeric address; etl resolves `host` itself.
        hostaddr: None,
        port,
        name,
        username,
        password,
        tls: make_etl_tls_config(tls, endpoint_name)?,
        keepalive: TcpKeepaliveConfig::default(),
    })
}

/// Convert the connector's batch configuration into etl's `BatchConfig`.
pub(super) fn batch_config_from(config: &PostgresCdcBatchConfig) -> BatchConfig {
    BatchConfig {
        max_fill_ms: config.max_fill_ms,
        memory_budget_ratio: config.memory_budget_ratio,
        max_bytes: config.max_size.as_usize(),
    }
}

/// Convert the connector's memory backpressure configuration into etl's
/// `MemoryBackpressureConfig`.
pub(super) fn memory_backpressure_config_from(
    config: &PostgresCdcMemoryBackpressureConfig,
) -> MemoryBackpressureConfig {
    MemoryBackpressureConfig {
        activate_threshold: config.activate_threshold,
        resume_threshold: config.resume_threshold,
    }
}
