use super::*;

/// The etl pipeline the connector runs: Postgres-backed state, Feldera sink.
pub(super) type EtlPipeline = Pipeline<PostgresStore, DispatchingDestination>;

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
    shared: Arc<SharedCdcPipeline>,
}

impl PostgresCdcInputEndpoint {
    /// `pipeline_config` and `layout` identify the Feldera pipeline and the
    /// host, which a replication slot shared by several connectors is named
    /// after; see [`PublicationKey`].
    pub fn new(
        endpoint_name: &str,
        config: &PostgresCdcReaderConfig,
        pipeline_config: &feldera_types::config::PipelineConfig,
        layout: &feldera_types::circuit::Layout,
        registry: Arc<ConnectorRegistry>,
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
        let key = if pipeline_config.global.dev_tweaks.shared_cdc_slot() {
            PublicationKey::per_publication(config, pipeline_config, layout).map_err(|e| {
                ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
            })?
        } else {
            PublicationKey::per_table(config)
        };
        let shared = registry.get_or_create(&format!("postgres_cdc/{}", key.pipeline_id()), || {
            SharedCdcPipeline::new(key.clone(), registry.clone())
        });

        Ok(Self {
            inner: Arc::new(PostgresCdcInputInner::new(
                endpoint_name,
                config.clone(),
                key,
                consumer,
                Arc::clone(&shared.stopping),
                Arc::clone(&shared.parked),
            )),
            shared,
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
            &self.shared,
            input_handle,
            resume_info,
        )?))
    }
}

pub(super) struct PostgresCdcInputInner {
    pub(super) endpoint_name: String,
    pub(super) config: PostgresCdcReaderConfig,
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
    /// etl is being stopped; shared with the [`SharedCdcPipeline`].
    stopping: Arc<AtomicBool>,
    /// Results the stop could not answer; shared with the
    /// [`SharedCdcPipeline`], see [`ParkedResults`].
    parked: Arc<ParkedResults>,
}

impl PostgresCdcInputInner {
    pub(super) fn new(
        endpoint_name: &str,
        config: PostgresCdcReaderConfig,
        key: PublicationKey,
        consumer: Box<dyn InputConsumer>,
        stopping: Arc<AtomicBool>,
        parked: Arc<ParkedResults>,
    ) -> Self {
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let completion_rx = consumer.completion_watcher();

        let pipeline_id = key.pipeline_id();

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
            stopping,
            parked,
        }
    }

    /// Whether the answers to etl wait for the step that holds their rows.
    pub(super) fn defers_acks(&self) -> bool {
        self.completion_task_tx.is_some()
    }

    /// Spawns the completion watcher, if the consumer tracks steps. Takes the
    /// channel, so a second call spawns nothing.
    pub(super) fn spawn_completion_watcher(
        &self,
        parked: &Arc<ParkedResults>,
    ) -> Option<CompletionWatcher> {
        match (
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
                        Arc::clone(parked),
                        self.endpoint_name.clone(),
                    )),
                })
            }
            _ => None,
        }
    }

    /// Parks the held copy barrier, if any, once etl is stopping.
    pub(super) fn park_terminal(&self) {
        if let Some(terminal) = self.copy.take_terminal() {
            self.parked.park(terminal);
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
}

impl Drop for PostgresCdcInputInner {
    fn drop(&mut self) {
        // Only a held copy barrier can be left to park once the worker that
        // owned the etl pipeline has returned.
        self.stopping.store(true, Ordering::SeqCst);
        self.park_terminal();
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
