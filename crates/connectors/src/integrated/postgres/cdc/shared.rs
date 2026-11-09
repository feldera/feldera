//! One etl pipeline, and so one replication slot, for every connector whose
//! [`PublicationKey`] is the same.

use super::*;
use crate::integrated::{ConnectorRegistry, SharedConnectorState};
use futures::future::select_all;
use std::sync::Weak;

/// The etl pipeline the connectors of one [`PublicationKey`] share.
///
/// The connectors register through [`Self::join`], and etl starts once the
/// controller has opened every connector, or at once for a connector that
/// joins later. Each connector holds an `Arc` to it; the controller's
/// [`ConnectorRegistry`] holds it weakly.
pub(super) struct SharedCdcPipeline {
    key: PublicationKey,
    /// The first connector that joined, by endpoint name, and its settings,
    /// which configure the whole etl pipeline; every later member must agree.
    first: Mutex<Option<(String, PostgresCdcReaderConfig)>>,
    registry: Arc<ConnectorRegistry>,
    /// Connectors that joined and wait for etl to start.
    joined: Mutex<Vec<Member>>,
    /// The worker runs etl for the members it took.
    running: AtomicBool,
    /// The worker has returned; the pipeline takes no more members.
    retired: AtomicBool,
    /// etl is being stopped. A write that starts afterwards queues nothing
    /// and parks its result; see [`FelderaDestination::wait_unpaused`].
    pub(super) stopping: Arc<AtomicBool>,
    /// Results the stop could not answer; see [`ParkedResults`].
    pub(super) parked: Arc<ParkedResults>,
    /// Set when the pipeline stops; the worker then shuts etl down.
    terminate: Sender<bool>,
    me: Weak<Self>,
}

impl SharedCdcPipeline {
    pub(super) fn new(key: PublicationKey, registry: Arc<ConnectorRegistry>) -> Arc<Self> {
        let (terminate, _) = channel(false);
        Arc::new_cyclic(|me| Self {
            key,
            first: Mutex::new(None),
            registry,
            joined: Mutex::new(Vec::new()),
            running: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            stopping: Arc::new(AtomicBool::new(false)),
            parked: Arc::new(ParkedResults::default()),
            terminate,
            me: me.clone(),
        })
    }

    /// The etl pipeline id, which names the replication slots.
    pub(super) fn pipeline_id(&self) -> u64 {
        self.key.pipeline_id()
    }

    /// The settings of the first member, which configure etl.
    fn config(&self) -> PostgresCdcReaderConfig {
        self.first
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, config)| config.clone())
            .expect("etl starts only once a member has joined")
    }

    /// Registers `member`. Until the controller has opened every connector,
    /// the member waits for [`SharedConnectorState::initial_connect_done`];
    /// a connector that joins afterwards starts etl now.
    ///
    /// The connection user, TLS, `run_source_migrations`, `batch` and
    /// `memory_backpressure` configure the whole etl pipeline, so a member
    /// whose values differ from the first member's is refused.
    pub(super) fn join(&self, member: Member) -> Result<(), ControllerError> {
        let name = member.inner.endpoint_name.clone();
        {
            let mut first = self.first.lock().unwrap();
            if let Some((first, config)) = first.as_ref()
                && let Some(field) = shared_settings_differ(config, &member.inner.config)
            {
                return Err(ControllerError::invalid_transport_configuration(
                    &name,
                    &format!(
                        "connectors '{first}' and '{name}' read publication '{}' of '{}' with \
                         different '{field}' settings. They share one replication slot, so \
                         they must use the same settings. Make '{field}' the same on both, or \
                         give one of them its own publication",
                        self.key.publication(),
                        self.key.connection()
                    ),
                ));
            }
            if first.is_none() {
                *first = Some((name.clone(), member.inner.config.clone()));
            }
        }
        if self.running.load(Ordering::SeqCst) {
            return Err(ControllerError::input_transport_error(
                &name,
                true,
                anyhow!(
                    "the replication pipeline of publication '{}' is already running for \
                     another connector; connecting a connector to it while it runs is not \
                     supported yet. Stop the pipeline, then start it with this connector",
                    self.key.publication()
                ),
            ));
        }
        self.joined.lock().unwrap().push(member);
        if self.registry.is_initial_connect_done() {
            self.start()
        } else {
            Ok(())
        }
    }

    /// Starts etl for the members that joined, on a thread of its own, and
    /// returns once etl runs or has failed to start.
    fn start(&self) -> Result<(), ControllerError> {
        let members = std::mem::take(&mut *self.joined.lock().unwrap());
        let Some(first) = members.first() else {
            return Ok(());
        };
        let label = first.inner.endpoint_name.clone();
        self.running.store(true, Ordering::SeqCst);
        let (init_status_sender, init_status_receiver) =
            tokio::sync::oneshot::channel::<Result<(), ControllerError>>();
        let this = self
            .me
            .upgrade()
            .expect("the pipeline is alive while a member starts it");
        let thread_label = label.clone();
        thread::Builder::new()
            .name(format!("pgcdc-{label}"))
            .stack_size(RUNTIME_THREAD_STACK_SIZE)
            .spawn(move || {
                // etl spawns its workers onto the ambient runtime. A runtime of
                // the connector's own keeps them off the process-wide one and,
                // above all, lets a stop end them: shutting it down once the
                // worker task has returned cancels whatever etl left running
                // and closes its replication connections, so PostgreSQL
                // releases the slots however the stop went (#7170, #7201).
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(RUNTIME_WORKER_THREADS)
                    .thread_name(format!("pgcdc-rt-{thread_label}"))
                    .thread_stack_size(RUNTIME_THREAD_STACK_SIZE)
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ =
                            init_status_sender.send(Err(ControllerError::input_transport_error(
                                &thread_label,
                                true,
                                anyhow!("failed to start the connector's tokio runtime: {e}"),
                            )));
                        return;
                    }
                };
                block_on_then_shut_down(runtime, this.worker_task(members, init_status_sender));
            })
            .expect("failed to create Postgres CDC input connector thread");

        init_status_receiver.blocking_recv().map_err(|_| {
            ControllerError::input_transport_error(
                &label,
                true,
                anyhow!("worker thread terminated unexpectedly during initialization"),
            )
        })?
    }

    async fn worker_task(
        self: Arc<Self>,
        members: Vec<Member>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
    ) {
        let label = members[0].inner.endpoint_name.clone();
        self.worker_task_inner(members, init_status_sender).await;
        debug!("postgres_cdc {label}: worker task terminated");
    }

    async fn worker_task_inner(
        self: Arc<Self>,
        members: Vec<Member>,
        init_status_sender: tokio::sync::oneshot::Sender<Result<(), ControllerError>>,
    ) {
        let label = members[0].inner.endpoint_name.clone();
        let config = self.config();
        let pg_conn = match parse_pg_uri(&config.uri, &config.tls, &label) {
            Ok(conn) => conn,
            Err(e) => {
                let _ =
                    init_status_sender.send(Err(ControllerError::invalid_transport_configuration(
                        &label,
                        &format!("failed to parse Postgres URI: {e}"),
                    )));
                return;
            }
        };

        // A `source_table` the publication does not carry is a configuration
        // error, and the most likely one after an unqualified name came to
        // mean `public`. Say so now, with the publication's table list, rather
        // than ingest nothing until the stall monitor speaks.
        let catalog = connect_pg(&config.uri, &config.tls, &label).await;
        match &catalog {
            Ok(client) => match publication_tables(client, &config.publication).await {
                Ok(tables) => {
                    for member in &members {
                        if let Some(problem) = source_table_missing_from(
                            &member.inner.config.source_table,
                            &config.publication,
                            &tables,
                        ) {
                            let _ = init_status_sender.send(Err(
                                ControllerError::input_transport_error(
                                    &member.inner.endpoint_name,
                                    true,
                                    anyhow!(problem),
                                ),
                            ));
                            return;
                        }
                    }
                }
                Err(e) => warn_publication_unlisted(&label, &config.publication, &e),
            },
            Err(e) => warn_publication_unlisted(&label, &config.publication, e),
        }

        // Skipped source migrations leave the event trigger to an
        // administrator, so check it before touching the state store: a
        // never-bootstrapped database then stays untouched. With migrations
        // enabled the trigger is checked after etl installs it. A failed
        // catalog connection is left for the state store to report.
        if let (false, Ok(client)) = (config.run_source_migrations, &catalog)
            && let Err(e) = verify_ddl_event_trigger(client).await
        {
            let _ = init_status_sender
                .send(Err(ControllerError::input_transport_error(&label, true, e)));
            return;
        }

        let pipeline_config = PipelineConfig {
            id: self.pipeline_id(),
            publication_name: config.publication.clone(),
            pg_connection: pg_conn.clone(),
            // etl stores its replication state in the source database itself, so
            // the state store reuses the source connection.
            store_pg_connection: None,
            // etl names its replication slots after the pipeline id alone, and
            // failover slots need PostgreSQL 17 or later, so the connector
            // keeps the default slot configuration.
            replication_slot: Default::default(),
            batch: batch_config_from(&config.batch),
            table_error_retry_delay_ms: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_DELAY_MS,
            table_error_retry_max_attempts: PipelineConfig::DEFAULT_TABLE_ERROR_RETRY_MAX_ATTEMPTS,
            max_table_sync_workers: PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(memory_backpressure_config_from(&config.memory_backpressure)),
            table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
            table_sync_monitor_refresh_interval_ms:
                PipelineConfig::DEFAULT_TABLE_SYNC_MONITOR_REFRESH_INTERVAL_MS,
            invalidated_slot_behavior: InvalidatedSlotBehavior::default(),
            // The initial copy needs etl's schema helpers. When migrations are
            // disabled, an administrator must have installed them beforehand.
            run_source_migrations: config.run_source_migrations,
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
        let store = match PostgresStore::new(self.pipeline_id(), pg_conn).await {
            Ok(store) => store,
            Err(e) => {
                let message = format!("failed to initialize PostgresStore: {e}");
                let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                    &label,
                    true,
                    with_setup_hint(anyhow!(e).context(message), config.run_source_migrations),
                )));
                return;
            }
        };

        for member in &members {
            let inner = &member.inner;
            match reconcile_table_state(
                &store,
                &inner.config.source_table,
                member.redo_snapshot,
                member.resumed_copy,
                &inner.endpoint_name,
            )
            .await
            {
                Ok(reconciled) => {
                    if let Some(lsn) = member.resumed_copy.lsn().max(reconciled.sync_done_lsn) {
                        inner.copy.note_sync_done_lsn(lsn);
                    }
                    inner.copy.set_copy_open(reconciled.copy_outstanding);
                }
                Err(e) => {
                    let message = format!(
                        "could not reconcile the replication state of table '{}' before \
                         starting: {e}. The connector keeps its resume position in schema 'etl' \
                         of the source database and must read and update it at startup; fix the \
                         reported problem, then restart the pipeline",
                        inner.config.source_table
                    );
                    let _ = init_status_sender.send(Err(ControllerError::input_transport_error(
                        &inner.endpoint_name,
                        true,
                        with_setup_hint(anyhow!(e).context(message), config.run_source_migrations),
                    )));
                    return;
                }
            }
        }

        let mut inners = Vec::with_capacity(members.len());
        let mut state_rxs = Vec::with_capacity(members.len());
        let mut destinations = Vec::with_capacity(members.len());
        let mut monitors = Vec::with_capacity(members.len());
        for member in members {
            let inner = member.inner;
            destinations.push(Arc::new(FelderaDestination {
                input_stream: Arc::new(Mutex::new(member.input_stream)),
                queue: Arc::clone(&inner.queue),
                source_table: inner.config.source_table.clone(),
                endpoint_name: inner.endpoint_name.clone(),
                feldera_required_columns: member.feldera_required_columns,
                // One predicate decides deferral for the destination and the
                // reader alike, so neither can answer etl on its own terms.
                defer_acks: inner.defers_acks(),
                pipeline_state_rx: member.state_rx.clone(),
                copy: Arc::clone(&inner.copy),
                consumer: inner.consumer.clone(),
                stopping: Arc::clone(&self.stopping),
            }));
            monitors.push(TableErrorMonitor {
                endpoint_name: inner.endpoint_name.clone(),
                run_source_migrations: config.run_source_migrations,
                consumer: inner.consumer.clone(),
                store: store.clone(),
                source_table: inner.config.source_table.clone(),
                publication: config.publication.clone(),
                copy: Arc::clone(&inner.copy),
                pipeline_state_rx: member.state_rx.clone(),
            });
            state_rxs.push(member.state_rx);
            inners.push(inner);
        }
        let destination = DispatchingDestination::new(
            destinations,
            Arc::clone(&self.stopping),
            Arc::clone(&self.parked),
        );

        // Stopping the pipeline needs the pipeline itself, so a stop that
        // arrives while `start` runs takes effect once it returns: the
        // termination branch below fires at once.
        let mut pipeline = Pipeline::new(pipeline_config, store, destination);

        let started = match pipeline.start().await {
            Ok(()) if config.run_source_migrations => match &catalog {
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
                    config.run_source_migrations,
                ))
            }
        };
        drop(catalog);
        match started {
            Ok(()) => {
                for inner in &inners {
                    info!(
                        "postgres_cdc {}: etl pipeline started for publication '{}', table '{}'",
                        &inner.endpoint_name, &config.publication, &inner.config.source_table,
                    );
                }
                let _ = init_status_sender.send(Ok(()));
            }
            Err(e) => {
                let _ = init_status_sender
                    .send(Err(ControllerError::input_transport_error(&label, true, e)));
                self.shutdown_etl_pipeline(Some(&pipeline), &inners);
                return;
            }
        }

        // Spawn the completion watcher background tasks if tracking is
        // available. The watchers and the channels were created in `new()`;
        // we take the channels here after etl has started so startup
        // failures do not leave a task behind. The watcher is cloned rather
        // than taken: the reader keeps its own to read the frontier after the
        // watcher stops, so a flush that races the stop can still tell the
        // durable writes from the rest.
        let mut completion_handles = inners
            .iter()
            .map(|inner| inner.spawn_completion_watcher(&self.parked))
            .collect::<Vec<_>>();

        // Run the pipeline alongside a watcher for non-retriable per-table
        // errors. etl marks a table errored (e.g. on a source schema change)
        // without failing the whole pipeline, so `pipeline.wait` would block
        // forever while the input silently stalls; the watcher reports such an
        // error so the controller fails the endpoint instead.
        let mut terminate_rx = self.terminate.subscribe();
        let pipeline_wait = pipeline.wait();
        tokio::pin!(pipeline_wait);
        // Drop the watch guards inside the blocks: holding one while awaiting
        // `pipeline_wait` blocks etl tasks that read the same watch in
        // `wait_unpaused`, once a reader's `Drop` queues another write.
        let member_terminated = select_all(state_rxs.iter_mut().map(|rx| {
            Box::pin(async move {
                let _ = rx
                    .wait_for(|state| state == &PipelineState::Terminated)
                    .await;
            }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        }));
        let monitor_spoke = select_all(monitors.into_iter().map(|monitor| {
            Box::pin(monitor.run()) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        }));
        let (pipeline_result, report_error) = select! {
            result = &mut pipeline_wait => (result, true),
            _ = async {
                let _ = terminate_rx.wait_for(|terminate| *terminate).await;
            } => {
                debug!("postgres_cdc {label}: the pipeline stops; shutting down etl pipeline");
                self.shutdown_etl_pipeline(Some(&pipeline), &inners);
                stop_completion_watchers(&mut completion_handles, &label).await;
                (self.await_etl_stop(&mut pipeline_wait, &label).await, false)
            }
            _ = member_terminated => {
                debug!(
                    "postgres_cdc {label}: received termination command; shutting down etl pipeline"
                );
                self.shutdown_etl_pipeline(Some(&pipeline), &inners);
                stop_completion_watchers(&mut completion_handles, &label).await;
                (self.await_etl_stop(&mut pipeline_wait, &label).await, false)
            }
            _ = monitor_spoke => {
                self.shutdown_etl_pipeline(Some(&pipeline), &inners);
                stop_completion_watchers(&mut completion_handles, &label).await;
                (self.await_etl_stop(&mut pipeline_wait, &label).await, false)
            }
        };

        if let Err(e) = pipeline_result {
            let terminated = state_rxs
                .iter()
                .any(|rx| *rx.borrow() == PipelineState::Terminated);
            if report_error && !terminated {
                error!("postgres_cdc {label}: etl pipeline error: {e}");
                let text = e.to_string();
                for inner in &inners {
                    inner.consumer.error(
                        true,
                        with_setup_hint(anyhow!(text.clone()), config.run_source_migrations),
                        None,
                    );
                }
            } else {
                debug!("postgres_cdc {label}: etl pipeline stopped during shutdown: {e}");
            }
        }

        stop_completion_watchers(&mut completion_handles, &label).await;

        self.retired.store(true, Ordering::SeqCst);
        self.running.store(false, Ordering::SeqCst);
        for inner in &inners {
            inner.consumer.eoi();
        }
    }

    /// Wait for etl to stop after [`Self::shutdown_etl_pipeline`], for at
    /// most [`ETL_STOP_GRACE`].
    ///
    /// The stop reaches this wait once the completion watchers have stopped,
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
        label: &str,
    ) -> EtlResult<()> {
        match tokio::time::timeout(ETL_STOP_GRACE, pipeline_wait).await {
            Ok(result) => result,
            Err(_) => {
                STOPPED_PAST_GRACE
                    .lock()
                    .unwrap()
                    .insert(self.pipeline_id());
                warn!(
                    "postgres_cdc {label}: etl did not stop within {ETL_STOP_GRACE:?}, so its \
                     replication connections are closed by force. PostgreSQL offers the changes \
                     they had not confirmed again on the next start"
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
    fn shutdown_etl_pipeline(
        &self,
        pipeline: Option<&EtlPipeline>,
        inners: &[Arc<PostgresCdcInputInner>],
    ) {
        if let Some(pipeline) = pipeline {
            pipeline.shutdown();
        }
        // etl is asked to stop first; the flag then turns away the writes it
        // still issues before it sees the request.
        self.stopping.store(true, Ordering::SeqCst);
        for inner in inners {
            inner.park_terminal();
        }
    }
}

impl SharedConnectorState for SharedCdcPipeline {
    fn initial_connect_done(&self) -> Result<(), ControllerError> {
        self.start()
    }

    fn shutdown(&self) {
        self.terminate.send_replace(true);
    }

    /// A pipeline that stops, or has stopped, takes no new member: a
    /// connector that connects again gets a pipeline of its own.
    fn accepts_members(&self) -> bool {
        !self.stopping.load(Ordering::SeqCst) && !self.retired.load(Ordering::SeqCst)
    }
}

/// The first of the settings that configure the whole etl pipeline on which
/// `a` and `b` differ, by the name the connector configuration uses.
pub(super) fn shared_settings_differ(
    a: &PostgresCdcReaderConfig,
    b: &PostgresCdcReaderConfig,
) -> Option<&'static str> {
    let user = |config: &PostgresCdcReaderConfig| {
        Url::parse(&config.uri)
            .map(|url| url.username().to_string())
            .unwrap_or_else(|_| config.uri.clone())
    };
    if user(a) != user(b) {
        Some("uri user")
    } else if a.tls != b.tls {
        Some("tls")
    } else if a.run_source_migrations != b.run_source_migrations {
        Some("run_source_migrations")
    } else if a.batch != b.batch {
        Some("batch")
    } else if a.memory_backpressure != b.memory_backpressure {
        Some("memory_backpressure")
    } else {
        None
    }
}

fn warn_publication_unlisted(label: &str, publication: &str, error: &anyhow::Error) {
    warn!(
        "postgres_cdc {label}: could not list the tables of publication '{publication}' \
         ({error}); a source table the publication does not carry is reported by the stall \
         monitor instead"
    );
}

/// Stop every completion watcher, each within [`WATCHER_SHUTDOWN_TIMEOUT`].
async fn stop_completion_watchers(watchers: &mut [Option<CompletionWatcher>], label: &str) {
    for watcher in watchers {
        stop_completion_watcher(watcher, label).await;
    }
}
