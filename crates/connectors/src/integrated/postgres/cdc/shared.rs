//! One etl pipeline, and so one replication slot, for every connector whose
//! [`PublicationKey`] is the same.

use super::*;
use crate::integrated::{ConnectorRegistry, SharedConnectorState};
use futures::future::select_all;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use tokio::sync::oneshot;

/// Answers a connector that waits for etl to start.
type InitSender = oneshot::Sender<Result<(), ControllerError>>;

/// How long a pipeline waits after a member left before it starts etl again
/// for the members that remain, in milliseconds. Members that leave one after
/// another, as at a pipeline stop that the registry did not announce, then
/// cost one restart. Tests set it to 0.
pub(crate) static RESTART_GRACE_MS: AtomicU64 = AtomicU64::new(1000);

/// How many times etl started, by pipeline id. Tests read it to tell a
/// restart from none.
pub(crate) static ETL_STARTS: Mutex<BTreeMap<u64, usize>> = Mutex::new(BTreeMap::new());

/// The etl pipeline the connectors of one [`PublicationKey`] share.
///
/// The connectors register through [`Self::join`], and etl starts once the
/// controller has opened every connector. A connector that joins later, or
/// one that leaves, makes the worker stop etl and start it again for the
/// members that remain. Each connector holds an `Arc` to the pipeline; the
/// controller's [`ConnectorRegistry`] holds it weakly.
pub(super) struct SharedCdcPipeline {
    key: PublicationKey,
    /// The first connector that joined, by endpoint name, and its settings,
    /// which configure the whole etl pipeline; every later member must agree.
    first: Mutex<Option<(String, PostgresCdcReaderConfig)>>,
    registry: Arc<ConnectorRegistry>,
    membership: Mutex<Membership>,
    /// etl is being stopped. A write that starts afterwards queues nothing
    /// and parks its result; see [`FelderaDestination::wait_unpaused`].
    pub(super) stopping: Arc<AtomicBool>,
    /// Results the stop could not answer; see [`ParkedResults`].
    pub(super) parked: Arc<ParkedResults>,
    /// Set when the pipeline stops; the worker then shuts etl down for good.
    terminate: Sender<bool>,
    /// Bumped when a connector joins while etl runs; the worker then starts
    /// etl again with it.
    joined_late: Sender<u64>,
    me: Weak<Self>,
}

/// Who is in the pipeline, and whether the worker runs.
#[derive(Default)]
struct Membership {
    /// Connectors that joined and wait for etl to start, or to start again.
    joined: Vec<Member>,
    /// Connectors that joined while etl ran, answered with the result of the
    /// next start.
    late_joiners: Vec<InitSender>,
    /// The worker runs etl for the members it took.
    running: bool,
    /// The worker has returned for good; the pipeline takes no more members.
    retired: bool,
}

/// A member whose connector has reached etl at least once.
struct RunningMember {
    inner: Arc<PostgresCdcInputInner>,
    destination: Arc<FelderaDestination>,
    state_rx: Receiver<PipelineState>,
    /// Answers etl once the step that holds a write is durable. Lives as long
    /// as the member, across restarts of etl: the member's channel to it can
    /// be taken only once.
    watcher: Option<CompletionWatcher>,
    /// The table error monitor reported the member's error: it stops reading,
    /// like a disconnected member.
    errored: AtomicBool,
}

/// Why one run of etl ended.
enum EndReason {
    /// The Feldera pipeline stops.
    PipelineStop,
    /// etl stopped on its own.
    Etl(EtlResult<()>),
    /// A connector was disconnected, or joined while etl ran, or its table
    /// error monitor spoke. etl starts again for the members that remain.
    MembershipChanged,
}

impl SharedCdcPipeline {
    pub(super) fn new(key: PublicationKey, registry: Arc<ConnectorRegistry>) -> Arc<Self> {
        let (terminate, _) = channel(false);
        let (joined_late, _) = channel(0);
        Arc::new_cyclic(|me| Self {
            key,
            first: Mutex::new(None),
            registry,
            membership: Mutex::new(Membership::default()),
            stopping: Arc::new(AtomicBool::new(false)),
            parked: Arc::new(ParkedResults::default()),
            terminate,
            joined_late,
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
    /// the member waits for [`SharedConnectorState::initial_connect_done`].
    /// A connector that joins afterwards starts etl, or makes the running
    /// etl stop and start again with it, and returns once etl runs.
    ///
    /// The connection user, TLS, `run_source_migrations`, `batch` and
    /// `memory_backpressure` configure the whole etl pipeline, so a member
    /// whose values differ from the first member's is refused.
    pub(super) fn join(&self, mut member: Member) -> Result<(), ControllerError> {
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
        let late = {
            let mut membership = self.membership.lock().unwrap();
            if membership.running {
                // The Feldera table of a connector that joins a running
                // pipeline is empty, so its copy is read again.
                member.redo_snapshot = true;
                let (sender, receiver) = oneshot::channel();
                membership.late_joiners.push(sender);
                membership.joined.push(member);
                Some(receiver)
            } else {
                membership.joined.push(member);
                None
            }
        };
        match late {
            Some(receiver) => {
                self.joined_late.send_modify(|n| *n += 1);
                receiver.blocking_recv().map_err(|_| {
                    ControllerError::input_transport_error(
                        &name,
                        true,
                        anyhow!(
                            "the replication pipeline of publication '{}' stopped before this \
                             connector joined it; connect the connector again",
                            self.key.publication()
                        ),
                    )
                })?
            }
            None if self.registry.is_initial_connect_done() => self.start(),
            None => Ok(()),
        }
    }

    /// Starts etl for the members that joined, on a thread of its own, and
    /// returns once etl runs or has failed to start.
    fn start(&self) -> Result<(), ControllerError> {
        let members = {
            let mut membership = self.membership.lock().unwrap();
            if membership.joined.is_empty() || membership.running {
                return Ok(());
            }
            membership.running = true;
            std::mem::take(&mut membership.joined)
        };
        let label = members[0].inner.endpoint_name.clone();
        let (init_status_sender, init_status_receiver) = oneshot::channel();
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
                        this.retire();
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

    /// Takes the connectors that joined while etl ran, with the senders that
    /// wait for the next start.
    fn take_late_joiners(&self) -> (Vec<Member>, Vec<InitSender>) {
        let mut membership = self.membership.lock().unwrap();
        (
            std::mem::take(&mut membership.joined),
            std::mem::take(&mut membership.late_joiners),
        )
    }

    /// The worker returns for good. Connectors that joined too late to be
    /// started are told so, and the registry hands the next connector a new
    /// pipeline.
    fn retire(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        let (_, late_joiners) = {
            let mut membership = self.membership.lock().unwrap();
            membership.running = false;
            membership.retired = true;
            (
                std::mem::take(&mut membership.joined),
                std::mem::take(&mut membership.late_joiners),
            )
        };
        // The receivers see the dropped senders and report the stop.
        drop(late_joiners);
    }

    /// Runs etl for `members`, and again after each change of membership,
    /// until the pipeline stops or no member is left.
    async fn worker_task(self: Arc<Self>, members: Vec<Member>, init: InitSender) {
        let label = members[0].inner.endpoint_name.clone();
        let config = self.config();
        let mut running: Vec<RunningMember> = Vec::new();
        let mut newcomers = members;
        let mut init = vec![init];
        let mut terminate_rx = self.terminate.subscribe();
        loop {
            let Ok(reason) = self
                .run_etl(&config, &label, &mut running, newcomers, init)
                .await
            else {
                // The start failed, and every connector that waited for it
                // has the error. Members that streamed before are left
                // without etl, so they fail too.
                for member in &running {
                    member.inner.consumer.error(
                        true,
                        anyhow!(
                            "the replication pipeline of publication '{}' failed to start again \
                             after a connector joined or left; see the error of that connector",
                            config.publication
                        ),
                        None,
                    );
                }
                break;
            };
            match reason {
                EndReason::PipelineStop => break,
                EndReason::Etl(Err(e)) => {
                    error!("postgres_cdc {label}: etl pipeline error: {e}");
                    let text = e.to_string();
                    for member in &running {
                        member.inner.consumer.error(
                            true,
                            with_setup_hint(anyhow!(text.clone()), config.run_source_migrations),
                            None,
                        );
                    }
                    break;
                }
                EndReason::Etl(Ok(())) => {
                    debug!("postgres_cdc {label}: etl pipeline ended");
                    break;
                }
                EndReason::MembershipChanged => {
                    if !self
                        .after_leave(&mut running, &label, &mut terminate_rx)
                        .await
                    {
                        break;
                    }
                }
            }
            (newcomers, init) = self.take_late_joiners();
            if running.is_empty() && newcomers.is_empty() {
                break;
            }
            info!(
                "postgres_cdc {label}: starting etl again for publication '{}' with {} connectors",
                config.publication,
                running.len() + newcomers.len()
            );
        }
        for member in &mut running {
            stop_completion_watcher(&mut member.watcher, &label).await;
        }
        self.retire();
        for member in &running {
            member.inner.consumer.eoi();
        }
        debug!("postgres_cdc {label}: worker task terminated");
    }

    /// Drops the members that left, waits for the restart grace, and drops
    /// those that left meanwhile: members that leave one after another cost
    /// one restart. Returns false when the pipeline stops instead.
    async fn after_leave(
        &self,
        running: &mut Vec<RunningMember>,
        label: &str,
        terminate_rx: &mut Receiver<bool>,
    ) -> bool {
        self.drop_terminated(running, label).await;
        if !self.wait_grace(terminate_rx).await {
            return false;
        }
        self.drop_terminated(running, label).await;
        true
    }

    /// Waits for the restart grace, unless the pipeline stops meanwhile.
    async fn wait_grace(&self, terminate_rx: &mut Receiver<bool>) -> bool {
        let grace = Duration::from_millis(RESTART_GRACE_MS.load(Ordering::Relaxed));
        select! {
            _ = tokio::time::sleep(grace) => true,
            _ = async { let _ = terminate_rx.wait_for(|terminate| *terminate).await; } => false,
        }
    }

    /// Removes the members whose connectors were disconnected, or whose
    /// monitor spoke, from `running`, and ends their input.
    async fn drop_terminated(&self, running: &mut Vec<RunningMember>, label: &str) {
        let mut index = 0;
        while index < running.len() {
            if running[index].leaves() {
                let mut member = running.remove(index);
                stop_completion_watcher(&mut member.watcher, label).await;
                member.inner.consumer.eoi();
                info!(
                    "postgres_cdc {}: left publication '{}'",
                    member.inner.endpoint_name,
                    self.key.publication()
                );
            } else {
                index += 1;
            }
        }
    }

    /// One run of etl: starts it for the running members and the newcomers,
    /// answers `init` with the start result, and waits until it ends.
    ///
    /// Returns `Err` when the start failed; `init` then holds the error.
    async fn run_etl(
        &self,
        config: &PostgresCdcReaderConfig,
        label: &str,
        running: &mut Vec<RunningMember>,
        newcomers: Vec<Member>,
        init: Vec<InitSender>,
    ) -> Result<EndReason, ()> {
        let pg_conn = match parse_pg_uri(&config.uri, &config.tls, label) {
            Ok(conn) => conn,
            Err(e) => {
                return fail_init(
                    init,
                    label,
                    &config.publication,
                    ControllerError::invalid_transport_configuration(
                        label,
                        &format!("failed to parse Postgres URI: {e}"),
                    ),
                );
            }
        };

        // A `source_table` the publication does not carry is a configuration
        // error, and the most likely one after an unqualified name came to
        // mean `public`. Say so now, with the publication's table list, rather
        // than ingest nothing until the stall monitor speaks.
        let catalog = connect_pg(&config.uri, &config.tls, label).await;
        match &catalog {
            Ok(client) => match publication_tables(client, &config.publication).await {
                Ok(tables) => {
                    for member in &newcomers {
                        if let Some(problem) = source_table_missing_from(
                            &member.inner.config.source_table,
                            &config.publication,
                            &tables,
                        ) {
                            return fail_init(
                                init,
                                label,
                                &config.publication,
                                ControllerError::input_transport_error(
                                    &member.inner.endpoint_name,
                                    true,
                                    anyhow!(problem),
                                ),
                            );
                        }
                    }
                }
                Err(e) => warn_publication_unlisted(label, &config.publication, &e),
            },
            Err(e) => warn_publication_unlisted(label, &config.publication, e),
        }

        // Skipped source migrations leave the event trigger to an
        // administrator, so check it before touching the state store: a
        // never-bootstrapped database then stays untouched. With migrations
        // enabled the trigger is checked after etl installs it. A failed
        // catalog connection is left for the state store to report.
        if let (false, Ok(client)) = (config.run_source_migrations, &catalog)
            && let Err(e) = verify_ddl_event_trigger(client).await
        {
            return fail_init(
                init,
                label,
                &config.publication,
                ControllerError::input_transport_error(label, true, e),
            );
        }

        // The publication costs one apply slot plus one transient slot per
        // copy worker. Size the copy workers to the slots that are free, so
        // that a publication with more tables than slots still starts; a
        // database with no slot left for the apply worker is reported now.
        let mut max_table_sync_workers = PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS;
        if let Ok(client) = &catalog {
            match replication_slots_in_use(client, self.pipeline_id()).await {
                Ok((max_slots, other_slots)) => match copy_worker_budget(max_slots, other_slots) {
                    Some(workers) => max_table_sync_workers = workers,
                    None => {
                        return fail_init(
                            init,
                            label,
                            &config.publication,
                            ControllerError::input_transport_error(
                                label,
                                true,
                                anyhow!(
                                    "cannot read publication '{}': the database allows \
                                     {max_slots} replication slots (max_replication_slots) \
                                     and all {max_slots} are in use. Free a slot (see \
                                     pg_replication_slots), or raise max_replication_slots \
                                     and restart PostgreSQL",
                                    config.publication
                                ),
                            ),
                        );
                    }
                },
                Err(e) => warn!(
                    "postgres_cdc {label}: could not read the replication slots in use ({e}); \
                     the copy runs with {max_table_sync_workers} workers"
                ),
            }
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
            max_table_sync_workers,
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
                return fail_init(
                    init,
                    label,
                    &config.publication,
                    ControllerError::input_transport_error(
                        label,
                        true,
                        with_setup_hint(anyhow!(e).context(message), config.run_source_migrations),
                    ),
                );
            }
        };

        // etl reads the table states from the store's cache, which the
        // reconciliation below fills for newcomers; a restart with no
        // newcomer would otherwise start every table from scratch.
        if let Err(e) = store.load_cache().await {
            let message = format!("failed to load the replication state: {e}");
            return fail_init(
                init,
                label,
                &config.publication,
                ControllerError::input_transport_error(
                    label,
                    true,
                    with_setup_hint(anyhow!(e).context(message), config.run_source_migrations),
                ),
            );
        }

        // Members that streamed before keep their copy barrier as it is: the
        // circuit holds what they pushed, and the restarted etl reads the
        // changes it had not confirmed again.
        for member in &newcomers {
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
                    return fail_init(
                        init,
                        label,
                        &config.publication,
                        ControllerError::input_transport_error(
                            &inner.endpoint_name,
                            true,
                            with_setup_hint(
                                anyhow!(e).context(message),
                                config.run_source_migrations,
                            ),
                        ),
                    );
                }
            }
        }

        for member in newcomers {
            let inner = member.inner;
            let destination = Arc::new(FelderaDestination {
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
                high_water: Arc::new(Mutex::new(HighWater::default())),
            });
            running.push(RunningMember {
                inner,
                destination,
                state_rx: member.state_rx,
                watcher: None,
                errored: AtomicBool::new(false),
            });
        }
        let monitors = running
            .iter()
            .map(|member| TableErrorMonitor {
                endpoint_name: member.inner.endpoint_name.clone(),
                run_source_migrations: config.run_source_migrations,
                consumer: member.inner.consumer.clone(),
                store: store.clone(),
                source_table: member.inner.config.source_table.clone(),
                publication: config.publication.clone(),
                copy: Arc::clone(&member.inner.copy),
                pipeline_state_rx: member.state_rx.clone(),
            })
            .collect::<Vec<_>>();
        let destination = DispatchingDestination::new(
            running
                .iter()
                .map(|member| Arc::clone(&member.destination))
                .collect(),
            Arc::clone(&self.stopping),
            Arc::clone(&self.parked),
        );

        // A new run of etl takes writes again.
        self.stopping.store(false, Ordering::SeqCst);

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
        if let Err(e) = started {
            self.shutdown_etl_pipeline(Some(&pipeline), running);
            return fail_init(
                init,
                label,
                &config.publication,
                ControllerError::input_transport_error(label, true, e),
            );
        }
        *ETL_STARTS
            .lock()
            .unwrap()
            .entry(self.pipeline_id())
            .or_default() += 1;
        for member in running.iter() {
            info!(
                "postgres_cdc {}: etl pipeline started for publication '{}', table '{}'",
                &member.inner.endpoint_name, &config.publication, &member.inner.config.source_table,
            );
        }
        for sender in init {
            let _ = sender.send(Ok(()));
        }

        // Spawn the completion watcher of each new member, if tracking is
        // available. The channel is taken here, after etl has started, so
        // startup failures do not leave a task behind. The watcher is cloned
        // rather than taken: the reader keeps its own to read the frontier
        // after the watcher stops, so a flush that races the stop can still
        // tell the durable writes from the rest.
        for member in running.iter_mut() {
            if member.watcher.is_none() {
                member.watcher = member.inner.spawn_completion_watcher(&self.parked);
            }
        }

        // Run the pipeline alongside a watcher for non-retriable per-table
        // errors. etl marks a table errored (e.g. on a source schema change)
        // without failing the whole pipeline, so `pipeline.wait` would block
        // forever while the input silently stalls; the watcher reports such an
        // error so the controller fails the endpoint instead.
        let mut terminate_rx = self.terminate.subscribe();
        let mut joined_late_rx = self.joined_late.subscribe();
        joined_late_rx.mark_unchanged();
        let pipeline_wait = pipeline.wait();
        tokio::pin!(pipeline_wait);
        // Drop the watch guards inside the blocks: holding one while awaiting
        // `pipeline_wait` blocks etl tasks that read the same watch in
        // `wait_unpaused`, once a reader's `Drop` queues another write.
        let mut member_terminated = Box::pin(select_all(running.iter().map(|member| {
            let mut rx = member.state_rx.clone();
            Box::pin(async move {
                let _ = rx
                    .wait_for(|state| state == &PipelineState::Terminated)
                    .await;
            }) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        })));
        let mut monitor_spoke = Box::pin(select_all(monitors.into_iter().map(|monitor| {
            Box::pin(monitor.run()) as std::pin::Pin<Box<dyn Future<Output = ()> + Send>>
        })));
        let mut joined_late = Box::pin(async move {
            let _ = joined_late_rx.changed().await;
        });
        // A change of membership stops etl once no member that stays has a
        // copy under way: etl would copy such a table again from scratch,
        // into a circuit that already holds the rows it pushed. The members
        // that leave are excluded, and their rows are dropped meanwhile. The
        // monitors keep running, because they are what records a completed
        // copy as closed.
        let mut membership_changed = false;
        let mut monitor_done = false;
        let mut copy_wait_logged = false;
        let reason = loop {
            select! {
                result = &mut pipeline_wait => break EndReason::Etl(result),
                _ = async {
                    let _ = terminate_rx.wait_for(|terminate| *terminate).await;
                } => break EndReason::PipelineStop,
                _ = &mut member_terminated, if !membership_changed => {
                    debug!("postgres_cdc {label}: a connector was disconnected");
                    membership_changed = true;
                }
                _ = &mut joined_late, if !membership_changed => {
                    debug!("postgres_cdc {label}: a connector joined");
                    membership_changed = true;
                }
                (_, index, _) = &mut monitor_spoke, if !monitor_done => {
                    running[index].errored.store(true, Ordering::SeqCst);
                    monitor_done = true;
                    membership_changed = true;
                }
                _ = tokio::time::sleep(Duration::from_millis(50)), if membership_changed => {
                    let copying: Vec<&str> = running
                        .iter()
                        .filter(|member| !member.leaves())
                        .filter(|member| member.inner.copy.copy_open())
                        .map(|member| member.inner.endpoint_name.as_str())
                        .collect();
                    if copying.is_empty() {
                        break EndReason::MembershipChanged;
                    }
                    if !copy_wait_logged {
                        copy_wait_logged = true;
                        info!(
                            "postgres_cdc {label}: the members change; etl starts again once the \
                             copy of the tables of connectors {} completes, since etl reads an \
                             interrupted copy again from the start",
                            copying.join(", ")
                        );
                    }
                }
            }
        };
        // A stop disconnects the members one by one; the stop comes first.
        let reason = if *terminate_rx.borrow() {
            EndReason::PipelineStop
        } else {
            reason
        };
        match &reason {
            EndReason::Etl(_) => {
                // etl is gone; the flag turns away what it still issues.
                self.stopping.store(true, Ordering::SeqCst);
                for member in running.iter() {
                    member.inner.park_terminal();
                }
            }
            EndReason::PipelineStop => {
                debug!("postgres_cdc {label}: the pipeline stops; shutting down etl pipeline");
                self.shutdown_etl_pipeline(Some(&pipeline), running);
                let result = self.await_etl_stop(&mut pipeline_wait, label).await;
                debug_etl_stop(label, result);
            }
            EndReason::MembershipChanged => {
                debug!("postgres_cdc {label}: the members change; shutting down etl pipeline");
                self.shutdown_etl_pipeline(Some(&pipeline), running);
                let result = self.await_etl_stop(&mut pipeline_wait, label).await;
                debug_etl_stop(label, result);
            }
        }
        Ok(reason)
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
    fn shutdown_etl_pipeline(&self, pipeline: Option<&EtlPipeline>, running: &[RunningMember]) {
        if let Some(pipeline) = pipeline {
            pipeline.shutdown();
        }
        // etl is asked to stop first; the flag then turns away the writes it
        // still issues before it sees the request.
        self.stopping.store(true, Ordering::SeqCst);
        for member in running {
            member.inner.park_terminal();
        }
    }
}

impl RunningMember {
    /// Whether the member leaves the pipeline: its connector was
    /// disconnected, or its monitor reported an error.
    fn leaves(&self) -> bool {
        *self.state_rx.borrow() == PipelineState::Terminated || self.errored.load(Ordering::SeqCst)
    }
}

impl SharedConnectorState for SharedCdcPipeline {
    fn initial_connect_done(&self) -> Result<(), ControllerError> {
        self.start()
    }

    fn shutdown(&self) {
        self.terminate.send_replace(true);
    }

    /// A pipeline whose worker has returned takes no new member: a connector
    /// that connects again gets a pipeline of its own.
    fn accepts_members(&self) -> bool {
        !self.membership.lock().unwrap().retired
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

/// Answers every connector that waited for the start with `error`: the first
/// gets the original, the others its text.
fn fail_init(
    init: Vec<InitSender>,
    label: &str,
    publication: &str,
    error: ControllerError,
) -> Result<EndReason, ()> {
    let text = error.to_string();
    let mut init = init.into_iter();
    if let Some(first) = init.next() {
        let _ = first.send(Err(error));
    }
    for sender in init {
        let _ = sender.send(Err(ControllerError::input_transport_error(
            label,
            true,
            anyhow!(
                "the replication pipeline of publication '{publication}' failed to start: {text}"
            ),
        )));
    }
    Err(())
}

/// `max_replication_slots` of the source and the number of slots in use that
/// are not this pipeline's (etl names its slots after the pipeline id).
async fn replication_slots_in_use(
    client: &tokio_postgres::Client,
    pipeline_id: u64,
) -> AnyResult<(u64, u64)> {
    let max_slots: String = client
        .query_one("SHOW max_replication_slots", &[])
        .await?
        .get(0);
    let max_slots = max_slots.parse::<u64>()?;
    let id = pipeline_id.to_string();
    let other_slots = client
        .query("SELECT slot_name::text FROM pg_replication_slots", &[])
        .await?
        .iter()
        .map(|row| row.get::<_, String>(0))
        .filter(|name| !name.split('_').any(|token| token == id))
        .count() as u64;
    Ok((max_slots, other_slots))
}

/// How many copy workers fit next to the apply slot in the slots that
/// `other_slots` leave free out of `max_slots`, at most etl's default and at
/// least one, or `None` when not even the apply slot fits.
pub(super) fn copy_worker_budget(max_slots: u64, other_slots: u64) -> Option<u16> {
    let free = max_slots
        .checked_sub(other_slots)
        .filter(|free| *free > 0)?;
    let workers = (free - 1).clamp(1, u64::from(PipelineConfig::DEFAULT_MAX_TABLE_SYNC_WORKERS));
    Some(workers as u16)
}

fn warn_publication_unlisted(label: &str, publication: &str, error: &anyhow::Error) {
    warn!(
        "postgres_cdc {label}: could not list the tables of publication '{publication}' \
         ({error}); a source table the publication does not carry is reported by the stall \
         monitor instead"
    );
}

fn debug_etl_stop(label: &str, result: EtlResult<()>) {
    if let Err(e) = result {
        debug!("postgres_cdc {label}: etl pipeline stopped during shutdown: {e}");
    }
}
