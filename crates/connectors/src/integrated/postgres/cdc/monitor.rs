use super::*;

/// Monitor that turns etl table-state failures into Feldera connector failures.
pub(super) struct TableErrorMonitor {
    pub(super) endpoint_name: String,
    pub(super) run_source_migrations: bool,
    pub(super) consumer: Box<dyn InputConsumer>,
    pub(super) store: PostgresStore,
    pub(super) source_table: String,
    pub(super) publication: String,
    /// Checkpoint barrier of the initial copy, which the monitor only reads:
    /// the copy it counts buffers for is the one sign of progress a copy under
    /// way gives.
    pub(super) copy: Arc<CopyBarrier>,
    /// Errors recorded while the pipeline is not running wait until it runs
    /// again; the shutdown errors among them are the connector's own doing
    /// and the next start rolls them back.
    pub(super) pipeline_state_rx: Receiver<PipelineState>,
}

/// How long etl may leave this connector's table where it stands, while having
/// no other table of the publication left to copy, before the connector says
/// so. What it says depends on [`stall_verdict`]: a table etl never picked up,
/// most often one the publication does not carry, is a fatal error, because
/// without it the connector would ingest nothing, report nothing, and hold its
/// checkpoint barrier for the life of the pipeline; a table etl is working on
/// only draws a warning, repeated at this interval.
pub(super) const TABLE_STALL_REPORT: Duration = Duration::from_secs(600);

/// Whether the source table standing still says nothing about etl, so that
/// the stall clock has to start over:
///
/// - the Feldera pipeline is not running, which holds every copy where it
///   stands;
/// - the table sync of the source table has completed, after which the
///   connector streams changes and a table nobody writes to is meant to be
///   quiet;
/// - etl has another table of the publication still to copy. It copies a
///   bounded number of tables at a time, so the source table may be
///   waiting its turn, and a copy under way records no state transition to
///   tell a busy etl from a stuck one.
pub(super) fn nothing_to_report(
    pipeline_state: PipelineState,
    states: &BTreeMap<TableId, TableState>,
    target: Option<TableId>,
) -> bool {
    if pipeline_state != PipelineState::Running {
        return true;
    }
    if target.is_some_and(|table_id| states.get(&table_id).is_some_and(sync_completed)) {
        return true;
    }
    states.iter().any(|(table_id, state)| {
        Some(*table_id) != target
            && !sync_completed(state)
            && !matches!(state, TableState::Errored { .. })
    })
}

/// What a source table that has not moved in [`TABLE_STALL_REPORT`] means.
#[derive(Debug, PartialEq)]
pub(super) enum StallVerdict {
    /// etl has not taken the table up, so nothing will change: report it.
    NeverPickedUp,
    /// etl is working on the table and waiting on the source. Creating the
    /// copy slot waits for every transaction that was running when the copy
    /// started, so a long transaction on the source holds a healthy copy here
    /// for as long as it runs. Warn and keep waiting.
    WaitingOnSource,
}

/// Decide what a stalled source table means from etl's state for it.
pub(super) fn stall_verdict(state: Option<&TableState>) -> StallVerdict {
    match state {
        None | Some(TableState::Init) => StallVerdict::NeverPickedUp,
        Some(_) => StallVerdict::WaitingOnSource,
    }
}

/// What the monitor watches to tell whether etl is moving the source table
/// forward.
// etl's `TableState` implements `PartialEq` and not `Eq`.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct TableProgress {
    /// The etl table the source table resolves to, once etl has stored its
    /// schema.
    pub(super) table_id: Option<TableId>,
    /// Its replication state, which etl records at each step of the copy.
    pub(super) state: Option<TableState>,
    /// Snapshot buffers the copy has queued and flushed into the circuit.
    /// Between the start and the end of a copy etl records no state
    /// transition, so for a copy under way these are the only signs of progress
    /// there are. Once etl has handed over its last batch and waits on the
    /// terminal barrier, only the flushed count still moves, and a slow circuit
    /// may take longer than the stall report to drain it.
    pub(super) snapshot_buffers_queued: usize,
    pub(super) snapshot_buffers_flushed: usize,
}

impl TableErrorMonitor {
    /// Surface a non-retriable replication error on the source table as a
    /// fatal endpoint error, and report a table etl leaves untouched.
    ///
    /// When etl cannot continue replicating a table — most notably after a
    /// source schema change, which Feldera does not support — it marks the
    /// table `Errored` and stops applying its changes but keeps the pipeline
    /// running. From Feldera's side the input would then silently stall. This
    /// polls etl's state store and, on an error whose retry policy is `NoRetry`
    /// or `ManualRetry` (i.e. it will not clear on its own), reports it via the
    /// consumer so the controller fails the endpoint. `TimedRetry` errors are
    /// left alone: etl retries them and, once retries are exhausted, the apply
    /// worker propagates the failure through `pipeline.wait`.
    pub(super) async fn run(self) {
        const POLL_INTERVAL: Duration = Duration::from_millis(500);

        let mut last_progress: Option<TableProgress> = None;
        let mut progress_seen_at = Instant::now();

        loop {
            tokio::time::sleep(POLL_INTERVAL).await;

            // Read the states before resolving the source table, and keep the
            // states of the other tables: etl marks a table `Errored` from the
            // same worker that would have stored its schema, and it stores the
            // schema only after it has created the copy slot and read the
            // schema from the source, so a table that errors on the way there
            // has a state and no name.
            let Some(states) = self.read_table_states().await else {
                // The poll learned nothing about etl, so it says nothing about
                // whether etl is moving.
                progress_seen_at = Instant::now();
                continue;
            };

            let target = match target_table_id(&self.store, &self.source_table).await {
                Ok(target) => target,
                Err(e) => {
                    debug!(
                        "postgres_cdc {}: failed to resolve the source table: {e}",
                        &self.endpoint_name
                    );
                    progress_seen_at = Instant::now();
                    continue;
                }
            };

            if states
                .keys()
                .any(|table_id| self.report_table_error(&states, *table_id, target))
            {
                return;
            }

            let progress = TableProgress {
                table_id: target,
                state: target.and_then(|table_id| states.get(&table_id).cloned()),
                snapshot_buffers_queued: self.copy.snapshot_buffers_queued(),
                snapshot_buffers_flushed: self.copy.snapshot_buffers_flushed(),
            };
            if let Some(state) = &progress.state
                && self.copy.note_sync_state_observed(state)
            {
                // The reader reports the lowered barrier on its next step, and
                // a checkpoint deferred on it stops the controller from
                // stepping on its own, so ask for that step.
                self.consumer.request_step();
            }
            if last_progress.as_ref() != Some(&progress)
                || nothing_to_report(*self.pipeline_state_rx.borrow(), &states, target)
            {
                last_progress = Some(progress);
                progress_seen_at = Instant::now();
                continue;
            }

            if progress_seen_at.elapsed() >= TABLE_STALL_REPORT {
                let state = target.and_then(|table_id| states.get(&table_id));
                match stall_verdict(state) {
                    StallVerdict::NeverPickedUp => {
                        self.report_stall(&states, target);
                        return;
                    }
                    StallVerdict::WaitingOnSource => {
                        warn!(
                            "postgres_cdc {}: etl has been working on source table '{}' for {} \
                             seconds without handing over rows; its replication state is {:?}. \
                             etl is most likely waiting for PostgreSQL to create the copy slot, \
                             which waits for every transaction that was running when the copy \
                             started: check pg_stat_activity on the source for long-running \
                             transactions",
                            &self.endpoint_name,
                            &self.source_table,
                            TABLE_STALL_REPORT.as_secs(),
                            state,
                        );
                        progress_seen_at = Instant::now();
                    }
                }
            }
        }
    }

    /// etl's state for every table of the publication, or `None` when the
    /// store cannot be read. A read that fails says nothing about etl, and the
    /// connector accuses it of nothing.
    async fn read_table_states(&self) -> Option<Arc<BTreeMap<TableId, TableState>>> {
        match self.store.get_table_states().await {
            Ok(states) => Some(states),
            Err(e) => {
                debug!(
                    "postgres_cdc {}: failed to read table replication states: {e}",
                    &self.endpoint_name
                );
                None
            }
        }
    }

    /// Report a non-retriable error on a table of the publication, and say
    /// whether one was reported. etl stops replicating an errored table but
    /// keeps the pipeline running, so from Feldera's side nothing else would
    /// say why the input stalled; a table this connector never reads is
    /// reported too, since the same publication feeds it.
    fn report_table_error(
        &self,
        states: &BTreeMap<TableId, TableState>,
        table_id: TableId,
        target: Option<TableId>,
    ) -> bool {
        let Some(TableState::Errored {
            reason,
            solution,
            retry_policy,
            ..
        }) = states.get(&table_id)
        else {
            return false;
        };

        // A timed retry clears on its own; leave it to etl.
        if matches!(retry_policy, TableRetryPolicy::TimedRetry { .. }) {
            return false;
        }

        // Report nothing while the pipeline is paused or terminated: a start
        // reconciles what a stop left behind. An error seen while the
        // pipeline runs is one etl raised mid-run, or one the start could not
        // clear; the table has stalled either way.
        if *self.pipeline_state_rx.borrow() != PipelineState::Running {
            return false;
        }

        let detail = match solution {
            Some(solution) => format!("{reason} ({solution})"),
            None => reason.clone(),
        };
        let table = if Some(table_id) == target {
            format!(
                "source table '{}' (etl table {table_id})",
                self.source_table
            )
        } else {
            format!("table {table_id}")
        };
        let error = with_setup_hint(
            anyhow!("postgres replication error on {table}: {detail}"),
            self.run_source_migrations,
        );
        error!("postgres_cdc {}: {error}", &self.endpoint_name);
        self.consumer.error(true, error, None);
        true
    }

    /// Report a source table etl never took up while having nothing else to
    /// copy. The connector would otherwise ingest nothing, report nothing, and
    /// hold its checkpoint barrier for the life of the pipeline.
    fn report_stall(&self, states: &BTreeMap<TableId, TableState>, target: Option<TableId>) {
        // A non-retriable error on another table was reported the moment it
        // appeared, so the errors left here are the ones etl still retries.
        // They are the likeliest reason etl never reached this table, and
        // while the source table is unresolved one of them may be on it under
        // a name etl never got as far as storing.
        let other_errors: Vec<String> = states
            .iter()
            .filter(|(table_id, _)| Some(**table_id) != target)
            .filter_map(|(table_id, state)| match state {
                TableState::Errored { reason, .. } => Some(format!("table {table_id}: {reason}")),
                _ => None,
            })
            .collect();
        let errors = if other_errors.is_empty() {
            String::new()
        } else {
            format!(
                ". etl reports errors on other tables of the publication: {}",
                other_errors.join("; ")
            )
        };

        let error = match target {
            Some(table_id) => {
                let state = match states.get(&table_id) {
                    Some(state) => format!("{state:?}"),
                    None => "unknown".to_string(),
                };
                anyhow!(
                    "etl has not moved source table '{}' (etl table {table_id}) forward in {} \
                     seconds, having no other table of publication '{}' left to copy. Its \
                     replication state is {state}. Check that the pipeline and Postgres server \
                     logs report no replication failure{errors}",
                    self.source_table,
                    TABLE_STALL_REPORT.as_secs(),
                    self.publication,
                )
            }
            None => anyhow!(
                "table '{}' has not appeared in the replication state of publication '{}' in {} \
                 seconds, in which etl had no other table of the publication left to copy. Check \
                 that the publication carries this table (an unqualified name refers to schema \
                 'public') and that the pipeline and Postgres server logs report no replication \
                 failure{errors}",
                self.source_table,
                self.publication,
                TABLE_STALL_REPORT.as_secs(),
            ),
        };
        error!("postgres_cdc {}: {error}", &self.endpoint_name);
        self.consumer.error(true, error, None);
    }
}

/// Typed watch receiver used by the completion watcher background task.
///
/// `Fast` waits for step completion (`total_completed_steps`); used when fault
/// tolerance is not enabled.  `Strict` waits for checkpoint completion; used
/// when fault tolerance is enabled so the replication slot only advances past
/// the last durable checkpoint, preserving at-least-once correctness for
/// stateful circuits after a crash.
#[derive(Clone)]
pub(super) enum WatcherReceiver {
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

    fn frontier(&self) -> Step {
        match self {
            Self::Fast(rx) => rx.borrow().total_completed_steps,
            Self::Strict(rx) => *rx.borrow(),
        }
    }
}

/// Background task that answers etl for a write once the completion frontier
/// passes the step whose input held it.
///
/// Each entry is `(flush_step, acks)`, where `flush_step` is the step the
/// flush that earned those acknowledgments fed. The rows are in that step
/// itself, and a frontier of `n` means the steps below `n` are durable, so the
/// frontier has to pass `flush_step` strictly.
///
/// `shutdown_rx` stops the task, which then answers the writes the frontier
/// has already passed and parks the rest, so a stop costs etl only the writes
/// that are not durable.
pub(super) async fn completion_watcher_task(
    mut watcher: WatcherReceiver,
    mut pending_rx: mpsc::UnboundedReceiver<HeldAcks>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    parked: Arc<ParkedResults>,
    endpoint_name: String,
) {
    let mut waiting: Vec<HeldAcks> = Vec::new();

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
                    Some(held) => {
                        if watcher.frontier() > held.flush_step {
                            // Already past the threshold, so answer at once.
                            for ack in held.acks {
                                ack();
                            }
                        } else {
                            waiting.push(held);
                        }
                    }
                    None => break, // Channel closed
                }
            }
            _ = &mut shutdown_rx => break,
        }
    }

    // Answer the writes the frontier has already passed, by the same rule the
    // loop uses, and judge what the channel still holds by that rule too. An
    // answer etl still sees before it stops advances its checkpoint past the
    // write, which keeps the replay after the next start short.
    answer_durable_on_stop(&mut waiting, &mut pending_rx, watcher.frontier());

    // What is left is not durable. etl has been asked to stop and abandons
    // those writes, so PostgreSQL offers their changes again on the next
    // start. Their senders are parked rather than dropped; see
    // `ParkedResults`.
    debug!(
        "postgres_cdc {endpoint_name}: completion watcher exiting with {} pending entries",
        waiting.len()
    );
    for held in waiting {
        for ack in held.acks {
            parked.park(ack);
        }
    }
}

/// Answer the writes a stop finds durable, and leave the rest waiting.
///
/// An answer sent but not yet received is as durable as one already waiting,
/// so a stop judges it by the same frontier rather than drop it. Closing the
/// channel first bounds the drain: no send can enter it afterwards.
pub(super) fn answer_durable_on_stop(
    waiting: &mut Vec<HeldAcks>,
    pending_rx: &mut mpsc::UnboundedReceiver<HeldAcks>,
    frontier: Step,
) {
    pending_rx.close();
    while let Ok(entry) = pending_rx.try_recv() {
        waiting.push(entry);
    }
    fire_completed(waiting, frontier);
}

/// Answers the writes whose rows the frontier has passed.
pub(super) fn fire_completed(waiting: &mut Vec<HeldAcks>, frontier: Step) {
    waiting.retain_mut(|held| {
        if frontier > held.flush_step {
            for ack in held.acks.drain(..) {
                ack();
            }
            false
        } else {
            true
        }
    });
}

/// Answer the writes a flush completed after the completion watcher stopped.
///
/// The watcher task closes its channel when it stops, so a flush that lands
/// while the stop drains that channel has its send refused. The frontier is
/// still readable through the watcher clone the reader keeps, so the writes it
/// has passed are answered and the rest parked, by the same rule the watcher
/// would have used.
pub(super) fn answer_durable_after_stop(
    watcher_rx: &Mutex<Option<WatcherReceiver>>,
    parked: &ParkedResults,
    flush_step: Step,
    acks: DeferredAcks,
) {
    let durable = watcher_rx
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|watcher| watcher.frontier() > flush_step);
    for ack in acks {
        if durable {
            ack();
        } else {
            parked.park(ack);
        }
    }
}

/// The completion watcher task and the signal that stops it.
pub(super) struct CompletionWatcher {
    pub(super) shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub(super) handle: tokio::task::JoinHandle<()>,
}
