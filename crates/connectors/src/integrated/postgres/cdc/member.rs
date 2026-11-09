use super::*;

/// Answers one etl write. etl keeps the constructor of its result senders
/// private, so the connector boxes the answer rather than move the sender
/// around, as the copy barrier already does.
pub(super) type Ack = Box<dyn FnOnce() + Send>;

/// Acknowledgments waiting for the step that holds their rows to become
/// durable.
pub(super) type DeferredAcks = Vec<Ack>;

/// Acknowledgments and the step whose input holds their rows.
///
/// The step and the frontier are both step counts, so naming this one keeps a
/// call site from passing one where the other belongs.
pub(super) struct HeldAcks {
    /// The step the flush that earned these acknowledgments fed.
    pub(super) flush_step: Step,
    pub(super) acks: DeferredAcks,
}

pub(super) struct PostgresCdcInputReader {
    sender: Sender<PipelineState>,
    inner: Arc<PostgresCdcInputInner>,
    /// Keeps the shared pipeline registered while this connector reads.
    _shared: Arc<SharedCdcPipeline>,
}

/// One connector of a [`SharedCdcPipeline`]: what the reader learned at
/// `open` and the pipeline needs to feed its Feldera table.
pub(super) struct Member {
    pub(super) inner: Arc<PostgresCdcInputInner>,
    pub(super) input_stream: Box<dyn DeCollectionStream>,
    /// Non-nullable columns of the Feldera table, in canonical form. Each of
    /// them must exist in the PostgreSQL table.
    pub(super) feldera_required_columns: Vec<String>,
    /// The reader's pipeline state: `Paused`, `Running` or `Terminated`.
    pub(super) state_rx: Receiver<PipelineState>,
    pub(super) redo_snapshot: bool,
    pub(super) resumed_copy: ResumedCopy,
}

impl PostgresCdcInputReader {
    pub(super) fn new(
        endpoint: &Arc<PostgresCdcInputInner>,
        shared: &Arc<SharedCdcPipeline>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        // The connector reports a barrier for every step until a complete copy
        // of the table is in the circuit, so no checkpoint and no suspended
        // state can hold a partial copy. Resuming from one therefore needs no
        // redo. Fault tolerance without any checkpoint is the case #6121
        // describes: the circuit starts empty while etl may have recorded the
        // table sync as complete, so the copy has to be read again. A restart
        // without fault tolerance keeps resuming from the replication slot
        // alone.
        let redo_snapshot = resume_info.is_none() && endpoint.strict;
        let resumed_copy = ResumedCopy::of(resume_info.as_ref());
        let (sender, state_rx) = channel(PipelineState::Paused);

        let input_stream = input_handle
            .handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))?;

        let feldera_required_columns: Vec<String> = input_handle
            .schema
            .fields
            .iter()
            .filter(|f| !f.columntype.nullable)
            .map(|f| f.name.name())
            .collect();

        shared.join(Member {
            inner: endpoint.clone(),
            input_stream,
            feldera_required_columns,
            state_rx,
            redo_snapshot,
            resumed_copy,
        })?;

        Ok(Self {
            sender,
            inner: endpoint.clone(),
            _shared: Arc::clone(shared),
        })
    }
}

impl InputReader for PostgresCdcInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        if matches!(command, InputReaderCommand::Replay { .. }) {
            panic!(
                "replay command is not supported by PostgresCdcInputReader; this is a bug, please report it to Feldera developers: https://github.com/feldera/feldera/issues/"
            );
        }

        match command.as_nonft().unwrap() {
            NonFtInputReaderCommand::Queue => {
                // Flush queue to circuit, collecting timestamps for watermarks.
                let (buffer_size, _hasher, flushed) = self.inner.queue.flush_with_aux();

                let watermarks: Vec<Watermark> = flushed
                    .iter()
                    .map(|(ts, _)| Watermark::new(*ts, None))
                    .collect();
                // The flush is what puts the rows in the circuit, so it is
                // also what answers a held copy barrier.
                let snapshot_buffers = count_snapshot_buffers(&flushed);
                let barrier_held = self.inner.copy.note_buffers_flushed(snapshot_buffers);

                let copy_open = self.inner.copy.copy_open();
                // A pending checkpoint stops the controller from stepping on
                // the checkpoint timer, so with etl's copy barrier held and
                // only queued buffers left, ask for the step that clears it.
                // Queued buffers ask for a step of their own, so this is a
                // backstop.
                if copy_open && barrier_held {
                    self.inner.consumer.request_step();
                }
                let resume = resume_for(
                    copy_open,
                    self.inner.pipeline_id,
                    self.inner.copy.sync_done_lsn(),
                );

                // Report data to controller with resume metadata (must be
                // called exactly once per Queue command).
                self.inner
                    .consumer
                    .extended(buffer_size, Some(resume), watermarks);

                // The flush answers the writes it completed, and only those.
                let acks = take_acks(flushed);
                if !acks.is_empty() {
                    self.inner.acknowledge_after_step(acks);
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

/// etl result senders the connector keeps alive without answering, once it is
/// stopping.
///
/// etl's workers leave their waits when the shutdown request reaches them,
/// and with draining off the apply loop abandons the result it has pending.
/// A sender dropped before then is an error instead, which etl's table-sync
/// worker persists as a manual retry that only a rollback at the next start
/// clears. Parked senders drop with the connector, after etl has stopped.
#[derive(Default)]
pub(super) struct ParkedResults(Mutex<Vec<Box<dyn Any + Send>>>);

impl ParkedResults {
    pub(super) fn park(&self, result: impl Any + Send) {
        self.0.lock().unwrap().push(Box::new(result));
    }
}

/// The checkpoint barrier the connector holds while a copy of the source table
/// is on its way into the circuit.
///
/// etl issues a terminal empty `write_table_rows` once every copy worker of an
/// attempt has finished, and records the copy as finished only if that call
/// answers `Durable`. The connector therefore answers each batch of copy rows
/// with `Accepted` and holds the terminal call until the buffers it queued have
/// been flushed into the circuit. Until that happens the reader reports
/// [`Resume::Barrier`], so neither a checkpoint nor a suspended state can hold
/// a partial copy (#6121).
pub(super) struct CopyBarrier {
    state: Mutex<CopyBarrierState>,
    /// Snapshot buffers queued since the connector started, never reset. etl
    /// records no state transition between the start and the end of a copy, so
    /// for a copy under way this and `flushed` are the only signs of progress
    /// [`TableProgress`] has.
    queued: AtomicUsize,
    /// Snapshot buffers flushed into the circuit since the connector started,
    /// never reset. Once etl has handed over its last batch and waits on the
    /// terminal barrier, `queued` stands still and this is what still moves.
    flushed: AtomicUsize,
    /// The `sync_done` LSN etl has recorded for the source table, as
    /// [`TableErrorMonitor`] last saw it; zero until etl records one. etl
    /// records it only after the connector answered the copy's terminal
    /// barrier, so any value seen here belongs to a copy the circuit already
    /// holds, and the reader writes it into every step's resume metadata. A
    /// start that resumes from a checkpoint compares it with etl's current
    /// LSN: a newer one means etl completed a copy after that checkpoint,
    /// whose rows are in neither the checkpoint nor the events etl replays,
    /// so the copy has to be read again.
    sync_done_lsn: AtomicU64,
}

/// The barrier proper, which the destination and the reader move in step.
pub(super) struct CopyBarrierState {
    /// A copy of the source table is under way, or its rows have not all
    /// reached the circuit.
    copy_open: bool,
    /// The circuit holds the copy, but etl has not yet been seen to record it
    /// as `sync_done`. The reader keeps reporting a barrier until then, so
    /// every resumable step after a copy carries that copy's LSN in its
    /// resume metadata; see [`CopyBarrier::sync_done_lsn`].
    awaiting_sync_record: bool,
    /// Snapshot buffers queued and not yet flushed into the circuit. Streamed
    /// events share the queue, so "the queue is empty" is the wrong test.
    pending: usize,
    /// etl's terminal copy barrier, held until `pending` reaches zero. Calling
    /// it answers etl with `Durable`.
    terminal: Option<Ack>,
}

impl CopyBarrier {
    /// A barrier that is up. What etl has stored about the source table decides
    /// whether it stays up, and the connector reads that only once it has
    /// connected; see [`CopyBarrier::set_copy_open`].
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(CopyBarrierState {
                copy_open: true,
                awaiting_sync_record: false,
                pending: 0,
                terminal: None,
            }),
            queued: AtomicUsize::new(0),
            flushed: AtomicUsize::new(0),
            sync_done_lsn: AtomicU64::new(0),
        }
    }

    /// Record the `sync_done` LSN etl holds for the source table.
    pub(super) fn note_sync_done_lsn(&self, lsn: u64) {
        self.sync_done_lsn.fetch_max(lsn, Ordering::AcqRel);
    }

    /// The `sync_done` LSN the monitor last saw, if etl has recorded one.
    pub(super) fn sync_done_lsn(&self) -> Option<u64> {
        match self.sync_done_lsn.load(Ordering::Acquire) {
            0 => None,
            lsn => Some(lsn),
        }
    }

    /// Whether a checkpoint would hold an incomplete copy, which is what the
    /// reader reports a barrier for.
    pub(super) fn copy_open(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.copy_open || state.awaiting_sync_record
    }

    /// Record what etl's stored state says about the source table, before etl
    /// runs. A table whose sync etl has completed needs no barrier: etl copies
    /// it no more, so no terminal barrier would ever arrive to lower one.
    pub(super) fn set_copy_open(&self, open: bool) {
        self.state.lock().unwrap().copy_open = open;
    }

    /// Note a copy of the source table, and say whether the barrier went back
    /// up. etl hands over no row of an attempt before it announces the attempt
    /// through [`Destination::drop_table_for_copy`] or the first batch of it.
    pub(super) fn note_copy_batch(&self) -> bool {
        !std::mem::replace(&mut self.state.lock().unwrap().copy_open, true)
    }

    /// Account for one snapshot buffer, before it is queued, so the reader
    /// never counts a flush it has not seen queued.
    pub(super) fn note_buffer_queued(&self) {
        self.queued.fetch_add(1, Ordering::AcqRel);
        self.state.lock().unwrap().pending += 1;
    }

    /// Hold etl's terminal copy barrier until every queued snapshot buffer has
    /// reached the circuit, answering it at once when none is left, as for an
    /// empty table. `confirm` answers etl with `Durable`.
    ///
    /// Reports whether the barrier came down here, away from the reader's step.
    /// The reader reports the lowered barrier only on its next step, and
    /// nothing else makes that step happen, so the caller has to ask for it.
    pub(super) fn hold_terminal(&self, confirm: Ack) -> bool {
        let mut state = self.state.lock().unwrap();
        if let Some(stale) = state.terminal.take() {
            // etl awaits one copy barrier before it issues the next, so a
            // second one is a bug. Dropping the first quietly would make etl
            // record a shutdown error and the next start roll the table back
            // and re-copy it, which would hide the bug behind routine
            // behaviour. Say so, then let the stale barrier drop.
            error!(
                "postgres_cdc: a second etl copy barrier arrived while one was held; \
                 this is a bug, please report it to Feldera developers"
            );
            drop(stale);
        }
        if state.pending > 0 {
            state.terminal = Some(confirm);
            return false;
        }
        state.copy_open = false;
        state.awaiting_sync_record = true;
        drop(state);
        confirm();
        true
    }

    /// Take a held terminal barrier away when the connector stops, for the
    /// caller to park.
    ///
    /// etl's copy worker stops waiting on the barrier as soon as the shutdown
    /// reaches it, so there is nothing left to answer, and the copy is not
    /// recorded as finished: the next start reads the table again. What this
    /// guards is the flush that races the stop. A buffer the reader flushes
    /// after the stop must not find a barrier to answer, or it would take the
    /// barrier down and record as finished a copy that etl has abandoned; with
    /// the barrier gone, the copy stays open.
    pub(super) fn take_terminal(&self) -> Option<Ack> {
        self.state.lock().unwrap().terminal.take()
    }

    /// Account for snapshot buffers the reader has flushed into the circuit,
    /// answering a held terminal barrier once the last of them lands, and say
    /// whether a terminal barrier is still held.
    ///
    /// Buffers of an attempt etl abandoned may still sit in the queue when the
    /// next attempt's barrier arrives. Both counts cover every attempt, so
    /// waiting for `pending` to reach zero waits for more than the current
    /// attempt, never for less.
    pub(super) fn note_buffers_flushed(&self, buffers: usize) -> bool {
        self.flushed.fetch_add(buffers, Ordering::AcqRel);
        let mut state = self.state.lock().unwrap();
        state.pending = state.pending.saturating_sub(buffers);
        if state.pending > 0 {
            return state.terminal.is_some();
        }
        let Some(confirm) = state.terminal.take() else {
            return false;
        };
        state.copy_open = false;
        state.awaiting_sync_record = true;
        drop(state);
        confirm();
        false
    }

    /// Take note of etl's state for the source table, as the monitor sees it,
    /// and say whether that lowered the barrier. A `sync_done` state records
    /// the copy's LSN. Either `sync_done` or `ready` shows etl has recorded a
    /// copy the circuit holds, which is what the barrier waited for after
    /// answering the terminal barrier; `ready` needs no LSN, because etl
    /// reaches it only once its persisted progress, which under fault
    /// tolerance advances only past checkpointed steps, has passed the LSN.
    pub(super) fn note_sync_state_observed(&self, state: &TableState) -> bool {
        match state {
            TableState::SyncDone { lsn, .. } => self.note_sync_done_lsn(u64::from(*lsn)),
            TableState::Ready => {}
            _ => return false,
        }
        let mut state = self.state.lock().unwrap();
        std::mem::replace(&mut state.awaiting_sync_record, false)
    }

    /// Snapshot buffers queued so far, which the monitor watches as the sign
    /// that a copy in progress is making progress.
    pub(super) fn snapshot_buffers_queued(&self) -> usize {
        self.queued.load(Ordering::Acquire)
    }

    /// Snapshot buffers flushed into the circuit so far, the sign of progress
    /// a copy draining into a slow circuit gives after etl has handed over its
    /// last batch.
    pub(super) fn snapshot_buffers_flushed(&self) -> usize {
        self.flushed.load(Ordering::Acquire)
    }
}

/// What [`FelderaDestination::wait_unpaused`] lets a write do.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission {
    /// The pipeline runs: queue the rows.
    Admit,
    /// The connector is stopping: queue nothing and park the result.
    Stopping,
}

/// How a streamed write ends, once its rows are in the queue.
///
/// The choice is what ties etl's answer to the flush that carries the write's
/// rows into the circuit (#7122); see [`write_ending`].
#[derive(Debug, PartialEq, Eq)]
pub(super) enum WriteEnding {
    /// Answer etl at once and queue nothing more.
    ///
    /// Either the write queued nothing at all, because it was an empty
    /// durability barrier or carried only another table's events, or the
    /// consumer reports no step to wait for. Deferring an empty write would
    /// risk wedging etl, which pauses its intake while a streaming write is in
    /// flight: with nothing in the queue, no step has to follow that would
    /// answer it.
    AnswerUnqueued,

    /// Queue the write's last rows, then answer etl at once.
    ///
    /// The consumer reports no step to wait for, so the write is as durable as
    /// it will ever be.
    QueueTailAndAnswer,

    /// Close the write with an entry carrying its acknowledgment.
    ///
    /// The entry holds the write's last rows, or none at all when the 2 MiB
    /// push emptied the stream; either way it sits behind every other entry of
    /// the write, which is what ties the answer to the flush that completes the
    /// write rather than to whichever flush happens to run next. An entry
    /// without rows still reaches the circuit: the queue asks the controller
    /// for a step when it takes one, and drains it with the flush that popped
    /// the entry before it.
    QueueClosingAck,
}

/// Decide how a streamed write ends.
///
/// `queued` counts the buffers the write already pushed at the 2 MiB mark,
/// `tail_rows` says whether rows or parse errors are still held back, and
/// `defer_acks` says whether the consumer reports a step an answer can wait on.
pub(super) fn write_ending(queued: usize, tail_rows: bool, defer_acks: bool) -> WriteEnding {
    if queued == 0 && !tail_rows {
        return WriteEnding::AnswerUnqueued;
    }
    if !defer_acks {
        return if tail_rows {
            WriteEnding::QueueTailAndAnswer
        } else {
            WriteEnding::AnswerUnqueued
        };
    }
    WriteEnding::QueueClosingAck
}

/// One etl `write_events` answer, owed by every member that got rows of the
/// write. The last share answers etl; a write with no share is answered at
/// once.
pub(super) struct AckFanIn {
    pending: AtomicUsize,
    answer: Mutex<Option<Ack>>,
}

impl AckFanIn {
    /// `answer` answers etl; `shares` is the number of members that owe a
    /// share, each taken with [`Self::share`].
    pub(super) fn new(answer: Ack, shares: usize) -> Arc<Self> {
        let fan_in = Arc::new(Self {
            pending: AtomicUsize::new(shares),
            answer: Mutex::new(Some(answer)),
        });
        if shares == 0 {
            fan_in.answer();
        }
        fan_in
    }

    /// One member's share of the answer; see [`QueueAux::ack`].
    pub(super) fn share(self: &Arc<Self>) -> Ack {
        let fan_in = Arc::clone(self);
        Box::new(move || {
            if fan_in.pending.fetch_sub(1, Ordering::AcqRel) == 1 {
                fan_in.answer();
            }
        })
    }

    fn answer(&self) {
        if let Some(answer) = self.answer.lock().unwrap().take() {
            answer();
        }
    }
}

/// Per-entry auxiliary data on the input queue.
#[derive(Default)]
pub(super) struct QueueAux {
    /// The entry holds rows of the initial table copy.
    pub(super) snapshot: bool,
    /// Answers etl for the streamed write this entry ends, once the step that
    /// holds the write is durable. The entry sits behind every other entry of
    /// its write, and the queue pops entries whole and in order, so the flush
    /// that returns this acknowledgment is the flush that put the write's last
    /// row into the circuit (#7122).
    pub(super) ack: Option<Ack>,
}

impl Debug for QueueAux {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueAux")
            .field("snapshot", &self.snapshot)
            .field("ack", &self.ack.as_ref().map(|_| ".."))
            .finish()
    }
}

/// Snapshot buffers among the entries a flush put into the circuit.
pub(super) fn count_snapshot_buffers(flushed: &[(chrono::DateTime<Utc>, QueueAux)]) -> usize {
    flushed.iter().filter(|(_, aux)| aux.snapshot).count()
}

/// The acknowledgments a flush earned, in the order their writes were queued.
///
/// A write is here only once every one of its entries has reached the circuit,
/// so these are exactly the writes this step may answer.
pub(super) fn take_acks(flushed: Vec<(chrono::DateTime<Utc>, QueueAux)>) -> DeferredAcks {
    flushed.into_iter().filter_map(|(_, aux)| aux.ack).collect()
}
