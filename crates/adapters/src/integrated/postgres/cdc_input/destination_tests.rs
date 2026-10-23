//! The destination of the Postgres CDC input connector and the queue that its reader
//! flushes: a copy's terminal barrier waits for the copy's last buffer, a streamed write is
//! answered only once its last buffer reaches the circuit, and a stopped connector keeps no
//! write waiting.
//!
//! The cases drive the destination directly, with the input stream of an engine collection,
//! and need no database. The barrier tests of the CDC input module call the barrier directly,
//! so they cannot see a caller go missing; these drive the production destination and the
//! queue the reader flushes, and cross the two through the same functions the reader's Queue
//! handler uses.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use etl::schema::PgLsn;
use etl::store::TableState;
use feldera_adapterlib::format::ParseError;
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, MockInputConsumer};
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::Relation;
use serde_json::json;
use tokio::sync::watch::channel;

use super::{
    CopyBarrier, FelderaDestination, count_snapshot_buffers, is_shutdown_error,
    release_pending_answers, take_acks,
};
use crate::test::relations;
use crate::transport::InputQueue;
use crate::{PipelineState, RecordFormat};

/// The table whose input stream the destination writes to, with the columns of
/// [`relations::basic`].
pub fn relation() -> Relation {
    relations::basic("postgres_cdc_destination")
}

/// Answer a terminal barrier by raising `answered`.
pub(super) fn terminal(answered: &Arc<AtomicBool>) -> Box<dyn FnOnce() + Send> {
    let answered = Arc::clone(answered);
    Box::new(move || answered.store(true, Ordering::Release))
}

/// Whether the barrier has been answered.
pub(super) fn answered(flag: &Arc<AtomicBool>) -> bool {
    flag.load(Ordering::Acquire)
}

/// etl's `sync_done` state at `lsn`.
pub(super) fn sync_done(lsn: u64) -> TableState {
    TableState::SyncDone {
        lsn: PgLsn::from(lsn),
        table_decoding_state: None,
    }
}

/// A destination with a live queue and a running pipeline, sharing `copy`.
fn destination(engine: &dyn ConnectorTestEngine, copy: &Arc<CopyBarrier>) -> FelderaDestination {
    destination_with(engine, copy, MockInputConsumer::new(), false)
}

/// A destination sharing `consumer` with its queue, so a test can set the
/// batch size the reader's flush obeys, and deferring its answers to etl
/// when `defer_acks` says so.
fn destination_with(
    engine: &dyn ConnectorTestEngine,
    copy: &Arc<CopyBarrier>,
    consumer: MockInputConsumer,
    defer_acks: bool,
) -> FelderaDestination {
    destination_stopping(
        engine,
        copy,
        consumer,
        defer_acks,
        &Arc::new(AtomicBool::new(false)),
    )
}

/// A destination that shares `stopping` with a reader shutting etl down.
fn destination_stopping(
    engine: &dyn ConnectorTestEngine,
    copy: &Arc<CopyBarrier>,
    consumer: MockInputConsumer,
    defer_acks: bool,
    stopping: &Arc<AtomicBool>,
) -> FelderaDestination {
    let stream = engine
        .input_collection(&relation(), &InputOptions::default())
        .unwrap()
        .input_handle()
        .handle
        .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))
        .unwrap();
    let (_state_tx, pipeline_state_rx) = channel(PipelineState::Running);
    FelderaDestination {
        input_stream: Arc::new(Mutex::new(stream)),
        queue: Arc::new(InputQueue::new(Box::new(consumer.clone()))),
        source_table: "public.t".to_string(),
        endpoint_name: "cdc_in".to_string(),
        feldera_required_columns: Vec::new(),
        defer_acks,
        pipeline_state_rx,
        copy: Arc::clone(copy),
        consumer: Box::new(consumer),
        stopping: Arc::clone(stopping),
    }
}

/// One row of streamed events, serialized through the destination's own
/// deserializer as `write_events` does, so the buffer holds a record and
/// counts against the reader's batch size.
fn event_buffer(
    destination: &FelderaDestination,
    id: u32,
) -> (
    Option<Box<dyn feldera_adapterlib::format::InputBuffer>>,
    Vec<ParseError>,
) {
    let mut stream = destination.input_stream.lock().unwrap();
    let json = json!({ "id": id, "b": false, "i": null, "s": "" }).to_string();
    stream.insert(json.as_bytes(), &None).unwrap();
    (stream.take_all(), Vec::new())
}

pub fn a_queued_snapshot_buffer_holds_the_terminal_barrier_until_the_reader_flushes_it(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination(engine, &copy);
    destination.push_snapshot_buffer((None, Vec::new()), Utc::now());
    destination.push_snapshot_buffer((None, Vec::new()), Utc::now());

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        !copy.hold_terminal(terminal(&flag)),
        "two buffers sit in the queue, so the barrier is held"
    );
    assert!(!answered(&flag));

    // The reader's flush, as the Queue handler performs it.
    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(count_snapshot_buffers(&flushed), 2);
    assert!(!copy.note_buffers_flushed(count_snapshot_buffers(&flushed)));
    assert!(
        take_acks(flushed).is_empty(),
        "the copy answers etl through the barrier, never per entry"
    );
    assert!(answered(&flag), "the flush answered etl");
    assert!(copy.note_sync_state_observed(&sync_done(3)));
    assert!(!copy.copy_open());
}

// -----------------------------------------------------------------------
// Streamed write acknowledgment (#7122)
// -----------------------------------------------------------------------
//
// A write is answered only once every row of it is in the circuit. These
// drive the production destination and the queue the reader flushes, and
// cross the two through `take_acks`, as the Queue handler does.
//
// To validate them, restore the architecture the fix replaced: have
// `push_event_buffer` drop the answer into one shared list and `take_acks`
// drain that list instead of reading the entries its flush returned. Both
// tests then go red, the first because the opening flush answers a write
// whose rows are still queued, the second because the answer of a write
// that arrived mid-handler goes out with the flush that preceded it.

pub fn a_streamed_write_is_answered_only_when_its_last_buffer_reaches_the_circuit(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let consumer = MockInputConsumer::new();
    // One record per step, so the write spans three flushes. This is the
    // `max_batch_size` split of #7122, without depending on its default.
    consumer.set_max_batch_size(1);
    let destination = destination_with(engine, &copy, consumer, true);

    let flag = Arc::new(AtomicBool::new(false));
    for id in 0..2 {
        let buffer = event_buffer(&destination, id);
        destination.push_event_buffer(buffer, Utc::now(), None);
    }
    let last = event_buffer(&destination, 2);
    destination.push_event_buffer(last, Utc::now(), Some(terminal(&flag)));

    for flush in 1..=2 {
        let (_, _, flushed) = destination.queue.flush_with_aux();
        let taken = flushed.len();
        assert!(
            take_acks(flushed).is_empty(),
            "flush {flush} left rows of the write in the queue, so it must not \
             answer etl for it"
        );
        assert!(!answered(&flag));
        assert_eq!(
            taken, 1,
            "flush {flush} took more than its one buffer, so the batch size \
             stopped splitting the write and the test proves nothing"
        );
    }

    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(flushed.len(), 1);
    let mut acks = take_acks(flushed);
    assert_eq!(acks.len(), 1, "the last buffer carries the answer");
    acks.pop().unwrap()();
    assert!(answered(&flag), "every row of the write is in the circuit");
}

pub fn a_write_that_arrives_while_the_handler_runs_is_not_answered_by_it(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination_with(engine, &copy, MockInputConsumer::new(), true);

    let buffer = event_buffer(&destination, 0);
    destination.push_event_buffer(buffer, Utc::now(), None);

    // The handler's flush.
    let (_, _, flushed) = destination.queue.flush_with_aux();

    // etl hands a write over here, after the flush and before the handler
    // answers: the second race of #7122. The old handler took every stored
    // sender at this point, stamping this write with a step that holds no
    // row of it.
    let flag = Arc::new(AtomicBool::new(false));
    let buffer = event_buffer(&destination, 1);
    destination.push_event_buffer(buffer, Utc::now(), Some(terminal(&flag)));

    // The handler's answers, drawn from what its own flush returned.
    assert!(
        take_acks(flushed).is_empty(),
        "the write's rows are still in the queue"
    );
    assert!(!answered(&flag));

    let (_, _, flushed) = destination.queue.flush_with_aux();
    let mut acks = take_acks(flushed);
    assert_eq!(acks.len(), 1, "the flush that takes its rows answers it");
    acks.pop().unwrap()();
    assert!(answered(&flag));
}

pub fn a_write_queued_while_the_connector_stops_keeps_nothing_waiting(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let consumer = MockInputConsumer::new();
    let stopping = Arc::new(AtomicBool::new(false));
    let destination = destination_stopping(engine, &copy, consumer, true, &stopping);

    // etl runs alongside the shutdown, so a write can reach the queue
    // after the shutdown drained it. Leaving its answer there would keep
    // etl waiting for a step that no longer comes.
    stopping.store(true, Ordering::SeqCst);
    let flag = Arc::new(AtomicBool::new(false));
    let buffer = event_buffer(&destination, 0);
    destination.push_event_buffer(buffer, Utc::now(), Some(terminal(&flag)));

    assert!(
        !answered(&flag),
        "the write was never in a step, so it must fail rather than be answered"
    );

    // The rows stay queued. They were charged to the endpoint when they
    // were queued and only a flush credits them back, so discarding them
    // would leave the endpoint owing records no step can consume.
    let (size, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(size.records, 1, "the flush credits the queued record back");
    assert!(
        take_acks(flushed).is_empty(),
        "the stop already failed the write, so the flush answers nothing"
    );
}

pub fn a_write_that_starts_once_the_connector_stops_is_refused(engine: &dyn ConnectorTestEngine) {
    let copy = Arc::new(CopyBarrier::new());
    let stopping = Arc::new(AtomicBool::new(false));
    let destination =
        destination_stopping(engine, &copy, MockInputConsumer::new(), true, &stopping);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    assert!(runtime.block_on(destination.wait_unpaused()).is_ok());

    stopping.store(true, Ordering::SeqCst);
    let error = runtime
        .block_on(destination.wait_unpaused())
        .expect_err("a write that starts once the connector stops is refused");
    assert!(
        is_shutdown_error(&error.to_string()),
        "the next start must roll the refusal back, got: {error}"
    );
    let (size, _, _) = destination.queue.flush_with_aux();
    assert_eq!(size.records, 0, "nothing reached the queue");
}

pub fn a_stop_leaves_no_snapshot_buffer_charged_to_the_copy_barrier(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination_stopping(
        engine,
        &copy,
        MockInputConsumer::new(),
        true,
        &Arc::new(AtomicBool::new(false)),
    );

    let buffer = event_buffer(&destination, 0);
    destination.push_snapshot_buffer(buffer, Utc::now());
    release_pending_answers(&destination.queue);

    // The reader still flushes the buffer, and the barrier still counts it,
    // which is what lets a terminal barrier be answered afterwards.
    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(count_snapshot_buffers(&flushed), 1);
    copy.note_buffers_flushed(count_snapshot_buffers(&flushed));

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        copy.hold_terminal(terminal(&flag)),
        "no snapshot buffer is left pending, so a terminal barrier answers at once"
    );
    assert!(answered(&flag));
}
