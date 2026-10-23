//! The clock connector that fills the `now` table: ticks driven by the wall clock with an
//! offset, ticks driven over HTTP, the timezone offset, checkpoints and restarts, and an engine
//! without a wall clock.

use crate::test::engine_test;
use std::fs::create_dir;
use std::thread::sleep;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Int64Type, TimestampMicrosecondType};
use feldera_adapterlib::test::{Capability, default_timeout_ms, wait};
use feldera_storage::tokio::TOKIO;
use feldera_types::config::PipelineConfig;
use feldera_types::transport::clock::ClockTimezoneOffset;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::Controller;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit, query};
use crate::transport::clock::ClockReader;

/// The anchor of `NOW()` in the cases driven over HTTP.
const HTTP_ANCHOR: &str = "2023-11-14T22:13:20Z";

/// Returns `rfc3339` in milliseconds since the epoch.
fn millis(rfc3339: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .timestamp_millis()
}

/// Returns the number of ticks that the circuit received, and the last one in milliseconds
/// since the epoch, or `None` if the pipeline does not answer queries yet.
fn ticks(controller: &Controller) -> Option<(usize, Option<i64>)> {
    let batches = query(
        controller,
        "select count(*) as ticks, max(now) as last_now from now_ticks",
    )
    .ok()?;
    let batch = batches.first()?;
    let ticks = batch.column(0).as_primitive::<Int64Type>().value(0) as usize;
    let last = batch.column(1).as_primitive::<TimestampMicrosecondType>();
    let last = (!last.is_null(0)).then(|| last.value(0) / 1000);
    Some((ticks, last))
}

/// Returns the number of ticks that the circuit received.
fn tick_count(controller: &Controller) -> usize {
    ticks(controller).map_or(0, |(count, _)| count)
}

/// Returns the number of ticks that the circuit received before `millis`, in milliseconds
/// since the epoch.
fn ticks_before(controller: &Controller, millis: i64) -> usize {
    query(controller, "select now from now_ticks")
        .unwrap()
        .iter()
        .map(|batch| {
            batch
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .flatten()
                .filter(|micros| *micros < millis * 1000)
                .count()
        })
        .sum()
}

/// Waits until the circuit holds every tick that the clock ingested. The clock is the only
/// input, so each input record is one tick.
fn wait_for_ingested_ticks(controller: &Controller) {
    let caught_up = wait(
        || tick_count(controller) as u64 == controller.status().num_total_input_records(),
        default_timeout_ms(),
    );
    assert!(
        caught_up.is_ok(),
        "the circuit holds {} ticks, but the clock ingested {}",
        tick_count(controller),
        controller.status().num_total_input_records()
    );
}

/// Waits for the circuit to receive `expected` ticks, then for the count to settle, and
/// returns it.
///
/// A loaded machine can take longer than any fixed sleep, so the case waits for its lower
/// bound. The settle time gives a tick past `expected` the time to arrive, so that the case
/// checks its upper bound against the final count.
fn settled_ticks(controller: &Controller, expected: usize) -> usize {
    // A timeout is not an error here: the caller's assertion reports the shortfall.
    let _ = wait(|| tick_count(controller) >= expected, default_timeout_ms());
    sleep(Duration::from_secs(2));
    tick_count(controller)
}

/// Waits until the last tick that the circuit received is `expected`, in milliseconds since
/// the epoch.
fn wait_for_clock_timestamp(controller: &Controller, expected: i64) {
    assert!(
        wait(
            || ticks(controller).and_then(|(_, last)| last) == Some(expected),
            default_timeout_ms()
        )
        .is_ok(),
        "the circuit never received NOW() = {expected}; the last tick is {:?}",
        ticks(controller)
    );
}

/// Returns the clock connector of the pipeline.
fn clock_reader(controller: &Controller) -> std::sync::Arc<ClockReader> {
    controller
        .get_input_endpoint("now")
        .expect("the pipeline has a clock connector")
        .as_any()
        .downcast::<ClockReader>()
        .expect("the `now` endpoint is a ClockReader")
}

/// Advances the clock of the pipeline by `delta_ms`, or by its resolution for `None`, and
/// returns the new `NOW()`.
fn advance(controller: &Controller, delta_ms: Option<u64>) -> anyhow::Result<i64> {
    let reader = clock_reader(controller);
    TOKIO.block_on(reader.advance(delta_ms))
}

/// Returns a pipeline configuration of one worker with storage in `storage`, fault tolerance,
/// and the global properties in `extra`.
fn config(storage: &std::path::Path, workers: usize, extra: Value) -> PipelineConfig {
    let mut config = json!({
        "name": "test",
        "workers": workers,
        "storage_config": { "path": storage },
        "fault_tolerance": {},
        "inputs": {},
    });
    for (key, value) in extra.as_object().unwrap() {
        config[key] = value.clone();
    }
    serde_json::from_value(config).unwrap()
}

/// Returns a new storage directory in `tempdir`.
fn storage(tempdir: &TempDir) -> std::path::PathBuf {
    let storage = tempdir.path().join("storage");
    create_dir(&storage).unwrap();
    storage
}

/// Starts a controller of the clock circuit with `config`.
fn controller(
    engine: &dyn PipelineTestEngine,
    config: &PipelineConfig,
    name: &'static str,
) -> Controller {
    engine
        .controller(
            &TestCircuit::Clock,
            config,
            Box::new(move |error, _| panic!("{name}: error: {error}")),
        )
        .unwrap()
}

/// While the pipeline runs, the wall clock fills `now` about once a second. An engine without a
/// wall clock connects no clock, so `now` stays empty.
#[engine_test]
pub fn now_follows_the_wall_clock_of_the_engine(engine: &dyn PipelineTestEngine) {
    let config: PipelineConfig =
        serde_json::from_value(json!({ "name": "test", "workers": 4, "inputs": {} })).unwrap();
    let pipeline = controller(engine, &config, "wall clock");
    pipeline.start();
    if engine.has(Capability::WallClock) {
        assert!(
            wait(|| tick_count(&pipeline) >= 2, default_timeout_ms()).is_ok(),
            "the wall clock did not tick"
        );
    } else {
        assert!(
            pipeline.get_input_endpoint("now").is_none(),
            "an engine without a wall clock has a clock connector"
        );
        sleep(Duration::from_secs(3));
        assert_eq!(
            tick_count(&pipeline),
            0,
            "`now` received ticks without a wall clock"
        );
    }
    pipeline.stop().unwrap();
}

/// The wall clock ticks about once a second, not while paused; a restart replays exactly the
/// ticks since the checkpoint, and `NOW()` then continues from where it stopped, not from the
/// anchor.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn clock(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    // An anchor far in the past makes a return to the anchor visible.
    let anchor = "2000-01-01T00:00:00Z";
    let config = config(
        &storage(&tempdir),
        4,
        json!({ "dev_tweaks": { "now_offset": anchor } }),
    );

    let pipeline = controller(engine, &config, "clock run 1");
    pipeline.start();
    sleep(Duration::from_secs(5));
    // The clock is the only input, so the pipeline takes one step a second.
    let running = tick_count(&pipeline);
    assert!((3..=10).contains(&running), "{running} ticks in 5 s");

    // No ticks while paused, besides those that slipped in before the pause.
    pipeline.pause();
    sleep(Duration::from_secs(5));
    let paused = tick_count(&pipeline);
    assert!(paused <= running + 2, "{paused} ticks after {running}");

    // Checkpoint, then run a few more seconds to journal ticks past the checkpoint.
    pipeline.checkpoint().unwrap();
    pipeline.start();
    sleep(Duration::from_secs(5));
    pipeline.pause();
    wait_for_ingested_ticks(&pipeline);
    let (_, last_run1) = ticks(&pipeline).unwrap();
    let last_run1 = last_run1.unwrap();
    let below = ticks_before(&pipeline, last_run1);
    // A step that races the pause still ingests a tick, and the journal holds it, so count
    // the ticks once the stop has joined the circuit thread.
    let stopped = {
        let observer = pipeline.clone();
        pipeline.stop().unwrap();
        observer.status().num_total_processed_records() as usize
    };

    // The replay check below allows one live tick, so it needs at least two journaled ticks
    // to tell a replay from no replay.
    let journaled = stopped - paused;
    assert!(
        journaled >= 2,
        "only {journaled} ticks were journaled between the checkpoint and the stop"
    );
    assert!(last_run1 > millis(anchor), "no tick past the anchor");

    // The restart restores the ticks of the checkpoint and replays the journaled ones. The
    // first step after the replay commits the last replayed transaction and asks every
    // connector for input, so the clock may add one live tick although the pipeline stays
    // paused.
    let pipeline = controller(engine, &config, "clock run 2");
    let restored = settled_ticks(&pipeline, stopped);
    assert!(
        restored == stopped || restored == stopped + 1,
        "{restored} ticks after the restart, but {stopped} before the stop, plus at most one \
         live tick"
    );

    // Ticks after the restart continue from the last tick of the first run, so none falls
    // below it, as each tick would if the clock went back to the anchor.
    pipeline.start();
    assert!(
        wait(|| tick_count(&pipeline) > restored, default_timeout_ms()).is_ok(),
        "no tick arrived after the restart"
    );
    assert_eq!(
        ticks_before(&pipeline, last_run1),
        below,
        "NOW() fell below {last_run1}, the last tick before the restart"
    );
    pipeline.pause();
    pipeline.stop().unwrap();
}

/// With HTTP-driven ticks, the first `NOW()` is the anchor plus the timezone offset, and
/// advances build on it.
#[engine_test(requires(Storage, FaultTolerance))]
pub fn timezone_offset_http_driven(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    let timezone_ms: i64 = (5 * 60 + 30) * 60_000;
    let config = config(
        &storage(&tempdir),
        1,
        json!({
            "clock_resolution_usecs": 1_000_000,
            "clock_timezone_offset": "+05:30",
            "dev_tweaks": { "now_offset": HTTP_ANCHOR, "now_http_driven": true }
        }),
    );
    let pipeline = controller(engine, &config, "timezone offset");
    pipeline.start();
    // The first tick also shows that the connector is registered.
    wait_for_clock_timestamp(&pipeline, millis(HTTP_ANCHOR) + timezone_ms);

    assert_eq!(
        advance(&pipeline, Some(0)).unwrap(),
        millis(HTTP_ANCHOR) + timezone_ms
    );
    assert_eq!(
        advance(&pipeline, Some(60_000)).unwrap(),
        millis(HTTP_ANCHOR) + timezone_ms + 60_000
    );
    pipeline.stop().unwrap();
}

/// The timezone offset of the first start holds after a restart from a checkpoint with
/// another offset.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn timezone_offset_pinned_across_restart(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    let storage = storage(&tempdir);
    let timezone_ms: i64 = (5 * 60 + 30) * 60_000;
    let config_of = |offset: &str| {
        config(
            &storage,
            1,
            json!({
                "clock_resolution_usecs": 1_000_000,
                "clock_timezone_offset": offset,
                "dev_tweaks": { "now_offset": HTTP_ANCHOR, "now_http_driven": true }
            }),
        )
    };

    let pipeline = controller(engine, &config_of("+05:30"), "timezone pinning run 1");
    pipeline.start();
    wait_for_clock_timestamp(&pipeline, millis(HTTP_ANCHOR) + timezone_ms);
    pipeline.checkpoint().unwrap();
    pipeline.stop().unwrap();

    let pipeline = controller(engine, &config_of("-08:00"), "timezone pinning run 2");
    assert_eq!(
        pipeline
            .status()
            .pipeline_config
            .global
            .clock_timezone_offset,
        Some("+05:30".parse::<ClockTimezoneOffset>().unwrap()),
        "a restart keeps the checkpointed `clock_timezone_offset`"
    );
    // A replayed journal entry and a fresh tick of the merged configuration both give the
    // anchor plus the pinned offset.
    assert!(
        wait(|| !pipeline.is_replaying(), default_timeout_ms()).is_ok(),
        "the replay did not complete within 30 s"
    );
    assert_eq!(
        advance(&pipeline, Some(0)).unwrap(),
        millis(HTTP_ANCHOR) + timezone_ms
    );
    pipeline.stop().unwrap();
}

/// An HTTP-driven `advance` reads `NOW()` for 0, moves it by the resolution for `None`, and
/// moves it by a delta rounded up to the resolution otherwise.
#[engine_test(requires(Storage, FaultTolerance))]
pub fn advance_http_driven(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    let resolution_ms: i64 = 1_000;
    let config = config(
        &storage(&tempdir),
        1,
        json!({
            "clock_resolution_usecs": resolution_ms * 1_000,
            "dev_tweaks": { "now_offset": HTTP_ANCHOR, "now_http_driven": true }
        }),
    );
    let pipeline = controller(engine, &config, "advance");
    pipeline.start();
    // Let the first tick drain.
    sleep(Duration::from_millis(500));

    let anchor = millis(HTTP_ANCHOR);
    let now0 = advance(&pipeline, Some(0)).unwrap();
    assert_eq!(now0, anchor, "the first NOW() is the anchor");
    assert_eq!(
        advance(&pipeline, Some(0)).unwrap(),
        now0,
        "0 does not move the clock"
    );
    assert_eq!(advance(&pipeline, None).unwrap(), anchor + resolution_ms);
    assert_eq!(
        advance(&pipeline, Some(60_000)).unwrap(),
        anchor + resolution_ms + 60_000
    );
    let now3 = advance(&pipeline, None).unwrap();
    assert_eq!(now3, anchor + 2 * resolution_ms + 60_000);
    let now4 = advance(&pipeline, Some((resolution_ms / 3) as u64)).unwrap();
    assert_eq!(
        now4,
        now3 + resolution_ms,
        "a delta below the resolution rounds up"
    );
    assert_eq!(advance(&pipeline, Some(0)).unwrap(), now4);
    pipeline.stop().unwrap();
}

/// A clock driven by the wall clock refuses `advance`.
#[engine_test(requires(Storage, FaultTolerance))]
pub fn advance_rejected_when_wall_driven(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    let config = config(&storage(&tempdir), 1, json!({}));
    let pipeline = controller(engine, &config, "advance rejected");
    pipeline.start();
    sleep(Duration::from_millis(500));
    let error = advance(&pipeline, Some(60_000)).expect_err("advance must fail");
    assert!(error.to_string().contains("now_http_driven"), "{error}");
    pipeline.stop().unwrap();
}

/// After a restart from a checkpoint, `NOW()` resumes from the last replayed advance, not
/// from the checkpoint or the anchor.
///
/// A step is journaled after the circuit evaluates it, so that the entry carries the
/// transaction id assigned there; a stop between a step's output and its journal write must
/// not drop the step, or the advance past the checkpoint is not replayed.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance))]
pub fn advance_survives_checkpoint(engine: &dyn PipelineTestEngine) {
    let tempdir = TempDir::new().unwrap();
    let anchor = millis(HTTP_ANCHOR);
    let one_day_ms: i64 = 24 * 60 * 60 * 1_000;
    let config = config(
        &storage(&tempdir),
        1,
        json!({
            "clock_resolution_usecs": 1_000_000,
            "dev_tweaks": { "now_offset": HTTP_ANCHOR, "now_http_driven": true }
        }),
    );

    // `advance` returns once the connector requests a step, before the step journals the
    // value, so the case waits for the circuit to receive each value.
    let pipeline = controller(engine, &config, "checkpoint survival run 1");
    pipeline.start();
    advance(&pipeline, Some(one_day_ms as u64)).unwrap();
    wait_for_clock_timestamp(&pipeline, anchor + one_day_ms);
    pipeline.checkpoint().unwrap();
    let last = advance(&pipeline, Some(one_day_ms as u64)).unwrap();
    assert_eq!(last, anchor + 2 * one_day_ms);
    wait_for_clock_timestamp(&pipeline, last);
    pipeline.stop().unwrap();

    let pipeline = controller(engine, &config, "checkpoint survival run 2");
    // The replay feeds the journaled steps one at a time, so wait for it before reading
    // `NOW()`.
    assert!(
        wait(|| !pipeline.is_replaying(), default_timeout_ms()).is_ok(),
        "the replay did not complete within 30 s"
    );
    assert_eq!(
        advance(&pipeline, Some(0)).unwrap(),
        last,
        "NOW() resumes from the last replayed value"
    );
    assert_eq!(
        advance(&pipeline, Some(one_day_ms as u64)).unwrap(),
        last + one_day_ms
    );
    pipeline.stop().unwrap();
}
