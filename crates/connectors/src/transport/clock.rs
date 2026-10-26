//! Real-time clock connector.
//!
//! An instance of this connector is created automatically for each pipeline.
//! The connector assumes that the `now` stream has the following schema:
//! `Clock{ now: Timestamp }`, i.e., it's a struct with a single field named `now`
//! of type `Timestamp`.
//!
//! The connector triggers a step every `clock_resolution`, rounded to the nearest
//! millisecond boundary, if it's not triggered by some other connector. On every step
//! it feeds precisely one record to the pipeline, containing the current time rounded
//! to clock resolution.  The rounding is important, because it prevents the pipeline
//! from recomputing queries that depend on time more often than requested by the user via
//! the clock resolution pipeline property.
//!
//! The `clock_timezone_offset` pipeline property adds a constant offset to every
//! emitted value, so `NOW()` returns local time in a fixed timezone instead of UTC.
//! The offset is pinned at the first start of the pipeline; it cannot be changed
//! when the pipeline resumes from a checkpoint.
//!
//! The connector supports exactly-once FT.

use anyhow::{Result as AnyResult, anyhow};
use chrono::Utc;
use feldera_adapterlib::{
    PipelineState,
    format::{BufferSize, Parser},
    transport::{
        InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume,
        TransportInputEndpoint, Watermark,
    },
};
use feldera_storage::tokio::TOKIO;
use feldera_types::{
    config::{
        ConnectorConfig, FormatConfig, FtModel, InputEndpointConfig, PipelineConfig,
        TransportConfig,
    },
    format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat},
    program_schema::Relation,
    transport::clock::ClockConfig,
};
use rmpv::Value as RmpValue;
use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    select,
    sync::{
        mpsc::{
            Receiver as MpscReceiver, Sender as MpscSender, UnboundedReceiver, UnboundedSender,
            channel as bounded_channel, unbounded_channel,
        },
        oneshot,
    },
    time::{Instant, sleep_until},
};

/// Side-channel commands to [`ClockReader`], used by `POST /clock/advance`.
enum ClockCommand {
    /// Move the externally-driven clock forward by `delta_ms` (or one
    /// `clock_resolution_ms` if `None`); replies with the new `NOW()`.
    Advance {
        delta_ms: Option<u64>,
        reply: oneshot::Sender<AnyResult<i64>>,
    },
}

/// The controller uses this configuration to add a clock input connector to each pipeline.
pub fn now_endpoint_config(config: &PipelineConfig) -> InputEndpointConfig {
    InputEndpointConfig::new(
        "now",
        ConnectorConfig::new(
            TransportConfig::ClockInput(ClockConfig {
                clock_resolution: Some(config.global.clock_resolution()),
                timezone_offset_ms: config
                    .global
                    .clock_timezone_offset
                    .map(|offset| offset.offset_ms())
                    .unwrap_or(0),
                now_offset_ms: config.global.dev_tweaks.now_offset_ms(),
                http_driven: config.global.dev_tweaks.now_http_driven(),
            }),
            Some(FormatConfig {
                name: Cow::Borrowed("json"),
                config: serde_json::to_value(JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::ClockInput,
                    array: false,
                    lines: JsonLines::Single,
                })
                .unwrap(),
            }),
        )
        .with_max_batch_size(Some(1))
        .with_max_queued_records(
            // This must be >1; otherwise the controller will pause the connector after every input.
            2,
        ),
    )
}

pub struct ClockEndpoint {
    config: Arc<ClockConfig>,
}

impl ClockEndpoint {
    pub fn new(config: ClockConfig) -> AnyResult<Self> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for ClockEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for ClockEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(ClockReader::new(
            self.config.clone(),
            consumer,
            parser,
        )))
    }
}

pub struct ClockReader {
    sender: UnboundedSender<InputReaderCommand>,
    /// Side-channel for the `POST /clock/advance` testing endpoint.
    /// Bounded at 1 so concurrent callers serialize instead of queueing.
    clock_sender: MpscSender<ClockCommand>,
}

impl ClockReader {
    fn new(
        config: Arc<ClockConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> Self {
        let (sender, receiver) = unbounded_channel::<InputReaderCommand>();
        let (clock_sender, clock_receiver) = bounded_channel::<ClockCommand>(1);

        TOKIO.spawn(async {
            let _ = Self::worker_task(config, parser, consumer, receiver, clock_receiver).await;
        });

        Self {
            sender,
            clock_sender,
        }
    }

    /// Advance the clock by `delta_ms` (or one `clock_resolution_ms` if
    /// `None`) and return the new `NOW()` as milliseconds since the Unix
    /// epoch.  `Some(0)` is a read: it returns the current value without
    /// scheduling a step or rounding.
    ///
    /// Non-zero advances round the result up to the next
    /// `clock_resolution_ms` boundary, so any positive advance
    /// guarantees `NOW()` moves strictly forward (a sub-resolution
    /// advance moves the clock by one full resolution).  Requires
    /// `http_driven` mode.
    ///
    /// The returned value is the `NOW()` the worker will emit on its
    /// next pipeline step; queries against materialized views may
    /// observe the previous value until that step completes.
    pub async fn advance(&self, delta_ms: Option<u64>) -> AnyResult<i64> {
        let (reply, rx) = oneshot::channel();
        self.clock_sender
            .send(ClockCommand::Advance { delta_ms, reply })
            .await
            .map_err(|_| anyhow!("clock connector is shut down"))?;
        rx.await
            .map_err(|_| anyhow!("clock connector dropped the advance reply"))?
    }

    /// Delta between emitted `NOW()` and wall clock at worker start: the
    /// shift to the `now_offset_ms` anchor (if set) plus the constant
    /// timezone offset.  The timezone offset thereby applies to every
    /// emitted value, including anchored and http-driven ones.
    fn initial_delta_ms(config: &ClockConfig, wall_at_start_ms: i64) -> i64 {
        config
            .now_offset_ms
            .map(|target| target.saturating_sub(wall_at_start_ms))
            .unwrap_or(0)
            .saturating_add(config.timezone_offset_ms)
    }

    /// Wall clock + `delta_ms`, rounded down to `clock_resolution_ms`.
    fn current_time(config: &ClockConfig, delta_ms: i64) -> i64 {
        Self::current_time_at(config, SystemTime::now(), delta_ms)
    }

    fn current_time_at(config: &ClockConfig, wall: SystemTime, delta_ms: i64) -> i64 {
        let wall_ms = wall
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let shifted_ms = wall_ms.saturating_add(delta_ms);
        let resolution = config.clock_resolution_ms() as i64;
        // `div_euclid` floors toward negative infinity, so pre-epoch
        // values round in the same direction as positive ones.
        shifted_ms.div_euclid(resolution) * resolution
    }

    /// Time when we want to trigger the next clock tick, assuming we
    /// triggered one at `previous_tick`.
    ///
    /// Returned as `Instant` so it can feed `sleep_until` directly, and
    /// because `Instant` is monotonic and unaffected by timezone or
    /// daylight-savings shifts in the underlying `SystemTime`.
    ///
    /// `previous_tick` is in the shifted timeline (if `delta_ms != 0`);
    /// wake-ups are paced by wall-clock duration, so we undo the offset
    /// before scheduling.
    fn next_tick_time(config: &ClockConfig, previous_tick: &i64, delta_ms: i64) -> Instant {
        let next_tick_ms = previous_tick.saturating_add(config.clock_resolution_ms() as i64);
        // Wall clocks aren't pre-epoch, so clamp negative wakeups to 0.
        let next_wall_clock_ms = next_tick_ms.saturating_sub(delta_ms).max(0) as u64;

        let target_time = UNIX_EPOCH + Duration::from_millis(next_wall_clock_ms);
        let now = SystemTime::now();
        let duration_until = target_time.duration_since(now).unwrap_or(Duration::ZERO);

        Instant::now() + duration_until
    }

    /// Convert timestamp to a JSON object that can be fed to the parser.
    fn timestamp_to_record(ts_millis: i64) -> String {
        format!("{{\"now\":{ts_millis}}}")
    }

    async fn worker_task(
        config: Arc<ClockConfig>,
        mut parser: Box<dyn Parser>,
        consumer: Box<dyn InputConsumer>,
        mut receiver: UnboundedReceiver<InputReaderCommand>,
        mut clock_receiver: MpscReceiver<ClockCommand>,
    ) {
        const RECORD_SIZE: BufferSize = BufferSize {
            records: 1,
            bytes: std::mem::size_of::<u64>(),
        };
        let mut next_tick: Option<Instant> = None;
        let mut pipeline_state = PipelineState::Paused;

        // Take a single wall-clock reading and use it to both seed
        // `current_now_ms` and compute the offset delta.
        let wall_at_start = SystemTime::now();
        let wall_at_start_ms = wall_at_start
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut effective_delta_ms: i64 = Self::initial_delta_ms(&config, wall_at_start_ms);
        let mut current_now_ms: i64 =
            Self::current_time_at(&config, wall_at_start, effective_delta_ms);

        loop {
            select! {
                // When the next clock tick is scheduled, wakeup at `next_tick` time.
                _ = sleep_until(next_tick.unwrap_or_else(Instant::now)), if next_tick.is_some() => {
                    next_tick = None;
                    if pipeline_state == PipelineState::Running {
                        consumer.request_step();
                    }
                }
                Some(clock_cmd) = clock_receiver.recv() => { match clock_cmd {
                    ClockCommand::Advance { delta_ms, reply } => {
                        if !config.http_driven {
                            let _ = reply.send(Err(anyhow!(
                                "clock is wall-clock driven; set dev_tweaks.now_http_driven = true to enable POST /clock/advance"
                            )));
                            continue;
                        }
                        let resolution = config.clock_resolution_ms();
                        let effective_delta = delta_ms.unwrap_or(resolution);

                        // `Some(0)` is a pure read; skip rounding so an
                        // off-grid `current_now_ms` stays put.
                        if effective_delta == 0 {
                            let _ = reply.send(Ok(current_now_ms));
                            continue;
                        }

                        // Ceil to `clock_resolution_ms`; positive advances
                        // also snap an off-grid `current_now_ms` to the grid.
                        let advanced = current_now_ms.saturating_add_unsigned(effective_delta);
                        let res_i64 = resolution as i64;
                        current_now_ms = advanced
                            .saturating_add(res_i64 - 1)
                            .div_euclid(res_i64)
                            .saturating_mul(res_i64);
                        consumer.request_step();
                        let _ = reply.send(Ok(current_now_ms));
                    }
                }}
                message = receiver.recv() => match message {
                    None => {
                        // channel closed
                        break;
                    }
                    Some(InputReaderCommand::Replay { data, .. }) => {
                        // Parse serialized timestamp, push it to the circuit.
                        let Some(ts_millis) = (match data {
                            RmpValue::Integer(int) => int.as_i64(),
                            _ => None,
                        }) else {
                            consumer.error(true, anyhow!("Invalid timestamp in replay log: {data:?}"), Some("clock"));
                            continue;
                        };
                        consumer.buffered(RECORD_SIZE);
                        let mut buffer = parser.parse(Self::timestamp_to_record(ts_millis).as_bytes(), None).0.unwrap();
                        consumer.check_flush(buffer.flush());
                        consumer.replayed(RECORD_SIZE, 0);
                        // Pin state so the next http-driven advance starts
                        // from `ts_millis`, not the configured anchor.
                        current_now_ms = ts_millis;
                        // Re-anchor `effective_delta_ms` only if the current
                        // config still has `now_offset`; the configured
                        // value itself is not consulted (see the
                        // `now_offset` doc table for the no-catchup
                        // contract).
                        if config.now_offset_ms.is_some() {
                            let wall_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64;
                            effective_delta_ms = ts_millis.saturating_sub(wall_ms);
                        }
                    }
                    Some(InputReaderCommand::Extend) => {
                        pipeline_state = PipelineState::Running;
                        // Schedule a step, so we can feed the initial timestamp to the circuit.
                        consumer.request_step();
                    }
                    Some(InputReaderCommand::Pause) => {
                        // Stop timer.
                        pipeline_state = PipelineState::Paused;
                        next_tick = None;
                    }
                    Some(InputReaderCommand::Queue { .. }) => {
                        // Push current time;
                        consumer.buffered(RECORD_SIZE);
                        let now = if config.http_driven {
                            current_now_ms
                        } else {
                            Self::current_time(&config, effective_delta_ms)
                        };
                        let mut buffer = parser.parse(Self::timestamp_to_record(now).as_bytes(), None).0.unwrap();
                        consumer.check_flush(buffer.flush());
                        consumer.extended(RECORD_SIZE, Some(Resume::Replay {
                             seek: serde_json::Value::Null,
                             replay: RmpValue::from(now),
                             hash: 0
                        }), vec![Watermark::new(Utc::now(), None)]);

                        current_now_ms = now;

                        // Schedule the next wall-clock tick only in wall
                        // mode; externally-driven mode waits for advance().
                        if !config.http_driven {
                            next_tick = Some(Self::next_tick_time(&config, &now, effective_delta_ms));
                        }

                    }
                    Some(InputReaderCommand::Disconnect) => {
                        break;
                    }
                }
            }
        }
    }
}

impl InputReader for ClockReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod test {
    //! Unit tests of the clock's arithmetic and configuration. The connector tests in
    //! `tests` cover the clock in a pipeline.

    use crate::{
        Controller,
        test::{
            engine_test,
            pipeline::{PipelineTestEngine, TestCircuit, query},
        },
        transport::clock::ClockReader,
    };
    use arrow::{
        array::{Array, AsArray},
        datatypes::{Int64Type, TimestampMicrosecondType},
    };
    use feldera_adapterlib::test::{Capability, default_timeout_ms, wait};
    use feldera_storage::tokio::TOKIO;
    #[cfg(test)]
    use feldera_types::duration::ConfigDuration;
    use feldera_types::{config::PipelineConfig, transport::clock::ClockTimezoneOffset};
    use serde_json::{Value, json};
    use std::{fs::create_dir, thread::sleep, time::Duration};
    use tempfile::TempDir;

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

    /// `current_time_at` with a fixed wall clock and a configured offset
    /// emits exactly the configured target timestamp.  Fully deterministic:
    /// the wall clock is supplied by the test, not read from the system.
    #[test]
    fn test_current_time_with_offset() {
        use chrono::{TimeZone, Utc};
        use feldera_types::transport::clock::ClockConfig;
        use std::time::{Duration, UNIX_EPOCH};

        // An arbitrary fixed wall-clock instant; the actual value is
        // irrelevant because we compute the delta against it.
        let wall_ms: i64 = 1_700_000_000_000;
        let wall = UNIX_EPOCH + Duration::from_millis(wall_ms as u64);

        let make = |target: chrono::DateTime<Utc>| {
            let target_ms = target.timestamp_millis();
            let cfg = ClockConfig {
                clock_resolution: Some(ConfigDuration::from_secs(1)),
                timezone_offset_ms: 0,
                now_offset_ms: Some(target_ms),
                http_driven: false,
            };
            // `current_time_at` takes the delta as an explicit argument;
            // the worker computes it from its own `SystemTime::now()`
            // reading, so we replicate that here against `wall`.
            let delta = target_ms - wall_ms;
            (cfg, delta)
        };

        for target in [
            Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(2036, 1, 1, 0, 0, 0).unwrap(),
            // Upper bound of the documented supported range.
            Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap(),
        ] {
            let (cfg, delta) = make(target);
            let ts = super::ClockReader::current_time_at(&cfg, wall, delta);
            assert_eq!(ts, target.timestamp_millis(), "target {target}");
        }
    }

    /// Pre-epoch anchors emit negative epoch-ms verbatim (no clamping).
    #[test]
    fn test_current_time_with_pre_epoch_offset() {
        use chrono::{TimeZone, Utc};
        use feldera_types::transport::clock::ClockConfig;
        use std::time::{Duration, UNIX_EPOCH};

        let wall_ms: i64 = 1_700_000_000_000;
        let wall = UNIX_EPOCH + Duration::from_millis(wall_ms as u64);

        for target in [
            Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap(),
            Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap(),
            Utc.timestamp_millis_opt(-1).unwrap(),
        ] {
            let target_ms = target.timestamp_millis();
            let config = ClockConfig {
                clock_resolution: Some(ConfigDuration::from_secs(1)),
                timezone_offset_ms: 0,
                now_offset_ms: Some(target_ms),
                http_driven: false,
            };
            let delta = target_ms - wall_ms;
            let ts = super::ClockReader::current_time_at(&config, wall, delta);
            // Resolution floor at 1 s; -1ms rounds down to -1000ms.
            let expected = target_ms.div_euclid(1_000) * 1_000;
            assert_eq!(ts, expected, "target {target}");
        }

        // Exactly epoch round-trips as 0.
        let epoch_config = ClockConfig {
            clock_resolution: Some(ConfigDuration::from_secs(1)),
            timezone_offset_ms: 0,
            now_offset_ms: Some(0),
            http_driven: false,
        };
        assert_eq!(
            super::ClockReader::current_time_at(&epoch_config, wall, -wall_ms),
            0
        );
    }

    /// `now_endpoint_config` writes the absolute target into
    /// `ClockConfig::now_offset_ms` (delta arithmetic is the worker's job).
    #[test]
    fn test_now_endpoint_config_with_offset() {
        use feldera_types::config::{PipelineConfig, TransportConfig};

        let target_of = |body: serde_json::Value| -> Option<i64> {
            let config: PipelineConfig = serde_json::from_value(body).unwrap();
            let endpoint = super::now_endpoint_config(&config);
            let TransportConfig::ClockInput(clock_config) = &endpoint.connector_config.transport
            else {
                panic!("expected ClockInput transport");
            };
            clock_config.now_offset_ms
        };

        // Past target: the literal epoch.
        assert_eq!(
            target_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "dev_tweaks": { "now_offset": "1970-01-01T00:00:00Z" },
            })),
            Some(0),
        );
        // Future target.
        assert_eq!(
            target_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "dev_tweaks": { "now_offset": "2036-01-01T00:00:00Z" },
            })),
            Some(
                chrono::DateTime::parse_from_rfc3339("2036-01-01T00:00:00Z")
                    .unwrap()
                    .timestamp_millis()
            ),
        );
        // No override → no target.
        assert!(
            target_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
            }))
            .is_none(),
        );
    }

    /// `now_endpoint_config` converts the `clock_timezone_offset` pipeline
    /// property to `ClockConfig::timezone_offset_ms`.
    #[test]
    fn test_now_endpoint_config_with_timezone_offset() {
        use feldera_types::config::{PipelineConfig, TransportConfig};

        let tz_ms_of = |body: serde_json::Value| -> i64 {
            let config: PipelineConfig = serde_json::from_value(body).unwrap();
            let endpoint = super::now_endpoint_config(&config);
            let TransportConfig::ClockInput(clock_config) = &endpoint.connector_config.transport
            else {
                panic!("expected ClockInput transport");
            };
            clock_config.timezone_offset_ms
        };

        const MINUTE_MS: i64 = 60_000;
        assert_eq!(
            tz_ms_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "clock_timezone_offset": "+05:30",
            })),
            (5 * 60 + 30) * MINUTE_MS,
        );
        assert_eq!(
            tz_ms_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "clock_timezone_offset": "-08:00",
            })),
            -8 * 60 * MINUTE_MS,
        );
        // Old configurations without the property default to UTC.
        assert_eq!(
            tz_ms_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
            })),
            0,
        );
        // Invalid offsets are rejected when the config is deserialized.
        assert!(
            serde_json::from_value::<PipelineConfig>(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "clock_timezone_offset": "pancake",
            }))
            .is_err(),
        );
    }

    /// `initial_delta_ms` composes the `now_offset` anchor shift with the
    /// constant timezone offset.
    #[test]
    fn test_initial_delta_ms() {
        use feldera_types::transport::clock::ClockConfig;

        const MINUTE_MS: i64 = 60_000;
        let wall_ms: i64 = 1_700_000_000_000;
        let tz_ms: i64 = (5 * 60 + 30) * MINUTE_MS; // +05:30
        let anchor_ms: i64 = wall_ms - 24 * 60 * MINUTE_MS; // one day in the past

        let base = ClockConfig {
            clock_resolution: Some(ConfigDuration::from_secs(1)),
            timezone_offset_ms: 0,
            now_offset_ms: None,
            http_driven: false,
        };

        // No offsets: emitted NOW() tracks wall clock.
        assert_eq!(super::ClockReader::initial_delta_ms(&base, wall_ms), 0);

        // Timezone offset alone shifts every value by the offset.
        let tz = ClockConfig {
            timezone_offset_ms: tz_ms,
            ..base.clone()
        };
        assert_eq!(super::ClockReader::initial_delta_ms(&tz, wall_ms), tz_ms);

        // Anchor alone shifts to the anchor.
        let anchored = ClockConfig {
            now_offset_ms: Some(anchor_ms),
            ..base.clone()
        };
        assert_eq!(
            super::ClockReader::initial_delta_ms(&anchored, wall_ms),
            anchor_ms - wall_ms
        );

        // Anchor and timezone offset compose.
        let both = ClockConfig {
            now_offset_ms: Some(anchor_ms),
            timezone_offset_ms: tz_ms,
            ..base
        };
        assert_eq!(
            super::ClockReader::initial_delta_ms(&both, wall_ms),
            anchor_ms - wall_ms + tz_ms
        );
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
}
