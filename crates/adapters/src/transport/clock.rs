//! Real-time clock connector.
//!
//! An instance of this connector is created automatically for each pipeline.
//! The connector assumes that the `now` stream has the following schema:
//! `Clock{ now: Timestamp }`, i.e., it's a struct with a single field named `now`
//! of type `Timestamp`.
//!
//! The connector triggers a step every `clock_resolution_usecs`, rounded to the nearest
//! millisecond boundary, if it's not triggered by some other connector. On every step
//! it feeds precisely one record to the pipeline, containing the current time rounded
//! to clock resolution.  The rounding is important, because it prevents the pipeline
//! from recomputing queries that depend on time more often than requested by the user via
//! the clock resolution pipeline property.
//!
//! The connector supports exactly-once FT.

use anyhow::{Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    PipelineState,
    format::{BufferSize, Parser},
    transport::{
        InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume,
        TransportInputEndpoint, Watermark,
    },
};
use feldera_types::{
    config::{
        ConnectorConfig, DEFAULT_CLOCK_RESOLUTION_USECS, FormatConfig, FtModel,
        InputEndpointConfig, PipelineConfig, TransportConfig,
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
                clock_resolution_usecs: config
                    .global
                    .clock_resolution_usecs
                    .unwrap_or(DEFAULT_CLOCK_RESOLUTION_USECS),
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

        let config_clone = config.clone();
        TOKIO.spawn(async {
            let _ =
                Self::worker_task(config_clone, parser, consumer, receiver, clock_receiver).await;
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
    pub async fn advance(&self, delta_ms: Option<u64>) -> AnyResult<i64> {
        let (reply, rx) = oneshot::channel();
        self.clock_sender
            .send(ClockCommand::Advance { delta_ms, reply })
            .await
            .map_err(|_| anyhow!("clock connector is shut down"))?;
        rx.await
            .map_err(|_| anyhow!("clock connector dropped the advance reply"))?
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
        let mut effective_delta_ms: i64 = config
            .now_offset_ms
            .map(|target| target.saturating_sub(wall_at_start_ms))
            .unwrap_or(0);
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
                        buffer.flush();
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
                        buffer.flush();
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

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        fs::create_dir,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread::sleep,
        time::Duration,
    };

    use dbsp::{DBSPHandle, Runtime, circuit::CircuitConfig, utils::Tup1};
    use feldera_adapterlib::catalog::CircuitCatalog;
    use feldera_sqllib::{Timestamp, Variant};
    use feldera_types::{
        deserialize_table_record,
        program_schema::{ColumnType, Field, Relation, SqlIdentifier},
        serialize_table_record,
    };
    use serde_json::json;
    use tempfile::TempDir;

    use crate::{Catalog, Controller};

    struct ClockStats {
        ticks: AtomicUsize,
        /// Last emitted `NOW()` in ms since epoch.  Used to assert
        /// monotonicity across checkpoint/restart in wall-clock-with-offset
        /// mode (the "no catchup" contract).  Signed so pre-1970 anchors
        /// round-trip without wrapping.
        last_value: std::sync::atomic::AtomicI64,
    }

    impl ClockStats {
        fn new() -> Self {
            Self {
                ticks: AtomicUsize::new(0),
                last_value: std::sync::atomic::AtomicI64::new(0),
            }
        }

        fn ticks(&self) -> usize {
            self.ticks.load(Ordering::Acquire)
        }

        fn last_value(&self) -> i64 {
            self.last_value.load(Ordering::Acquire)
        }

        fn clear(&self) {
            self.ticks.store(0, Ordering::Release);
            self.last_value.store(0, Ordering::Release);
        }
    }

    /// Create a simple test circuit that passes each of a number of streams right
    /// through to the corresponding output.  The number of streams is the length of
    /// `persistent_output_ids`, which also specifies optional persistent output ids.
    fn clock_test_circuit(
        config: CircuitConfig,
        test_stats: Arc<ClockStats>,
    ) -> (DBSPHandle, Box<dyn CircuitCatalog>) {
        let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
            let mut catalog = Catalog::new();

            let input_schema = Relation::new(
                "now".into(),
                vec![Field::new(
                    SqlIdentifier::from("now"),
                    ColumnType::timestamp(false),
                )],
                false,
                BTreeMap::new(),
            );

            let input_schema_str = serde_json::to_string(&input_schema).unwrap();

            let (now_stream, now_handle) = circuit.add_input_zset::<Tup1<Timestamp>>();

            now_stream.map(move |x| {
                test_stats.ticks.fetch_add(1, Ordering::AcqRel);
                // Record the most recent emitted timestamp in epoch-ms.
                test_stats
                    .last_value
                    .store(x.0.milliseconds(), Ordering::Release);
                *x
            });

            now_stream.set_persistent_id(Some("now"));

            #[derive(Clone, Debug, Eq, PartialEq, Default, PartialOrd, Ord)]
            pub struct Clock {
                now: Timestamp,
            }
            impl From<Clock> for Tup1<Timestamp> {
                fn from(t: Clock) -> Self {
                    Tup1::new(t.now)
                }
            }
            impl From<Tup1<Timestamp>> for Clock {
                fn from(t: Tup1<Timestamp>) -> Self {
                    Self { now: t.0 }
                }
            }
            deserialize_table_record!(Clock["now", Variant, 1] {
                (now, "now", false, Timestamp, |_| None)
            });
            serialize_table_record!(Clock[1]{
                now["now"]: Timestamp
            });

            catalog.register_input_zset::<_, Clock>(
                now_stream.clone(),
                now_handle,
                &input_schema_str,
            );

            Ok(catalog)
        })
        .unwrap();
        (circuit, Box::new(catalog))
    }

    #[test]
    fn test_clock() {
        let tempdir = TempDir::new().unwrap();
        let tempdir_path = tempdir.path();

        //let tempdir_path = tempdir.into_path();
        //println!("{}", tempdir_path.display());

        let storage_dir = tempdir_path.join("storage");
        create_dir(&storage_dir).unwrap();

        // Anchor far in the past so a regression in the no-catchup
        // restart contract (NOW() jumping back to anchor) is detectable.
        let anchor_rfc = "2000-01-01T00:00:00Z";
        let anchor_ms = chrono::DateTime::parse_from_rfc3339(anchor_rfc)
            .unwrap()
            .timestamp_millis();
        let config = serde_json::from_value(json!({
            "name": "test",
            "workers": 4,
            "storage_config": {
                "path": storage_dir,
            },
            "fault_tolerance": {},
            "inputs": {},
            "dev_tweaks": { "now_offset": anchor_rfc },
        }))
        .unwrap();

        println!("Pipeline config: {config:?}");

        let test_stats = Arc::new(ClockStats::new());
        let test_stats_clone = test_stats.clone();

        let controller = Controller::with_test_config(
            move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
            &config,
            Box::new(move |e, _| panic!("clock_test pipeline 1: error: {e}")),
        )
        .unwrap();

        controller.start();

        sleep(Duration::from_secs(5));

        // The clock is the only input connector, so there should be one step/s.
        let ticks = test_stats.ticks();
        assert!((3..=10).contains(&ticks));

        // No clock ticks in the paused state, except whatever slipped in before the controller is paused.
        controller.pause();
        sleep(Duration::from_secs(5));

        let old_ticks = ticks;
        let ticks = test_stats.ticks();

        assert!(ticks <= old_ticks + 2);

        // Checkpoint the controller; let it run for a few seconds
        // after the checkpoint to accumulate several new changes.
        controller.checkpoint().unwrap();
        controller.start();

        sleep(Duration::from_secs(5));

        // Pause the controller first to prevent more ticks from being generated
        // before we count them and stop the pipeline
        controller.pause();

        let old_ticks = ticks;
        let ticks_after_checkpoint = test_stats.ticks() - old_ticks;

        println!("{ticks_after_checkpoint} additional ticks after the checkpoint");

        // Capture the last `NOW()` emitted in Run 1.  Used below to verify
        // the no-catchup contract across the restart.
        let last_v_run1 = test_stats.last_value();
        println!("Run 1 last emitted NOW(): {last_v_run1} ms (anchor {anchor_ms})");
        assert!(
            last_v_run1 > anchor_ms,
            "Run 1 should have emitted at least one tick past the anchor"
        );

        println!("Stopping the pipeline");
        controller.stop().unwrap();

        test_stats.clear();

        // The connector should replay the exact number of clock ticks.
        println!("Restarting from checkpoint");

        let test_stats_clone = test_stats.clone();

        let controller = Controller::with_test_config(
            move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
            &config,
            Box::new(move |e, _| panic!("clock_test pipeline 2: error: {e}")),
        )
        .unwrap();

        sleep(Duration::from_secs(5));

        let ticks = test_stats.ticks();
        println!("{ticks} ticks replayed after restart");
        // Allow at most one extra tick after pause.
        assert!(ticks >= ticks_after_checkpoint && ticks <= ticks_after_checkpoint + 1);

        // No-catchup contract: post-replay emissions continue from
        // `last_v_run1` rather than jumping back to the anchor.
        controller.start();
        sleep(Duration::from_secs(5));
        controller.pause();

        let last_v_run2 = test_stats.last_value();
        println!("Run 2 last emitted NOW(): {last_v_run2} ms");
        assert!(
            last_v_run2 >= last_v_run1,
            "no-catchup: post-restart NOW() ({last_v_run2}) regressed below \
             Run 1's last emission ({last_v_run1}), anchor {anchor_ms}"
        );

        controller.stop().unwrap();
        println!("Clock test is finished");
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
                clock_resolution_usecs: 1_000_000,
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
                clock_resolution_usecs: 1_000_000,
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
            clock_resolution_usecs: 1_000_000,
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

    /// `ClockReader::advance` contract: read, explicit forward, and tick-by-resolution.
    #[test]
    fn test_clock_advance_http_driven() {
        use dbsp::circuit::tokio::TOKIO;

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        create_dir(&storage_dir).unwrap();

        // Anchor `NOW()` at a fixed past instant so the test asserts
        // against literal values rather than wall clock.
        let anchor_rfc = "2023-11-14T22:13:20Z";
        let anchor_ms = chrono::DateTime::parse_from_rfc3339(anchor_rfc)
            .unwrap()
            .timestamp_millis();

        // 1-second resolution so the `None`-tick step is a clean +1000ms.
        let resolution_ms: i64 = 1_000;
        let config = serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
            "storage_config": { "path": storage_dir },
            "fault_tolerance": {},
            "inputs": {},
            "clock_resolution_usecs": resolution_ms * 1_000,
            "dev_tweaks": {
                "now_offset": anchor_rfc,
                "now_http_driven": true,
            },
        }))
        .unwrap();

        let test_stats = Arc::new(ClockStats::new());
        let test_stats_clone = test_stats.clone();

        let controller = Controller::with_test_config(
            move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
            &config,
            Box::new(move |e, _| panic!("clock_advance test: error: {e}")),
        )
        .unwrap();

        controller.start();
        // Let the initial tick (triggered by `InputReaderCommand::Extend`) drain.
        sleep(Duration::from_millis(500));

        let reader = controller
            .get_input_endpoint("now")
            .expect("clock connector should be registered");
        let clock_reader = reader
            .as_any()
            .downcast::<super::ClockReader>()
            .expect("`now` should downcast to ClockReader");

        // Read-only: Some(0) returns the current NOW() without moving it.
        let now0 = TOKIO.block_on(clock_reader.advance(Some(0))).unwrap();
        assert_eq!(now0, anchor_ms, "initial NOW() should be the anchor");

        let now1 = TOKIO.block_on(clock_reader.advance(Some(0))).unwrap();
        assert_eq!(now1, now0, "Some(0) must not move the clock");

        // None advances by one `clock_resolution_ms` (one wall-clock tick).
        let now_tick = TOKIO.block_on(clock_reader.advance(None)).unwrap();
        assert_eq!(now_tick, anchor_ms + resolution_ms);

        // Some(n) advances by n ms (rounded up to clock_resolution_ms;
        // 60_000 is already a multiple of the 1 s resolution).
        let now2 = TOKIO.block_on(clock_reader.advance(Some(60_000))).unwrap();
        assert_eq!(now2, anchor_ms + resolution_ms + 60_000);

        // Compounding works across explicit and tick-style advances.
        let now3 = TOKIO.block_on(clock_reader.advance(None)).unwrap();
        assert_eq!(now3, anchor_ms + 2 * resolution_ms + 60_000);

        // Sub-resolution advance rounds up to one full resolution.
        let now4 = TOKIO
            .block_on(clock_reader.advance(Some((resolution_ms / 3) as u64)))
            .unwrap();
        assert_eq!(now4, now3 + resolution_ms);

        // Read-only after the sub-resolution advance returns the new value.
        let now5 = TOKIO.block_on(clock_reader.advance(Some(0))).unwrap();
        assert_eq!(now5, now4);

        controller.stop().unwrap();
    }

    /// `advance` errors when `http_driven` is false.
    #[test]
    fn test_clock_advance_rejected_when_wall_driven() {
        use dbsp::circuit::tokio::TOKIO;

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        create_dir(&storage_dir).unwrap();

        let config = serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
            "storage_config": { "path": storage_dir },
            "fault_tolerance": {},
            "inputs": {},
        }))
        .unwrap();

        let test_stats = Arc::new(ClockStats::new());
        let test_stats_clone = test_stats.clone();

        let controller = Controller::with_test_config(
            move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
            &config,
            Box::new(move |e, _| panic!("clock_advance reject test: error: {e}")),
        )
        .unwrap();

        controller.start();
        sleep(Duration::from_millis(500));

        let reader = controller.get_input_endpoint("now").unwrap();
        let clock_reader = reader.as_any().downcast::<super::ClockReader>().unwrap();

        let err = TOKIO
            .block_on(clock_reader.advance(Some(60_000)))
            .expect_err("advance must error in wall-clock mode");
        assert!(
            err.to_string().contains("now_http_driven"),
            "unexpected error: {err}"
        );

        controller.stop().unwrap();
    }

    /// After checkpoint/restart, `NOW()` resumes from the last replayed value, not the anchor.
    #[test]
    fn test_clock_advance_survives_checkpoint() {
        use dbsp::circuit::tokio::TOKIO;

        let tempdir = TempDir::new().unwrap();
        let storage_dir = tempdir.path().join("storage");
        create_dir(&storage_dir).unwrap();

        let anchor_rfc = "2023-11-14T22:13:20Z";
        let anchor_ms = chrono::DateTime::parse_from_rfc3339(anchor_rfc)
            .unwrap()
            .timestamp_millis();
        let resolution_ms: i64 = 1_000;
        let one_day_ms: i64 = 24 * 60 * 60 * 1_000;

        let config = serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
            "storage_config": { "path": storage_dir },
            "fault_tolerance": {},
            "inputs": {},
            "clock_resolution_usecs": resolution_ms * 1_000,
            "dev_tweaks": {
                "now_offset": anchor_rfc,
                "now_http_driven": true,
            },
        }))
        .unwrap();

        // Run 1: advance once → checkpoint → advance again → stop.
        let last_emitted_in_run1 = {
            let test_stats = Arc::new(ClockStats::new());
            let test_stats_clone = test_stats.clone();
            let controller = Controller::with_test_config(
                move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
                &config,
                Box::new(move |e, _| panic!("checkpoint survival run 1: {e}")),
            )
            .unwrap();
            controller.start();

            let reader = controller.get_input_endpoint("now").unwrap();
            let clock_reader = reader.as_any().downcast::<super::ClockReader>().unwrap();

            // First advance, lands before the checkpoint.
            TOKIO
                .block_on(clock_reader.advance(Some(one_day_ms as u64)))
                .unwrap();
            controller.checkpoint().unwrap();

            // Second advance, lands after the checkpoint marker.  This
            // is the journal entry that will be replayed on restart.
            let post = TOKIO
                .block_on(clock_reader.advance(Some(one_day_ms as u64)))
                .unwrap();
            assert_eq!(post, anchor_ms + 2 * one_day_ms);
            // `advance()` returns when the worker has requested a step,
            // but the Queue that actually records the value to the
            // journal is asynchronous.  Wait for it to drain or there
            // will be nothing for run 2 to replay.
            sleep(Duration::from_secs(1));
            controller.stop().unwrap();
            post
        };

        // Run 2: restart from the checkpoint.
        let test_stats = Arc::new(ClockStats::new());
        let test_stats_clone = test_stats.clone();
        let controller = Controller::with_test_config(
            move |workers| Ok(clock_test_circuit(workers, test_stats_clone)),
            &config,
            Box::new(move |e, _| panic!("checkpoint survival run 2: {e}")),
        )
        .unwrap();

        let reader = controller.get_input_endpoint("now").unwrap();
        let clock_reader = reader.as_any().downcast::<super::ClockReader>().unwrap();

        let post_replay = TOKIO.block_on(clock_reader.advance(Some(0))).unwrap();
        assert_eq!(
            post_replay, last_emitted_in_run1,
            "NOW() must resume from the last replayed value, not jump back to anchor"
        );

        // A further advance continues monotonically.
        let after_more = TOKIO
            .block_on(clock_reader.advance(Some(one_day_ms as u64)))
            .unwrap();
        assert_eq!(after_more, last_emitted_in_run1 + one_day_ms);

        controller.stop().unwrap();
    }
}
