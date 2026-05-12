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
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{Instant, sleep_until},
};

/// The controller uses this configuration to add a clock input connector to each pipeline.
pub fn now_endpoint_config(config: &PipelineConfig) -> InputEndpointConfig {
    // Compute the offset delta once, at endpoint construction, against the
    // current wall clock.  The connector then advances `NOW()` at wall-clock
    // cadence from this anchor for the rest of the pipeline's lifetime.
    let now_offset_delta_ms = config
        .global
        .dev_tweaks
        .now_offset_delta_ms(chrono::Utc::now());

    InputEndpointConfig::new(
        "now",
        ConnectorConfig::new(
            TransportConfig::ClockInput(ClockConfig {
                clock_resolution_usecs: config
                    .global
                    .clock_resolution_usecs
                    .unwrap_or(DEFAULT_CLOCK_RESOLUTION_USECS),
                now_offset_delta_ms,
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

struct ClockReader {
    sender: UnboundedSender<InputReaderCommand>,
}

impl ClockReader {
    fn new(
        config: Arc<ClockConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> Self {
        let (sender, receiver) = unbounded_channel::<InputReaderCommand>();

        let config_clone = config.clone();
        TOKIO.spawn(async {
            let _ = Self::worker_task(config_clone, parser, consumer, receiver).await;
        });

        Self { sender }
    }

    /// Current timestamp in milliseconds, rounded to `clock_resolution_ms`.
    ///
    /// Reads the wall clock and forwards to [`Self::current_time_at`] for
    /// the actual math.  Split out so tests can supply a fixed wall clock.
    fn current_time(config: &ClockConfig) -> u64 {
        Self::current_time_at(config, SystemTime::now())
    }

    /// Timestamp in milliseconds for a given wall-clock instant, shifted
    /// by `config.now_offset_delta_ms` if set, then rounded to the clock
    /// resolution.
    fn current_time_at(config: &ClockConfig, wall: SystemTime) -> u64 {
        let wall_ms = wall
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let shifted_ms = wall_ms
            .saturating_add(config.now_offset_delta_ms.unwrap_or(0))
            .max(0) as u64;
        let clock_resolution_ms = config.clock_resolution_ms();
        shifted_ms - shifted_ms % clock_resolution_ms
    }

    /// Time when we want to trigger the next clock tick, assuming we triggered one at `previous_tick`.
    ///
    /// Returns time as `Instant`, so it can be used with `sleep_until`. This also ensures that
    /// we don't need to worry about timezone or daylight changes.
    ///
    /// `previous_tick` is the timestamp we just emitted (in the shifted
    /// timeline if `now_offset_delta_ms` is set).  Wake-ups are paced by
    /// wall-clock duration, so we undo the offset before scheduling.
    fn next_tick_time(config: &ClockConfig, previous_tick: &u64) -> Instant {
        let next_tick_ms = previous_tick.saturating_add(config.clock_resolution_ms());
        let next_wall_clock_ms = (next_tick_ms as i64)
            .saturating_sub(config.now_offset_delta_ms.unwrap_or(0))
            .max(0) as u64;

        let target_time = UNIX_EPOCH + Duration::from_millis(next_wall_clock_ms);
        let now = SystemTime::now();
        let duration_until = target_time.duration_since(now).unwrap_or(Duration::ZERO);

        Instant::now() + duration_until
    }

    /// Convert timestamp to a JSON object that can be fed to the parser.
    fn timestamp_to_record(ts_millis: u64) -> String {
        format!("{{\"now\":{ts_millis}}}")
    }

    async fn worker_task(
        config: Arc<ClockConfig>,
        mut parser: Box<dyn Parser>,
        consumer: Box<dyn InputConsumer>,
        mut receiver: UnboundedReceiver<InputReaderCommand>,
    ) {
        const RECORD_SIZE: BufferSize = BufferSize {
            records: 1,
            bytes: std::mem::size_of::<u64>(),
        };
        let mut next_tick: Option<Instant> = None;
        let mut pipeline_state = PipelineState::Paused;
        loop {
            select! {
                // When the next clock tick is scheduled, wakeup at `next_tick` time.
                _ = sleep_until(next_tick.unwrap_or_else(Instant::now)), if next_tick.is_some() => {
                    next_tick = None;
                    if pipeline_state == PipelineState::Running {
                        consumer.request_step();
                    }
                }
                message = receiver.recv() => match message {
                    None => {
                        // channel closed
                        break;
                    }
                    Some(InputReaderCommand::Replay { data, .. }) => {
                        // Parse serialized timestamp, push it to the circuit.
                        let Some(ts_millis) = (match data {
                            RmpValue::Integer(int) => int.as_u64(),
                            _ => None,
                        }) else {
                            consumer.error(true, anyhow!("Invalid timestamp in replay log: {data:?}"), Some("clock"));
                            continue;
                        };
                        consumer.buffered(RECORD_SIZE);
                        let mut buffer = parser.parse(Self::timestamp_to_record(ts_millis).as_bytes(), None).0.unwrap();
                        buffer.flush();
                        consumer.replayed(RECORD_SIZE, 0);
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
                        let now = Self::current_time(&config);
                        let mut buffer = parser.parse(Self::timestamp_to_record(now).as_bytes(), None).0.unwrap();
                        buffer.flush();
                        consumer.extended(RECORD_SIZE, Some(Resume::Replay {
                             seek: serde_json::Value::Null,
                             replay: RmpValue::from(now),
                             hash: 0
                        }), vec![Watermark::new(Utc::now(), None)]);

                        // Schedule next tick.
                        next_tick = Some(Self::next_tick_time(&config, &now));
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
    }

    impl ClockStats {
        fn new() -> Self {
            Self {
                ticks: AtomicUsize::new(0),
            }
        }

        fn ticks(&self) -> usize {
            self.ticks.load(Ordering::Acquire)
        }

        fn clear(&self) {
            self.ticks.store(0, Ordering::Release);
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

        // Create controller.
        let config = serde_json::from_value(json!({
            "name": "test",
            "workers": 4,
            "storage_config": {
                "path": storage_dir,
            },
            "fault_tolerance": {},
            "inputs": {}
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

        let past = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        let future = Utc.with_ymd_and_hms(2036, 1, 1, 0, 0, 0).unwrap();

        let make = |target: chrono::DateTime<Utc>| ClockConfig {
            clock_resolution_usecs: 1_000_000,
            now_offset_delta_ms: Some(target.timestamp_millis() - wall_ms),
        };

        let past_ts = super::ClockReader::current_time_at(&make(past), wall);
        let future_ts = super::ClockReader::current_time_at(&make(future), wall);

        assert_eq!(past_ts, past.timestamp_millis() as u64);
        assert_eq!(future_ts, future.timestamp_millis() as u64);
    }

    /// Pre-epoch offsets clamp to `0`.
    #[test]
    fn test_current_time_with_pre_epoch_offset_clamps_to_zero() {
        use chrono::{TimeZone, Utc};
        use feldera_types::transport::clock::ClockConfig;
        use std::time::{Duration, UNIX_EPOCH};

        let wall_ms: i64 = 1_700_000_000_000;
        let wall = UNIX_EPOCH + Duration::from_millis(wall_ms as u64);

        // A pre-epoch target (year 1900) and a one-millisecond-before-epoch
        // target both fall under the wire-format floor.
        for target in [
            Utc.with_ymd_and_hms(1900, 1, 1, 0, 0, 0).unwrap(),
            Utc.timestamp_millis_opt(-1).unwrap(),
        ] {
            let config = ClockConfig {
                clock_resolution_usecs: 1_000_000,
                now_offset_delta_ms: Some(target.timestamp_millis() - wall_ms),
            };
            let ts = super::ClockReader::current_time_at(&config, wall);
            assert_eq!(ts, 0, "{target} should clamp to epoch, got {ts}");
        }

        // Exactly epoch is the floor: it should *not* clamp away, it
        // should emit 0 as the legitimate epoch timestamp.
        let epoch_config = ClockConfig {
            clock_resolution_usecs: 1_000_000,
            now_offset_delta_ms: Some(0 - wall_ms),
        };
        assert_eq!(super::ClockReader::current_time_at(&epoch_config, wall), 0);
    }

    /// `now_endpoint_config` translates `DevTweaks::now_offset` into a
    /// populated `ClockConfig::now_offset_delta_ms`, and leaves the field
    /// `None` when no override is configured.  Checks only the wiring;
    /// the delta arithmetic itself lives in [`test_current_time_with_offset`].
    #[test]
    fn test_now_endpoint_config_with_offset() {
        use feldera_types::config::{PipelineConfig, TransportConfig};

        let delta_of = |body: serde_json::Value| -> Option<i64> {
            let config: PipelineConfig = serde_json::from_value(body).unwrap();
            let endpoint = super::now_endpoint_config(&config);
            let TransportConfig::ClockInput(clock_config) = &endpoint.connector_config.transport
            else {
                panic!("expected ClockInput transport");
            };
            clock_config.now_offset_delta_ms
        };

        // Past target.
        assert!(
            delta_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "dev_tweaks": { "now_offset": "1970-01-01T00:00:00Z" },
            }))
            .is_some(),
        );
        // Future target.
        assert!(
            delta_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
                "dev_tweaks": { "now_offset": "2036-01-01T00:00:00Z" },
            }))
            .is_some(),
        );
        // No override → no delta.
        assert!(
            delta_of(json!({
                "name": "test", "workers": 1, "fault_tolerance": {}, "inputs": {},
            }))
            .is_none(),
        );
    }
}
