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

use anyhow::{anyhow, Result as AnyResult};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::{
    format::{BufferSize, Parser},
    transport::{
        InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume,
        TransportInputEndpoint, Watermark,
    },
    PipelineState,
};
use feldera_types::{
    config::{
        ConnectorConfig, FormatConfig, FtModel, InputEndpointConfig, OutputBufferConfig,
        PipelineConfig, TransportConfig, DEFAULT_CLOCK_RESOLUTION_USECS,
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
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep_until, Instant},
};

/// The controller uses this configuration to add a clock input connector to each pipeline.
pub fn now_endpoint_config(config: &PipelineConfig) -> InputEndpointConfig {
    InputEndpointConfig {
        stream: Cow::Borrowed("now"),
        connector_config: ConnectorConfig {
            transport: TransportConfig::ClockInput(ClockConfig {
                clock_resolution_usecs: config
                    .global
                    .clock_resolution_usecs
                    .unwrap_or(DEFAULT_CLOCK_RESOLUTION_USECS),
            }),
            format: Some(FormatConfig {
                name: Cow::Borrowed("json"),
                config: serde_yaml::to_value(JsonParserConfig {
                    update_format: JsonUpdateFormat::Raw,
                    json_flavor: JsonFlavor::ClockInput,
                    array: false,
                    lines: JsonLines::Single,
                })
                .unwrap(),
            }),
            index: None,
            output_buffer_config: OutputBufferConfig::default(),
            max_batch_size: 1,
            // This must be >1; otherwise the controller will pause the connector after every input.
            max_queued_records: 2,
            paused: false,
            labels: vec![],
            start_after: None,
        },
    }
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
    fn current_time(config: &ClockConfig) -> u64 {
        let now = SystemTime::now();
        let now_millis = now
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let clock_resolution_ms = config.clock_resolution_ms();
        now_millis - now_millis % clock_resolution_ms
    }

    /// Time when we want to trigger the next clock tick, assuming we triggered one at `previous_tick`.
    ///
    /// Returns time as `Instant`, so it can be used with `sleep_until`. This also ensures that
    /// we don't need to worry about timezone or daylight changes.
    fn next_tick_time(config: &ClockConfig, previous_tick: &u64) -> Instant {
        let next_tick = previous_tick + config.clock_resolution_ms();

        let target_time = UNIX_EPOCH + Duration::from_millis(next_tick);
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
                        let mut buffer = parser.parse(Self::timestamp_to_record(ts_millis).as_bytes()).0.unwrap();
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
                        let mut buffer = parser.parse(Self::timestamp_to_record(now).as_bytes()).0.unwrap();
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
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    };

    use dbsp::{circuit::CircuitConfig, utils::Tup1, DBSPHandle, Runtime};
    use feldera_adapterlib::catalog::CircuitCatalog;
    use feldera_sqllib::Timestamp;
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
            deserialize_table_record!(Clock["now", 1] {
                (now, "now", false, Timestamp, None)
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

        let controller = Controller::with_config(
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

        let controller = Controller::with_config(
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
}
