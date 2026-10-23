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

#[cfg(test)]
mod test {
    //! Unit tests of the clock's arithmetic and configuration. The connector tests in
    //! `tests` cover the clock in a pipeline.

    use feldera_types::duration::ConfigDuration;
    use serde_json::json;

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
}

#[cfg(any(test, feature = "testing"))]
pub mod tests;
