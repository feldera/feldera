//! Live engine statistics from `GET /v0/pipelines/{name}/stats`.

use serde_json::Value;
use std::collections::BTreeMap;

use super::json;
use super::text::{terminal_safe, terminal_safe_multiline};

/// Classify usage against a quota with the engine's own memory-pressure
/// thresholds (85% moderate, 90% high, 95% critical), so storage pressure
/// reads on the same scale as the reported memory pressure.
pub fn pressure_level(used_bytes: i64, quota_bytes: i64) -> &'static str {
    if quota_bytes <= 0 {
        return "low";
    }
    let ratio = used_bytes.max(0) as f64 / quota_bytes as f64;
    if ratio >= 0.95 {
        "critical"
    } else if ratio >= 0.90 {
        "high"
    } else if ratio >= 0.85 {
        "moderate"
    } else {
        "low"
    }
}

/// Whether a connector feeds or drains the pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectorKind {
    #[default]
    Input,
    Output,
}

impl ConnectorKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Input => "IN",
            Self::Output => "OUT",
        }
    }

    /// What a relation of this direction is in SQL.
    pub const fn relation_noun(self) -> &'static str {
        match self {
            Self::Input => "table",
            Self::Output => "view",
        }
    }
}

/// One-word connector state, ordered most severe first so an ascending
/// sort surfaces problems and a relation reports its worst connector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConnectorStatus {
    Fatal,
    Unhealthy,
    Paused,
    #[default]
    Active,
    Eoi,
}

impl ConnectorStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Fatal => "Fatal",
            Self::Unhealthy => "Unhealthy",
            Self::Paused => "Paused",
            Self::Active => "Active",
            Self::Eoi => "EOI",
        }
    }
}

/// The health a connector reports about its external system.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectorHealth {
    pub is_healthy: bool,
    pub description: Option<String>,
}

impl ConnectorHealth {
    fn from_json(health: &Value) -> Option<Self> {
        let status = json::string(health, "status")?;
        Some(Self {
            is_healthy: !status.eq_ignore_ascii_case("unhealthy"),
            description: json::string(health, "description").map(|text| terminal_safe(&text)),
        })
    }
}

/// The latest input position the pipeline fully processed.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Frontier {
    /// Connector-specific position, e.g. Kafka partition offsets.
    pub metadata: Value,
    pub ingested_at_millis: Option<i64>,
    pub processed_at_millis: Option<i64>,
    pub completed_at_millis: Option<i64>,
}

impl Frontier {
    fn from_json(frontier: &Value) -> Option<Self> {
        if !frontier.is_object() {
            return None;
        }
        Some(Self {
            metadata: frontier.get("metadata").cloned().unwrap_or(Value::Null),
            ingested_at_millis: json::timestamp_millis(frontier, "ingested_at"),
            processed_at_millis: json::timestamp_millis(frontier, "processed_at"),
            completed_at_millis: json::timestamp_millis(frontier, "completed_at"),
        })
    }
}

/// Which counter a recent error belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectorErrorKind {
    Parse,
    Encode,
    Transport,
}

impl ConnectorErrorKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Parse => "parse",
            Self::Encode => "encode",
            Self::Transport => "transport",
        }
    }
}

/// Identity of a recent error: its kind and the server's sequence number.
pub type ConnectorErrorKey = (ConnectorErrorKind, i64);

/// One recent error message; only the per-connector status route lists them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectorError {
    pub kind: ConnectorErrorKind,
    pub at_millis: Option<i64>,
    /// The server's sequence number within its kind; orders errors that
    /// share a millisecond.
    pub index: i64,
    pub message: String,
}

impl ConnectorError {
    pub fn key(&self) -> ConnectorErrorKey {
        (self.kind, self.index)
    }
}

/// One connector endpoint with its operational counters. Fields that only
/// one direction reports stay zero (or `None`) for the other.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectorRow {
    pub kind: ConnectorKind,
    pub endpoint_name: String,
    /// Table (inputs) or view (outputs) the endpoint attaches to.
    pub relation: String,
    pub paused: bool,
    pub barrier: bool,
    /// Records received (inputs) or transmitted (outputs).
    pub records: i64,
    pub bytes: i64,
    pub buffered_records: i64,
    pub error_count: i64,
    pub end_of_input: bool,
    pub fatal_error: Option<String>,
    pub health: Option<ConnectorHealth>,
    pub transport_errors: i64,
    /// Input only.
    pub parse_errors: i64,
    pub buffered_bytes: i64,
    pub latency_p99_micros: Option<i64>,
    pub frontier: Option<Frontier>,
    /// Output only.
    pub encode_errors: i64,
    pub queued_records: i64,
    pub queued_batches: i64,
    pub buffered_batches: i64,
    pub memory_bytes: i64,
    /// Pipeline input records whose output this endpoint has fully handled.
    pub processed_input_records: i64,
    /// Records written in the batch now being committed, when supported.
    pub batch_records_written: Option<i64>,
    /// Newest first; empty unless read from the per-connector status route.
    pub recent_errors: Vec<ConnectorError>,
}

impl ConnectorRow {
    pub fn from_json(connector: &Value, kind: ConnectorKind) -> Self {
        let metrics = connector.get("metrics").cloned().unwrap_or(Value::Null);
        let count = |key: &str| json::integer_or_zero(&metrics, key);
        let relation = connector
            .get("config")
            .and_then(|config| {
                json::string(config, "stream").or_else(|| json::string(config, "relation"))
            })
            .map(|relation| terminal_safe(&relation))
            .unwrap_or_else(|| "?".to_string());
        let mut row = Self {
            kind,
            endpoint_name: json::string(connector, "endpoint_name")
                .map(|name| terminal_safe(&name))
                .unwrap_or_else(|| "<unnamed>".to_string()),
            relation,
            paused: json::boolean(connector, "paused"),
            barrier: json::boolean(connector, "barrier"),
            buffered_records: count("buffered_records"),
            end_of_input: json::boolean(&metrics, "end_of_input"),
            fatal_error: json::string(connector, "fatal_error")
                .map(|error| terminal_safe_multiline(&error)),
            health: connector.get("health").and_then(ConnectorHealth::from_json),
            transport_errors: count("num_transport_errors"),
            recent_errors: recent_errors(connector),
            ..Self::default()
        };
        match kind {
            ConnectorKind::Input => {
                row.records = count("total_records");
                row.bytes = count("total_bytes");
                row.parse_errors = count("num_parse_errors");
                row.buffered_bytes = count("buffered_bytes");
                row.latency_p99_micros = json::integer(&metrics, "processing_latency_p99_micros");
                row.frontier = connector
                    .get("completed_frontier")
                    .and_then(Frontier::from_json);
            }
            ConnectorKind::Output => {
                row.records = count("transmitted_records");
                row.bytes = count("transmitted_bytes");
                row.encode_errors = count("num_encode_errors");
                row.queued_records = count("queued_records");
                row.queued_batches = count("queued_batches");
                row.buffered_batches = count("buffered_batches");
                row.memory_bytes = count("memory");
                row.processed_input_records = count("total_processed_input_records");
                row.batch_records_written = json::integer(&metrics, "batch_records_written");
            }
        }
        row.error_count = row.parse_errors + row.encode_errors + row.transport_errors;
        row
    }

    /// Name the per-connector REST routes expect; stats report endpoints
    /// qualified as `relation.connector`.
    pub fn connector_name(&self) -> &str {
        self.endpoint_name
            .strip_prefix(self.relation.as_str())
            .and_then(|rest| rest.strip_prefix('.'))
            .filter(|rest| !rest.is_empty())
            .unwrap_or(&self.endpoint_name)
    }

    /// Whether the connector counted errors or stopped on a fatal one.
    pub fn has_errors(&self) -> bool {
        self.error_count > 0 || self.fatal_error.is_some()
    }

    /// Records waiting inside the endpoint: the input buffer, or the output
    /// queue plus the output buffer.
    pub fn backlog_records(&self) -> i64 {
        match self.kind {
            ConnectorKind::Input => self.buffered_records,
            ConnectorKind::Output => self.queued_records + self.buffered_records,
        }
    }

    /// One state word: fatal errors win, then health, pause, and progress.
    pub fn status(&self) -> ConnectorStatus {
        if self.fatal_error.is_some() {
            return ConnectorStatus::Fatal;
        }
        if self
            .health
            .as_ref()
            .is_some_and(|health| !health.is_healthy)
        {
            return ConnectorStatus::Unhealthy;
        }
        if self.paused {
            return ConnectorStatus::Paused;
        }
        if self.end_of_input {
            return ConnectorStatus::Eoi;
        }
        ConnectorStatus::Active
    }
}

/// The error lists of a full connector status, newest first.
fn recent_errors(connector: &Value) -> Vec<ConnectorError> {
    let lists = [
        ("parse_errors", ConnectorErrorKind::Parse),
        ("encode_errors", ConnectorErrorKind::Encode),
        ("transport_errors", ConnectorErrorKind::Transport),
    ];
    let mut errors: Vec<ConnectorError> = lists
        .iter()
        .flat_map(|(key, kind)| {
            json::array(connector, key)
                .iter()
                .map(|error| ConnectorError {
                    kind: *kind,
                    at_millis: json::timestamp_millis(error, "timestamp"),
                    index: json::integer_or_zero(error, "index"),
                    message: json::string(error, "message")
                        .map(|message| terminal_safe_multiline(&message))
                        .unwrap_or_default(),
                })
        })
        .collect();
    errors.sort_by_key(|error| std::cmp::Reverse((error.at_millis, error.index)));
    errors
}

/// A pipeline's global metrics plus its connector rows.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PipelineStats {
    pub state: String,
    pub total_input_records: i64,
    pub total_processed_records: i64,
    pub total_completed_records: i64,
    pub buffered_input_records: i64,
    pub rss_bytes: i64,
    pub storage_bytes: i64,
    pub cpu_msecs: i64,
    pub uptime_msecs: i64,
    pub incarnation_uuid: String,
    /// `low`, `moderate`, `high`, or `critical`; meaningful when the
    /// pipeline runs with a configured `max_rss`.
    pub memory_pressure: String,
    pub pipeline_complete: bool,
    /// Every scalar in `global_metrics`, preserved for the metrics table.
    pub all_metrics: BTreeMap<String, Value>,
    pub inputs: Vec<ConnectorRow>,
    pub outputs: Vec<ConnectorRow>,
}

impl PipelineStats {
    pub fn from_json(stats: &Value) -> Self {
        let global = stats.get("global_metrics").cloned().unwrap_or(Value::Null);
        let all_metrics = global
            .as_object()
            .map(|fields| {
                fields
                    .iter()
                    .map(|(key, value)| (terminal_safe(key), value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Self {
            state: json::string(&global, "state").unwrap_or_else(|| "Unknown".to_string()),
            total_input_records: json::integer_or_zero(&global, "total_input_records"),
            total_processed_records: json::integer_or_zero(&global, "total_processed_records"),
            total_completed_records: json::integer_or_zero(&global, "total_completed_records"),
            buffered_input_records: json::integer_or_zero(&global, "buffered_input_records"),
            rss_bytes: json::integer_or_zero(&global, "rss_bytes"),
            storage_bytes: json::integer_or_zero(&global, "storage_bytes"),
            cpu_msecs: json::integer_or_zero(&global, "cpu_msecs"),
            uptime_msecs: json::integer_or_zero(&global, "uptime_msecs"),
            incarnation_uuid: json::string(&global, "incarnation_uuid").unwrap_or_default(),
            memory_pressure: json::string(&global, "memory_pressure")
                .map(|pressure| terminal_safe(&pressure))
                .unwrap_or_else(|| "low".to_string()),
            pipeline_complete: json::boolean(&global, "pipeline_complete"),
            all_metrics,
            inputs: json::array(stats, "inputs")
                .iter()
                .map(|connector| ConnectorRow::from_json(connector, ConnectorKind::Input))
                .collect(),
            outputs: json::array(stats, "outputs")
                .iter()
                .map(|connector| ConnectorRow::from_json(connector, ConnectorKind::Output))
                .collect(),
        }
    }

    /// Total input bytes; prefers the engine's own counter and falls back to
    /// summing connectors for servers that do not report it.
    pub fn total_input_bytes(&self) -> i64 {
        self.all_metrics
            .get("total_input_bytes")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| self.inputs.iter().map(|connector| connector.bytes).sum())
    }

    /// All connectors, inputs first.
    pub fn connectors(&self) -> impl Iterator<Item = &ConnectorRow> {
        self.inputs.iter().chain(self.outputs.iter())
    }

    /// Sum of connector-level errors.
    pub fn connector_error_count(&self) -> i64 {
        self.connectors()
            .map(|connector| connector.error_count)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectorKind, PipelineStats};
    use serde_json::json;

    fn sample() -> serde_json::Value {
        json!({
            "global_metrics": {
                "state": "Running",
                "total_input_records": 100,
                "total_processed_records": 90,
                "total_completed_records": 80,
                "buffered_input_records": 10,
                "rss_bytes": 1024,
                "storage_bytes": 2048,
                "cpu_msecs": 500,
                "uptime_msecs": 60_000,
                "incarnation_uuid": "abc",
                "pipeline_complete": false
            },
            "inputs": [{
                "endpoint_name": "orders_in",
                "config": {"stream": "orders"},
                "paused": false,
                "barrier": false,
                "metrics": {
                    "total_records": 100, "total_bytes": 5_000,
                    "buffered_records": 10, "num_parse_errors": 1,
                    "num_transport_errors": 2, "end_of_input": false
                }
            }],
            "outputs": [{
                "endpoint_name": "totals_out",
                "config": {"stream": "totals"},
                "metrics": {
                    "transmitted_records": 42, "transmitted_bytes": 900,
                    "num_encode_errors": 3, "num_transport_errors": 4
                }
            }]
        })
    }

    #[test]
    fn stats_expose_global_counters_and_the_raw_metric_map() {
        let stats = PipelineStats::from_json(&sample());
        assert_eq!(stats.state, "Running");
        assert_eq!(stats.total_processed_records, 90);
        assert_eq!(stats.total_input_bytes(), 5_000);
        assert_eq!(stats.all_metrics["rss_bytes"], json!(1024));
        assert_eq!(stats.connectors().count(), 2);
        assert_eq!(stats.connector_error_count(), 10);
    }

    #[test]
    fn connector_counters_use_direction_specific_field_names() {
        let stats = PipelineStats::from_json(&sample());
        let input = &stats.inputs[0];
        assert_eq!(input.kind, ConnectorKind::Input);
        assert_eq!(input.records, 100);
        assert_eq!(input.error_count, 3);
        assert_eq!(input.relation, "orders");
        let output = &stats.outputs[0];
        assert_eq!(output.records, 42);
        assert_eq!(output.error_count, 7);
        assert_eq!(output.kind.label(), "OUT");
    }

    #[test]
    fn connector_name_drops_only_the_relation_qualifier() {
        let mut stats = PipelineStats::from_json(&sample());
        let input = &mut stats.inputs[0];
        assert_eq!(input.connector_name(), "orders_in");
        input.endpoint_name = "orders.orders_in".to_string();
        assert_eq!(input.connector_name(), "orders_in");
        input.endpoint_name = "orders.a.b".to_string();
        assert_eq!(input.connector_name(), "a.b");
        input.endpoint_name = "orders_v2.c".to_string();
        assert_eq!(input.connector_name(), "orders_v2.c");
        input.endpoint_name = "orders.".to_string();
        assert_eq!(input.connector_name(), "orders.");
    }

    #[test]
    fn connector_status_prioritizes_fatal_over_health_over_paused_over_eoi() {
        use super::{ConnectorHealth, ConnectorStatus};
        let mut stats = PipelineStats::from_json(&sample());
        let input = &mut stats.inputs[0];
        assert_eq!(input.status(), ConnectorStatus::Active);
        input.end_of_input = true;
        assert_eq!(input.status(), ConnectorStatus::Eoi);
        input.paused = true;
        assert_eq!(input.status(), ConnectorStatus::Paused);
        input.health = Some(ConnectorHealth {
            is_healthy: false,
            description: None,
        });
        assert_eq!(input.status(), ConnectorStatus::Unhealthy);
        input.fatal_error = Some("boom".to_string());
        assert_eq!(input.status(), ConnectorStatus::Fatal);
        assert_eq!(input.status().label(), "Fatal");
        assert!(
            ConnectorStatus::Fatal < ConnectorStatus::Eoi,
            "severity orders first"
        );
    }

    #[test]
    fn input_connectors_expose_latency_health_frontier_and_error_split() {
        let input = super::ConnectorRow::from_json(
            &json!({
                "endpoint_name": "orders.kafka",
                "config": {"stream": "orders"},
                "barrier": true,
                "health": {"status": "Unhealthy", "description": "broker down"},
                "completed_frontier": {
                    "metadata": {"offsets": [7]},
                    "ingested_at": "2026-01-02T03:04:05Z",
                    "processed_at": "2026-01-02T03:04:06Z",
                    "completed_at": "2026-01-02T03:04:07Z"
                },
                "metrics": {
                    "total_records": 10, "buffered_bytes": 64,
                    "num_parse_errors": 2, "num_transport_errors": 1,
                    "processing_latency_p99_micros": 1_500
                }
            }),
            ConnectorKind::Input,
        );
        assert!(input.barrier);
        assert_eq!((input.parse_errors, input.transport_errors), (2, 1));
        assert_eq!(input.error_count, 3);
        assert_eq!(input.buffered_bytes, 64);
        assert_eq!(input.latency_p99_micros, Some(1_500));
        let health = input.health.as_ref().expect("health parsed");
        assert!(!health.is_healthy);
        assert_eq!(health.description.as_deref(), Some("broker down"));
        let frontier = input.frontier.as_ref().expect("frontier parsed");
        assert_eq!(frontier.metadata, json!({"offsets": [7]}));
        assert_eq!(frontier.ingested_at_millis, Some(1_767_323_045_000));
        assert_eq!(frontier.completed_at_millis, Some(1_767_323_047_000));
    }

    #[test]
    fn output_connectors_expose_queues_memory_and_progress() {
        let output = super::ConnectorRow::from_json(
            &json!({
                "endpoint_name": "totals.delta",
                "config": {"stream": "totals"},
                "health": {"status": "Healthy"},
                "metrics": {
                    "transmitted_records": 5, "queued_records": 3, "queued_batches": 1,
                    "buffered_records": 4, "buffered_batches": 2, "memory": 4096,
                    "total_processed_input_records": 90, "batch_records_written": 12,
                    "num_encode_errors": 1
                }
            }),
            ConnectorKind::Output,
        );
        assert_eq!(output.backlog_records(), 7, "queue plus buffer");
        assert_eq!((output.queued_batches, output.buffered_batches), (1, 2));
        assert_eq!(output.memory_bytes, 4_096);
        assert_eq!(output.processed_input_records, 90);
        assert_eq!(output.batch_records_written, Some(12));
        assert!(
            output
                .health
                .as_ref()
                .is_some_and(|health| health.is_healthy)
        );
        assert_eq!(output.latency_p99_micros, None, "inputs only");
        assert!(output.frontier.is_none());
    }

    #[test]
    fn recent_errors_merge_every_list_newest_first() {
        use super::ConnectorErrorKind;
        let input = super::ConnectorRow::from_json(
            &json!({
                "endpoint_name": "orders.kafka",
                "parse_errors": [
                    {"timestamp": "2026-01-02T03:04:05Z", "index": 1, "tag": "json",
                     "message": "bad row\nline 2"}
                ],
                "transport_errors": [
                    {"timestamp": "2026-01-02T03:04:09Z", "index": 4, "message": "reset"},
                    {"timestamp": "2026-01-02T03:04:09Z", "index": 5, "message": "refused"}
                ]
            }),
            ConnectorKind::Input,
        );
        let kinds: Vec<ConnectorErrorKind> =
            input.recent_errors.iter().map(|error| error.kind).collect();
        assert_eq!(
            kinds,
            vec![
                ConnectorErrorKind::Transport,
                ConnectorErrorKind::Transport,
                ConnectorErrorKind::Parse
            ]
        );
        assert_eq!(
            input.recent_errors[0].message, "refused",
            "the higher index wins a shared millisecond"
        );
        assert_eq!(input.recent_errors[2].message, "bad row\nline 2");
        assert!(
            PipelineStats::from_json(&sample()).inputs[0]
                .recent_errors
                .is_empty(),
            "/stats omits the lists"
        );
    }

    #[test]
    fn pressure_levels_use_the_engine_thresholds() {
        use super::pressure_level;
        assert_eq!(pressure_level(0, 1_000), "low");
        assert_eq!(pressure_level(849, 1_000), "low");
        assert_eq!(pressure_level(850, 1_000), "moderate");
        assert_eq!(pressure_level(900, 1_000), "high");
        assert_eq!(pressure_level(950, 1_000), "critical");
        assert_eq!(pressure_level(2_000, 1_000), "critical");
        assert_eq!(pressure_level(500, 0), "low", "no quota, no pressure");
        assert_eq!(pressure_level(-5, 1_000), "low");
    }

    #[test]
    fn foreign_payloads_produce_empty_stats() {
        let stats = PipelineStats::from_json(&json!("nonsense"));
        assert_eq!(stats.state, "Unknown");
        assert!(stats.inputs.is_empty());
        assert!(stats.all_metrics.is_empty());
    }
}
