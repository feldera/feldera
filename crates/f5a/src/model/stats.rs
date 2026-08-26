//! Live engine statistics from `GET /v0/pipelines/{name}/stats`.

use serde_json::Value;
use std::collections::BTreeMap;

use super::json;
use super::text::terminal_safe;

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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
}

/// One connector endpoint with its operational counters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectorRow {
    pub kind: ConnectorKind,
    pub endpoint_name: String,
    /// Table (inputs) or view (outputs) the endpoint attaches to.
    pub relation: String,
    pub paused: bool,
    pub barrier: bool,
    pub records: i64,
    pub bytes: i64,
    pub buffered_records: i64,
    pub error_count: i64,
    pub end_of_input: bool,
    pub fatal_error: Option<String>,
    /// The connector's raw JSON, for the inspect overlay.
    pub raw: Value,
}

impl ConnectorRow {
    fn from_json(connector: &Value, kind: ConnectorKind) -> Self {
        let metrics = connector.get("metrics").cloned().unwrap_or(Value::Null);
        let (records, bytes, error_count) = match kind {
            ConnectorKind::Input => (
                json::integer_or_zero(&metrics, "total_records"),
                json::integer_or_zero(&metrics, "total_bytes"),
                json::integer_or_zero(&metrics, "num_parse_errors")
                    + json::integer_or_zero(&metrics, "num_transport_errors"),
            ),
            ConnectorKind::Output => (
                json::integer_or_zero(&metrics, "transmitted_records"),
                json::integer_or_zero(&metrics, "transmitted_bytes"),
                json::integer_or_zero(&metrics, "num_encode_errors")
                    + json::integer_or_zero(&metrics, "num_transport_errors"),
            ),
        };
        let relation = connector
            .get("config")
            .and_then(|config| {
                json::string(config, "stream").or_else(|| json::string(config, "relation"))
            })
            .map(|relation| terminal_safe(&relation))
            .unwrap_or_else(|| "?".to_string());
        Self {
            kind,
            endpoint_name: json::string(connector, "endpoint_name")
                .map(|name| terminal_safe(&name))
                .unwrap_or_else(|| "<unnamed>".to_string()),
            relation,
            paused: json::boolean(connector, "paused"),
            barrier: json::boolean(connector, "barrier"),
            records,
            bytes,
            buffered_records: json::integer_or_zero(&metrics, "buffered_records"),
            error_count,
            end_of_input: json::boolean(&metrics, "end_of_input"),
            fatal_error: json::string(connector, "fatal_error").map(|error| terminal_safe(&error)),
            raw: connector.clone(),
        }
    }

    /// Status label combining fatal errors, pause state, and progress.
    pub fn status_label(&self) -> &'static str {
        if self.fatal_error.is_some() {
            return "Fatal";
        }
        if self.paused {
            return "Paused";
        }
        if self.end_of_input {
            return "EOI";
        }
        "Active"
    }
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
    fn connector_status_prioritizes_fatal_over_paused_over_eoi() {
        let mut stats = PipelineStats::from_json(&sample());
        assert_eq!(stats.inputs[0].status_label(), "Active");
        stats.inputs[0].end_of_input = true;
        assert_eq!(stats.inputs[0].status_label(), "EOI");
        stats.inputs[0].paused = true;
        assert_eq!(stats.inputs[0].status_label(), "Paused");
        stats.inputs[0].fatal_error = Some("boom".to_string());
        assert_eq!(stats.inputs[0].status_label(), "Fatal");
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
