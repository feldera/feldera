//! Circuit profile from `GET /v0/pipelines/{name}/circuit_json_profile`.
//!
//! The payload is a `DbspProfile`: one metadata map per worker keyed by node
//! id (`n0_123`), each entry a list of `{metric_id, labels, value}` readings,
//! plus a visual graph whose labels name the operators. Readings are summed
//! across workers where that is meaningful (times, bytes, counts).

use serde_json::Value;
use std::collections::BTreeMap;

use super::json;
use super::text::terminal_safe;

/// A parsed `MetaItem` reading.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    Count(i64),
    Bytes(i64),
    Seconds(f64),
    Percent { numerator: i64, denominator: i64 },
    Text(String),
    Flag(bool),
    Other(Value),
}

impl MetricValue {
    pub fn from_json(value: &Value) -> Self {
        let type_name = json::string(value, "type").unwrap_or_default();
        let inner = value.get("value").cloned().unwrap_or(Value::Null);
        match type_name.as_str() {
            "count" | "int" => Self::Count(inner.as_i64().unwrap_or(0)),
            "bytes" => Self::Bytes(inner.as_i64().unwrap_or(0)),
            "duration" => Self::Seconds(
                json::integer_or_zero(&inner, "secs") as f64
                    + json::integer_or_zero(&inner, "nanos") as f64 / 1e9,
            ),
            "percent" => Self::Percent {
                numerator: json::integer_or_zero(&inner, "numerator"),
                denominator: json::integer_or_zero(&inner, "denominator"),
            },
            "string" => Self::Text(terminal_safe(inner.as_str().unwrap_or(""))),
            "bool" => Self::Flag(inner.as_bool().unwrap_or(false)),
            _ => Self::Other(value.clone()),
        }
    }

    /// Sum two readings of the same metric across workers, when meaningful.
    fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Count(total), Self::Count(other)) => *total += other,
            (Self::Bytes(total), Self::Bytes(other)) => *total += other,
            (Self::Seconds(total), Self::Seconds(other)) => *total += other,
            (
                Self::Percent {
                    numerator,
                    denominator,
                },
                Self::Percent {
                    numerator: other_numerator,
                    denominator: other_denominator,
                },
            ) => {
                *numerator += other_numerator;
                *denominator += other_denominator;
            }
            // Strings, flags, and complex values are per-worker labels; the
            // first worker's value stands for all of them.
            _ => {}
        }
    }

    pub fn display(&self) -> String {
        match self {
            Self::Count(count) => crate::ui::format::compact_number(*count),
            Self::Bytes(bytes) => crate::ui::format::human_bytes(*bytes),
            Self::Seconds(seconds) => format!("{seconds:.3}s"),
            Self::Percent {
                numerator,
                denominator,
            } => {
                if *denominator == 0 {
                    "-".to_string()
                } else {
                    format!("{:.1}%", *numerator as f64 * 100.0 / *denominator as f64)
                }
            }
            Self::Text(text) => text.clone(),
            Self::Flag(flag) => flag.to_string(),
            Self::Other(_) => "…".to_string(),
        }
    }
}

/// One worker's share of the costs the hotspots rank by.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WorkerCost {
    pub runtime_seconds: f64,
    pub memory_bytes: i64,
    pub storage_bytes: i64,
    pub state_records: i64,
}

impl WorkerCost {
    fn from_readings(readings: &Value) -> Self {
        let mut cost = Self::default();
        for reading in json::array_or_empty(readings) {
            let value = || MetricValue::from_json(reading.get("value").unwrap_or(&Value::Null));
            match json::string(reading, "metric_id").as_deref() {
                Some("runtime_seconds") => {
                    if let MetricValue::Seconds(seconds) = value() {
                        cost.runtime_seconds += seconds;
                    }
                }
                Some("used_memory_bytes") => {
                    if let MetricValue::Bytes(bytes) = value() {
                        cost.memory_bytes += bytes;
                    }
                }
                Some("spine_storage_size_bytes") => {
                    if let MetricValue::Bytes(bytes) = value() {
                        cost.storage_bytes += bytes;
                    }
                }
                Some("state_records_count") => {
                    if let MetricValue::Count(count) = value() {
                        cost.state_records += count;
                    }
                }
                _ => {}
            }
        }
        cost
    }
}

/// One circuit operator with metrics aggregated across workers.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OperatorProfile {
    pub node_id: String,
    pub label: String,
    pub persistent_id: Option<String>,
    pub runtime_seconds: f64,
    pub memory_bytes: i64,
    /// Bytes the operator's spine keeps in storage.
    pub storage_bytes: i64,
    pub state_records: i64,
    pub invocations: i64,
    pub metrics: BTreeMap<String, MetricValue>,
    /// Indexed by worker; a worker without the operator costs nothing.
    pub worker_costs: Vec<WorkerCost>,
}

/// Memory the circuit reports outside any one operator, summed over workers.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CircuitMemory {
    /// In-memory state of every operator together.
    pub state_bytes: i64,
    pub bloom_filter_bytes: i64,
    pub buffer_cache_bytes: i64,
    pub buffer_cache_capacity_bytes: i64,
}

impl CircuitMemory {
    /// Add one worker's root-node readings. The foreground and background
    /// threads of a worker can share one buffer cache (the runtime dedups
    /// caches by backend); identical readings count once.
    fn add_worker(&mut self, readings: &Value) {
        let mut caches: Vec<(i64, i64)> = Vec::new();
        for reading in json::array_or_empty(readings) {
            let value = reading.get("value").unwrap_or(&Value::Null);
            match json::string(reading, "metric_id").as_deref() {
                Some("used_memory_bytes") => self.state_bytes += bytes_value(value),
                Some("bloom_filter_size_bytes") => self.bloom_filter_bytes += bytes_value(value),
                Some("foreground_cache_occupancy" | "background_cache_occupancy") => {
                    let used = value.get("used").map_or(0, bytes_value);
                    let capacity = value.get("max").map_or(0, bytes_value);
                    if !caches.contains(&(used, capacity)) {
                        caches.push((used, capacity));
                    }
                }
                _ => {}
            }
        }
        for (used, capacity) in caches {
            self.buffer_cache_bytes += used;
            self.buffer_cache_capacity_bytes += capacity;
        }
    }
}

fn bytes_value(value: &Value) -> i64 {
    match MetricValue::from_json(value) {
        MetricValue::Bytes(bytes) => bytes,
        _ => 0,
    }
}

/// What the profile says about one metric, for grouping and ordering.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricInfo {
    pub name: String,
    pub category: String,
    /// Detail most readers can skip.
    pub is_advanced: bool,
    pub description: String,
}

/// The whole parsed circuit profile.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CircuitProfile {
    pub operators: Vec<OperatorProfile>,
    pub worker_count: usize,
    pub total_runtime_seconds: f64,
    pub memory: CircuitMemory,
    /// The profile's own description of its metrics, in its order.
    pub catalog: Vec<MetricInfo>,
    /// Dataflow edges `(from, to)` between node ids.
    pub edges: Vec<(String, String)>,
}

impl CircuitProfile {
    pub fn from_json(profile: &Value) -> Self {
        let labels = graph_labels(profile.get("graph").unwrap_or(&Value::Null));
        let workers = json::array(profile, "worker_profiles");
        let mut operators: BTreeMap<String, OperatorProfile> = BTreeMap::new();

        let mut memory = CircuitMemory::default();
        for (worker_index, worker) in workers.iter().enumerate() {
            let Some(metadata) = worker.get("metadata").and_then(Value::as_object) else {
                continue;
            };
            for (node_id, readings) in metadata {
                // The root pseudo-node aggregates the whole circuit.
                if node_id == "n" {
                    memory.add_worker(readings);
                    continue;
                }
                let operator =
                    operators
                        .entry(node_id.clone())
                        .or_insert_with(|| OperatorProfile {
                            node_id: terminal_safe(node_id),
                            label: labels
                                .get(node_id)
                                .cloned()
                                .unwrap_or_else(|| terminal_safe(node_id)),
                            worker_costs: vec![WorkerCost::default(); workers.len()],
                            ..Default::default()
                        });
                merge_readings(operator, readings);
                operator.worker_costs[worker_index] = WorkerCost::from_readings(readings);
            }
        }

        let mut operators: Vec<OperatorProfile> = operators.into_values().collect();
        for operator in &mut operators {
            operator.runtime_seconds = seconds_of(operator, "runtime_seconds");
            operator.memory_bytes = bytes_of(operator, "used_memory_bytes");
            operator.storage_bytes = bytes_of(operator, "spine_storage_size_bytes");
            operator.state_records = count_of(operator, "state_records_count");
            operator.invocations = count_of(operator, "invocations_count");
            operator.persistent_id = match operator.metrics.get("persistent_id") {
                Some(MetricValue::Text(id)) if !id.is_empty() => Some(id.clone()),
                _ => None,
            };
        }
        operators.sort_by(|left, right| {
            right
                .runtime_seconds
                .total_cmp(&left.runtime_seconds)
                .then_with(|| right.memory_bytes.cmp(&left.memory_bytes))
        });

        let total_runtime_seconds = operators
            .iter()
            .map(|operator| operator.runtime_seconds)
            .sum();
        Self {
            operators,
            worker_count: workers.len(),
            total_runtime_seconds,
            memory,
            catalog: json::array(profile, "metrics")
                .iter()
                .filter_map(|metric| {
                    Some(MetricInfo {
                        name: terminal_safe(&json::string(metric, "name")?),
                        category: json::string(metric, "category")
                            .map(|category| terminal_safe(&category))
                            .unwrap_or_default(),
                        is_advanced: json::boolean(metric, "advanced"),
                        description: json::string(metric, "description")
                            .map(|description| terminal_safe(&description))
                            .unwrap_or_default(),
                    })
                })
                .collect(),
            edges: json::array(profile.get("graph").unwrap_or(&Value::Null), "edges")
                .iter()
                .filter_map(|edge| {
                    Some((
                        terminal_safe(&json::string(edge, "from_node")?),
                        terminal_safe(&json::string(edge, "to_node")?),
                    ))
                })
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }
}

fn merge_readings(operator: &mut OperatorProfile, readings: &Value) {
    let Some(readings) = readings.as_array() else {
        return;
    };
    for reading in readings {
        let Some(metric_id) = json::string(reading, "metric_id") else {
            continue;
        };
        let value = MetricValue::from_json(reading.get("value").unwrap_or(&Value::Null));
        operator
            .metrics
            .entry(terminal_safe(&metric_id))
            .and_modify(|existing| existing.merge(&value))
            .or_insert(value);
    }
}

fn seconds_of(operator: &OperatorProfile, metric: &str) -> f64 {
    match operator.metrics.get(metric) {
        Some(MetricValue::Seconds(seconds)) => *seconds,
        _ => 0.0,
    }
}

fn bytes_of(operator: &OperatorProfile, metric: &str) -> i64 {
    match operator.metrics.get(metric) {
        Some(MetricValue::Bytes(bytes)) => *bytes,
        _ => 0,
    }
}

fn count_of(operator: &OperatorProfile, metric: &str) -> i64 {
    match operator.metrics.get(metric) {
        Some(MetricValue::Count(count)) => *count,
        _ => 0,
    }
}

/// Flatten the visual graph into `node id -> first label line`.
fn graph_labels(graph: &Value) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    collect_labels(graph.get("nodes").unwrap_or(&Value::Null), &mut labels);
    labels
}

fn collect_labels(node: &Value, labels: &mut BTreeMap<String, String>) {
    if let Some(simple) = node.get("Simple") {
        insert_label(simple, labels);
        return;
    }
    let cluster = node.get("Cluster").unwrap_or(node);
    insert_label(cluster, labels);
    for child in json::array(cluster, "nodes") {
        collect_labels(child, labels);
    }
}

fn insert_label(node: &Value, labels: &mut BTreeMap<String, String>) {
    let (Some(id), Some(label)) = (json::string(node, "id"), json::string(node, "label")) else {
        return;
    };
    // Dot labels separate lines with "\l" and append the operator's Rust
    // source location after " @ "; keep only the operator name.
    let first_line = label.split("\\l").next().unwrap_or(&label);
    let name = first_line.split(" @ ").next().unwrap_or(first_line).trim();
    if !name.is_empty() {
        labels.insert(id, terminal_safe(name));
    }
}

#[cfg(test)]
mod tests {
    use super::{CircuitProfile, MetricValue};
    use serde_json::json;

    fn reading(metric_id: &str, value: serde_json::Value) -> serde_json::Value {
        json!({"metric_id": metric_id, "value": value})
    }

    fn sample_profile() -> serde_json::Value {
        let join_readings = json!([
            reading(
                "runtime_seconds",
                json!({"type": "duration", "value": {"secs": 2, "nanos": 500_000_000}})
            ),
            reading("used_memory_bytes", json!({"type": "bytes", "value": 1024})),
            reading(
                "spine_storage_size_bytes",
                json!({"type": "bytes", "value": 4096})
            ),
            reading("state_records_count", json!({"type": "count", "value": 10})),
            reading("invocations_count", json!({"type": "count", "value": 5})),
            reading(
                "persistent_id",
                json!({"type": "string", "value": "join-abc"})
            ),
            reading(
                "nonblocking_percent",
                json!({"type": "percent", "value": {"numerator": 80, "denominator": 100}})
            ),
        ]);
        json!({
            "metrics": [],
            "worker_profiles": [
                {"metadata": {"n0_1": join_readings, "n": []}},
                {"metadata": {"n0_1": [
                    reading("runtime_seconds", json!({"type": "duration", "value": {"secs": 1, "nanos": 0}})),
                    reading("used_memory_bytes", json!({"type": "bytes", "value": 6}))
                ]}}
            ],
            "graph": {
                "nodes": {"id": "", "label": "", "nodes": [
                    {"Simple": {"id": "n0_1", "label": "join @ src/x.rs:1\\lmore"}},
                    {"Cluster": {"id": "c1", "label": "region", "nodes": [
                        {"Simple": {"id": "n0_2", "label": "map"}}
                    ]}}
                ]},
                "edges": [{"from_node": "n0_1", "to_node": "n0_2"}, {"to_node": "n0_9"}]
            }
        })
    }

    #[test]
    fn readings_are_summed_across_workers() {
        let profile = CircuitProfile::from_json(&sample_profile());
        assert_eq!(profile.worker_count, 2);
        let join = &profile.operators[0];
        assert_eq!(join.node_id, "n0_1");
        assert_eq!(join.label, "join");
        assert_eq!(join.runtime_seconds, 3.5);
        assert_eq!(join.memory_bytes, 1030);
        assert_eq!(join.storage_bytes, 4096);
        assert_eq!(join.state_records, 10);
        assert_eq!(join.persistent_id.as_deref(), Some("join-abc"));
        assert_eq!(profile.total_runtime_seconds, 3.5);
        assert_eq!(
            profile.edges,
            vec![("n0_1".to_string(), "n0_2".to_string())],
            "edges without both ends are dropped"
        );
    }

    #[test]
    fn per_worker_costs_keep_the_skew_the_sums_hide() {
        let profile = CircuitProfile::from_json(&sample_profile());
        let join = &profile.operators[0];
        assert_eq!(join.worker_costs.len(), 2);
        assert_eq!(join.worker_costs[0].runtime_seconds, 2.5);
        assert_eq!(join.worker_costs[1].runtime_seconds, 1.0);
        assert_eq!(join.worker_costs[0].memory_bytes, 1024);
        assert_eq!(join.worker_costs[0].storage_bytes, 4096);
        assert_eq!(join.worker_costs[1].storage_bytes, 0);
        assert_eq!(join.worker_costs[1].state_records, 0);
    }

    #[test]
    fn the_root_node_reports_circuit_memory_with_shared_caches_once() {
        let cache = |used: i64, max: i64| json!({"used": {"type": "bytes", "value": used}, "max": {"type": "bytes", "value": max}});
        let profile = CircuitProfile::from_json(&json!({
            "metrics": [
                {"name": "used_memory_bytes", "category": "State", "advanced": true,
                 "description": "Bytes used."},
                {"name": "steps_count", "category": "Time"}
            ],
            "worker_profiles": [
                {"metadata": {"n": [
                    reading("used_memory_bytes", json!({"type": "bytes", "value": 100})),
                    reading("bloom_filter_size_bytes", json!({"type": "bytes", "value": 7})),
                    reading("foreground_cache_occupancy", cache(30, 50)),
                    reading("background_cache_occupancy", cache(30, 50))
                ]}},
                {"metadata": {"n": [
                    reading("used_memory_bytes", json!({"type": "bytes", "value": 200})),
                    reading("foreground_cache_occupancy", cache(10, 50)),
                    reading("background_cache_occupancy", cache(5, 20))
                ]}}
            ]
        }));
        assert_eq!(profile.memory.state_bytes, 300);
        assert_eq!(profile.memory.bloom_filter_bytes, 7);
        assert_eq!(
            profile.memory.buffer_cache_bytes, 45,
            "30 once, then 10 + 5"
        );
        assert_eq!(profile.memory.buffer_cache_capacity_bytes, 120);
        assert_eq!(profile.catalog.len(), 2);
        assert!(profile.catalog[0].is_advanced);
        assert_eq!(profile.catalog[1].category, "Time");
        assert!(profile.operators.is_empty(), "the root is no operator");
    }

    #[test]
    fn the_root_pseudo_node_is_excluded() {
        let profile = CircuitProfile::from_json(&sample_profile());
        assert!(
            profile
                .operators
                .iter()
                .all(|operator| operator.node_id != "n")
        );
    }

    #[test]
    fn metric_values_parse_every_meta_item_shape() {
        assert_eq!(
            MetricValue::from_json(&json!({"type": "count", "value": 3})),
            MetricValue::Count(3)
        );
        assert_eq!(
            MetricValue::from_json(&json!({"type": "int", "value": 4})),
            MetricValue::Count(4)
        );
        assert_eq!(
            MetricValue::from_json(&json!({"type": "bytes", "value": 9})),
            MetricValue::Bytes(9)
        );
        assert_eq!(
            MetricValue::from_json(
                &json!({"type": "duration", "value": {"secs": 1, "nanos": 250_000_000}})
            ),
            MetricValue::Seconds(1.25)
        );
        assert_eq!(
            MetricValue::from_json(&json!({"type": "bool", "value": true})),
            MetricValue::Flag(true)
        );
        assert_eq!(
            MetricValue::from_json(&json!({"type": "string", "value": "hi"})),
            MetricValue::Text("hi".to_string())
        );
        assert!(matches!(
            MetricValue::from_json(&json!([1, 2, 3])),
            MetricValue::Other(_)
        ));
    }

    #[test]
    fn metric_display_is_compact_and_safe() {
        assert_eq!(MetricValue::Count(1_500).display(), "1.5K");
        assert_eq!(MetricValue::Bytes(2_048).display(), "2.0 KiB");
        assert_eq!(MetricValue::Seconds(0.5).display(), "0.500s");
        assert_eq!(
            MetricValue::Percent {
                numerator: 1,
                denominator: 4
            }
            .display(),
            "25.0%"
        );
        assert_eq!(
            MetricValue::Percent {
                numerator: 1,
                denominator: 0
            }
            .display(),
            "-"
        );
        assert_eq!(MetricValue::Flag(false).display(), "false");
        assert_eq!(MetricValue::Other(serde_json::Value::Null).display(), "…");
    }

    #[test]
    fn foreign_payloads_yield_an_empty_profile() {
        let profile = CircuitProfile::from_json(&json!({"unexpected": true}));
        assert!(profile.is_empty());
        assert_eq!(profile.worker_count, 0);
    }

    #[test]
    fn operators_sort_by_runtime_then_memory() {
        let profile = CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {
                "n0_1": [reading("used_memory_bytes", json!({"type": "bytes", "value": 10}))],
                "n0_2": [reading("used_memory_bytes", json!({"type": "bytes", "value": 20}))]
            }}]
        }));
        assert_eq!(profile.operators[0].node_id, "n0_2");
    }
}
