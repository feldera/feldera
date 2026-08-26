//! SQL hotspot report: circuit profile joined with the dataflow graph.
//!
//! `GET /v0/pipelines/{name}/dataflow_graph` returns the SQL compiler's MIR;
//! each MIR node carries a `persistent_id` and SQL source positions. Profiled
//! circuit operators carry the same `persistent_id`, which links runtime cost
//! back to lines of SQL.

use serde_json::Value;
use std::collections::HashMap;

use super::json;
use super::profile::CircuitProfile;
use super::text::terminal_safe;

/// An inclusive span of SQL source, 1-based.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SourceSpan {
    pub start_line: usize,
    pub start_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

/// What one MIR node contributes to the join: its relation and SQL spans.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MirTarget {
    relation: Option<String>,
    operation: Option<String>,
    spans: Vec<SourceSpan>,
}

/// Index of the dataflow graph keyed by `persistent_id`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataflowIndex {
    targets: HashMap<String, MirTarget>,
}

impl DataflowIndex {
    pub fn from_json(dataflow: &Value) -> Self {
        let mut index = Self::default();
        let Some(mir) = dataflow.get("mir").and_then(Value::as_object) else {
            return index;
        };
        for node in mir.values() {
            index.add_node(node);
        }
        index
    }

    fn add_node(&mut self, node: &Value) {
        let operation = json::string(node, "operation");
        if operation.as_deref() == Some("nested") {
            // Nested nodes hold their children as extra object fields.
            for (key, child) in node.as_object().into_iter().flatten() {
                if child.is_object() && child.get("operation").is_some() && key != "operation" {
                    self.add_node(child);
                }
            }
            return;
        }
        let Some(persistent_id) = json::string(node, "persistent_id") else {
            return;
        };
        let relation = json::string(node, "table")
            .or_else(|| json::string(node, "view"))
            .map(|relation| terminal_safe(&relation));
        let spans = json::array(node, "positions")
            .iter()
            .filter_map(parse_span)
            .collect();
        self.targets.insert(
            persistent_id,
            MirTarget {
                relation,
                operation: operation.map(|operation| terminal_safe(&operation)),
                spans,
            },
        );
    }

    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

fn parse_span(position: &Value) -> Option<SourceSpan> {
    let start_line = json::integer(position, "start_line_number")? as usize;
    Some(SourceSpan {
        start_line,
        start_column: json::integer_or_zero(position, "start_column") as usize,
        end_line: json::integer(position, "end_line_number").unwrap_or(start_line as i64) as usize,
        end_column: json::integer_or_zero(position, "end_column") as usize,
    })
}

/// Which cost drives hotspot ranking and SQL heat.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CostMetric {
    #[default]
    Time,
    Memory,
    Records,
}

impl CostMetric {
    pub const ALL: [Self; 3] = [Self::Time, Self::Memory, Self::Records];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Time => "TIME",
            Self::Memory => "MEMORY",
            Self::Records => "STATE RECORDS",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::Time => Self::Memory,
            Self::Memory => Self::Records,
            Self::Records => Self::Time,
        }
    }
}

/// One ranked hotspot row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Hotspot {
    pub node_id: String,
    pub operator: String,
    pub relation: Option<String>,
    pub time_seconds: f64,
    pub memory_bytes: i64,
    pub state_records: i64,
    pub invocations: i64,
    pub spans: Vec<SourceSpan>,
}

impl Hotspot {
    pub fn cost(&self, metric: CostMetric) -> f64 {
        match metric {
            CostMetric::Time => self.time_seconds,
            CostMetric::Memory => self.memory_bytes as f64,
            CostMetric::Records => self.state_records as f64,
        }
    }

    /// First SQL line this hotspot points at.
    pub fn first_line(&self) -> Option<usize> {
        self.spans.iter().map(|span| span.start_line).min()
    }
}

/// The joined profiler result: ranked hotspots plus per-line SQL heat.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HotspotReport {
    pub hotspots: Vec<Hotspot>,
    pub worker_count: usize,
    pub total_time_seconds: f64,
    pub mapped_count: usize,
}

impl HotspotReport {
    /// Keep the ranking table focused on operators that cost something.
    const MAX_HOTSPOTS: usize = 200;

    pub fn build(profile: &CircuitProfile, dataflow: &DataflowIndex) -> Self {
        let mut hotspots: Vec<Hotspot> = profile
            .operators
            .iter()
            .filter(|operator| {
                operator.runtime_seconds > 0.0
                    || operator.memory_bytes > 0
                    || operator.state_records > 0
            })
            .map(|operator| {
                // Circuit persistent ids may extend the MIR id with an
                // operator-role suffix ("<hash>.shard_accintegral"); fall
                // back to the bare hash when the full id does not match.
                let target = operator.persistent_id.as_ref().and_then(|id| {
                    dataflow.targets.get(id).or_else(|| {
                        dataflow
                            .targets
                            .get(id.split_once('.').map(|(base, _)| base).unwrap_or(id))
                    })
                });
                Hotspot {
                    node_id: operator.node_id.clone(),
                    operator: target
                        .and_then(|target| target.operation.clone())
                        .map(|operation| format!("{operation} · {}", operator.label))
                        .unwrap_or_else(|| operator.label.clone()),
                    relation: target.and_then(|target| target.relation.clone()),
                    time_seconds: operator.runtime_seconds,
                    memory_bytes: operator.memory_bytes,
                    state_records: operator.state_records,
                    invocations: operator.invocations,
                    spans: target
                        .map(|target| target.spans.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();
        hotspots.truncate(Self::MAX_HOTSPOTS);
        let mapped_count = hotspots
            .iter()
            .filter(|hotspot| !hotspot.spans.is_empty())
            .count();
        Self {
            hotspots,
            worker_count: profile.worker_count,
            total_time_seconds: profile.total_runtime_seconds,
            mapped_count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hotspots.is_empty()
    }

    /// Total cost under the chosen metric, for share-of-total columns.
    pub fn total_cost(&self, metric: CostMetric) -> f64 {
        self.hotspots
            .iter()
            .map(|hotspot| hotspot.cost(metric))
            .sum()
    }

    /// Hotspot indices reordered for the chosen metric, costliest first.
    pub fn ranking(&self, metric: CostMetric) -> Vec<usize> {
        let mut order: Vec<usize> = (0..self.hotspots.len()).collect();
        order.sort_by(|left, right| {
            self.hotspots[*right]
                .cost(metric)
                .total_cmp(&self.hotspots[*left].cost(metric))
        });
        order
    }

    /// Heat per SQL line in `0.0..=1.0`, sized for `line_count` lines.
    pub fn line_heat(&self, metric: CostMetric, line_count: usize) -> Vec<f64> {
        let mut heat = vec![0.0; line_count];
        for hotspot in &self.hotspots {
            let cost = hotspot.cost(metric);
            if cost <= 0.0 {
                continue;
            }
            for span in &hotspot.spans {
                let first = span.start_line.max(1) - 1;
                let last = span.end_line.max(span.start_line).max(1) - 1;
                // Spread the cost so a whole-view span does not drown the
                // single line of an expensive expression.
                let per_line = cost / (last - first + 1) as f64;
                for line in heat.iter_mut().take(last + 1).skip(first) {
                    *line += per_line;
                }
            }
        }
        let peak = heat.iter().cloned().fold(0.0, f64::max);
        if peak > 0.0 {
            for line in &mut heat {
                *line /= peak;
            }
        }
        heat
    }
}

#[cfg(test)]
mod tests {
    use super::{CostMetric, DataflowIndex, HotspotReport};
    use crate::model::profile::CircuitProfile;
    use serde_json::json;

    fn dataflow() -> DataflowIndex {
        DataflowIndex::from_json(&json!({
            "calcite_plan": {},
            "mir": {
                "m1": {
                    "operation": "join",
                    "persistent_id": "join-abc",
                    "view": "expensive_view",
                    "positions": [
                        {"start_line_number": 2, "start_column": 1, "end_line_number": 3, "end_column": 10}
                    ]
                },
                "m2": {
                    "operation": "nested",
                    "child": {
                        "operation": "map",
                        "persistent_id": "map-def",
                        "positions": [{"start_line_number": 5, "start_column": 1, "end_line_number": 5, "end_column": 4}]
                    }
                },
                "m3": {"operation": "constant"}
            }
        }))
    }

    fn profile() -> CircuitProfile {
        CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {
                "n0_1": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 3, "nanos": 0}}},
                    {"metric_id": "persistent_id", "value": {"type": "string", "value": "join-abc"}}
                ],
                "n0_2": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}},
                    {"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 4096}},
                    {"metric_id": "persistent_id", "value": {"type": "string", "value": "map-def"}}
                ],
                "n0_3": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 0, "nanos": 0}}}
                ]
            }}]
        }))
    }

    #[test]
    fn hotspots_join_profile_nodes_to_sql_spans() {
        let report = HotspotReport::build(&profile(), &dataflow());
        assert_eq!(report.hotspots.len(), 2);
        assert_eq!(report.mapped_count, 2);
        let top = &report.hotspots[0];
        assert_eq!(top.relation.as_deref(), Some("expensive_view"));
        assert_eq!(top.first_line(), Some(2));
        assert!(top.operator.starts_with("join"));
    }

    #[test]
    fn ranking_reorders_by_the_chosen_metric() {
        let report = HotspotReport::build(&profile(), &dataflow());
        assert_eq!(report.ranking(CostMetric::Time), vec![0, 1]);
        assert_eq!(report.ranking(CostMetric::Memory), vec![1, 0]);
        assert!(report.total_cost(CostMetric::Memory) == 4096.0);
    }

    #[test]
    fn line_heat_normalizes_to_the_hottest_line() {
        let report = HotspotReport::build(&profile(), &dataflow());
        let heat = report.line_heat(CostMetric::Time, 6);
        assert_eq!(heat.len(), 6);
        // Lines 2 and 3 split 3s of join time; line 5 has 1s of map time.
        assert_eq!(heat[1], 1.0);
        assert_eq!(heat[2], 1.0);
        assert!((heat[4] - 1.0 / 1.5).abs() < 1e-9);
        assert_eq!(heat[0], 0.0);
    }

    #[test]
    fn spans_beyond_the_sql_are_clipped() {
        let report = HotspotReport::build(&profile(), &dataflow());
        let heat = report.line_heat(CostMetric::Time, 2);
        assert_eq!(heat.len(), 2);
        assert_eq!(heat[1], 1.0);
    }

    #[test]
    fn suffixed_persistent_ids_fall_back_to_the_bare_hash() {
        let profile = CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {
                "n0_9": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}},
                    {"metric_id": "persistent_id", "value": {"type": "string", "value": "join-abc.shard_accintegral"}}
                ]
            }}]
        }));
        let report = HotspotReport::build(&profile, &dataflow());
        assert_eq!(report.mapped_count, 1);
        assert_eq!(
            report.hotspots[0].relation.as_deref(),
            Some("expensive_view")
        );
    }

    #[test]
    fn unmapped_profiles_still_rank() {
        let report = HotspotReport::build(&profile(), &DataflowIndex::default());
        assert_eq!(report.mapped_count, 0);
        assert!(!report.is_empty());
        assert!(
            report
                .line_heat(CostMetric::Time, 4)
                .iter()
                .all(|heat| *heat == 0.0)
        );
    }

    #[test]
    fn cost_metric_cycles_through_all_choices() {
        assert_eq!(CostMetric::Time.next(), CostMetric::Memory);
        assert_eq!(CostMetric::Memory.next(), CostMetric::Records);
        assert_eq!(CostMetric::Records.next(), CostMetric::Time);
        assert_eq!(CostMetric::Memory.label(), "MEMORY");
    }
}
