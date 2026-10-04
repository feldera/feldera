//! SQL hotspot report: circuit profile joined with the dataflow graph.
//!
//! `GET /v0/pipelines/{name}/dataflow_graph` returns the SQL compiler's MIR;
//! each MIR node carries a `persistent_id` and SQL source positions. Profiled
//! circuit operators carry the same `persistent_id`, which links runtime cost
//! back to lines of SQL.

use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use super::json;
use super::profile::{CircuitMemory, CircuitProfile, MetricInfo, MetricValue, WorkerCost};
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

impl MirTarget {
    /// Whether the node says where in the SQL it comes from.
    fn locates(&self) -> bool {
        self.relation.is_some() || !self.spans.is_empty()
    }
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

    /// The MIR node of a circuit persistent id. Circuit ids may extend the
    /// MIR id with an operator-role suffix (`<hash>.shard_accintegral`);
    /// the bare hash is the fallback.
    fn resolve(&self, persistent_id: &str) -> Option<&MirTarget> {
        self.targets.get(persistent_id).or_else(|| {
            let base = persistent_id
                .split_once('.')
                .map_or(persistent_id, |(base, _)| base);
            self.targets.get(base)
        })
    }
}

/// Finds the SQL owner of an operator the compiler did not map: traces,
/// delays, and exchanges hold or move their producer's output, so the
/// nearest mapped operator upstream owns them, else the nearest downstream.
struct OwnerSearch<'a> {
    upstream: HashMap<&'a str, Vec<&'a str>>,
    downstream: HashMap<&'a str, Vec<&'a str>>,
    owners: &'a HashMap<&'a str, &'a MirTarget>,
}

impl<'a> OwnerSearch<'a> {
    /// Search no further than this many edges: enough to cross the
    /// exchange, merge, and accumulator chain of a region; farther owners
    /// are guesses.
    const MAX_HOPS: usize = 16;

    fn new(edges: &'a [(String, String)], owners: &'a HashMap<&'a str, &'a MirTarget>) -> Self {
        let mut upstream: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut downstream: HashMap<&str, Vec<&str>> = HashMap::new();
        for (from, to) in edges {
            upstream.entry(to.as_str()).or_default().push(from.as_str());
            downstream
                .entry(from.as_str())
                .or_default()
                .push(to.as_str());
        }
        Self {
            upstream,
            downstream,
            owners,
        }
    }

    /// The nearest owner with a relation or SQL position, and its node id.
    fn nearest(&self, node_id: &str) -> Option<(&'a str, &'a MirTarget)> {
        self.breadth_first(node_id, &self.upstream)
            .or_else(|| self.breadth_first(node_id, &self.downstream))
    }

    fn breadth_first(
        &self,
        start: &str,
        links: &HashMap<&'a str, Vec<&'a str>>,
    ) -> Option<(&'a str, &'a MirTarget)> {
        let mut seen: std::collections::HashSet<&str> = [start].into();
        let mut frontier: Vec<&str> = vec![start];
        for _ in 0..Self::MAX_HOPS {
            let mut next = Vec::new();
            for node in frontier {
                for &neighbor in links.get(node).into_iter().flatten() {
                    if !seen.insert(neighbor) {
                        continue;
                    }
                    let owner = self.owners.get(neighbor).filter(|target| target.locates());
                    if let Some(target) = owner {
                        return Some((neighbor, target));
                    }
                    next.push(neighbor);
                }
            }
            if next.is_empty() {
                return None;
            }
            frontier = next;
        }
        None
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
    /// Bytes the operator's spine keeps in storage.
    Storage,
    Records,
}

impl CostMetric {
    pub const ALL: [Self; 4] = [Self::Time, Self::Memory, Self::Storage, Self::Records];

    pub const fn label(self) -> &'static str {
        match self {
            Self::Time => "TIME",
            Self::Memory => "MEMORY",
            Self::Storage => "STORAGE",
            Self::Records => "STATE RECORDS",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::Time => Self::Memory,
            Self::Memory => Self::Storage,
            Self::Storage => Self::Records,
            Self::Records => Self::Time,
        }
    }
}

/// Columns the hotspot table can sort by.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HotspotSort {
    /// The cost under the chosen [`CostMetric`].
    #[default]
    Cost,
    Skew,
    Operator,
    Relation,
    Line,
}

impl HotspotSort {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Cost => "cost",
            Self::Skew => "skew",
            Self::Operator => "operator",
            Self::Relation => "relation",
            Self::Line => "sql",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            Self::Cost => Self::Skew,
            Self::Skew => Self::Operator,
            Self::Operator => Self::Relation,
            Self::Relation => Self::Line,
            Self::Line => Self::Cost,
        }
    }
}

/// The hotspot table's sort: costliest first until the user picks another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HotspotOrder {
    pub column: HotspotSort,
    pub is_descending: bool,
}

impl Default for HotspotOrder {
    fn default() -> Self {
        Self {
            column: HotspotSort::Cost,
            is_descending: true,
        }
    }
}

/// One ranked hotspot row.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Hotspot {
    pub node_id: String,
    pub operator: String,
    pub relation: Option<String>,
    /// The node id of the neighbor whose relation and SQL lines this
    /// operator borrows, when it has no mapping of its own.
    pub inferred_from: Option<String>,
    pub persistent_id: Option<String>,
    pub time_seconds: f64,
    pub memory_bytes: i64,
    pub storage_bytes: i64,
    pub state_records: i64,
    pub invocations: i64,
    pub spans: Vec<SourceSpan>,
    /// Every reading of the operator, summed across workers.
    pub metrics: BTreeMap<String, MetricValue>,
    /// Indexed by worker.
    pub worker_costs: Vec<WorkerCost>,
}

impl Hotspot {
    pub fn cost(&self, metric: CostMetric) -> f64 {
        match metric {
            CostMetric::Time => self.time_seconds,
            CostMetric::Memory => self.memory_bytes as f64,
            CostMetric::Storage => self.storage_bytes as f64,
            CostMetric::Records => self.state_records as f64,
        }
    }

    /// Each worker's cost under `metric`, in worker order.
    pub fn worker_costs(&self, metric: CostMetric) -> Vec<f64> {
        self.worker_costs
            .iter()
            .map(|worker| match metric {
                CostMetric::Time => worker.runtime_seconds,
                CostMetric::Memory => worker.memory_bytes as f64,
                CostMetric::Storage => worker.storage_bytes as f64,
                CostMetric::Records => worker.state_records as f64,
            })
            .collect()
    }

    /// The busiest worker's cost over the mean across workers: `1.0` is
    /// perfectly balanced, the worker count means one worker does it all.
    /// `None` when nothing was spent.
    ///
    /// ```
    /// use f5a::model::dataflow::{CostMetric, Hotspot};
    /// use f5a::model::profile::WorkerCost;
    ///
    /// let worker = |runtime_seconds| WorkerCost { runtime_seconds, ..Default::default() };
    /// let hotspot = Hotspot {
    ///     worker_costs: vec![worker(3.0), worker(1.0)],
    ///     ..Default::default()
    /// };
    /// assert_eq!(hotspot.skew(CostMetric::Time), Some(1.5));
    /// assert_eq!(hotspot.skew(CostMetric::Memory), None);
    /// ```
    pub fn skew(&self, metric: CostMetric) -> Option<f64> {
        let costs = self.worker_costs(metric);
        let total: f64 = costs.iter().sum();
        if total <= 0.0 {
            return None;
        }
        let busiest = costs.iter().copied().fold(0.0, f64::max);
        Some(busiest * costs.len() as f64 / total)
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
    /// Operators the compiler mapped to SQL themselves.
    pub mapped_count: usize,
    /// Operators that borrow a neighbor's mapping.
    pub inferred_count: usize,
    pub memory: CircuitMemory,
    pub catalog: Vec<MetricInfo>,
}

impl HotspotReport {
    pub fn build(profile: &CircuitProfile, dataflow: &DataflowIndex) -> Self {
        let direct: HashMap<&str, &MirTarget> = profile
            .operators
            .iter()
            .filter_map(|operator| {
                let target = dataflow.resolve(operator.persistent_id.as_deref()?)?;
                Some((operator.node_id.as_str(), target))
            })
            .collect();
        let owners = OwnerSearch::new(&profile.edges, &direct);
        let hotspots: Vec<Hotspot> = profile
            .operators
            .iter()
            .filter(|operator| {
                operator.runtime_seconds > 0.0
                    || operator.memory_bytes > 0
                    || operator.storage_bytes > 0
                    || operator.state_records > 0
            })
            .map(|operator| {
                let own_target = direct.get(operator.node_id.as_str()).copied();
                // The compiler gives some of its own lowering steps (indexes,
                // maps, constants) no position; those borrow one too.
                let own_location = own_target.filter(|target| target.locates());
                let inferred = own_location
                    .is_none()
                    .then(|| owners.nearest(&operator.node_id))
                    .flatten();
                let target = own_location.or(inferred.map(|(_, target)| target));
                Hotspot {
                    node_id: operator.node_id.clone(),
                    // Only the operator's own MIR node names its operation.
                    operator: own_target
                        .and_then(|target| target.operation.clone())
                        .map(|operation| format!("{operation} · {}", operator.label))
                        .unwrap_or_else(|| operator.label.clone()),
                    relation: target.and_then(|target| target.relation.clone()),
                    inferred_from: inferred.map(|(owner, _)| owner.to_string()),
                    persistent_id: operator.persistent_id.clone(),
                    time_seconds: operator.runtime_seconds,
                    memory_bytes: operator.memory_bytes,
                    storage_bytes: operator.storage_bytes,
                    state_records: operator.state_records,
                    invocations: operator.invocations,
                    spans: target
                        .map(|target| target.spans.clone())
                        .unwrap_or_default(),
                    metrics: operator.metrics.clone(),
                    worker_costs: operator.worker_costs.clone(),
                }
            })
            .collect();
        let mapped_count = hotspots
            .iter()
            .filter(|hotspot| !hotspot.spans.is_empty() && hotspot.inferred_from.is_none())
            .count();
        let inferred_count = hotspots
            .iter()
            .filter(|hotspot| hotspot.inferred_from.is_some())
            .count();
        Self {
            hotspots,
            worker_count: profile.worker_count,
            total_time_seconds: profile.total_runtime_seconds,
            mapped_count,
            inferred_count,
            memory: profile.memory,
            catalog: profile.catalog.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hotspots.is_empty()
    }

    /// Name the relation of hotspots that point at SQL lines but whose MIR
    /// node names none (nodes inside a view): the statement around the
    /// line declares it.
    pub fn name_relations_from_sql(&mut self, sql: &str) {
        let statements = super::sql_locate::statement_spans(sql);
        for hotspot in &mut self.hotspots {
            if hotspot.relation.is_some() {
                continue;
            }
            hotspot.relation = hotspot
                .first_line()
                .and_then(|line| super::sql_locate::relation_at_line(&statements, line))
                .map(str::to_string);
        }
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
        self.order(metric, HotspotOrder::default())
    }

    /// Hotspot indices in display order; ties put the costlier first.
    pub fn order(&self, metric: CostMetric, order: HotspotOrder) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..self.hotspots.len()).collect();
        indices.sort_by(|&left_index, &right_index| {
            let (left, right) = (&self.hotspots[left_index], &self.hotspots[right_index]);
            let ordering = match order.column {
                HotspotSort::Cost => left.cost(metric).total_cmp(&right.cost(metric)),
                HotspotSort::Skew => left
                    .skew(metric)
                    .unwrap_or(0.0)
                    .total_cmp(&right.skew(metric).unwrap_or(0.0)),
                HotspotSort::Operator => left.operator.cmp(&right.operator),
                HotspotSort::Relation => left.relation.cmp(&right.relation),
                HotspotSort::Line => left.first_line().cmp(&right.first_line()),
            };
            let ordering = if order.is_descending {
                ordering.reverse()
            } else {
                ordering
            };
            ordering.then_with(|| right.cost(metric).total_cmp(&left.cost(metric)))
        });
        indices
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

    /// A big memory holder that costs no time must survive into the memory
    /// ranking, however many busier operators the circuit has.
    #[test]
    fn every_costed_operator_is_ranked_not_only_the_busiest() {
        let mut metadata = serde_json::Map::new();
        for index in 0..300 {
            metadata.insert(
                format!("n0_{index}"),
                json!([{"metric_id": "runtime_seconds",
                        "value": {"type": "duration", "value": {"secs": 10, "nanos": 0}}},
                       {"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 1}}]),
            );
        }
        metadata.insert(
            "n0_idle".to_string(),
            json!([{"metric_id": "used_memory_bytes",
                    "value": {"type": "bytes", "value": 1_000_000_000i64}}]),
        );
        let profile =
            CircuitProfile::from_json(&json!({"worker_profiles": [{"metadata": metadata}]}));
        let report = HotspotReport::build(&profile, &DataflowIndex::default());
        assert_eq!(report.hotspots.len(), 301);
        let top = report.ranking(CostMetric::Memory)[0];
        assert_eq!(report.hotspots[top].node_id, "n0_idle");
    }

    #[test]
    fn storage_ranks_spines_that_cost_nothing_else() {
        let profile = CircuitProfile::from_json(&json!({"worker_profiles": [
            {"metadata": {"spine": [{"metric_id": "spine_storage_size_bytes",
                                      "value": {"type": "bytes", "value": 300}}]}},
            {"metadata": {"spine": [{"metric_id": "spine_storage_size_bytes",
                                      "value": {"type": "bytes", "value": 100}}]}}
        ]}));
        let report = HotspotReport::build(&profile, &DataflowIndex::default());
        assert_eq!(report.hotspots.len(), 1, "storage alone makes a hotspot");
        let spine = &report.hotspots[0];
        assert_eq!(spine.cost(CostMetric::Storage), 400.0);
        assert_eq!(spine.skew(CostMetric::Storage), Some(1.5));
        assert_eq!(report.total_cost(CostMetric::Storage), 400.0);
    }

    #[test]
    fn every_column_sorts_both_ways_with_the_costlier_first_on_ties() {
        use super::{HotspotOrder, HotspotSort};
        let report = HotspotReport::build(&profile(), &dataflow());
        let first = |column, is_descending| {
            let order = report.order(
                CostMetric::Time,
                HotspotOrder {
                    column,
                    is_descending,
                },
            );
            report.hotspots[order[0]].node_id.clone()
        };
        assert_eq!(first(HotspotSort::Cost, true), "n0_1");
        assert_eq!(
            first(HotspotSort::Cost, false),
            "n0_2",
            "zero-cost operators drop out"
        );
        assert_eq!(first(HotspotSort::Operator, false), "n0_1", "join < map");
        assert_eq!(first(HotspotSort::Operator, true), "n0_2");
        assert_eq!(
            first(HotspotSort::Relation, true),
            "n0_1",
            "expensive_view > none"
        );
        assert_eq!(first(HotspotSort::Line, false), "n0_1", "line 2 < line 5");
        assert_eq!(
            first(HotspotSort::Skew, true),
            "n0_1",
            "equal skew: costlier first"
        );
        let mut column = HotspotSort::Cost;
        for _ in 0..5 {
            column = column.cycle();
        }
        assert_eq!(column, HotspotSort::Cost);
        assert_eq!(HotspotSort::Line.title(), "sql");
    }

    #[test]
    fn unmapped_operators_borrow_the_nearest_upstream_owner() {
        let dataflow = dataflow();
        let profile = CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {
                "join": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}},
                    {"metric_id": "persistent_id", "value": {"type": "string", "value": "join-abc"}}
                ],
                "trace": [{"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 9}}],
                "delay": [{"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 8}}],
                "source": [{"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 7}}],
                "island": [{"metric_id": "used_memory_bytes", "value": {"type": "bytes", "value": 6}}],
                "mapper": [
                    {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}},
                    {"metric_id": "persistent_id", "value": {"type": "string", "value": "map-def"}}
                ]
            }}],
            "graph": {"edges": [
                {"from_node": "join", "to_node": "trace"},
                {"from_node": "trace", "to_node": "mapper"},
                {"from_node": "trace", "to_node": "delay"},
                {"from_node": "delay", "to_node": "trace"},
                {"from_node": "source", "to_node": "join"}
            ]}
        }));
        let report = HotspotReport::build(&profile, &dataflow);
        let hotspot = |node: &str| {
            report
                .hotspots
                .iter()
                .find(|hotspot| hotspot.node_id == node)
                .expect("costed operator")
        };
        assert_eq!(hotspot("join").inferred_from, None, "its own mapping");
        assert_eq!(
            hotspot("trace").inferred_from.as_deref(),
            Some("join"),
            "upstream wins over the mapper downstream"
        );
        assert_eq!(hotspot("trace").relation.as_deref(), Some("expensive_view"));
        assert_eq!(hotspot("trace").first_line(), Some(2));
        assert_eq!(hotspot("trace").operator, "trace", "no borrowed operation");
        assert_eq!(
            hotspot("delay").inferred_from.as_deref(),
            Some("join"),
            "two hops, cycle-safe"
        );
        assert_eq!(
            hotspot("source").inferred_from.as_deref(),
            Some("join"),
            "downstream fallback"
        );
        assert_eq!(hotspot("island").relation, None, "no neighbors, no owner");
        assert_eq!((report.mapped_count, report.inferred_count), (2, 3));
    }

    #[test]
    fn lines_without_a_relation_take_the_statement_around_them() {
        let mut report = HotspotReport::build(&profile(), &dataflow());
        let map = |report: &HotspotReport| {
            report
                .hotspots
                .iter()
                .find(|hotspot| hotspot.node_id == "n0_2")
                .and_then(|hotspot| hotspot.relation.clone())
        };
        assert_eq!(map(&report), None, "the map node at line 5 names no view");
        report.name_relations_from_sql(
            "CREATE TABLE t (x INT);\nCREATE VIEW v AS\nSELECT x\nFROM t\nWHERE x > 0;",
        );
        assert_eq!(map(&report).as_deref(), Some("v"));
        let join = report
            .hotspots
            .iter()
            .find(|hotspot| hotspot.node_id == "n0_1");
        assert_eq!(
            join.and_then(|hotspot| hotspot.relation.as_deref()),
            Some("expensive_view"),
            "a named relation stays"
        );
    }

    #[test]
    fn nodes_the_compiler_gave_no_position_borrow_one_and_keep_their_operation() {
        let dataflow = DataflowIndex::from_json(&json!({"mir": {
            "m1": {"operation": "join", "persistent_id": "join-abc", "view": "v",
                   "positions": [{"start_line_number": 4}]},
            "m2": {"operation": "map_index", "persistent_id": "plumb"}
        }}));
        let profile = CircuitProfile::from_json(&json!({
            "worker_profiles": [{"metadata": {
                "join": [{"metric_id": "persistent_id", "value": {"type": "string", "value": "join-abc"}},
                         {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}}],
                "index": [{"metric_id": "persistent_id", "value": {"type": "string", "value": "plumb"}},
                          {"metric_id": "runtime_seconds", "value": {"type": "duration", "value": {"secs": 1, "nanos": 0}}}]
            }}],
            "graph": {"edges": [{"from_node": "join", "to_node": "index"}]}
        }));
        let report = HotspotReport::build(&profile, &dataflow);
        let index = report
            .hotspots
            .iter()
            .find(|hotspot| hotspot.node_id == "index")
            .expect("costed operator");
        assert_eq!(index.inferred_from.as_deref(), Some("join"));
        assert_eq!(index.first_line(), Some(4));
        assert_eq!(index.relation.as_deref(), Some("v"));
        assert!(
            index.operator.starts_with("map_index"),
            "its own operation stays"
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
        let mut metric = CostMetric::Time;
        let mut seen = Vec::new();
        for _ in 0..CostMetric::ALL.len() {
            seen.push(metric);
            metric = metric.next();
        }
        assert_eq!(seen, CostMetric::ALL, "memory, then storage, then records");
        assert_eq!(metric, CostMetric::Time);
        assert_eq!(CostMetric::Storage.label(), "STORAGE");
    }
}
