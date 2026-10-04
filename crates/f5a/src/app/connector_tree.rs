//! The Connectors view's state: each connector grouped under the table or
//! view it attaches to, sortable, collapsible, and multi-selectable.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::model::stats::{
    ConnectorError, ConnectorErrorKey, ConnectorKind, ConnectorRow, ConnectorStatus, PipelineStats,
};

use super::state::{counter_rate, push_spark};

/// Identity of one tree row; survives re-sorting and refreshes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ConnectorNode {
    Relation {
        kind: ConnectorKind,
        name: String,
    },
    /// An endpoint name; unique within a pipeline.
    Endpoint(String),
}

impl ConnectorNode {
    /// The relation row a connector sits under.
    pub fn relation_of(connector: &ConnectorRow) -> Self {
        Self::Relation {
            kind: connector.kind,
            name: connector.relation.clone(),
        }
    }
}

/// Columns the connectors tree can sort by.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConnectorSort {
    #[default]
    Name,
    Status,
    Rate,
    Records,
    Backlog,
    Errors,
}

impl ConnectorSort {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Name => "name",
            Self::Status => "status",
            Self::Rate => "rps",
            Self::Records => "records",
            Self::Backlog => "buffer",
            Self::Errors => "errors",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            Self::Name => Self::Status,
            Self::Status => Self::Rate,
            Self::Rate => Self::Records,
            Self::Records => Self::Backlog,
            Self::Backlog => Self::Errors,
            Self::Errors => Self::Name,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "name" => Some(Self::Name),
            "status" => Some(Self::Status),
            "rps" | "rate" | "throughput" => Some(Self::Rate),
            "records" => Some(Self::Records),
            "buffer" | "backlog" => Some(Self::Backlog),
            "errors" | "err" => Some(Self::Errors),
            _ => None,
        }
    }
}

/// Rates derived from consecutive stats samples of one endpoint.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConnectorLive {
    pub records_per_sec: f64,
    pub bytes_per_sec: f64,
    pub spark: VecDeque<i64>,
    /// `(millis, records, bytes)` of the previous sample.
    last_sample: Option<(i64, i64, i64)>,
}

impl ConnectorLive {
    const SPARK_CAPACITY: usize = 30;

    fn observe(&mut self, connector: &ConnectorRow, now_millis: i64) {
        if let Some((millis, records, bytes)) = self.last_sample {
            let Some(records_per_sec) =
                counter_rate((millis, records), (now_millis, connector.records))
            else {
                return;
            };
            self.records_per_sec = records_per_sec;
            self.bytes_per_sec =
                counter_rate((millis, bytes), (now_millis, connector.bytes)).unwrap_or(0.0);
            push_spark(
                &mut self.spark,
                records_per_sec as i64,
                Self::SPARK_CAPACITY,
            );
        }
        self.last_sample = Some((now_millis, connector.records, connector.bytes));
    }
}

/// Recent error messages of one endpoint, fetched from its status route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentErrors {
    pub endpoint_name: String,
    /// The endpoint's error count when fetched; a change triggers a refetch.
    pub error_count: i64,
    /// `Err` holds why the server could not list them.
    pub result: Result<Vec<ConnectorError>, String>,
}

/// Everything the Connectors view remembers between frames.
#[derive(Clone, Debug, Default)]
pub struct ConnectorPane {
    pub cursor: Option<ConnectorNode>,
    /// Marked endpoint names; connector actions apply to all of them.
    pub marked: HashSet<String>,
    /// Row where the current shift run began, as in the pipelines table.
    pub mark_anchor: Option<ConnectorNode>,
    /// Relation nodes whose connectors are hidden.
    pub collapsed: HashSet<ConnectorNode>,
    pub sort: ConnectorSort,
    pub sort_descending: bool,
    /// Rates per endpoint name.
    pub live: HashMap<String, ConnectorLive>,
    pub recent_errors: Option<RecentErrors>,
    pub is_loading_errors: bool,
    /// Whether the keys walk the connector's errors instead of moving the
    /// cursor; `enter` on a connector with errors moves them there.
    pub is_detail_focused: bool,
    /// The expanded error. Anchored to the error itself, not its position,
    /// so new errors arriving on top do not move it; `None` is the newest.
    pub selected_error: Option<ConnectorErrorKey>,
    /// Whether the first look at this pipeline's tree already folded the
    /// relations without an Active connector.
    pub has_default_folds: bool,
    /// Relations whose connectors had all reached end of input in the
    /// previous sample; a relation that joins this set folds once.
    pub relations_at_eoi: HashSet<ConnectorNode>,
}

impl ConnectorPane {
    /// Forget everything tied to the previous pipeline; the sort stays.
    pub fn reset(&mut self) {
        *self = Self {
            sort: self.sort,
            sort_descending: self.sort_descending,
            ..Self::default()
        };
    }

    /// Move the cursor; the detail pane of another row starts unfocused at
    /// its top.
    pub fn set_cursor(&mut self, node: Option<ConnectorNode>) {
        if self.cursor == node {
            return;
        }
        self.cursor = node;
        self.selected_error = None;
        self.is_detail_focused = false;
    }

    /// The recent errors listed for `endpoint_name`, newest first; empty
    /// until they load.
    pub fn listed_errors(&self, endpoint_name: &str) -> &[ConnectorError] {
        match &self.recent_errors {
            Some(RecentErrors {
                endpoint_name: listed,
                result: Ok(errors),
                ..
            }) if listed == endpoint_name => errors,
            _ => &[],
        }
    }

    /// Position of the selected error in `errors`: the anchored one, the
    /// oldest once the anchored one rotated out of the server's list, else
    /// the newest.
    pub fn selected_error_position(&self, errors: &[ConnectorError]) -> Option<usize> {
        let last = errors.len().checked_sub(1)?;
        let Some(key) = self.selected_error else {
            return Some(0);
        };
        Some(
            errors
                .iter()
                .position(|error| error.key() == key)
                .unwrap_or(last),
        )
    }

    /// Move the selection `delta` errors, parking at the ends.
    pub fn step_selected_error(&mut self, endpoint_name: &str, delta: isize) {
        let errors = self.listed_errors(endpoint_name);
        let Some(position) = self.selected_error_position(errors) else {
            return;
        };
        let next = (position as isize)
            .saturating_add(delta)
            .clamp(0, errors.len() as isize - 1) as usize;
        let key = errors[next].key();
        self.selected_error = Some(key);
    }

    /// Fold in a stats sample: update rates, drop state of endpoints that
    /// vanished, and fold relations that just reached end of input.
    pub fn observe(&mut self, stats: &PipelineStats, now_millis: i64) {
        let endpoints: HashSet<&str> = stats
            .connectors()
            .map(|connector| connector.endpoint_name.as_str())
            .collect();
        self.live
            .retain(|name, _| endpoints.contains(name.as_str()));
        self.marked.retain(|name| endpoints.contains(name.as_str()));
        for connector in stats.connectors() {
            self.live
                .entry(connector.endpoint_name.clone())
                .or_default()
                .observe(connector, now_millis);
        }
        // Only the change folds, so a relation unfolded afterwards stays open.
        let relations_at_eoi = relations_at_end_of_input(stats);
        for relation in relations_at_eoi.difference(&self.relations_at_eoi) {
            self.collapsed.insert(relation.clone());
        }
        self.relations_at_eoi = relations_at_eoi;
    }

    /// Fold every relation without an Active connector, once per pipeline,
    /// so the first look at the tree shows where data flows. Stats without
    /// connectors (a pipeline still starting) do not use up the first look.
    pub fn apply_default_folds(&mut self, stats: &PipelineStats) {
        if self.has_default_folds || stats.connectors().next().is_none() {
            return;
        }
        self.has_default_folds = true;
        let mut active_relations = HashSet::new();
        let mut relations = HashSet::new();
        for connector in stats.connectors() {
            let relation = ConnectorNode::relation_of(connector);
            if connector.status() == ConnectorStatus::Active {
                active_relations.insert(relation.clone());
            }
            relations.insert(relation);
        }
        self.collapsed = relations.difference(&active_relations).cloned().collect();
    }

    pub fn records_per_sec(&self, endpoint_name: &str) -> f64 {
        self.live
            .get(endpoint_name)
            .map_or(0.0, |live| live.records_per_sec)
    }

    /// Mark every endpoint unless all already are; then unmark them all.
    pub fn toggle_marks(&mut self, endpoint_names: Vec<String>) {
        if endpoint_names.is_empty() {
            return;
        }
        if endpoint_names.iter().all(|name| self.marked.contains(name)) {
            for name in &endpoint_names {
                self.marked.remove(name);
            }
        } else {
            self.marked.extend(endpoint_names);
        }
    }
}

/// A relation and its connectors, in display order.
#[derive(Clone, Debug)]
pub struct RelationGroup<'a> {
    pub kind: ConnectorKind,
    pub name: &'a str,
    pub connectors: Vec<&'a ConnectorRow>,
}

impl RelationGroup<'_> {
    pub fn node(&self) -> ConnectorNode {
        ConnectorNode::Relation {
            kind: self.kind,
            name: self.name.to_string(),
        }
    }

    /// The most severe connector status.
    pub fn status(&self) -> ConnectorStatus {
        self.connectors
            .iter()
            .map(|connector| connector.status())
            .min()
            .unwrap_or_default()
    }

    pub fn records(&self) -> i64 {
        self.connectors
            .iter()
            .map(|connector| connector.records)
            .sum()
    }

    pub fn bytes(&self) -> i64 {
        self.connectors
            .iter()
            .map(|connector| connector.bytes)
            .sum()
    }

    pub fn backlog_records(&self) -> i64 {
        self.connectors
            .iter()
            .map(|connector| connector.backlog_records())
            .sum()
    }

    pub fn error_count(&self) -> i64 {
        self.connectors
            .iter()
            .map(|connector| connector.error_count)
            .sum()
    }

    pub fn records_per_sec(&self, pane: &ConnectorPane) -> f64 {
        self.connectors
            .iter()
            .map(|connector| pane.records_per_sec(&connector.endpoint_name))
            .sum()
    }

    /// How many connectors are in each status, most severe first.
    pub fn status_counts(&self) -> Vec<(ConnectorStatus, usize)> {
        let mut counts: Vec<(ConnectorStatus, usize)> = Vec::new();
        for status in self.connectors.iter().map(|connector| connector.status()) {
            match counts.iter_mut().find(|(seen, _)| *seen == status) {
                Some((_, count)) => *count += 1,
                None => counts.push((status, 1)),
            }
        }
        counts.sort();
        counts
    }
}

/// One visible row of the tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeRow {
    pub node: ConnectorNode,
    /// Index into [`ConnectorTree::groups`].
    pub group: usize,
    /// Index into the group's connectors; `None` on a relation row.
    pub connector: Option<usize>,
    /// The last connector of its group, drawn with `└─`.
    pub is_last: bool,
}

/// Connectors grouped by relation, sorted, with collapsed groups folded.
#[derive(Clone, Debug)]
pub struct ConnectorTree<'a> {
    pub groups: Vec<RelationGroup<'a>>,
    pub rows: Vec<TreeRow>,
}

impl<'a> ConnectorTree<'a> {
    pub fn build(stats: Option<&'a PipelineStats>, pane: &ConnectorPane) -> Self {
        let mut groups: Vec<RelationGroup<'a>> = Vec::new();
        for connector in stats.into_iter().flat_map(PipelineStats::connectors) {
            let existing = groups
                .iter_mut()
                .find(|group| group.kind == connector.kind && group.name == connector.relation);
            match existing {
                Some(group) => group.connectors.push(connector),
                None => groups.push(RelationGroup {
                    kind: connector.kind,
                    name: &connector.relation,
                    connectors: vec![connector],
                }),
            }
        }
        for group in &mut groups {
            group
                .connectors
                .sort_by(|left, right| directed(pane, compare_connectors(pane, left, right)));
        }
        groups.sort_by(|left, right| directed(pane, compare_groups(pane, left, right)));

        let mut rows = Vec::new();
        for (group_index, group) in groups.iter().enumerate() {
            let node = group.node();
            let is_collapsed = pane.collapsed.contains(&node);
            rows.push(TreeRow {
                node,
                group: group_index,
                connector: None,
                is_last: false,
            });
            if is_collapsed {
                continue;
            }
            let last = group.connectors.len().saturating_sub(1);
            rows.extend(
                group
                    .connectors
                    .iter()
                    .enumerate()
                    .map(|(index, connector)| TreeRow {
                        node: ConnectorNode::Endpoint(connector.endpoint_name.clone()),
                        group: group_index,
                        connector: Some(index),
                        is_last: index == last,
                    }),
            );
        }
        Self { groups, rows }
    }

    pub fn position(&self, node: &ConnectorNode) -> Option<usize> {
        self.rows.iter().position(|row| &row.node == node)
    }

    /// The connector a row shows; `None` on relation rows.
    pub fn connector_of(&self, row: &TreeRow) -> Option<&'a ConnectorRow> {
        let group = self.groups.get(row.group)?;
        group.connectors.get(row.connector?).copied()
    }

    /// The group that owns `node`, also when `node` is a hidden endpoint.
    pub fn group_of(&self, node: &ConnectorNode) -> Option<&RelationGroup<'a>> {
        self.groups.iter().find(|group| match node {
            ConnectorNode::Relation { .. } => &group.node() == node,
            ConnectorNode::Endpoint(endpoint) => group
                .connectors
                .iter()
                .any(|connector| &connector.endpoint_name == endpoint),
        })
    }

    /// The connector an endpoint node names, collapsed or not.
    pub fn find_connector(&self, endpoint_name: &str) -> Option<&'a ConnectorRow> {
        self.groups
            .iter()
            .flat_map(|group| group.connectors.iter())
            .find(|connector| connector.endpoint_name == endpoint_name)
            .copied()
    }

    /// Connectors a node stands for: a relation covers all of its own,
    /// collapsed or not.
    pub fn connectors_under(&self, node: &ConnectorNode) -> Vec<&'a ConnectorRow> {
        match node {
            ConnectorNode::Relation { .. } => self
                .group_of(node)
                .map(|group| group.connectors.clone())
                .unwrap_or_default(),
            ConnectorNode::Endpoint(endpoint) => {
                self.find_connector(endpoint).into_iter().collect()
            }
        }
    }

    /// Endpoint names covered by the visible rows from `from` through `to`,
    /// both inclusive and in either order; empty when either is not visible.
    pub fn endpoints_between(&self, from: &ConnectorNode, to: &ConnectorNode) -> HashSet<String> {
        let (Some(from), Some(to)) = (self.position(from), self.position(to)) else {
            return HashSet::new();
        };
        let (low, high) = if from <= to { (from, to) } else { (to, from) };
        self.rows[low..=high]
            .iter()
            .flat_map(|row| self.connectors_under(&row.node))
            .map(|connector| connector.endpoint_name.clone())
            .collect()
    }

    /// Every connector in display order, including collapsed ones.
    pub fn all_connectors(&self) -> impl Iterator<Item = &'a ConnectorRow> + '_ {
        self.groups
            .iter()
            .flat_map(|group| group.connectors.iter().copied())
    }
}

/// Relations whose connectors all show the EOI status.
fn relations_at_end_of_input(stats: &PipelineStats) -> HashSet<ConnectorNode> {
    let mut relations = HashSet::new();
    let mut still_running = HashSet::new();
    for connector in stats.connectors() {
        let relation = ConnectorNode::relation_of(connector);
        if connector.status() != ConnectorStatus::Eoi {
            still_running.insert(relation.clone());
        }
        relations.insert(relation);
    }
    relations.difference(&still_running).cloned().collect()
}

fn directed(pane: &ConnectorPane, ordering: Ordering) -> Ordering {
    if pane.sort_descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_connectors(pane: &ConnectorPane, left: &ConnectorRow, right: &ConnectorRow) -> Ordering {
    let by_name = || left.connector_name().cmp(right.connector_name());
    match pane.sort {
        ConnectorSort::Name => by_name(),
        ConnectorSort::Status => left.status().cmp(&right.status()).then_with(by_name),
        ConnectorSort::Rate => pane
            .records_per_sec(&left.endpoint_name)
            .total_cmp(&pane.records_per_sec(&right.endpoint_name))
            .then_with(by_name),
        ConnectorSort::Records => left.records.cmp(&right.records).then_with(by_name),
        ConnectorSort::Backlog => left
            .backlog_records()
            .cmp(&right.backlog_records())
            .then_with(by_name),
        ConnectorSort::Errors => left.error_count.cmp(&right.error_count).then_with(by_name),
    }
}

/// Groups compare by their totals; ties keep tables ahead of views, then
/// order by name.
fn compare_groups(pane: &ConnectorPane, left: &RelationGroup, right: &RelationGroup) -> Ordering {
    let by_name = || {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.name.cmp(right.name))
    };
    match pane.sort {
        ConnectorSort::Name => by_name(),
        ConnectorSort::Status => left.status().cmp(&right.status()).then_with(by_name),
        ConnectorSort::Rate => left
            .records_per_sec(pane)
            .total_cmp(&right.records_per_sec(pane))
            .then_with(by_name),
        ConnectorSort::Records => left.records().cmp(&right.records()).then_with(by_name),
        ConnectorSort::Backlog => left
            .backlog_records()
            .cmp(&right.backlog_records())
            .then_with(by_name),
        ConnectorSort::Errors => left
            .error_count()
            .cmp(&right.error_count())
            .then_with(by_name),
    }
}

#[cfg(test)]
mod tests {
    use super::{ConnectorNode, ConnectorPane, ConnectorSort, ConnectorTree};
    use crate::model::stats::{
        ConnectorError, ConnectorErrorKind, ConnectorKind, ConnectorStatus, PipelineStats,
    };
    use serde_json::json;

    /// Two tables (one with two connectors) and one view.
    pub(crate) fn sample_stats() -> PipelineStats {
        PipelineStats::from_json(&json!({
            "inputs": [
                {"endpoint_name": "orders.kafka", "config": {"stream": "orders"},
                 "metrics": {"total_records": 500, "buffered_records": 9, "num_parse_errors": 2}},
                {"endpoint_name": "orders.http", "config": {"stream": "orders"}, "paused": true,
                 "metrics": {"total_records": 100}},
                {"endpoint_name": "users.unnamed-0", "config": {"stream": "users"},
                 "metrics": {"total_records": 50, "end_of_input": true}}
            ],
            "outputs": [
                {"endpoint_name": "totals.delta", "config": {"stream": "totals"},
                 "fatal_error": "disk full",
                 "metrics": {"transmitted_records": 70, "queued_records": 3}}
            ]
        }))
    }

    fn row_names(tree: &ConnectorTree) -> Vec<String> {
        tree.rows
            .iter()
            .map(|row| match &row.node {
                ConnectorNode::Relation { name, .. } => format!("[{name}]"),
                ConnectorNode::Endpoint(endpoint) => endpoint.clone(),
            })
            .collect()
    }

    fn relation(kind: ConnectorKind, name: &str) -> ConnectorNode {
        ConnectorNode::Relation {
            kind,
            name: name.to_string(),
        }
    }

    #[test]
    fn connectors_group_under_their_relation_tables_before_views() {
        let stats = sample_stats();
        let tree = ConnectorTree::build(Some(&stats), &ConnectorPane::default());
        assert_eq!(
            row_names(&tree),
            vec![
                "[orders]",
                "orders.http",
                "orders.kafka",
                "[users]",
                "users.unnamed-0",
                "[totals]",
                "totals.delta",
            ]
        );
        assert!(tree.rows[2].is_last && !tree.rows[1].is_last);
        assert_eq!(tree.groups[0].records(), 600);
        assert_eq!(tree.groups[0].error_count(), 2);
        assert_eq!(
            tree.groups[0].status(),
            ConnectorStatus::Paused,
            "worst child"
        );
        assert_eq!(
            tree.groups[0].status_counts(),
            vec![(ConnectorStatus::Paused, 1), (ConnectorStatus::Active, 1)]
        );
        assert_eq!(tree.groups[2].backlog_records(), 3);
    }

    #[test]
    fn collapsed_relations_hide_their_connectors_but_still_cover_them() {
        let stats = sample_stats();
        let mut pane = ConnectorPane::default();
        let orders = relation(ConnectorKind::Input, "orders");
        pane.collapsed.insert(orders.clone());
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(row_names(&tree)[..2], ["[orders]", "[users]"]);
        assert_eq!(tree.connectors_under(&orders).len(), 2);
        let hidden = ConnectorNode::Endpoint("orders.kafka".to_string());
        assert_eq!(tree.position(&hidden), None);
        assert_eq!(
            tree.group_of(&hidden).map(|group| group.name),
            Some("orders")
        );
        assert!(tree.find_connector("orders.kafka").is_some());
    }

    #[test]
    fn sorting_orders_groups_by_totals_and_connectors_within_them() {
        let stats = sample_stats();
        let mut pane = ConnectorPane {
            sort: ConnectorSort::Records,
            sort_descending: true,
            ..Default::default()
        };
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(
            row_names(&tree),
            vec![
                "[orders]",
                "orders.kafka",
                "orders.http",
                "[totals]",
                "totals.delta",
                "[users]",
                "users.unnamed-0",
            ]
        );
        pane.sort = ConnectorSort::Status;
        pane.sort_descending = false;
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(row_names(&tree)[0], "[totals]", "fatal sorts first");
        pane.sort = ConnectorSort::Errors;
        pane.sort_descending = true;
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(row_names(&tree)[1], "orders.kafka");
        pane.sort = ConnectorSort::Backlog;
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(row_names(&tree)[0], "[orders]", "9 buffered beats 3 queued");
    }

    #[test]
    fn rates_come_from_consecutive_samples_and_sort_the_tree() {
        let mut pane = ConnectorPane {
            sort: ConnectorSort::Rate,
            sort_descending: true,
            ..Default::default()
        };
        let first = sample_stats();
        pane.observe(&first, 1_000);
        let mut second = sample_stats();
        second.inputs[2].records = 2_050;
        pane.observe(&second, 2_000);
        assert_eq!(pane.records_per_sec("users.unnamed-0"), 2_000.0);
        assert_eq!(pane.records_per_sec("orders.kafka"), 0.0);
        assert_eq!(pane.live["users.unnamed-0"].spark.len(), 1);
        let tree = ConnectorTree::build(Some(&second), &pane);
        assert_eq!(row_names(&tree)[0], "[users]");
    }

    #[test]
    fn vanished_endpoints_lose_their_rates_and_marks() {
        let mut pane = ConnectorPane::default();
        pane.marked.insert("orders.kafka".to_string());
        pane.marked.insert("gone.endpoint".to_string());
        pane.observe(&sample_stats(), 1_000);
        assert!(pane.marked.contains("orders.kafka"));
        assert!(!pane.marked.contains("gone.endpoint"));
        assert!(!pane.live.contains_key("gone.endpoint"));
        assert_eq!(pane.live.len(), 4);
    }

    #[test]
    fn ranges_expand_relation_rows_into_their_connectors() {
        let stats = sample_stats();
        let tree = ConnectorTree::build(Some(&stats), &ConnectorPane::default());
        let span = tree.endpoints_between(
            &ConnectorNode::Endpoint("orders.kafka".to_string()),
            &relation(ConnectorKind::Input, "users"),
        );
        let mut names: Vec<String> = span.into_iter().collect();
        names.sort();
        assert_eq!(names, vec!["orders.kafka", "users.unnamed-0"]);
        assert!(
            tree.endpoints_between(
                &relation(ConnectorKind::Output, "missing"),
                &tree.rows[0].node
            )
            .is_empty()
        );
    }

    #[test]
    fn the_first_look_folds_relations_without_an_active_connector() {
        let mut pane = ConnectorPane::default();
        pane.apply_default_folds(&PipelineStats::default());
        assert!(
            !pane.has_default_folds,
            "an empty pipeline keeps the first look"
        );
        let stats = sample_stats();
        pane.apply_default_folds(&stats);
        let tree = ConnectorTree::build(Some(&stats), &pane);
        assert_eq!(
            row_names(&tree),
            vec![
                "[orders]",
                "orders.http",
                "orders.kafka",
                "[users]",
                "[totals]"
            ],
            "orders has an Active connector; EOI-only users and Fatal totals fold"
        );
        pane.collapsed.clear();
        pane.apply_default_folds(&stats);
        assert!(
            pane.collapsed.is_empty(),
            "later looks keep the user's folds"
        );
        pane.reset();
        assert!(!pane.has_default_folds, "a new pipeline gets a first look");
    }

    #[test]
    fn a_relation_folds_once_when_its_last_connector_reaches_eoi() {
        let users = relation(ConnectorKind::Input, "users");
        let mut running = sample_stats();
        running.inputs[2].end_of_input = false;
        let mut pane = ConnectorPane::default();
        pane.observe(&running, 1_000);
        assert!(!pane.collapsed.contains(&users));

        pane.observe(&sample_stats(), 2_000);
        assert!(pane.collapsed.contains(&users), "users just reached EOI");
        pane.collapsed.remove(&users);
        pane.observe(&sample_stats(), 3_000);
        assert!(!pane.collapsed.contains(&users), "the user unfolded it");

        pane.observe(&running, 4_000);
        pane.observe(&sample_stats(), 5_000);
        assert!(pane.collapsed.contains(&users), "a second EOI folds again");

        let mut half_done = sample_stats();
        half_done.inputs[1].paused = false;
        half_done.inputs[1].end_of_input = true;
        pane.collapsed.clear();
        pane.observe(&half_done, 6_000);
        assert!(
            !pane
                .collapsed
                .contains(&relation(ConnectorKind::Input, "orders")),
            "kafka is still Active"
        );
    }

    #[test]
    fn moving_the_cursor_unfocuses_and_forgets_the_selected_error() {
        let kafka = ConnectorNode::Endpoint("orders.kafka".to_string());
        let mut pane = ConnectorPane::default();
        pane.set_cursor(Some(kafka.clone()));
        pane.is_detail_focused = true;
        pane.selected_error = Some((ConnectorErrorKind::Parse, 7));
        pane.set_cursor(Some(kafka));
        assert!(
            pane.is_detail_focused && pane.selected_error.is_some(),
            "same row"
        );
        pane.set_cursor(None);
        assert!(!pane.is_detail_focused && pane.selected_error.is_none());
    }

    fn parse_errors(indices: impl Iterator<Item = i64>) -> Vec<ConnectorError> {
        indices
            .map(|index| ConnectorError {
                kind: ConnectorErrorKind::Parse,
                at_millis: Some(index),
                index,
                message: format!("error {index}"),
            })
            .collect()
    }

    fn list(pane: &mut ConnectorPane, errors: Vec<ConnectorError>) {
        pane.recent_errors = Some(super::RecentErrors {
            endpoint_name: "e".to_string(),
            error_count: errors.len() as i64,
            result: Ok(errors),
        });
    }

    #[test]
    fn the_error_selection_walks_parks_and_stays_on_its_error() {
        let mut pane = ConnectorPane::default();
        pane.step_selected_error("e", 1);
        assert_eq!(
            pane.selected_error, None,
            "nothing listed, nothing to select"
        );
        list(&mut pane, parse_errors((1..=5).rev()));
        assert_eq!(
            pane.selected_error_position(pane.listed_errors("e")),
            Some(0)
        );
        pane.step_selected_error("e", 2);
        assert_eq!(pane.selected_error, Some((ConnectorErrorKind::Parse, 3)));
        pane.step_selected_error("e", 10);
        assert_eq!(pane.selected_error, Some((ConnectorErrorKind::Parse, 1)));
        pane.step_selected_error("e", -10);
        assert_eq!(pane.selected_error, Some((ConnectorErrorKind::Parse, 5)));

        // Two newer errors arrive on top: the selection stays on error 5.
        list(&mut pane, parse_errors((1..=7).rev()));
        assert_eq!(
            pane.selected_error_position(pane.listed_errors("e")),
            Some(2)
        );
        // Error 5 rotates out of the server's list: the oldest takes over.
        list(&mut pane, parse_errors((6..=9).rev()));
        assert_eq!(
            pane.selected_error_position(pane.listed_errors("e")),
            Some(3)
        );
        assert!(pane.listed_errors("other").is_empty());
    }

    #[test]
    fn toggling_marks_all_unless_all_are_marked() {
        let mut pane = ConnectorPane::default();
        let names = vec!["a".to_string(), "b".to_string()];
        pane.marked.insert("a".to_string());
        pane.toggle_marks(names.clone());
        assert_eq!(pane.marked.len(), 2, "a partial set completes");
        pane.toggle_marks(names);
        assert!(pane.marked.is_empty());
        pane.toggle_marks(Vec::new());
        assert!(pane.marked.is_empty());
    }

    #[test]
    fn resets_keep_only_the_sort() {
        let mut pane = ConnectorPane {
            sort: ConnectorSort::Errors,
            cursor: Some(ConnectorNode::Endpoint("x".to_string())),
            ..Default::default()
        };
        pane.marked.insert("x".to_string());
        pane.reset();
        assert_eq!(pane.sort, ConnectorSort::Errors);
        assert!(pane.cursor.is_none() && pane.marked.is_empty());
    }

    #[test]
    fn sort_columns_cycle_and_parse() {
        let mut sort = ConnectorSort::Name;
        let mut titles = Vec::new();
        for _ in 0..6 {
            titles.push(sort.title());
            assert_eq!(ConnectorSort::parse(sort.title()), Some(sort));
            sort = sort.cycle();
        }
        assert_eq!(sort, ConnectorSort::Name);
        assert_eq!(
            titles,
            vec!["name", "status", "rps", "records", "buffer", "errors"]
        );
        assert_eq!(ConnectorSort::parse("bogus"), None);
    }

    #[test]
    fn an_absent_pipeline_builds_an_empty_tree() {
        let tree = ConnectorTree::build(None, &ConnectorPane::default());
        assert!(tree.rows.is_empty() && tree.groups.is_empty());
    }
}
