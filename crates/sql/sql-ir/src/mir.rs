//! Dataflow graph consumed by the profiler. Compact names label ports, while
//! the relational origins refer back to the query plan stored in the checkpoint.

use crate::{
    provenance::{OriginId, OriginKind, Provenance, SourceRange},
    BodyOrder, Circuit, Error, Field, InnerKind, Node, NodeId, OperatorKind, OutputPort,
};
use serde_json::{json, Map, Value};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, HashMap},
};

impl Circuit {
    /// Emit the MIR section of a dataflow report for the supported operators.
    /// The plan and SQL source lines live in the checkpoint envelope.
    pub fn mir(&self) -> Result<Value, Error> {
        // Valid programs use about 3 links per node; the budget grows with the circuit
        // so that large programs pass and a small hostile input cannot amplify.
        self.mir_with_link_limit(65_536.max(16 * self.nodes.len()))
    }

    fn mir_with_link_limit(&self, limit: usize) -> Result<Value, Error> {
        let origins = self.provenance()?;
        let order = self.operator_order(BodyOrder::Postorder)?;
        let mut links = PlanLinks::default();
        let mut remaining = limit;
        for &id in &order {
            if self.nodes[&id].operator_kind() == Some(OperatorKind::Nested) {
                continue;
            }
            if let Some(origin) = origins.operator_origin(id) {
                let count = links.measure(&origins, origin, 0, limit)?;
                remaining = remaining
                    .checked_sub(count)
                    .ok_or_else(|| link_limit(limit))?;
            }
        }

        let mut nested = HashMap::new();
        for id in order {
            if self.nodes[&id].operator_kind() == Some(OperatorKind::Nested) {
                let report = self.mir_body(id, &origins, &links, &mut nested)?;
                nested.insert(id, report);
            }
        }
        self.mir_body(self.root_id(), &origins, &links, &mut nested)
    }

    fn mir_port(&self, port: OutputPort) -> Result<Value, Error> {
        Ok(json!({"node":compact_name(&self.nodes[&port.operator])?,"output":port.number}))
    }

    fn mir_body(
        &self,
        parent: NodeId,
        origins: &Provenance<'_>,
        links: &PlanLinks<'_>,
        nested: &mut HashMap<NodeId, Value>,
    ) -> Result<Value, Error> {
        let body = &self.nodes[&parent];
        let operators = body.edges("allOperators")?;
        let mut views = BTreeMap::new();
        for &id in &operators {
            let node = &self.nodes[&id];
            if node.operator_kind() == Some(OperatorKind::View) {
                views.insert(
                    identifier(node.field("viewName"))?,
                    OutputPort {
                        operator: id,
                        number: 0,
                    },
                );
            }
        }
        let mut graph = Map::new();
        if body.operator_kind() == Some(OperatorKind::Nested) {
            let Some(Field::Array(ports)) = body.field("internalOutputs") else {
                return Err(Error::new("missing nested outputs"));
            };
            let Some(Field::Array(names)) = body.field("outputViews") else {
                return Err(Error::new("missing nested view names"));
            };
            if names.len() != ports.len() {
                return Err(Error::new("nested output names and ports differ in length"));
            }
            let mut outputs = Vec::with_capacity(ports.len());
            for (name, port) in names.iter().zip(ports) {
                if matches!(port, Field::Data(Value::Null)) {
                    outputs.push(Value::Null);
                    continue;
                }
                let port = crate::wire::output(port)
                    .ok_or_else(|| Error::new("invalid internal output"))?;
                views.entry(identifier(Some(name))?).or_insert(OutputPort {
                    operator: port.operator,
                    number: 0,
                });
                outputs.push(self.mir_port(port)?);
            }
            graph.insert("operation".into(), "nested".into());
            graph.insert("outputs".into(), outputs.into());
        }
        for id in operators {
            let node = &self.nodes[&id];
            let name = compact_name(node)?;
            let kind = node
                .operator_kind()
                .ok_or_else(|| Error::new("expected operator in circuit body"))?;
            let entry = if kind == OperatorKind::Nested {
                nested
                    .remove(&id)
                    .expect("postorder visits nested bodies first")
            } else {
                self.mir_operator(id, node, kind, origins, links, &views)?
            };
            if graph.insert(name.into(), entry).is_some() {
                return Err(Error(format!("duplicate compact operator name {name}")));
            }
        }
        Ok(graph.into())
    }

    fn mir_operator(
        &self,
        id: NodeId,
        node: &Node,
        kind: OperatorKind,
        origins: &Provenance<'_>,
        links: &PlanLinks<'_>,
        views: &BTreeMap<&str, OutputPort>,
    ) -> Result<Value, Error> {
        let name = compact_name(node)?;
        let inputs = node
            .inputs()
            .map(|p| self.mir_port(p))
            .collect::<Result<Vec<_>, _>>()?;
        let mut entry = json!({"operation":operation(kind,node)?,"inputs":inputs});
        let origin = origins
            .operator_origin(id)
            .ok_or_else(|| Error(format!("missing provenance for {name}")))?;
        let origin_node = origins.origin(origin).expect("validated origin");
        let mut positions = Vec::new();
        let extra_roots: &[&str] = match kind {
            OperatorKind::Waterline => &["init", "extractTs"],
            OperatorKind::Lag => &["comparator", "projection"],
            OperatorKind::IndexedTopK => &["limit", "outputProducer", "equalityComparator"],
            OperatorKind::PartitionedRollingAggregate => {
                &["partitioningFunction", "lower", "upper", "aggregate"]
            }
            OperatorKind::InputMapWithWaterline => &[
                "initializer",
                "timestamp",
                "lub",
                "filter",
                "keyFilter",
                "error",
                "keyError",
            ],
            OperatorKind::ControlledKeyFilter => &["errorType", "error"],
            OperatorKind::Rank => &["projectionFunc", "outputProducer", "rankCmpFunc"],
            OperatorKind::RowNumber => &["outputProducer"],
            OperatorKind::AsofJoin => &["comparator"],
            OperatorKind::ConcreteAsofJoin => &["leftTimestamp", "rightTimestamp", "comparator"],
            OperatorKind::Hop => &["interval", "start", "size"],
            OperatorKind::Chain => &["inputType", "chain"],
            OperatorKind::Aggregate | OperatorKind::StreamAggregate => &["aggregate"],
            OperatorKind::PartitionedRollingAggregateWithWaterline => {
                &["partitioningFunction", "aggregateList", "lower", "upper"]
            }
            OperatorKind::JoinFilterMap
            | OperatorKind::LeftJoinFilterMap
            | OperatorKind::StarJoinFilterMap => &["filter", "map"],
            OperatorKind::ChainAggregate => &["init"],
            OperatorKind::AggregateLinearPostprocess => &["postProcess"],
            OperatorKind::AggregateLinearPostprocessRetainKeys => {
                &["postProcess", "retainKeysFunction"]
            }
            _ => &[],
        };
        // Java calls accept() for every operator property on the same visitor.
        for property in ["originalRowType", "outputType", "function"]
            .iter()
            .chain(extra_roots)
        {
            if let Some(root) = node.field(property) {
                self.expression_positions(root, &mut positions)?;
            }
        }
        if let OriginKind::Intersection(children) = &origin_node.kind {
            for child in children {
                positions.extend_from_slice(
                    &origins.origin(*child).expect("validated origin").positions,
                );
            }
        } else {
            positions.extend_from_slice(&origin_node.positions);
        }
        let name_field = match kind {
            OperatorKind::SourceMap
            | OperatorKind::SourceLazyMap
            | OperatorKind::SourceMultiset
            | OperatorKind::ViewDeclaration => Some(("table", "tableName")),
            OperatorKind::Sink => Some(("view", "viewName")),
            _ => None,
        };
        if let Some((label, field)) = name_field {
            let text = identifier(node.field(field))?;
            entry[label] = text.into();
            if kind == OperatorKind::ViewDeclaration {
                if let Some(port) = views.get(text.replace("-decl", "").as_str()) {
                    entry["backedges"] = json!([self.mir_port(*port)?]);
                }
            }
            let range = if label == "table"
                && !matches!(
                    node.field("sourceNameEmpty"),
                    Some(Field::Data(Value::Bool(true)))
                ) {
                position(node.field("sourceNamePosition"))?.filter(|p| p.is_valid())
            } else {
                None
            };
            positions.push(range.unwrap_or(origin_node.position));
        }
        entry["calcite"] = links.render(origin, 0)?;
        entry["positions"] = json!(merge_positions(positions));
        if let Some(hash) = node.operator_hash(true)? {
            entry["persistent_id"] = hash.into();
        }
        Ok(entry)
    }

    fn expression_positions(
        &self,
        field: &Field,
        positions: &mut Vec<SourceRange>,
    ) -> Result<(), Error> {
        let mut work = vec![field];
        let mut visited = BTreeSet::new();
        while let Some(field) = work.pop() {
            match field {
                Field::Array(items) => work.extend(items),
                Field::Object(fields) => work.extend(fields.values()),
                Field::Reference(id) | Field::Definition(id) if visited.insert(*id) => {
                    let node = &self.nodes[id];
                    if matches!(
                        node.inner_kind(),
                        Some(InnerKind::Expression(_) | InnerKind::Parameter)
                    ) {
                        if let Some(range) = position(node.field("position"))? {
                            positions.push(range);
                        }
                    }
                    for (key, field) in node.fields.iter() {
                        if !matches!(key.as_str(), "originalStruct" | "cachedEmptySetResult") {
                            work.push(field);
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

pub(crate) fn identifier(value: Option<&Field>) -> Result<&str, Error> {
    if let Some(Field::Object(fields)) = value {
        if let Some(Field::Data(Value::String(name))) = fields.get("name") {
            return Ok(name);
        }
    }
    Err(Error::new("missing relation name"))
}

pub(super) fn annotations(node: &Node) -> Result<&[Value], Error> {
    match node.field("annotations") {
        Some(Field::Data(Value::Array(values))) => Ok(values),
        _ => Err(Error(format!("missing annotations on {}", node.class()))),
    }
}

fn compact_name(node: &Node) -> Result<&str, Error> {
    annotations(node)?
        .iter()
        .find(|a| a["class"] == "CompactName")
        .and_then(|a| a["name"].as_str())
        .ok_or_else(|| Error(format!("missing compact name on {}", node.class())))
}

fn bool_field(node: &Node, name: &str) -> Result<bool, Error> {
    match node.field(name) {
        Some(Field::Data(Value::Bool(value))) => Ok(*value),
        _ => Err(Error(format!("invalid {name} on {}", node.class()))),
    }
}

fn join_name(
    node: &Node,
    regular: &'static str,
    balanced: &'static str,
) -> Result<&'static str, Error> {
    Ok(if bool_field(node, "balanced")? {
        balanced
    } else {
        regular
    })
}

fn operation(kind: OperatorKind, node: &Node) -> Result<Cow<'static, str>, Error> {
    use OperatorKind::*;
    Ok(match kind {
        SourceMultiset => "source_multiset",
        SourceMap => "source_map",
        Sink => "inspect",
        Constant => "constant",
        IncrementalConstant => "incremental_constant",
        AggregateLinearPostprocess => "aggregate_linear_postprocess",
        AggregateLinearPostprocessRetainKeys => "aggregate_linear_postprocess_retain_keys",
        AtomicSum => "atomic_sum",
        Differentiate => "differentiate",
        Integrate => "integrate",
        StreamDistinct => "stream_distinct",
        Deindex => "map",
        FlatMap => "flat_map",
        FlatMapIndex => "flat_map_index",
        Map => "map",
        Positive => "positive",
        WeightValidator => "integral_weight_validator",
        Filter => "filter",
        Negate => "neg",
        MapIndex => "map_index",
        Apply => "apply",
        ChainAggregate => "chain_aggregate",
        Join => join_name(node, "join", "join_balanced")?,
        JoinIndex => join_name(node, "join_index", "join_index_balanced")?,
        JoinFilterMap => join_name(node, "join_flatmap", "join_flatmap_balanced")?,
        LeftJoin => join_name(node, "left_join", "left_join_balanced")?,
        LeftJoinIndex => join_name(node, "left_join_index", "left_join_index_balanced")?,
        LeftJoinFilterMap => join_name(node, "left_join_flatmap", "left_join_flatmap_balanced")?,
        Aggregate => "aggregate",
        Lag => "lag_custom_order",
        IndexedTopK => "topK",
        PartitionedRollingAggregate => "partitioned_rolling_aggregate",
        IntegrateTraceRetainKeys if bool_field(node, "accumulate")? => {
            "accumulate_integrate_trace_retain_keys"
        }
        IntegrateTraceRetainKeys => "integrate_trace_retain_keys",
        Distinct => "distinct",
        Sum => "sum",
        Waterline => "waterline",
        ControlledKeyFilter => "controlled_key_filter_typed",
        Delay if node.field("function").and_then(Field::node_id).is_some() => {
            "transaction_delay_with_initial_value"
        }
        Delay => "transaction_delay",
        AggregateZero => "aggregate_zero",
        AntiJoin => "antijoin",
        Apply2 => "apply2",
        ApplyN => "apply_n",
        AsofJoin => "asof_join_abstract",
        ConcreteAsofJoin => "asof_join",
        BinaryDistinct => "distinct_component",
        Chain => {
            let Some(Field::Array(kinds)) = node.field("kinds") else {
                return Err(Error::new("missing chain kinds"));
            };
            let mut summary = String::from("chain");
            for kind in kinds {
                summary.push(match kind {
                    Field::Data(Value::String(name)) => match name.as_str() {
                        "Map" => 'M',
                        "Filter" => 'F',
                        "MapIndex" => 'I',
                        _ => return Err(Error::new("unknown chain operation")),
                    },
                    _ => return Err(Error::new("invalid chain operation")),
                });
            }
            return Ok(summary.into());
        }
        DelayedIntegral => "accumulate_delay_trace",
        Delta => "delta0",
        Hop => "hop",
        InputMapWithWaterline => "input_map_with_waterline",
        IntegrateTraceRetainNValues => match node.data("which").and_then(Value::as_str) {
            Some("LastN") => "accumulate_integrate_trace_retain_values_last_n",
            Some("TopN") => "accumulate_integrate_trace_retain_values_top_n",
            Some("BottomN") => "accumulate_integrate_trace_retain_values_bottom_n",
            _ => return Err(Error::new("invalid retain-N direction")),
        },
        IntegrateTraceRetainValues => "accumulate_integrate_trace_retain_values",
        Nested => "nested",
        Noop => "noop",
        Now => "now",
        PartitionedRollingAggregateWithWaterline => "partitioned_rolling_aggregate_with_waterline",
        PrimitiveAggregate => "AggregateIncremental",
        Rank => "rank",
        RowNumber => "row_number_custom_order",
        SourceLazyMap => "lazy_source_map",
        StarJoin => "inner_star_join",
        StarJoinFilterMap => "inner_star_join_flatmap",
        StarJoinIndex => "inner_star_join_index",
        StreamAggregate => "stream_aggregate",
        StreamAntiJoin => "stream_antijoin",
        StreamJoin => "stream_join",
        StreamJoinIndex => "stream_join_index",
        Subtract => "minus",
        UpsertFeedback => "upsert_feedback",
        View => "map",
        ViewDeclaration => "Z",
        Weigh => "weigh",
        Window => "window",
    }
    .into())
}

fn link_limit(limit: usize) -> Error {
    Error(format!(
        "MIR exceeds the resource limit of {limit} expanded origin links. Reduce repeated relational origins."
    ))
}

fn check_link_depth(depth: usize) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error::new(
            "MIR exceeds 128 nested origin links. Reduce origin nesting.",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Link<'a> {
    Empty,
    Leaf(&'static str, i32),
    Children(&'static str, &'a [OriginId]),
}

#[derive(Default)]
struct PlanLinks<'a> {
    measured: HashMap<OriginId, (usize, Link<'a>)>,
}

impl<'a> PlanLinks<'a> {
    // Count shared subtrees before allocating any part of the expanded report.
    fn measure(
        &mut self,
        origins: &'a Provenance<'_>,
        id: OriginId,
        depth: usize,
        limit: usize,
    ) -> Result<usize, Error> {
        #[cfg(test)]
        LINK_VISITS.with(|visits| visits.set(visits.get() + 1));
        check_link_depth(depth)?;
        if let Some(&(count, _)) = self.measured.get(&id) {
            return Ok(count);
        }
        let kind = &origins.origin(id).expect("validated origin").kind;
        let mut count: usize = 1;
        let link = match kind {
            OriginKind::Empty | OriginKind::PositionOnly { .. } => Link::Empty,
            OriginKind::Final(rel) => Link::Leaf("final", origins.plan_id(*rel)),
            OriginKind::Intermediate(rel) => Link::Leaf("partial", origins.plan_id(*rel)),
            OriginKind::Sequence(children) | OriginKind::Intersection(children) => {
                for &child in children {
                    let size = self.measure(origins, child, depth + 1, limit)?;
                    count = count
                        .checked_add(size)
                        .filter(|&n| n <= limit)
                        .ok_or_else(|| link_limit(limit))?;
                }
                match kind {
                    OriginKind::Sequence(_) => Link::Children("seq", children),
                    // A singleton RelAnd contains a final relation and emits only that leaf.
                    OriginKind::Intersection(_) if children.len() == 1 => {
                        self.measured[&children[0]].1
                    }
                    _ => Link::Children("and", children),
                }
            }
        };
        self.measured.insert(id, (count, link));
        Ok(count)
    }

    fn render(&self, id: OriginId, depth: usize) -> Result<Value, Error> {
        #[cfg(test)]
        LINK_VISITS.with(|visits| visits.set(visits.get() + 1));
        check_link_depth(depth)?;
        Ok(match self.measured[&id].1 {
            Link::Empty => Value::Null,
            Link::Leaf(key, plan) => json!({key: plan}),
            Link::Children(key, children) => {
                let mut links = Vec::with_capacity(children.len());
                for &child in children {
                    links.push(self.render(child, depth + 1)?);
                }
                Value::Object([(key.into(), Value::Array(links))].into_iter().collect())
            }
        })
    }
}

fn position(field: Option<&Field>) -> Result<Option<SourceRange>, Error> {
    field.map(Field::source_range).transpose()
}

fn merge_positions(mut positions: Vec<SourceRange>) -> Vec<SourceRange> {
    positions.retain(|p| p.is_valid());
    positions.sort_unstable();
    let mut merged: Vec<SourceRange> = Vec::new();
    for range in positions {
        if let Some(prev) = merged.last_mut() {
            let end = (prev.end_line_number, prev.end_column);
            let adjacent = (prev.end_line_number, prev.end_column.saturating_add(1));
            if (range.start_line_number, range.start_column) <= adjacent {
                (prev.end_line_number, prev.end_column) =
                    end.max((range.end_line_number, range.end_column));
                continue;
            }
        }
        merged.push(range);
    }
    merged
}

#[cfg(test)]
thread_local! {
    static LINK_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_origin_cost_is_checked_before_expansion() {
        let position = json!(SourceRange::default());
        let mut origin =
            json!({"kind":"CalciteEmptyRel","id":0,"position":position,"positions":[]});
        for id in 1..=8 {
            origin = json!({"kind":"RelSequence","id":id,"children":[origin, {"ref":id - 1}],
                            "position":position,"positions":[]});
        }
        let mut wire = json!({"class":"DBSPCircuit","id":0,"declarations":[],
            "allOperators":[{"class":"DBSPConstantOperator","id":1,"inputs":[],"provenance":origin,
                             "annotations":[{"class":"CompactName","name":"s"}]}]
        });
        let graph = Circuit::from_json(wire.clone()).unwrap();
        LINK_VISITS.set(0);
        assert!(graph.mir_with_link_limit(510).is_err());
        let visits = LINK_VISITS.get();
        assert_eq!(visits, 17, "9 origins with 16 child references");
        LINK_VISITS.set(0);
        let mir = graph.mir_with_link_limit(511).unwrap();
        assert_eq!(LINK_VISITS.get(), 17 + 511);
        let mut sequence = &mir["s"]["calcite"];
        for _ in 0..8 {
            assert_eq!(sequence["seq"][0], sequence["seq"][1]);
            sequence = &sequence["seq"][0];
        }
        assert!(sequence.is_null());

        wire["allOperators"].as_array_mut().unwrap().push(json!({
            "class":"DBSPConstantOperator","id":2,"inputs":[],"provenance":{"ref":8},
            "annotations":[{"class":"CompactName","name":"t"}]
        }));
        let graph = Circuit::from_json(wire).unwrap();
        LINK_VISITS.set(0);
        assert!(graph.mir_with_link_limit(1021).is_err());
        assert_eq!(LINK_VISITS.get(), 18);
        let mir = graph.mir_with_link_limit(1022).unwrap();
        assert_eq!(mir["s"]["calcite"], mir["t"]["calcite"]);
    }

    #[test]
    fn merges_contained_adjacent_and_multiline_spans() {
        fn range(line: i32, column: i32, end_line: i32, end_column: i32) -> SourceRange {
            SourceRange {
                start_line_number: line,
                start_column: column,
                end_line_number: end_line,
                end_column,
            }
        }
        let ranges = vec![
            range(3, 1, 3, 6),
            range(1, 7, 2, 3),
            range(1, 2, 1, 6),
            range(2, 1, 2, 2),
            range(0, 0, 0, 0),
        ];
        assert_eq!(
            merge_positions(ranges),
            [range(1, 2, 2, 3), range(3, 1, 3, 6)]
        );
    }
}
