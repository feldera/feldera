//! The counters exposed by the Java CircuitStatistics trace option.

use feldera_sql_ir::{
    BodyOrder, Circuit, Error, Field, InnerKind, NodeId, OperatorKind, OutputPort, TypeKind,
};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CircuitStatistics {
    pub total_operators: usize,
    pub tables: usize,
    pub views: usize,
    pub star_joins: usize,
    pub gc_operators: usize,
    pub windows: usize,
    pub joins: usize,
    pub linear_aggregates: usize,
    pub linear: usize,
    pub stateful: usize,
    pub non_linear_aggregates: usize,
    pub nested: usize,
    pub non_incremental: usize,
    pub max_tuple_width: usize,
    pub max_depth: usize,
}

impl CircuitStatistics {
    fn record_operator(&mut self, kind: OperatorKind) {
        use OperatorKind::*;
        self.total_operators += 1;
        self.tables += usize::from(matches!(
            kind,
            InputMapWithWaterline | SourceLazyMap | SourceMap | SourceMultiset
        ));
        self.views += usize::from(matches!(kind, Sink | View));
        self.star_joins +=
            usize::from(matches!(kind, StarJoin | StarJoinFilterMap | StarJoinIndex));
        self.gc_operators += usize::from(matches!(
            kind,
            IntegrateTraceRetainKeys | IntegrateTraceRetainNValues | IntegrateTraceRetainValues
        ));
        self.windows += usize::from(matches!(kind, Window));
        self.joins += usize::from(matches!(
            kind,
            AntiJoin
                | AsofJoin
                | ConcreteAsofJoin
                | Join
                | JoinFilterMap
                | JoinIndex
                | LeftJoin
                | LeftJoinFilterMap
                | LeftJoinIndex
                | StarJoin
                | StarJoinFilterMap
                | StarJoinIndex
                | StreamJoin
                | StreamJoinIndex
        ));
        self.linear_aggregates += usize::from(matches!(
            kind,
            AggregateLinearPostprocess | AggregateLinearPostprocessRetainKeys | ChainAggregate
        ));
        self.linear += usize::from(matches!(
            kind,
            AggregateZero
                | Apply
                | Apply2
                | ApplyN
                | AtomicSum
                | Chain
                | ControlledKeyFilter
                | Deindex
                | Delay
                | Delta
                | Differentiate
                | Filter
                | FlatMap
                | FlatMapIndex
                | Hop
                | Integrate
                | Map
                | MapIndex
                | Negate
                | Noop
                | Now
                | Subtract
                | Sum
                | Weigh
        ));
        self.stateful += usize::from(kind.is_stateful());
        self.non_linear_aggregates += usize::from(matches!(
            kind,
            Aggregate
                | PartitionedRollingAggregate
                | PartitionedRollingAggregateWithWaterline
                | PrimitiveAggregate
        ));
        self.nested += usize::from(matches!(kind, Nested));
        self.non_incremental += usize::from(matches!(
            kind,
            StreamAggregate | StreamAntiJoin | StreamDistinct | StreamJoin | StreamJoinIndex
        ));
    }
}

/// Traverse producer ports in execution order; an operator's depth is one more
/// than its deepest input. Recursion backedges are outside the input list.
pub fn circuit_statistics(circuit: &Circuit) -> Result<CircuitStatistics, Error> {
    let mut depths = BTreeMap::new();
    let mut widths = BTreeMap::new();
    let mut stats = CircuitStatistics::default();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let op = circuit.node(id).expect("visited operator");
        stats.record_operator(op.operator_kind().expect("visited operator"));
        let mut depth = 0;
        for port in op.inputs() {
            let input_depth = depths
                .get(&port.operator)
                .ok_or_else(|| Error::new("input precedes its definition"))?;
            depth = depth.max(input_depth + 1);
            stats.max_tuple_width =
                stats
                    .max_tuple_width
                    .max(port_width(circuit, port, &mut widths, 0)?);
        }
        depths.insert(id, depth);
        stats.max_depth = stats.max_depth.max(depth);
    }
    Ok(stats)
}

fn port_width(
    circuit: &Circuit,
    port: OutputPort,
    widths: &mut BTreeMap<NodeId, usize>,
    depth: usize,
) -> Result<usize, Error> {
    if depth > 128 {
        return Err(Error::new("nested output exceeds circuit nesting limit"));
    }
    let op = circuit
        .node(port.operator)
        .ok_or_else(|| Error::new("unknown output port"))?;
    if op.operator_kind() == Some(OperatorKind::Nested) {
        let Some(Field::Array(ports)) = op.field("internalOutputs") else {
            return Err(Error::new("missing nested outputs"));
        };
        let target = ports
            .get(port.number)
            .and_then(OutputPort::from_field)
            .ok_or_else(|| Error::new("missing nested output port"))?;
        return port_width(circuit, target, widths, depth + 1);
    }
    if let Some(Field::Array(types)) = op.field("statisticsTypes") {
        let ty = types
            .get(port.number)
            .and_then(Field::node_id)
            .ok_or_else(|| Error::new("missing projected output type"))?;
        return type_width(circuit, ty, widths);
    }
    let ty = match port.number {
        0 => op.edge("outputType")?,
        // The keyed input's error ZSet is constructed from its error closure on import.
        1 if op.operator_kind() == Some(OperatorKind::InputMapWithWaterline) => {
            let error = circuit
                .node(op.edge("error")?)
                .ok_or_else(|| Error::new("missing error closure"))?;
            error.edge("type")?
        }
        1 => op.edge("errorType")?,
        2 if op.operator_kind() == Some(OperatorKind::InputMapWithWaterline) => return Ok(1),
        _ => return Err(Error::new("invalid statistics output port")),
    };
    type_width(circuit, ty, widths)
}

pub(super) fn type_width(
    circuit: &Circuit,
    root: NodeId,
    widths: &mut BTreeMap<NodeId, usize>,
) -> Result<usize, Error> {
    enum Visit {
        Type(NodeId),
        Sum(NodeId, Vec<NodeId>),
    }
    let mut pending = vec![Visit::Type(root)];
    let mut active = HashSet::new();
    while let Some(visit) = pending.pop() {
        let id = match visit {
            Visit::Type(id) if widths.contains_key(&id) => continue,
            Visit::Type(id) => id,
            Visit::Sum(id, children) => {
                let width = children.into_iter().try_fold(0usize, |sum, child| {
                    sum.checked_add(widths[&child])
                        .ok_or_else(|| Error::new("row width exceeds platform size"))
                })?;
                widths.insert(id, width);
                active.remove(&id);
                continue;
            }
        };
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing row type"))?;
        let Some(InnerKind::Type(kind)) = node.inner_kind() else {
            return Err(Error::new("expected row type"));
        };
        let children = match kind {
            TypeKind::RawTuple => node.edges("tupFields")?,
            TypeKind::IndexedZSet => vec![node.edge("keyType")?, node.edge("elementType")?],
            TypeKind::ZSet | TypeKind::Stream => vec![node.edge("elementType")?],
            TypeKind::Function => vec![node.edge("resultType")?],
            TypeKind::Ref => vec![node.edge("type")?],
            TypeKind::Any => return Err(Error::new("ANY has no tuple width")),
            _ => {
                let width = match kind {
                    TypeKind::Tuple => node.edges("tupFields")?.len(),
                    TypeKind::Struct => node.edges("fields")?.len(),
                    _ => 1,
                };
                widths.insert(id, width);
                continue;
            }
        };
        if !active.insert(id) {
            return Err(Error::new("cycle in row width"));
        }
        let mark = pending.len();
        pending.extend(children.iter().copied().rev().map(Visit::Type));
        pending.insert(mark, Visit::Sum(id, children));
    }
    Ok(widths[&root])
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::CircuitEditor;
    use serde_json::json;

    #[test]
    fn widths_follow_deep_wrappers_and_count_shared_fields_twice() -> Result<(), Error> {
        let mut edit = CircuitEditor::new(Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?);
        let site = edit.circuit().root_id();
        let mut ty = edit.bool_type(site, false)?;
        for _ in 0..256 {
            ty = edit.reference_type(ty)?;
        }
        let pair = edit.tuple_type(site, true, [ty.into(), ty.into()])?;
        assert_eq!(
            type_width(edit.circuit(), pair.into(), &mut BTreeMap::new())?,
            2
        );
        Ok(())
    }
}
