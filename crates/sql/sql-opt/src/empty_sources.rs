//! Propagate proven empty multisets without removing atomic update barriers.
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node,
    NodeId, OperatorKind as Op, OutputClocks, OutputPort, StreamKind,
};
use std::collections::{BTreeMap, BTreeSet};

fn empty_constant(circuit: &Circuit, node: &Node) -> Result<bool, Error> {
    let value = circuit
        .node(node.edge("function")?)
        .expect("validated function edge");
    match value.inner_kind() {
        Some(InnerKind::Expression(Expr::IndexedZSet)) => Ok(true),
        Some(InnerKind::Expression(Expr::ZSet)) => match value.field("data") {
            Some(Field::Array(entries)) => Ok(entries.is_empty()),
            _ => Err(Error::new("a multiset literal requires its entries")),
        },
        _ => Ok(false),
    }
}

enum Rewrite<C> {
    Empty(C),
    Sum(Vec<OutputPort>),
    LeftJoin(OutputPort),
}

struct Propagation<C> {
    forwarded: BTreeMap<NodeId, OutputPort>,
    changes: Vec<(NodeId, Rewrite<C>)>,
}

fn plan<C>(
    circuit: &Circuit,
    is_empty_literal: impl Fn(&Node) -> Result<bool, Error>,
    mut clock: impl FnMut(OutputPort) -> Result<C, Error>,
) -> Result<Propagation<C>, Error> {
    let mut empty = BTreeSet::new();
    let mut forwarded = BTreeMap::new();
    let mut changes = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        let op = node.operator_kind().expect("circuit body member");
        if matches!(op, Op::Constant | Op::IncrementalConstant) {
            if is_empty_literal(node)? {
                empty.insert(id);
            }
            continue;
        }
        let inputs: Vec<_> = node
            .inputs()
            .map(|p| forwarded.get(&p.operator).copied().unwrap_or(p))
            .collect();
        let is_empty = |p: &OutputPort| empty.contains(&p.operator);
        let replace = match (op, inputs.as_slice()) {
            (
                Op::Aggregate
                | Op::AggregateLinearPostprocess
                | Op::ChainAggregate
                | Op::Deindex
                | Op::Differentiate
                | Op::Distinct
                | Op::Filter
                | Op::FlatMap
                | Op::Hop
                | Op::IndexedTopK
                | Op::Integrate
                | Op::Lag
                | Op::Map
                | Op::MapIndex
                | Op::Negate
                | Op::Noop
                | Op::PartitionedRollingAggregate
                | Op::Positive
                | Op::Rank
                | Op::RowNumber
                | Op::StreamAggregate
                | Op::StreamDistinct
                | Op::View,
                [source],
            ) => is_empty(source),
            (
                Op::Join | Op::StarJoin | Op::StarJoinIndex | Op::StreamJoin | Op::StreamJoinIndex,
                _,
            ) => inputs.iter().any(is_empty),
            (
                Op::LeftJoin | Op::LeftJoinIndex | Op::LeftJoinFilterMap | Op::AsofJoin,
                [left, right],
            ) => {
                if op == Op::LeftJoin && !is_empty(left) && is_empty(right) {
                    changes.push((id, Rewrite::LeftJoin(*left)));
                }
                is_empty(left)
            }
            (Op::StreamAntiJoin | Op::Subtract, [left, right]) => {
                if !is_empty(left) && is_empty(right) {
                    forwarded.insert(id, *left);
                }
                is_empty(left) && (op == Op::StreamAntiJoin || is_empty(right))
            }
            (Op::Sum | Op::AtomicSum, _) => {
                let remaining: Vec<_> = inputs.iter().copied().filter(|p| !is_empty(p)).collect();
                if remaining.is_empty() {
                    true
                } else {
                    if op == Op::Sum && remaining.len() == 1 {
                        forwarded.insert(id, remaining[0]);
                    } else if remaining.len() != inputs.len() {
                        changes.push((id, Rewrite::Sum(remaining)));
                    }
                    false
                }
            }
            _ => false,
        };
        if replace {
            empty.insert(id);
            changes.push((
                id,
                Rewrite::Empty(clock(OutputPort {
                    operator: id,
                    number: 0,
                })?),
            ));
        }
    }
    Ok(Propagation { forwarded, changes })
}

fn empty_literal_flag(node: &Node) -> Result<bool, Error> {
    node.data("emptyLiteral")
        .and_then(|v| v.as_bool())
        .ok_or_else(|| Error::new("constant topology requires its literal emptiness"))
}

pub(super) fn topology_needs_rewrite(circuit: &Circuit) -> Result<bool, Error> {
    let edits = plan(circuit, empty_literal_flag, |_| Ok(()))?;
    Ok(!(edits.forwarded.is_empty() && edits.changes.is_empty()))
}

/// These edits refer to original ports. The consumer resolves them to their
/// replacements as it visits the body in execution order.
pub enum EmptyStreamEdit {
    Constant(StreamKind),
    Forward(OutputPort),
    Sum(Vec<OutputPort>),
    LeftJoin(OutputPort),
}

pub fn empty_stream_edits(circuit: &Circuit) -> Result<BTreeMap<NodeId, EmptyStreamEdit>, Error> {
    let mut clocks = OutputClocks::new(circuit)?;
    let propagation = plan(circuit, empty_literal_flag, |port| clocks.get(port))?;
    let mut edits = propagation
        .forwarded
        .into_iter()
        .map(|(id, port)| (id, EmptyStreamEdit::Forward(port)))
        .collect::<BTreeMap<_, _>>();
    for (id, change) in propagation.changes {
        let edit = match change {
            Rewrite::Empty(clock) => EmptyStreamEdit::Constant(clock),
            Rewrite::Sum(ports) => EmptyStreamEdit::Sum(ports),
            Rewrite::LeftJoin(left) => EmptyStreamEdit::LeftJoin(left),
        };
        edits.insert(id, edit);
    }
    Ok(edits)
}

pub fn propagate_empty_sources(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut clocks = OutputClocks::new(&circuit)?;
    let Propagation { forwarded, changes } = plan(
        &circuit,
        |node| empty_constant(&circuit, node),
        |port| clocks.get(port),
    )?;
    let mut edit = CircuitEditor::new(circuit);
    let mut counter = *next;
    for (id, change) in changes {
        match change {
            Rewrite::Empty(kind) => edit.replace_with_empty_stream(id, kind)?,
            Rewrite::Sum(inputs) => {
                let ty = input_type(&edit, inputs[0])?;
                edit.replace_stream_inputs(id, &inputs, ty)?;
            }
            Rewrite::LeftJoin(left) => {
                let function = edit.node(id)?.edge("function")?;
                let indexed = input_type(&edit, left)?;
                let closure =
                    crate::empty_right_join_closure(&mut edit, function, indexed, &mut counter)?;
                edit.left_join_as_map(id, left, closure)?;
            }
        }
    }
    let result = rebuild_input_closures(edit.finish_forwarding(&forwarded)?, &mut counter)?;
    *next = counter;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_input_is_removed_from_atomic_sum_but_its_barrier_survives() {
        let port = |n| json!({"operator":{"node":n},"outputNumber":0});
        let graph = json!({"class":"DBSPCircuit","id":0,"incremental":false,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeInteger","id":2}}},
            {"class":"DBSPConstantOperator","id":2,"inputs":[],"outputType":{"node":1},"function":{"class":"DBSPZSetExpression","id":3,"type":{"node":1},"elementType":{"node":2},"data":[],"weights":[]}},
            {"class":"DBSPAtomicSumOperator","id":3,"inputs":[port(2),port(1)],"outputType":{"node":1},"annotations":[]},
            {"class":"DBSPSumOperator","id":4,"inputs":[port(3),port(2)],"outputType":{"node":1},"annotations":[]},
            {"class":"DBSPSinkOperator","id":5,"inputs":[port(4)],"outputType":{"node":1}},
            {"class":"DBSPAggregateZeroOperator","id":6,"inputs":[port(2)],"outputType":{"node":1}}
        ]});
        let graph = propagate_empty_sources(Circuit::from_json(graph).unwrap(), &mut 0).unwrap();
        let barrier = graph.node(NodeId::Outer(3)).unwrap();
        assert_eq!(barrier.operator_kind(), Some(Op::AtomicSum));
        assert_eq!(
            barrier.inputs().map(|p| p.operator).collect::<Vec<_>>(),
            [NodeId::Outer(1)]
        );
        assert!(graph.node(NodeId::Outer(4)).is_none());
        assert_eq!(
            graph
                .node(NodeId::Outer(5))
                .unwrap()
                .inputs()
                .next()
                .unwrap()
                .operator,
            NodeId::Outer(3)
        );
        assert_eq!(
            graph.node(NodeId::Outer(6)).unwrap().operator_kind(),
            Some(Op::AggregateZero)
        );
    }

    #[test]
    fn empty_indexed_result_copies_the_index_type_location() {
        let position =
            json!({"start_line_number":2,"start_column":3,"end_line_number":2,"end_column":9});
        let graph = json!({"class":"DBSPCircuit","id":0,"incremental":true,"declarations":[],"allOperators":[
            {"class":"DBSPIncrementalConstantOperator","id":1,"inputs":[],"outputType":{"class":"DBSPTypeIndexedZSet","id":1,"sourceEmpty":false,"position":position,"keyType":{"class":"DBSPTypeInteger","id":2},"elementType":{"node":2}},"function":{"class":"DBSPIndexedZSetExpression","id":3,"type":{"node":1}}},
            {"class":"DBSPMapIndexOperator","id":2,"inputs":[{"operator":{"node":1},"outputNumber":0}],"outputType":{"node":1},"annotations":[]}
        ]});
        let graph = propagate_empty_sources(Circuit::from_json(graph).unwrap(), &mut 0).unwrap();
        let output = graph.node(NodeId::Outer(2)).unwrap();
        assert_eq!(output.operator_kind(), Some(Op::IncrementalConstant));
        let literal = graph.node(output.edge("function").unwrap()).unwrap();
        assert_eq!(
            graph.to_json()["allOperators"][1]["function"]["position"],
            position
        );
        assert_eq!(literal.data("sourceEmpty"), Some(&json!(false)));
    }
}
