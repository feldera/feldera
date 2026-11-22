// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: MIT
// The original permission notice is retained in LICENSE-VMWARE.

use crate::operator_inputs::{OperatorInputs, output_port};
use feldera_sql_ir::{BodyOrder, Circuit, Error, Field, NodeId, OperatorKind as Op, OutputPort};
use std::collections::{BTreeSet, HashMap};

/// Java's DeadCode placement after graph optimizations keeps table sources, output
/// sinks, and the inputs of every trace-retention operator. Trace individual ports
/// through recursive circuits before pruning their unused exports.
pub fn remove_dead_operators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    Ok(eliminate_dead_code(circuit, next, true)?.circuit)
}

pub struct DeadCodeResult {
    pub circuit: Circuit,
    /// Root table sources in declaration order for the compiler's "Unused" warning.
    pub unused_sources: Vec<NodeId>,
}

/// With all-table generation disabled, unused table sources are reported and
/// removed. Recursive view declarations participate in reachability but do not
/// produce unused-table diagnostics.
pub fn eliminate_dead_code(
    circuit: Circuit,
    next: &mut u64,
    keep_all_sources: bool,
) -> Result<DeadCodeResult, Error> {
    let kept = reachable(&circuit, keep_all_sources)?;
    let unused_sources = circuit
        .operators()
        .filter_map(|(id, node)| {
            unused_table(node.operator_kind()?, id, &kept, keep_all_sources).then_some(id)
        })
        .collect();
    Ok(DeadCodeResult {
        circuit: crate::rebuild::rebuild_input_closures(circuit.retain_operators(&kept)?, next)?,
        unused_sources,
    })
}

fn unused_table(kind: Op, id: NodeId, kept: &BTreeSet<NodeId>, keep_all_sources: bool) -> bool {
    !keep_all_sources
        && matches!(
            kind,
            Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap | Op::InputMapWithWaterline
        )
        && !kept.contains(&id)
}

/// Operators to remove from their enclosing bodies. Nested shells retain their
/// identities and output numbers when some of their internal streams disappear.
pub struct Pruning {
    pub removed: Vec<NodeId>,
    pub unused_sources: Vec<NodeId>,
}

fn classify_pruning(
    members: impl Iterator<Item = (NodeId, Op)>,
    root_members: impl Iterator<Item = (NodeId, Op)>,
    kept: &BTreeSet<NodeId>,
    keep_all_sources: bool,
) -> Pruning {
    let removed = members
        .filter_map(|(id, kind)| (kind != Op::Nested && !kept.contains(&id)).then_some(id))
        .collect();
    let unused_sources = root_members
        .filter_map(|(id, kind)| unused_table(kind, id, kept, keep_all_sources).then_some(id))
        .collect();
    Pruning {
        removed,
        unused_sources,
    }
}

pub fn pruning(circuit: &Circuit, keep_all_sources: bool) -> Result<Pruning, Error> {
    let kept = reachable(circuit, keep_all_sources)?;
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    let members = order.iter().map(|&id| {
        (
            id,
            circuit
                .node(id)
                .and_then(|node| node.operator_kind())
                .expect("validated operator"),
        )
    });
    let root_members = circuit
        .operators()
        .map(|(id, node)| (id, node.operator_kind().expect("validated operator")));
    Ok(classify_pruning(
        members,
        root_members,
        &kept,
        keep_all_sources,
    ))
}

pub(super) fn reachable(
    circuit: &Circuit,
    keep_all_sources: bool,
) -> Result<BTreeSet<NodeId>, Error> {
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    let members = order.into_iter().map(|id| {
        (
            id,
            circuit
                .node(id)
                .and_then(|node| node.operator_kind())
                .expect("validated operator"),
        )
    });
    trace_ports(members, keep_all_sources, |port| {
        let node = circuit
            .node(port.operator)
            .ok_or_else(|| Error::new("unknown reachability input"))?;
        let internal = if node.operator_kind() == Some(Op::Nested) {
            let Some(Field::Array(outputs)) = node.field("internalOutputs") else {
                return Err(Error::new("nested circuit has no output list"));
            };
            Some(
                outputs
                    .get(port.number)
                    .and_then(OutputPort::from_field)
                    .ok_or_else(|| Error::new("referenced nested output is vacant"))?,
            )
        } else {
            None
        };
        Ok((internal, node.inputs()))
    })
}

/// The JVM supplies members in preorder, starting with the outermost body.
pub fn projected_pruning(
    operators: &[OperatorInputs],
    keep_all_sources: bool,
) -> Result<Pruning, Error> {
    let root = operators.first().map(|op| op.body);
    let mut by_id = HashMap::with_capacity(operators.len());
    for op in operators {
        if by_id.insert(NodeId::Outer(op.id), op).is_some() {
            return Err(Error::new("duplicate operator in reachability input"));
        }
    }
    let kept = trace_ports(
        operators.iter().map(|op| (NodeId::Outer(op.id), op.kind)),
        keep_all_sources,
        |port| {
            let node = by_id
                .get(&port.operator)
                .ok_or_else(|| Error::new("unknown reachability input"))?;
            let internal = if node.kind == Op::Nested {
                Some(
                    node.internal_outputs
                        .get(port.number)
                        .copied()
                        .flatten()
                        .map(output_port)
                        .ok_or_else(|| Error::new("referenced nested output is vacant"))?,
                )
            } else {
                None
            };
            Ok((internal, node.inputs.iter().copied().map(output_port)))
        },
    )?;
    let member = |op: &OperatorInputs| (NodeId::Outer(op.id), op.kind);
    let root_members = operators
        .iter()
        .filter(|op| Some(op.body) == root)
        .map(member);
    Ok(classify_pruning(
        operators.iter().map(member),
        root_members,
        &kept,
        keep_all_sources,
    ))
}

fn trace_ports<I: Iterator<Item = OutputPort>>(
    members: impl Iterator<Item = (NodeId, Op)>,
    keep_all_sources: bool,
    inputs: impl Fn(OutputPort) -> Result<(Option<OutputPort>, I), Error>,
) -> Result<BTreeSet<NodeId>, Error> {
    let mut kept = BTreeSet::new();
    let mut pending = Vec::new();
    for (id, kind) in members {
        match kind {
            Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap if keep_all_sources => {
                kept.insert(id);
            }
            Op::Sink
            | Op::IntegrateTraceRetainKeys
            | Op::IntegrateTraceRetainNValues
            | Op::IntegrateTraceRetainValues => {
                pending.push(OutputPort {
                    operator: id,
                    number: 0,
                });
            }
            _ => (),
        }
    }
    let mut seen = BTreeSet::new();
    while let Some(port) = pending.pop() {
        if !seen.insert(port) {
            continue;
        }
        let (internal, upstream) = inputs(port)?;
        // A nested operator can be reached through several distinct exports.
        pending.extend(internal);
        if kept.insert(port.operator) {
            pending.extend(upstream);
        }
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compact_ports_trace_each_live_recursive_export_and_warn_in_root_order() {
        let mut ops: Vec<OperatorInputs> = serde_json::from_str(
            r#"[
            [0, 91, "DBSPSourceMapOperator", []],
            [0, 1, "DBSPSourceMultisetOperator", []],
            [0, 20, "DBSPNestedOperator", [[1,0]], [[4,0], null, [6,0]]],
            [20, 4, "DBSPDeltaOperator", [[1,0]]],
            [20, 5, "DBSPNegateOperator", [[4,0]]],
            [20, 6, "DBSPNoopOperator", [[4,0]]],
            [0, 8, "DBSPSinkOperator", [[20,0]]],
            [0, 9, "DBSPIntegrateTraceRetainKeysOperator", [[20,2], [20,0]]],
            [0, 3, "DBSPSourceMultisetOperator", []]
        ]"#,
        )
        .unwrap();
        let used = projected_pruning(&ops, false).unwrap();
        assert_eq!(used.removed, [91, 5, 3].map(NodeId::Outer));
        assert_eq!(used.unused_sources, [91, 3].map(NodeId::Outer));
        let all = projected_pruning(&ops, true).unwrap();
        assert_eq!(all.removed, [NodeId::Outer(5)]);
        assert!(all.unused_sources.is_empty());
        ops[7].inputs[0].1 = 1;
        assert!(projected_pruning(&ops, true).is_err());
    }

    #[test]
    fn reports_unused_tables_in_declaration_order_and_keeps_view_declarations_silent() {
        let circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMapOperator","id":9,"inputs":[]},
            {"class":"DBSPSourceMultisetOperator","id":3,"inputs":[]},
            {"class":"DBSPViewDeclarationOperator","id":4,"inputs":[]},
            {"class":"DBSPSourceMultisetOperator","id":2,"inputs":[]},
            {"class":"DBSPSinkOperator","id":5,"inputs":[{"operator":{"node":3},"outputNumber":0}]}
        ]})).unwrap();
        let result = eliminate_dead_code(circuit, &mut 0, false).unwrap();
        assert_eq!(result.unused_sources, [NodeId::Outer(9), NodeId::Outer(2)]);
        assert_eq!(result.circuit.operators().count(), 2);
    }

    #[test]
    fn removing_one_of_two_delta_imports_preserves_the_live_output_number() {
        let port = |id: u64, n: usize| json!({"operator":{"node":id},"outputNumber":n});
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPNestedOperator","id":2,"inputs":[port(1,0),port(1,0)],"allOperators":[
                    {"class":"DBSPDeltaOperator","id":3,"inputs":[port(1,0)]},
                    {"class":"DBSPDeltaOperator","id":4,"inputs":[port(1,0)]}
                ],"internalOutputs":[port(3,0),port(4,0)]},
                {"class":"DBSPSinkOperator","id":5,"inputs":[port(2,1)]}
            ]}),
        )
        .unwrap();
        let result = remove_dead_operators(circuit, &mut 0).unwrap();
        let region = result.node(NodeId::Outer(2)).unwrap();
        assert_eq!(region.inputs().count(), 1);
        assert_eq!(region.edges("allOperators").unwrap(), [NodeId::Outer(4)]);
        assert_eq!(
            result.to_json()["allOperators"][1]["internalOutputs"],
            json!([null, port(4, 0)])
        );
    }
}
