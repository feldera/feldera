// Copyright 2023 VMware, Inc.
// SPDX-License-Identifier: MIT
// The original permission notice is retained in LICENSE-VMWARE.

use feldera_sql_ir::{BodyOrder, Circuit, Error, Field, NodeId, OperatorKind as Op, OutputPort};
use std::collections::BTreeSet;

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
    let unused_sources = unused_sources(&circuit, &kept, keep_all_sources);
    Ok(DeadCodeResult {
        circuit: crate::rebuild::rebuild_input_closures(circuit.retain_operators(&kept)?, next)?,
        unused_sources,
    })
}

fn unused_sources(
    circuit: &Circuit,
    kept: &BTreeSet<NodeId>,
    keep_all_sources: bool,
) -> Vec<NodeId> {
    if keep_all_sources {
        Vec::new()
    } else {
        circuit
            .operators()
            .filter_map(|(id, node)| {
                let table = matches!(
                    node.operator_kind(),
                    Some(
                        Op::SourceMultiset
                            | Op::SourceMap
                            | Op::SourceLazyMap
                            | Op::InputMapWithWaterline
                    )
                );
                (table && !kept.contains(&id)).then_some(id)
            })
            .collect()
    }
}

/// Operators to remove from their enclosing bodies. Nested shells retain their
/// identities and output numbers when some of their internal streams disappear.
pub struct Pruning {
    pub removed: Vec<NodeId>,
    pub unused_sources: Vec<NodeId>,
}

pub fn pruning(circuit: &Circuit, keep_all_sources: bool) -> Result<Pruning, Error> {
    let kept = reachable(circuit, keep_all_sources)?;
    let removed = circuit
        .operator_order(BodyOrder::Preorder)?
        .into_iter()
        .filter(|id| {
            !kept.contains(id)
                && circuit.node(*id).and_then(|n| n.operator_kind()) != Some(Op::Nested)
        })
        .collect();
    Ok(Pruning {
        removed,
        unused_sources: unused_sources(circuit, &kept, keep_all_sources),
    })
}

pub(super) fn reachable(
    circuit: &Circuit,
    keep_all_sources: bool,
) -> Result<BTreeSet<NodeId>, Error> {
    let mut kept = BTreeSet::new();
    let mut pending = Vec::new();
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        match circuit.node(id).and_then(|node| node.operator_kind()) {
            Some(Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap) if keep_all_sources => {
                kept.insert(id);
            }
            Some(
                Op::Sink
                | Op::IntegrateTraceRetainKeys
                | Op::IntegrateTraceRetainNValues
                | Op::IntegrateTraceRetainValues,
            ) => {
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
        let node = circuit
            .node(port.operator)
            .ok_or_else(|| Error::new("unknown reachability input"))?;
        if node.operator_kind() == Some(Op::Nested) {
            let Some(Field::Array(outputs)) = node.field("internalOutputs") else {
                return Err(Error::new("nested circuit has no output list"));
            };
            let internal = outputs
                .get(port.number)
                .and_then(OutputPort::from_field)
                .ok_or_else(|| Error::new("referenced nested output is vacant"))?;
            pending.push(internal);
        }
        if kept.insert(port.operator) {
            pending.extend(node.inputs());
        }
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
