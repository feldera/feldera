//! Give a linear aggregate access to its trace retention waterline.
use crate::{dependency_order, rebuild::rebuild_input_closures};
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};
use std::collections::{HashMap, HashSet};

/// Every retainer constrains the order. The first in the resulting order
/// supplies the predicate for the replacement aggregate.
fn retention_edges(circuit: &Circuit, body: NodeId) -> Result<Vec<(NodeId, NodeId)>, Error> {
    let mut edges = Vec::new();
    for id in circuit
        .node(body)
        .expect("validated body")
        .edges("allOperators")?
    {
        let op = circuit.node(id).expect("validated operator");
        if op.operator_kind() == Some(Op::IntegrateTraceRetainKeys) {
            for port in op.inputs() {
                if circuit.node(port.operator).and_then(|n| n.operator_kind())
                    == Some(Op::AggregateLinearPostprocess)
                {
                    edges.push((port.operator, id));
                }
            }
        }
    }
    Ok(edges)
}

fn waterline_order(
    circuit: &Circuit,
    mut retainers: Vec<(NodeId, NodeId)>,
) -> Result<Vec<NodeId>, Error> {
    let positions: HashMap<_, _> = circuit.operators().map(|(id, _)| id).zip(0..).collect();
    retainers.sort_by_key(|(aggregate, _)| positions[aggregate]);
    let edges = retainers
        .into_iter()
        .map(|(aggregate, retainer)| {
            let waterline = circuit
                .node(retainer)
                .expect("validated retainer")
                .inputs()
                .nth(1)
                .ok_or_else(|| Error::new("key retainer requires a waterline"))?;
            Ok((waterline.operator, aggregate))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    dependency_order(circuit, edges)
}

/// The optional in-place ordering step observed before Java builds the replacement graph.
pub fn linear_retention_order(circuit: &Circuit) -> Result<Option<Vec<NodeId>>, Error> {
    let retainers = retention_edges(circuit, circuit.root_id())?;
    if retainers.is_empty() {
        return Ok(None);
    }
    waterline_order(circuit, retainers).map(Some)
}

pub fn linear_postprocess_retain_keys(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let Some(order) = linear_retention_order(&circuit)? else {
        return Ok(circuit);
    };
    let root = circuit.root_id();
    let mut editor = CircuitEditor::new(circuit);
    editor.order_root_operators(&order)?;
    let nested = editor.circuit().operator_order(BodyOrder::Preorder)?;
    for body in std::iter::once(root).chain(nested) {
        if editor.node(body)?.field("allOperators").is_none() {
            continue;
        }
        let mut selected = HashSet::new();
        for (aggregate, retainer) in retention_edges(editor.circuit(), body)? {
            if selected.insert(aggregate) {
                editor.retain_aggregate_keys(aggregate, retainer)?;
            }
        }
    }
    rebuild_input_closures(editor.finish_with_rewrites()?, next)
}
