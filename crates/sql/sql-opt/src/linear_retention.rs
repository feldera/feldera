//! Give a linear aggregate access to its trace retention waterline.
use crate::{dependency_order, rebuild::rebuild_input_closures};
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};
use std::collections::{HashMap, HashSet};

/// Every retainer constrains the order. The first in the resulting order
/// supplies the predicate for the replacement aggregate.
fn retention_edges(circuit: &Circuit, order: &[NodeId]) -> Vec<(NodeId, NodeId)> {
    let mut edges = Vec::new();
    for &id in order {
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
    edges
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
    let retainers = retention_edges(circuit, &circuit.root().edges("allOperators")?);
    if retainers.is_empty() {
        return Ok(None);
    }
    waterline_order(circuit, retainers).map(Some)
}

/// Decisions for both the detached replay and the live JVM. The first retainer
/// in each body's final order supplies the predicate for its aggregate.
pub struct LinearRetentionPlan {
    pub order: Vec<NodeId>,
    pub replacements: Vec<(NodeId, NodeId)>,
}

pub fn linear_retention_plan(circuit: &Circuit) -> Result<Option<LinearRetentionPlan>, Error> {
    let Some(order) = linear_retention_order(circuit)? else {
        return Ok(None);
    };
    let mut replacements = retention_edges(circuit, &order);
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        let op = circuit.node(id).expect("validated operator");
        if op.operator_kind() == Some(Op::Nested) {
            replacements.extend(retention_edges(circuit, &op.edges("allOperators")?));
        }
    }
    let mut selected = HashSet::new();
    replacements.retain(|&(aggregate, _)| selected.insert(aggregate));
    Ok(Some(LinearRetentionPlan {
        order,
        replacements,
    }))
}

pub fn linear_postprocess_retain_keys(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let Some(plan) = linear_retention_plan(&circuit)? else {
        return Ok(circuit);
    };
    let mut editor = CircuitEditor::new(circuit);
    editor.order_root_operators(&plan.order)?;
    for (aggregate, retainer) in plan.replacements {
        editor.retain_aggregate_keys(aggregate, retainer)?;
    }
    rebuild_input_closures(editor.finish_with_rewrites()?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn root_retention_enables_nested_replacements_and_orders_every_waterline() -> Result<(), Error>
    {
        let operators = |offset: u64| {
            let port = |id: u64| json!({"operator":{"node":offset + id},"outputNumber":0});
            vec![
                json!({"class":"DBSPConstantOperator","id":offset+1,"inputs":[]}),
                json!({"class":"DBSPAggregateLinearPostprocessOperator","id":offset+2,"inputs":[port(1)]}),
                json!({"class":"DBSPConstantOperator","id":offset+3,"inputs":[]}),
                json!({"class":"DBSPConstantOperator","id":offset+4,"inputs":[]}),
                json!({"class":"DBSPIntegrateTraceRetainKeysOperator","id":offset+5,"inputs":[port(2),port(3)]}),
                json!({"class":"DBSPIntegrateTraceRetainKeysOperator","id":offset+6,"inputs":[port(2),port(4)]}),
            ]
        };
        let nested = json!({"class":"DBSPNestedOperator","id":20,"inputs":[],
            "allOperators":operators(10),"internalOutputs":[]});
        let circuit = |body| {
            Circuit::from_json(json!({"class":"DBSPCircuit","id":0,
            "declarations":[],"allOperators":body}))
        };
        assert!(linear_retention_plan(&circuit(vec![nested.clone()])?)?.is_none());

        let mut body = operators(0);
        body.push(nested);
        let plan = linear_retention_plan(&circuit(body)?)?.expect("root retainer activates pass");
        let position = |id| {
            plan.order
                .iter()
                .position(|op| *op == NodeId::Outer(id))
                .unwrap()
        };
        assert!(position(3) < position(2) && position(4) < position(2));
        assert_eq!(
            plan.replacements,
            [
                (NodeId::Outer(2), NodeId::Outer(5)),
                (NodeId::Outer(12), NodeId::Outer(15)),
            ]
        );
        Ok(())
    }
}
