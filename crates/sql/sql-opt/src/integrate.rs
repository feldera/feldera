//! Cancel integration after differentiation and integrate incremental constants.
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, Node, OperatorKind, OutputPort};
use std::collections::BTreeMap;

fn input(node: &Node) -> Result<OutputPort, Error> {
    let mut ports = node.inputs();
    ports
        .next()
        .filter(|_| ports.next().is_none())
        .ok_or_else(|| Error::new("differentiation and integration require one input"))
}

/// Apply RemoveIAfterD in constructor order. A differentiator can have other
/// consumers, so only the integrator is bypassed.
pub fn remove_redundant_integrates(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut editor = CircuitEditor::new(circuit);
    let mut forwarded = BTreeMap::new();
    for id in order {
        let node = editor.node(id)?;
        if node.operator_kind() != Some(OperatorKind::Integrate) {
            continue;
        }
        let port = input(node)?;
        let source: OutputPort = forwarded.get(&port.operator).copied().unwrap_or(port);
        let producer = editor.node(source.operator)?;
        match producer.operator_kind() {
            Some(OperatorKind::Differentiate) => {
                let port = input(producer)?;
                forwarded.insert(id, forwarded.get(&port.operator).copied().unwrap_or(port));
            }
            Some(OperatorKind::IncrementalConstant) => {
                editor.integrate_constant(id, source.operator)?
            }
            _ => {}
        }
    }
    crate::rebuild::rebuild_input_closures(editor.finish_forwarding(&forwarded)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::NodeId;
    use serde_json::json;

    #[test]
    fn consecutive_pairs_forward_the_original_error_stream() {
        let port = |n, port| json!({"operator":{"node":n},"outputNumber":port});
        let node = |kind: &str, id, inputs: Vec<_>| json!({"class":kind,"id":id,"inputs":inputs});
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            node("DBSPControlledKeyFilterOperator",1,vec![]),
            node("DBSPDifferentiateOperator",2,vec![port(1,1)]),
            node("DBSPIntegrateOperator",3,vec![port(2,0)]),
            node("DBSPDifferentiateOperator",4,vec![port(3,0)]),
            node("DBSPIntegrateOperator",5,vec![port(4,0)]),
            node("DBSPSinkOperator",6,vec![port(5,0)])]});
        let mut next = 7;
        let graph =
            remove_redundant_integrates(Circuit::from_json(graph).unwrap(), &mut next).unwrap();
        assert!(graph.node(NodeId::Outer(2)).is_some());
        assert!(graph.node(NodeId::Outer(3)).is_none());
        assert!(graph.node(NodeId::Outer(5)).is_none());
        assert_eq!(
            input(graph.node(NodeId::Outer(6)).unwrap()).unwrap(),
            OutputPort {
                operator: NodeId::Outer(1),
                number: 1
            }
        );
        assert_eq!(next, 7);
    }
}
