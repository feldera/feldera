use feldera_sql_ir::{BodyOrder, Circuit, Error, OperatorKind as Op};
use std::collections::BTreeSet;

/// Bypass no-ops unless a garbage-collection operator consumes their output.
/// The retention decision uses the original graph, as in Java's RemoveNoops.
pub fn remove_noops(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    let operators = || {
        order
            .iter()
            .map(|&id| (id, circuit.node(id).expect("validated circuit member")))
    };
    let retained: BTreeSet<_> = operators()
        .filter(|(_, node)| {
            matches!(
                node.operator_kind(),
                Some(
                    Op::IntegrateTraceRetainKeys
                        | Op::IntegrateTraceRetainNValues
                        | Op::IntegrateTraceRetainValues
                )
            )
        })
        .flat_map(|(_, node)| node.inputs().map(|port| port.operator))
        .collect();
    let removed = operators()
        .filter(|(id, node)| node.operator_kind() == Some(Op::Noop) && !retained.contains(id))
        .map(|(id, _)| id)
        .collect();
    crate::rebuild::rebuild_input_closures(circuit.bypass_unary_operators(&removed)?, next)
}

/// The complete OptimizeWithGraph(RemoveNoops) placement runs no-op removal
/// and dead-code elimination until neither removes an operator.
pub fn optimize_noops(mut circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut operators = circuit.operator_order(BodyOrder::Preorder)?.len();
    let mut counter = *next;
    loop {
        circuit = remove_noops(circuit, &mut counter)?;
        circuit = crate::remove_dead_operators(circuit, &mut counter)?;
        let remaining = circuit.operator_order(BodyOrder::Preorder)?.len();
        if remaining == operators {
            *next = counter;
            return Ok(circuit);
        }
        operators = remaining;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{NodeId, OutputPort};
    use serde_json::json;

    #[test]
    fn composite_keeps_an_unused_table_and_a_retention_branch_outside_the_sink() {
        let edge = |id: u64| json!({"operator":{"node":id},"outputNumber":0});
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPSourceMultisetOperator","id":2,"inputs":[]},
                {"class":"DBSPConstantOperator","id":3,"inputs":[]},
                {"class":"DBSPNoopOperator","id":4,"inputs":[edge(1)]},
                {"class":"DBSPMapOperator","id":5,"inputs":[edge(2)]},
                {"class":"DBSPSinkOperator","id":6,"inputs":[edge(4)]},
                {"class":"DBSPIntegrateTraceRetainKeysOperator","id":7,"inputs":[edge(1),edge(3)]}
            ]
        }))
        .unwrap();
        let mut next = 21;
        let result = optimize_noops(circuit, &mut next).unwrap();
        let ids: Vec<_> = result.operators().map(|(id, _)| id).collect();
        assert_eq!(ids, [1, 2, 3, 6, 7].map(NodeId::Outer));
        assert_eq!(
            result
                .node(NodeId::Outer(6))
                .unwrap()
                .inputs()
                .next()
                .unwrap()
                .operator,
            NodeId::Outer(1)
        );
        assert_eq!(next, 21);
    }

    #[test]
    fn forwards_the_error_stream_but_retains_the_gc_input() {
        let edge = |id: u64, number: usize| json!({"operator":{"node":id},"outputNumber":number});
        let alpha = json!({"class":"DBSPTypeZSet","id":10});
        let errors = json!({"class":"DBSPTypeZSet","id":11,"elementType":{"class":"DBSPTypeInteger","id":12,"mayBeNull":false,"width":32,"signed":true}});
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPConstantOperator","id":1,"inputs":[],"outputType":alpha},
                {"class":"DBSPControlledKeyFilterOperator","id":2,"inputs":[edge(1,0),edge(1,0)],
                 "outputType":{"node":10},"errorType":errors},
                {"class":"DBSPNoopOperator","id":3,"inputs":[edge(2,1)],"outputType":{"node":11}},
                {"class":"DBSPNoopOperator","id":4,"inputs":[edge(3,0)],"outputType":{"node":11}},
                {"class":"DBSPNoopOperator","id":5,"inputs":[edge(4,0)],"outputType":{"node":11}},
                {"class":"DBSPIntegrateTraceRetainKeysOperator","id":6,"inputs":[edge(5,0),edge(1,0)]},
                {"class":"DBSPNegateOperator","id":7,"inputs":[edge(4,0)],"outputType":{"node":11}}
            ]
        })).unwrap();
        let mut next = 7;
        let result = remove_noops(circuit, &mut next).unwrap();
        assert_eq!(next, 8);
        let error_stream = OutputPort {
            operator: NodeId::Outer(2),
            number: 1,
        };
        assert!(result.node(NodeId::Outer(3)).is_none());
        assert!(result.node(NodeId::Outer(4)).is_none());
        for id in [5, 7] {
            let node = result.node(NodeId::Outer(id)).unwrap();
            assert_eq!(node.inputs().next(), Some(error_stream));
            assert_eq!(node.edge("outputType").unwrap(), NodeId::Inner(11));
        }
        assert_eq!(
            result
                .node(NodeId::Outer(6))
                .unwrap()
                .inputs()
                .next()
                .unwrap()
                .operator,
            NodeId::Outer(5)
        );
    }
}
