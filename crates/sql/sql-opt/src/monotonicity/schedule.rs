//! Schedule transfer functions on the delta-expanded circuit. A transfer can
//! only propagate an existing waterline, except where the operator establishes
//! a bound itself. Expression analysis determines which fields retain it.
use crate::state_bounds::has_lateness;
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::HashSet;

pub fn monotonicity_transfers(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let mut bounded = HashSet::new();
    let mut transfers = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        let mut inputs = node.inputs().map(|p| bounded.contains(&p.operator));
        let first = inputs.next().unwrap_or(false);
        let required = match node.operator_kind() {
            // The expression visitor reports indexed sources as invalid at this stage.
            Some(Op::SourceMap | Op::SourceLazyMap) => true,
            Some(Op::SourceMultiset) => has_lateness(node, "columnMetadata")?,
            Some(Op::View) => first || has_lateness(node, "columns")?,
            Some(Op::Window) => {
                let unbounded = node
                    .data("lowerUnbounded")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("window requires its lower-bound policy"))?;
                !unbounded || first
            }
            Some(Op::ChainAggregate) => {
                first
                    || node
                        .data("annotations")
                        .and_then(|v| v.as_array())
                        .is_some_and(|a| a.iter().any(|a| a["class"] == "AlwaysMonotone"))
            }
            Some(Op::BinaryDistinct) => inputs.next().unwrap_or(false),
            Some(Op::AntiJoin) => first && inputs.next().unwrap_or(false),
            Some(Op::Sum | Op::AtomicSum | Op::Subtract) => {
                // An empty sum must still receive the visitor's normal validation.
                node.inputs().all(|p| bounded.contains(&p.operator))
            }
            Some(Op::StreamJoin | Op::StreamJoinIndex | Op::AsofJoin) => {
                first || inputs.next().unwrap_or(false)
            }
            // The enclosing recursive operator publishes no monotonicity facts.
            // Its individual children already occurred in body order.
            Some(Op::Nested) => false,
            _ => first,
        };
        if required {
            transfers.push(id);
            bounded.insert(id);
        }
    }
    Ok(transfers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn window_and_view_seed_bounds_on_otherwise_unbounded_inputs() {
        let port = |id| json!({"outputNumber": 0, "operator": {"node": id}});
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                 "metadata": {"columnMetadata": []}},
                {"class": "DBSPViewOperator", "id": 2, "inputs": [port(1)],
                 "metadata": {"columns": [{"lateness": true}]}},
                {"class": "DBSPWindowOperator", "id": 3, "lowerUnbounded": true,
                 "inputs": [port(1), port(2)]},
                {"class": "DBSPWindowOperator", "id": 4, "lowerUnbounded": false,
                 "inputs": [port(1), port(2)]},
                {"class": "DBSPBinaryDistinctOperator", "id": 5, "inputs": [port(1), port(4)]},
                {"class": "DBSPSumOperator", "id": 6, "inputs": [port(5), port(3)]}
            ]
        }))
        .unwrap();
        assert_eq!(
            monotonicity_transfers(&graph).unwrap(),
            [2, 4, 5].map(NodeId::Outer)
        );
    }
}
