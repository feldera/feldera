//! Schedule transfer functions on the delta-expanded circuit. A transfer can
//! only propagate an existing waterline, except where the operator establishes
//! a bound itself. Expression analysis determines which fields retain it.
use crate::state_bounds::has_lateness;
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "rule")]
pub enum MonotonicityTransfer {
    Identity { input: usize },
    ApplyClosure,
    Analyze,
}

pub fn monotonicity_transfers(
    circuit: &Circuit,
) -> Result<Vec<(NodeId, MonotonicityTransfer)>, Error> {
    use MonotonicityTransfer::*;
    let mut bounded = HashSet::new();
    let mut transfers = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        let mut inputs = node.inputs().map(|p| bounded.contains(&p.operator));
        let first = inputs.next().unwrap_or(false);
        let clock = || {
            node.data("annotations")
                .and_then(|v| v.as_array())
                .is_some_and(|items| items.iter().any(|a| a["class"] == "AlwaysMonotone"))
        };
        let transfer = match node.operator_kind() {
            // The expression visitor reports indexed sources as invalid at this stage.
            Some(Op::SourceMap | Op::SourceLazyMap) => Some(Analyze),
            Some(Op::SourceMultiset) => has_lateness(node, "columnMetadata")?.then_some(Analyze),
            Some(Op::View) => (first || has_lateness(node, "columns")?).then_some(Analyze),
            Some(Op::Window) => {
                let unbounded = node
                    .data("lowerUnbounded")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("window requires its lower-bound policy"))?;
                (!unbounded || first).then_some(Analyze)
            }
            Some(Op::ChainAggregate) => (first || clock()).then_some(Analyze),
            Some(Op::DelayedIntegral) => (first && clock()).then_some(Identity { input: 0 }),
            Some(
                Op::Sink
                | Op::Distinct
                | Op::Positive
                | Op::UpsertFeedback
                | Op::Noop
                | Op::Negate
                | Op::StreamAntiJoin,
            ) => first.then_some(Identity { input: 0 }),
            Some(Op::Map | Op::MapIndex | Op::FlatMap) => first.then_some(ApplyClosure),
            Some(Op::BinaryDistinct) => inputs
                .next()
                .unwrap_or(false)
                .then_some(Identity { input: 1 }),
            Some(Op::AntiJoin) => (first && inputs.next().unwrap_or(false)).then_some(Analyze),
            Some(Op::Sum | Op::AtomicSum | Op::Subtract) => {
                // An empty sum must still receive the visitor's normal validation.
                node.inputs()
                    .all(|p| bounded.contains(&p.operator))
                    .then_some(Analyze)
            }
            Some(Op::StreamJoin | Op::StreamJoinIndex | Op::AsofJoin) => {
                (first || inputs.next().unwrap_or(false)).then_some(Analyze)
            }
            // The enclosing recursive operator publishes no monotonicity facts.
            // Its individual children already occurred in body order.
            Some(Op::Nested) => None,
            _ => first.then_some(Analyze),
        };
        if let Some(transfer) = transfer {
            transfers.push((id, transfer));
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
            [
                (NodeId::Outer(2), MonotonicityTransfer::Analyze),
                (NodeId::Outer(4), MonotonicityTransfer::Analyze),
                (
                    NodeId::Outer(5),
                    MonotonicityTransfer::Identity { input: 1 }
                ),
            ]
        );
    }
}
