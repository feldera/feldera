//! Schedule transfer functions on the delta-expanded circuit. A transfer can
//! only propagate an existing waterline, except where the operator establishes
//! a bound itself. Expression analysis determines which fields retain it.
use crate::state_bounds::column_lateness;
use feldera_sql_ir::{BodyOrder, Circuit, Error, Node, NodeId, OperatorKind as Op};
use serde::Serialize;
use std::collections::HashSet;

/// Construct a waterline type from the corresponding live key or value type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum FieldWaterline {
    Whole,
    Unbounded,
    Unit,
    AllColumns,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexedWaterline {
    pub require_input_key: bool,
    pub key: FieldWaterline,
    pub value: FieldWaterline,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "rule")]
pub enum MonotonicityTransfer {
    Identity {
        input: usize,
    },
    /// Declared fields get new bounds. A view inherits the remaining fields
    /// from its input, and a source leaves the remaining fields unbounded.
    DeclaredColumns {
        lateness: Vec<bool>,
    },
    ProjectIndex {
        projection: IndexedWaterline,
    },
    ApplyClosure,
    Analyze,
}

fn relation_transfer(
    node: &Node,
    field: &str,
    inherit: bool,
) -> Result<Option<MonotonicityTransfer>, Error> {
    let lateness = column_lateness(node, field)?.collect::<Result<Vec<_>, _>>()?;
    Ok(if lateness.contains(&true) {
        Some(MonotonicityTransfer::DeclaredColumns { lateness })
    } else {
        inherit.then_some(MonotonicityTransfer::Identity { input: 0 })
    })
}

fn tuple_view(circuit: &Circuit, view: &Node) -> Result<bool, Error> {
    if let Some(tuple) = view.data("tupleRows").and_then(|value| value.as_bool()) {
        return Ok(tuple);
    }
    let stream = circuit
        .node(view.edge("outputType")?)
        .expect("validated stream");
    Ok(circuit
        .node(stream.edge("elementType")?)
        .is_some_and(|ty| ty.class() == "DBSPTypeTuple"))
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
            Some(Op::SourceMultiset) => relation_transfer(node, "columnMetadata", false)?,
            Some(Op::View) if tuple_view(circuit, node)? => {
                relation_transfer(node, "columns", first)?
            }
            Some(Op::View) => None, // ORDER BY returns a collection inside each row.
            Some(Op::Window) => {
                let unbounded = node
                    .data("lowerUnbounded")
                    .and_then(|v| v.as_bool())
                    .ok_or_else(|| Error::new("window requires its lower-bound policy"))?;
                // Without a lower bound, newly arriving rows may have arbitrary
                // keys unless the input key itself has a waterline.
                (!unbounded || first).then_some(ProjectIndex {
                    projection: IndexedWaterline {
                        require_input_key: unbounded,
                        key: FieldWaterline::Whole,
                        value: FieldWaterline::Unbounded,
                    },
                })
            }
            Some(Op::ChainAggregate) if clock() => Some(ProjectIndex {
                projection: IndexedWaterline {
                    require_input_key: false,
                    key: FieldWaterline::Unit,
                    value: FieldWaterline::AllColumns,
                },
            }),
            Some(Op::ChainAggregate) => first.then_some(Analyze),
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
            Some(
                Op::Deindex
                | Op::Filter
                | Op::Hop
                | Op::Lag
                | Op::PrimitiveAggregate
                | Op::AggregateLinearPostprocess
                | Op::PartitionedRollingAggregate,
            ) => first.then_some(Analyze),
            // The other operators have no transfer in the expanded graph.
            // Visiting an input with a bound does not give them a bound.
            // Nested circuits have already contributed their children in postorder.
            _ => None,
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
                {"class": "DBSPViewOperator", "id": 2, "inputs": [port(1)], "tupleRows": true,
                 "metadata": {"columns": [{}, {"lateness": true}, {}]}},
                {"class": "DBSPWindowOperator", "id": 3, "lowerUnbounded": true,
                 "inputs": [port(1), port(2)]},
                {"class": "DBSPWindowOperator", "id": 4, "lowerUnbounded": false,
                 "inputs": [port(1), port(2)]},
                {"class": "DBSPBinaryDistinctOperator", "id": 5, "inputs": [port(1), port(4)]},
                {"class": "DBSPSumOperator", "id": 6, "inputs": [port(5), port(3)]},
                {"class": "DBSPWindowOperator", "id": 7, "lowerUnbounded": true,
                 "inputs": [port(4), port(2)]},
                {"class": "DBSPChainAggregateOperator", "id": 8, "inputs": [port(1)],
                 "annotations": [{"class": "AlwaysMonotone"}]}
            ]
        }))
        .unwrap();
        assert_eq!(
            monotonicity_transfers(&graph).unwrap(),
            [
                (
                    NodeId::Outer(2),
                    MonotonicityTransfer::DeclaredColumns {
                        lateness: vec![false, true, false]
                    }
                ),
                (
                    NodeId::Outer(4),
                    MonotonicityTransfer::ProjectIndex {
                        projection: IndexedWaterline {
                            require_input_key: false,
                            key: FieldWaterline::Whole,
                            value: FieldWaterline::Unbounded,
                        },
                    }
                ),
                (
                    NodeId::Outer(5),
                    MonotonicityTransfer::Identity { input: 1 }
                ),
                (
                    NodeId::Outer(7),
                    MonotonicityTransfer::ProjectIndex {
                        projection: IndexedWaterline {
                            require_input_key: true,
                            key: FieldWaterline::Whole,
                            value: FieldWaterline::Unbounded,
                        },
                    }
                ),
                (
                    NodeId::Outer(8),
                    MonotonicityTransfer::ProjectIndex {
                        projection: IndexedWaterline {
                            require_input_key: false,
                            key: FieldWaterline::Unit,
                            value: FieldWaterline::AllColumns,
                        },
                    }
                ),
            ]
        );
    }

    #[test]
    fn an_operator_without_a_transfer_stops_propagation_on_its_branch() -> Result<(), Error> {
        let port = |id| json!({"outputNumber": 0, "operator": {"node": id}});
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                 "metadata": {"columnMetadata": [{"lateness": true}]}},
                {"class": "DBSPLagOperator", "id": 2, "inputs": [port(1)]},
                {"class": "DBSPIntegrateOperator", "id": 3, "inputs": [port(1)]},
                {"class": "DBSPMapOperator", "id": 4, "inputs": [port(3)]},
                {"class": "DBSPStreamJoinOperator", "id": 5, "inputs": [port(4), port(2)]},
                {"class": "DBSPAtomicSumOperator", "id": 6, "inputs": [port(4), port(2)]}
            ]
        }))?;
        assert_eq!(
            monotonicity_transfers(&graph)?,
            [
                (
                    NodeId::Outer(1),
                    MonotonicityTransfer::DeclaredColumns {
                        lateness: vec![true]
                    }
                ),
                (NodeId::Outer(2), MonotonicityTransfer::Analyze),
                (NodeId::Outer(5), MonotonicityTransfer::Analyze),
            ]
        );
        Ok(())
    }

    #[test]
    fn plain_views_inherit_lateness_only_for_tuple_rows() -> Result<(), Error> {
        let port = |id| json!({"outputNumber": 0, "operator": {"node": id}});
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                 "metadata": {"columnMetadata": [{"lateness": true}, {}, {"lateness": true}]}},
                {"class": "DBSPViewOperator", "id": 2, "inputs": [port(1)], "tupleRows": true,
                 "metadata": {"columns": [{}, {}, {}]}},
                {"class": "DBSPViewOperator", "id": 3, "inputs": [port(2)], "tupleRows": false,
                 "metadata": {"columns": [{"lateness": true}]}},
                {"class": "DBSPSinkOperator", "id": 4, "inputs": [port(3)]}
            ]
        }))?;
        assert_eq!(
            monotonicity_transfers(&graph)?,
            [
                (
                    NodeId::Outer(1),
                    MonotonicityTransfer::DeclaredColumns {
                        lateness: vec![true, false, true]
                    }
                ),
                (
                    NodeId::Outer(2),
                    MonotonicityTransfer::Identity { input: 0 }
                ),
            ]
        );
        Ok(())
    }
}
