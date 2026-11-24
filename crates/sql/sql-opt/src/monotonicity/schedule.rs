//! Schedule transfer functions on the delta-expanded circuit. A transfer can
//! only propagate an existing waterline, except where the operator establishes
//! a bound itself. Expression analysis determines which fields retain it.
use crate::state_bounds::column_lateness;
use feldera_sql_ir::{BodyOrder, Circuit, Error, InnerKind, Node, NodeId, OperatorKind as Op};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ClosureBody {
    Operator,
    /// Deindex discards the key. Its value must carry a bound of its own.
    IndexedValue,
    Filter,
    Hop,
    Lag,
}

/// Source of a lower bound for one column of a table or view. A closure
/// upstream of a view can drop all bounds, even when its transfer was scheduled.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ColumnWaterline {
    Declared,
    InheritIfPresent,
    Unbounded,
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
        columns: Vec<ColumnWaterline>,
    },
    ProjectIndex {
        projection: IndexedWaterline,
    },
    ApplyClosure {
        function: ClosureBody,
    },
    Analyze,
    JoinClosure,
    AggregateKey,
    RollingTimestamp,
    InvalidRollingOffset,
    IntersectRows,
    IntersectKeys,
    PrimitiveMinimumOrKey,
    ChainMinimumOrKey,
    InvalidIndexedSource,
}

fn primitive_transfer(circuit: &Circuit, node: &Node) -> MonotonicityTransfer {
    use MonotonicityTransfer::{AggregateKey, Analyze, PrimitiveMinimumOrKey};
    // A known absent function propagates the key. Older compact snapshots
    // can omit both the function and the classification.
    if node.data("function").is_some_and(|v| v.is_null()) {
        return AggregateKey;
    }
    let kind = node
        .data("aggregateKind")
        .and_then(|v| v.as_str())
        .or_else(|| {
            let function = circuit.node(node.field("function")?.node_id()?)?;
            if function.class() == "DBSPMinMax" {
                function.data("aggregation")?.as_str()
            } else {
                Some("Other")
            }
        });
    match kind {
        Some("Min" | "MinSome1") => PrimitiveMinimumOrKey,
        Some(_) => AggregateKey,
        None => Analyze, // Older compact topologies omit the aggregate function.
    }
}

fn relation_transfer(
    node: &Node,
    field: &str,
    inherit: bool,
) -> Result<Option<MonotonicityTransfer>, Error> {
    let lateness = column_lateness(node, field)?.collect::<Result<Vec<_>, _>>()?;
    if !lateness.contains(&true) {
        return Ok(inherit.then_some(MonotonicityTransfer::Identity { input: 0 }));
    }
    let undeclared = if inherit {
        ColumnWaterline::InheritIfPresent
    } else {
        ColumnWaterline::Unbounded
    };
    let columns = lateness
        .into_iter()
        .map(|declared| {
            if declared {
                ColumnWaterline::Declared
            } else {
                undeclared
            }
        })
        .collect();
    Ok(Some(MonotonicityTransfer::DeclaredColumns { columns }))
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

fn asof_ascending(circuit: &Circuit, node: &Node) -> Option<bool> {
    let order = match node.data("directComparator").and_then(|v| v.as_bool()) {
        Some(false) => return Some(false),
        Some(true) => node,
        None => {
            let order = circuit.node(node.field("comparator")?.node_id()?)?;
            if order.class() != "DBSPDirectComparatorExpression" {
                return Some(false);
            }
            order
        }
    };
    order.data("ascending")?.as_bool()
}

fn rolling_transfer(
    circuit: &Circuit,
    node: &Node,
    bounded_input: bool,
) -> Result<Option<MonotonicityTransfer>, Error> {
    use MonotonicityTransfer::{Analyze, InvalidRollingOffset, RollingTimestamp};

    let timestamp = bounded_input.then_some(RollingTimestamp);
    if let Some(bound) = node.field("upper").and_then(|f| f.node_id()) {
        let bound = circuit.node(bound).expect("validated upper bound");
        match bound.data("isPreceding").and_then(|v| v.as_bool()) {
            Some(true) => return Ok(timestamp),
            Some(false) => {}
            None => return Err(Error::new("rolling upper bound requires isPreceding")),
        }
        let offset = circuit
            .node(bound.edge("representation")?)
            .expect("validated offset");
        if !matches!(offset.inner_kind(), Some(InnerKind::Expression(kind)) if kind.is_numeric_literal())
        {
            return Ok(None);
        }
        // The visitor checks the frame before looking for an input waterline.
        if offset.data("value").is_none_or(|value| value.is_null()) {
            return Ok(Some(InvalidRollingOffset));
        }
        return Ok(if feldera_sql_expr::numeric_value_is_positive(offset)? {
            None
        } else {
            timestamp
        });
    }
    // Earlier compact topologies sent the frame classification without its expression.
    let following = match node.data("upperPreceding").and_then(|v| v.as_bool()) {
        Some(true) => Some(false),
        Some(false) if node.data("upperDynamic").and_then(|v| v.as_bool()) == Some(true) => {
            Some(true)
        }
        Some(false) => node.data("upperPositive").and_then(|v| v.as_bool()),
        None => None,
    };
    Ok(match following {
        Some(false) => timestamp,
        Some(true) => None,
        None => bounded_input.then_some(Analyze),
    })
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
            Some(Op::SourceMap | Op::SourceLazyMap) => Some(InvalidIndexedSource),
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
            Some(Op::ChainAggregate) => first.then_some(ChainMinimumOrKey),
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
            Some(Op::Map | Op::MapIndex | Op::FlatMap) => first.then_some(ApplyClosure {
                function: ClosureBody::Operator,
            }),
            Some(Op::Deindex) => first.then_some(ApplyClosure {
                function: ClosureBody::IndexedValue,
            }),
            Some(Op::Hop) => first.then_some(ApplyClosure {
                function: ClosureBody::Hop,
            }),
            Some(Op::Lag) => first.then_some(ApplyClosure {
                function: ClosureBody::Lag,
            }),
            Some(Op::Filter) if first => {
                let indexed = node
                    .data("indexedRows")
                    .and_then(|v| v.as_bool())
                    .or_else(|| {
                        let output = node.field("outputType")?.node_id()?;
                        Some(circuit.node(output)?.class() == "DBSPTypeIndexedZSet")
                    });
                Some(match indexed {
                    Some(true) => Identity { input: 0 },
                    Some(false) => ApplyClosure {
                        function: ClosureBody::Filter,
                    },
                    // Earlier compact snapshots omit the output stream type.
                    None => Analyze,
                })
            }
            Some(Op::BinaryDistinct) => inputs
                .next()
                .unwrap_or(false)
                .then_some(Identity { input: 1 }),
            Some(Op::AntiJoin) => {
                (first && inputs.next().unwrap_or(false)).then_some(IntersectKeys)
            }
            Some(Op::Sum | Op::AtomicSum | Op::Subtract) => {
                // An empty sum must still receive the visitor's normal validation.
                node.inputs()
                    .all(|p| bounded.contains(&p.operator))
                    .then_some(IntersectRows)
            }
            Some(Op::StreamJoin | Op::StreamJoinIndex) => {
                (first || inputs.next().unwrap_or(false)).then_some(JoinClosure)
            }
            Some(Op::AsofJoin) if first || inputs.next().unwrap_or(false) => {
                // Only a left timestamp at or after the right timestamp has a lower bound.
                match asof_ascending(circuit, node) {
                    Some(true) => Some(JoinClosure),
                    Some(false) => None,
                    None => Some(Analyze),
                }
            }
            Some(Op::AggregateLinearPostprocess) => first.then_some(AggregateKey),
            Some(Op::PartitionedRollingAggregate) => rolling_transfer(circuit, node, first)?,
            Some(Op::PrimitiveAggregate) => first.then(|| primitive_transfer(circuit, node)),
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
    fn set_operations_and_minimums_select_their_distinct_bound_rules() -> Result<(), Error> {
        use MonotonicityTransfer::*;
        let port = json!({"operator": {"node": 1}, "outputNumber": 0});
        let mut operators = vec![json!({"class": "DBSPSourceMultisetOperator", "id": 1,
            "inputs": [], "metadata": {"columnMetadata": [{"lateness": true}]}})];
        let mut expected = Vec::new();
        for (class, kind, rule, arity) in [
            ("DBSPSumOperator", None, IntersectRows, 3),
            ("DBSPSubtractOperator", None, IntersectRows, 2),
            ("DBSPAntiJoinOperator", None, IntersectKeys, 2),
            (
                "DBSPPrimitiveAggregateOperator",
                Some("Min"),
                PrimitiveMinimumOrKey,
                1,
            ),
            (
                "DBSPPrimitiveAggregateOperator",
                Some("MinSome1"),
                PrimitiveMinimumOrKey,
                1,
            ),
            (
                "DBSPPrimitiveAggregateOperator",
                Some("ArgMinSome"),
                AggregateKey,
                1,
            ),
            (
                "DBSPPrimitiveAggregateOperator",
                Some("Other"),
                AggregateKey,
                1,
            ),
            ("DBSPPrimitiveAggregateOperator", None, Analyze, 1),
            ("DBSPChainAggregateOperator", None, ChainMinimumOrKey, 1),
            ("DBSPSourceLazyMapOperator", None, InvalidIndexedSource, 0),
        ] {
            let id = operators.len() as u64 + 1;
            let mut operator =
                json!({"class": class, "id": id, "inputs": vec![port.clone(); arity]});
            if let Some(kind) = kind {
                operator["aggregateKind"] = json!(kind);
            }
            operators.push(operator);
            expected.push((NodeId::Outer(id), rule));
        }
        let graph = Circuit::from_json(json!({"class":"DBSPCircuit", "id":0,
            "declarations":[], "allOperators":operators}))?;
        assert_eq!(monotonicity_transfers(&graph)?[1..], expected);
        Ok(())
    }

    #[test]
    fn primitive_waterlines_distinguish_minimums_absent_functions_and_missing_details()
    -> Result<(), Error> {
        use MonotonicityTransfer::{AggregateKey, Analyze, PrimitiveMinimumOrKey};
        let input = json!({"operator":{"node":1},"outputNumber":0});
        let mut operators = vec![
            json!({"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
            "metadata":{"columnMetadata":[{"lateness":true}]}}),
        ];
        let mut expected = Vec::new();
        for (function, rule) in [
            (
                Some(json!({"class":"DBSPMinMax","id":1,"aggregation":"Min"})),
                PrimitiveMinimumOrKey,
            ),
            (Some(json!({"node":1})), PrimitiveMinimumOrKey),
            (
                Some(json!({"class":"DBSPMinMax","id":2,"aggregation":"MinSome1"})),
                PrimitiveMinimumOrKey,
            ),
            (
                Some(json!({"class":"DBSPMinMax","id":3,"aggregation":"Max"})),
                AggregateKey,
            ),
            (Some(json!({"class":"DBSPFold","id":4})), AggregateKey),
            (Some(json!(null)), AggregateKey),
            (None, Analyze),
        ] {
            let id = operators.len() as u64 + 1;
            let mut aggregate =
                json!({"class":"DBSPPrimitiveAggregateOperator","id":id,"inputs":[input,input]});
            if let Some(function) = function {
                aggregate["function"] = function;
            }
            operators.push(aggregate);
            expected.push((NodeId::Outer(id), rule));
        }
        let circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,
            "declarations":[],"allOperators":operators}))?;
        assert_eq!(monotonicity_transfers(&circuit)?[1..], expected);
        Ok(())
    }

    #[test]
    fn asof_order_and_rolling_upper_bound_control_timestamp_transfers() -> Result<(), Error> {
        use MonotonicityTransfer::{Analyze, JoinClosure, RollingTimestamp};
        let input = json!({"outputNumber": 0, "operator": {"node": 1}});
        let mut operators = vec![
            json!({"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
            "metadata":{"columnMetadata":[{"lateness":true}]}}),
        ];
        let mut expected = Vec::new();
        for (class, facts, rule) in [
            (
                "DBSPAsofJoinOperator",
                json!({"directComparator": true, "ascending": true}),
                Some(JoinClosure),
            ),
            (
                "DBSPAsofJoinOperator",
                json!({"directComparator": true, "ascending": false}),
                None,
            ),
            (
                "DBSPAsofJoinOperator",
                json!({"directComparator": false}),
                None,
            ),
            ("DBSPAsofJoinOperator", json!({}), Some(Analyze)),
            (
                "DBSPAsofJoinOperator",
                json!({"comparator":{"class":"DBSPDirectComparatorExpression","id":1,"ascending":true}}),
                Some(JoinClosure),
            ),
            (
                "DBSPAsofJoinOperator",
                json!({"comparator":{"class":"DBSPDirectComparatorExpression","id":2,"ascending":false}}),
                None,
            ),
            (
                "DBSPAsofJoinOperator",
                json!({"comparator":{"class":"DBSPFieldComparatorExpression","id":3,"ascending":true}}),
                None,
            ),
            (
                "DBSPPartitionedRollingAggregateOperator",
                json!({"upperPreceding": true}),
                Some(RollingTimestamp),
            ),
            (
                "DBSPPartitionedRollingAggregateOperator",
                json!({"upperPreceding": false, "upperPositive": false}),
                Some(RollingTimestamp),
            ),
            (
                "DBSPPartitionedRollingAggregateOperator",
                json!({"upperPreceding": false, "upperPositive": true}),
                None,
            ),
            (
                "DBSPPartitionedRollingAggregateOperator",
                json!({"upperPreceding": false, "upperDynamic": true}),
                None,
            ),
            (
                "DBSPPartitionedRollingAggregateOperator",
                json!({"upperPreceding": false}),
                Some(Analyze),
            ),
        ] {
            let id = operators.len() as u64 + 1;
            let mut op = facts;
            op["class"] = json!(class);
            op["id"] = json!(id);
            op["inputs"] = if class == "DBSPAsofJoinOperator" {
                json!([input, input])
            } else {
                json!([input])
            };
            operators.push(op);
            if let Some(rule) = rule {
                expected.push((NodeId::Outer(id), rule));
            }
        }
        let circuit = Circuit::from_json(json!({"class": "DBSPCircuit", "id": 0,
            "declarations": [], "allOperators": operators}))?;
        assert_eq!(&monotonicity_transfers(&circuit)?[1..], expected);
        Ok(())
    }

    #[test]
    fn upper_frame_offsets_use_the_literal_value_from_the_circuit() -> Result<(), Error> {
        use MonotonicityTransfer::{InvalidRollingOffset, RollingTimestamp};
        for (preceding, class, value, rule) in [
            (true, "DBSPI64Literal", json!("1"), Some(RollingTimestamp)),
            (false, "DBSPU128Literal", json!(u128::MAX.to_string()), None),
            (false, "DBSPDecimalLiteral", json!("1E-1000"), None),
            (
                false,
                "DBSPShortIntervalLiteral",
                json!("-1"),
                Some(RollingTimestamp),
            ),
            (
                false,
                "DBSPRealLiteral",
                json!((-0.0_f32).to_bits().cast_signed()),
                Some(RollingTimestamp),
            ),
            (
                false,
                "DBSPDoubleLiteral",
                json!(f64::NAN.to_bits().cast_signed()),
                Some(RollingTimestamp),
            ),
            (false, "DBSPI64Literal", json!(null), Some(InvalidRollingOffset)),
            (true, "DBSPI64Literal", json!(null), Some(RollingTimestamp)),
            (false, "DBSPVariablePath", json!(null), None),
        ] {
            let mut offset = json!({"class":class,"id":2});
            if !value.is_null() {
                offset["value"] = value;
            }
            let graph = Circuit::from_json(json!({
                "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                    {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
                     "metadata":{"columnMetadata":[{"lateness":true}]}},
                    {"class":"DBSPPartitionedRollingAggregateOperator","id":2,
                     "inputs":[{"outputNumber":0,"operator":{"node":1}}],
                     "upper":{"class":"DBSPWindowBoundExpression","id":1,"isPreceding":preceding,
                              "representation":offset}}
                ]
            }))?;
            let transfers = monotonicity_transfers(&graph)?;
            assert_eq!(transfers.get(1).map(|(_, t)| t), rule.as_ref(), "{class}");
        }
        Ok(())
    }

    #[test]
    fn indexed_filters_keep_their_input_waterline_and_row_filters_refine_it() -> Result<(), Error> {
        use MonotonicityTransfer::{Analyze, ApplyClosure, Identity};
        let input = json!([{"outputNumber": 0, "operator": {"node": 1}}]);
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                 "metadata": {"columnMetadata": [{"lateness": true}]}},
                {"class": "DBSPFilterOperator", "id": 2, "inputs": input, "indexedRows": true},
                {"class": "DBSPFilterOperator", "id": 3, "inputs": input, "indexedRows": false},
                {"class": "DBSPFilterOperator", "id": 4, "inputs": input},
                {"class": "DBSPFilterOperator", "id": 5, "inputs": input,
                 "outputType": {"class": "DBSPTypeIndexedZSet", "id": 10}},
                {"class": "DBSPDeindexOperator", "id": 6, "inputs": input},
                {"class": "DBSPHopOperator", "id": 7, "inputs": input}
            ]
        }))?;
        assert_eq!(
            &monotonicity_transfers(&graph)?[1..],
            &[
                (NodeId::Outer(2), Identity { input: 0 }),
                (
                    NodeId::Outer(3),
                    ApplyClosure {
                        function: ClosureBody::Filter
                    }
                ),
                (NodeId::Outer(4), Analyze),
                (NodeId::Outer(5), Identity { input: 0 }),
                (
                    NodeId::Outer(6),
                    ApplyClosure {
                        function: ClosureBody::IndexedValue
                    }
                ),
                (
                    NodeId::Outer(7),
                    ApplyClosure {
                        function: ClosureBody::Hop
                    }
                ),
            ]
        );
        Ok(())
    }

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
                        columns: vec![
                            ColumnWaterline::Unbounded,
                            ColumnWaterline::Declared,
                            ColumnWaterline::Unbounded
                        ]
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
                        columns: vec![ColumnWaterline::Declared]
                    }
                ),
                (
                    NodeId::Outer(2),
                    MonotonicityTransfer::ApplyClosure {
                        function: ClosureBody::Lag
                    }
                ),
                (NodeId::Outer(5), MonotonicityTransfer::JoinClosure),
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
                {"class": "DBSPSinkOperator", "id": 4, "inputs": [port(3)]},
                {"class": "DBSPViewOperator", "id": 5, "inputs": [port(2)], "tupleRows": true,
                 "metadata": {"columns": [{}, {"lateness": true}, {}]}},
                {"class": "DBSPViewOperator", "id": 6, "inputs": [port(5)],
                 "outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeTuple","id":2}},
                 "metadata":{"columns":[{},{},{}]}},
                {"class": "DBSPViewOperator", "id": 7, "inputs": [port(6)],
                 "outputType":{"class":"DBSPTypeZSet","id":3,"elementType":{"class":"DBSPTypeVec","id":4}},
                 "metadata":{"columns":[{"lateness":true}]}}
            ]
        }))?;
        assert_eq!(
            monotonicity_transfers(&graph)?,
            [
                (
                    NodeId::Outer(1),
                    MonotonicityTransfer::DeclaredColumns {
                        columns: vec![
                            ColumnWaterline::Declared,
                            ColumnWaterline::Unbounded,
                            ColumnWaterline::Declared
                        ]
                    }
                ),
                (
                    NodeId::Outer(2),
                    MonotonicityTransfer::Identity { input: 0 }
                ),
                (
                    NodeId::Outer(5),
                    MonotonicityTransfer::DeclaredColumns {
                        columns: vec![
                            ColumnWaterline::InheritIfPresent,
                            ColumnWaterline::Declared,
                            ColumnWaterline::InheritIfPresent
                        ]
                    }
                ),
                (
                    NodeId::Outer(6),
                    MonotonicityTransfer::Identity { input: 0 }
                ),
            ]
        );
        Ok(())
    }
}
