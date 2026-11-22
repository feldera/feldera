//! Determine which published streams may contain duplicate rows. On a delta
//! stream this describes its integral, so a retraction alone does not imply
//! that the stream is a multiset.

use crate::lineage;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node,
    NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::BTreeMap;

pub type Multiplicities = BTreeMap<OutputPort, bool>;

pub(super) fn input_fact(facts: &Multiplicities, input: OutputPort) -> Result<bool, Error> {
    facts
        .get(&input)
        .copied()
        .ok_or_else(|| Error::new(format!("missing multiplicity for {input:?}")))
}

fn certainly_distinct_constant(value: &Node) -> Result<bool, Error> {
    match value.inner_kind() {
        // The indexed literal constructor represents only the empty collection.
        Some(InnerKind::Expression(Expr::IndexedZSet)) => Ok(true),
        Some(InnerKind::Expression(Expr::ZSet)) => match value.field("weights") {
            Some(Field::Array(weights)) => Ok(match weights.as_slice() {
                [] => true,
                [Field::Data(weight)] => matches!(weight.as_i64(), Some(0 | 1)),
                _ => false,
            }),
            _ => Err(Error::new("constant Z-set requires weights")),
        },
        _ => Err(Error::new(
            "constant operator requires a collection literal",
        )),
    }
}

fn emits_rank(edit: &CircuitEditor, op: &Node) -> Result<bool, Error> {
    let producer = edit.node(op.edge("outputProducer")?)?;
    let params = producer.edges("parameters")?;
    let rank = edit.node(
        *params
            .first()
            .ok_or_else(|| Error::new("rank producer needs a parameter"))?,
    )?;
    let body = edit.node(producer.edge("body")?)?;
    if !matches!(body.inner_kind(), Some(InnerKind::Expression(Expr::Tuple)))
        || !matches!(body.field("fields"), Some(Field::Array(_)))
    {
        return Ok(false);
    }
    for id in body.edges("fields")? {
        let field = edit.node(id)?;
        if matches!(
            field.inner_kind(),
            Some(InnerKind::Expression(Expr::VariablePath))
        ) && field.data("variable") == rank.data("name")
        {
            return Ok(true);
        }
    }
    Ok(false)
}

// Compare the coefficient and decimal point exactly, including exponent notation.
fn decimal_one(raw: &str) -> bool {
    let (mantissa, exponent) = match raw.split_once(['e', 'E']) {
        Some((digits, exp)) => match exp.parse::<i64>() {
            Ok(exp) => (digits, exp),
            Err(_) => return false,
        },
        None => (raw, 0),
    };
    let mantissa = mantissa.strip_prefix('+').unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits: Vec<_> = whole.bytes().chain(fraction.bytes()).collect();
    let Some(one) = digits.iter().position(|&c| c != b'0') else {
        return false;
    };
    digits[one] == b'1'
        && digits[one + 1..].iter().all(|&c| c == b'0')
        && exponent == one as i64 + 1 - whole.len() as i64
}

fn is_limit_one(edit: &CircuitEditor, mut id: NodeId) -> Result<bool, Error> {
    // A serializer may share the underlying literal or wrap it in several casts.
    let mut seen = std::collections::HashSet::new();
    loop {
        let node = edit.node(id)?;
        let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
            return Err(Error::new("rank limit requires an expression"));
        };
        match kind {
            Expr::Cast if seen.insert(id) => id = node.edge("source")?,
            Expr::Cast => return Err(Error::new("cycle in rank limit")),
            Expr::DecimalLiteral
            | Expr::USizeLiteral
            | Expr::I8Literal
            | Expr::I16Literal
            | Expr::I32Literal
            | Expr::I64Literal
            | Expr::I128Literal
            | Expr::U8Literal
            | Expr::U16Literal
            | Expr::U32Literal
            | Expr::U64Literal
            | Expr::U128Literal => {
                let Some(value) = node.data("value") else {
                    return Ok(false);
                };
                let raw = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                return Ok(decimal_one(&raw));
            }
            _ => return Ok(false),
        }
    }
}

/// Operators whose multiplicity needs expression data once their inputs are known.
#[derive(Clone, Copy)]
enum Payload {
    Constant,
    Rank,
    Chain,
    Mapping,
}

fn infer(
    node: &Node,
    facts: &Multiplicities,
    inspect: impl FnOnce(Payload) -> Result<bool, Error>,
) -> Result<bool, Error> {
    let first = || {
        node.inputs()
            .next()
            .ok_or_else(|| Error::new("operator requires a data input"))
    };
    let may_repeat = || {
        for port in node.inputs() {
            if input_fact(facts, port)? {
                return Ok(true);
            }
        }
        Ok::<_, Error>(false)
    };
    let kind = node
        .operator_kind()
        .ok_or_else(|| Error::new("expected operator"))?;
    Ok(match kind {
        Op::SourceMultiset
        | Op::ViewDeclaration
        | Op::Sum
        | Op::AtomicSum
        | Op::Subtract
        | Op::Negate
        | Op::FlatMap
        | Op::FlatMapIndex
        | Op::PartitionedRollingAggregate
        | Op::PartitionedRollingAggregateWithWaterline
        | Op::Weigh => true,
        Op::SourceMap
        | Op::SourceLazyMap
        | Op::Now
        | Op::Aggregate
        | Op::StreamAggregate
        | Op::AggregateZero
        | Op::AggregateLinearPostprocess
        | Op::AggregateLinearPostprocessRetainKeys
        | Op::ChainAggregate
        | Op::PrimitiveAggregate
        | Op::UpsertFeedback
        | Op::Apply
        | Op::Apply2
        | Op::ApplyN
        | Op::Waterline
        | Op::StreamDistinct
        | Op::Distinct
        | Op::BinaryDistinct
        | Op::InputMapWithWaterline
        | Op::Nested => false,
        Op::Constant | Op::IncrementalConstant => inspect(Payload::Constant)?,
        Op::ControlledKeyFilter
        | Op::Filter
        | Op::Positive
        | Op::Integrate
        | Op::Differentiate
        | Op::Delta
        | Op::Delay
        | Op::DelayedIntegral
        | Op::WeightValidator
        | Op::Noop
        | Op::View
        | Op::Sink
        | Op::Hop
        | Op::Window
        | Op::IntegrateTraceRetainKeys
        | Op::IntegrateTraceRetainValues
        | Op::IntegrateTraceRetainNValues
        | Op::AntiJoin
        | Op::StreamAntiJoin => input_fact(facts, first()?)?,
        Op::IndexedTopK | Op::Rank | Op::RowNumber => inspect(Payload::Rank)?,
        Op::Chain => may_repeat()? || inspect(Payload::Chain)?,
        Op::Map
        | Op::MapIndex
        | Op::Deindex
        | Op::Lag
        | Op::Join
        | Op::JoinIndex
        | Op::LeftJoin
        | Op::LeftJoinIndex
        | Op::JoinFilterMap
        | Op::LeftJoinFilterMap
        | Op::StreamJoin
        | Op::StreamJoinIndex
        | Op::StarJoin
        | Op::StarJoinIndex
        | Op::StarJoinFilterMap
        | Op::AsofJoin
        | Op::ConcreteAsofJoin => may_repeat()? || inspect(Payload::Mapping)?,
    })
}

impl Payload {
    fn may_repeat(self, edit: &CircuitEditor, node: &Node) -> Result<bool, Error> {
        Ok(match self {
            Self::Constant => !certainly_distinct_constant(edit.node(node.edge("function")?)?)?,
            Self::Rank => {
                let kind = node.operator_kind();
                let unique_numbers = kind == Some(Op::RowNumber)
                    || node.data("numbering").and_then(|v| v.as_str()) == Some("ROW_NUMBER");
                !(unique_numbers
                    && (emits_rank(edit, node)?
                        || (kind == Some(Op::IndexedTopK)
                            && is_limit_one(edit, node.edge("limit")?)?)))
            }
            Self::Mapping => {
                !lineage::injective(
                    edit,
                    node.edge("function")?,
                    node.operator_kind() == Some(Op::Lag),
                )? || match node.field("map").and_then(Field::node_id) {
                    Some(map) => !lineage::injective(edit, map, false)?,
                    None => false,
                }
            }
            Self::Chain => {
                let functions = node.edges("chain")?;
                let Some(Field::Array(kinds)) = node.field("kinds") else {
                    return Err(Error::new("chain requires computation kinds"));
                };
                if kinds.len() != functions.len() {
                    return Err(Error::new("chain kind count differs from closure count"));
                }
                for (kind, f) in kinds.iter().zip(functions) {
                    if !matches!(kind, Field::Data(v) if v == "Filter")
                        && !lineage::injective(edit, f, false)?
                    {
                        return Ok(true);
                    }
                }
                false
            }
        })
    }
}

/// Topology alone often determines every multiplicity. Otherwise request all
/// operators whose expressions could affect the result. Treat unresolved
/// outputs as sets during selection: downstream maps on sets need a proof
/// of injectivity, whereas maps on multisets need no expression data.
pub enum MultiplicityPlan {
    Complete(Multiplicities),
    NeedsPayloads(Vec<NodeId>),
}

pub fn plan_multiplicities(circuit: &Circuit) -> Result<MultiplicityPlan, Error> {
    let mut facts = Multiplicities::new();
    let mut missing = Vec::new();
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        let node = circuit.node(id).expect("ordered operator");
        publish_outputs(id, node, &mut facts, |_| {
            missing.push(id);
            Ok(false)
        })?;
    }
    Ok(if missing.is_empty() {
        MultiplicityPlan::Complete(facts)
    } else {
        MultiplicityPlan::NeedsPayloads(missing)
    })
}

/// All live output ports, including the error and waterline outputs of sources.
/// Recursive regions apply distinct at their published boundary.
pub fn stream_multiplicities(circuit: &Circuit) -> Result<Multiplicities, Error> {
    let edit = CircuitEditor::new(circuit.clone());
    let mut facts = Multiplicities::new();
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        record_outputs(&edit, id, &mut facts)?;
    }
    Ok(facts)
}

pub(super) fn record_outputs(
    edit: &CircuitEditor,
    id: NodeId,
    facts: &mut Multiplicities,
) -> Result<(), Error> {
    let node = edit.node(id)?;
    publish_outputs(id, node, facts, |payload| payload.may_repeat(edit, node))
}

/// Lower bound on multiplicity, using only operator kinds and ports.
pub(super) fn record_topology_outputs(
    id: NodeId,
    node: &Node,
    facts: &mut Multiplicities,
) -> Result<(), Error> {
    publish_outputs(id, node, facts, |_| Ok(false))
}

fn publish_outputs(
    id: NodeId,
    node: &Node,
    facts: &mut Multiplicities,
    inspect: impl FnOnce(Payload) -> Result<bool, Error>,
) -> Result<(), Error> {
    if node.operator_kind() == Some(Op::Nested) {
        let Some(Field::Array(ports)) = node.field("internalOutputs") else {
            return Err(Error::new("missing nested outputs"));
        };
        for (number, port) in ports.iter().enumerate() {
            if OutputPort::from_field(port).is_some() {
                facts.insert(
                    OutputPort {
                        operator: id,
                        number,
                    },
                    false,
                );
            }
        }
    } else {
        let primary = infer(node, facts, inspect)?;
        facts.insert(
            OutputPort {
                operator: id,
                number: 0,
            },
            primary,
        );
        if matches!(
            node.operator_kind(),
            Some(Op::InputMapWithWaterline | Op::ControlledKeyFilter)
        ) {
            facts.insert(
                OutputPort {
                    operator: id,
                    number: 1,
                },
                true,
            );
        }
        if node.operator_kind() == Some(Op::InputMapWithWaterline) {
            facts.insert(
                OutputPort {
                    operator: id,
                    number: 2,
                },
                false,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rank_limit_one_is_exact() {
        for raw in ["1", "01.00", "+1E+0", ".001e3", "100000000000000000000e-20"] {
            assert!(decimal_one(raw), "{raw}");
        }
        for raw in [
            "0",
            "-1",
            "1.0000000000000000000001",
            "0.999999999999999999",
            "NaN",
            "1e-1",
            "1e9223372036854775808",
        ] {
            assert!(!decimal_one(raw), "{raw}");
        }
    }

    #[test]
    fn payload_selection_follows_possible_sets_through_maps() {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPMapOperator","id":2,"inputs":[{"operator":{"node":1},"outputNumber":0}]},
                {"class":"DBSPDistinctOperator","id":3,"inputs":[{"operator":{"node":2},"outputNumber":0}]},
                {"class":"DBSPMapOperator","id":4,"inputs":[{"operator":{"node":3},"outputNumber":0}]},
                {"class":"DBSPMapOperator","id":5,"inputs":[{"operator":{"node":4},"outputNumber":0}]}
            ]
        })).unwrap();
        let MultiplicityPlan::NeedsPayloads(ids) = plan_multiplicities(&circuit).unwrap() else {
            panic!("map on a distinct input needs its closure");
        };
        assert_eq!(ids, [NodeId::Outer(4), NodeId::Outer(5)]);
    }
}
