//! Empty collection propagation for the flat circuit subset.

use crate::model::{Circuit, CollectionKind, Edit, EmptyInputRule, OperatorKind, Port};
use crate::{Error, Result};

pub(crate) fn propagate(circuit: &Circuit) -> Result<Vec<Edit>> {
    let mut aliases = Vec::<Option<Port>>::with_capacity(circuit.operators.len());
    let mut empty_outputs = Vec::<Option<CollectionKind>>::with_capacity(circuit.operators.len());
    let mut edits = Vec::with_capacity(circuit.operators.len());

    for operator in &circuit.operators {
        let empty = |port: Port| empty_outputs[port.operator.0];
        let mut inputs: Vec<_> = operator
            .inputs
            .iter()
            .map(|port| aliases[port.operator.0].unwrap_or(*port))
            .collect();
        let mut alias = None;
        let mut empty_output = None;
        let edit = match &operator.kind {
            OperatorKind::Constant {
                weights, stream, ..
            } if weights.is_empty() => {
                empty_output = Some(*stream);
                Edit::Retain(inputs)
            }
            OperatorKind::Sum => {
                let first_kind = empty(inputs[0]);
                inputs.retain(|port| empty(*port).is_none());
                match inputs.as_slice() {
                    [] => {
                        let kind =
                            first_kind.expect("nonempty original sum with only empty inputs");
                        empty_output = Some(kind);
                        Edit::Empty(kind)
                    }
                    [only] => {
                        alias = Some(*only);
                        Edit::Bypass
                    }
                    _ => Edit::Retain(inputs),
                }
            }
            OperatorKind::InnerJoin(_, kind)
                if inputs.iter().any(|port| empty(*port).is_some()) =>
            {
                empty_output = Some(*kind);
                Edit::Empty(*kind)
            }
            OperatorKind::StreamOperator {
                empty: rule,
                collection,
                ..
            } => {
                use EmptyInputRule::*;
                let is_empty = |port: &Port| empty(*port).is_some();
                let first_empty = inputs.first().is_some_and(is_empty);
                let right_empty = inputs.get(1).is_some_and(is_empty);
                let replace = match rule {
                    Keep => false,
                    First | StreamAntiJoin | LeftJoin => first_empty,
                    Any => inputs.iter().any(is_empty),
                    AtomicSum => {
                        inputs.retain(|port| !is_empty(port));
                        inputs.is_empty()
                    }
                    Subtract => first_empty && right_empty,
                };
                if replace {
                    let kind = collection
                        .ok_or_else(|| Error("empty replacement requires a data stream".into()))?;
                    empty_output = Some(kind);
                    Edit::Empty(kind)
                } else if right_empty && matches!(rule, Subtract | StreamAntiJoin) {
                    alias = inputs.first().copied();
                    Edit::Bypass
                } else if right_empty && matches!(rule, LeftJoin) {
                    Edit::MapUnmatchedLeft(inputs[0])
                } else {
                    Edit::Retain(inputs)
                }
            }
            OperatorKind::Differentiate
            | OperatorKind::Integrate
            | OperatorKind::Distinct(_)
            | OperatorKind::Negate
            | OperatorKind::Positive
            | OperatorKind::Noop
            | OperatorKind::Filter(_)
            | OperatorKind::View { .. }
            | OperatorKind::Map(_)
            | OperatorKind::Deindex(_)
            | OperatorKind::ChainAggregate
            | OperatorKind::FlatMap => match empty(inputs[0]) {
                Some(kind) => {
                    let kind = match (&operator.kind, kind) {
                        (OperatorKind::Differentiate, CollectionKind::Snapshot) => {
                            CollectionKind::Delta
                        }
                        (OperatorKind::Integrate, CollectionKind::Delta) => {
                            CollectionKind::Snapshot
                        }
                        (OperatorKind::Differentiate | OperatorKind::Integrate, _) => {
                            return Err(Error(
                                "invalid stream kind at empty collection conversion".into(),
                            ));
                        }
                        _ => kind,
                    };
                    empty_output = Some(kind);
                    Edit::Empty(kind)
                }
                None => Edit::Retain(inputs),
            },
            _ => Edit::Retain(inputs),
        };
        aliases.push(alias);
        empty_outputs.push(empty_output);
        edits.push(edit);
    }
    Ok(edits)
}
