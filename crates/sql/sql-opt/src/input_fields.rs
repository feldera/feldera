//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::{BTreeMap, BTreeSet};

pub fn input_projections(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let readers = reader_counts(circuit, &body_members(circuit)?);
    Ok(circuit
        .operator_order(BodyOrder::Postorder)?
        .into_iter()
        .filter(|&id| {
            let reader = circuit.node(id).expect("validated operator");
            matches!(reader.operator_kind(), Some(Op::Map | Op::MapIndex))
                && reader.inputs().next().is_some_and(|input| {
                    readers.get(&input.operator) == Some(&1)
                        && circuit.node(input.operator).and_then(|n| n.operator_kind())
                            == Some(Op::SourceMultiset)
                })
        })
        .collect())
}

#[derive(serde::Serialize)]
pub struct InputFieldUse {
    pub reader: u64,
    pub paths: BTreeSet<Vec<usize>>,
}

pub fn input_field_uses(circuit: &Circuit) -> Result<Vec<InputFieldUse>, Error> {
    input_projections(circuit)?
        .into_iter()
        .map(|reader| {
            let function = circuit
                .node(reader)
                .expect("validated projection")
                .edge("function")?;
            let NodeId::Outer(reader) = reader else {
                unreachable!("operator ID")
            };
            Ok(InputFieldUse {
                reader,
                paths: feldera_sql_expr::input_field_paths(circuit, function)?,
            })
        })
        .collect()
}

/// Map closure IDs to the input paths used by one unused-field iteration.
/// The caller supplies a distinct closure for each root in the sparse circuit.
pub fn map_field_dependencies(
    circuit: &Circuit,
) -> Result<BTreeMap<u64, BTreeSet<Vec<usize>>>, Error> {
    circuit
        .root()
        .edges("allOperators")?
        .into_iter()
        .map(|id| {
            let function = circuit
                .node(id)
                .expect("validated map reader")
                .edge("function")?;
            let NodeId::Inner(number) = function else {
                return Err(Error::new("expected an inner map closure"));
            };
            Ok((
                number,
                feldera_sql_expr::input_field_paths(circuit, function)?,
            ))
        })
        .collect()
}
