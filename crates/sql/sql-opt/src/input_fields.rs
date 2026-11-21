//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::BTreeSet;

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
