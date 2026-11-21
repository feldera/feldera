//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::row_fields::RowFields;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{BodyOrder, Circuit, Error, InnerKind, NodeId, OperatorKind as Op, TypeKind};
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

/// Record the paths to retain for each closure with unused fields. A fully used
/// input has a null layout, distinct from a constant reader with no paths.
/// The caller supplies a distinct closure for each root in the sparse circuit.
pub type MapInputLayouts = BTreeMap<u64, Option<BTreeSet<Vec<usize>>>>;

pub fn map_field_dependencies(circuit: &Circuit) -> Result<MapInputLayouts, Error> {
    let mut fields = RowFields::new(circuit);
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
            let paths = feldera_sql_expr::input_field_paths(circuit, function)?;
            let [param] = circuit
                .node(function)
                .expect("validated closure")
                .edges("parameters")?
                .try_into()
                .map_err(|_| Error::new("expected a unary map"))?;
            let ty = circuit
                .node(param)
                .expect("validated parameter")
                .edge("type")?;
            let borrowed = matches!(
                circuit.node(ty).and_then(|n| n.inner_kind()),
                Some(InnerKind::Type(TypeKind::Ref))
            );
            let depth = if borrowed { 1 } else { 2 };
            let unused = fields.unused(ty, &mut Vec::new(), &paths, depth, 0)?;
            Ok((number, unused.then_some(paths)))
        })
        .collect()
}
