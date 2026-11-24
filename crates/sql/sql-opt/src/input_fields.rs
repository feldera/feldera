//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::row_fields::RowFields;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{BodyOrder, Circuit, Error, InnerKind, NodeId, OperatorKind as Op, TypeKind};
use std::collections::{BTreeMap, BTreeSet};

pub type ParameterPaths = Vec<BTreeSet<Vec<usize>>>;

/// Nested closures contribute no field paths, but the compiler resolves their
/// variables before interpreting the outer reader. On a binding conflict the
/// live visitor first copies shared expressions and then resolves that tree.
pub fn live_field_dependencies(
    circuit: &Circuit,
    function: NodeId,
) -> Result<Option<ParameterPaths>, Error> {
    if !matches!(
        crate::lexical_references(circuit, function)?,
        crate::LexicalResolution::Bound(_)
    ) {
        return Ok(None);
    }
    feldera_sql_expr::live_parameter_field_paths(circuit, function)
}

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
    pub paths: Option<BTreeSet<Vec<usize>>>,
}

/// A discarded statement can fail even when the result uses no input fields.
/// Keep an unanswered reader distinct from a reader that returns a constant.
fn checked_paths(
    circuit: &Circuit,
    function: NodeId,
) -> Result<Option<BTreeSet<Vec<usize>>>, Error> {
    let Some(paths) = live_field_dependencies(circuit, function)? else {
        return Ok(None);
    };
    let [paths] = paths
        .try_into()
        .map_err(|_| Error::new("expected a unary map"))?;
    Ok(Some(paths))
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
                paths: checked_paths(circuit, function)?,
            })
        })
        .collect()
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", content = "paths")]
pub enum MapInputLayout {
    Keep,
    Project(BTreeSet<Vec<usize>>),
    /// Resume at this reader's turn so earlier diagnostics and edits survive.
    Inspect,
}

pub fn map_field_dependencies(circuit: &Circuit) -> Result<BTreeMap<u64, MapInputLayout>, Error> {
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
            let Some(paths) = checked_paths(circuit, function)? else {
                return Ok((number, MapInputLayout::Inspect));
            };
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
            let unused = fields.unused(ty, &[], &paths, depth)?;
            Ok((
                number,
                if unused {
                    MapInputLayout::Project(paths)
                } else {
                    MapInputLayout::Keep
                },
            ))
        })
        .collect()
}
