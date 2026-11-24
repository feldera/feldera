//! Identify the source rows that a single consumer can trim and the fields it reads.
use crate::row_fields::RowFields;
use crate::stream_rewrites::{body_members, reader_counts};
use crate::{LexicalResolution, lexical_references};
use feldera_sql_expr::{ensure_expression_tree, live_parameter_field_paths};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, InnerKind, NodeId, OperatorKind as Op, TypeKind,
};
use std::collections::{BTreeMap, BTreeSet};

pub type ParameterPaths = Vec<BTreeSet<Vec<usize>>>;

/// Nested closures contribute no field paths, but the compiler resolves their
/// variables before interpreting the outer reader. On a binding conflict the
/// live visitor first copies shared expressions and then resolves that tree.
pub fn live_field_dependencies(
    circuit: &Circuit,
    function: NodeId,
) -> Result<Option<ParameterPaths>, Error> {
    match lexical_references(circuit, function)? {
        LexicalResolution::Bound(_) => return live_parameter_field_paths(circuit, function),
        LexicalResolution::Free { .. } => return Ok(None),
        LexicalResolution::Conflict { .. } => {}
    }

    let mut edit = CircuitEditor::new(circuit.clone());
    let tree = ensure_expression_tree(&mut edit, function)?;
    // Deep-copying RETURN, for example, retains its argument. Two uses can
    // still share a variable with different declarations after the copy.
    original_field_dependencies(edit.circuit(), tree)
}

/// A fused rewriter edits the original reader. A path discovered in a copy
/// cannot establish that the original variables have unique declarations.
pub(super) fn original_field_dependencies(
    circuit: &Circuit,
    function: NodeId,
) -> Result<Option<ParameterPaths>, Error> {
    if !matches!(
        lexical_references(circuit, function)?,
        LexicalResolution::Bound(_)
    ) {
        return Ok(None);
    }
    live_parameter_field_paths(circuit, function)
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
fn unary_paths(paths: Option<ParameterPaths>) -> Result<Option<BTreeSet<Vec<usize>>>, Error> {
    let Some(paths) = paths else {
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
                paths: unary_paths(live_field_dependencies(circuit, function)?)?,
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
            let Some(paths) = unary_paths(original_field_dependencies(circuit, function)?)? else {
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
