//! Split a map that reads a subset of its input into a projection and a narrower reader.
use crate::{
    inspect_references, row_fields::RowFields, row_projection::RowProjection, terms::Terms,
    FieldRewriteError,
};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort, TypeId,
    TypeKind,
};
use std::collections::BTreeSet;

pub enum MapOutput {
    Row,
    Indexed,
}

/// Insert a map that projects the input and infer its collection type from the closure.
pub fn append_input_projection(
    edit: &mut CircuitEditor,
    body: NodeId,
    origin: NodeId,
    input: OutputPort,
    kind: OperatorKind,
    projection: ExpressionId,
) -> Result<OutputPort, Error> {
    let narrowed = crate::replace_projections::projected_type(edit, projection.into(), kind)?;
    let operator = edit.append_operator(
        body,
        origin,
        kind,
        &[input],
        [
            ("outputType", Field::Reference(narrowed.into())),
            ("function", Field::Reference(projection.into())),
        ],
    )?;
    Ok(OutputPort {
        operator,
        number: 0,
    })
}

/// Insert the projection before the rebound reader. The reader keeps the
/// original output type object, even when its new closure has a fresh tuple.
/// Return whether the projection inherits the reader's live annotations.
pub fn append_split_map(
    edit: &mut CircuitEditor,
    body: NodeId,
    origin: NodeId,
    input: OutputPort,
    input_type: TypeId,
    output_type: TypeId,
    [projection, reader]: [ExpressionId; 2],
) -> Result<bool, Error> {
    let kind = |ty| match edit.type_kind(ty)? {
        TypeKind::ZSet => Ok(OperatorKind::Map),
        TypeKind::IndexedZSet => Ok(OperatorKind::MapIndex),
        _ => Err(Error::new("map split requires a collection type")),
    };
    let project_kind = kind(input_type)?;
    let read_kind = kind(output_type)?;
    let projected = append_input_projection(edit, body, origin, input, project_kind, projection)?;
    edit.append_operator(
        body,
        origin,
        read_kind,
        &[projected],
        [
            ("outputType", Field::Reference(output_type.into())),
            ("function", Field::Reference(reader.into())),
        ],
    )?;
    // An indexed-to-indexed map preserves the original annotations on its projection.
    Ok(project_kind == OperatorKind::MapIndex && read_kind == OperatorKind::MapIndex)
}

pub fn split_map_input(
    edit: &mut CircuitEditor,
    reader: NodeId,
    paths: &BTreeSet<Vec<usize>>,
    output: MapOutput,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 2]>, Error> {
    split_with(edit, reader, paths, output, next, RowProjection::rebind)
}

/// Field analysis can succeed on a copied tree even when the original reader
/// shares a variable between declarations. Rewriting still uses that original.
pub fn checked_split_map_input<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    reader: NodeId,
    paths: &BTreeSet<Vec<usize>>,
    output: MapOutput,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 2]>, FieldRewriteError<'a>> {
    if !RowFields::new(original).map_has_unused_fields(reader, paths)? {
        return Ok(None);
    }
    split_with(edit, reader, paths, output, next, |layout, edit, reader| {
        if let Some(issue) = inspect_references(original, reader, false)?.diagnostic {
            return Err(FieldRewriteError::Binding(issue));
        }
        Ok(layout.rebind(edit, reader)?)
    })
}

fn split_with<E: From<Error>>(
    edit: &mut CircuitEditor,
    reader: NodeId,
    paths: &BTreeSet<Vec<usize>>,
    output: MapOutput,
    next: &mut u64,
    rebind: impl FnOnce(&RowProjection, &mut CircuitEditor, NodeId) -> Result<ExpressionId, E>,
) -> Result<Option<[ExpressionId; 2]>, E> {
    let layout = RowProjection::new(edit, reader, paths)?;
    let indexed_input = layout.is_indexed();
    let indexed_output = matches!(output, MapOutput::Indexed);
    let mut terms = Terms::default();
    let original = terms.field(edit.circuit(), &Field::Reference(reader), &[]);
    let mut equivalent = |edit: &CircuitEditor, f: ExpressionId| {
        terms.field(edit.circuit(), &Field::Reference(f.into()), &[]) == original
    };
    // A change of stream kind rewrites the reader first. Otherwise projection
    // comes first. The order is observable even if rebinding raises a diagnostic.
    let (projection, rebound) = if indexed_input != indexed_output {
        let rebound = rebind(&layout, edit, reader)?;
        if indexed_input && equivalent(edit, rebound) {
            return Ok(None);
        }
        let projection = layout.project(edit, next, !paths.is_empty())?;
        if indexed_output && equivalent(edit, rebound) {
            return Ok(None);
        }
        (projection, rebound)
    } else {
        let projection = layout.project(edit, next, !paths.is_empty())?;
        if indexed_input && equivalent(edit, projection) {
            return Ok(None);
        }
        let rebound = rebind(&layout, edit, reader)?;
        if !indexed_input && (equivalent(edit, projection) || equivalent(edit, rebound)) {
            return Ok(None);
        }
        (projection, rebound)
    };
    Ok(Some([projection, rebound]))
}
