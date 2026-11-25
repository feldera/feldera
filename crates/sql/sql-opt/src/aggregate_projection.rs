//! Drop unused aggregate components while preserving the following map's input layout.
use crate::{
    FieldRewriteError, compressed_field_type, inspect_references, project_used_fields,
    projected_reader::rebind_parameters,
    row_fields::RowFields,
    row_projection::{RowProjection, tuple_of_fields},
    terms::Terms,
};
use feldera_sql_expr::{SimplifyWarning, reduce_closure_body_with_warnings};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, TypeId, TypeKind,
};
use std::collections::BTreeSet;

/// Lift a projection of the input value to the indexed stream consumed by
/// aggregates and TopK. Nested key fields must survive the narrowing.
pub fn project_indexed_value(
    edit: &mut CircuitEditor,
    input: TypeId,
    projection: ExpressionId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let pair = edit.variable(input, next)?;
    let key = edit.tuple_field(pair, 0)?;
    let key = edit.dereference(key)?;
    let key = edit.expand_tuple(pair.into(), key)?;
    let value = edit.tuple_field(pair, 1)?;
    let value = edit.call_closure(projection, &[value])?;
    let body = edit.tuple_expr(pair.into(), true, &[key, value])?;
    let body = reduce_closure_body_with_warnings(edit, body.into(), &[], &[], Some(warnings))?;
    edit.closure_from_variables(edit.expression(body)?, &[pair])
}

/// Narrow the value read by a linear aggregate, retaining the key of its indexed input.
/// The reader must change before the optimizer allocates a projection for it.
pub fn trim_linear_aggregate_input<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    reader: NodeId,
    input: TypeId,
    paths: &BTreeSet<Vec<usize>>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<[ExpressionId; 2]>, FieldRewriteError<'a>> {
    let [parameter] = edit.node(reader)?.edges("parameters")?[..] else {
        return Err(Error::new("a linear aggregate requires one input parameter").into());
    };
    let declared = edit.type_id(edit.node(parameter)?.edge("type")?)?;
    let mut fields = RowFields::new(original);
    if !fields.unused(declared.into(), &[], paths, 1)? {
        return Ok(None);
    }
    if let Some(issue) = inspect_references(original, reader, false)?.diagnostic {
        return Err(FieldRewriteError::Binding(issue));
    }

    let mut row = declared;
    while edit.type_kind(row)? == TypeKind::Ref {
        row = edit.type_id(edit.node(row.into())?.edge("type")?)?;
    }
    let used = if matches!(edit.type_kind(row)?, TypeKind::Tuple | TypeKind::RawTuple) {
        fields.column_mask(&edit.node(row.into())?.edges("tupFields")?, &[], paths)?
    } else {
        vec![!paths.is_empty()]
    };
    let narrowed = compressed_field_type(edit, declared, 1, &used)?;
    let replacement =
        edit.derive_inner(parameter, [("type", Field::Reference(narrowed.into()))])?;
    let rebound = rebind_parameters(edit, reader, [(0, replacement, used.as_slice())])?;
    let mut terms = Terms::default();
    if terms.field(edit.circuit(), &Field::Reference(reader), &[])
        == terms.field(edit.circuit(), &Field::Reference(rebound.into()), &[])
    {
        return Ok(None);
    }

    // Recording a use borrows the original value again. A constant reader
    // retains the original reference type, which is shared in the output graph.
    let projection_type = if !paths.is_empty() && edit.type_kind(declared)? == TypeKind::Ref {
        edit.reference_type(edit.type_id(edit.node(declared.into())?.edge("type")?)?)?
    } else {
        declared
    };
    let value = project_used_fields(edit, projection_type, used, next)?;
    let projection = project_indexed_value(edit, input, value, next, warnings)?;
    Ok(Some([projection, rebound]))
}

pub struct AggregateProjection {
    pub retained: Vec<usize>,
    pub empty_row: ExpressionId,
    pub filler: ExpressionId,
}

/// Each aggregate contributes one column. Reuse its empty-set expression to
/// fill discarded positions, which the unchanged reader can never observe.
pub fn trim_aggregate_projection(
    edit: &mut CircuitEditor,
    reader: NodeId,
    empty_row: NodeId,
    key: NodeId,
    next: &mut u64,
) -> Result<Option<AggregateProjection>, Error> {
    let projection = RowProjection::preserving_key(edit, reader)?;
    let used = projection.used_value_fields()?;
    let empty_fields = edit.node(empty_row)?.edges("fields")?;
    if empty_fields.len() != used.len() {
        return Err(Error::new(
            "aggregate component count differs from the reader's value arity",
        ));
    }
    if !used.contains(&false) {
        return Ok(None);
    }

    let retained: Vec<_> = (0..used.len()).filter(|&index| used[index]).collect();
    let types = retained
        .iter()
        .map(|&i| {
            Ok(edit
                .expression_type(edit.expression(empty_fields[i])?)?
                .into())
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let value_type = edit.tuple_type(empty_row, false, types)?;
    let kept_defaults = retained
        .iter()
        .map(|&i| edit.expression(empty_fields[i]))
        .collect::<Result<Vec<_>, _>>()?;
    let narrow_empty = edit.tuple_with_type_at(empty_row, value_type, &kept_defaults)?;

    let key_ref = edit.reference_type(edit.type_id(key)?)?;
    let value_ref = edit.reference_type(value_type)?;
    let pair_type = edit.tuple_type(reader, true, [key_ref.into(), value_ref.into()])?;
    let pair = edit.variable(pair_type, next)?;
    let mut restored = Vec::with_capacity(used.len());
    let mut offset = 0;
    for (keep, default) in used.iter().zip(empty_fields) {
        restored.push(if *keep {
            let value = edit.tuple_field(pair, 1)?;
            let row = edit.dereference(value)?;
            let field = edit.tuple_field(row, offset)?;
            offset += 1;
            field
        } else {
            edit.expression(default)?
        });
    }
    let borrowed_key = edit.tuple_field(pair, 0)?;
    let key = edit.dereference(borrowed_key)?;
    let key = tuple_of_fields(edit, key)?;
    let restored = edit.tuple_expr(reader, false, &restored)?;
    let body = edit.tuple_expr(reader, true, &[key, restored])?;
    Ok(Some(AggregateProjection {
        retained,
        empty_row: narrow_empty,
        filler: edit.closure_from_variables(body, &[pair])?,
    }))
}
