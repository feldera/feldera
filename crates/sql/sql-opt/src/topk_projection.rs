//! Narrow a TopK output producer and rebind the indexed map that reads it.
use crate::row_projection::RowProjection;
use feldera_sql_expr::{copy_expression, ensure_expression_tree, input_field_paths};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind, NodeId};

pub fn trim_topk_projection(
    edit: &mut CircuitEditor,
    producer: NodeId,
    reader: NodeId,
) -> Result<Option<[ExpressionId; 2]>, Error> {
    let producer = ensure_expression_tree(edit, producer)?;
    let body = edit.expression(edit.node(producer)?.edge("body")?)?;
    if edit.expr_kind(body)? != ExpressionKind::Tuple
        || edit.nullable(edit.expression_type(body)?)?
        || edit.node(body.into())?.field("fields").is_none()
    {
        return Ok(None);
    }
    let mut paths = input_field_paths(edit.circuit(), reader)?;
    paths.insert(vec![0]); // TopK always keeps the whole key.
    let projection = RowProjection::new(edit, reader, &paths)?;
    let used = projection.used_value_fields()?;
    if !used.contains(&false) {
        return Ok(None);
    }
    let fields = edit.node(body.into())?.edges("fields")?;
    if fields.len() != used.len() {
        return Err(Error::new(
            "TopK output and reader value have different arities",
        ));
    }
    let mut kept = Vec::new();
    for (field, &used) in fields.into_iter().zip(used) {
        if used {
            let copy = copy_expression(edit, field)?;
            kept.push(edit.expression(copy)?);
        }
    }
    let narrow_row = edit.tuple_expr(producer, false, &kept)?;
    let parameters = edit.node(producer)?.edges("parameters")?;
    let narrow_producer = edit.closure_with_parameters(narrow_row, &parameters)?;
    let reader = projection.rebind(edit, reader)?;
    Ok(Some([narrow_producer, reader]))
}
