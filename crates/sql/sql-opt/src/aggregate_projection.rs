//! Drop unused aggregate components while preserving the following map's input layout.
use crate::row_projection::{RowProjection, tuple_of_fields};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId};

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
