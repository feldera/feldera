//! Append the clock to a row for NOW expressions that require a stream join.
use feldera_sql_expr::flatten_tuple_fields;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, TypeId};

fn parameter(edit: &mut CircuitEditor, ty: TypeId, next: &mut u64) -> Result<ExpressionId, Error> {
    let reference = edit.reference_type(ty)?;
    edit.variable(reference, next)
}

pub fn clock_join_functions(
    edit: &mut CircuitEditor,
    row: TypeId,
    empty_key: TypeId,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let input = parameter(edit, row, next)?;
    let value = edit.dereference(input)?;
    let fields = flatten_tuple_fields(edit, value)?;
    let key = edit.tuple_expr(input.into(), false, &[])?;
    let value = edit.tuple_expr(input.into(), false, &fields)?;
    let indexed = edit.tuple_expr(input.into(), true, &[key, value])?;
    let index = edit.closure_from_variables(indexed, &[input])?;

    let key = parameter(edit, empty_key, next)?;
    let left = parameter(edit, row, next)?;
    let clock_row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let right = parameter(edit, clock_row, next)?;
    let value = edit.dereference(left)?;
    let mut fields = flatten_tuple_fields(edit, value)?;
    let clock = edit.dereference(right)?;
    fields.push(edit.tuple_field(clock, 0)?);
    let joined = edit.tuple_expr(left.into(), false, &fields)?;
    Ok([
        index,
        edit.closure_from_variables(joined, &[key, left, right])?,
    ])
}

pub fn drop_clock(
    edit: &mut CircuitEditor,
    joined_row: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let row = parameter(edit, joined_row, next)?;
    let value = edit.dereference(row)?;
    let mut fields = flatten_tuple_fields(edit, value)?;
    fields
        .pop()
        .ok_or_else(|| Error::new("missing clock column on a temporal join"))?;
    let restored = edit.tuple_expr(row.into(), false, &fields)?;
    edit.closure_from_variables(restored, &[row])
}
