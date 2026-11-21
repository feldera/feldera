//! Expressions for the built-in clock and the table-backed clock used during replay.
use crate::limiters::timestamp_max;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind, Field, TypeId};

/// The clock relation contains one timestamp column.
fn read_timestamp(
    edit: &mut CircuitEditor,
    parameter: ExpressionId,
) -> Result<ExpressionId, Error> {
    let row = edit.dereference(parameter)?;
    edit.tuple_field(row, 0)
}

fn clock_parameter(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let reference = edit.reference_type(row)?;
    edit.variable(reference, next)
}

pub fn clock_index(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let input = clock_parameter(edit, timestamp, next)?;
    let time = read_timestamp(edit, input)?;
    let key = edit.tuple_expr(input.into(), false, &[])?;
    let value = edit.tuple_expr(input.into(), false, &[time])?;
    let pair = edit.tuple_expr(input.into(), true, &[key, value])?;
    edit.closure_from_variables(pair, &[input])
}

/// Initialize and update the maximum timestamp from changes to the clock table.
pub fn clock_aggregate(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    weight: ExpressionId,
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let input = clock_parameter(edit, timestamp, next)?;
    let first = read_timestamp(edit, input)?;
    let initial = edit.tuple_expr(input.into(), false, &[first])?;
    let init = edit.closure_from_variables(initial, &[input, weight])?;

    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let accumulator = edit.variable(row, next)?;
    let current = edit.tuple_field(accumulator, 0)?;
    let incoming = read_timestamp(edit, input)?;
    let maximum = edit.make_expr(
        accumulator.into(),
        ExpressionKind::ConditionalIncrement,
        timestamp,
        [
            ("left", Field::Reference(current.into())),
            ("right", Field::Reference(incoming.into())),
            ("opcode", Field::Data("AGG_MAX".into())),
        ],
    )?;
    let body = edit.tuple_expr(input.into(), false, &[maximum])?;
    let update = edit.closure_from_variables(body, &[accumulator, input, weight])?;
    Ok([init, update])
}

/// The scalar clock starts at the timestamp minimum. Its waterline advances
/// to the maximum of the timestamps seen in successive input batches.
pub fn scalar_clock(
    edit: &mut CircuitEditor,
    row: TypeId,
    empty_key: TypeId,
    minimum: ExpressionId,
    next: &mut u64,
) -> Result<[ExpressionId; 3], Error> {
    let reference = edit.reference_type(row)?;
    let input = edit.variable(reference, next)?;
    let time = read_timestamp(edit, input)?;
    let initial = edit.tuple_expr(row.into(), false, &[minimum])?;
    let value = edit.tuple_expr(row.into(), false, &[time])?;
    let ty = edit.expression_type(initial)?;
    let maximum = timestamp_max(edit, initial.into(), ty, next)?;
    let initial = edit.closure_with_parameters(initial, &[])?;
    let key = edit.reference_type(empty_key)?;
    let key = edit.variable(key, next)?;
    let extract = edit.closure_from_variables(value, &[input, key])?;
    Ok([initial, extract, maximum])
}
