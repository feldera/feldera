//! Lower bounds for an ASOF join whose left timestamp follows its right timestamp.
use super::unknown;
use crate::projection::{join_projection, simple_tuple};
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{BinaryOp, CircuitEditor, Error, ExpressionId, NodeId, TypeId, TypeKind};

/// Only the left timestamp can carry a bound. A right update can change a
/// match to an old right row with the same key.
pub fn asof_waterline(
    edit: &mut CircuitEditor,
    site: NodeId,
    function: ExpressionId,
    key_type: TypeId,
    right_type: TypeId,
    timestamps: [usize; 2],
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let join = edit.node(function.into())?;
    let [_, left_parameter, _] = join.edges("parameters")?[..] else {
        return Err(Error::new(
            "ASOF join requires key, left and right parameters",
        ));
    };
    let left_param_type = edit.type_id(edit.node(left_parameter)?.edge("type")?)?;
    let output = edit.expression_type(edit.expression(join.edge("body")?)?)?;
    if !simple_tuple(edit, output.into())? {
        return Err(Error::new("ASOF join requires a flat output projection"));
    }
    let projection = join_projection(edit, function.into())?;
    let fields = edit.node(output.into())?.edges("tupFields")?;
    let width = if edit.type_kind(output)? == TypeKind::RawTuple {
        // Projection.IOMap counts the columns of both the key and value.
        let mut width = 0;
        for part in fields {
            width += edit.node(part)?.edges("tupFields")?.len();
        }
        width
    } else {
        fields.len()
    };
    if projection.len() != width {
        return Err(Error::new(
            "ASOF projection must select one input field per output",
        ));
    }
    let [left_index, right_index] = timestamps;
    let key_ref = edit.reference_type(key_type)?;
    let key = edit.variable(key_ref, next)?;
    let left = edit.variable(left_param_type, next)?;
    let right_ref = edit.reference_type(right_type)?;
    let right = edit.variable(right_ref, next)?;

    let left_row = edit.dereference(left)?;
    let right_row = edit.dereference(right)?;
    let left_type = edit.expression_type(left_row)?;
    let left_ts = edit.tuple_field(left_row, left_index)?;
    let right_ts = edit.tuple_field(right_row, right_index)?;
    let ty = edit.with_nullability(edit.expression_type(right_ts)?, false)?;
    let right_ts = edit.cast(right_ts.into(), right_ts, ty)?;
    let minimum = edit.binary_expression(
        site,
        edit.expression_type(left_ts)?,
        BinaryOp::Min,
        left_ts,
        right_ts,
    )?;

    let mut fields = Vec::with_capacity(projection.len());
    for (input, index) in projection {
        let bound = match input {
            1 if index == left_index => {
                let copy = copy_expression(edit, minimum.into())?;
                edit.expression(copy)?
            }
            0 | 1 => {
                let row = if input == 0 { key_type } else { left_type };
                let ty = edit.tuple_field_type(row, index)?;
                unknown(edit, ty.into())?
            }
            2 => {
                let ty = edit.tuple_field_type(right_type, index)?;
                let nullable = edit.with_nullability(ty, true)?;
                unknown(edit, nullable.into())?
            }
            _ => return Err(Error::new(format!("invalid ASOF projection input {input}"))),
        };
        fields.push(bound);
    }
    let row = edit.tuple_expr(key.into(), false, &fields)?;
    edit.closure_from_variables(row, &[key, left, right])
}
