//! Combine compacted bounds from the two indexed inputs of a join.

use super::{Projection, apply_waterlines};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, TypeId};

struct InputBound<'a> {
    key: Option<(ExpressionId, &'a Projection)>,
    value: Option<ExpressionId>,
}

/// Read a compacted (key, value) bound. A missing key shifts the value to slot zero.
fn input_bound<'a>(
    edit: &mut CircuitEditor,
    parameter: ExpressionId,
    shape: &'a Projection,
) -> Result<InputBound<'a>, Error> {
    let Projection::Tuple { fields, .. } = shape else {
        return Err(Error::new("expected an indexed join waterline"));
    };
    let [key, value] = fields.as_slice() else {
        return Err(Error::new(
            "expected a key and a value in the join waterline",
        ));
    };
    let mut field = |index| {
        let input = edit.dereference(parameter)?;
        edit.tuple_field(input, index)
    };
    let key = key
        .monotone()
        .then(|| field(0).map(|read| (read, key)))
        .transpose()?;
    let value = value
        .monotone()
        .then(|| field(usize::from(key.is_some())))
        .transpose()?;
    Ok(InputBound { key, value })
}

/// Build the apply closure over the available input bounds. The join's transfer
/// function expects (key, left value, right value), with empty tuples for absent
/// components. If both sides bound the key, merge their fields pointwise.
pub fn join_waterline(
    edit: &mut CircuitEditor,
    sides: [Option<(TypeId, &Projection)>; 2],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let mut names = *next;
    let mut types = Vec::new();
    let mut parameters = Vec::new();
    let [left, right] = sides.map(|side| -> Result<_, Error> {
        let Some((ty, shape)) = side else {
            return Ok(InputBound {
                key: None,
                value: None,
            });
        };
        let param = super::filter::arg(edit, ty, &mut names)?;
        types.push(ty);
        parameters.push(param);
        input_bound(edit, param, shape)
    });
    let (left, right) = (left?, right?);
    let &site = types
        .first()
        .ok_or_else(|| Error::new("join needs at least one waterline"))?;
    let key = match (left.key, right.key) {
        (Some((l, shape)), Some((r, other))) => shape.merge(edit, l, other, r)?,
        (Some((key, _)), None) | (None, Some((key, _))) => edit.clone_if_needed(key)?,
        (None, None) => edit.tuple_expr(site.into(), false, &[])?,
    };
    let mut value = |field| match field {
        Some(read) => edit.clone_if_needed(read),
        None => edit.tuple_expr(site.into(), false, &[]),
    };
    let fields = [key, value(left.value)?, value(right.value)?];
    let body = edit.tuple_expr(key.into(), true, &fields)?;
    let closure = edit.closure_from_variables(body, &parameters)?;
    let result = apply_waterlines(edit, closure.into(), &types, &mut names, warnings)?;
    *next = names;
    Ok(result)
}
