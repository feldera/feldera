//! Transfer a group key or a rolling timestamp through an aggregate.
use super::{indexed_parameter, unknown};
use feldera_sql_expr::{
    SimplifyWarning, apply_closure, copy_expression, reduce_closure_body_with_warnings,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId, TypeId, TypeKind};

/// Tuple fields are analyzed individually. Preserve the tuple constructor,
/// including its type and nullability, and mark each leaf as unknown.
fn unknown_value(
    edit: &mut CircuitEditor,
    ty: TypeId,
    depth: usize,
) -> Result<ExpressionId, Error> {
    if depth > 128 {
        return Err(Error::new(
            "aggregate waterline exceeds the circuit nesting limit",
        ));
    }
    match edit.type_kind(ty)? {
        TypeKind::Tuple | TypeKind::RawTuple => {
            let members = edit.node(ty.into())?.edges("tupFields")?;
            let mut fields = Vec::with_capacity(members.len());
            for member in members {
                fields.push(unknown_value(edit, edit.type_id(member)?, depth + 1)?);
            }
            edit.tuple_with_type(ty, &fields)
        }
        TypeKind::Any
        | TypeKind::Function
        | TypeKind::Ref
        | TypeKind::Stream
        | TypeKind::Struct => Err(Error::new("expected a SQL value in aggregate waterline")),
        _ => unknown(edit, ty.into()),
    }
}

pub enum AggregateTransfer {
    Group,
    /// A frame ending at or before the current row preserves the input timestamp.
    Rolling,
}

pub(super) fn first_leaf(
    edit: &mut CircuitEditor,
    mut value: ExpressionId,
) -> Result<ExpressionId, Error> {
    while matches!(
        edit.type_kind(edit.expression_type(value)?)?,
        TypeKind::Tuple | TypeKind::RawTuple
    ) {
        value = edit.tuple_field(value, 0)?;
    }
    Ok(value)
}

/// Place the bound in the first leaf of the accumulator. ARG_MIN can also
/// carry a payload alongside that leaf. Its fields have no known lower bound.
fn with_first_leaf(
    edit: &mut CircuitEditor,
    origin: NodeId,
    value: ExpressionId,
    ty: TypeId,
    depth: usize,
) -> Result<ExpressionId, Error> {
    if depth > 128 {
        return Err(Error::new(
            "minimum waterline exceeds the circuit nesting limit",
        ));
    }
    if !matches!(edit.type_kind(ty)?, TypeKind::Tuple | TypeKind::RawTuple) {
        return edit.cast(origin, value, ty);
    }
    let members = edit.node(ty.into())?.edges("tupFields")?;
    let (first, payload) = members
        .split_first()
        .ok_or_else(|| Error::new("minimum waterline requires a compared field"))?;
    let mut fields = Vec::with_capacity(members.len());
    fields.push(with_first_leaf(
        edit,
        origin,
        value,
        edit.type_id(*first)?,
        depth + 1,
    )?);
    for &field in payload {
        fields.push(unknown_value(edit, edit.type_id(field)?, depth + 1)?);
    }
    edit.tuple_with_type(ty, &fields)
}

pub fn minimum_transfer(
    edit: &mut CircuitEditor,
    origin: NodeId,
    pair: TypeId,
    postprocess: Option<NodeId>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let [key, value] = edit
        .node(pair.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("minimum waterline input requires a key and value"))?;
    let value_type = edit.type_id(value)?;
    let row = indexed_parameter(edit, edit.type_id(key)?, value_type, next)?;
    let second_read = edit.derive_inner(row.into(), [])?;
    let value = edit.tuple_field(edit.expression(second_read)?, 1)?;
    let value = edit.dereference(value)?;
    let first = first_leaf(edit, value)?;
    let owned = edit.clone_if_needed(first)?;
    let minimum = with_first_leaf(edit, origin, owned, value_type, 0)?;
    let output = if let Some(function) = postprocess {
        let borrowed = edit.borrow_value(minimum)?;
        apply_closure(edit, function, &[borrowed], warnings)?
    } else {
        minimum
    };
    let key = edit.tuple_field(row, 0)?;
    let key = edit.dereference(key)?;
    let body = edit.tuple_expr(origin, true, &[key, output])?;
    edit.closure_from_variables(body, &[row])
}

/// A chain of minimum updates retains the group key and the compared leaf of
/// every accumulator. The caller has verified the initializer and update agree.
pub fn chain_minimum_transfer(
    edit: &mut CircuitEditor,
    origin: NodeId,
    pair: TypeId,
    accumulators: TypeId,
    update: NodeId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let [key, value] = edit
        .node(pair.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("chain minimum input requires a key and value"))?;
    let row = indexed_parameter(edit, edit.type_id(key)?, edit.type_id(value)?, next)?;
    let update = edit.node(update)?;
    let [_, parameter, _] = update
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("chain update requires accumulator, row and weight parameters"))?;
    let updates = edit.node(update.edge("body")?)?.edges("fields")?;
    let fields = edit.node(accumulators.into())?.edges("tupFields")?;
    let row_field = edit.tuple_field(row, 1)?;
    if fields.len() != updates.len()
        || !edit.same_type(
            edit.type_id(edit.node(parameter)?.edge("type")?)?,
            edit.expression_type(row_field)?,
        )?
    {
        return Ok(None);
    }
    let mut bounds = Vec::with_capacity(fields.len());
    for (field, update) in fields.into_iter().zip(updates) {
        let operand = edit.expression(edit.node(update)?.edge("right")?)?;
        let compared = first_leaf(edit, operand)?;
        let reduced = reduce_closure_body_with_warnings(
            edit,
            compared.into(),
            &[parameter],
            &[row_field.into()],
            Some(warnings),
        )?;
        let copy = copy_expression(edit, reduced)?;
        let field_type = edit.type_id(field)?;
        let bound = with_first_leaf(edit, origin, edit.expression(copy)?, field_type, 0)?;
        if !edit.same_type(edit.expression_type(bound)?, field_type)? {
            return Ok(None);
        }
        bounds.push(bound);
    }
    let key = edit.tuple_field(row, 0)?;
    let key = edit.dereference(key)?;
    let values = edit.tuple_expr(origin, false, &bounds)?;
    let body = edit.tuple_expr(origin, true, &[key, values])?;
    edit.closure_from_variables(body, &[row]).map(Some)
}

pub fn aggregate_transfer(
    edit: &mut CircuitEditor,
    kind: AggregateTransfer,
    input: TypeId,
    output: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let [key, value] = edit
        .node(input.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("aggregate waterline input requires a key and value"))?;
    let output_type = edit.node(output.into())?;
    let output_key = edit.type_id(output_type.edge("keyType")?)?;
    let output_value = edit.type_id(output_type.edge("elementType")?)?;
    let key = edit.type_id(key)?;
    let row = indexed_parameter(edit, key, edit.type_id(value)?, next)?;
    let input_key = edit.tuple_field(row, 0)?;
    let input_key = edit.dereference(input_key)?;
    let (key_field, value_field) = match kind {
        AggregateTransfer::Group => {
            if !edit.same_type(key, output_key)? {
                return Err(Error::new("aggregate changed the group key type"));
            }
            (input_key, unknown_value(edit, output_value, 0)?)
        }
        AggregateTransfer::Rolling => {
            let [timestamp, aggregate] = edit
                .node(output_value.into())?
                .edges("tupFields")?
                .try_into()
                .map_err(|_| {
                    Error::new("rolling aggregate output requires a timestamp and value")
                })?;
            if !edit.same_type(key, edit.type_id(timestamp)?)? {
                return Err(Error::new("rolling aggregate changed the timestamp type"));
            }
            let partition = unknown_value(edit, output_key, 0)?;
            let aggregate = unknown_value(edit, edit.type_id(aggregate)?, 0)?;
            let aggregate = edit.some_at(aggregate.into(), aggregate)?;
            (
                partition,
                edit.tuple_expr(output.into(), false, &[input_key, aggregate])?,
            )
        }
    };
    let pair = edit.tuple_expr(output.into(), true, &[key_field, value_field])?;
    edit.closure_from_variables(pair, &[row])
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, ExpressionKind};
    use serde_json::json;

    #[test]
    fn minimum_of_nullable_tuple_bounds_only_the_nested_compared_field() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"mayBeNull":false,"code":"INT32","width":32,"signed":true
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let integer = roots[0];
        let tuple = edit.tuple_type(integer, false, [integer, integer])?;
        let nullable = edit.with_nullability(tuple, true)?;
        let data = edit.tuple_type(integer, false, [nullable.into(), tuple.into()])?;
        let pair = edit.tuple_type(integer, true, [integer, data.into()])?;
        let mut next = 12;
        let transfer = minimum_transfer(&mut edit, integer, pair, None, &mut next, &mut vec![])?;
        let body = edit.node(transfer.into())?.edge("body")?;
        let value = edit.node(body)?.edges("fields")?[1];
        let fields = edit.node(value)?.edges("fields")?;
        let compared = edit.node(fields[0])?.edges("fields")?;
        let cast = edit.node(compared[0])?;
        assert_eq!(
            edit.expr_kind(edit.expression(compared[0])?)?,
            ExpressionKind::Cast
        );
        assert_eq!(cast.edge("type")?, integer);
        assert_eq!(edit.node(fields[0])?.edge("type")?, nullable.into());
        for field in std::iter::once(compared[1]).chain(edit.node(fields[1])?.edges("fields")?) {
            assert_eq!(
                edit.expr_kind(edit.expression(field)?)?,
                ExpressionKind::NoExpression
            );
        }
        assert_eq!(next, 13);
        Ok(())
    }

    #[test]
    fn group_and_rolling_waterlines_place_the_key_in_different_output_fields() -> Result<(), Error>
    {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"mayBeNull":false,"code":"INT32","width":32,"signed":true
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let integer = edit.type_id(roots[0])?;
        let row = edit.tuple_type(roots[0], false, [roots[0]])?;
        let input = edit.tuple_type(roots[0], true, [integer.into(), row.into()])?;
        let nullable_row = edit.with_nullability(row, true)?;
        let pair = edit.tuple_type(roots[0], false, [integer.into(), nullable_row.into()])?;
        let mut next = 0;
        for (kind, key, expected) in [
            (AggregateTransfer::Group, integer, ExpressionKind::Deref),
            (AggregateTransfer::Rolling, row, ExpressionKind::Tuple),
        ] {
            let output = edit.indexed_zset_type(roots[0], key, pair)?;
            let function = aggregate_transfer(&mut edit, kind, input, output, &mut next)?;
            let body = edit.node(function.into())?.edge("body")?;
            let fields = edit.node(body)?.edges("fields")?;
            assert_eq!(edit.expr_kind(edit.expression(fields[0])?)?, expected);
            let value = edit.node(fields[1])?.edges("fields")?;
            let nested = edit.node(value[1])?;
            assert_eq!(nested.edge("type")?, nullable_row.into());
            assert_eq!(
                edit.expr_kind(edit.expression(nested.edges("fields")?[0])?)?,
                ExpressionKind::NoExpression
            );
            let bound = edit.expr_kind(edit.expression(value[0])?)?;
            assert_eq!(
                bound,
                if expected == ExpressionKind::Deref {
                    ExpressionKind::NoExpression
                } else {
                    ExpressionKind::Deref
                }
            );
        }
        assert_eq!(next, 2);
        Ok(())
    }
}
