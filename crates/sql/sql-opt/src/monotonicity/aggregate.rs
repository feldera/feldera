//! Transfer a group key or a rolling timestamp through an aggregate.
use super::{indexed_parameter, unknown};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, TypeId, TypeKind};

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
