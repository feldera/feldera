//! Transfer lower bounds through the unsigned sort keys used by rolling windows.
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind, Field};

pub(super) fn encode_bound(
    edit: &mut CircuitEditor,
    operation: ExpressionId,
    bound: ExpressionId,
) -> Result<Option<ExpressionId>, Error> {
    let wrap = edit.node(operation.into())?;
    let flag = |name| {
        wrap.data(name)
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new(format!("sort key requires a boolean {name}")))
    };
    let source = edit.expression(wrap.edge("source")?)?;
    let nullable = edit.nullable(edit.expression_type(source)?)?;
    // A descending key reverses the bound. A NULL may arrive below any
    // finite bound when the nullable input sorts NULLs first.
    if !flag("ascending")? || (nullable && !flag("nullsLast")?) {
        return Ok(None);
    }
    let bound = if nullable {
        edit.some_at(bound.into(), bound)?
    } else {
        bound
    };
    // The data and bound use the same encoding width. The new unsigned type
    // inherits the bound's source position and is fresh on each encoding.
    let original = edit.expression_type(operation)?;
    let width = edit
        .node(original.into())?
        .data("width")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::new("missing unsigned sort-key width"))?;
    let ty = edit.integer_type(edit.expression_type(bound)?, width, false, false)?;
    let encoded = edit.derive_inner(
        operation.into(),
        [
            ("source", Field::Reference(bound.into())),
            ("type", Field::Reference(ty.into())),
        ],
    )?;
    edit.expression(encoded).map(Some)
}

pub(super) fn decode_bound(
    edit: &mut CircuitEditor,
    operation: ExpressionId,
    bound: ExpressionId,
) -> Result<ExpressionId, Error> {
    let decoded = edit.derive_inner(
        operation.into(),
        [("source", Field::Reference(bound.into()))],
    )?;
    let ty = edit.expression_type(operation)?;
    if !edit.nullable(ty)? {
        return edit.expression(decoded);
    }
    // The bound was encoded as Some(w), so decoding cannot produce NULL.
    let required = edit.with_nullability(ty, false)?;
    edit.make_expr(
        operation.into(),
        ExpressionKind::Unwrap,
        required,
        [
            ("expression", Field::Reference(decoded)),
            ("message", Field::Data("".into())),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn rolling_keys_preserve_bounds_for_ascending_values_and_trailing_nulls() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let (circuit, types) = circuit.with_inner_roots([json!({
            "class": "DBSPTypeInteger", "id": 1, "width": 32, "signed": false,
            "mayBeNull": false, "code": "UINT32"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let integer = edit.type_id(types[0])?;
        let bound = edit.named_variable(integer, "bound")?;
        for nullable in [false, true] {
            let data_type = edit.with_nullability(integer, nullable)?;
            let data = edit.named_variable(data_type, "data")?;
            let width = if nullable { 64 } else { 32 };
            let encoded_type = edit.integer_type(data_type, width, false, false)?;
            for (ascending, nulls_last) in
                [(false, false), (false, true), (true, false), (true, true)]
            {
                let wrap = edit.make_expr(
                    data.into(),
                    ExpressionKind::UnsignedWrap,
                    encoded_type,
                    [
                        ("source", Field::Reference(data.into())),
                        ("ascending", Field::Data(ascending.into())),
                        ("nullsLast", Field::Data(nulls_last.into())),
                    ],
                )?;
                let encoded = encode_bound(&mut edit, wrap, bound)?;
                assert_eq!(encoded.is_some(), ascending && (!nullable || nulls_last));
                let Some(encoded) = encoded else { continue };
                let key_type = edit.expression_type(encoded)?;
                assert_ne!(key_type, encoded_type);
                assert!(edit.same_type(key_type, encoded_type)?);
                let value = edit.node(encoded.into())?.edge("source")?;
                if nullable {
                    assert_eq!(edit.node(value)?.class(), "DBSPSomeExpression");
                    assert_eq!(edit.node(value)?.edge("expression")?, bound.into());
                } else {
                    assert_eq!(value, bound.into());
                }
                let unwrap = edit.make_expr(
                    data.into(),
                    ExpressionKind::UnsignedUnwrap,
                    data_type,
                    [
                        ("source", Field::Reference(wrap.into())),
                        ("ascending", Field::Data(ascending.into())),
                        ("nullsLast", Field::Data(nulls_last.into())),
                    ],
                )?;
                let decoded = decode_bound(&mut edit, unwrap, encoded)?;
                assert!(edit.same_type(edit.expression_type(decoded)?, integer)?);
                let decoded = edit.node(decoded.into())?;
                let inner = if nullable {
                    assert_eq!(decoded.data("message").and_then(|v| v.as_str()), Some(""));
                    edit.node(decoded.edge("expression")?)?
                } else {
                    decoded
                };
                assert_eq!(inner.class(), "DBSPUnsignedUnwrapExpression");
                assert_eq!(inner.edge("source")?, encoded.into());
            }
        }
        Ok(())
    }
}
