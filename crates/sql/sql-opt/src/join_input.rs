//! Narrow an indexed join input while retaining its key and the nullability of its value.
use crate::row_fields::RowFields;
use feldera_sql_expr::{copy_expression, live_parameter_field_paths};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId, TypeKind,
};

use std::collections::BTreeSet;

#[derive(serde::Serialize)]
pub struct AsofInputFields {
    paths: Vec<BTreeSet<Vec<usize>>>,
    timestamps: [usize; 2],
}

/// An ASOF join compares its timestamps even if neither appears in the result.
/// Return the parameter dependencies and both timestamp positions after projection.
pub fn asof_input_fields(
    edit: &CircuitEditor,
    closure: NodeId,
    timestamps: [usize; 2],
) -> Result<Option<AsofInputFields>, Error> {
    let function = edit.node(closure)?;
    let body = edit.expression(function.edge("body")?)?;
    if edit.expr_kind(body)? != Expr::Tuple || edit.node(body.into())?.field("fields").is_none() {
        return Ok(None);
    }
    let Some(mut paths) = live_parameter_field_paths(edit.circuit(), closure)? else {
        return Ok(None);
    };
    let [_, left, right] = function
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("ASOF output requires key, left and right parameters"))?;
    let mut projected = [0; 2];
    for (side, param) in [left, right].into_iter().enumerate() {
        let reference = edit.type_id(edit.node(param)?.edge("type")?)?;
        if edit.type_kind(reference)? != TypeKind::Ref {
            return Ok(None);
        }
        let row = edit.type_id(edit.node(reference.into())?.edge("type")?)?;
        let fields = edit.node(row.into())?.edges("tupFields")?;
        let ts = timestamps[side];
        if ts >= fields.len() {
            return Err(Error::new(
                "ASOF timestamp index exceeds the input row width",
            ));
        }
        let uses = &mut paths[side + 1];
        uses.insert(vec![ts]);
        if uses.contains(&Vec::new()) || edit.type_kind(row)? == TypeKind::RawTuple {
            projected[side] = ts;
            continue;
        }
        let mut shape = RowFields::new(edit.circuit());
        for (i, &ty) in fields[..ts].iter().enumerate() {
            if !shape.unused(ty, &mut vec![i], uses, 0, 0)? {
                projected[side] += 1;
            }
        }
    }
    Ok(Some(AsofInputFields {
        paths,
        timestamps: projected,
    }))
}

pub fn project_join_input(
    edit: &mut CircuitEditor,
    join: NodeId,
    input: TypeId,
    columns: &[usize],
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let pair = edit.variable(input, next)?;
    let value = edit.tuple_field(pair, 1)?;
    let value = edit.dereference(value)?;
    let value_type = edit.expression_type(value)?;
    let nullable = edit.nullable(value_type)?;
    let mut fields = Vec::with_capacity(columns.len());
    for &column in columns {
        let copy = copy_expression(edit, value.into())?;
        let mut row = edit.expression(copy)?;
        if nullable {
            let required = edit.with_nullability(value_type, false)?;
            row = edit.make_expr(
                value.into(),
                Expr::Unwrap,
                required,
                [
                    ("expression", Field::Reference(row.into())),
                    ("message", Field::Data("Field should not be NULL".into())),
                ],
            )?;
        }
        let field = edit.tuple_field(row, column)?;
        fields.push(edit.clone_if_needed(field)?);
    }
    let narrowed = if nullable {
        let types = fields
            .iter()
            .map(|&f| Ok(edit.expression_type(f)?.into()))
            .collect::<Result<Vec<_>, Error>>()?;
        let ty = edit.tuple_type(value.into(), false, types)?;
        let ty = edit.with_nullability(ty, true)?;
        let present = edit.tuple_with_type(ty, &fields)?;
        let missing = edit.typed_null(ty)?;
        let condition = edit.null_test(value)?;
        edit.make_expr(
            join,
            Expr::If,
            edit.expression_type(missing)?,
            [
                ("condition", Field::Reference(condition.into())),
                ("positive", Field::Reference(missing.into())),
                ("negative", Field::Reference(present.into())),
            ],
        )?
    } else {
        edit.tuple_expr(value.into(), false, &fields)?
    };

    let key = edit.tuple_field(pair, 0)?;
    let key = edit.dereference(key)?;
    let width = edit
        .node(edit.expression_type(key)?.into())?
        .edges("tupFields")?
        .len();
    let mut key_fields = Vec::with_capacity(width);
    for index in 0..width {
        let row = copy_expression(edit, key.into())?;
        let field = edit.tuple_field(edit.expression(row)?, index)?;
        key_fields.push(edit.clone_if_needed(field)?);
    }
    let key_tuple = edit.tuple_expr(key.into(), false, &key_fields)?;
    let result = edit.tuple_expr(join, true, &[key_tuple, narrowed])?;
    edit.closure_from_variables(result, &[pair])
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;

    #[test]
    fn asof_keeps_hidden_timestamps_and_empty_fields_before_them() -> Result<(), Error> {
        let circuit = Circuit::from_json(serde_json::json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let scalar = edit.bool_type(site, false)?;
        let empty = edit.tuple_type(site, false, [])?;
        let row = edit.tuple_type(
            site,
            false,
            [empty.into(), scalar.into(), scalar.into(), scalar.into()],
        )?;
        let reference = edit.reference_type(row)?;
        let key = edit.named_variable(reference, "key")?;
        let left = edit.named_variable(reference, "left")?;
        let right = edit.named_variable(reference, "right")?;
        let value = edit.dereference(right)?;
        let second = edit.tuple_field(value, 1)?;
        let result = edit.tuple_expr(site, false, &[second])?;
        let closure = edit.closure_from_variables(result, &[key, left, right])?;

        let fields = asof_input_fields(&edit, closure.into(), [2, 3])?.unwrap();
        assert_eq!(fields.timestamps, [1, 2]);
        assert_eq!(
            fields.paths,
            [
                BTreeSet::new(),
                BTreeSet::from([vec![2]]),
                BTreeSet::from([vec![1], vec![3]])
            ]
        );
        Ok(())
    }
}
