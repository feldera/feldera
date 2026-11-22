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

/// The join key is never narrowed. Each changed value input gets its own
/// projection before the join; unchanged inputs keep their original ports.
pub enum JoinInputRewrite {
    Unsupported,
    Unchanged,
    Narrowed {
        inputs: Vec<Option<ExpressionId>>,
        reader: ExpressionId,
    },
}

struct JoinValue {
    fields: Vec<bool>,
    has_unused_leaf: bool,
}

/// Narrow all value inputs together, including the outer rows of star joins.
/// A partially-read nested value still occupies its full top-level column.
pub fn narrow_join_inputs(
    edit: &mut CircuitEditor,
    site: NodeId,
    reader: NodeId,
    input_pairs: &[NodeId],
    next: &mut u64,
) -> Result<JoinInputRewrite, Error> {
    let Some(paths) = live_parameter_field_paths(edit.circuit(), reader)? else {
        return Ok(JoinInputRewrite::Unsupported);
    };
    let parameters = edit.node(reader)?.edges("parameters")?;
    if parameters.len() != input_pairs.len() + 1 || parameters.len() != paths.len() {
        return Err(Error::new(
            "join closure needs a key and one row parameter per input",
        ));
    }
    let mut shapes = RowFields::new(edit.circuit());
    let mut values = Vec::with_capacity(input_pairs.len());
    for (&parameter, paths) in parameters[1..].iter().zip(&paths[1..]) {
        let ty = edit.type_id(edit.node(parameter)?.edge("type")?)?;
        if edit.type_kind(ty)? != TypeKind::Ref {
            return Ok(JoinInputRewrite::Unsupported);
        }
        let row = edit.type_id(edit.node(ty.into())?.edge("type")?)?;
        if !matches!(edit.type_kind(row)?, TypeKind::Tuple | TypeKind::RawTuple) {
            return Ok(JoinInputRewrite::Unsupported);
        }
        let columns = edit.node(row.into())?.edges("tupFields")?;
        let mut fields = Vec::with_capacity(columns.len());
        for (index, column) in columns.into_iter().enumerate() {
            fields.push(
                paths.contains(&Vec::new())
                    || !shapes.unused(column, &mut vec![index], paths, 0, 0)?,
            );
        }
        let has_unused_leaf = shapes.unused(row.into(), &mut vec![], paths, usize::MAX, 0)?;
        values.push(JoinValue {
            fields,
            has_unused_leaf,
        });
    }
    if values.iter().all(|row| !row.fields.contains(&false)) {
        return Ok(JoinInputRewrite::Unchanged);
    }

    let mut inputs = Vec::with_capacity(values.len());
    let mut replacements = Vec::with_capacity(values.len());
    for ((&pair, &param), row) in input_pairs.iter().zip(&parameters[1..]).zip(&values) {
        let reference = edit.type_id(edit.node(param)?.edge("type")?)?;
        let ty = crate::compressed_field_type(edit, reference, 1, &row.fields)?;
        replacements.push(edit.derive_inner(param, [("type", Field::Reference(ty.into()))])?);
        let projection = if row.has_unused_leaf {
            let columns = row
                .fields
                .iter()
                .enumerate()
                .filter_map(|(i, &used)| used.then_some(i))
                .collect::<Vec<_>>();
            Some(project_join_input(
                edit,
                site,
                edit.type_id(pair)?,
                &columns,
                next,
            )?)
        } else {
            None
        };
        inputs.push(projection);
    }
    let projected = replacements
        .into_iter()
        .zip(&values)
        .enumerate()
        .map(|(i, (decl, value))| (i + 1, decl, value.fields.as_slice()));
    let reader = crate::projected_reader::rebind_parameters(edit, reader, projected)?;
    Ok(JoinInputRewrite::Narrowed { inputs, reader })
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
