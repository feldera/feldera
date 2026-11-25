//! Narrow an indexed join input while retaining its key and the nullability of its value.
use crate::{inspect_references, row_fields::RowFields, BindingDiagnostic};
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
    TypeKind,
};
use std::collections::BTreeSet;

/// The join key is never narrowed. Each changed value input gets its own
/// projection before the join; unchanged inputs keep their original ports.
pub enum JoinInputRewrite<'a> {
    InspectReader,
    InvalidInput {
        parameter: usize,
        paths: BTreeSet<Vec<usize>>,
    },
    Binding(BindingDiagnostic<'a>),
    Unchanged,
    Narrowed {
        inputs: Vec<Option<ExpressionId>>,
        reader: ExpressionId,
        timestamps: Option<[usize; 2]>,
    },
}

struct JoinValue {
    fields: Vec<bool>,
    has_unused_leaf: bool,
    projectable: bool,
}

/// Narrow all value inputs together, including the outer rows of star joins.
/// A partially-read nested value still occupies its full top-level column.
pub fn narrow_join_inputs<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    reader: NodeId,
    input_pairs: &[NodeId],
    mut timestamps: Option<[usize; 2]>,
    checked_paths: Option<Vec<BTreeSet<Vec<usize>>>>,
    next: &mut u64,
) -> Result<JoinInputRewrite<'a>, Error> {
    let site = original.root_id();
    if timestamps.is_some() {
        if input_pairs.len() != 2 {
            return Err(Error::new(
                "ASOF join requires two timestamp-bearing inputs",
            ));
        }
        let body = edit.expression(edit.node(reader)?.edge("body")?)?;
        if edit.expr_kind(body)? != Expr::Tuple || edit.node(body.into())?.field("fields").is_none()
        {
            return Ok(JoinInputRewrite::InspectReader);
        }
    }
    let checked = checked_paths.is_some();
    let mut paths = match checked_paths {
        Some(paths) => paths,
        None => match feldera_sql_expr::live_parameter_field_paths(original, reader)? {
            Some(paths) => paths,
            None => return Ok(JoinInputRewrite::InspectReader),
        },
    };
    let parameters = edit.node(reader)?.edges("parameters")?;
    if parameters.len() != input_pairs.len() + 1 || parameters.len() != paths.len() {
        return Err(Error::new(
            "join closure needs a key and one row parameter per input",
        ));
    }
    let mut shapes = RowFields::new(edit.circuit());
    let mut values = Vec::with_capacity(input_pairs.len());
    for (side, (&parameter, paths)) in parameters[1..].iter().zip(&mut paths[1..]).enumerate() {
        let ty = edit.type_id(edit.node(parameter)?.edge("type")?)?;
        let mut row = ty;
        while edit.type_kind(row)? == TypeKind::Ref {
            row = edit.type_id(edit.node(row.into())?.edge("type")?)?;
        }
        if !matches!(edit.type_kind(row)?, TypeKind::Tuple | TypeKind::RawTuple) {
            let unused = shapes.unused(ty.into(), &[], paths, 1)?;
            values.push(JoinValue {
                fields: vec![!unused],
                has_unused_leaf: unused,
                projectable: false,
            });
            continue;
        }
        let projectable = edit.type_kind(ty)? == TypeKind::Ref
            && edit.node(ty.into())?.edge("type")? == row.into();
        let columns = edit.node(row.into())?.edges("tupFields")?;
        if let Some(indices) = timestamps {
            let ts = indices[side];
            if ts >= columns.len() {
                // Constructing the live timestamp read reports its source location.
                return Ok(JoinInputRewrite::InspectReader);
            }
            // The match condition reads each timestamp even if the output does not.
            paths.insert(vec![ts]);
        }
        let fields = shapes.column_mask(&columns, &[], paths)?;
        if let Some(indices) = &mut timestamps {
            // Raw tuples retain field positions even when a field becomes empty.
            if edit.type_kind(row)? != TypeKind::RawTuple {
                indices[side] = fields[..indices[side]].iter().filter(|&&used| used).count();
            }
        }
        let has_unused_leaf = shapes.unused(row.into(), &[], paths, usize::MAX)?;
        values.push(JoinValue {
            fields,
            has_unused_leaf,
            projectable,
        });
    }
    if values.iter().all(|row| !row.fields.contains(&false)) {
        return Ok(JoinInputRewrite::Unchanged);
    }

    let mut inputs = Vec::with_capacity(values.len());
    let mut replacements = Vec::with_capacity(values.len());
    for (side, ((&pair, &param), row)) in input_pairs
        .iter()
        .zip(&parameters[1..])
        .zip(&values)
        .enumerate()
    {
        let reference = edit.type_id(edit.node(param)?.edge("type")?)?;
        let ty = crate::compressed_field_type(edit, reference, 1, &row.fields)?;
        replacements.push(edit.derive_inner(param, [("type", Field::Reference(ty.into()))])?);
        let projection = if row.has_unused_leaf {
            if !row.projectable {
                return Ok(JoinInputRewrite::InvalidInput {
                    parameter: side + 1,
                    paths: std::mem::take(&mut paths[side + 1]),
                });
            }
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
    // Field uses may belong to a copied tree. Rebinding the original reader
    // happens after allocating every input projection.
    if checked {
        if let Some(issue) = inspect_references(original, reader, false)?.diagnostic {
            return Ok(JoinInputRewrite::Binding(issue));
        }
    }
    let projected = replacements
        .into_iter()
        .zip(&values)
        .enumerate()
        .map(|(i, (decl, value))| (i + 1, decl, value.fields.as_slice()));
    let reader = crate::projected_reader::rebind_parameters(edit, reader, projected)?;
    Ok(JoinInputRewrite::Narrowed {
        inputs,
        reader,
        timestamps,
    })
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

        let pair = edit.tuple_type(site, true, [reference.into(), reference.into()])?;
        let mut next = 0;
        let original = edit.circuit().clone();
        let rewrite = narrow_join_inputs(
            &original,
            &mut edit,
            closure.into(),
            &[pair.into(), pair.into()],
            Some([2, 3]),
            None,
            &mut next,
        )?;
        let JoinInputRewrite::Narrowed {
            inputs,
            reader,
            timestamps,
        } = rewrite
        else {
            panic!("both ASOF inputs contain unused columns");
        };
        assert_eq!(timestamps, Some([1, 2]));
        assert!(inputs.iter().all(Option::is_some));
        let parameters = edit.node(reader.into())?.edges("parameters")?;
        assert_eq!(
            parameters[0],
            edit.node(closure.into())?.edges("parameters")?[0]
        );
        for (slot, width) in [(1, 2), (2, 3)] {
            let reference = edit.node(parameters[slot])?.edge("type")?;
            let projected_row = edit.node(reference)?.edge("type")?;
            assert_eq!(edit.node(projected_row)?.edges("tupFields")?.len(), width);
        }
        Ok(())
    }
}
