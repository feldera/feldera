//! Narrow an indexed join input while retaining its key and the nullability of its value.
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
};

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
