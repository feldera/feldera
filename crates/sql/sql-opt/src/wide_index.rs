//! Project the common key and every narrow value onto one shared row variable.
use feldera_sql_expr::{reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
    TypeKind as Ty,
};

pub struct WideIndexFields {
    pub variable: ExpressionId,
    pub key: ExpressionId,
    pub rows: Vec<Vec<ExpressionId>>,
    pub nullable: bool,
}

fn parameter_type(edit: &CircuitEditor, function: NodeId) -> Result<TypeId, Error> {
    let [parameter] = edit
        .node(function)?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("a shared index requires one row parameter"))?;
    edit.type_id(edit.node(parameter)?.edge("type")?)
}

fn reduce(
    edit: &mut CircuitEditor,
    value: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let reduced = reduce_closure_body_with_warnings(edit, value.into(), &[], &[], Some(warnings))?;
    edit.expression(reduced)
}

pub fn wide_index_fields(
    edit: &mut CircuitEditor,
    functions: &[NodeId],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<WideIndexFields, Error> {
    let &[first, _, ..] = functions else {
        return Err(Error::new("a wide index needs at least two input indexes"));
    };
    let input = parameter_type(edit, first)?;
    let variable = edit.variable(input, next)?;
    let call = edit.call_closure(edit.expression(first)?, &[variable])?;
    let value_type = edit.tuple_field_type(edit.expression_type(call)?, 1)?;
    let nullable = edit.nullable(value_type)?;
    let key = edit.tuple_field(call, 0)?;
    let key = reduce(edit, key, warnings)?;
    let mut rows = Vec::with_capacity(functions.len());
    for &function in functions {
        if !edit.same_type(input, parameter_type(edit, function)?)? {
            return Err(Error::new(
                "shared indexes require equivalent row parameter types",
            ));
        }
        let function = edit.expression(function)?;
        let call = edit.call_closure(function, &[variable])?;
        let row = edit.tuple_field(call, 1)?;
        let row_type = edit.expression_type(row)?;
        if edit.type_kind(row_type)? != Ty::Tuple || edit.nullable(row_type)? != nullable {
            return Err(Error::new(
                "shared index values require tuples with equal nullability",
            ));
        }
        let types = edit.node(row_type.into())?.edges("tupFields")?;
        let mut fields = Vec::with_capacity(types.len());
        for (index, ty) in types.into_iter().enumerate() {
            let mut field = edit.tuple_field(row, index)?;
            let ty = edit.type_id(ty)?;
            let projected = edit.expression_type(field)?;
            // A nullable containing tuple lifts even a required field. Remove
            // that outer nullability before rebuilding the shared tuple.
            if !edit.nullable(ty)? && edit.nullable(projected)? {
                let target = edit.with_nullability(projected, false)?;
                field = edit.make_expr(
                    field.into(),
                    Expr::Unwrap,
                    target,
                    [
                        ("expression", Field::Reference(field.into())),
                        ("message", Field::Data("".into())),
                    ],
                )?;
            }
            fields.push(reduce(edit, field, warnings)?);
        }
        rows.push(fields);
    }
    Ok(WideIndexFields {
        variable,
        key,
        rows,
        nullable,
    })
}
