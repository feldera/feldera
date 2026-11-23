//! Derive lower bounds for named scalar calls from the bounds of their arguments.
use super::{unbounded, ProjectedValue, Shape};
use crate::monotonicity::functions::{
    build_function_waterline, double_literal_above_one, function_waterline, Argument,
};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind,
};

pub(super) fn call_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    name: Option<&str>,
    values: impl Iterator<Item = ProjectedValue>,
) -> Result<Option<ProjectedValue>, Error> {
    let site = expression.into();
    let call = edit.node(site)?;
    let callee = edit.expression(call.edge("function")?)?;
    let ty = edit.expression_type(expression)?;
    let arguments = call.edges("arguments")?;
    if name == Some("now") {
        if !arguments.is_empty() {
            return Ok(None);
        }
        // replaceArguments() retains the live callee and SQL type, using
        // the constructor with an empty source location.
        let clock = edit.empty_origin_node(
            site,
            InnerKind::Expression(Expr::Apply),
            [
                ("type", Field::Reference(ty.into())),
                ("function", Field::Reference(callee.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        return ProjectedValue::bounded(edit, expression, edit.expression(clock)?, false).map(Some);
    }

    let mut facts = Vec::with_capacity(arguments.len());
    let mut bounds = Vec::with_capacity(arguments.len());
    for (&argument, value) in arguments.iter().zip(values) {
        let arg = edit.expression(argument)?;
        facts.push(Argument {
            monotone: value.reduced.is_some(),
            constant: value.constant,
            double_literal_above_one: double_literal_above_one(edit, arg)?,
        });
        bounds.push(value.reduced);
    }
    let constant = facts.iter().all(|fact| fact.constant);
    let Some(target) = name.and_then(|name| function_waterline(name, &facts)) else {
        let Some(mut result) = unbounded(edit, expression)? else {
            return Ok(None);
        };
        result.constant = constant;
        return Ok(Some(result));
    };
    if !matches!(
        super::UnorderedRow::from_type(edit, ty)?.map(|row| row.shape),
        Some(Shape::Scalar(_))
    ) {
        return Ok(None);
    }
    let reduced = build_function_waterline(edit, expression, &target, &bounds)?;
    ProjectedValue::bounded(edit, expression, reduced, constant).map(Some)
}
