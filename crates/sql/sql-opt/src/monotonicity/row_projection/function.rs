//! Derive lower bounds for named scalar calls from the bounds of their arguments.
use super::{unbounded, ProjectedValue, Shape};
use crate::monotonicity::functions::{
    build_function_waterline, clock_waterline, double_literal_above_one, function_rule, FunctionRule,
    Argument,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId};

pub(super) fn call_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    name: Option<&str>,
    values: impl Iterator<Item = Option<ProjectedValue>>,
) -> Result<Option<ProjectedValue>, Error> {
    if name == Some("now") {
        let clock = clock_waterline(edit, expression)?;
        return ProjectedValue::bounded(edit, expression, clock, false).map(Some);
    }

    let ty = edit.expression_type(expression)?;
    let arguments = edit.node(expression.into())?.edges("arguments")?;
    let mut facts = Vec::with_capacity(arguments.len());
    let mut bounds = Vec::with_capacity(arguments.len());
    let mut missing = false;
    for (&argument, value) in arguments.iter().zip(values) {
        let arg = edit.expression(argument)?;
        let value = value.as_ref();
        missing |= value.is_none_or(ProjectedValue::missing_bound);
        let bound = value.and_then(|v| v.reduced);
        facts.push(Argument {
            monotone: bound.is_some(),
            constant: value.is_some_and(|v| v.constant),
            double_literal_above_one: double_literal_above_one(edit, arg)?,
        });
        bounds.push(bound);
    }
    let constant = facts.iter().all(|fact| fact.constant);
    let rule = name.map_or(FunctionRule::Unknown, |name| function_rule(name, &facts));
    if missing && !matches!(rule, FunctionRule::Unknown) {
        // A registered function asks for each argument in order. The general
        // visitor retains facts already observed when that lookup fails.
        return Ok(None);
    }
    let Some(target) = rule.target() else {
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
