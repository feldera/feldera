//! Derive lower bounds for named scalar calls from the bounds of their arguments.
use super::evaluate::{Evaluation, Visited};
use super::{ProjectedValue, Shape, WaterlineError, WaterlineTypeError, unbounded};
use crate::monotonicity::functions::{
    Argument, FunctionRule, build_function_waterline, clock_waterline, double_literal_above_one,
    function_rule,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId};

pub(super) fn call_bound(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
    name: Option<&str>,
    values: impl ExactSizeIterator<Item = Visited>,
) -> Result<Evaluation, Error> {
    // Java constructs the result type before consulting the function rule or
    // demanding the waterlines of its already visited arguments.
    let mut result = match unbounded(edit, expression, false)? {
        Evaluation::Value(value) => value,
        failure => return Ok(failure),
    };
    let result_type = edit.expression_type(expression)?;
    let tuple_result = matches!(result.shape, Shape::Tuple { .. });
    let expected_scalar = || {
        Evaluation::Failed(WaterlineError::Type(
            WaterlineTypeError::ExpectedScalar,
            result_type,
        ))
    };
    if name == Some("now") {
        if tuple_result {
            return Ok(expected_scalar());
        }
        let clock = clock_waterline(edit, expression)?;
        return ProjectedValue::bounded(edit, expression, clock, false).map(Evaluation::Value);
    }

    let mut facts = Vec::with_capacity(values.len());
    let mut bounds = Vec::with_capacity(values.len());
    let mut first_missing = None;
    for arg in values {
        let value = arg.value.as_ref();
        if first_missing.is_none() {
            first_missing = match value {
                None => Some(WaterlineError::MissingFact(arg.id)),
                Some(value) if value.missing_bound() => Some(WaterlineError::MissingBound(arg.id)),
                Some(_) => None,
            };
        }
        let bound = value.and_then(|v| v.reduced);
        facts.push(Argument {
            monotone: bound.is_some(),
            constant: value.is_some_and(|v| v.constant),
            double_literal_above_one: double_literal_above_one(edit, arg.id)?,
        });
        bounds.push(bound);
    }
    let constant = facts.iter().all(|fact| fact.constant);
    let rule = name.map_or(FunctionRule::Unknown, |name| function_rule(name, &facts));
    if !matches!(rule, FunctionRule::Unknown) {
        if let Some(error) = first_missing {
            return Ok(Evaluation::Failed(error));
        }
    }
    let Some(target) = rule.target() else {
        result.constant = constant;
        return Ok(Evaluation::Value(result));
    };
    let reduced = build_function_waterline(edit, expression, &target, &bounds)?;
    if tuple_result {
        return Ok(expected_scalar());
    }
    ProjectedValue::bounded(edit, expression, reduced, constant).map(Evaluation::Value)
}
