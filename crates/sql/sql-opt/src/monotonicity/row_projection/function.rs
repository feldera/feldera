//! Derive lower bounds for named scalar calls from the bounds of their arguments.
use super::{ProjectedValue, Row, Shape, unbounded, unbounded_shape};
use crate::function_calls::function_name;
use crate::monotonicity::functions::{Argument, double_literal_above_one, function_waterline};
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind,
};

impl Row {
    pub(super) fn call(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let site = expression.into();
        let call = edit.node(site)?;
        let name = function_name(edit.circuit(), call)?.map(|s| s.into_owned());
        let callee = edit.expression(call.edge("function")?)?;
        let ty = edit.expression_type(expression)?;
        let arguments = call.edges("arguments")?;
        if name.as_deref() == Some("now") {
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
            return ProjectedValue::bounded(edit, expression, edit.expression(clock)?, false)
                .map(Some);
        }

        // An indirect call has no named transfer rule. Visit the callee and its
        // arguments so nested expressions still contribute their clock facts.
        if name.is_none() && self.visit(edit, callee, depth + 1)?.is_none() {
            return Ok(None);
        }
        let mut facts = Vec::with_capacity(arguments.len());
        let mut bounds = Vec::with_capacity(arguments.len());
        for &argument in &arguments {
            let arg = edit.expression(argument)?;
            let Some(value) = self.visit(edit, arg, depth + 1)? else {
                return Ok(None);
            };
            facts.push(Argument {
                monotone: value.reduced.is_some(),
                constant: value.constant,
                double_literal_above_one: double_literal_above_one(edit, arg)?,
            });
            bounds.push(value.reduced);
        }
        let constant = facts.iter().all(|fact| fact.constant);
        let Some(target) = name
            .as_deref()
            .and_then(|name| function_waterline(name, &facts))
        else {
            let Some(mut result) = unbounded(edit, expression, depth)? else {
                return Ok(None);
            };
            result.constant = constant;
            return Ok(Some(result));
        };
        if !matches!(unbounded_shape(edit, ty, depth)?, Some(Shape::Scalar(_))) {
            return Ok(None);
        }
        let arguments = arguments
            .into_iter()
            .zip(bounds)
            .map(|(arg, bound)| {
                Ok(Field::Reference(match bound {
                    Some(bound) => bound.into(),
                    None => copy_expression(edit, arg)?,
                }))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let callee = edit.function_path(site, &target)?;
        let required = edit.with_nullability(ty, false)?;
        let reduced = edit.make_expr(
            site,
            Expr::Apply,
            required,
            [
                ("function", Field::Reference(callee.into())),
                ("arguments", Field::Array(arguments)),
            ],
        )?;
        ProjectedValue::bounded(edit, expression, reduced, constant).map(Some)
    }
}
