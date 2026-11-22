//! Resolve block locals in statement order. Only a block's final expression contributes its result.
use super::{ProjectedValue, Row, RowType, Shape, unbounded};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, TypeId};

pub(super) struct Local {
    name: String,
    initializer: Option<Initializer>,
}

struct Initializer {
    ty: TypeId,
    shape: Shape,
    reduced: Option<ExpressionId>,
}

impl Row {
    pub(super) fn scope(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let outer = self.locals.len();
        let result = self.scope_body(edit, expression, depth);
        self.locals.truncate(outer);
        result
    }

    fn scope_body(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        depth: usize,
    ) -> Result<Option<ProjectedValue>, Error> {
        let mut initializers = Vec::new();
        let node = edit.node(expression.into())?;
        let tail = if edit.expr_kind(expression)? == Expr::Let {
            let variable = edit.node(node.edge("variable")?)?;
            let name = variable
                .data("variable")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("expected a LET variable name"))?
                .to_owned();
            let init = edit.expression(node.edge("initializer")?)?;
            let tail = edit.expression(node.edge("consumer")?)?;
            if !self.bind_local(edit, name, Some(init), depth, &mut initializers)? {
                return Ok(None);
            }
            Some(tail)
        } else {
            let tail = node
                .field("lastExpression")
                .and_then(Field::node_id)
                .map(|id| edit.expression(id))
                .transpose()?;
            for id in node.edges("contents")? {
                let statement = edit.node(id)?;
                match statement.class() {
                    "DBSPComment" => {}
                    "DBSPExpressionStatement" => {
                        let value = edit.expression(statement.edge("expression")?)?;
                        if self.visit(edit, value, depth + 1)?.is_none() {
                            return Ok(None);
                        }
                    }
                    "DBSPLetStatement" => {
                        let name = statement
                            .data("variable")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| Error::new("expected a block variable name"))?
                            .to_owned();
                        let init = statement
                            .field("initializer")
                            .and_then(Field::node_id)
                            .map(|id| edit.expression(id))
                            .transpose()?;
                        if !self.bind_local(edit, name, init, depth, &mut initializers)? {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                }
            }
            tail
        };
        let Some(mut value) = (match tail {
            Some(tail) => self.visit(edit, tail, depth + 1)?,
            None => unbounded(edit, expression, depth)?,
        }) else {
            return Ok(None);
        };
        if !initializers.is_empty() {
            value.row_type = RowType::Scope {
                initializers,
                result: Box::new(value.row_type),
            };
        }
        Ok(Some(value))
    }

    fn bind_local(
        &mut self,
        edit: &mut CircuitEditor,
        name: String,
        init: Option<ExpressionId>,
        depth: usize,
        initializers: &mut Vec<Option<RowType>>,
    ) -> Result<bool, Error> {
        // The initializer sees the previous declaration of the same name.
        let initial = if let Some(expr) = init {
            let Some(value) = self.visit(edit, expr, depth + 1)? else {
                return Ok(false);
            };
            initializers.push(Some(value.row_type));
            Some(Initializer {
                ty: edit.expression_type(expr)?,
                shape: value.shape,
                reduced: value.reduced,
            })
        } else {
            initializers.push(None);
            None
        };
        self.locals.push(Local {
            name,
            initializer: initial,
        });
        Ok(true)
    }

    pub(super) fn variable(
        &self,
        edit: &CircuitEditor,
        expression: ExpressionId,
    ) -> Result<Option<ProjectedValue>, Error> {
        let node = edit.node(expression.into())?;
        let Some(name) = node.data("variable").and_then(|v| v.as_str()) else {
            return Ok(None);
        };
        let (shape, row_type, reduced) = if let Some((index, local)) = self
            .locals
            .iter()
            .enumerate()
            .rev()
            .find(|(_, local)| local.name == name)
        {
            let Some(local) = &local.initializer else {
                return Ok(None);
            };
            if !edit.same_type(edit.expression_type(expression)?, local.ty)? {
                return Ok(None);
            }
            (&local.shape, RowType::Local { index }, local.reduced)
        } else if let Some((index, parameter)) = self
            .bindings
            .iter()
            .enumerate()
            .find(|(_, p)| p.name == name)
        {
            (
                &parameter.shape,
                RowType::Parameter { index },
                parameter.shape.monotone().then_some(parameter.variable),
            )
        } else {
            return Ok(None);
        };
        Ok(Some(ProjectedValue {
            shape: shape.clone(),
            row_type,
            reduced,
            constant: false,
        }))
    }
}
