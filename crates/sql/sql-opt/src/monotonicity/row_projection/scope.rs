//! Resolve block locals in statement order. Only a block's final expression contributes its result.
use super::{unbounded, ProjectedValue, Row, RowType, Shape};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
};

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
        let mut scopes = std::collections::HashSet::new();
        let mut tail = expression;
        // A tail scope sees every enclosing declaration until its result is returned.
        // Resolve successive LETs and blocks in one environment, in declaration order.
        let mut value = loop {
            if !scopes.insert(tail) {
                return Err(Error::new("cycle in a waterline binding"));
            }
            let node = edit.node(tail.into())?;
            let next = if edit.expr_kind(tail)? == Expr::Let {
                let variable = edit.node(node.edge("variable")?)?;
                let name = variable
                    .data("variable")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::new("expected a LET variable name"))?
                    .to_owned();
                let init = edit.expression(node.edge("initializer")?)?;
                let consumer = edit.expression(node.edge("consumer")?)?;
                if !self.bind_local(edit, name, Some(init), depth, &mut initializers)? {
                    return Ok(None);
                }
                Some(consumer)
            } else {
                let last = node
                    .field("lastExpression")
                    .and_then(Field::node_id)
                    .map(|id| edit.expression(id))
                    .transpose()?;
                let statements = node.edges("contents")?;
                if !self.block_statements(edit, statements, depth, &mut initializers)? {
                    return Ok(None);
                }
                last
            };
            let result = match next {
                Some(next) if matches!(edit.expr_kind(next)?, Expr::Block | Expr::Let) => {
                    tail = next;
                    continue;
                }
                Some(next) => self.visit(edit, next, depth + 1)?,
                None => unbounded(edit, tail)?,
            };
            let Some(value) = result else { return Ok(None) };
            break value;
        };
        if value.shape.monotone() {
            if let Some(operands) = &mut self.clock_operands {
                operands.extend(scopes);
            }
        }
        if !initializers.is_empty() {
            value.row_type = RowType::Scope {
                initializers,
                result: Box::new(value.row_type),
            };
        }
        Ok(Some(value))
    }

    fn block_statements(
        &mut self,
        edit: &mut CircuitEditor,
        statements: Vec<NodeId>,
        depth: usize,
        initializers: &mut Vec<Option<RowType>>,
    ) -> Result<bool, Error> {
        for id in statements {
            let statement = edit.node(id)?;
            match statement.class() {
                "DBSPComment" => {}
                "DBSPExpressionStatement" => {
                    let value = edit.expression(statement.edge("expression")?)?;
                    if self.visit(edit, value, depth + 1)?.is_none() {
                        return Ok(false);
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
                    if !self.bind_local(edit, name, init, depth, initializers)? {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        Ok(true)
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
            .rev()
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
