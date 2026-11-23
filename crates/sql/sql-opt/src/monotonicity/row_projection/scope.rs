//! Resolve block locals in statement order. Only a block's final expression contributes its result.
use super::{evaluate::Step, ProjectedValue, Row, RowType, Shape};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, TypeId};

pub(super) struct Local {
    name: String,
    initializer: Option<Initializer>,
}

struct Initializer {
    row_type: RowType,
    ty: TypeId,
    shape: Shape,
    reduced: Option<ExpressionId>,
}

/// Consecutive tail scopes share the same lifetime. Flatten their declarations so
/// the projected type refers to the same local slots when Java reconstructs it.
pub(super) fn schedule(
    edit: &CircuitEditor,
    expression: ExpressionId,
    outer: usize,
    work: &mut Vec<Step>,
) -> Result<bool, Error> {
    let mark = work.len();
    let mut seen = std::collections::HashSet::new();
    let mut scopes = Vec::new();
    let mut tail = expression;
    loop {
        if !seen.insert(tail) {
            return Err(Error::new("cycle in a waterline binding"));
        }
        scopes.push(tail);
        let node = edit.node(tail.into())?;
        let next = if edit.expr_kind(tail)? == Expr::Let {
            let variable = edit.node(node.edge("variable")?)?;
            let name = variable
                .data("variable")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("expected a LET variable name"))?
                .to_owned();
            let init = edit.expression(node.edge("initializer")?)?;
            work.extend([
                Step::Read(init),
                Step::Bind {
                    name,
                    initializer: Some(init),
                },
            ]);
            Some(edit.expression(node.edge("consumer")?)?)
        } else {
            for statement in node.edges("contents")? {
                let statement = edit.node(statement)?;
                match statement.class() {
                    "DBSPComment" => {}
                    "DBSPExpressionStatement" => work.extend([
                        Step::Read(edit.expression(statement.edge("expression")?)?),
                        Step::Discard,
                    ]),
                    "DBSPLetStatement" => {
                        let name = statement
                            .data("variable")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| Error::new("expected a block variable name"))?
                            .to_owned();
                        let initializer = statement
                            .field("initializer")
                            .and_then(Field::node_id)
                            .map(|id| edit.expression(id))
                            .transpose()?;
                        work.extend(initializer.map(Step::Read));
                        work.push(Step::Bind { name, initializer });
                    }
                    _ => return Ok(false),
                }
            }
            node.field("lastExpression")
                .and_then(Field::node_id)
                .map(|id| edit.expression(id))
                .transpose()?
        };
        match next {
            Some(id) if matches!(edit.expr_kind(id)?, Expr::Let | Expr::Block) => tail = id,
            Some(id) => {
                work.push(Step::Read(id));
                break;
            }
            None => {
                work.push(Step::EmptyBlock(tail));
                break;
            }
        }
    }
    work.push(Step::FinishScope { scopes, outer });
    work[mark..].reverse();
    Ok(true)
}

impl Row {
    pub(super) fn bind_local(&mut self, name: String, initial: Option<(TypeId, ProjectedValue)>) {
        let initializer = initial.map(|(ty, value)| Initializer {
            ty,
            shape: value.shape,
            reduced: value.reduced,
            row_type: value.row_type,
        });
        self.locals.push(Local { name, initializer });
    }

    pub(super) fn finish_scope(
        &mut self,
        outer: usize,
        mut value: ProjectedValue,
    ) -> ProjectedValue {
        if outer < self.locals.len() {
            let initializers = self
                .locals
                .drain(outer..)
                .map(|local| local.initializer.map(|i| i.row_type))
                .collect();
            value.row_type = RowType::Scope {
                initializers,
                result: Box::new(value.row_type),
            };
        }
        value
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
