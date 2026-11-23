//! Resolve block locals in statement order. Only a block's final expression contributes its result.
use super::{Observations, ProjectedValue, Row, RowType, Shape, evaluate::Step};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, Node, TypeId,
};

pub(super) struct Local {
    name: String,
    initializer: Option<Initializer>,
}

pub(super) enum VariableRead {
    Projected(ProjectedValue),
    Uninitialized,
    NeedsVisitor,
}

struct Initializer {
    row_type: RowType,
    ty: TypeId,
    shape: Shape,
    reduced: Option<ExpressionId>,
}

fn schedule_statement(
    edit: &CircuitEditor,
    statement: &Node,
    work: &mut Vec<Step>,
) -> Result<bool, Error> {
    match statement.class() {
        "DBSPComment" | "DBSPStructItem" => {}
        "DBSPExpressionStatement" | "DBSPStaticItem" | "DBSPComparatorItem" => work.extend([
            Step::Read(edit.expression(statement.edge("expression")?)?),
            Step::Discard,
        ]),
        "DBSPFunctionItem" => {
            let function = edit.node(statement.edge("function")?)?;
            // The resolver enters these names in the enclosing block. Unlike
            // the analyzed closure's inputs, they have no projected values.
            for parameter in function.edges("parameters")? {
                work.push(Step::Bind {
                    name: binding_name(edit.node(parameter)?, "name")?,
                    initializer: None,
                });
            }
            if let Some(body) = function.field("body").and_then(Field::node_id) {
                work.extend([Step::Read(edit.expression(body)?), Step::Discard]);
            }
        }
        "DBSPLetStatement" => {
            let initializer = statement
                .field("initializer")
                .and_then(Field::node_id)
                .map(|id| edit.expression(id))
                .transpose()?;
            work.extend(initializer.map(Step::Read));
            work.push(Step::Bind {
                name: binding_name(statement, "variable")?,
                initializer,
            });
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn binding_name(node: &Node, field: &str) -> Result<String, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new(format!("missing {field} in {}", node.class())))
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
            let name = binding_name(variable, "variable")?;
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
                if !schedule_statement(edit, edit.node(statement)?, work)? {
                    return Ok(false);
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
        if matches!(self.observations, Observations::Facts(_)) {
            self.locals.truncate(outer);
        } else if outer < self.locals.len() {
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
    ) -> Result<VariableRead, Error> {
        let node = edit.node(expression.into())?;
        let Some(name) = node.data("variable").and_then(|v| v.as_str()) else {
            return Ok(VariableRead::NeedsVisitor);
        };
        let (shape, row_type, reduced) = if let Some((index, local)) = self
            .locals
            .iter()
            .enumerate()
            .rev()
            .find(|(_, local)| local.name == name)
        {
            let Some(local) = &local.initializer else {
                return Ok(VariableRead::Uninitialized);
            };
            if !edit.same_type(edit.expression_type(expression)?, local.ty)? {
                return Ok(VariableRead::NeedsVisitor);
            }
            let ty = match &local.row_type {
                RowType::Prior { index } => RowType::Prior { index: *index },
                _ => RowType::Local { index },
            };
            (&local.shape, ty, local.reduced)
        } else if let Some((index, parameter)) = self
            .bindings
            .iter()
            .enumerate()
            .rev()
            .find(|(_, p)| p.name == name)
        {
            if let Some(declared) = parameter.declared_type {
                if !edit.same_type(edit.expression_type(expression)?, declared)? {
                    return Ok(VariableRead::NeedsVisitor);
                }
            }
            (
                &parameter.shape,
                RowType::Parameter { index },
                parameter.shape.monotone().then_some(parameter.variable),
            )
        } else {
            return Ok(VariableRead::NeedsVisitor);
        };
        Ok(VariableRead::Projected(ProjectedValue {
            shape: shape.clone(),
            row_type,
            reduced,
            constant: false,
        }))
    }
}
