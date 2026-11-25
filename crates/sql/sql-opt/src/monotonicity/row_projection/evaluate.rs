//! Evaluate children before their parent and bind locals between successive statements.
use super::{
    ExpressionWaterline, Observations, ProjectedValue, Row, RowType, WaterlineError, function,
    project_tuple, project_wrapper, scalar, unbounded,
};
use super::{FactType, scope::VariableRead};
use crate::function_calls::function_name;
use crate::monotonicity::literal::ordered_literal;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind};
use std::collections::HashSet;

/// Visiting an uninitialized variable or an aggregate descriptor produces no
/// fact. Its parent can discard it or demand it in the original operand order.
pub(super) struct Visited {
    pub id: ExpressionId,
    pub value: Option<ProjectedValue>,
}

pub(super) enum Evaluation {
    Value(ProjectedValue),
    Failed(WaterlineError),
    IncompleteInput,
}

pub(super) enum FlatmapInput<'a> {
    ZSet(&'a mut u64),
    JoinOrIndex,
    /// A detached row projection has no live generated-name allocator.
    Detached,
}

impl Evaluation {
    pub(super) fn missing_fact(id: ExpressionId) -> Self {
        Self::Failed(WaterlineError::MissingFact(id))
    }

    pub(super) fn with_bound(mut self, bound: Option<ExpressionId>) -> Self {
        if let Self::Value(value) = &mut self {
            value.reduced = bound;
        }
        self
    }

    pub(super) fn projected(self) -> Option<ProjectedValue> {
        match self {
            Self::Value(value) => Some(value),
            Self::Failed(_) | Self::IncompleteInput => None,
        }
    }
}

pub(super) enum Step {
    Read(ExpressionId),
    NoFact(ExpressionId),
    Reduce(ExpressionId, Operation, usize),
    Discard,
    Bind {
        name: String,
        initializer: Option<ExpressionId>,
    },
    FinishScope {
        scopes: Vec<ExpressionId>,
        outer: usize,
    },
    EmptyBlock(ExpressionId),
}

pub(super) enum Operation {
    Wrapper,
    Unary,
    Binary([ExpressionId; 2]),
    Tuple,
    Call(Option<String>),
    Unbounded,
}

impl Operation {
    fn reduce(
        self,
        edit: &mut CircuitEditor,
        id: ExpressionId,
        children: std::vec::Drain<'_, Visited>,
    ) -> Result<Evaluation, Error> {
        if let Self::Call(name) = self {
            return function::call_bound(edit, id, name.as_deref(), children);
        }
        // Tuple construction and scalar operations look up every operand's
        // fact before asking any of those facts for a reduced expression.
        if let Some(child) = children.as_slice().iter().find(|v| v.value.is_none()) {
            return Ok(Evaluation::missing_fact(child.id));
        }
        // A tuple consumes every field's bound. Scalar operations and wrappers
        // check their result types and select their own operands in visitor order.
        if matches!(self, Self::Tuple) {
            let ty = edit.expression_type(id)?;
            if edit.expr_kind(id)? == Expr::RawTuple && edit.nullable(ty)? {
                return Ok(Evaluation::Failed(WaterlineError::Type(
                    super::WaterlineTypeError::NullableRawTuple, ty,
                )));
            }
            if let Some(child) = children
                .as_slice()
                .iter()
                .find(|v| v.value.as_ref().is_some_and(ProjectedValue::missing_bound))
            {
                return Ok(Evaluation::Failed(WaterlineError::MissingBound(child.id)));
            }
        }
        let mut children = children.filter_map(|child| child.value);
        match self {
            Self::Wrapper => {
                project_wrapper(edit, id, children.next().expect("visited wrapper operand"))
            }
            Self::Unary => {
                scalar::unary_bound(edit, id, children.next().expect("visited unary operand"))
            }
            Self::Binary(operands) => {
                let left = children.next().expect("visited left operand");
                let right = children.next().expect("visited right operand");
                scalar::binary_bound(edit, id, operands, [left, right])
            }
            Self::Tuple => Ok(Evaluation::Value(project_tuple(edit, id, children)?)),
            Self::Call(_) => unreachable!("calls consume optional argument facts above"),
            Self::Unbounded => unbounded(edit, id, false),
        }
    }
}

/// Queue operands in reverse so the visitor sees them in Java expression order.
fn schedule(
    edit: &CircuitEditor,
    id: ExpressionId,
    base: usize,
    record_callee: bool,
    work: &mut Vec<Step>,
) -> Result<(), Error> {
    let node = edit.node(id.into())?;
    let child = |name| edit.expression(node.edge(name)?);
    let mark = work.len();
    let kind = edit.expr_kind(id)?;
    let reduce = |op| Step::Reduce(id, op, base);
    let finish = match kind {
        Expr::Borrow | Expr::Deref | Expr::Field | Expr::Clone | Expr::Some | Expr::Unwrap => {
            work.push(Step::Read(child("expression")?));
            reduce(Operation::Wrapper)
        }
        Expr::Cast | Expr::Unary | Expr::UnsignedWrap | Expr::UnsignedUnwrap => {
            work.push(Step::Read(child("source")?));
            reduce(Operation::Unary)
        }
        Expr::Binary | Expr::TimeAddSub => {
            let operands = [child("left")?, child("right")?];
            work.extend(operands.map(Step::Read));
            reduce(Operation::Binary(operands))
        }
        Expr::Apply => {
            let name = function_name(edit.circuit(), node)?.map(|s| s.into_owned());
            // An indirect callee can itself contain an observed clock operand.
            if record_callee || name.is_none() {
                work.extend([Step::Read(child("function")?), Step::Discard]);
            }
            for arg in node.edges("arguments")? {
                work.push(Step::Read(edit.expression(arg)?));
            }
            reduce(Operation::Call(name))
        }
        Expr::Tuple | Expr::RawTuple if node.field("fields").is_some() => {
            for field in node.edges("fields")? {
                work.push(Step::Read(edit.expression(field)?));
            }
            reduce(Operation::Tuple)
        }
        // A lambda has its own binding scope. Its body is opaque to this visitor.
        Expr::Tuple
        | Expr::RawTuple
        | Expr::Closure
        | Expr::Fail
        | Expr::Path
        | Expr::NullLiteral
        | Expr::KeywordLiteral
        | Expr::StrLiteral
        | Expr::VoidLiteral
        | Expr::NoExpression
        | Expr::StringLiteral
        | Expr::BinaryLiteral
        | Expr::UuidLiteral
        | Expr::VariantNullLiteral => reduce(Operation::Unbounded),
        // Expressions without a transfer rule still visit their operands. Type
        // information and the aggregate's cached empty result are not operands.
        _ => {
            let mut pending = crate::lexical_scope::expression_children(node);
            pending.reverse();
            while let Some(child) = pending.pop() {
                let node = edit.node(child)?;
                match node.inner_kind() {
                    Some(InnerKind::Expression(_)) => {
                        work.extend([Step::Read(edit.expression(child)?), Step::Discard]);
                    }
                    Some(InnerKind::Type(_) | InnerKind::Parameter) => {}
                    _ => pending.extend(crate::lexical_scope::expression_children(node).into_iter().rev()),
                }
            }
            match kind {
                // The visitor walks IAggregate's children but never calls
                // postorder(DBSPExpression) for the descriptor itself.
                Expr::LinearAggregate | Expr::NonLinearAggregate | Expr::MinMaxAggregate => {
                    Step::NoFact(id)
                }
                _ => reduce(Operation::Unbounded),
            }
        }
    };
    work.push(finish);
    work[mark..].reverse();
    Ok(())
}

impl Row {
    fn observe(
        &mut self,
        edit: &CircuitEditor,
        id: ExpressionId,
        value: &mut ProjectedValue,
    ) -> Result<(), Error> {
        match &mut self.observations {
            Observations::ResultOnly => {}
            Observations::Clocks(clocks) => {
                if value.shape.monotone() {
                    clocks.insert(id);
                }
            }
            Observations::Facts(facts) => {
                let index = facts.len();
                let row_type = std::mem::replace(&mut value.row_type, RowType::Prior { index });
                let (origin, bound) = match (edit.expr_kind(id)?, row_type) {
                    (Expr::VariablePath, RowType::Parameter { index }) => (
                        FactType::Parameter(index),
                        Some(self.bindings[index].variable),
                    ),
                    (
                        Expr::VariablePath | Expr::Let | Expr::Block | Expr::Flatmap,
                        RowType::Prior { index },
                    ) => (FactType::Alias(index), None),
                    (_, ty) => (FactType::Computed(ty), value.reduced),
                };
                facts.push(ExpressionWaterline {
                    expression: id,
                    origin,
                    bound,
                });
            }
        }
        Ok(())
    }

    fn nested_flatmap(
        &mut self,
        edit: &mut CircuitEditor,
        id: ExpressionId,
        next: &mut u64,
    ) -> Result<Evaluation, Error> {
        let closure = crate::flatmap_waterline(edit, id.into(), next)?;
        let result_type = |expr| {
            let function = edit.expression_type(expr)?;
            edit.type_id(edit.node(function.into())?.edge("resultType")?)
        };
        let computed = result_type(closure)?;
        if !edit.same_type(result_type(id)?, computed)? {
            return Ok(Evaluation::Failed(WaterlineError::FlatmapResult {
                flatmap: id,
                computed,
            }));
        }
        // Nested closures are opaque. The synthetic closure and the flatmap
        // point to the very same analysis, whose type belongs to the closure.
        let mut result = unbounded(edit, closure, false)?;
        if let (Evaluation::Value(value), Observations::Facts(facts)) =
            (&mut result, &mut self.observations)
        {
            value.row_type = RowType::Prior { index: facts.len() };
            facts.push(ExpressionWaterline {
                expression: id,
                origin: FactType::Synthesized(closure),
                bound: None,
            });
        }
        Ok(result)
    }

    pub(super) fn visit(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
        mut input: FlatmapInput<'_>,
    ) -> Result<Evaluation, Error> {
        let mut work = vec![Step::Read(expression)];
        let mut values = Vec::new();
        let mut references_replaced = false;
        let mut active = HashSet::new();
        while let Some(step) = work.pop() {
            let (id, mut value) = match step {
                Step::Read(id) => {
                    if !active.insert(id) {
                        return Err(Error::new("cycle in a waterline expression"));
                    }
                    match edit.expr_kind(id)? {
                        Expr::Let | Expr::Block => {
                            super::scope::schedule(edit, id, self.locals.len(), &mut work)?;
                            continue;
                        }
                        Expr::Flatmap => {
                            let names = match &mut input {
                                FlatmapInput::ZSet(names) => &mut **names,
                                FlatmapInput::JoinOrIndex => {
                                    return Ok(Evaluation::Failed(WaterlineError::FlatmapInput(id)));
                                }
                                FlatmapInput::Detached => return Ok(Evaluation::IncompleteInput),
                            };
                            references_replaced = true;
                            match self.nested_flatmap(edit, id, names)? {
                                Evaluation::Value(value) => (id, value),
                                other => return Ok(other),
                            }
                        }
                        Expr::VariablePath if references_replaced => {
                            return Ok(Evaluation::Failed(WaterlineError::MissingReference(id)));
                        }
                        Expr::VariablePath => match self.variable(edit, id)? {
                            VariableRead::Projected(value) => (id, value),
                            VariableRead::TypeMismatch(mut declaration) => {
                                // The visitor records the declaration's fact at the read
                                // before checking the path's annotated type.
                                self.observe(edit, id, &mut declaration)?;
                                return Ok(Evaluation::Failed(WaterlineError::VariableTypeMismatch(id)));
                            }
                            VariableRead::Uninitialized => {
                                work.push(Step::NoFact(id));
                                continue;
                            }
                            VariableRead::MissingDeclaration => {
                                // Detached projections receive only their supplied bindings.
                                // Whole closures resolved this path before visiting the body.
                                if matches!(input, FlatmapInput::Detached) {
                                    return Ok(Evaluation::IncompleteInput);
                                }
                                work.push(Step::NoFact(id));
                                continue;
                            }
                        },
                        kind if ordered_literal(kind) => match scalar::literal_bound(edit, id)? {
                            Evaluation::Value(value) => (id, value),
                            other => return Ok(other),
                        },
                        _ => {
                            schedule(
                                edit,
                                id,
                                values.len(),
                                matches!(self.observations, Observations::Facts(_)),
                                &mut work,
                            )?;
                            continue;
                        }
                    }
                }
                Step::Reduce(id, op, base) => match op.reduce(edit, id, values.drain(base..))? {
                    Evaluation::Value(value) => (id, value),
                    other => return Ok(other),
                },
                Step::NoFact(id) => {
                    // Its parent decides whether evaluating the operation requires a fact.
                    values.push(Visited { id, value: None });
                    active.remove(&id);
                    continue;
                }
                Step::Discard => {
                    values.pop().expect("visited expression statement");
                    continue;
                }
                Step::Bind { name, initializer } => {
                    let initial = if let Some(expr) = initializer {
                        let initial = values.pop().expect("visited initializer");
                        let Some(value) = initial.value else {
                            return Ok(Evaluation::missing_fact(initial.id));
                        };
                        Some((edit.expression_type(expr)?, value))
                    } else {
                        None
                    };
                    self.bind_local(name, initial);
                    continue;
                }
                Step::EmptyBlock(id) => {
                    let value = match unbounded(edit, id, false)? {
                        Evaluation::Value(value) => value,
                        failure => return Ok(failure),
                    };
                    values.push(Visited {
                        id,
                        value: Some(value),
                    });
                    continue;
                }
                Step::FinishScope { scopes, outer } => {
                    let tail = values.pop().expect("visited scope result");
                    let Some(result) = tail.value else {
                        return Ok(Evaluation::missing_fact(tail.id));
                    };
                    let mut value = self.finish_scope(outer, result);
                    // Blocks and LETs take the very same analysis as their tail.
                    for &scope in scopes.iter().skip(1).rev() {
                        self.observe(edit, scope, &mut value)?;
                    }
                    (scopes[0], value)
                }
            };
            active.remove(&id);
            self.observe(edit, id, &mut value)?;
            // Some(()) exposes its operand's bound to callers inspecting this node.
            // Its parent can only consume the bound when the result shape is monotone.
            if !value.shape.monotone() {
                value.reduced = None;
            }
            values.push(Visited {
                id,
                value: Some(value),
            });
        }
        debug_assert_eq!(values.len(), 1);
        let result = values.pop().expect("visited closure body");
        Ok(match result.value {
            Some(value) if !value.missing_bound() => Evaluation::Value(value),
            Some(_) => Evaluation::Failed(WaterlineError::MissingBound(result.id)),
            None => Evaluation::missing_fact(result.id),
        })
    }
}
