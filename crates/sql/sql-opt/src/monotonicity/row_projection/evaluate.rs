//! Evaluate children before their parent and bind locals between successive statements.
use super::{
    ExpressionWaterline, Observations, ProjectedValue, Row, RowType, function, project_tuple,
    project_wrapper, scalar, unbounded,
};
use super::{FactType, scope::VariableRead};
use crate::function_calls::function_name;
use crate::monotonicity::literal::ordered_literal;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind};
use std::collections::HashSet;

pub(super) enum Step {
    Read(ExpressionId),
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
        children: std::vec::Drain<'_, Option<ProjectedValue>>,
    ) -> Result<Option<ProjectedValue>, Error> {
        if let Self::Call(name) = self {
            return function::call_bound(edit, id, name.as_deref(), children);
        }
        // Field reads and scalar operators decide which operand bounds they need.
        let selects_bounds =
            matches!(self, Self::Binary(_) | Self::Unary) || edit.expr_kind(id)? == Expr::Field;
        if !children.as_slice().iter().all(|v| {
            v.as_ref().is_some_and(|v| selects_bounds || !v.missing_bound())
        }) {
            return Ok(None);
        }
        let mut children = children.flatten();
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
            Self::Tuple => project_tuple(edit, id, children),
            Self::Call(_) => unreachable!("calls consume optional argument facts above"),
            Self::Unbounded => unbounded(edit, id),
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
) -> Result<bool, Error> {
    let node = edit.node(id.into())?;
    let child = |name| edit.expression(node.edge(name)?);
    let mark = work.len();
    let operation = match edit.expr_kind(id)? {
        Expr::Borrow | Expr::Deref | Expr::Field | Expr::Clone | Expr::Some | Expr::Unwrap => {
            work.push(Step::Read(child("expression")?));
            Operation::Wrapper
        }
        Expr::Cast | Expr::Unary | Expr::UnsignedWrap | Expr::UnsignedUnwrap => {
            work.push(Step::Read(child("source")?));
            Operation::Unary
        }
        Expr::Binary | Expr::TimeAddSub => {
            let operands = [child("left")?, child("right")?];
            work.extend(operands.map(Step::Read));
            Operation::Binary(operands)
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
            Operation::Call(name)
        }
        Expr::Tuple | Expr::RawTuple if node.field("fields").is_some() => {
            for field in node.edges("fields")? {
                work.push(Step::Read(edit.expression(field)?));
            }
            Operation::Tuple
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
        | Expr::VariantNullLiteral => Operation::Unbounded,
        // These results have no transfer rule, but their operands still contribute clock facts.
        Expr::If
        | Expr::IsNull
        | Expr::Array
        | Expr::Map
        | Expr::ApplyMethod
        | Expr::Constructor
        | Expr::Lazy
        | Expr::Question
        | Expr::Variant
        | Expr::GeoPointConstructor
        | Expr::UnwrapCustomOrd
        | Expr::CustomOrd
        | Expr::HandleError
        | Expr::Assignment
        | Expr::ConditionalIncrement
        | Expr::Return
        | Expr::QualifyType
        | Expr::CustomOrdField
        | Expr::DirectComparator
        | Expr::FieldComparator
        | Expr::EqualityComparator
        | Expr::AsymmetricFieldComparator
        | Expr::NoComparator
        | Expr::WindowBound
        | Expr::Sort
        | Expr::Static
        | Expr::EnumValue
        | Expr::For
        | Expr::Fold
        | Expr::MinMax
        | Expr::ZSet
        | Expr::IndexedZSet => {
            let mut children = Vec::new();
            for (_, field) in node.fields() {
                field.collect_inner_edges(&mut children);
            }
            for child in children {
                match edit.node(child)?.inner_kind() {
                    Some(InnerKind::Type(_)) => {}
                    Some(InnerKind::Expression(_)) => {
                        work.extend([Step::Read(edit.expression(child)?), Step::Discard])
                    }
                    _ => return Ok(false),
                }
            }
            Operation::Unbounded
        }
        _ => return Ok(false),
    };
    work.push(Step::Reduce(id, operation, base));
    work[mark..].reverse();
    Ok(true)
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
                    (Expr::VariablePath | Expr::Let | Expr::Block, RowType::Prior { index }) => {
                        (FactType::Alias(index), None)
                    }
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

    pub(super) fn visit(
        &mut self,
        edit: &mut CircuitEditor,
        expression: ExpressionId,
    ) -> Result<Option<ProjectedValue>, Error> {
        let mut work = vec![Step::Read(expression)];
        let mut values = Vec::new();
        let mut active = HashSet::new();
        while let Some(step) = work.pop() {
            let (id, value) = match step {
                Step::Read(id) => {
                    if !active.insert(id) {
                        return Err(Error::new("cycle in a waterline expression"));
                    }
                    match edit.expr_kind(id)? {
                        Expr::Let | Expr::Block => {
                            if !super::scope::schedule(edit, id, self.locals.len(), &mut work)? {
                                return Ok(None);
                            }
                            continue;
                        }
                        Expr::VariablePath => match self.variable(edit, id)? {
                            VariableRead::Projected(value) => (id, Some(value)),
                            VariableRead::Uninitialized => {
                                // The visitor records no fact for this read. Its parent
                                // decides whether evaluating the operation requires one.
                                values.push(None);
                                active.remove(&id);
                                continue;
                            }
                            VariableRead::NeedsVisitor => {
                                return Ok(None);
                            }
                        },
                        kind if ordered_literal(kind) => (id, scalar::literal_bound(edit, id)?),
                        _ => {
                            if !schedule(
                                edit,
                                id,
                                values.len(),
                                matches!(self.observations, Observations::Facts(_)),
                                &mut work,
                            )? {
                                return Ok(None);
                            }
                            continue;
                        }
                    }
                }
                Step::Reduce(id, op, base) => (id, op.reduce(edit, id, values.drain(base..))?),
                Step::Discard => {
                    values.pop().expect("visited expression statement");
                    continue;
                }
                Step::Bind { name, initializer } => {
                    let initial = if let Some(expr) = initializer {
                        let Some(value) = values.pop().expect("visited initializer") else {
                            return Ok(None);
                        };
                        Some((edit.expression_type(expr)?, value))
                    } else {
                        None
                    };
                    self.bind_local(name, initial);
                    continue;
                }
                Step::EmptyBlock(id) => {
                    let Some(value) = unbounded(edit, id)? else {
                        return Ok(None);
                    };
                    values.push(Some(value));
                    continue;
                }
                Step::FinishScope { scopes, outer } => {
                    let Some(result) = values.pop().expect("visited scope result") else {
                        return Ok(None);
                    };
                    let mut value = self.finish_scope(outer, result);
                    // Blocks and LETs take the very same analysis as their tail.
                    for &scope in scopes.iter().skip(1).rev() {
                        self.observe(edit, scope, &mut value)?;
                    }
                    (scopes[0], Some(value))
                }
            };
            let Some(mut value) = value else {
                return Ok(None);
            };
            active.remove(&id);
            self.observe(edit, id, &mut value)?;
            // Some(()) exposes its operand's bound to callers inspecting this node.
            // Its parent can only consume the bound when the result shape is monotone.
            if !value.shape.monotone() {
                value.reduced = None;
            }
            values.push(Some(value));
        }
        debug_assert_eq!(values.len(), 1);
        Ok(values.pop().flatten().filter(|v| !v.missing_bound()))
    }
}
