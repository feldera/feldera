//! Evaluate children before their parent and bind locals between successive statements.
use super::{function, project_tuple, project_wrapper, scalar, unbounded, ProjectedValue, Row};
use crate::function_calls::function_name;
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
        mut children: impl Iterator<Item = ProjectedValue>,
    ) -> Result<Option<ProjectedValue>, Error> {
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
            Self::Call(name) => function::call_bound(edit, id, name.as_deref(), children),
            Self::Unbounded => unbounded(edit, id),
        }
    }
}

/// Queue operands in reverse so the visitor sees them in Java expression order.
fn schedule(
    edit: &CircuitEditor,
    id: ExpressionId,
    base: usize,
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
            if name.is_none() {
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
                        Expr::VariablePath => (id, self.variable(edit, id)?),
                        kind if scalar::literal(kind) => (id, scalar::literal_bound(edit, id)?),
                        _ => {
                            if !schedule(edit, id, values.len(), &mut work)? {
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
                    let initial = initializer
                        .map(|expr| {
                            Ok::<_, Error>((
                                edit.expression_type(expr)?,
                                values.pop().expect("visited initializer"),
                            ))
                        })
                        .transpose()?;
                    self.bind_local(name, initial);
                    continue;
                }
                Step::EmptyBlock(id) => {
                    let Some(value) = unbounded(edit, id)? else {
                        return Ok(None);
                    };
                    values.push(value);
                    continue;
                }
                Step::FinishScope { scopes, outer } => {
                    let value =
                        self.finish_scope(outer, values.pop().expect("visited scope result"));
                    if value.shape.monotone() {
                        if let Some(clocks) = &mut self.clock_operands {
                            clocks.extend(&scopes);
                        }
                    }
                    (scopes[0], Some(value))
                }
            };
            let Some(value) = value else { return Ok(None) };
            active.remove(&id);
            if value.shape.monotone() {
                if let Some(clocks) = &mut self.clock_operands {
                    clocks.insert(id);
                }
            }
            values.push(value);
        }
        debug_assert_eq!(values.len(), 1);
        Ok(values.pop())
    }
}
