//! Split a filter into a prefix of window bounds and the predicates evaluated after it.
#[cfg(test)]
mod clock_bounds;
use crate::monotonicity::row_projection::WaterlineTypeError;
use crate::monotonicity::functions::FunctionArgumentError;
use crate::{WindowComparison, function_calls::contains_now};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind, NodeId, TypeId,
};
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq)]
pub enum TemporalPredicate {
    NoNow(ExpressionId),
    Residual(ExpressionId),
    Bound {
        key: ExpressionId,
        clock: ExpressionId,
        comparison: WindowComparison,
    },
}

/// A window that one conjunct can implement before the other conjuncts run.
pub struct ConjunctWindow {
    pub key: ExpressionId,
    pub lower: bool,
}

/// Window sharing inspects each conjunct independently. A residual interrupts
/// window lowering in predicate order, but later conjuncts can still be moved
/// to share a window with another filter.
pub fn conjunct_windows(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
    next: &mut u64,
) -> Result<Option<Vec<Option<ConjunctWindow>>>, Error> {
    let Some(terms) = analyze_conjuncts(edit, parameter, body)? else {
        return Ok(None);
    };
    let mut windows = Vec::with_capacity(terms.len());
    for term in terms {
        *next = next
            .checked_add(term.names)
            .ok_or_else(|| Error::new("generated names exhausted classifying conjuncts"))?;
        windows.push(term.window);
    }
    Ok(Some(windows))
}

pub(super) struct AnalyzedConjunct {
    pub expression: ExpressionId,
    pub window: Option<ConjunctWindow>,
    pub names: u64,
}

pub(super) fn analyze_conjuncts(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
) -> Result<Option<Vec<AnalyzedConjunct>>, Error> {
    let terms = crate::filters::conjuncts(edit, body)?;
    let mut analyzed = Vec::with_capacity(terms.len());
    for expression in terms {
        let term = crate::filters::strip_wrap_bool(edit, expression)?;
        let mut names = 0;
        let Some(parts) = infer_temporal_predicates(edit, parameter, term, &mut names)? else {
            return Ok(None);
        };
        let window = match parts.as_slice() {
            [
                TemporalPredicate::Bound {
                    key, comparison, ..
                },
            ] => Some(ConjunctWindow {
                key: *key,
                lower: matches!(
                    comparison,
                    WindowComparison::Greater | WindowComparison::GreaterEqual
                ),
            }),
            _ => None,
        };
        analyzed.push(AnalyzedConjunct {
            expression,
            names,
            window,
        });
    }
    Ok(Some(analyzed))
}

/// Java keys its waterline facts by node ID and its lexical references by expression.
#[derive(Clone, Copy)]
pub enum MissingClockKey {
    Fact(ExpressionId),
    Reference(ExpressionId),
}

impl MissingClockKey {
    pub fn expression(self) -> ExpressionId {
        match self {
            Self::Fact(id) | Self::Reference(id) => id,
        }
    }
}

pub enum TemporalDiagnostic {
    Missing(MissingClockKey),
    MissingBound,
    Type(WaterlineTypeError, TypeId),
    FunctionArgument(ExpressionId, FunctionArgumentError),
    Comparison(crate::ComparisonDiagnostic),
    /// The diagnostic needs the live types of earlier expressions in this predicate.
    Predicate { parameter: NodeId, body: ExpressionId },
}

pub enum ClockError {
    Diagnostic(TemporalDiagnostic),
    InvalidIr(Error),
}

/// Only a complete filter closure binds a projected ZSet row. Detached
/// expression analysis can use an arbitrary value as its binding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClockInput {
    Expression,
    Filter,
}

impl From<TemporalDiagnostic> for ClockError {
    fn from(diagnostic: TemporalDiagnostic) -> Self {
        Self::Diagnostic(diagnostic)
    }
}

impl From<crate::KeyComparisonError> for ClockError {
    fn from(error: crate::KeyComparisonError) -> Self {
        match error {
            crate::KeyComparisonError::InvalidIr(error) => Self::InvalidIr(error),
            crate::KeyComparisonError::Diagnostic(error) => TemporalDiagnostic::Comparison(error).into(),
        }
    }
}

impl From<Error> for ClockError {
    fn from(error: Error) -> Self {
        Self::InvalidIr(error)
    }
}

/// A speculative classifier leaves the name allocator intact on a refusal.
pub(super) fn speculative_clock<T>(
    next: &mut u64,
    check: impl FnOnce(&mut u64) -> Result<Option<T>, ClockError>,
) -> Result<Option<T>, Error> {
    let mut names = *next;
    match check(&mut names) {
        Ok(Some(result)) => {
            *next = names;
            Ok(Some(result))
        }
        Ok(None) | Err(ClockError::Diagnostic(_)) => Ok(None),
        Err(ClockError::InvalidIr(error)) => Err(error),
    }
}

/// Classify clock operands with the same waterline transfer as other filter closures.
pub fn infer_temporal_predicates(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
    next: &mut u64,
) -> Result<Option<Vec<TemporalPredicate>>, Error> {
    speculative_clock(next, |names| {
        checked_temporal_predicates(edit, parameter, body, names, ClockInput::Expression)
    })
}

pub(super) fn checked_temporal_predicates(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
    next: &mut u64,
    input: ClockInput,
) -> Result<Option<Vec<TemporalPredicate>>, ClockError> {
    let Some(operands) =
        crate::monotonicity::row_projection::clock_facts(edit, parameter, body, next, input)?
    else {
        return Ok(None);
    };
    let predicate = crate::filters::strip_wrap_bool(edit, body)?;
    Ok(Some(classify_temporal_predicates(edit, predicate, &operands)?))
}

fn comparison(opcode: &str, clock_on_left: bool) -> Option<WindowComparison> {
    use WindowComparison::*;
    match (opcode, clock_on_left) {
        ("LT", false) | ("GT", true) => Some(Less),
        ("LTE", false) | ("GTE", true) => Some(LessEqual),
        ("GT", false) | ("LT", true) => Some(Greater),
        ("GTE", false) | ("LTE", true) => Some(GreaterEqual),
        _ => None,
    }
}

fn has_clock(edit: &CircuitEditor, expression: ExpressionId) -> Result<bool, Error> {
    contains_now(edit.circuit(), &[expression.into()])
}

fn classify_binary(
    edit: &CircuitEditor,
    id: ExpressionId,
    monotone: &HashSet<ExpressionId>,
) -> Result<TemporalPredicate, Error> {
    let node = edit.node(id.into())?;
    let left = edit.expression(node.edge("left")?)?;
    let right = edit.expression(node.edge("right")?)?;
    let (key, clock, swapped) = match (has_clock(edit, left)?, has_clock(edit, right)?) {
        (false, false) => return Ok(TemporalPredicate::NoNow(id)),
        (true, true) => return Ok(TemporalPredicate::Residual(id)),
        (true, false) => (right, left, true),
        (false, true) => (left, right, false),
    };
    let opcode = node
        .data("opcode")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing binary opcode"))?;
    if let Some(comparison) = comparison(opcode, swapped).filter(|_| monotone.contains(&clock)) {
        return Ok(TemporalPredicate::Bound {
            key,
            clock,
            comparison,
        });
    }
    Ok(TemporalPredicate::Residual(id))
}

pub(super) fn conjoin(
    edit: &mut CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<ExpressionId, Error> {
    let nullable = edit.nullable(edit.expression_type(left)?)?
        || edit.nullable(edit.expression_type(right)?)?;
    let ty = edit.bool_type(left.into(), nullable)?;
    let and = edit.binary_expression(left.into(), ty, BinaryOp::And, left, right)?;
    edit.wrap_bool_if_needed(and)
}

/// Once a residual interrupts the prefix, consume each pending right subtree as
/// a whole. The first clock-dependent residual absorbs all following predicates.
pub fn classify_temporal_predicates(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    monotone: &HashSet<ExpressionId>,
) -> Result<Vec<TemporalPredicate>, Error> {
    use TemporalPredicate::*;
    let mut parts = Vec::new();
    let mut pending = vec![body];
    let mut scanning = true;
    while let Some(id) = pending.pop() {
        let node = edit.node(id.into())?;
        if scanning && edit.expr_kind(id)? == ExpressionKind::Binary {
            if node.data("opcode").and_then(|v| v.as_str()) == Some("AND") {
                pending.push(edit.expression(node.edge("right")?)?);
                pending.push(edit.expression(node.edge("left")?)?);
            } else {
                let part = classify_binary(edit, id, monotone)?;
                scanning = !matches!(part, Residual(_));
                parts.push(part);
            }
            continue;
        }

        let clock = has_clock(edit, id)?;
        if !scanning {
            let previous = match parts.last_mut() {
                Some(Residual(previous)) => Some(previous),
                Some(NoNow(previous)) if !clock => Some(previous),
                _ => None,
            };
            if let Some(previous) = previous {
                *previous = conjoin(edit, *previous, id)?;
                continue;
            }
        }
        scanning = false;
        parts.push(if clock { Residual(id) } else { NoNow(id) });
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn a_clock_bound_requires_one_monotone_clock_operand() -> Result<(), Error> {
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPApplyExpression","id":1,"arguments":[],
            "type":{"class":"DBSPTypeInteger","id":8,"width":32,"signed":true,"mayBeNull":false,"code":"INT32"},
            "function":{"class":"DBSPPathExpression","id":2,"path":{"class":"DBSPPath","id":3,
            "components":[{"class":"DBSPSimplePathSegment","id":4,"identifier":"now","genericArgs":[]}]}}
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let clock = edit.expression(roots[0])?;
        let key = edit.named_variable(edit.expression_type(clock)?, "key")?;
        let boolean = edit.bool_type(key.into(), false)?;
        let compare = |edit: &mut CircuitEditor, op, a, b| {
            edit.binary_expression(key.into(), boolean, op, a, b)
        };
        let direct = compare(&mut edit, BinaryOp::Gte, key, clock)?;
        let swapped = compare(&mut edit, BinaryOp::Lte, clock, key)?;
        let monotone = HashSet::from([clock]);
        for id in [direct, swapped] {
            assert_eq!(
                classify_binary(&edit, id, &monotone)?,
                TemporalPredicate::Bound {
                    key,
                    clock,
                    comparison: WindowComparison::GreaterEqual
                }
            );
            assert_eq!(
                classify_binary(&edit, id, &HashSet::new())?,
                TemporalPredicate::Residual(id)
            );
        }
        let both = compare(&mut edit, BinaryOp::Gt, clock, clock)?;
        assert_eq!(
            classify_binary(&edit, both, &monotone)?,
            TemporalPredicate::Residual(both)
        );
        let ordinary = compare(&mut edit, BinaryOp::Eq, key, key)?;
        assert_eq!(
            classify_binary(&edit, ordinary, &monotone)?,
            TemporalPredicate::NoNow(ordinary)
        );

        // The scalar predicate ends the prefix. Preserve the following AND as
        // one residual, even though its individual comparisons are window bounds.
        let scalar = edit.named_variable(boolean, "flag")?;
        let bounds = compare(&mut edit, BinaryOp::And, direct, swapped)?;
        let interrupted = compare(&mut edit, BinaryOp::And, scalar, bounds)?;
        let body = compare(&mut edit, BinaryOp::And, direct, interrupted)?;
        assert_eq!(
            classify_temporal_predicates(&mut edit, body, &monotone)?,
            [
                TemporalPredicate::Bound {
                    key,
                    clock,
                    comparison: WindowComparison::GreaterEqual
                },
                TemporalPredicate::NoNow(scalar),
                TemporalPredicate::Residual(bounds),
            ]
        );
        Ok(())
    }
}
