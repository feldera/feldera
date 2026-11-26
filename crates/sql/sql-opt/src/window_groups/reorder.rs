//! Rebuild a filter's conjunction in the order chosen for its temporal windows.
use super::temporal::{temporal_filter_order, TemporalBound};
use crate::filters::{conjuncts, is_boolean_wrapper, rewrite_filters};
use crate::projection_key::{row_parameter, ProjectionKey};
use crate::{function_calls::contains_now, ClockError, ConjunctWindow, TemporalDiagnostic};
use feldera_sql_expr::ensure_expression_tree;
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, NodeId, OperatorKind, OutputPort,
};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    rc::Rc,
};

/// The timestamp classification belongs to each original comparison. An implicit
/// cast around a rebuilt subtree is an opaque conjunct in subsequent partitions.
pub fn reorder_temporal_predicate<K: Eq + Hash>(
    edit: &mut CircuitEditor,
    mut body: ExpressionId,
    bounds: &[Option<TemporalBound<K>>],
    chosen: Option<usize>,
) -> Result<ExpressionId, Error> {
    let terms = conjuncts(edit, body)?;
    if terms.len() != bounds.len() {
        return Err(Error::new(
            "expected one temporal classification per conjunct",
        ));
    }
    let shared = chosen
        .map(|i| {
            bounds
                .get(i)
                .ok_or_else(|| Error::new("shared conjunct is out of range"))
        })
        .transpose()?
        .and_then(Option::as_ref)
        .map(|bound| &bound.fingerprint);
    let keys: HashMap<_, _> = terms
        .into_iter()
        .zip(bounds)
        .filter_map(|(id, bound)| Some((id, &bound.as_ref()?.fingerprint)))
        .collect();
    for group in temporal_filter_order(bounds, shared) {
        body = partition(edit, body, group.first, |_, term| {
            Ok::<_, Error>(
                keys.get(&term)
                    .is_some_and(|key| group.timestamps.contains(key)),
            )
        })?;
    }
    Ok(body)
}

/// Evaluate every operand once in source order before rebuilding the conjunction.
fn partition<E: From<Error>>(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    first: bool,
    mut selected: impl FnMut(&mut CircuitEditor, ExpressionId) -> Result<bool, E>,
) -> Result<ExpressionId, E> {
    let terms = conjuncts(edit, body)?;
    if terms.len() < 2 {
        return Ok(body);
    }
    let mut leading = Vec::new();
    let mut trailing = Vec::new();
    for &term in &terms {
        if selected(edit, term)? == first {
            leading.push(term);
        } else {
            trailing.push(term);
        }
    }
    leading.extend(trailing);
    if leading == terms {
        return Ok(body);
    }
    let mut nullable = false;
    for &term in &leading {
        nullable |= edit.nullable(edit.expression_type(term)?)?;
    }
    let origin = leading[0].into();
    let ty = edit.bool_type(origin, nullable)?;
    let predicate = edit.binary_expressions(origin, ty, BinaryOp::And, &leading)?;
    if is_boolean_wrapper(edit, body)? {
        let wrapper = edit.derive_inner(
            body.into(),
            [("source", Field::Reference(predicate.into()))],
        )?;
        Ok(edit.expression(wrapper)?)
    } else {
        Ok(predicate)
    }
}

/// Classification of one filter in original conjunct order. The Java monotonicity
/// analysis can allocate temporary names even when the filter stays in place.
#[derive(Deserialize)]
pub struct TemporalOrder<K = String> {
    bounds: Vec<Option<TemporalBound<K>>>,
    chosen: Option<usize>,
    classification_names: u64,
}

pub enum ReorderError {
    Unsupported,
    Diagnostic {
        graph: Box<Circuit>,
        diagnostic: TemporalDiagnostic,
        next_name: u64,
    },
    InvalidIr(Error),
}

impl From<Error> for ReorderError {
    fn from(error: Error) -> Self {
        Self::InvalidIr(error)
    }
}

#[derive(Clone, Copy)]
struct WindowKey {
    row: NodeId,
    expression: ExpressionId,
    lower: bool,
}

/// A pass can classify the same term several times. Reuse its result and
/// advance the name counter on each visit, including visits before a failure.
struct WindowBounds {
    parameter: NodeId,
    cache: HashMap<ExpressionId, (Option<WindowKey>, u64)>,
    fingerprints: HashMap<ExpressionId, Rc<ProjectionKey>>,
}

impl WindowBounds {
    fn classify(
        &mut self,
        edit: &mut CircuitEditor,
        term: ExpressionId,
        next: &mut u64,
    ) -> Result<Option<WindowKey>, ReorderError> {
        if let Some((bound, cost)) = self.cache.get(&term) {
            *next = next
                .checked_add(*cost)
                .ok_or_else(|| Error::new("generated names exhausted"))?;
            return Ok(*bound);
        }
        let start = *next;
        let (row, predicates) = match super::checked_conjunct(edit, self.parameter, term, next) {
            Ok(Some(parts)) => parts,
            Ok(None) => return Err(ReorderError::Unsupported),
            Err(ClockError::InvalidIr(error)) => return Err(error.into()),
            Err(ClockError::Diagnostic(diagnostic)) => {
                return Err(ReorderError::Diagnostic {
                    graph: Box::new(edit.circuit().clone()),
                    diagnostic,
                    next_name: *next,
                });
            }
        };
        let bound = ConjunctWindow::from_predicates(&predicates).map(|window| WindowKey {
            row,
            expression: window.key,
            lower: window.lower,
        });
        self.cache.insert(term, (bound, *next - start));
        Ok(bound)
    }

    fn bound(
        &mut self,
        edit: &mut CircuitEditor,
        term: ExpressionId,
        next: &mut u64,
    ) -> Result<Option<TemporalBound<Rc<ProjectionKey>>>, ReorderError> {
        let Some(window) = self.classify(edit, term, next)? else {
            return Ok(None);
        };
        let fingerprint = match self.fingerprints.entry(term) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let row = row_parameter(edit, window.row)?;
                let key = ProjectionKey::read(edit, window.expression, row)?
                    .ok_or(ReorderError::Unsupported)?;
                entry.insert(Rc::new(key))
            }
        };
        Ok(Some(TemporalBound {
            fingerprint: Rc::clone(fingerprint),
            lower: window.lower,
        }))
    }

    fn reorder(
        &mut self,
        edit: &mut CircuitEditor,
        body: ExpressionId,
        chosen: Option<usize>,
        next: &mut u64,
    ) -> Result<ExpressionId, ReorderError> {
        let terms = conjuncts(edit, body)?;
        let (mut ordered, shared) = if let Some(i) = chosen {
            let term = *terms
                .get(i)
                .ok_or_else(|| Error::new("shared conjunct is out of range"))?;
            let shared = self.bound(edit, term, next)?.map(|bound| bound.fingerprint);
            self.bound(edit, term, next)?;
            let trailing = partition(edit, body, false, |edit, term| {
                Ok::<_, ReorderError>(
                    self.bound(edit, term, next)?
                        .is_some_and(|bound| Some(&bound.fingerprint) != shared.as_ref()),
                )
            })?;
            let ordered = partition(edit, trailing, true, |edit, term| {
                Ok::<_, ReorderError>(
                    self.bound(edit, term, next)?
                        .is_some_and(|bound| Some(&bound.fingerprint) == shared.as_ref()),
                )
            })?;
            (ordered, shared)
        } else {
            // The first partition tests only whether the term defines a window.
            // A later term can fail before a timestamp is ever printed.
            let ordered = partition(edit, body, false, |edit, term| {
                Ok::<_, ReorderError>(self.classify(edit, term, next)?.is_some())
            })?;
            (ordered, None)
        };
        let mut upper_only = HashSet::new();
        let mut lower = HashSet::new();
        for term in terms {
            if let Some(bound) = self.bound(edit, term, next)? {
                // The reference visitor reads the fingerprint and the direction separately.
                self.classify(edit, term, next)?;
                if bound.lower {
                    lower.insert(bound.fingerprint);
                } else {
                    upper_only.insert(bound.fingerprint);
                }
            }
        }
        upper_only.retain(|key| Some(key) != shared.as_ref() && !lower.contains(key));
        if !upper_only.is_empty() {
            ordered = partition(edit, ordered, false, |edit, term| {
                Ok::<_, ReorderError>(
                    self.bound(edit, term, next)?
                        .is_some_and(|bound| upper_only.contains(&bound.fingerprint)),
                )
            })?;
        }
        Ok(ordered)
    }
}

pub fn reorder_shared_windows(
    circuit: Circuit,
    chosen: &HashMap<u64, usize>,
    next: &mut u64,
) -> Result<Circuit, ReorderError> {
    let (result, _) =
        rewrite_filters::<ReorderError>(circuit, next, |edit, parent, id, next, _| {
            let Some(function) = edit.node(id)?.field("function").and_then(Field::node_id) else {
                return Ok(None);
            };
            let closure = edit.node(function)?;
            if !matches!(
                closure.inner_kind(),
                Some(InnerKind::Expression(Expr::Closure))
            ) || closure.edges("parameters")?.len() != 1
                || !contains_now(edit.circuit(), &[function])?
            {
                return Ok(None);
            }
            let NodeId::Outer(number) = id else {
                return Err(Error::new("expected a filter operator").into());
            };
            let tree = ensure_expression_tree(edit, function)?;
            let condition = edit.node(tree)?;
            let parameter = condition.edges("parameters")?[0];
            let body = edit.expression(condition.edge("body")?)?;
            let mut bounds = WindowBounds {
                parameter,
                cache: HashMap::new(),
                fingerprints: HashMap::new(),
            };
            let ordered = bounds.reorder(edit, body, chosen.get(&number).copied(), next)?;
            if ordered == body {
                return Ok(None);
            }
            Ok(Some(replace_filter(edit, parent, id, parameter, ordered)?))
        })?;
    Ok(result)
}

pub fn reorder_temporal_filters(
    circuit: Circuit,
    orders: HashMap<u64, TemporalOrder>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    apply_orders(circuit, orders, next)
}

fn apply_orders<K: Eq + Hash>(
    circuit: Circuit,
    mut orders: HashMap<u64, TemporalOrder<K>>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    let (result, _) = rewrite_filters(circuit, next, |edit, parent, id, next, _| {
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected a filter operator"));
        };
        let Some(order) = orders.remove(&number) else {
            return Ok(None);
        };
        *next = next
            .checked_add(order.classification_names)
            .ok_or_else(|| Error::new("generated names exhausted classifying temporal filters"))?;
        if order.bounds.iter().all(Option::is_none) {
            return Ok(None);
        }
        let filter = edit.node(id)?;
        let function = filter.edge("function")?;
        let tree = ensure_expression_tree(edit, function)?;
        let body = edit.expression(edit.node(tree)?.edge("body")?)?;
        let ordered = reorder_temporal_predicate(edit, body, &order.bounds, order.chosen)?;
        if ordered == body {
            return Ok(None);
        }
        let parameter = edit.node(tree)?.edges("parameters")?[0];
        Ok(Some(replace_filter(edit, parent, id, parameter, ordered)?))
    })?;
    if !orders.is_empty() {
        return Err(Error::new("temporal order refers to a missing filter"));
    }
    Ok(result)
}

fn replace_filter(
    edit: &mut CircuitEditor,
    parent: NodeId,
    filter: NodeId,
    parameter: NodeId,
    predicate: ExpressionId,
) -> Result<(NodeId, OutputPort), Error> {
    let node = edit.node(filter)?;
    let output_type = node.edge("outputType")?;
    let input = node
        .inputs()
        .next()
        .ok_or_else(|| Error::new("temporal filter requires an input"))?;
    let closure = edit.closure_with_parameters(predicate, &[parameter])?;
    let replacement = edit.append_operator(
        parent,
        filter,
        OperatorKind::Filter,
        &[input],
        [
            ("function", Field::Reference(closure.into())),
            ("outputType", Field::Reference(output_type)),
        ],
    )?;
    Ok((
        replacement,
        OutputPort {
            operator: replacement,
            number: 0,
        },
    ))
}
