//! Rebuild a filter's conjunction in the order chosen for its temporal windows.
use super::temporal::{TemporalBound, temporal_filter_order};
use crate::filters::{conjuncts, is_boolean_wrapper, rewrite_filters, strip_wrap_bool};
use crate::projection_key::{ProjectionKey, row_parameter};
use crate::{
    function_calls::contains_now,
    temporal_comparisons::{AnalyzedConjunct, analyze_conjuncts},
};
use feldera_sql_expr::ensure_expression_tree;
use feldera_sql_ir::{
    BinaryOp, BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr,
    Field, InnerKind, NodeId, OperatorKind, OutputPort,
};
use serde::Deserialize;
use std::{collections::HashMap, hash::Hash};

/// The timestamp classification belongs to each original comparison. An implicit
/// cast around a rebuilt subtree is an opaque conjunct in subsequent partitions.
pub fn reorder_temporal_predicate<K: Eq + Hash>(
    edit: &mut CircuitEditor,
    body: ExpressionId,
    bounds: &[Option<TemporalBound<K>>],
    chosen: Option<usize>,
) -> Result<ExpressionId, Error> {
    reorder_partitions(edit, body, bounds, chosen, |_, _| Ok(()))
}

fn reorder_partitions<K: Eq + Hash>(
    edit: &mut CircuitEditor,
    mut body: ExpressionId,
    bounds: &[Option<TemporalBound<K>>],
    chosen: Option<usize>,
    mut visit: impl FnMut(&mut CircuitEditor, &[ExpressionId]) -> Result<(), Error>,
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
    for partition in temporal_filter_order(bounds, shared) {
        let terms = conjuncts(edit, body)?;
        if terms.len() < 2 {
            continue;
        }
        visit(edit, &terms)?;
        let mut ordered = terms.clone();
        ordered.sort_by_key(|term| {
            let selected = keys
                .get(term)
                .is_some_and(|key| partition.timestamps.contains(key));
            selected != partition.first
        });
        if ordered == terms {
            continue;
        }
        let mut nullable = false;
        for &term in &ordered {
            nullable |= edit.nullable(edit.expression_type(term)?)?;
        }
        let origin = ordered[0].into();
        let ty = edit.bool_type(origin, nullable)?;
        let predicate = edit.binary_expressions(origin, ty, BinaryOp::And, &ordered)?;
        body = if is_boolean_wrapper(edit, body)? {
            let wrapper = edit.derive_inner(
                body.into(),
                [("source", Field::Reference(predicate.into()))],
            )?;
            edit.expression(wrapper)?
        } else {
            predicate
        };
    }
    Ok(body)
}

/// Classification of one filter in original conjunct order. The Java monotonicity
/// analysis can allocate temporary names even when the filter stays in place.
#[derive(Deserialize)]
pub struct TemporalOrder<K = String> {
    bounds: Vec<Option<TemporalBound<K>>>,
    chosen: Option<usize>,
    classification_names: u64,
}

/// Classify against the original row. Even when two windows share a source
/// after map composition, Reorder compares the keys *before* composition.
pub fn reorder_shared_windows(
    circuit: Circuit,
    chosen: &HashMap<u64, usize>,
    next: &mut u64,
) -> Result<Option<Circuit>, Error> {
    let mut edit = CircuitEditor::new(circuit.clone());
    let mut orders = HashMap::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let filter = edit.node(id)?;
        if filter.operator_kind() != Some(OperatorKind::Filter) {
            continue;
        }
        let Some(function) = filter.field("function").and_then(Field::node_id) else {
            continue;
        };
        let closure = edit.node(function)?;
        if !matches!(
            closure.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) || closure.edges("parameters")?.len() != 1
        {
            continue;
        }
        if !contains_now(edit.circuit(), &[function])? {
            continue;
        }
        let tree = ensure_expression_tree(&mut edit, function)?;
        let condition = edit.node(tree)?;
        let parameter = condition.edges("parameters")?[0];
        let body = edit.expression(condition.edge("body")?)?;
        let Some(terms) = analyze_conjuncts(&mut edit, parameter, body)? else {
            return Ok(None);
        };
        let has_names = terms.iter().any(|term| term.names != 0);
        if !has_names && (terms.len() < 2 || terms.iter().all(|term| term.window.is_none())) {
            continue;
        }
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected a filter operator"));
        };
        let chosen = chosen.get(&number).copied();
        let mut bounds = Vec::with_capacity(terms.len());
        let row = row_parameter(&edit, parameter)?;
        for term in &terms {
            bounds.push(match &term.window {
                Some(window) => {
                    let Some(fingerprint) = ProjectionKey::read(&edit, window.key, row)? else {
                        return Ok(None);
                    };
                    Some(TemporalBound {
                        fingerprint,
                        lower: window.lower,
                    })
                }
                None => None,
            });
        }
        let Some(classification_names) =
            classification_names(&mut edit, parameter, body, &terms, &bounds, chosen)?
        else {
            return Ok(None);
        };
        orders.insert(
            number,
            TemporalOrder {
                bounds,
                chosen,
                classification_names,
            },
        );
    }
    drop(edit);
    if orders.is_empty() {
        return Ok(Some(circuit));
    }
    apply_orders(circuit, orders, next).map(Some)
}

/// Account for the repeated classifications in Reorder.reorderWithJava.
fn classification_names(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    body: ExpressionId,
    terms: &[AnalyzedConjunct],
    bounds: &[Option<TemporalBound<ProjectionKey>>],
    chosen: Option<usize>,
) -> Result<Option<u64>, Error> {
    if terms.iter().all(|term| term.names == 0) {
        return Ok(Some(0));
    }
    let mut classification_names = 0;
    // The shared fingerprint is read twice. Checking for a lower bound
    // visits each original conjunct once, and each window a second time.
    if let Some(index) = chosen {
        let term = terms
            .get(index)
            .ok_or_else(|| Error::new("shared conjunct is out of range"))?;
        classification_names += 2 * term.names;
    }
    let mut costs = HashMap::new();
    for term in terms {
        costs.insert(term.expression, term.names);
        classification_names += if term.window.is_some() {
            2 * term.names
        } else {
            term.names
        };
    }
    // Stable partitioning classifies its current operands. A nullable
    // cast around a rebuilt AND can make a later partition see a new term.
    let mut resolved = true;
    reorder_partitions(edit, body, bounds, chosen, |edit, operands| {
        for operand in operands {
            if let Some(cost) = costs.get(operand) {
                classification_names += cost;
            } else {
                let term = strip_wrap_bool(edit, *operand)?;
                let mut names = 0;
                if crate::infer_temporal_predicates(edit, parameter, term, &mut names)?.is_none() {
                    resolved = false;
                    break;
                }
                costs.insert(*operand, names);
                classification_names += names;
            }
        }
        Ok(())
    })?;
    if !resolved {
        return Ok(None);
    }
    Ok(Some(classification_names))
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
        let output_type = filter.edge("outputType")?;
        let function = filter.edge("function")?;
        let input = filter
            .inputs()
            .next()
            .ok_or_else(|| Error::new("temporal filter requires an input"))?;
        let tree = ensure_expression_tree(edit, function)?;
        let body = edit.expression(edit.node(tree)?.edge("body")?)?;
        let ordered = reorder_temporal_predicate(edit, body, &order.bounds, order.chosen)?;
        if ordered == body {
            return Ok(None);
        }
        let parameters = edit.node(tree)?.edges("parameters")?;
        let closure = edit.closure_with_parameters(ordered, &parameters)?;
        let replacement = edit.append_operator(
            parent,
            id,
            OperatorKind::Filter,
            &[input],
            [
                ("function", Field::Reference(closure.into())),
                ("outputType", Field::Reference(output_type)),
            ],
        )?;
        Ok(Some((
            replacement,
            OutputPort {
                operator: replacement,
                number: 0,
            },
        )))
    })?;
    if !orders.is_empty() {
        return Err(Error::new("temporal order refers to a missing filter"));
    }
    Ok(result)
}
