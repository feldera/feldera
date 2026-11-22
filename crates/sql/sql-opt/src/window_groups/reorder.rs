//! Rebuild a filter's conjunction in the order chosen for its temporal windows.
use super::temporal::{temporal_filter_order, TemporalBound};
use crate::filters::{conjuncts, is_boolean_wrapper, rewrite_filters};
use feldera_sql_expr::ensure_expression_tree;
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId,
    OperatorKind, OutputPort,
};
use serde::Deserialize;
use std::collections::HashMap;

/// The timestamp classification belongs to each original comparison. An implicit
/// cast around a rebuilt subtree is an opaque conjunct in subsequent partitions.
pub fn reorder_temporal_predicate(
    edit: &mut CircuitEditor,
    mut body: ExpressionId,
    bounds: &[Option<TemporalBound>],
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
        .map(|bound| bound.fingerprint.as_str());
    let keys: HashMap<_, _> = terms
        .into_iter()
        .zip(bounds)
        .filter_map(|(id, bound)| Some((id, bound.as_ref()?.fingerprint.as_str())))
        .collect();
    for partition in temporal_filter_order(bounds, shared) {
        let terms = conjuncts(edit, body)?;
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
pub struct TemporalOrder {
    bounds: Vec<Option<TemporalBound>>,
    chosen: Option<usize>,
    classification_names: u64,
}

pub fn reorder_temporal_filters(
    circuit: Circuit,
    mut orders: HashMap<u64, TemporalOrder>,
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
