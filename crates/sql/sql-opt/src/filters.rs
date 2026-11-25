//! Replace filters with operator groups while preserving circuit and generated-name order.
use crate::{rebuild::ProjectionNames, stream_rewrites::body_members};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, NodeId,
    OperatorKind as Op, OutputPort,
};
use std::collections::{BTreeMap, HashMap};

pub(super) fn is_boolean_wrapper(edit: &CircuitEditor, expr: ExpressionId) -> Result<bool, Error> {
    Ok(edit.expr_kind(expr)? == ExpressionKind::Unary
        && edit
            .node(expr.into())?
            .data("opcode")
            .and_then(|v| v.as_str())
            == Some("WRAP_BOOL"))
}

pub(super) fn strip_wrap_bool(
    edit: &CircuitEditor,
    mut expr: ExpressionId,
) -> Result<ExpressionId, Error> {
    while is_boolean_wrapper(edit, expr)? {
        expr = edit.expression(edit.node(expr.into())?.edge("source")?)?;
    }
    Ok(expr)
}

pub(super) fn conjuncts(
    edit: &CircuitEditor,
    body: ExpressionId,
) -> Result<Vec<ExpressionId>, Error> {
    let mut pending = vec![strip_wrap_bool(edit, body)?];
    let mut terms = Vec::new();
    while let Some(term) = pending.pop() {
        let node = edit.node(term.into())?;
        if edit.expr_kind(term)? == ExpressionKind::Binary
            && node.data("opcode").and_then(|v| v.as_str()) == Some("AND")
        {
            pending.push(edit.expression(node.edge("right")?)?);
            pending.push(edit.expression(node.edge("left")?)?);
        } else {
            terms.push(term);
        }
    }
    Ok(terms)
}

pub(super) fn rewrite_filters<E: From<Error>>(
    circuit: Circuit,
    next: &mut u64,
    mut rewrite: impl FnMut(
        &mut CircuitEditor,
        NodeId,
        NodeId,
        &mut u64,
        &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(NodeId, OutputPort)>, E>,
) -> Result<(Circuit, Vec<SimplifyWarning>), E> {
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut placements = Vec::new();
    let mut warnings = Vec::new();
    let mut names = *next;
    let mut projections = ProjectionNames::default();
    for id in order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if node.operator_kind() == Some(Op::Filter) {
            let body = bodies[&id];
            if let Some((first, output)) = rewrite(&mut edit, body, id, &mut names, &mut warnings)?
            {
                placements.push((body, first, id));
                replacements.insert(id, output);
                projections.replaced(id);
            }
        }
    }
    // Keep original provenance definitions ahead of new references while building
    // expressions. Each placement moves the remaining suffix of its body.
    for (body, first, original) in placements.into_iter().rev() {
        edit.place_before(body, first, original)?;
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, warnings))
}
