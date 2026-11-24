//! Select the smallest expensive expressions that can move between sibling map closures.
use crate::fanout::expensive;
use crate::function_calls::contains_now;
use crate::index_key_equivalence::{KeyComparisonError, check_bodies};
use crate::lexical_scope::{closed_over, expression_children, resolve};
use feldera_sql_expr::{SimplifyWarning, simplify_closure_with_warnings};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, InnerKind, Node, NodeId,
};
use std::collections::HashSet;

/// Find the shareable fragments in the simplified map, keeping their identities
/// in that closure. Simplification can emit warnings even for a rejected map.
pub fn map_fusion_candidate(
    edit: &mut CircuitEditor,
    closure: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<(NodeId, Vec<u64>)>, Error> {
    if contains_now(edit.circuit(), &[closure])? || !expensive(edit.circuit(), closure)? {
        return Ok(None);
    }
    let closure = simplify_closure_with_warnings(edit, closure, Some(warnings))?;
    let body = edit.node(closure)?.edge("body")?;
    if edit.expr_kind(edit.expression(body)?)? != Expr::Tuple {
        return Ok(None);
    }
    let fragments = map_fusion_fragments(edit.circuit(), closure)?;
    Ok((!fragments.is_empty()).then_some((closure, fragments)))
}

/// A map without a sibling to fuse is still profitable if it computes an
/// expensive output twice. Compare fields in the closure's parameter scope.
pub fn has_duplicate_expensive_field(
    edit: &CircuitEditor,
    closure: NodeId,
) -> Result<Option<bool>, KeyComparisonError> {
    let function = edit.node(closure)?;
    let [parameter] = function.edges("parameters")?[..] else {
        return Err(Error::new("map fusion expects one closure parameter").into());
    };
    let parameter = edit.node(parameter)?;
    let fields = edit.node(function.edge("body")?)?.edges("fields")?;
    for (index, &field) in fields.iter().enumerate() {
        if !expensive(edit.circuit(), field)? {
            continue;
        }
        for &previous in &fields[..index] {
            match check_bodies(edit, previous, field, Some((parameter, parameter)))? {
                Some(false) => {}
                answer => return Ok(answer),
            }
        }
    }
    Ok(Some(false))
}

/// Visit a shared expression once per incoming edge, in operand order. A let
/// with only open expensive children can still be a closed fragment itself.
pub fn map_fusion_fragments(circuit: &Circuit, closure: NodeId) -> Result<Vec<u64>, Error> {
    let function = circuit
        .node(closure)
        .ok_or_else(|| Error::new("missing map closure"))?;
    let [parameter] = function
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("map fusion expects one closure parameter"))?;

    let references = resolve(circuit, closure)?;
    enum Visit {
        Enter(NodeId),
        Leave { id: NodeId, start: usize },
    }
    let mut pending = vec![Visit::Enter(function.edge("body")?)];
    let mut visiting = HashSet::new();
    let mut fragments = Vec::new();
    while let Some(visit) = pending.pop() {
        match visit {
            Visit::Enter(id) => {
                let node = circuit.node(id).expect("validated inner edge");
                match node.inner_kind() {
                    Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure | Expr::For)) => {
                        continue;
                    }
                    Some(InnerKind::Expression(_)) if !expensive(circuit, id)? => continue,
                    _ => {}
                }
                if !visiting.insert(id) {
                    return Err(Error::new("cycle in a map expression"));
                }
                pending.push(Visit::Leave {
                    id,
                    start: fragments.len(),
                });
                pending.extend(
                    expression_children(node)
                        .into_iter()
                        .rev()
                        .map(Visit::Enter),
                );
            }
            Visit::Leave { id, start } => {
                visiting.remove(&id);
                if fragments.len() == start
                    && matches!(
                        circuit.node(id).and_then(Node::inner_kind),
                        Some(InnerKind::Expression(_))
                    )
                    && closed_over(circuit, id, Some(parameter), &references)?
                {
                    let NodeId::Inner(number) = id else {
                        unreachable!("map expression")
                    };
                    fragments.push(number);
                }
            }
        }
    }
    Ok(fragments)
}
