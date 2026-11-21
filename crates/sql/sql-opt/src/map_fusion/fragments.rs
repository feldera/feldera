//! Select the smallest expensive expressions that can move between sibling map closures.
use super::expression_children;
use super::references::{resolve, References};
use crate::fanout::expensive;
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId};
use std::collections::HashSet;

/// A fragment may use the map parameter and any declaration inside the fragment.
fn closed(
    circuit: &Circuit,
    root: NodeId,
    parameter: NodeId,
    references: &References,
) -> Result<bool, Error> {
    let mut inside = HashSet::new();
    let mut required = Vec::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !inside.insert(id) {
            continue;
        }
        let node = circuit.node(id).expect("validated inner edge");
        if matches!(
            node.inner_kind(),
            Some(InnerKind::Expression(Expr::VariablePath))
        ) {
            let declaration = references
                .get(&id)
                .ok_or_else(|| Error::new("unresolved map variable"))?;
            required.push(*declaration);
        }
        pending.extend(expression_children(node));
    }
    Ok(required
        .into_iter()
        .all(|id| id == parameter || inside.contains(&id)))
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
                        continue
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
                    && closed(circuit, id, parameter, &references)?
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
