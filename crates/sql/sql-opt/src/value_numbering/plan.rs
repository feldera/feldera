//! Choose each lazy cell's first read and insertion point in expression visitor order.
use super::ValueGroup;
use super::collect::CollectedNumbers;
use crate::binding_placement::{BindingScope, PendingBinding};
use crate::lexical_scope::expression_children;
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, NodeId};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Default, Serialize)]
pub struct RewritePlan {
    reads: HashMap<u64, CellRead>,
    // Scope -> declaration after which cells are inserted -> first reads in evaluation order.
    insertions: HashMap<u64, HashMap<u64, Vec<u64>>>,
}

#[derive(Serialize)]
struct CellRead {
    first: u64,
    borrowed: bool,
}

#[derive(Clone, Copy, Default)]
struct Context {
    closure: Option<u64>,
    parent: Option<NodeId>,
}

fn id(node: NodeId) -> u64 {
    match node {
        NodeId::Inner(id) => id,
        NodeId::Outer(_) => unreachable!("expression traversal only visits inner nodes"),
    }
}

impl CollectedNumbers {
    /// An unknown write disables the entire rewrite, including tree normalization.
    pub fn plan(&self, circuit: &Circuit, root: NodeId) -> Result<Option<RewritePlan>, Error> {
        if self.numbers.unknown_write {
            return Ok(None);
        }
        let mut plan = RewritePlan::default();
        if self.numbers.groups.is_empty() {
            return Ok(Some(plan));
        }
        let groups: HashMap<_, &ValueGroup> = self
            .numbers
            .groups
            .iter()
            .flat_map(|group| group.occurrences.iter().map(move |&node| (node, group)))
            .collect();
        let mut cells = HashMap::new();
        let mut pending = Vec::new();
        let mut visits = vec![(root, Context::default(), false)];
        while let Some((node_id, context, postorder)) = visits.pop() {
            let node = circuit
                .node(node_id)
                .ok_or_else(|| Error::new("missing CSE rewrite node"))?;
            let kind = match node.inner_kind() {
                Some(InnerKind::Type(_)) => continue,
                Some(InnerKind::Expression(kind)) => Some(*kind),
                _ => None,
            };
            let current = id(node_id);
            if !postorder {
                visits.push((node_id, context, true));
                // The translator handles LET in preorder. Both children see the enclosing
                // visitor context and the binder itself is retained by identity.
                let (children, nested) = if kind == Some(Expr::Let) {
                    (
                        vec![node.edge("initializer")?, node.edge("consumer")?],
                        context,
                    )
                } else {
                    let closure = if kind == Some(Expr::Closure) {
                        Some(current)
                    } else {
                        context.closure
                    };
                    (
                        expression_children(node),
                        Context {
                            closure,
                            parent: Some(node_id),
                        },
                    )
                };
                visits.extend(
                    children
                        .into_iter()
                        .rev()
                        .map(|child| (child, nested, false)),
                );
                continue;
            }
            if !pending.is_empty() {
                let declarations = match kind {
                    Some(Expr::Closure) => node.edges("parameters")?,
                    Some(Expr::Let) => vec![node_id],
                    Some(Expr::Block) => node
                        .edges("contents")?
                        .into_iter()
                        .filter(|&s| {
                            circuit
                                .node(s)
                                .is_some_and(|n| n.class() == "DBSPLetStatement")
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                let closure = (kind == Some(Expr::Closure)).then_some(current);
                if closure.is_some() || !declarations.is_empty() {
                    let scope = BindingScope {
                        declarations: declarations.into_iter().map(id).collect(),
                        closure,
                        outermost: context.parent.is_none(),
                    };
                    let positions = scope.positions(pending.iter().map(|(_, binding)| binding));
                    let mut positions = positions.into_iter();
                    pending.retain(|(first, _)| {
                        if let Some(position) = positions.next().expect("one placement per binding")
                        {
                            let anchor = scope
                                .closure
                                .unwrap_or_else(|| scope.declarations[position]);
                            plan.insertions
                                .entry(current)
                                .or_default()
                                .entry(anchor)
                                .or_default()
                                .push(*first);
                            false
                        } else {
                            true
                        }
                    });
                }
            }
            let (Some(owner), Some(group)) = (context.closure, groups.get(&current)) else {
                continue;
            };
            if !group.shared.contains(&owner) {
                continue;
            }
            let first = *cells
                .entry((owner, group.representative))
                .or_insert_with(|| {
                    pending.push((
                        current,
                        PendingBinding {
                            owner,
                            dependencies: group.declarations.iter().copied().collect(),
                        },
                    ));
                    current
                });
            let borrowed = context
                .parent
                .and_then(|p| circuit.node(p))
                .is_some_and(|parent| {
                    matches!(
                        parent.inner_kind(),
                        Some(InnerKind::Expression(Expr::Borrow))
                    ) && parent.data("mut").and_then(|value| value.as_bool()) == Some(false)
                });
            plan.reads.insert(current, CellRead { first, borrowed });
        }
        if !pending.is_empty() {
            return Err(Error::new("unplaced common subexpression bindings"));
        }
        Ok(Some(plan))
    }
}
