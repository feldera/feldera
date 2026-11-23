//! Choose each lazy cell's first read and insertion point in expression visitor order.
use super::collect::CollectedNumbers;
use super::{inner, ValueGroup};
use crate::binding_placement::{BindingScope, PendingBinding};
use crate::lexical_scope::expression_children;
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Default, Serialize)]
pub struct RewritePlan {
    pub(super) reads: HashMap<u64, CellRead>,
    // Scope -> declaration after which cells are inserted -> first reads in evaluation order.
    pub(super) insertions: HashMap<u64, HashMap<u64, Vec<u64>>>,
}

#[derive(Serialize)]
pub(super) struct CellRead {
    pub(super) first: u64,
    pub(super) borrowed: bool,
}

#[derive(Clone, Copy, Default)]
struct Context {
    closure: Option<u64>,
    parent: Option<NodeId>,
}

fn binding_scope(
    circuit: &Circuit,
    node_id: NodeId,
    node: &Node,
    context: Context,
) -> Result<Option<BindingScope>, Error> {
    let (closure, declarations) = match node.inner_kind() {
        Some(InnerKind::Expression(Expr::Closure)) => {
            (Some(inner(node_id)), node.edges("parameters")?)
        }
        Some(InnerKind::Expression(Expr::Let)) => (None, vec![node_id]),
        Some(InnerKind::Expression(Expr::Block)) => (
            None,
            node.edges("contents")?
                .into_iter()
                .filter(|&id| {
                    circuit
                        .node(id)
                        .is_some_and(|statement| statement.class() == "DBSPLetStatement")
                })
                .collect(),
        ),
        _ => return Ok(None),
    };
    Ok(Some(BindingScope {
        declarations: declarations.into_iter().map(inner).collect(),
        closure,
        outermost: context.parent.is_none(),
    }))
}

impl RewritePlan {
    fn place_bindings(
        &mut self,
        node_id: u64,
        scope: BindingScope,
        pending: &mut Vec<(u64, PendingBinding)>,
    ) {
        let positions = scope.positions(pending.iter().map(|(_, binding)| binding));
        for ((first, binding), position) in std::mem::take(pending).into_iter().zip(positions) {
            match position {
                Some(index) => {
                    let anchor = scope.closure.unwrap_or_else(|| scope.declarations[index]);
                    self.insertions
                        .entry(node_id)
                        .or_default()
                        .entry(anchor)
                        .or_default()
                        .push(first);
                }
                None => pending.push((first, binding)),
            }
        }
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
            let current = inner(node_id);
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
                if let Some(scope) = binding_scope(circuit, node_id, node, context)? {
                    plan.place_bindings(current, scope, &mut pending);
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
