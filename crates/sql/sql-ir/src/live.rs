//! Encode a changed circuit for the JVM that supplied its input. Live inner
//! objects can be shared across passes, including closure parameters and types.
use crate::{Circuit, Field, NodeId};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// A definition can move to an earlier reader without changing the object.
/// Inner objects with a reference to an outer operator require a full replay.
fn same_value(a: &Field, b: &Field) -> bool {
    match (a, b) {
        (Field::Array(a), Field::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_value(a, b))
        }
        (Field::Object(a), Field::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, a)| b.get(k).is_some_and(|b| same_value(a, b)))
        }
        (Field::Data(a), Field::Data(b)) => a == b,
        _ => matches!(a.node_id(), Some(NodeId::Inner(_))) && a.node_id() == b.node_id(),
    }
}

impl Circuit {
    /// Return an in-process reply. A standalone checkpoint still needs the
    /// definitions of every object reachable from its root.
    pub fn changed_live_graph(&self, previous: &Self) -> Option<Value> {
        if std::sync::Arc::ptr_eq(&self.nodes.0, &previous.nodes.0) {
            return None;
        }
        // Check every surviving input object before retaining any of them.
        // Checking just the root of a shared subtree would miss a changed
        // descendant that kept its number.
        let can_share = self.nodes.range(NodeId::Inner(0)..).all(|(id, node)| {
            previous.node(*id).is_none_or(|old| {
                old.class() == node.class()
                    && old.fields.len() == node.fields.len()
                    && node
                        .fields
                        .iter()
                        .all(|(k, v)| old.field(k).is_some_and(|o| same_value(v, o)))
            })
        });
        if !can_share {
            return self.changed_graph(previous);
        }
        // Unused expressions allocated while testing a rewrite are invisible
        // to the next pass. The original wire root reaches each input definition.
        if self.root == previous.root
            && previous.nodes.iter().all(|(id, old)| {
                self.node(*id)
                    .is_some_and(|new| old.class() == new.class() && old.fields == new.fields)
            })
        {
            return None;
        }
        let mut retained = BTreeSet::new();
        let graph = crate::wire::encode_with(self.root, &self.nodes, &mut |id| {
            if let NodeId::Inner(n) = id {
                if previous.node(id).is_some() {
                    retained.insert(n);
                    return true;
                }
            }
            false
        });
        Some(json!({"graph":graph,"retained_inner":retained}))
    }
}
