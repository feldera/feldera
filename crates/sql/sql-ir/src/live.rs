//! Encode a changed circuit for the JVM that supplied its input. Live inner
//! objects can be shared across passes, including closure parameters and types.
use crate::{Circuit, Field, NodeId};
use serde_json::Value;
use std::collections::BTreeSet;
use std::sync::Arc;

/// A surviving inner object must be decoded again from its complete definition.
#[derive(Debug)]
pub struct FullSnapshotRequired;

fn has_outer_edge(field: &Field) -> bool {
    match field {
        Field::Reference(NodeId::Outer(_)) | Field::Definition(NodeId::Outer(_)) => true,
        Field::Object(fields) => fields.values().any(has_outer_edge),
        Field::Array(fields) => fields.iter().any(has_outer_edge),
        _ => false,
    }
}

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
        self.retained_live_graph(previous)
            .unwrap_or_else(|FullSnapshotRequired| self.changed_graph(previous))
    }

    /// Encode edits only if surviving input objects can be borrowed from the JVM.
    /// A caller which supplied only closure signatures must retry with complete
    /// definitions when any of those objects needs reconstruction.
    pub fn retained_live_graph(
        &self,
        previous: &Self,
    ) -> Result<Option<Value>, FullSnapshotRequired> {
        if Arc::ptr_eq(&self.nodes.0, &previous.nodes.0) {
            return Ok(None);
        }
        // Check every surviving input object before retaining any of them.
        // Checking just the root of a shared subtree would miss a changed
        // descendant that kept its number.
        let can_share = self.nodes.range(NodeId::Inner(0)..).all(|(id, node)| {
            previous.node(*id).is_none_or(|old| {
                if Arc::ptr_eq(&old.fields.0, &node.fields.0) {
                    return old.class() == node.class()
                        && !node.fields.values().any(has_outer_edge);
                }
                old.class() == node.class()
                    && old.fields.len() == node.fields.len()
                    && node
                        .fields
                        .iter()
                        .all(|(k, v)| old.field(k).is_some_and(|o| same_value(v, o)))
            })
        });
        if !can_share {
            return Err(FullSnapshotRequired);
        }
        // Unused expressions allocated while testing a rewrite are invisible
        // to the next pass. The original wire root reaches each input definition.
        if self.root == previous.root
            && previous.nodes.iter().all(|(id, old)| {
                self.node(*id)
                    .is_some_and(|new| old.class() == new.class() && old.fields == new.fields)
            })
        {
            return Ok(None);
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
        Ok(Some(Value::Object(
            [
                ("graph".into(), graph),
                ("retained_inner".into(), retained.into_iter().collect()),
            ]
            .into_iter()
            .collect(),
        )))
    }
}

impl Circuit {
    /// Encode analysis expressions against the input snapshot. Every new node
    /// gets one definition across the entire batch, even if multiple roots share it.
    pub fn additional_expressions(
        &self,
        input: &Self,
        roots: &[crate::ExpressionId],
    ) -> Result<Vec<Value>, crate::Error> {
        self.additional_inner_nodes(input, roots.iter().copied().map(NodeId::from))
    }

    /// Encode new expressions or statements, preserving shared nodes across roots.
    pub fn additional_inner_nodes(
        &self,
        input: &Self,
        roots: impl IntoIterator<Item = NodeId>,
    ) -> Result<Vec<Value>, crate::Error> {
        let mut emitted = BTreeSet::new();
        roots
            .into_iter()
            .map(|root| {
                if !matches!(root, NodeId::Inner(_)) {
                    return Err(crate::Error::new("expected an inner node"));
                }
                self.additional_field(&Field::Reference(root), input, &mut emitted, 0)
            })
            .collect()
    }

    /// Encode one live expression. Deep expressions travel as definitions in
    /// dependency order, with the root last. Both forms reuse the JVM's input nodes.
    pub fn live_expression(
        &self,
        input: &Self,
        root: crate::ExpressionId,
    ) -> Result<Value, crate::Error> {
        let root = NodeId::from(root);
        let mut emitted = BTreeSet::new();
        if let Ok(inline) = self.additional_field(&Field::Reference(root), input, &mut emitted, 0) {
            return Ok(inline);
        }

        // The inline encoder bounds document nesting. Emit each definition
        // separately so the same field checks apply to each shallow document.
        let mut order = Vec::new();
        emitted.clear();
        let mut visiting = BTreeSet::new();
        let mut pending = vec![(root, false)];
        while let Some((id, finish)) = pending.pop() {
            if input.node(id).is_some() || emitted.contains(&id) {
                continue;
            }
            if finish {
                visiting.remove(&id);
                emitted.insert(id);
                order.push(id);
                continue;
            }
            if !visiting.insert(id) {
                return Err(crate::Error::new("cycle in a live expression"));
            }
            let node = self
                .node(id)
                .ok_or_else(|| crate::Error::new("missing live expression node"))?;
            pending.push((id, true));
            let mut children = Vec::new();
            for (_, field) in node.fields() {
                field.collect_inner_edges(&mut children);
            }
            pending.extend(children.into_iter().rev().map(|child| (child, false)));
        }
        self.additional_inner_nodes(input, order).map(Value::Array)
    }

    /// Serialize a new operator sequence for a caller that owns its inputs.
    /// Each input must already exist in the input graph or precede its reader in
    /// the sequence. Share newly built inner objects across the entire sequence.
    pub fn additional_operators(
        &self,
        input: &Self,
        operators: &[NodeId],
    ) -> Result<Vec<Value>, crate::Error> {
        let mut emitted = BTreeSet::new();
        let mut result = Vec::with_capacity(operators.len());
        for &id in operators {
            let node = self
                .node(id)
                .filter(|n| n.operator_kind().is_some())
                .ok_or_else(|| crate::Error::new("expected a fragment operator"))?;
            if input.node(id).is_some() || emitted.contains(&id) {
                return Err(crate::Error::new("fragment operator is already defined"));
            }
            let mut object = serde_json::Map::new();
            object.insert("class".into(), node.class().into());
            object.insert("id".into(), id.number().into());
            for (key, field) in node.fields() {
                object.insert(
                    key.into(),
                    self.additional_field(field, input, &mut emitted, 1)?,
                );
            }
            emitted.insert(id);
            result.push(Value::Object(object));
        }
        Ok(result)
    }

    fn additional_field(
        &self,
        field: &Field,
        input: &Self,
        emitted: &mut BTreeSet<NodeId>,
        depth: usize,
    ) -> Result<Value, crate::Error> {
        crate::wire::check_depth(depth)?;
        match field {
            Field::Reference(id) | Field::Definition(id) => {
                if input.node(*id).is_some() || !emitted.insert(*id) {
                    return Ok(serde_json::json!({"node": id.number()}));
                }
                if !matches!(id, NodeId::Inner(_)) {
                    return Err(crate::Error::new(
                        "reference to an outer operator that has not been supplied",
                    ));
                }
                let node = self
                    .node(*id)
                    .ok_or_else(|| crate::Error::new("missing analysis expression"))?;
                let mut object = serde_json::Map::with_capacity(node.fields.len() + 2);
                object.insert("class".into(), node.class().into());
                object.insert("id".into(), id.number().into());
                for (key, value) in node.fields.iter() {
                    object.insert(
                        key.clone(),
                        self.additional_field(value, input, emitted, depth + 1)?,
                    );
                }
                Ok(Value::Object(object))
            }
            Field::Data(value) => Ok(value.clone()),
            Field::Array(values) => values
                .iter()
                .map(|v| self.additional_field(v, input, emitted, depth + 1))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            Field::Object(fields) => fields
                .iter()
                .map(|(k, v)| {
                    Ok((
                        k.clone(),
                        self.additional_field(v, input, emitted, depth + 1)?,
                    ))
                })
                .collect::<Result<_, _>>()
                .map(Value::Object),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CircuitEditor, OperatorKind, OutputPort};
    use serde_json::json;

    #[test]
    fn operator_fragment_requires_each_input_before_its_reader() -> Result<(), crate::Error> {
        let base = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPNowOperator","id":1,"inputs":[]}
            ]
        }))?;
        let mut edit = CircuitEditor::new(base.clone());
        let clock = NodeId::Outer(1);
        let mut append = |kind, producer| edit.append_operator(
            base.root_id(), clock, kind, &[OutputPort { operator: producer, number: 0 }], [],
        );
        let changes = append(OperatorKind::Differentiate, clock)?;
        let values = append(OperatorKind::Integrate, changes)?;
        let graph = edit.circuit();
        let wire = graph.additional_operators(&base, &[changes, values])?;
        assert_eq!(wire[0]["inputs"][0]["operator"], json!({"node":1}));
        assert_eq!(wire[1]["inputs"][0]["operator"]["node"], wire[0]["id"]);
        assert!(graph.additional_operators(&base, &[values, changes]).is_err());
        assert!(graph.additional_operators(&base, &[changes, changes]).is_err());
        assert!(graph.additional_operators(&base, &[clock]).is_err());
        Ok(())
    }
}
