use crate::{BodyOrder, Circuit, CircuitRewrite, Error, Field, NodeId, OperatorKind, OutputPort};
use std::collections::{BTreeMap, BTreeSet};

impl Circuit {
    /// Remove an input from the public schema while preserving SQL settings and
    /// the declaration order of all other relations.
    pub fn remove_input_schema(&mut self, name: &str) -> Result<(), Error> {
        let root = self.nodes.get_mut(&self.root).expect("circuit root");
        let Some(metadata) = root.fields.get_mut("metadata") else {
            return Ok(());
        };
        let Field::Data(metadata) = metadata else {
            return Err(Error::new("expected program metadata"));
        };
        let inputs = metadata
            .get_mut("inputs")
            .and_then(|v| v.as_array_mut())
            .ok_or_else(|| Error::new("program metadata requires an inputs array"))?;
        inputs.retain(|entry| entry["name"].as_str() != Some(name));
        Ok(())
    }

    /// Prune unused body entries while retaining nested circuit shells. An unused
    /// numbered export becomes a vacant slot so live consumers keep their port numbers.
    /// The caller must retain the producers of surviving edges.
    pub fn retain_operators(self, kept: &BTreeSet<NodeId>) -> Result<CircuitRewrite, Error> {
        let order = self.operator_order(BodyOrder::Postorder)?;
        let removed: BTreeSet<_> = order
            .iter()
            .copied()
            .filter(|id| {
                !kept.contains(id) && self.nodes[id].operator_kind() != Some(OperatorKind::Nested)
            })
            .collect();
        self.coalesce_pruning(&BTreeMap::new(), &removed)
    }

    pub(super) fn remove_body_entries(
        &mut self,
        removed: &BTreeSet<NodeId>,
    ) -> Result<BTreeSet<NodeId>, Error> {
        if removed.is_empty() {
            return Ok(BTreeSet::new());
        }
        let order = self.operator_order(BodyOrder::Postorder)?;
        let mut changed = BTreeSet::new();
        for id in order.into_iter().chain([self.root]) {
            // The circuit writer derives nested imports from the surviving delta
            // operators when it rebuilds the graph.
            let node = self.nodes.get_mut(&id).expect("validated circuit member");
            let Some(Field::Array(body)) = node.fields.get_mut("allOperators") else {
                continue;
            };
            let original = body.len();
            body.retain(|field| !removed.contains(&field.node_id().expect("validated body entry")));
            if original != body.len() {
                changed.insert(id);
            }
            if let Some(Field::Array(outputs)) = node.fields.get_mut("internalOutputs") {
                for output in outputs {
                    if OutputPort::from_field(output)
                        .is_some_and(|port| removed.contains(&port.operator))
                    {
                        *output = Field::Data(serde_json::Value::Null);
                        changed.insert(id);
                    }
                }
            }
        }
        Ok(changed)
    }
}
