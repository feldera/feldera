//! Construct recursive circuit bodies while keeping their members' identities.
use super::CircuitEditor;
use crate::{Error, Field, NodeId, OperatorKind as Op, OutputPort};

impl CircuitEditor {
    pub fn begin_recursive_body(&mut self, site: NodeId) -> Result<NodeId, Error> {
        let root = self.circuit.root_id();
        let block = self.append_operator(root, site, Op::Nested, &[], [])?;
        let fields = &mut self
            .circuit
            .nodes
            .get_mut(&block)
            .expect("new block")
            .fields;
        for name in ["allOperators", "internalOutputs", "outputViews"] {
            fields.insert(name.into(), Field::Array(Vec::new()));
        }
        self.annotate(block, serde_json::json!({"class":"Recursive"}))?;
        Ok(block)
    }

    /// Move a member from the outer circuit into the block in visit order.
    pub fn move_into_recursive_body(&mut self, block: NodeId, member: NodeId) -> Result<(), Error> {
        if self.node(block)?.operator_kind() != Some(Op::Nested) || block == member {
            return Err(Error::new(
                "expected a nested destination and a separate member",
            ));
        }
        let root = self.circuit.root_id();
        let Some(Field::Array(outer)) = self
            .circuit
            .nodes
            .get_mut(&root)
            .unwrap()
            .fields
            .get_mut("allOperators")
        else {
            unreachable!("validated circuit");
        };
        let index = outer
            .iter()
            .position(|field| field.node_id() == Some(member))
            .ok_or_else(|| Error::new("recursive component member missing from root"))?;
        let definition = outer.remove(index);
        let Some(Field::Array(body)) = self
            .circuit
            .nodes
            .get_mut(&block)
            .unwrap()
            .fields
            .get_mut("allOperators")
        else {
            unreachable!("validated nested circuit");
        };
        body.push(definition);
        self.changed.insert(block);
        Ok(())
    }

    pub fn export_recursive_view(
        &mut self,
        block: NodeId,
        view: NodeId,
        output: OutputPort,
    ) -> Result<OutputPort, Error> {
        let name = self
            .node(view)?
            .field("viewName")
            .cloned()
            .ok_or_else(|| Error::new("missing recursive output name"))?;
        if self.node(block)?.operator_kind() != Some(Op::Nested)
            || self.node(output.operator)?.operator_kind() != Some(Op::Differentiate)
        {
            return Err(Error::new(
                "expected a differentiated recursive view output",
            ));
        }
        let fields = &mut self
            .circuit
            .nodes
            .get_mut(&block)
            .expect("validated block")
            .fields;
        let Some(Field::Array(names)) = fields.get_mut("outputViews") else {
            return Err(Error::new("missing recursive output names"));
        };
        let number = names.len();
        names.push(name);
        let Some(Field::Array(ports)) = fields.get_mut("internalOutputs") else {
            return Err(Error::new("missing recursive outputs"));
        };
        ports.push(output.as_field());
        self.changed.insert(block);
        Ok(OutputPort {
            operator: block,
            number,
        })
    }
}
