//! Graph edits for moving integrations across a circuit and recursive boundaries.
use super::CircuitEditor;
use crate::{Error, Field, NodeClass, NodeId, OperatorKind as Op, OutputPort, TypeId};

impl CircuitEditor {
    /// Insert a trace boundary. Its identity projection and inherited type are
    /// built in circuit order alongside the closures of other rewired operators.
    pub fn insert_trace_noop(
        &mut self,
        body: NodeId,
        reader: NodeId,
        input: OutputPort,
        ty: TypeId,
    ) -> Result<NodeId, Error> {
        let id = self.append_operator(
            body,
            reader,
            Op::Noop,
            &[input],
            [("outputType", Field::Reference(ty.into()))],
        )?;
        self.place_before(body, id, reader)?;
        self.rewired.insert(id);
        Ok(id)
    }

    /// Give a linear aggregate the same key predicate and waterline as its retainer.
    pub fn retain_aggregate_keys(
        &mut self,
        aggregate: NodeId,
        retainer: NodeId,
    ) -> Result<(), Error> {
        let op = self.node(retainer)?;
        if op.operator_kind() != Some(Op::IntegrateTraceRetainKeys) {
            return Err(Error::new("expected a key retainer"));
        }
        let watermark = op
            .inputs()
            .nth(1)
            .ok_or_else(|| Error::new("missing retention waterline"))?;
        let function = self.expression(op.edge("function")?)?;
        let op = self.node(aggregate)?;
        if op.operator_kind() != Some(Op::AggregateLinearPostprocess) {
            return Err(Error::new("expected a linear postprocess aggregate"));
        }
        let input = op
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing aggregate input"))?;
        self.check_ports([input, watermark])?;
        self.clear_annotations(aggregate)?;
        let op = self
            .circuit
            .nodes
            .get_mut(&aggregate)
            .expect("validated aggregate");
        op.class = NodeClass::Operator(Op::AggregateLinearPostprocessRetainKeys);
        op.fields.insert(
            "inputs".into(),
            Field::Array(vec![input.as_field(), watermark.as_field()]),
        );
        op.fields.insert(
            "retainKeysFunction".into(),
            Field::Reference(function.into()),
        );
        self.changed.insert(aggregate);
        Ok(())
    }

    /// Replace the incoming ports of an existing operator. Re-run the original
    /// constructor's type and binding reconstruction during the ordered rebuild.
    pub fn rewire_inputs(&mut self, id: NodeId, inputs: &[OutputPort]) -> Result<(), Error> {
        let node = self.node(id)?;
        if node.inputs().count() != inputs.len() || node.operator_kind().is_none() {
            return Err(Error::new("rewiring must preserve operator arity"));
        }
        if node.inputs().eq(inputs.iter().copied()) {
            return Ok(());
        }
        self.check_ports(inputs.iter().copied())?;
        let ports = inputs.iter().map(|p| p.as_field()).collect();
        self.circuit
            .nodes
            .get_mut(&id)
            .expect("validated operator")
            .fields
            .insert("inputs".into(), Field::Array(ports));
        self.rewired.insert(id);
        self.changed.insert(id);
        Ok(())
    }

    pub fn remap_nested_outputs(
        &mut self,
        id: NodeId,
        outputs: &[Option<OutputPort>],
    ) -> Result<(), Error> {
        let node = self.node(id)?;
        let Some(Field::Array(ports)) = node.field("internalOutputs") else {
            return Err(Error::new("nested circuit requires output slots"));
        };
        if node.operator_kind() != Some(Op::Nested) || ports.len() != outputs.len() {
            return Err(Error::new("rewiring must preserve nested output slots"));
        }
        if ports
            .iter()
            .map(OutputPort::from_field)
            .eq(outputs.iter().copied())
        {
            return Ok(());
        }
        self.check_ports(outputs.iter().flatten().copied())?;
        let ports = outputs
            .iter()
            .map(|p| p.map_or(Field::Data(serde_json::Value::Null), OutputPort::as_field))
            .collect();
        self.circuit
            .nodes
            .get_mut(&id)
            .expect("nested circuit")
            .fields
            .insert("internalOutputs".into(), Field::Array(ports));
        self.changed.insert(id);
        Ok(())
    }

    fn check_ports(&self, ports: impl IntoIterator<Item = OutputPort>) -> Result<(), Error> {
        for port in ports {
            if !crate::wire::has_output(self.node(port.operator)?, port.number)? {
                return Err(Error::new("rewiring requires an existing output port"));
            }
        }
        Ok(())
    }

    /// Install the incremental form selected by the stream optimization. A
    /// physical aggregate inherits the input integrator's relational origin.
    pub fn incremental_form(
        &mut self,
        id: NodeId,
        aggregate_origin: Option<NodeId>,
    ) -> Result<(), Error> {
        let kind = match self.node(id)?.operator_kind() {
            Some(Op::StreamJoin) => Op::Join,
            Some(Op::StreamJoinIndex) => Op::JoinIndex,
            Some(Op::StreamAntiJoin) => Op::AntiJoin,
            Some(Op::StreamDistinct) => Op::Distinct,
            Some(Op::StreamAggregate) => Op::Aggregate,
            _ => return Ok(()),
        };
        let origin = if let Some(source) = aggregate_origin {
            self.node(source)?.field("provenance").cloned()
        } else {
            None
        };
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .expect("incremental operator");
        node.class = NodeClass::Operator(kind);
        if let Some(Field::Data(origin)) = origin {
            let identity = origin
                .get("id")
                .or_else(|| origin.get("ref"))
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("missing aggregate input origin"))?;
            node.fields.insert(
                "provenance".into(),
                Field::Data(serde_json::json!({"ref":identity})),
            );
        }
        self.changed.insert(id);
        Ok(())
    }

    /// A new integral uses the source site's relation, including the definition
    /// if the site is about to acquire a different origin.
    pub fn insert_integral(
        &mut self,
        body: NodeId,
        site: NodeId,
        input: OutputPort,
        output: TypeId,
        before: Option<NodeId>,
        preserve_origin_definition: bool,
    ) -> Result<OutputPort, Error> {
        let id = self.append_operator(
            body,
            site,
            Op::Integrate,
            &[input],
            [("outputType", Field::Reference(output.into()))],
        )?;
        if preserve_origin_definition {
            if let Some(origin) = self.node(site)?.field("provenance").cloned() {
                self.circuit
                    .nodes
                    .get_mut(&id)
                    .expect("new integrator")
                    .fields
                    .insert("provenance".into(), origin);
            }
        }
        if let Some(before) = before {
            self.place_before(body, id, before)?;
        }
        Ok(OutputPort {
            operator: id,
            number: 0,
        })
    }
}
