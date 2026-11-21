//! Insert stream conversions while preserving each original consumer's input identity.
use super::CircuitEditor;
use crate::{Error, Field, NodeClass, NodeId, OperatorKind as Op, OutputPort};

fn single_input(id: NodeId) -> Field {
    Field::Array(vec![Field::Object(
        [
            ("operator".into(), Field::Reference(id)),
            ("outputNumber".into(), Field::Data(0.into())),
        ]
        .into(),
    )])
}

impl CircuitEditor {
    /// A newly inserted intermediate operator can have a different relational
    /// origin from its source site. Position-free wire circuits omit the field.
    pub fn set_intermediate_origin(
        &mut self,
        id: NodeId,
        origin: Option<serde_json::Value>,
    ) -> Result<(), Error> {
        let Some(origin) = origin else {
            return Ok(());
        };
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .filter(|node| node.operator_kind().is_some())
            .ok_or_else(|| Error::new("intermediate origin requires an operator"))?;
        node.fields.insert("provenance".into(), Field::Data(origin));
        Ok(())
    }

    /// Move a definition to the first operator that will precede its original owner.
    pub fn move_origin_definition(&mut self, from: NodeId, to: NodeId) -> Result<(), Error> {
        if let Some(origin) = self.node(from)?.data("provenance").cloned() {
            if let Some(id) = origin.get("id") {
                let reference = serde_json::json!({"ref": id});
                if self.node(to)?.data("provenance") != Some(&reference) {
                    return Err(Error::new("operators do not share a relational origin"));
                }
                self.set_intermediate_origin(to, Some(origin))?;
                self.set_intermediate_origin(from, Some(reference))?;
            }
        }
        Ok(())
    }

    /// A property on the SQL view provides the source location for the window and its clock adapters.
    pub fn set_property_position(
        &mut self,
        operators: &[NodeId],
        position: crate::provenance::SourceRange,
    ) -> Result<(), Error> {
        let Some((&first, rest)) = operators.split_first() else {
            return Err(Error::new("no operator for property position"));
        };
        if !position.is_valid() || self.node(first)?.field("provenance").is_none() {
            return Ok(());
        }
        let id = self
            .circuit
            .provenance()?
            .last_origin_number()
            .checked_add(1)
            .filter(|&i| i <= i32::MAX as u32)
            .ok_or_else(|| Error::new("relational origin identity exhausted"))?;
        self.set_intermediate_origin(first, Some(serde_json::json!({
            "id":id, "kind":"PositionOnlyRel", "empty":true, "position":position, "positions":[position]
        })))?;
        for &op in rest {
            self.set_intermediate_origin(op, Some(serde_json::json!({"ref":id})))?;
        }
        Ok(())
    }

    pub fn aggregate_zero_to_sum(
        &mut self,
        site: NodeId,
        inputs: [OutputPort; 3],
        output: crate::TypeId,
    ) -> Result<(), Error> {
        if self.node(site)?.operator_kind() != Some(Op::AggregateZero) {
            return Err(Error::new("expected an aggregate-zero operator"));
        }
        let node = self
            .circuit
            .nodes
            .get_mut(&site)
            .expect("validated aggregate zero");
        node.class = NodeClass::Operator(Op::AtomicSum);
        node.fields.shift_remove("function");
        self.clear_annotations(site)?;
        self.replace_stream_inputs(site, &inputs, output)
    }

    /// Implement a snapshot join with two input differentiators, a delta
    /// join, and an integral at the original output slot.
    pub fn expand_snapshot_join(
        &mut self,
        body: NodeId,
        site: NodeId,
        input_types: [crate::TypeId; 2],
        origins: Option<[serde_json::Value; 3]>,
    ) -> Result<(), Error> {
        let op = self.node(site)?;
        let inputs: [OutputPort; 2] = op
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("snapshot join requires two inputs"))?;
        let (kind, output) = match op.operator_kind() {
            Some(Op::StreamAntiJoin) => (Op::AntiJoin, input_types[0].into()),
            Some(Op::StreamJoinIndex) => (Op::JoinIndex, op.edge("outputType")?),
            _ => return Err(Error::new("expected a snapshot anti-join or indexed join")),
        };
        let mut fields = vec![("outputType", Field::Reference(output))];
        if kind == Op::JoinIndex {
            fields.push(("function", Field::Reference(op.edge("function")?)));
            fields.push((
                "balanced",
                op.field("balanced")
                    .cloned()
                    .ok_or_else(|| Error::new("indexed join requires its balance flag"))?,
            ));
        }
        let mut differences = Vec::with_capacity(2);
        for (input, ty) in inputs.into_iter().zip(input_types) {
            let operator = self.append_operator(
                body,
                site,
                Op::Differentiate,
                &[input],
                [("outputType", Field::Reference(ty.into()))],
            )?;
            differences.push(OutputPort {
                operator,
                number: 0,
            });
        }
        let join = self.append_operator(body, site, kind, &differences, fields)?;
        if let Some(origins) = origins {
            for (id, origin) in [differences[0].operator, differences[1].operator, join]
                .into_iter()
                .zip(origins)
            {
                self.circuit
                    .nodes
                    .get_mut(&id)
                    .expect("new operator")
                    .fields
                    .insert("provenance".into(), Field::Data(origin));
            }
        }
        self.place_before(body, differences[0].operator, site)?;
        let integral = self
            .circuit
            .nodes
            .get_mut(&site)
            .expect("existing snapshot join");
        integral.class = NodeClass::Operator(Op::Integrate);
        integral.fields.retain(|key, _| {
            matches!(
                key.as_str(),
                "provenance" | "position" | "sourcePosition" | "sourcePositions" | "sourceEmpty"
            )
        });
        integral
            .fields
            .insert("annotations".into(), Field::Data(serde_json::json!([])));
        integral.fields.insert("inputs".into(), single_input(join));
        integral
            .fields
            .insert("outputType".into(), Field::Reference(output));
        self.changed.insert(site);
        Ok(())
    }

    /// Replace the differentiator's output with the map at its input. The
    /// original map stays in the body until the enclosing dead-code pass.
    pub fn differentiate_before_map(
        &mut self,
        body: NodeId,
        site: NodeId,
        map: NodeId,
        input_type: crate::TypeId,
    ) -> Result<(), Error> {
        let mut mapped = self.node(map)?.clone();
        if self.node(site)?.operator_kind() != Some(Op::Differentiate)
            || !matches!(mapped.operator_kind(), Some(Op::Map | Op::MapIndex))
        {
            return Err(Error::new("expected a differentiator after a map"));
        }
        let input = mapped
            .inputs()
            .next()
            .ok_or_else(|| Error::new("map requires an input"))?;
        let delta = self.append_operator(
            body,
            site,
            Op::Differentiate,
            &[input],
            [("outputType", Field::Reference(input_type.into()))],
        )?;
        // The original delta will be overwritten. Preserve its origin definition
        // on the new delta, and refer to the existing map's origin from its clone.
        if let Some(origin) = self.node(site)?.field("provenance").cloned() {
            self.circuit
                .nodes
                .get_mut(&delta)
                .expect("new differentiator")
                .fields
                .insert("provenance".into(), origin);
        }
        if let Some(Field::Data(origin)) = mapped.fields.get_mut("provenance") {
            if let Some(id) = origin["id"].as_u64() {
                *origin = serde_json::json!({"ref": id});
            }
        }
        self.place_before(body, delta, site)?;
        mapped.fields.insert("inputs".into(), single_input(delta));
        self.circuit.nodes.insert(site, mapped);
        self.changed.insert(site);
        Ok(())
    }

    /// Insert a copy of a unary producer on just this consumer's input. A
    /// previously rewritten producer can have a different input on its other uses.
    pub fn clone_projection_for_reader(
        &mut self,
        body: NodeId,
        site: NodeId,
        producer: NodeId,
        input: OutputPort,
    ) -> Result<(), Error> {
        let op = self.node(producer)?;
        let Some(kind @ (Op::Filter | Op::Map | Op::MapIndex)) = op.operator_kind() else {
            return Err(Error::new("fanout cloning requires a filter or projection"));
        };
        let fields = [
            ("outputType", Field::Reference(op.edge("outputType")?)),
            ("function", Field::Reference(op.edge("function")?)),
        ];
        let annotations = op.field("annotations").cloned();
        let copy = self.append_operator(body, producer, kind, &[input], fields)?;
        if let Some(annotations) = annotations {
            self.circuit
                .nodes
                .get_mut(&copy)
                .expect("new producer")
                .fields
                .insert("annotations".into(), annotations);
        }
        self.place_before(body, copy, site)?;
        self.circuit
            .nodes
            .get_mut(&site)
            .expect("validated reader")
            .fields
            .insert("inputs".into(), single_input(copy));
        self.changed.insert(site);
        Ok(())
    }

    /// Interpose a validator and keep every original consumer attached to
    /// its output, including consumers inside a recursive component.
    pub fn validate_source_weights(&mut self, body: NodeId, site: NodeId) -> Result<(), Error> {
        let node = self.node(site)?;
        if node.operator_kind() != Some(Op::SourceMultiset) {
            return Err(Error::new("weight validation requires an unkeyed table"));
        }
        let name = crate::mir::identifier(node.field("tableName"))?;
        let properties = [
            ("outputType", Field::Reference(node.edge("outputType")?)),
            ("message", Field::Data(format!("Table {name}").into())),
        ];
        let moved = self.append_operator(body, site, Op::WeightValidator, &[], properties)?;
        // Exchange the new and old slots so references to the original ID go
        // through validation. Move the source, including its provenance, intact.
        let mut validator = self.circuit.nodes.remove(&moved).expect("new validator");
        validator
            .fields
            .insert("inputs".into(), single_input(moved));
        let source = self
            .circuit
            .nodes
            .insert(site, validator)
            .expect("existing source");
        self.circuit.nodes.insert(moved, source);
        self.changed.insert(site);
        self.place_before(body, moved, site)
    }

    /// Source stream kinds are encoded by the incremental flag on the enclosing circuit.
    pub fn mark_incremental(&mut self) {
        let root = self.circuit.root_id();
        self.circuit
            .nodes
            .get_mut(&root)
            .expect("validated circuit")
            .fields
            .insert("incremental".into(), Field::Data(true.into()));
        self.changed.insert(root);
    }

    /// Consumers keep the original slot, which now supplies the integrated changes.
    /// A clock supplies snapshots, so its input to the integral must be differentiated.
    pub fn integrate_source(&mut self, body: NodeId, site: NodeId) -> Result<(), Error> {
        let mut source = self.node(site)?.clone();
        let kind = source.operator_kind();
        if !matches!(
            kind,
            Some(Op::SourceMap | Op::SourceMultiset | Op::Now | Op::Constant)
        ) {
            return Err(Error::new("expected a source, clock or constant"));
        }
        let ty = source.edge("outputType")?;
        if kind == Some(Op::Constant) {
            source.class = NodeClass::Operator(Op::IncrementalConstant);
        }

        // Move the source definition, including its inner types and provenance, to
        // the new slot. The integral inherits the same relational origin.
        let moved = self.append_operator(
            body,
            site,
            Op::Integrate,
            &[],
            [("outputType", Field::Reference(ty))],
        )?;
        let mut integral = self
            .circuit
            .nodes
            .insert(moved, source)
            .expect("new wrapper");
        if kind == Some(Op::Constant) {
            if let Some(annotations) = self.node(moved)?.field("annotations") {
                integral
                    .fields
                    .insert("annotations".into(), annotations.clone());
            }
        }
        let changes = if kind == Some(Op::Now) {
            self.append_operator(
                body,
                moved,
                Op::Differentiate,
                &[OutputPort {
                    operator: moved,
                    number: 0,
                }],
                [("outputType", Field::Reference(ty))],
            )?
        } else {
            moved
        };
        integral
            .fields
            .insert("inputs".into(), single_input(changes));
        self.circuit.nodes.insert(site, integral);
        self.changed.insert(site);
        self.place_before(body, moved, site)
    }

    /// A public view emits changes relative to its previous contents.
    pub fn differentiate_sink(&mut self, body: NodeId, site: NodeId) -> Result<(), Error> {
        let sink = self.node(site)?;
        if sink.operator_kind() != Some(Op::Sink) {
            return Err(Error::new("expected a sink"));
        }
        let mut inputs = sink.inputs();
        let port = inputs
            .next()
            .filter(|_| inputs.next().is_none())
            .ok_or_else(|| Error::new("sink requires one input"))?;
        drop(inputs);
        let ty = self.type_id(sink.edge("outputType")?)?;
        let delta = self.append_operator(
            body,
            site,
            Op::Differentiate,
            &[port],
            [("outputType", Field::Reference(ty.into()))],
        )?;
        self.place_before(body, delta, site)?;
        self.replace_stream_inputs(
            site,
            &[OutputPort {
                operator: delta,
                number: 0,
            }],
            ty,
        )?;
        self.clear_annotations(site)
    }
}
