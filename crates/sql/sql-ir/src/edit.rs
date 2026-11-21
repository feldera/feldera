//! Build inner nodes while retaining the identities of unchanged dependencies.
//! Finishing the edit resolves the new edges and relocates shared definitions.

mod incremental;
mod integrals;
mod nested;

use crate::{Circuit, Error, Field, InnerKind, Node, NodeClass, NodeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub struct CircuitEditor {
    circuit: Circuit,
    next: Option<u64>,
    changed: BTreeSet<NodeId>,
    rewired: BTreeSet<NodeId>,
    implicit_outputs: BTreeMap<crate::OutputPort, NodeId>,
    source_positions: BTreeMap<NodeId, feldera_sql_source::SourceRange>,
    singletons: HashMap<(crate::TypeKind, bool), NodeId>,
}

impl CircuitEditor {
    pub fn new(circuit: Circuit) -> Self {
        let next = match circuit.nodes.last_key_value() {
            Some((NodeId::Inner(id), _)) => id.checked_add(1),
            _ => Some(0),
        };
        let singletons = circuit
            .nodes
            .iter()
            .filter_map(|(&id, node)| {
                let InnerKind::Type(kind) = node.inner_kind()? else {
                    return None;
                };
                // Weight and ISIZE have one instance per supported nullability.
                // Their wire types carry no singleton marker.
                if matches!(kind, crate::TypeKind::ISize | crate::TypeKind::Weight)
                    || node.data("singleton").and_then(|v| v.as_bool()) == Some(true)
                {
                    Some(((*kind, node.data("mayBeNull")?.as_bool()?), id))
                } else {
                    None
                }
            })
            .collect();
        Self {
            singletons,
            circuit,
            next,
            changed: BTreeSet::new(),
            rewired: BTreeSet::new(),
            implicit_outputs: BTreeMap::new(),
            source_positions: BTreeMap::new(),
        }
    }

    pub fn circuit(&self) -> &Circuit {
        &self.circuit
    }

    pub fn node(&self, id: NodeId) -> Result<&Node, Error> {
        self.circuit
            .node(id)
            .ok_or_else(|| Error(format!("unknown circuit node {id:?}")))
    }

    pub(crate) fn has_replay_metadata(&self, sample: NodeId) -> Result<bool, Error> {
        let n = self.node(sample)?;
        Ok(n.field("sourceEmpty").is_some() || n.field("provenance").is_some())
    }

    fn insert(&mut self, node: Node) -> Result<NodeId, Error> {
        for field in node.fields.values() {
            crate::wire::check_links(field, &self.circuit.nodes)?;
        }
        let id = self
            .next
            .ok_or_else(|| Error("inner node identity exhausted".into()))?;
        self.next = id.checked_add(1);
        let id = NodeId::Inner(id);
        self.circuit.nodes.insert(id, node);
        Ok(id)
    }

    pub(crate) fn primitive_singleton(
        &mut self,
        sample: NodeId,
        kind: crate::TypeKind,
        code: &str,
        nullable: bool,
    ) -> Result<crate::TypeId, Error> {
        if let Some(&id) = self.singletons.get(&(kind, nullable)) {
            return self.type_id(id);
        }
        let checkpoint = self.has_replay_metadata(sample)?;
        let mut fields = vec![
            ("code", Field::Data(code.into())),
            ("mayBeNull", Field::Data(nullable.into())),
        ];
        use crate::TypeKind;
        match kind {
            TypeKind::String => fields.extend([
                ("fixed", Field::Data(false.into())),
                ("precision", Field::Data((-1).into())),
            ]),
            TypeKind::RawTuple => fields.push(("tupFields", Field::Array(Vec::new()))),
            TypeKind::Weight => fields.extend([
                ("typeArgs", Field::Array(Vec::new())),
                ("name", Field::Data("Weight".into())),
            ]),
            _ => (),
        }
        if checkpoint {
            fields.push(("sourceEmpty", Field::Data(true.into())));
            if !matches!(kind, TypeKind::Weight | TypeKind::ISize) {
                fields.push(("singleton", Field::Data(true.into())));
            }
        }
        let id = self.inner_with_fields(InnerKind::Type(kind), fields)?;
        self.singletons.insert((kind, nullable), id);
        self.type_id(id)
    }

    pub fn remember_position(
        &mut self,
        operator: NodeId,
        position: feldera_sql_source::SourceRange,
    ) -> Result<(), Error> {
        self.node(operator)?;
        self.source_positions.insert(operator, position);
        Ok(())
    }

    /// Register canonical types supplied by the compiler prefix when the
    /// observation format omits checkpoint identity markers.
    pub fn register_singleton(&mut self, id: NodeId) -> Result<(), Error> {
        let node = self.node(id)?;
        let Some(InnerKind::Type(kind)) = node.inner_kind() else {
            return Err(Error::new("canonical identity is not a type"));
        };
        let nullable = node
            .data("mayBeNull")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new("type lacks nullability"))?;
        self.singletons.insert((*kind, nullable), id);
        Ok(())
    }

    pub(crate) fn is_registered_singleton(&self, ty: crate::TypeId) -> Result<bool, Error> {
        let key = (self.type_kind(ty)?, self.nullable(ty)?);
        Ok(self.singletons.get(&key) == Some(&ty.into()))
    }

    pub(crate) fn change_nullability(
        &mut self,
        id: NodeId,
        nullable: bool,
    ) -> Result<NodeId, Error> {
        let mut node = self.node(id)?.clone();
        node.fields.shift_remove("singleton");
        node.fields
            .insert("mayBeNull".into(), Field::Data(nullable.into()));
        self.insert(node)
    }

    pub fn inner<const N: usize>(
        &mut self,
        kind: InnerKind,
        fields: [(&str, Field); N],
    ) -> Result<NodeId, Error> {
        self.inner_with_fields(kind, fields)
    }

    pub(crate) fn inner_with_fields<'a>(
        &mut self,
        kind: InnerKind,
        fields: impl IntoIterator<Item = (&'a str, Field)>,
    ) -> Result<NodeId, Error> {
        self.insert(Node::new(
            NodeClass::Inner(kind),
            fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        ))
    }

    /// Construct a node with the source of an existing expression or type.
    /// Checkpoint metadata distinguishes an empty origin from an invalid range.
    pub fn inner_at(
        &mut self,
        origin: NodeId,
        kind: InnerKind,
        fields: impl IntoIterator<Item = (&'static str, Field)>,
    ) -> Result<NodeId, Error> {
        let source = self.node(origin)?;
        let mut location = ["sourceEmpty", "position"]
            .into_iter()
            .filter_map(|name| source.field(name).cloned().map(|value| (name, value)))
            .collect::<Vec<_>>();
        if matches!(origin, NodeId::Outer(_)) && source.field("provenance").is_some() {
            use crate::provenance::OriginKind;
            let table = self.circuit.provenance()?;
            let entry = table
                .operator_origin(origin)
                .and_then(|id| table.origin(id))
                .ok_or_else(|| Error::new("missing source for expression created at operator"))?;
            let empty = match entry.kind {
                OriginKind::Empty => true,
                OriginKind::PositionOnly { empty } => empty,
                _ => false,
            };
            location.clear();
            location.push(("sourceEmpty", Field::Data(empty.into())));
            if entry.position.is_valid() || !empty {
                location.push(("position", entry.position.into()));
            }
        }
        if location.is_empty() {
            if let Some(pos) = self.source_positions.get(&origin).filter(|p| p.is_valid()) {
                location.push(("position", (*pos).into()));
            }
        }
        let fields = location
            .into_iter()
            .chain(fields)
            .map(|(name, value)| (name.to_owned(), value))
            .collect();
        self.insert(Node::new(NodeClass::Inner(kind), fields))
    }

    /// Reuse the source location and all unmodified fields of an existing node.
    pub fn derive_inner<'a>(
        &mut self,
        id: NodeId,
        changes: impl IntoIterator<Item = (&'a str, Field)>,
    ) -> Result<NodeId, Error> {
        let mut node = self.node(id)?.clone();
        if node.inner_kind().is_none() {
            return Err(Error(format!("expected an inner node at {id:?}")));
        }
        node.fields.extend(
            changes
                .into_iter()
                .map(|(name, field)| (name.into(), field)),
        );
        self.insert(node)
    }

    /// Rebuild an expression whose Java constructor uses an empty source location.
    pub fn derive_unlocated_inner<'a>(
        &mut self,
        id: NodeId,
        changes: impl IntoIterator<Item = (&'a str, Field)>,
    ) -> Result<NodeId, Error> {
        let replay = self.has_replay_metadata(id)?;
        let derived = self.derive_inner(id, changes)?;
        let node = self
            .circuit
            .nodes
            .get_mut(&derived)
            .expect("fresh inner node");
        node.fields.shift_remove("position");
        if replay {
            node.fields
                .insert("sourceEmpty".into(), Field::Data(true.into()));
        }
        Ok(derived)
    }

    /// Place a producer with one consumer at the consumer's position in its
    /// enclosing circuit. Fusion emits the replacement when it visits the reader.
    pub fn move_before(&mut self, producer: NodeId, consumer: NodeId) -> Result<(), Error> {
        for parent in self.circuit.nodes.values_mut() {
            let Some(Field::Array(body)) = parent.fields.get_mut("allOperators") else {
                continue;
            };
            let Some(to) = body.iter().position(|f| f.node_id() == Some(consumer)) else {
                continue;
            };
            let from = body[..to]
                .iter()
                .position(|f| f.node_id() == Some(producer))
                .ok_or_else(|| {
                    Error::new("fusion requires an earlier producer in the same circuit body")
                })?;
            body[from..to].rotate_left(1);
            self.changed.insert(producer);
            return Ok(());
        }
        Err(Error::new("fusion reader is missing from the circuit body"))
    }

    /// Construct the adaptive form even when the input join was already marked
    /// balanced. Downstream constructors observe the new operator identity.
    pub fn balance_join(&mut self, id: NodeId) -> Result<(), Error> {
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .filter(|n| {
                n.operator_kind()
                    .is_some_and(crate::OperatorKind::supports_balanced_join)
            })
            .ok_or_else(|| Error::new("operator does not support balanced joins"))?;
        node.fields
            .insert("balanced".into(), Field::Data(true.into()));
        self.changed.insert(id);
        Ok(())
    }

    /// Install the narrowed SQL record and keep the metadata for the same columns.
    pub fn trim_source_columns(
        &mut self,
        source: NodeId,
        record: NodeId,
        output: crate::TypeId,
        columns: &[usize],
    ) -> Result<(), Error> {
        if !matches!(
            self.node(record)?.inner_kind(),
            Some(InnerKind::Type(crate::TypeKind::Struct))
        ) {
            return Err(Error::new("trimmed source requires a struct type"));
        }
        let table = self
            .circuit
            .nodes
            .get_mut(&source)
            .filter(|n| n.operator_kind() == Some(crate::OperatorKind::SourceMultiset))
            .ok_or_else(|| Error::new("expected a multiset input table"))?;
        let Some(Field::Object(metadata)) = table.fields.get_mut("metadata") else {
            return Err(Error::new("missing source metadata"));
        };
        let Some(Field::Array(fields)) = metadata.get_mut("columnMetadata") else {
            return Err(Error::new("missing source columns"));
        };
        *fields = columns
            .iter()
            .map(|&i| {
                fields
                    .get(i)
                    .cloned()
                    .ok_or_else(|| Error::new("trimmed column outside table metadata"))
            })
            .collect::<Result<_, _>>()?;
        table
            .fields
            .insert("originalRowType".into(), Field::Reference(record));
        self.set_operator_field(source, "outputType", Field::Reference(output.into()))?;
        self.clear_annotations(source)
    }

    pub fn set_operator_field(
        &mut self,
        id: NodeId,
        name: &str,
        field: Field,
    ) -> Result<(), Error> {
        let target = field
            .node_id()
            .ok_or_else(|| Error::new("expected an inner node edge"))?;
        let kind = self.node(target)?.inner_kind();
        if !matches!(
            (name, kind),
            ("outputType", Some(InnerKind::Type(_)))
                | ("function" | "partitioningFunction", Some(InnerKind::Expression(_)))
                | ("aggregate", Some(InnerKind::AggregateList))
        ) {
            return Err(Error(format!("invalid {name} edge to {target:?}")));
        }
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .filter(|node| node.operator_kind().is_some())
            .ok_or_else(|| Error(format!("expected an operator at {id:?}")))?;
        node.fields.insert(name.into(), field);
        self.changed.insert(id);
        Ok(())
    }

    /// Visit the inner roots of every operator and declaration, including nested bodies.
    /// Leave topology, source metadata, and output port numbers in place.
    pub fn rewrite_inner_roots(
        mut self,
        mut rewrite: impl FnMut(&mut Self, NodeId) -> Result<NodeId, Error>,
    ) -> Result<Circuit, Error> {
        let owners: Vec<_> = self
            .circuit
            .nodes
            .range(..=NodeId::Outer(u64::MAX))
            .map(|(&id, _)| id)
            .collect();
        for owner in owners {
            self.rewrite_owner_roots(owner, &mut rewrite)?;
        }
        self.finish()
    }

    fn rewrite_owner_roots(
        &mut self,
        owner: NodeId,
        mut rewrite: impl FnMut(&mut Self, NodeId) -> Result<NodeId, Error>,
    ) -> Result<(), Error> {
        let names: Vec<_> = self.node(owner)?.fields.keys().cloned().collect();
        for name in names {
            // CircuitRewriter retains table metadata when rebuilding a source.
            if name != "metadata" {
                self.rewrite_field_roots(owner, &name, &mut rewrite)?;
            }
        }
        Ok(())
    }

    /// Rewrite the inner roots in one optional field, preserving their array order.
    /// The caller supplies the field order for visitors with traversal-dependent state.
    pub fn rewrite_field_roots(
        &mut self,
        owner: NodeId,
        name: &str,
        mut rewrite: impl FnMut(&mut Self, NodeId) -> Result<NodeId, Error>,
    ) -> Result<(), Error> {
        let Some(mut field) = self.node(owner)?.field(name).cloned() else {
            return Ok(());
        };
        if field.map_inner_edges(&mut |id| rewrite(self, id))? {
            let synthesized = name == "function"
                && matches!(
                    self.node(owner)?.operator_kind(),
                    Some(crate::OperatorKind::Deindex | crate::OperatorKind::Noop)
                );
            if synthesized {
                // These constructors discard the supplied closure and generate a
                // new projection from the input, using the original function site.
                self.rewired.insert(owner);
            } else {
                self.circuit
                    .nodes
                    .get_mut(&owner)
                    .expect("existing owner")
                    .fields
                    .insert(name.into(), field);
            }
            self.changed.insert(owner);
        }
        Ok(())
    }

    /// Rewrite defaults and LATENESS expressions in schema order. The source
    /// operator is reconstructed only when one of its column expressions changes.
    pub fn rewrite_column_initializers(
        &mut self,
        source: NodeId,
        mut rewrite: impl FnMut(&mut Self, crate::ExpressionId) -> Result<crate::ExpressionId, Error>,
    ) -> Result<(), Error> {
        let Some(Field::Object(mut metadata)) = self.node(source)?.field("metadata").cloned()
        else {
            return Err(Error::new("input source requires table metadata"));
        };
        let Some(Field::Array(columns)) = metadata.get_mut("columnMetadata") else {
            return Err(Error::new("table metadata requires a column list"));
        };
        let mut changed = false;
        for column in columns {
            let Field::Object(fields) = column else {
                return Err(Error::new("expected column metadata"));
            };
            for key in ["defaultValue", "lateness"] {
                let Some(field) = fields.get_mut(key) else {
                    continue;
                };
                let Some(id) = field.node_id() else {
                    return Err(Error::new("expected a column initializer expression"));
                };
                let original = self.expression(id)?;
                let replacement = rewrite(self, original)?;
                if replacement != original {
                    *field = Field::Reference(replacement.into());
                    changed = true;
                }
            }
        }
        if changed {
            self.clear_annotations(source)?;
            self.circuit
                .nodes
                .get_mut(&source)
                .expect("visited source")
                .fields
                .insert("metadata".into(), Field::Object(metadata));
        }
        Ok(())
    }

    /// Copy the source operator's annotations when reconstructing its inputs.
    pub fn copy_annotations(&mut self, source: NodeId, target: NodeId) -> Result<(), Error> {
        let annotations = crate::mir::annotations(self.node(source)?)?.to_vec();
        *self.circuit.annotations_mut(target)? = annotations;
        self.changed.insert(target);
        Ok(())
    }

    pub fn clear_annotations(&mut self, id: NodeId) -> Result<(), Error> {
        self.circuit.annotations_mut(id)?.clear();
        self.changed.insert(id);
        Ok(())
    }

    pub fn annotate(&mut self, id: NodeId, annotation: serde_json::Value) -> Result<(), Error> {
        self.circuit.annotations_mut(id)?.push(annotation);
        Ok(())
    }

    /// Replace a Deindex by its equivalent map, using its existing value projection.
    pub fn deindex_as_map(&mut self, id: NodeId) -> Result<(), Error> {
        if self.node(id)?.operator_kind() != Some(crate::OperatorKind::Deindex) {
            return Err(Error::new("expected a deindex operator"));
        }
        self.clear_annotations(id)?;
        self.circuit
            .nodes
            .get_mut(&id)
            .expect("validated operator")
            .class = NodeClass::Operator(crate::OperatorKind::Map);
        Ok(())
    }

    /// Lower an identity stream to the backend map that produces the same collection.
    pub fn lower_noop(&mut self, id: NodeId) -> Result<(), Error> {
        use crate::{OperatorKind as Op, TypeKind};
        let node = self.node(id)?;
        if node.operator_kind() != Some(Op::Noop) {
            return Err(Error::new("expected an identity stream"));
        }
        let kind = match self.type_kind(self.type_id(node.edge("outputType")?)?)? {
            TypeKind::ZSet => Op::Map,
            TypeKind::IndexedZSet => Op::MapIndex,
            _ => return Err(Error::new("identity stream requires a collection output")),
        };
        self.clear_annotations(id)?;
        self.circuit
            .nodes
            .get_mut(&id)
            .expect("validated operator")
            .class = NodeClass::Operator(kind);
        Ok(())
    }

    /// Replace the operator's high-level payload with the executable function.
    /// The decoder reads the function immediately after the output type, before rolling bounds.
    pub fn install_lowered_function(
        &mut self,
        id: NodeId,
        function: crate::ExpressionId,
    ) -> Result<(), Error> {
        use crate::OperatorKind as Op;
        let kind = self.node(id)?.operator_kind();
        match kind {
            Some(
                Op::Aggregate | Op::StreamAggregate | Op::PartitionedRollingAggregate
                | Op::PartitionedRollingAggregateWithWaterline,
            ) => self.clear_annotations(id)?,
            Some(Op::FlatMap | Op::JoinFilterMap | Op::LeftJoinFilterMap | Op::StarJoinFilterMap) => {}
            _ => return Err(Error::new("operator has no function to lower")),
        }
        self.set_operator_field(id, "function", Field::Reference(function.into()))?;
        let node = self.circuit.nodes.get_mut(&id).expect("validated operator");
        for key in ["aggregate", "aggregateList", "filter", "map"] {
            node.fields.shift_remove(key);
        }
        let function = node.fields.shift_remove("function").expect("installed function");
        let output = node.fields.get_index_of("outputType")
            .ok_or_else(|| Error::new("operator lacks output type"))?;
        node.fields.shift_insert(output + 1, "function".into(), function);
        Ok(())
    }

    /// Preserve a separate trace for an identity map whose retainer cannot move.
    /// Rebuild its projection in traversal order with the other rewritten Noops.
    pub fn identity_map_as_noop(&mut self, id: NodeId) -> Result<(), Error> {
        if !matches!(
            self.node(id)?.operator_kind(),
            Some(crate::OperatorKind::Map | crate::OperatorKind::MapIndex)
        ) {
            return Err(Error::new("expected a map or map index"));
        }
        self.clear_annotations(id)?;
        self.circuit
            .nodes
            .get_mut(&id)
            .expect("validated map")
            .class = NodeClass::Operator(crate::OperatorKind::Noop);
        self.rewired.insert(id);
        Ok(())
    }

    /// Replace a sum whose terms cancel with an empty snapshot of the same collection.
    pub fn cancel_sum(&mut self, id: NodeId) -> Result<(), Error> {
        if self.node(id)?.operator_kind() != Some(crate::OperatorKind::Sum) {
            return Err(Error::new("expected a sum"));
        }
        self.empty_constant(id, crate::StreamKind::Collection, false)
    }

    /// Construct an empty constant with the operator's output type and relational origin.
    pub fn replace_with_empty_stream(
        &mut self,
        id: NodeId,
        clock: crate::StreamKind,
    ) -> Result<(), Error> {
        self.empty_constant(id, clock, true)
    }

    fn empty_constant(
        &mut self,
        id: NodeId,
        clock: crate::StreamKind,
        inherit_indexed_origin: bool,
    ) -> Result<(), Error> {
        use crate::{ExpressionKind, OperatorKind, StreamKind, TypeKind};
        let kind = match clock {
            StreamKind::Collection => OperatorKind::Constant,
            StreamKind::Delta => OperatorKind::IncrementalConstant,
            StreamKind::Waterline => {
                return Err(Error::new("a waterline cannot contain an empty multiset"))
            }
        };
        let output = self.node(id)?.edge("outputType")?;
        let value = match self.type_kind(self.type_id(output)?)? {
            TypeKind::ZSet => {
                let element = self.node(output)?.edge("elementType")?;
                self.empty_zset(self.type_id(element)?)?.into()
            }
            TypeKind::IndexedZSet => {
                let expr = InnerKind::Expression(ExpressionKind::IndexedZSet);
                let fields = [("type", Field::Reference(output))];
                if inherit_indexed_origin {
                    self.inner_at(output, expr, fields)?
                } else {
                    self.empty_origin_node(output, expr, fields)?
                }
            }
            _ => return Err(Error::new("empty stream requires a collection type")),
        };
        let ty = self.node(value)?.edge("type")?;
        self.replace_simple(id, kind, &[], ty, value)
    }

    /// Specialize a left join whose right side is empty into a map on its left side.
    pub fn left_join_as_map(
        &mut self,
        id: NodeId,
        input: crate::OutputPort,
        function: crate::ExpressionId,
    ) -> Result<(), Error> {
        if self.node(id)?.operator_kind() != Some(crate::OperatorKind::LeftJoin) {
            return Err(Error::new("expected a left join"));
        }
        let body = self.expression(self.node(function.into())?.edge("body")?)?;
        let output = self.zset_type(self.expression_type(body)?)?;
        self.replace_simple(
            id,
            crate::OperatorKind::Map,
            &[input],
            output.into(),
            function.into(),
        )
    }

    /// A keyed left join whose result ignores the right input preserves its declared output type.
    pub fn project_left_input(
        &mut self,
        join: NodeId,
        function: crate::ExpressionId,
    ) -> Result<(), Error> {
        use crate::OperatorKind as Op;
        let op = self.node(join)?;
        let kind = match op.operator_kind() {
            Some(Op::LeftJoin) => Op::Map,
            Some(Op::LeftJoinIndex) => Op::MapIndex,
            _ => return Err(Error::new("expected a left join")),
        };
        let left = op
            .inputs()
            .next()
            .ok_or_else(|| Error::new("left join has no left input"))?;
        let ty = op.edge("outputType")?;
        self.replace_simple(join, kind, &[left], ty, function.into())
    }

    fn replace_simple(
        &mut self,
        id: NodeId,
        kind: crate::OperatorKind,
        inputs: &[crate::OutputPort],
        output: NodeId,
        function: NodeId,
    ) -> Result<(), Error> {
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .ok_or_else(|| Error::new("missing operator"))?;
        if node.operator_kind().is_none() || node.field("allOperators").is_some() {
            return Err(Error::new("expected a simple operator"));
        }
        node.class = NodeClass::Operator(kind);
        node.fields
            .retain(|name, _| matches!(name.as_str(), "position" | "provenance"));
        node.fields
            .insert("annotations".into(), Field::Data(serde_json::json!([])));
        node.fields.insert(
            "inputs".into(),
            Field::Array(
                inputs
                    .iter()
                    .map(|port| {
                        Field::Object(
                            [
                                ("operator".into(), Field::Reference(port.operator)),
                                ("outputNumber".into(), Field::Data(port.number.into())),
                            ]
                            .into(),
                        )
                    })
                    .collect(),
            ),
        );
        node.fields
            .insert("outputType".into(), Field::Reference(output));
        node.fields
            .insert("function".into(), Field::Reference(function));
        self.changed.insert(id);
        Ok(())
    }

    /// An integrated incremental constant uses the same value and annotations.
    /// The resulting constant retains the integrator's relational origin.
    pub fn integrate_constant(&mut self, id: NodeId, source: NodeId) -> Result<(), Error> {
        use crate::OperatorKind::{Constant, IncrementalConstant, Integrate};
        let delta = self.node(source)?;
        if delta.operator_kind() != Some(IncrementalConstant)
            || self.node(id)?.operator_kind() != Some(Integrate)
        {
            return Err(Error::new(
                "expected an integral of an incremental constant",
            ));
        }
        let function = delta.edge("function")?;
        let output_type = delta.edge("outputType")?;
        let annotations = delta
            .field("annotations")
            .cloned()
            .ok_or_else(|| Error::new("incremental constant requires annotations"))?;
        let node = self.circuit.nodes.get_mut(&id).expect("validated integral");
        node.class = NodeClass::Operator(Constant);
        node.fields
            .insert("inputs".into(), Field::Array(Vec::new()));
        node.fields.insert("annotations".into(), annotations);
        node.fields
            .insert("outputType".into(), Field::Reference(output_type));
        node.fields
            .insert("function".into(), Field::Reference(function));
        self.changed.insert(id);
        Ok(())
    }

    pub fn finish(self) -> Result<Circuit, Error> {
        Ok(self.finish_with_rewrites()?.circuit)
    }

    /// Includes input changes that cause Java constructors to allocate fresh bindings.
    pub fn finish_with_rewrites(self) -> Result<crate::CircuitRewrite, Error> {
        self.finish_forwarding(&BTreeMap::new())
    }

    /// Drop operators that have no surviving consumers before rebuilding the edited graph.
    pub fn remove_operators(&mut self, removed: &BTreeSet<NodeId>) -> Result<(), Error> {
        self.changed
            .extend(self.circuit.remove_body_entries(removed)?);
        Ok(())
    }

    /// Forward removed single-output operators to surviving ports and rebuild their consumers
    /// once, including consumers of operators changed by other edits in this pass.
    pub fn finish_forwarding(
        mut self,
        ports: &BTreeMap<NodeId, crate::OutputPort>,
    ) -> Result<crate::CircuitRewrite, Error> {
        if self.changed.is_empty() && ports.is_empty() {
            return Ok(crate::CircuitRewrite {
                circuit: self.circuit,
                rebuilt_inputs: Vec::new(),
            });
        }
        let mut replacements = BTreeMap::new();
        for (&id, &port) in ports {
            let producer = self.node(id)?;
            if producer.operator_kind() == Some(crate::OperatorKind::Nested)
                || !crate::wire::has_output(producer, 0)?
                || crate::wire::has_output(producer, 1)?
                || !crate::wire::has_output(self.node(port.operator)?, port.number)?
                || ports.contains_key(&port.operator)
            {
                return Err(Error::new(
                    "forwarding requires a single output and a surviving producer",
                ));
            }
            replacements.insert(id, crate::coalesce::Replacement::Stream(port));
        }
        // A rebuilt keyed source allocates one error ZSet shared by all its
        // readers. The wire format stores the error closure, not that wrapper.
        let keyed_sources: Vec<_> = self
            .changed
            .iter()
            .copied()
            .filter(|id| {
                self.circuit.node(*id).and_then(Node::operator_kind)
                    == Some(crate::OperatorKind::InputMapWithWaterline)
            })
            .collect();
        for source in keyed_sources {
            let port = crate::OutputPort {
                operator: source,
                number: 1,
            };
            if !self.implicit_outputs.contains_key(&port) {
                let error = self.node(source)?.edge("error")?;
                let closure = self.node(error)?.edge("type")?;
                let element = self.type_id(self.node(closure)?.edge("resultType")?)?;
                let errors = self.zset_type(element)?;
                self.implicit_outputs.insert(port, errors.into());
            }
        }
        self.circuit.rebuild(
            &replacements,
            self.changed,
            &self.rewired,
            &self.implicit_outputs,
        )
    }
}

impl CircuitEditor {
    pub(crate) fn reserve_outer_ids(&self, count: usize) -> Result<(), Error> {
        let count =
            u64::try_from(count).map_err(|_| Error::new("outer node identity exhausted"))?;
        self.circuit
            .nodes
            .range(..=NodeId::Outer(u64::MAX))
            .next_back()
            .and_then(|(id, _)| id.number().checked_add(count))
            .ok_or_else(|| Error::new("outer node identity exhausted"))?;
        Ok(())
    }

    /// Append an item to the circuit's ordered list of global declarations.
    pub fn declare_item(&mut self, item: NodeId) -> Result<(), Error> {
        self.node(item)?
            .inner_kind()
            .ok_or_else(|| Error::new("a declaration requires an inner item"))?;
        self.reserve_outer_ids(1)?;
        let last = self
            .circuit
            .nodes
            .range(..=NodeId::Outer(u64::MAX))
            .next_back()
            .expect("circuit has a root")
            .0
            .number();
        let id = NodeId::Outer(last + 1);
        let root = self.circuit.root_id();
        let Some(Field::Array(list)) = self
            .circuit
            .nodes
            .get_mut(&root)
            .expect("circuit root")
            .fields
            .get_mut("declarations")
        else {
            return Err(Error::new("circuit is missing its declarations"));
        };
        list.push(Field::Definition(id));
        self.circuit.nodes.insert(
            id,
            Node::new(
                NodeClass::Declaration,
                [("item".into(), Field::Reference(item))].into(),
            ),
        );
        self.changed.insert(root);
        Ok(())
    }

    /// Append an operator to the ordered body during lowering. Its source
    /// position and relational origin are inherited from the supplied site.
    pub fn append_operator(
        &mut self,
        body: NodeId,
        site: NodeId,
        kind: crate::OperatorKind,
        inputs: &[crate::OutputPort],
        fields: impl IntoIterator<Item = (&'static str, Field)>,
    ) -> Result<NodeId, Error> {
        let Some(Field::Array(_)) = self.node(body)?.field("allOperators") else {
            return Err(Error::new("operator insertion needs a circuit body"));
        };
        let origin = self.node(site)?;
        let mut properties = indexmap::IndexMap::new();
        if let Some(position) = origin.field("position") {
            properties.insert("position".to_owned(), position.clone());
        }
        if let Some(Field::Data(provenance)) = origin.field("provenance") {
            let id = provenance
                .get("id")
                .or_else(|| provenance.get("ref"))
                .and_then(|v| v.as_u64())
                .ok_or_else(|| Error::new("operator site needs a valid provenance identity"))?;
            properties.insert(
                "provenance".into(),
                Field::Data(serde_json::json!({"ref":id})),
            );
        }
        properties.insert("annotations".into(), Field::Data(serde_json::json!([])));
        let ports = inputs
            .iter()
            .map(|port| {
                Field::Object(
                    [
                        ("outputNumber".into(), Field::Data(port.number.into())),
                        ("operator".into(), Field::Reference(port.operator)),
                    ]
                    .into(),
                )
            })
            .collect();
        properties.insert("inputs".into(), Field::Array(ports));
        for (name, value) in fields {
            if properties.contains_key(name)
                || matches!(
                    name,
                    "allOperators" | "internalOutputs" | "declarations" | "class" | "id"
                )
            {
                return Err(Error::new(format!(
                    "cannot replace structural operator field {name}"
                )));
            }
            properties.insert(name.into(), value);
        }
        for value in properties.values() {
            crate::wire::check_links(value, &self.circuit.nodes)?;
        }
        let next = self
            .circuit
            .nodes
            .range(..=NodeId::Outer(u64::MAX))
            .next_back()
            .and_then(|(n, _)| n.number().checked_add(1))
            .ok_or_else(|| Error::new("outer node identity exhausted"))?;
        let id = NodeId::Outer(next);
        self.circuit
            .nodes
            .insert(id, Node::new(NodeClass::Operator(kind), properties));
        let parent = self.circuit.nodes.get_mut(&body).expect("validated body");
        let Some(Field::Array(operators)) = parent.fields.get_mut("allOperators") else {
            unreachable!()
        };
        operators.push(Field::Definition(id));
        self.changed.insert(body);
        Ok(id)
    }
}

impl CircuitEditor {
    /// Resolve a single stream input to a view definition or its recursive declaration.
    pub fn redirect_view_reference(
        &mut self,
        id: NodeId,
        slot: usize,
        view: NodeId,
    ) -> Result<(), Error> {
        use crate::OperatorKind::{View, ViewDeclaration};
        if !matches!(
            self.node(view)?.operator_kind(),
            Some(View | ViewDeclaration)
        ) {
            return Err(Error::new(
                "view reference requires a definition or declaration",
            ));
        }
        let node = self
            .circuit
            .nodes
            .get_mut(&id)
            .ok_or_else(|| Error::new("unknown view reader"))?;
        let Some(Field::Array(inputs)) = node.fields.get_mut("inputs") else {
            return Err(Error::new("view reader requires stream inputs"));
        };
        let Some(Field::Object(port)) = inputs.get_mut(slot) else {
            return Err(Error::new("invalid view reader input"));
        };
        port.insert("operator".into(), Field::Reference(view));
        port.insert("outputNumber".into(), Field::Data(0.into()));
        self.changed.insert(id);
        self.rewired.insert(id);
        Ok(())
    }

    /// Reorder the root operators without changing their identities or membership.
    pub fn order_root_operators(&mut self, order: &[NodeId]) -> Result<(), Error> {
        let current = self.circuit.root().edges("allOperators")?;
        if current.len() != order.len()
            || current.iter().collect::<BTreeSet<_>>() != order.iter().collect::<BTreeSet<_>>()
        {
            return Err(Error::new(
                "new operator order must be a permutation of the root body",
            ));
        }
        let root = self.circuit.root_id();
        let body = self.circuit.nodes.get_mut(&root).expect("validated root");
        body.fields.insert(
            "allOperators".into(),
            Field::Array(order.iter().copied().map(Field::Definition).collect()),
        );
        self.changed.insert(root);
        Ok(())
    }

    /// Schedule newly appended operators at a specific point in their body.
    pub fn place_before(
        &mut self,
        body: NodeId,
        first: NodeId,
        before: NodeId,
    ) -> Result<(), Error> {
        let Some(Field::Array(order)) = self
            .circuit
            .nodes
            .get_mut(&body)
            .and_then(|n| n.fields.get_mut("allOperators"))
        else {
            return Err(Error::new("expected a circuit body"));
        };
        let first = order
            .iter()
            .position(|f| f.node_id() == Some(first))
            .ok_or_else(|| Error::new("missing first appended operator"))?;
        let before = order
            .iter()
            .position(|f| f.node_id() == Some(before))
            .ok_or_else(|| Error::new("missing scheduling point"))?;
        if first < before {
            return Err(Error::new("new operators must follow the scheduling point"));
        }
        order[before..].rotate_left(first - before);
        self.changed.insert(body);
        Ok(())
    }

    /// Redirect the stream-preserving view after collecting errors.
    pub fn replace_view_input(
        &mut self,
        view: NodeId,
        input: crate::OutputPort,
        ty: crate::TypeId,
    ) -> Result<(), Error> {
        if self.node(view)?.operator_kind() != Some(crate::OperatorKind::View) {
            return Err(Error::new("expected a view"));
        }
        self.node(input.operator)?;
        let node = self.circuit.nodes.get_mut(&view).expect("checked view");
        node.fields.insert(
            "inputs".into(),
            Field::Array(vec![Field::Object(
                [
                    ("operator".into(), Field::Reference(input.operator)),
                    ("outputNumber".into(), Field::Data(input.number.into())),
                ]
                .into(),
            )]),
        );
        node.fields
            .insert("outputType".into(), Field::Reference(ty.into()));
        self.changed.insert(view);
        Ok(())
    }
}

impl CircuitEditor {
    pub fn replace_stream_inputs(
        &mut self,
        id: NodeId,
        inputs: &[crate::OutputPort],
        output: crate::TypeId,
    ) -> Result<(), Error> {
        let op = self.node(id)?;
        if !matches!(
            op.operator_kind(),
            Some(
                crate::OperatorKind::View
                    | crate::OperatorKind::Sink
                    | crate::OperatorKind::Sum
                    | crate::OperatorKind::AtomicSum
                    | crate::OperatorKind::Subtract
            )
        ) {
            return Err(Error::new("expected a stream-preserving input replacement"));
        }
        if inputs.is_empty()
            || (!matches!(
                op.operator_kind(),
                Some(crate::OperatorKind::Sum | crate::OperatorKind::AtomicSum)
            ) && op.inputs().count() != inputs.len())
        {
            return Err(Error::new("stream replacement changes operator arity"));
        }
        for p in inputs {
            self.node(p.operator)?;
        }
        let ports = inputs
            .iter()
            .map(|p| {
                Field::Object(
                    [
                        ("operator".into(), Field::Reference(p.operator)),
                        ("outputNumber".into(), Field::Data(p.number.into())),
                    ]
                    .into(),
                )
            })
            .collect();
        let op = self.circuit.nodes.get_mut(&id).expect("validated operator");
        op.fields.insert("inputs".into(), Field::Array(ports));
        op.fields
            .insert("outputType".into(), Field::Reference(output.into()));
        self.changed.insert(id);
        Ok(())
    }
}

impl CircuitEditor {
    /// Replace the source at its existing schedule position. The resulting source
    /// supplies indexed data, errors, and its waterline on three distinct ports.
    pub fn replace_with_keyed_waterline(
        &mut self,
        site: NodeId,
        error_type: crate::TypeId,
        fields: impl IntoIterator<Item = (&'static str, Field)>,
    ) -> Result<(), Error> {
        if self.node(site)?.operator_kind() != Some(crate::OperatorKind::SourceMultiset) {
            return Err(Error::new(
                "an indexed waterline requires a multiset source",
            ));
        }
        let mut source = self.node(site)?.clone();
        source.class = NodeClass::Operator(crate::OperatorKind::InputMapWithWaterline);
        source.fields.shift_remove("sourceNamePosition");
        source.fields.shift_remove("sourceNameEmpty");
        for (k, v) in fields {
            crate::wire::check_links(&v, &self.circuit.nodes)?;
            source.fields.insert(k.into(), v);
        }
        self.implicit_outputs.insert(
            crate::OutputPort {
                operator: site,
                number: 1,
            },
            error_type.into(),
        );
        self.circuit.nodes.insert(site, source);
        self.changed.insert(site);
        Ok(())
    }
}
