//! Rebuild a circuit after substituting an earlier operator in the same body.
//! A removed operator may own the first definition of a surviving type or
//! relational origin. Emit those definitions at their first surviving use.

use super::{wire, BodyOrder, Circuit, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort};
use indexmap::IndexMap;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Surviving operators whose input changed, in constructor order. Some
/// constructors synthesize fresh expressions and consume generated names.
pub struct CircuitRewrite {
    pub circuit: Circuit,
    pub rebuilt_inputs: Vec<NodeId>,
}

#[derive(Clone, Copy)]
pub(super) enum Replacement {
    Equivalent(NodeId),
    Stream(OutputPort),
}

impl Replacement {
    fn operator(self) -> NodeId {
        match self {
            Self::Equivalent(id) => id,
            Self::Stream(port) => port.operator,
        }
    }

    fn port(self, original: OutputPort) -> OutputPort {
        match self {
            Self::Equivalent(operator) => OutputPort {
                operator,
                ..original
            },
            Self::Stream(port) => port,
        }
    }
}

impl Circuit {
    pub fn coalesce_operators(self, aliases: &BTreeMap<NodeId, NodeId>) -> Result<Self, Error> {
        Ok(self.coalesce_with_rewrites(aliases)?.circuit)
    }

    /// Also retain the constructor order for passes that carry the generated-name counter.
    pub fn coalesce_with_rewrites(
        self,
        aliases: &BTreeMap<NodeId, NodeId>,
    ) -> Result<CircuitRewrite, Error> {
        self.coalesce_pruning(aliases, &BTreeSet::new())
    }

    /// Substitute equivalent producers and discard unused operators in one visitation.
    /// Rebuild each surviving reader once even when both edits affect its inputs.
    pub fn coalesce_pruning(
        mut self,
        aliases: &BTreeMap<NodeId, NodeId>,
        discarded: &BTreeSet<NodeId>,
    ) -> Result<CircuitRewrite, Error> {
        let changed = self.remove_body_entries(discarded)?;
        if aliases.is_empty() && changed.is_empty() {
            return Ok(CircuitRewrite {
                circuit: self,
                rebuilt_inputs: Vec::new(),
            });
        }
        if aliases.is_empty() {
            return self.rebuild(
                &BTreeMap::new(),
                changed,
                &BTreeSet::new(),
                &BTreeMap::new(),
            );
        }
        let mut removed = 0;
        for parent in self.nodes.values() {
            let Some(Field::Array(body)) = parent.field("allOperators") else {
                continue;
            };
            let mut earlier = BTreeSet::new();
            for field in body {
                let id = field.node_id().expect("validated body");
                if let Some(base) = aliases.get(&id) {
                    if !earlier.contains(base)
                        || !self.nodes[&id]
                            .operator_kind()
                            .zip(self.nodes[base].operator_kind())
                            .is_some_and(|(a, b)| a.cse_family() == b.cse_family())
                    {
                        return Err(Error(format!(
                            "invalid operator substitution {id:?} -> {base:?}"
                        )));
                    }
                    removed += 1;
                } else {
                    earlier.insert(id);
                }
            }
        }
        if removed != aliases.len() {
            return Err(Error(
                "substitution includes an operator outside its body".into(),
            ));
        }
        let replacements = aliases
            .iter()
            .map(|(&id, &base)| (id, Replacement::Equivalent(base)))
            .collect();
        self.rebuild(&replacements, changed, &BTreeSet::new(), &BTreeMap::new())
    }

    /// Remove the selected unary, stream-preserving operators. The caller proves
    /// that forwarding each input preserves semantics. Edges retain their source port,
    /// including an error or nested-circuit output reached through a chain.
    pub fn bypass_unary_operators(
        self,
        removed: &BTreeSet<NodeId>,
    ) -> Result<CircuitRewrite, Error> {
        if removed.is_empty() {
            return Ok(CircuitRewrite {
                circuit: self,
                rebuilt_inputs: Vec::new(),
            });
        }
        let mut replacements = BTreeMap::<NodeId, Replacement>::new();
        let mut implicit_outputs = BTreeMap::new();
        for id in self.operator_order(BodyOrder::Postorder)? {
            if removed.contains(&id) {
                let node = &self.nodes[&id];
                if matches!(
                    node.operator_kind(),
                    Some(Op::Nested | Op::InputMapWithWaterline | Op::ControlledKeyFilter)
                ) {
                    return Err(Error(format!("cannot bypass multi-output operator {id:?}")));
                }
                let mut inputs = node.inputs();
                let input = inputs
                    .next()
                    .filter(|_| inputs.next().is_none())
                    .ok_or_else(|| {
                        Error(format!("operator {id:?} must have one input to bypass it"))
                    })?;
                // A keyed waterline source omits its error ZSet wrapper on the wire.
                // Its view inherits the exact wrapper from input.outputType(). Keep
                // that identity available after forwarding the last direct view.
                if input.number == 1
                    && node.operator_kind() == Some(Op::View)
                    && self.nodes[&input.operator].operator_kind()
                        == Some(Op::InputMapWithWaterline)
                {
                    implicit_outputs.insert(input, node.edge("outputType")?);
                }
                let source = replacements
                    .get(&input.operator)
                    .map_or(input, |r| r.port(input));
                replacements.insert(id, Replacement::Stream(source));
            }
        }
        if replacements.len() != removed.len() {
            return Err(Error(
                "cannot bypass an operator outside the circuit".into(),
            ));
        }
        self.rebuild(
            &replacements,
            BTreeSet::new(),
            &BTreeSet::new(),
            &implicit_outputs,
        )
    }

    pub(super) fn rebuild(
        mut self,
        aliases: &BTreeMap<NodeId, Replacement>,
        changed: BTreeSet<NodeId>,
        rewired: &BTreeSet<NodeId>,
        implicit_outputs: &BTreeMap<crate::OutputPort, NodeId>,
    ) -> Result<CircuitRewrite, Error> {
        let mut rebuilt_inputs = Vec::new();
        let inherited_types = self.inherited_types(
            aliases,
            changed,
            rewired,
            implicit_outputs,
            &mut rebuilt_inputs,
        )?;
        let mut writer = Writer {
            circuit: &self,
            aliases,
            inherited_types,
            emitted: BTreeSet::new(),
            nodes: BTreeMap::new(),
            origins: OriginDefinitions::default(),
        };
        for node in self.nodes.values() {
            if let Some(Field::Data(origin)) = node.field("provenance") {
                writer.origins.collect(origin)?;
            }
        }
        writer.node(self.root, 0)?;
        Ok(CircuitRewrite {
            circuit: wire::validated(self.root, writer.nodes)?,
            rebuilt_inputs,
        })
    }
    /// Java's withInputs constructors take the output type of the new input
    /// for stream-preserving operators. Sharing this identity is observable.
    fn inherited_types(
        &mut self,
        aliases: &BTreeMap<NodeId, Replacement>,
        mut rebuilt: BTreeSet<NodeId>,
        rewired: &BTreeSet<NodeId>,
        implicit_outputs: &BTreeMap<crate::OutputPort, NodeId>,
        rebuilt_inputs: &mut Vec<NodeId>,
    ) -> Result<BTreeMap<NodeId, NodeId>, Error> {
        let mut inherited = BTreeMap::new();
        for id in self.operator_order(BodyOrder::Postorder)? {
            let node = &self.nodes[&id];
            if let Some(Field::Array(body)) = node.field("allOperators") {
                if body
                    .iter()
                    .filter_map(Field::node_id)
                    .any(|child| rebuilt.contains(&child) || aliases.contains_key(&child))
                {
                    rebuilt.insert(id);
                }
            }
            // The input type of a newly fused chain depends on the reconstructed
            // producer, including a window or filter whose stream type changed.
            let new_chain = node.operator_kind() == Some(Op::Chain)
                && node.data("inputType").is_some_and(Value::is_null);
            let input_changed = new_chain
                || rewired.contains(&id)
                || node.inputs().any(|port| {
                    aliases.contains_key(&port.operator) || rebuilt.contains(&port.operator)
                });
            if (input_changed || rebuilt.contains(&id))
                && matches!(
                    node.operator_kind(),
                    Some(Op::Apply | Op::Apply2 | Op::ApplyN)
                )
            {
                let function = &self.nodes[&node.edge("function")?];
                let function_type = &self.nodes[&function.edge("type")?];
                inherited.insert(id, function_type.edge("resultType")?);
            }
            // A flattened sum can acquire a new first input even when all of its producers survive.
            if !input_changed && !(rebuilt.contains(&id) && node.operator_kind() == Some(Op::Sum)) {
                continue;
            }
            if !aliases.contains_key(&id) {
                rebuilt_inputs.push(id);
            }
            rebuilt.insert(id);
            if node.operator_kind() == Some(Op::ControlledKeyFilter) {
                // withInputs creates a new error-stream ZSet. Its element is
                // shared with the error closure, even if another operator
                // still references the old ZSet wrapper.
                let old = node.edge("errorType")?;
                let mut wrapper = self.nodes[&old].clone();
                let function = self.nodes[&node.edge("error")?].edge("type")?;
                let result = self.nodes[&function].edge("resultType")?;
                wrapper
                    .fields
                    .insert("elementType".into(), Field::Reference(result));
                let fresh = self.insert_inner(wrapper)?;
                self.nodes
                    .get_mut(&id)
                    .expect("visited operator")
                    .fields
                    .insert("errorType".into(), Field::Reference(fresh));
            }
            let node = &self.nodes[&id];
            if !new_chain
                && (node.field("outputType").is_none()
                    || !matches!(
                        node.operator_kind(),
                        Some(
                            Op::AntiJoin
                                | Op::AtomicSum
                                | Op::Delay
                                | Op::Delta
                                | Op::DelayedIntegral
                                | Op::UpsertFeedback
                                | Op::WeightValidator
                                | Op::ControlledKeyFilter
                                | Op::Differentiate
                                | Op::Distinct
                                | Op::Filter
                                | Op::Integrate
                                | Op::IndexedTopK
                                | Op::IntegrateTraceRetainKeys
                                | Op::IntegrateTraceRetainNValues
                                | Op::IntegrateTraceRetainValues
                                | Op::Negate
                                | Op::Noop
                                | Op::Positive
                                | Op::Sink
                                | Op::StreamAntiJoin
                                | Op::StreamDistinct
                                | Op::Subtract
                                | Op::Sum
                                | Op::View
                                | Op::Window
                        )
                    ))
            {
                continue;
            }
            let mut port = node.inputs().next().expect("rewritten input");
            let mut visited = BTreeSet::new();
            loop {
                if !visited.insert(port) {
                    return Err(Error(format!("cyclic nested output at {port:?}")));
                }
                if let Some(replacement) = aliases.get(&port.operator) {
                    port = replacement.port(port);
                }
                let source = port.operator;
                let operator = &self.nodes[&source];
                if operator.operator_kind() == Some(Op::Nested) {
                    let Some(Field::Array(outputs)) = operator.field("internalOutputs") else {
                        unreachable!()
                    };
                    port = super::wire::output(&outputs[port.number])
                        .expect("validated nested output");
                    continue;
                }
                let ty = match port.number {
                    0 => inherited
                        .get(&source)
                        .copied()
                        .or_else(|| operator.field("outputType").and_then(Field::node_id)),
                    1 => operator
                        .field("errorType")
                        .and_then(Field::node_id)
                        .or_else(|| {
                            implicit_outputs
                                .get(&crate::OutputPort {
                                    operator: source,
                                    number: 1,
                                })
                                .copied()
                        }),
                    _ => None,
                }
                .ok_or_else(|| Error(format!("cannot inherit output type of {port:?}")))?;
                if new_chain {
                    let filters_only = matches!(node.field("kinds"), Some(Field::Array(kinds))
                        if kinds.iter().all(|entry| matches!(entry, Field::Data(k) if k == "Filter")));
                    self.nodes
                        .get_mut(&id)
                        .expect("visited chain")
                        .fields
                        .insert("inputType".into(), Field::Reference(ty));
                    if filters_only {
                        inherited.insert(id, ty);
                    }
                    break;
                }
                let indexed_result = if node.operator_kind() == Some(Op::IndexedTopK) {
                    node.field("outputProducer").and_then(Field::node_id)
                } else {
                    None
                };
                let output = if let Some(producer) = indexed_result {
                    let function_type = self.nodes[&producer].edge("type")?;
                    let row = self.nodes[&function_type].edge("resultType")?;
                    let mut indexed = self.nodes[&ty].clone();
                    indexed
                        .fields
                        .insert("elementType".into(), Field::Reference(row));
                    self.insert_inner(indexed)?
                } else {
                    ty
                };
                inherited.insert(id, output);
                break;
            }
        }
        Ok(inherited)
    }

    fn insert_inner(&mut self, node: super::Node) -> Result<NodeId, Error> {
        let next = match self.nodes.last_key_value() {
            Some((NodeId::Inner(n), _)) => n.checked_add(1),
            _ => Some(0),
        }
        .ok_or_else(|| Error::new("inner node identity exhausted"))?;
        let fresh = NodeId::Inner(next);
        self.nodes.insert(fresh, node);
        Ok(fresh)
    }
}

struct Writer<'a> {
    circuit: &'a Circuit,
    aliases: &'a BTreeMap<NodeId, Replacement>,
    inherited_types: BTreeMap<NodeId, NodeId>,
    emitted: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    origins: OriginDefinitions<'a>,
}

impl Writer<'_> {
    /// Place each surviving definition at its first use. Build the typed arena directly
    /// so a rewrite does not materialize another complete wire document.
    fn node(&mut self, id: NodeId, depth: usize) -> Result<Field, Error> {
        wire::check_depth(depth)?;
        let id = self.aliases.get(&id).map_or(id, |r| r.operator());
        if !self.emitted.insert(id) {
            return Ok(Field::Reference(id));
        }
        let node = self
            .circuit
            .node(id)
            .ok_or_else(|| Error::new(format!("rewritten circuit references missing {id:?}")))?;
        if matches!(id, NodeId::Inner(_)) {
            // Most expressions survive unchanged. Traverse their edges to place
            // definitions, but copy the field map only when an edge moves.
            let mut rebuilt = node.clone();
            for (name, field) in node.fields.iter() {
                if name != "annotations" {
                    if let Some(value) = self.relocated(field, depth + 1)? {
                        rebuilt.fields.insert(name.clone(), value);
                    }
                }
            }
            wire::insert_node(&mut self.nodes, id, rebuilt)?;
            return Ok(Field::Definition(id));
        }
        let mut fields = IndexMap::with_capacity(node.fields.len());
        for (name, field) in node.fields.iter() {
            let value = match (name.as_str(), field) {
                ("inputs", _) if node.operator_kind() == Some(Op::Nested) => {
                    let mut inputs = Vec::new();
                    for delta in node.edges("allOperators")? {
                        if self.aliases.contains_key(&delta) {
                            continue;
                        }
                        let child = &self.circuit.nodes[&delta];
                        if child.operator_kind() == Some(Op::Delta) {
                            let Some(Field::Array(ports)) = child.field("inputs") else {
                                return Err(Error::new("missing delta inputs"));
                            };
                            let [port] = ports.as_slice() else {
                                return Err(Error::new("delta requires one external input"));
                            };
                            inputs.push(self.field(port, depth + 2)?);
                        }
                    }
                    Field::Array(inputs)
                }
                ("allOperators", Field::Array(body)) => {
                    let mut kept = Vec::with_capacity(body.len());
                    for child in body {
                        let id = child.node_id().expect("validated body");
                        if !self.aliases.contains_key(&id) {
                            kept.push(self.node(id, depth + 2)?);
                        }
                    }
                    Field::Array(kept)
                }
                ("outputType", _) if self.inherited_types.contains_key(&id) => {
                    self.node(self.inherited_types[&id], depth + 1)?
                }
                ("provenance", Field::Data(origin)) => {
                    Field::Data(self.origins.emit(origin, false)?)
                }
                ("annotations", _) => field.clone(),
                ("metadata", _) if id == self.circuit.root => field.clone(),
                _ => self.field(field, depth + 1)?,
            };
            fields.insert(name.clone(), value);
        }
        wire::insert_node(&mut self.nodes, id, Node::new(node.class.clone(), fields))?;
        Ok(Field::Definition(id))
    }

    /// Visit an inner property and return its new representation only if it changed.
    fn relocated(&mut self, field: &Field, depth: usize) -> Result<Option<Field>, Error> {
        wire::check_depth(depth)?;
        match field {
            Field::Array(items) => {
                let mut edited = None;
                for (i, item) in items.iter().enumerate() {
                    if let Some(value) = self.relocated(item, depth + 1)? {
                        edited.get_or_insert_with(|| items.clone())[i] = value;
                    }
                }
                Ok(edited.map(Field::Array))
            }
            Field::Object(fields) if OutputPort::from_field(field).is_none() => {
                let mut edited = None;
                for (i, child) in fields.values().enumerate() {
                    if let Some(value) = self.relocated(child, depth + 1)? {
                        edited.get_or_insert_with(|| fields.clone())[i] = value;
                    }
                }
                Ok(edited.map(Field::Object))
            }
            Field::Data(value) if !value.is_object() && !value.is_array() => Ok(None),
            _ => {
                let value = self.field(field, depth)?;
                Ok((value != *field).then_some(value))
            }
        }
    }

    fn field(&mut self, field: &Field, depth: usize) -> Result<Field, Error> {
        wire::check_depth(depth)?;
        Ok(match field {
            Field::Reference(id @ NodeId::Outer(_)) => {
                Field::Reference(self.aliases.get(id).map_or(*id, |r| r.operator()))
            }
            Field::Reference(id) | Field::Definition(id) => self.node(*id, depth)?,
            Field::Array(items) => Field::Array(
                items
                    .iter()
                    .map(|f| self.field(f, depth + 1))
                    .collect::<Result<_, _>>()?,
            ),
            Field::Object(fields) => {
                let mut object = IndexMap::with_capacity(fields.len());
                for (name, child) in fields {
                    object.insert(name.clone(), self.field(child, depth + 1)?);
                }
                if let Some(port) = OutputPort::from_field(field) {
                    if let Some(Replacement::Stream(source)) = self.aliases.get(&port.operator) {
                        object.insert("outputNumber".into(), Field::Data(source.number.into()));
                    }
                }
                Field::Object(object)
            }
            // Builders can supply structured data such as source ranges and column indexes.
            // Normalize these fields exactly as the decoder does on a wire round trip.
            Field::Data(value @ (Value::Array(_) | Value::Object(_))) => {
                wire::read(value.clone(), false, &mut self.nodes, depth)?
            }
            Field::Data(value) => Field::Data(value.clone()),
        })
    }
}

#[derive(Default)]
struct OriginDefinitions<'a> {
    origins: BTreeMap<u64, &'a Value>,
    relations: BTreeMap<u64, &'a Value>,
    emitted_origins: BTreeMap<u64, usize>,
    emitted_relations: BTreeMap<u64, usize>,
}

impl<'a> OriginDefinitions<'a> {
    fn collect(&mut self, origin: &'a Value) -> Result<(), Error> {
        if let Some(id) = origin["id"].as_u64() {
            if self.origins.insert(id, origin).is_some() {
                return Err(Error(format!("duplicate origin {id}")));
            }
            if let Some(id) = origin["relation"]["id"].as_u64() {
                if self.relations.insert(id, &origin["relation"]).is_some() {
                    return Err(Error(format!("duplicate relation {id}")));
                }
            }
            if let Some(children) = origin["children"].as_array() {
                for child in children {
                    self.collect(child)?;
                }
            }
        }
        Ok(())
    }

    fn emit(&mut self, wire: &Value, relation: bool) -> Result<Value, Error> {
        let id = wire["id"]
            .as_u64()
            .or_else(|| wire["ref"].as_u64())
            .ok_or_else(|| Error("missing provenance identity".into()))?;
        let (definitions, emitted) = if relation {
            (&self.relations, &mut self.emitted_relations)
        } else {
            (&self.origins, &mut self.emitted_origins)
        };
        if let Some(previous) = emitted.get(&id) {
            return Ok(json!({"ref":previous}));
        }
        // Java numbers each identity at its first surviving use in the new circuit.
        let ordinal = emitted.len();
        emitted.insert(id, ordinal);
        let original = *definitions
            .get(&id)
            .ok_or_else(|| Error(format!("undefined provenance identity {id}")))?;
        let mut result = original.clone();
        result["id"] = ordinal.into();
        if !relation {
            if let Some(child) = original.get("relation") {
                result["relation"] = self.emit(child, true)?;
            }
            if let Some(children) = original["children"].as_array() {
                result["children"] = children
                    .iter()
                    .map(|child| self.emit(child, false))
                    .collect::<Result<Vec<_>, _>>()?
                    .into();
            }
        }
        Ok(result)
    }
}
