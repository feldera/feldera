//! Decode graph identity before validating edges. A definition may occur after
//! its first reference, and a recursive circuit may refer back to an ancestor.

mod streaming;
pub use streaming::LiveCircuitDecoder;

use super::{Circuit, Error, Field, Node, NodeClass, NodeId, OperatorKind, OutputPort};
use indexmap::IndexMap;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};

type Arena = BTreeMap<NodeId, Node>;

/// A bitmap for one node namespace during whole-circuit validation. For widely
/// spaced IDs, use the arena directly rather than allocating for the gaps.
struct IdBitmap {
    first_word: u64,
    words: Vec<u64>,
}

impl IdBitmap {
    fn from_range(nodes: &Arena, range: std::ops::RangeInclusive<NodeId>) -> Option<Self> {
        let ids = nodes.range(range);
        let first_word = ids.clone().next()?.0.number() / 64;
        let last_word = ids.clone().next_back()?.0.number() / 64;
        let len = usize::try_from(last_word - first_word + 1).ok()?;
        if len > nodes.len() {
            return None;
        }
        let mut words = vec![0; len];
        for (id, _) in ids {
            let n = id.number();
            words[(n / 64 - first_word) as usize] |= 1 << (n % 64);
        }
        Some(Self { first_word, words })
    }

    fn contains(&self, number: u64) -> bool {
        (number / 64)
            .checked_sub(self.first_word)
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| self.words.get(i))
            .is_some_and(|bits| bits & (1 << (number % 64)) != 0)
    }
}

pub(super) fn decode(value: Value) -> Result<Circuit, Error> {
    if value.get("class").and_then(Value::as_str) != Some("DBSPCircuit") {
        return Err(Error("expected a DBSPCircuit root".into()));
    }
    let mut nodes = Arena::new();
    let root = read(value, true, &mut nodes, 0)?
        .node_id()
        .ok_or_else(|| Error("circuit needs a numeric id".into()))?;
    validated(root, nodes)
}

pub(super) fn validated(root: NodeId, nodes: Arena) -> Result<Circuit, Error> {
    let outer = IdBitmap::from_range(&nodes, NodeId::Outer(0)..=NodeId::Outer(u64::MAX));
    let inner = IdBitmap::from_range(&nodes, NodeId::Inner(0)..=NodeId::Inner(u64::MAX));
    let defined = |id: &NodeId| {
        let bitmap = match id {
            NodeId::Outer(_) => &outer,
            NodeId::Inner(_) => &inner,
        };
        bitmap
            .as_ref()
            .map_or_else(|| nodes.contains_key(id), |b| b.contains(id.number()))
    };
    for (&id, node) in &nodes {
        for value in node.fields.values() {
            check_references(value, &defined)?;
        }
        if matches!(id, NodeId::Outer(_)) {
            validate_outer(id, node, &nodes)?;
        }
    }
    validate_bodies(root, &nodes)?;
    Ok(Circuit {
        live_relations: false,
        root,
        nodes: super::Shared(nodes.into()),
    })
}

impl Circuit {
    /// Decode a checkpoint into the arena in short runs. Only an inner
    /// definition starts a new run, so deeply nested expressions preserve the
    /// wire definition sites and ordinary JSON fields retain their depth limit.
    pub fn from_checkpoint_json(value: Value) -> Result<Self, Error> {
        let mut nodes = Arena::new();
        let mut deferred = Vec::new();
        let Field::Definition(root) = read_field(value, true, &mut nodes, 0, Some(&mut deferred))?
        else {
            return Err(Error::new("expected a circuit root definition"));
        };
        if nodes[&root].class() != "DBSPCircuit" {
            return Err(Error::new("expected a DBSPCircuit root"));
        }
        while let Some(value) = deferred.pop() {
            // Only the actual circuit root owns opaque schema metadata.
            read_field(value, false, &mut nodes, 1, Some(&mut deferred))?;
        }
        validated(root, nodes)
    }

    pub(super) fn attach_inner_roots(
        mut self,
        mut extra: Arena,
        roots: Vec<NodeId>,
    ) -> Result<(Self, Vec<NodeId>), Error> {
        if let Some((&first, _)) = extra.first_key_value() {
            if matches!(first, NodeId::Outer(_)) {
                return Err(Error(format!("invalid additional inner node {first:?}")));
            }
            // A fresh fragment normally supplies all inner definitions. Only
            // check for collisions if its IDs overlap the existing arena.
            if self
                .nodes
                .last_key_value()
                .is_some_and(|(&last, _)| last >= first)
            {
                if let Some(id) = extra.keys().find(|id| self.nodes.contains_key(id)) {
                    return Err(Error(format!("invalid additional inner node {id:?}")));
                }
            }
        }

        let inner = IdBitmap::from_range(&extra, NodeId::Inner(0)..=NodeId::Inner(u64::MAX));
        let defined = |id: &NodeId| {
            let in_batch = match (id, &inner) {
                (NodeId::Inner(n), Some(bits)) => bits.contains(*n),
                _ => extra.contains_key(id),
            };
            in_batch || self.nodes.contains_key(id)
        };
        for node in extra.values() {
            for field in node.fields.values() {
                check_references(field, &defined)?;
            }
        }
        for &root in &roots {
            check_references(&Field::Reference(root), &defined)?;
        }

        // Preserve the larger sorted map. For a closure batch, only the
        // circuit header needs insertion into the freshly decoded arena.
        if extra.len() > self.nodes.len() {
            std::mem::swap(&mut extra, &mut *self.nodes);
        }
        if !extra.is_empty() {
            self.nodes.extend(extra);
        }
        Ok((self, roots))
    }
}

pub(super) fn check_depth(depth: usize) -> Result<(), Error> {
    if depth > 128 {
        return Err(Error("circuit exceeds 128 nested fields".into()));
    }
    Ok(())
}

fn opaque_field(key: &str, depth: usize, outer_node: bool) -> bool {
    key == "annotations" || (key == "metadata" && depth == 0) || (key == "provenance" && outer_node)
}

fn check_container_depth(depth: usize) -> Result<(), Error> {
    if depth >= 127 {
        return Err(Error::new("recursion limit exceeded in checkpoint field"));
    }
    Ok(())
}

// Opaque fields are copied as plain JSON. Keep their local depth bounded
// even when another part of a checkpoint contains a deep expression chain.
fn check_opaque_depth(value: &Value, depth: usize) -> Result<(), Error> {
    let mut pending = vec![(value, depth)];
    while let Some((value, depth)) = pending.pop() {
        match value {
            Value::Array(items) => {
                check_container_depth(depth)?;
                pending.extend(items.iter().map(|child| (child, depth + 1)));
            }
            Value::Object(fields) => {
                check_container_depth(depth)?;
                pending.extend(fields.values().map(|child| (child, depth + 1)));
            }
            _ => (),
        }
    }
    Ok(())
}

fn outer_child(key: &str, port: bool) -> bool {
    matches!(key, "declarations" | "allOperators") || (key == "operator" && port)
}

pub(super) fn read(
    value: Value,
    outer: bool,
    nodes: &mut Arena,
    depth: usize,
) -> Result<Field, Error> {
    read_field(value, outer, nodes, depth, None)
}

fn read_field(
    value: Value,
    outer: bool,
    nodes: &mut Arena,
    depth: usize,
    mut deferred: Option<&mut Vec<Value>>,
) -> Result<Field, Error> {
    check_depth(depth)?;
    if deferred.is_some() && (value.is_object() || value.is_array()) {
        check_container_depth(depth)?;
    }
    let mut object = match value {
        Value::Array(values) => {
            return values
                .into_iter()
                .map(|v| read_field(v, outer, nodes, depth + 1, deferred.as_deref_mut()))
                .collect::<Result<_, _>>()
                .map(Field::Array);
        }
        Value::Object(object) => object,
        other => return Ok(Field::Data(other)),
    };
    let id = |value: Value| {
        value
            .as_u64()
            .map(|id| {
                if outer {
                    NodeId::Outer(id)
                } else {
                    NodeId::Inner(id)
                }
            })
            .ok_or_else(|| Error("node id must be a nonnegative integer".into()))
    };
    if object.len() == 1 {
        if let Some(value) = object.shift_remove("node") {
            return Ok(Field::Reference(id(value)?));
        }
    }

    if !outer && depth >= 48 && object.contains_key("class") {
        if let (Some(queue), Some(number)) =
            (deferred.as_mut(), object.get("id").and_then(Value::as_u64))
        {
            queue.push(Value::Object(object));
            return Ok(Field::Definition(NodeId::Inner(number)));
        }
    }

    let header = if object.contains_key("class") && object.contains_key("id") {
        let Some(Value::String(class)) = object.shift_remove("class") else {
            return Err(Error("node class must be a string".into()));
        };
        Some((id(object.shift_remove("id").expect("checked id"))?, class))
    } else {
        None
    };
    let port = object.contains_key("outputNumber");
    let mut fields = IndexMap::new();
    for (key, value) in object {
        let field = if opaque_field(&key, depth, outer && header.is_some()) {
            if deferred.is_some() {
                check_opaque_depth(&value, depth + 1)?;
            }
            Field::Data(value)
        } else {
            read_field(
                value,
                outer_child(&key, port),
                nodes,
                depth + 1,
                deferred.as_deref_mut(),
            )?
        };
        fields.insert(key, field);
    }
    if let Some((id, class)) = header {
        let class = NodeClass::decode(id, class.into())?;
        insert_node(nodes, id, Node::new(class, fields))?;
        Ok(Field::Definition(id))
    } else {
        Ok(Field::Object(fields))
    }
}

pub(super) fn insert_node(nodes: &mut Arena, id: NodeId, node: Node) -> Result<(), Error> {
    if nodes.insert(id, node).is_some() {
        return Err(Error(format!("duplicate circuit node {id:?}")));
    }
    Ok(())
}

pub(super) fn check_links(field: &Field, nodes: &Arena) -> Result<(), Error> {
    check_references(field, &|id| nodes.contains_key(id))
}

fn check_references(field: &Field, defined: &impl Fn(&NodeId) -> bool) -> Result<(), Error> {
    match field {
        Field::Reference(id) | Field::Definition(id) if !defined(id) => {
            Err(Error(format!("undefined node {id:?}")))
        }
        Field::Object(object) => object
            .values()
            .try_for_each(|f| check_references(f, defined)),
        Field::Array(array) => array.iter().try_for_each(|f| check_references(f, defined)),
        _ => Ok(()),
    }
}

fn array<'a>(node: &'a Node, name: &str) -> Result<&'a [Field], Error> {
    match node.field(name) {
        Some(Field::Array(fields)) => Ok(fields),
        _ => Err(Error(format!(
            "{} requires an array in {name}",
            node.class()
        ))),
    }
}

pub(super) fn output(port: &Field) -> Option<OutputPort> {
    let Field::Object(fields) = port else {
        return None;
    };
    let operator @ NodeId::Outer(_) = fields.get("operator")?.node_id()? else {
        return None;
    };
    let Field::Data(number) = fields.get("outputNumber")? else {
        return None;
    };
    Some(OutputPort {
        operator,
        number: usize::try_from(number.as_u64()?).ok()?,
    })
}

pub(super) fn has_output(source: &Node, number: usize) -> Result<bool, Error> {
    Ok(match source.operator_kind() {
        Some(OperatorKind::Nested) => array(source, "internalOutputs")?
            .get(number)
            .is_some_and(|field| !matches!(field, Field::Data(Value::Null))),
        Some(OperatorKind::InputMapWithWaterline) => number < 3,
        Some(OperatorKind::ControlledKeyFilter) => number < 2,
        None => false,
        _ => number == 0,
    })
}

fn validate_bodies(root: NodeId, nodes: &Arena) -> Result<(), Error> {
    // Each body adds an array and an object to the nested wire form.
    const MAX_NESTED_BODIES: usize = 64;
    let mut stack = vec![(root, array(&nodes[&root], "allOperators")?.iter())];
    let mut members = HashSet::new();
    while let Some((body, slots)) = stack.last_mut() {
        let Some(slot) = slots.next() else {
            stack.pop();
            continue;
        };
        let id = slot.node_id().expect("validated body slot");
        if !members.insert(id) {
            let parent = body.number();
            let reason = if parent == id.number() {
                format!("nested operator {parent} contains itself")
            } else if stack.iter().any(|(ancestor, _)| *ancestor == id) {
                format!("nested operator {parent} contains ancestor {}", id.number())
            } else {
                format!("operator {} occurs in multiple body slots", id.number())
            };
            return Err(Error(format!(
                "{reason}. Keep one body slot per operator under the circuit root."
            )));
        }
        if nodes[&id].operator_kind() == Some(OperatorKind::Nested) {
            if stack.len() > MAX_NESTED_BODIES {
                return Err(Error(format!(
                    "operator {} exceeds {MAX_NESTED_BODIES} nested circuit bodies. Reduce body nesting.",
                    id.number()
                )));
            }
            stack.push((id, array(&nodes[&id], "allOperators")?.iter()));
        }
    }
    for (&id, node) in nodes.range(NodeId::Outer(0)..=NodeId::Outer(u64::MAX)) {
        if node.operator_kind().is_some() && !members.contains(&id) {
            return Err(Error(format!(
                "operator {} has no body reachable from the circuit root. Add it to a reachable body.",
                id.number()
            )));
        }
    }
    Ok(())
}

fn validate_outer(id: NodeId, node: &Node, nodes: &Arena) -> Result<(), Error> {
    for (field, required) in [
        (
            "allOperators",
            matches!(node.class(), "DBSPCircuit" | "DBSPNestedOperator"),
        ),
        ("declarations", node.class() == "DBSPCircuit"),
    ] {
        if field == "allOperators" && !required && node.field(field).is_some() {
            return Err(Error(format!(
                "{} {} cannot own a circuit body. Move its operators to a circuit or nested operator.",
                node.class(), id.number()
            )));
        }
        if required || node.field(field).is_some() {
            for item in array(node, field)? {
                let Some(dep @ NodeId::Outer(_)) = item.node_id() else {
                    return Err(Error(format!("{id:?}.{field} requires outer nodes")));
                };
                let child = &nodes[&dep];
                let class = child.class();
                if (field == "declarations" && class != "DBSPDeclaration")
                    || (field == "allOperators" && child.operator_kind().is_none())
                {
                    return Err(Error(format!("{id:?}.{field} cannot contain {class}")));
                }
            }
        }
    }
    for field in ["inputs", "internalOutputs"] {
        if node.field(field).is_none() {
            continue;
        }
        for (index, link) in array(node, field)?.iter().enumerate() {
            if field == "internalOutputs" && matches!(link, Field::Data(Value::Null)) {
                continue;
            }
            let port = output(link)
                .ok_or_else(|| Error(format!("invalid port {id:?}.{field}[{index}]")))?;
            let source = &nodes[&port.operator];
            if !has_output(source, port.number)? {
                return Err(Error(format!(
                    "{id:?}.{field}[{index}] uses absent output {port:?}"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn matches_node(id: NodeId, value: &Value, nodes: &Arena) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let node = &nodes[&id];
    object.len() == node.fields.len() + 2
        && object.get("class").and_then(Value::as_str) == Some(node.class())
        && object.get("id").and_then(Value::as_u64) == Some(id.number())
        && node.fields.iter().all(|(k, field)| {
            object
                .get(k)
                .is_some_and(|v| matches_field(field, v, nodes))
        })
}

fn matches_field(field: &Field, value: &Value, nodes: &Arena) -> bool {
    match field {
        Field::Data(data) => data == value,
        Field::Definition(id) => matches_node(*id, value, nodes),
        Field::Reference(id) => value.as_object().is_some_and(|object| {
            object.len() == 1 && object.get("node").and_then(Value::as_u64) == Some(id.number())
        }),
        Field::Array(items) => value.as_array().is_some_and(|values| {
            items.len() == values.len()
                && items
                    .iter()
                    .zip(values)
                    .all(|(item, v)| matches_field(item, v, nodes))
        }),
        Field::Object(fields) => value.as_object().is_some_and(|object| {
            fields.len() == object.len()
                && fields
                    .iter()
                    .all(|(k, f)| object.get(k).is_some_and(|v| matches_field(f, v, nodes)))
        }),
    }
}

pub(super) fn encode_node(id: NodeId, nodes: &Arena) -> Value {
    encode_with(id, nodes, &mut |_| false)
}

pub(super) fn encode_with(
    id: NodeId,
    nodes: &Arena,
    retain: &mut impl FnMut(NodeId) -> bool,
) -> Value {
    let node = &nodes[&id];
    let mut object = Map::new();
    object.insert("class".into(), Value::String(node.class().into()));
    object.insert("id".into(), id.number().into());
    object.extend(
        node.fields
            .iter()
            .map(|(key, field)| (key.clone(), encode(field, nodes, retain))),
    );
    Value::Object(object)
}

fn encode(field: &Field, nodes: &Arena, retain: &mut impl FnMut(NodeId) -> bool) -> Value {
    match field {
        Field::Definition(id) | Field::Reference(id) => {
            if retain(*id) || matches!(field, Field::Reference(_)) {
                json!({ "node": id.number() })
            } else {
                encode_with(*id, nodes, retain)
            }
        }
        Field::Data(value) => value.clone(),
        Field::Array(fields) => Value::Array(
            fields
                .iter()
                .map(|field| encode(field, nodes, retain))
                .collect(),
        ),
        Field::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, field)| (key.clone(), encode(field, nodes, retain)))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(id: u64, members: Value) -> Value {
        json!({"class":"DBSPNestedOperator", "id":id, "inputs":[], "allOperators":members,
               "internalOutputs":[], "outputViews":[]})
    }

    fn check_decoders(graph: Value, reason: &str) {
        let bytes = serde_json::to_vec(&graph).unwrap();
        for result in [
            Circuit::from_json(graph.clone()),
            Circuit::from_checkpoint_json(graph),
            Circuit::from_json_bytes(&bytes),
            Circuit::from_live_json_bytes(&bytes),
            LiveCircuitDecoder::default().decode(&bytes),
        ] {
            let error = result
                .expect_err("accepted invalid body membership")
                .to_string();
            assert!(error.contains(reason), "{error}");
        }
    }

    #[test]
    fn all_decoders_require_a_rooted_body_tree() {
        let leaf = json!({"class":"DBSPMapOperator","id":8,"inputs":[]});
        for (body, reason) in [
            (
                json!([nested(5, json!([{"node":5}]))]),
                "operator 5 contains itself",
            ),
            (
                json!([nested(5, json!([nested(6, json!([{"node":5}]))]))]),
                "ancestor 5",
            ),
            (
                json!([nested(5, json!([leaf])), nested(6, json!([{"node":8}]))]),
                "operator 8 occurs",
            ),
            (json!([leaf, {"node":8}]), "operator 8 occurs"),
            (
                json!([{"class":"DBSPMapOperator","id":1,"inputs":[{"operator":leaf,"outputNumber":0}]}]),
                "operator 8 has no body",
            ),
        ] {
            let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":body});
            check_decoders(graph, reason);
        }
    }

    #[test]
    fn nested_view_can_read_its_own_declaration() {
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[{
            "class":"DBSPNestedOperator","id":1,"inputs":[],
            "annotations":[{"class":"CompactName","name":"body"}],
            "outputViews":[{"name":"v"}],"internalOutputs":[{"operator":{"node":3},"outputNumber":0}],
            "allOperators":[
                {"class":"DBSPViewDeclarationOperator","id":2,"inputs":[],"tableName":{"name":"v-decl"},
                 "annotations":[{"class":"CompactName","name":"decl"}],
                 "provenance":{"kind":"CalciteEmptyRel","id":0,
                               "position":crate::provenance::SourceRange::default(),"positions":[]}},
                {"class":"DBSPViewOperator","id":3,"viewName":{"name":"v"},
                 "inputs":[{"operator":{"node":2},"outputNumber":0}],
                 "annotations":[{"class":"CompactName","name":"view"}],"provenance":{"ref":0}}
            ]
        }]});
        let mir = Circuit::from_checkpoint_json(graph).unwrap().mir().unwrap();
        assert_eq!(
            mir["body"]["decl"]["backedges"],
            json!([{"node":"view","output":0}])
        );
        assert_eq!(
            mir["body"]["view"]["inputs"],
            json!([{"node":"decl","output":0}])
        );
    }

    #[test]
    fn body_depth_is_bounded_when_definitions_are_flat() {
        let graph = |depth| {
            let ports: Vec<_> = (1..=depth)
                .map(|id| {
                    let members = if id < depth {
                        json!([{"node":id + 1}])
                    } else {
                        json!([])
                    };
                    let mut op = nested(id, members);
                    op["annotations"] = json!([{"class":"CompactName","name":"n"}]);
                    op["internalOutputs"] = json!([{"operator":{"node":100},"outputNumber":0}]);
                    op["outputViews"] = json!([{"name":"v"}]);
                    json!({"operator":op,"outputNumber":0})
                })
                .collect();
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"node":1},{"class":"DBSPSumOperator","id":100,"inputs":ports,
                 "annotations":[{"class":"CompactName","name":"sum"}],
                 "provenance":{"kind":"CalciteEmptyRel","id":0,
                               "position":crate::provenance::SourceRange::default(),"positions":[]}}
            ]})
        };
        check_decoders(graph(65), "operator 65 exceeds 64 nested circuit bodies");
        let circuit = Circuit::from_checkpoint_json(graph(64)).unwrap();
        let mir = circuit.mir().unwrap();
        let mut body = &mir;
        for _ in 0..64 {
            body = &body["n"];
            assert_eq!(body["outputs"], json!([{"node":"sum","output":0}]));
        }
        assert!(body.get("n").is_none());
    }
}
