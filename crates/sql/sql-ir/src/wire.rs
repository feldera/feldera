//! Decode graph identity before validating edges. A definition may occur after
//! its first reference, and a recursive circuit may refer back to an ancestor.

mod streaming;

use super::{Circuit, Error, Field, Node, NodeClass, NodeId, OperatorKind, OutputPort};
use indexmap::IndexMap;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

type Arena = BTreeMap<NodeId, Node>;

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
    for (&id, node) in &nodes {
        for value in node.fields.values() {
            check_links(value, &nodes)?;
        }
        if matches!(id, NodeId::Outer(_)) {
            validate_outer(id, node, &nodes)?;
        }
    }
    Ok(Circuit {
        live_relations: false,
        root,
        nodes: super::Shared(nodes.into()),
    })
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

fn outer_child(key: &str, port: bool) -> bool {
    matches!(key, "declarations" | "allOperators") || (key == "operator" && port)
}

pub(super) fn read(
    value: Value,
    outer: bool,
    nodes: &mut Arena,
    depth: usize,
) -> Result<Field, Error> {
    check_depth(depth)?;
    let mut object = match value {
        Value::Array(values) => {
            return values
                .into_iter()
                .map(|v| read(v, outer, nodes, depth + 1))
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
            Field::Data(value)
        } else {
            read(value, outer_child(&key, port), nodes, depth + 1)?
        };
        fields.insert(key, field);
    }
    if let Some((id, class)) = header {
        let class = NodeClass::decode(id, class)?;
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
    match field {
        Field::Reference(id) | Field::Definition(id) if !nodes.contains_key(id) => {
            Err(Error(format!("undefined node {id:?}")))
        }
        Field::Object(object) => object.values().try_for_each(|f| check_links(f, nodes)),
        Field::Array(array) => array.iter().try_for_each(|f| check_links(f, nodes)),
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

fn validate_outer(id: NodeId, node: &Node, nodes: &Arena) -> Result<(), Error> {
    for (field, required) in [
        (
            "allOperators",
            matches!(node.class(), "DBSPCircuit" | "DBSPNestedOperator"),
        ),
        ("declarations", node.class() == "DBSPCircuit"),
    ] {
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
