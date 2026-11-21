//! Public Gen-2 circuit encoding. Replay nodes carry extra source ranges and
//! cached types. Dropping those subtrees can move the first use of a shared
//! inner node, so resolve each edge against the arena as the writer visits it.

use super::{Circuit, Error, Field, NodeId};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;

impl Circuit {
    pub fn to_gen2_json(&self) -> Result<Value, Error> {
        Encoder {
            circuit: self,
            inner: BTreeSet::new(),
        }
        .node(self.root, 0)
    }
}

struct Encoder<'a> {
    circuit: &'a Circuit,
    inner: BTreeSet<NodeId>,
}

impl Encoder<'_> {
    fn node(&mut self, id: NodeId, depth: usize) -> Result<Value, Error> {
        if matches!(id, NodeId::Inner(_)) && !self.inner.insert(id) {
            return Ok(json!({"node": id.number()}));
        }
        if depth > 128 {
            return Err(Error("Gen-2 circuit exceeds 128 nested nodes".into()));
        }
        let node = &self.circuit.nodes[&id];
        let mut object = Map::new();
        object.insert("class".into(), node.class().into());
        object.insert("id".into(), id.number().into());
        for (name, field) in node.fields.iter() {
            let value = match (id, name.as_str()) {
                (
                    NodeId::Outer(_),
                    "provenance" | "sourceEmpty" | "sourcePosition" | "sourcePositions"
                    | "sourceNameEmpty" | "sourceNamePosition" | "query",
                ) => continue,
                (
                    NodeId::Inner(_),
                    "sourceEmpty" | "singleton" | "cachedEmptySetResult" | "originalStruct",
                ) => continue,
                (NodeId::Inner(_), "position") if !valid_position(field) => continue,
                (NodeId::Outer(_), "annotations") => {
                    let mut annotations = self.field(field, depth)?;
                    if let Some(list) = annotations.as_array_mut() {
                        for entry in list {
                            if entry.get("class").and_then(Value::as_str) == Some("JoinStrategy") {
                                entry.as_object_mut().unwrap().shift_remove("position");
                            }
                        }
                    }
                    annotations
                }
                (NodeId::Outer(_), "metadata") if node.operator_kind().is_some() => {
                    let mut metadata = self.field(field, depth)?;
                    public_operator_metadata(&mut metadata)?;
                    metadata
                }
                (NodeId::Outer(_), "metadata") if id == self.circuit.root => {
                    let mut schema = self.field(field, depth)?;
                    stringify_schema_nulls(&mut schema);
                    schema
                }
                _ => self.field(field, depth)?,
            };
            object.insert(name.clone(), value);
        }
        Ok(object.into())
    }

    fn field(&mut self, field: &Field, depth: usize) -> Result<Value, Error> {
        Ok(match field {
            Field::Reference(id @ NodeId::Outer(_)) => json!({"node": id.number()}),
            Field::Reference(id) | Field::Definition(id) => self.node(*id, depth + 1)?,
            Field::Data(value) => value.clone(),
            Field::Array(items) => items
                .iter()
                .map(|item| self.field(item, depth))
                .collect::<Result<Vec<_>, _>>()?
                .into(),
            Field::Object(fields) => fields
                .iter()
                .map(|(name, value)| Ok((name.clone(), self.field(value, depth)?)))
                .collect::<Result<Map<_, _>, Error>>()?
                .into(),
        })
    }
}

fn valid_position(position: &Field) -> bool {
    let Field::Object(fields) = position else {
        return false;
    };
    ["start_line_number", "start_column", "end_line_number", "end_column"].iter().all(|key|
        matches!(fields.get(*key), Some(Field::Data(number)) if number.as_u64().is_some_and(|n| n > 0)))
}

fn public_operator_metadata(metadata: &mut Value) -> Result<(), Error> {
    for key in ["columnMetadata", "columns"] {
        if let Some(columns) = metadata.get_mut(key).and_then(Value::as_array_mut) {
            for column in columns.iter_mut().filter_map(Value::as_object_mut) {
                column.shift_remove("position");
                column.shift_remove("defaultValuePosition");
            }
        }
    }
    if let Some(properties) = metadata
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    {
        for (key, value) in properties {
            if value.is_string() {
                continue;
            }
            let text = value
                .get("value")
                .and_then(Value::as_str)
                .ok_or_else(|| Error(format!("expected string value for property {key}")))?;
            *value = Value::String(text.into());
        }
    }
    Ok(())
}

// The public Java schema writer encodes JSON-null leaves as the string "null".
fn stringify_schema_nulls(value: &mut Value) {
    match value {
        Value::Null => *value = "null".into(),
        Value::Object(fields) => fields.values_mut().for_each(stringify_schema_nulls),
        Value::Array(items) => items.iter_mut().for_each(stringify_schema_nulls),
        _ => (),
    }
}
