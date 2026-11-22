//! Public Gen-2 circuit encoding. Replay nodes carry extra source ranges and
//! cached types. Dropping those subtrees can move the first use of a shared
//! inner node, so resolve each edge against the arena as the writer visits it.

use super::{Circuit, Error, Field, NodeId};
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;

impl Circuit {
    pub fn to_gen2_json(&self) -> Result<Value, Error> {
        enum Step<'a> {
            Node(NodeId, usize),
            Field(&'a Field, usize),
            Array(usize),
            Object(Vec<&'a str>),
            Definition(NodeId, Vec<&'a str>),
        }

        let mut inner = BTreeSet::new();
        let mut pending = vec![Step::Node(self.root, 0)];
        let mut values = Vec::new();
        while let Some(step) = pending.pop() {
            let value = match step {
                Step::Node(id, depth) => {
                    if matches!(id, NodeId::Inner(_)) && !inner.insert(id) {
                        values.push(json!({"node": id.number()}));
                        continue;
                    }
                    if depth > 1000 {
                        return Err(Error(
                            "Gen-2 circuit exceeds the checkpoint reader's nesting limit".into(),
                        ));
                    }
                    let node = &self.nodes[&id];
                    let fields: Vec<_> = node
                        .fields()
                        .filter(|(name, field)| public_field(id, name, field))
                        .collect();
                    let keys = fields.iter().map(|(key, _)| *key).collect();
                    pending.push(Step::Definition(id, keys));
                    pending.extend(fields.into_iter().rev().map(|(_, f)| Step::Field(f, depth)));
                    continue;
                }
                Step::Field(field, depth) => match field {
                    Field::Reference(id @ NodeId::Outer(_)) => json!({"node": id.number()}),
                    Field::Reference(id) | Field::Definition(id) => {
                        pending.push(Step::Node(*id, depth + 1));
                        continue;
                    }
                    Field::Data(value) => value.clone(),
                    Field::Array(items) => {
                        pending.push(Step::Array(items.len()));
                        pending.extend(items.iter().rev().map(|f| Step::Field(f, depth)));
                        continue;
                    }
                    Field::Object(fields) => {
                        pending.push(Step::Object(fields.keys().map(String::as_str).collect()));
                        pending.extend(fields.values().rev().map(|f| Step::Field(f, depth)));
                        continue;
                    }
                },
                Step::Array(size) => Value::Array(values.split_off(values.len() - size)),
                Step::Object(keys) => {
                    let fields = values.drain(values.len() - keys.len()..);
                    Value::Object(keys.into_iter().map(str::to_owned).zip(fields).collect())
                }
                Step::Definition(id, keys) => {
                    let node = &self.nodes[&id];
                    let mut object = Map::with_capacity(keys.len() + 2);
                    object.insert("class".into(), node.class().into());
                    object.insert("id".into(), id.number().into());
                    let fields = values.drain(values.len() - keys.len()..);
                    for (key, mut value) in keys.into_iter().zip(fields) {
                        match key {
                            "annotations" if matches!(id, NodeId::Outer(_)) => {
                                if let Some(list) = value.as_array_mut() {
                                    for entry in list {
                                        if entry.get("class").and_then(Value::as_str)
                                            == Some("JoinStrategy")
                                        {
                                            entry.as_object_mut().unwrap().shift_remove("position");
                                        }
                                    }
                                }
                            }
                            "metadata" if node.operator_kind().is_some() => {
                                public_operator_metadata(&mut value)?
                            }
                            "metadata" if id == self.root => stringify_schema_nulls(&mut value),
                            _ => (),
                        }
                        object.insert(key.into(), value);
                    }
                    Value::Object(object)
                }
            };
            values.push(value);
        }
        Ok(values.pop().expect("encoded circuit root"))
    }
}

fn public_field(id: NodeId, name: &str, field: &Field) -> bool {
    match id {
        NodeId::Outer(_) => !matches!(
            name,
            "provenance"
                | "sourceEmpty"
                | "sourcePosition"
                | "sourcePositions"
                | "sourceNameEmpty"
                | "sourceNamePosition"
                | "query"
        ),
        NodeId::Inner(_) => {
            !matches!(
                name,
                "sourceEmpty" | "singleton" | "cachedEmptySetResult" | "originalStruct"
            ) && (name != "position" || valid_position(field))
        }
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
