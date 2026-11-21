//! Replay the JVM's schema side effects from the Rust input-field analysis.
use feldera_sql_ir::{Circuit, Field, NodeId};
use feldera_sql_opt::InputFieldUse;
use serde_json::Value;
use std::{collections::BTreeMap, error::Error};

pub fn mark_unused(input: &Circuit, uses: &[InputFieldUse]) -> Result<Circuit, Box<dyn Error>> {
    let mut graph = input.to_json();
    let mut edits = BTreeMap::new();
    for usage in uses {
        let reader = input
            .node(NodeId::Outer(usage.reader))
            .ok_or("missing input projection")?;
        let source = reader
            .inputs()
            .next()
            .ok_or("missing projection input")?
            .operator;
        let table = input.node(source).ok_or("missing input table")?;
        let Some(Field::Object(metadata)) = table.field("metadata") else {
            return Err("missing source metadata".into());
        };
        let Some(Field::Array(columns)) = metadata.get("columnMetadata") else {
            return Err("missing source columns".into());
        };
        let mut unused = Vec::new();
        for (index, column) in columns.iter().enumerate() {
            let Some(Field::Data(Value::Bool(key))) = (match column {
                Field::Object(properties) => properties.get("isPrimaryKey"),
                _ => None,
            }) else {
                return Err("missing primary key flag".into());
            };
            if !key
                && !usage
                    .paths
                    .iter()
                    .any(|path| path.is_empty() || path.first() == Some(&index))
            {
                unused.push(index);
            }
        }
        if unused.is_empty() {
            continue;
        }
        let name = table.relation_name("tableName")?;
        let schema = graph["metadata"]["inputs"]
            .as_array_mut()
            .ok_or("missing input schemas")?
            .iter_mut()
            .find(|schema| schema["name"].as_str() == Some(name))
            .ok_or("missing table schema")?;
        for &index in &unused {
            schema["fields"]
                .get_mut(index)
                .ok_or("missing schema column")?["unused"] = true.into();
        }
        let NodeId::Outer(source) = source else {
            return Err("expected source operator".into());
        };
        edits.insert(source, unused);
    }
    fn update(
        node: &mut Value,
        edits: &mut BTreeMap<u64, Vec<usize>>,
    ) -> Result<(), Box<dyn Error>> {
        let Some(operators) = node.get_mut("allOperators").and_then(Value::as_array_mut) else {
            return Ok(());
        };
        for op in operators {
            if let Some(indices) = op["id"].as_u64().and_then(|id| edits.remove(&id)) {
                let columns = op["metadata"]["columnMetadata"]
                    .as_array_mut()
                    .ok_or("missing serialized source columns")?;
                for index in indices {
                    if let Some(flag) = columns.get_mut(index).and_then(|c| c.get_mut("unused")) {
                        *flag = true.into();
                    }
                }
            }
            update(op, edits)?;
        }
        Ok(())
    }
    update(&mut graph, &mut edits)?;
    if !edits.is_empty() {
        return Err("missing serialized table definitions".into());
    }
    Ok(Circuit::from_json(graph)?)
}
