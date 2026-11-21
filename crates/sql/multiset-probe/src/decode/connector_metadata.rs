//! Check executable inner nodes for calls reserved for column defaults.

use super::{array, field, nesting, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashSet;

pub(crate) fn connector_metadata_violation(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut visited = HashSet::new();
    for declaration in array(circuit, "declarations")? {
        if let Some(report) = inspect(&definitions, field(declaration, "item")?, &mut visited, 0)? {
            return Ok(report);
        }
    }
    Ok(
        inspect_operators(&definitions, array(circuit, "allOperators")?, &mut visited)?
            .unwrap_or(Value::Null),
    )
}

fn inspect_operators(
    definitions: &Definitions<'_>,
    operators: &[Value],
    visited: &mut HashSet<u64>,
) -> Result<Option<Value>> {
    for operator in operators {
        if operator["class"] == "DBSPNestedOperator" {
            if let Some(report) =
                inspect_operators(definitions, array(operator, "allOperators")?, visited)?
            {
                return Ok(Some(report));
            }
            continue;
        }
        for (name, expression) in operator
            .as_object()
            .ok_or_else(|| Error("expected operator object".into()))?
        {
            // The circuit dispatcher traverses the operator's executable
            // fields. Input column defaults are carried only in metadata.
            if matches!(
                name.as_str(),
                "inputs" | "metadata" | "annotations" | "provenance"
            ) {
                continue;
            }
            if let Some(report) = inspect(definitions, expression, visited, 0)? {
                return Ok(Some(report));
            }
        }
    }
    Ok(None)
}

fn inspect(
    definitions: &Definitions<'_>,
    expression: &Value,
    visited: &mut HashSet<u64>,
    depth: usize,
) -> Result<Option<Value>> {
    nesting(depth)?;
    let expression = match expression.as_object() {
        Some(object) if object.len() == 1 && object.contains_key("node") => {
            definitions.resolve(expression)?
        }
        _ => expression,
    };
    if expression.get("class").is_some() && !visited.insert(uint(expression, "id")?) {
        return Ok(None);
    }
    let nested = match expression {
        Value::Object(object) => inspect_children(
            definitions,
            object.iter().filter_map(|(name, child)| {
                (!matches!(name.as_str(), "annotations" | "provenance")).then_some(child)
            }),
            visited,
            depth,
        )?,
        Value::Array(elements) => inspect_children(definitions, elements.iter(), visited, depth)?,
        _ => None,
    };
    if nested.is_some() {
        return Ok(nested);
    }
    let Some(name) = definitions.call_name(expression)? else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case("connector_metadata") {
        return Ok(None);
    }
    Ok(Some(json!({
        "function": name,
        "position": expression.get("position"),
    })))
}

fn inspect_children<'a>(
    definitions: &Definitions<'_>,
    children: impl Iterator<Item = &'a Value>,
    visited: &mut HashSet<u64>,
    depth: usize,
) -> Result<Option<Value>> {
    for child in children {
        if let Some(report) = inspect(definitions, child, visited, depth + 1)? {
            return Ok(Some(report));
        }
    }
    Ok(None)
}
