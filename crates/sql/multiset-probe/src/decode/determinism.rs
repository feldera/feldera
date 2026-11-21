//! Find nondeterministic SQL function bodies and input column defaults.

use super::{array, field, nesting, text, uint, Definitions};
use crate::Result;
use serde_json::{json, Value};
use std::collections::HashSet;

pub(crate) fn nondeterministic_initializers(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut names = HashSet::from(["now".to_owned()]);
    let mut findings = Vec::new();
    // Declarations precede inputs. A rejected function is then known to callers
    // further along in the circuit's visit order.
    for declaration in array(circuit, "declarations")? {
        let item = definitions.resolve(field(declaration, "item")?)?;
        if item["class"] != "DBSPFunctionItem" {
            continue;
        }
        let function = definitions.resolve(field(item, "function")?)?;
        if let Some(body) = function.get("body") {
            if has_nondeterministic_call(&definitions, &names, body, &mut HashSet::new(), 0)? {
                names.insert(text(function, "name")?.to_lowercase());
                findings.push(json!({"declaration": uint(declaration, "id")?}));
            }
        }
    }
    find_defaults(
        &definitions,
        &names,
        array(circuit, "allOperators")?,
        &mut findings,
    )?;
    Ok(Value::Array(findings))
}

fn find_defaults(
    definitions: &Definitions<'_>,
    names: &HashSet<String>,
    operators: &[Value],
    findings: &mut Vec<Value>,
) -> Result<()> {
    for operator in operators {
        match text(operator, "class")? {
            "DBSPNestedOperator" => {
                find_defaults(
                    definitions,
                    names,
                    array(operator, "allOperators")?,
                    findings,
                )?;
            }
            "DBSPSourceMultisetOperator" => {
                for (index, column) in array(field(operator, "metadata")?, "columnMetadata")?
                    .iter()
                    .enumerate()
                {
                    if let Some(value) = column.get("defaultValue") {
                        if has_nondeterministic_call(
                            definitions,
                            names,
                            value,
                            &mut HashSet::new(),
                            0,
                        )? {
                            findings.push(json!({
                                "source": uint(operator, "id")?,
                                "column": index,
                            }));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn has_nondeterministic_call(
    definitions: &Definitions<'_>,
    names: &HashSet<String>,
    expression: &Value,
    visited: &mut HashSet<u64>,
    depth: usize,
) -> Result<bool> {
    nesting(depth)?;
    let expression = if expression
        .as_object()
        .is_some_and(|o| o.len() == 1 && o.contains_key("node"))
    {
        definitions.resolve(expression)?
    } else {
        expression
    };
    if let Some(class) = expression["class"].as_str() {
        if class.starts_with("DBSPType") || !visited.insert(uint(expression, "id")?) {
            return Ok(false);
        }
        if definitions
            .call_name(expression)?
            .is_some_and(|name| names.contains(&name.to_lowercase()))
        {
            return Ok(true);
        }
    }
    let children: &mut dyn Iterator<Item = &Value> = match expression {
        Value::Object(object) => &mut object.iter().filter_map(|(name, child)| {
            (!matches!(name.as_str(), "annotations" | "provenance")).then_some(child)
        }),
        Value::Array(elements) => &mut elements.iter(),
        _ => return Ok(false),
    };
    for child in children {
        if has_nondeterministic_call(definitions, names, child, visited, depth + 1)? {
            return Ok(true);
        }
    }
    Ok(false)
}
