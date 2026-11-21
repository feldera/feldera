//! Expand shared variable paths in the measured indexed-row expression graphs.

use super::{array, encode_rewrite, field, nesting, text, uint, wire_port, Definitions};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    rewrite_selected(circuit, true)
}

pub(crate) fn ensure_expression_trees(circuit: &Value) -> Result<Value> {
    rewrite_selected(circuit, false)
}

fn rewrite_selected(circuit: &Value, indexed_only: bool) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("inner CSE identities exhausted".into()))?;
        Ok(next)
    };
    let mut rewritten = circuit.clone();
    split_projections(
        circuit,
        &mut rewritten,
        &definitions,
        &mut fresh,
        indexed_only,
    )?;
    encode_rewrite(circuit, &rewritten)
}

fn split_projections<'a>(
    original: &'a Value,
    rewritten: &mut Value,
    definitions: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
    indexed_only: bool,
) -> Result<()> {
    let operators = array(original, "allOperators")?;
    let classes = operators
        .iter()
        .map(|operator| Ok((uint(operator, "id")?, text(operator, "class")?)))
        .collect::<Result<HashMap<_, _>>>()?;
    let output = rewritten["allOperators"]
        .as_array_mut()
        .ok_or_else(|| Error("missing operator array".into()))?;
    for (before, after) in operators.iter().zip(output) {
        if before["class"] == "DBSPNestedOperator" {
            split_projections(before, after, definitions, fresh, indexed_only)?;
            continue;
        }
        if !indexed_only {
            for name in ["function", "error", "init", "extractTs"] {
                if let Some(root) = before.get(name).filter(|root| !root.is_null()) {
                    if has_repetition(root, definitions, &mut HashSet::new(), 0)?
                        && (before["class"] != "DBSPMapOperator" || name != "function")
                    {
                        return Err(Error("shared expression tree outside a map closure".into()));
                    }
                }
            }
        }
        if before["class"] != "DBSPMapOperator" {
            continue;
        }
        let [input] = array(before, "inputs")? else {
            return Err(Error("map requires one input".into()));
        };
        let source = wire_port(input)?;
        if indexed_only
            && !matches!(
                classes.get(&source.operator),
                Some(&"DBSPSourceMapOperator")
            )
        {
            continue;
        }
        let function = definitions.resolve(field(before, "function")?)?;
        let body = definitions.resolve(field(function, "body")?)?;
        if !indexed_only && !has_repetition(body, definitions, &mut HashSet::new(), 0)? {
            continue;
        }
        if body["class"] != "DBSPTupleExpression" {
            if !indexed_only {
                return Err(Error(
                    "shared expression outside the tuple projection slice".into(),
                ));
            }
            continue;
        }
        let mut seen = HashSet::new();
        if let Some(body) = split_variables(body, definitions, &mut seen, fresh, 0)? {
            let mut function = function.clone();
            function["id"] = fresh()?.into();
            function["body"] = body;
            after["function"] = function;
        }
    }
    Ok(())
}

fn has_repetition<'a>(
    value: &'a Value,
    definitions: &Definitions<'a>,
    seen: &mut HashSet<u64>,
    depth: usize,
) -> Result<bool> {
    nesting(depth)?;
    match value {
        Value::Array(values) => {
            for child in values {
                if has_repetition(child, definitions, seen, depth + 1)? {
                    return Ok(true);
                }
            }
        }
        Value::Object(object) => {
            let node = if object.contains_key("class") || object.contains_key("node") {
                definitions.resolve(value)?
            } else {
                value
            };
            if let Some(class) = node["class"].as_str() {
                if class.starts_with("DBSPType") {
                    return Ok(false);
                }
                if (class.ends_with("Expression")
                    || class.ends_with("Literal")
                    || class == "DBSPVariablePath")
                    && !seen.insert(uint(node, "id")?)
                {
                    return Ok(true);
                }
            }
            for (name, child) in node
                .as_object()
                .ok_or_else(|| Error("invalid expression node".into()))?
            {
                if name != "type" && has_repetition(child, definitions, seen, depth + 1)? {
                    return Ok(true);
                }
            }
        }
        _ => {}
    }
    Ok(false)
}

fn split_variables<'a>(
    value: &'a Value,
    definitions: &Definitions<'a>,
    seen: &mut HashSet<u64>,
    fresh: &mut impl FnMut() -> Result<u64>,
    depth: usize,
) -> Result<Option<Value>> {
    nesting(depth)?;
    match value {
        Value::Array(values) => {
            let mut result = None;
            for (index, child) in values.iter().enumerate() {
                if let Some(child) = split_variables(child, definitions, seen, fresh, depth + 1)? {
                    result.get_or_insert_with(|| values.clone())[index] = child;
                }
            }
            Ok(result.map(Value::Array))
        }
        Value::Object(_) => {
            let value = definitions.resolve(value)?;
            let class = text(value, "class")?;
            if class.starts_with("DBSPType") {
                return Ok(None);
            }
            if !matches!(
                class,
                "DBSPTupleExpression"
                    | "DBSPDerefExpression"
                    | "DBSPFieldExpression"
                    | "DBSPVariablePath"
                    | "DBSPSomeExpression"
            ) {
                return Err(Error(format!(
                    "indexed projection CSE outside the supported expression slice: {class}"
                )));
            }
            let mut replacement = None;
            if !seen.insert(uint(value, "id")?) {
                if class != "DBSPVariablePath" {
                    return Err(Error(
                        "indexed projection CSE needs to split a compound expression".into(),
                    ));
                }
                replacement = Some(value.clone());
            }
            for name in ["fields", "expression"] {
                if let Some(child) = value.get(name) {
                    if let Some(child) =
                        split_variables(child, definitions, seen, fresh, depth + 1)?
                    {
                        replacement.get_or_insert_with(|| value.clone())[name] = child;
                    }
                }
            }
            if let Some(node) = &mut replacement {
                node["id"] = fresh()?.into();
            }
            Ok(replacement)
        }
        _ => Ok(None),
    }
}
