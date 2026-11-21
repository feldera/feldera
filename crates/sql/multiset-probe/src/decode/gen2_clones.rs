//! Remove the classic clone wrapper from a Gen-2 apply closure.

use super::{array, encode_rewrite, field, inner_reference, nesting, uint, Definitions};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashSet;

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut result = circuit.clone();
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("Gen-2 expression identities exhausted".into()))?;
        Ok(next)
    };
    let output = result["allOperators"]
        .as_array_mut()
        .ok_or_else(|| Error("missing operator array".into()))?;
    for (before, after) in array(circuit, "allOperators")?.iter().zip(output) {
        if before["class"] != "DBSPApplyOperator" {
            continue;
        }
        let closure = definitions.resolve(field(before, "function")?)?;
        let body = definitions.resolve(field(closure, "body")?)?;
        if body["class"] != "DBSPCloneExpression" {
            continue;
        }
        let unwrapped = definitions.resolve(field(body, "expression")?)?;
        if definitions.ty(field(body, "type")?, 0)?
            != definitions.ty(field(unwrapped, "type")?, 0)?
        {
            return Err(Error("Gen-2 clone changes its expression type".into()));
        }
        let mut function_type = definitions.resolve(field(closure, "type")?)?.clone();
        function_type["id"] = fresh()?.into();
        function_type["resultType"] = inner_reference(field(unwrapped, "type")?)?;
        let mut closure = closure.clone();
        closure["id"] = fresh()?.into();
        closure["type"] = function_type;
        closure["body"] = inner_reference(unwrapped)?;
        after["function"] = closure;
        // CircuitRewriter reconstructs a changed Apply without annotations.
        after["annotations"] = Value::Array(Vec::new());
    }
    let encoded = encode_rewrite(circuit, &result)?;
    validate_executable_nodes(&encoded)?;
    Ok(encoded)
}

fn validate_executable_nodes(circuit: &Value) -> Result<()> {
    let defs = Definitions::read(circuit)?;
    let mut seen = HashSet::new();
    for declaration in array(circuit, "declarations")? {
        require_no_wrappers(field(declaration, "item")?, &defs, &mut seen, 0)?;
    }
    for operator in array(circuit, "allOperators")? {
        if operator["class"] == "DBSPNestedOperator" {
            return Err(Error("Gen-2 nested circuit is outside this slice".into()));
        }
        for (name, value) in operator.as_object().ok_or_else(invalid_node)? {
            // CircuitRewriter preserves defaults in source metadata.
            if !matches!(
                name.as_str(),
                "metadata" | "provenance" | "inputs" | "annotations"
            ) {
                require_no_wrappers(value, &defs, &mut seen, 0)?;
            }
        }
    }
    Ok(())
}

fn require_no_wrappers<'a>(
    value: &'a Value,
    defs: &Definitions<'a>,
    seen: &mut HashSet<u64>,
    depth: usize,
) -> Result<()> {
    nesting(depth)?;
    match value {
        Value::Array(values) => {
            for child in values {
                require_no_wrappers(child, defs, seen, depth + 1)?;
            }
        }
        Value::Object(object) => {
            let value = if (object.len() == 1 && object.contains_key("node"))
                || (object.contains_key("class") && object.contains_key("id"))
            {
                let value = defs.resolve(value)?;
                if !seen.insert(uint(value, "id")?) {
                    return Ok(());
                }
                value
            } else {
                value
            };
            if value["class"] == "DBSPCloneExpression" {
                return Err(Error(
                    "Gen-2 clone exists outside an apply closure body".into(),
                ));
            }
            if value["opcode"] == "TYPEDBOX" || value["code"] == "TYPEDBOX" {
                return Err(Error(
                    "Gen-2 typed-box removal is outside this slice".into(),
                ));
            }
            for child in value.as_object().ok_or_else(invalid_node)?.values() {
                require_no_wrappers(child, defs, seen, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn invalid_node() -> Error {
    Error("invalid Gen-2 circuit node".into())
}
