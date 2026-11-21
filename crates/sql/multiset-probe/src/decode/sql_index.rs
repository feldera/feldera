//! Lower one-based SQL array indexes and zero-based SAFE_OFFSET operands.

use super::{boolean, encode_with_inner, field, inner_reference, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) fn adjust(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("index rewrite exhausted identities".into()))?;
        Ok(next)
    };
    let mut indexes: Vec<_> = definitions
        .inner
        .iter()
        .filter(|(_, node)| {
            node["class"] == "DBSPBinaryExpression"
                && matches!(
                    node["opcode"].as_str(),
                    Some("SQL_INDEX" | "SAFE_RUST_INDEX")
                )
        })
        .collect();
    if indexes.is_empty() {
        return Ok(circuit.clone());
    }
    indexes.sort_unstable_by_key(|&(&id, _)| id);
    let mut types = [None, None];
    for node in definitions
        .inner
        .values()
        .filter(|node| node["class"] == "DBSPTypeISize")
    {
        let slot = &mut types[usize::from(boolean(node, "mayBeNull")?)];
        if slot.replace(inner_reference(node)?).is_some() {
            return Err(Error(
                "array index profile has conflicting isize type identities".into(),
            ));
        }
    }
    let mut replacements = HashMap::with_capacity(indexes.len());
    for (&id, node) in indexes {
        let right = definitions.resolve(field(node, "right")?)?;
        let ty = definitions.resolve(field(right, "type")?)?;
        let nullable = boolean(ty, "mayBeNull")?;
        let slot = &mut types[usize::from(nullable)];
        if slot.is_none() {
            *slot = Some(json!({
                "class": "DBSPTypeISize", "id": fresh()?, "sourceEmpty": true,
                "code": "ISIZE", "mayBeNull": nullable,
            }));
        }
        let isize_type = slot.as_ref().expect("created target index type");
        replacements.insert(id, adjust_index(node, right, ty, isize_type, &mut fresh)?);
    }
    encode_with_inner(circuit, circuit, &definitions, &replacements)
}

fn adjust_index(
    node: &Value,
    right: &Value,
    ty: &Value,
    isize_type: &Value,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let one_based = node["opcode"] == "SQL_INDEX";
    if ty["class"] != "DBSPTypeInteger" || !boolean(ty, "signed")? {
        return Err(Error(
            "array index requires a supported signed integer".into(),
        ));
    }
    let width = uint(ty, "width")?;
    let literal_class = match width {
        8 => "DBSPI8Literal",
        16 => "DBSPI16Literal",
        32 => "DBSPI32Literal",
        64 => "DBSPI64Literal",
        _ => {
            return Err(Error(
                "array index width is outside the signed integer slice".into(),
            ))
        }
    };
    let adjusted = if right["class"] == literal_class {
        let value = right.get("value").filter(|value| !value.is_null());
        let origin = if one_based && value.is_some() {
            node
        } else {
            right
        };
        let mut literal = expression("DBSPISizeLiteral", fresh()?, origin, isize_type.clone());
        if let Some(value) = value {
            let value = value
                .as_str()
                .and_then(|value| value.parse::<i64>().ok())
                .ok_or_else(|| Error("invalid integer array index literal".into()))?;
            let min = i64::MIN >> (64_u64 - width);
            let max = i64::MAX >> (64_u64 - width);
            if !(min..=max).contains(&value) || one_based && value == min {
                return Err(Error(
                    "array index literal reduction exceeds the original width".into(),
                ));
            }
            literal["value"] = (value - i64::from(one_based)).to_string().into();
        } else if !boolean(ty, "mayBeNull")? {
            return Err(Error("non-null array index literal has no value".into()));
        }
        literal
    } else {
        let mut source = inner_reference(right)?;
        if one_based {
            let literal_type = json!({
                "class": "DBSPTypeInteger", "id": fresh()?, "sourceEmpty": true,
                "width": width, "signed": true, "code": text(ty, "code")?,
                "mayBeNull": boolean(ty, "mayBeNull")?,
            });
            let one = json!({
                "class": literal_class, "id": fresh()?, "sourceEmpty": true,
                "type": literal_type, "value": "1",
            });
            let mut difference =
                expression("DBSPBinaryExpression", fresh()?, node, inner_reference(ty)?);
            difference["opcode"] = "SUB".into();
            difference["left"] = source;
            difference["right"] = one;
            source = difference;
        }
        let mut cast = expression("DBSPCastExpression", fresh()?, node, isize_type.clone());
        cast["safe"] = "SqlUnsafe".into();
        cast["source"] = source;
        cast
    };
    let mut result = node.clone();
    result["opcode"] = "RUST_INDEX".into();
    result["right"] = adjusted;
    Ok(result)
}

fn expression(class: &str, id: u64, origin: &Value, ty: Value) -> Value {
    let mut expression = json!({"class": class, "id": id, "type": ty});
    for field in ["sourceEmpty", "position"] {
        if let Some(source) = origin.get(field) {
            expression[field] = source.clone();
        }
    }
    expression
}
