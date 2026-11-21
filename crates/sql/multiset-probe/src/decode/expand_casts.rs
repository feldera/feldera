//! Expand the measured scalar casts and table defaults.

use super::{array, boolean, encode_with_inner, field, inner_reference, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(crate) fn expand(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut roots = HashSet::new();
    metadata_casts(circuit, &definitions, &mut roots)?;
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("cast identities exhausted".into()))?;
        Ok(next)
    };
    let mut casts: Vec<_> = definitions
        .inner
        .iter()
        .filter(|(_, node)| {
            node["class"] == "DBSPCastExpression"
                && matches!(node["safe"].as_str(), Some("SqlUnsafe" | "SqlSafe"))
        })
        .collect();
    if casts.is_empty() {
        return Ok(circuit.clone());
    }
    casts.sort_unstable_by_key(|entry| *entry.0);
    let mut replacements = HashMap::with_capacity(casts.len());
    for (&id, cast) in casts {
        let source = definitions.resolve(field(cast, "source")?)?;
        let from = definitions.resolve(field(source, "type")?)?;
        let to = definitions.resolve(field(cast, "type")?)?;
        let expanded = match (
            text(cast, "safe")?,
            text(from, "class")?,
            text(to, "class")?,
        ) {
            ("SqlUnsafe", "DBSPTypeInteger", "DBSPTypeInteger") => into_nullable_integer(
                cast,
                source,
                from,
                to,
                &definitions,
                roots.contains(&id),
                &mut fresh,
            )?,
            ("SqlUnsafe", "DBSPTypeString", "DBSPTypeString")
                if roots.contains(&id)
                    && source["class"] == "DBSPStringLiteral"
                    && !boolean(from, "mayBeNull")?
                    && boolean(to, "mayBeNull")?
                    && !boolean(to, "fixed")?
                    && to["precision"].as_i64() == Some(-1) =>
            {
                let mut literal = source.clone();
                literal["id"] = id.into();
                literal["type"] = inner_reference(to)?;
                literal
            }
            ("SqlSafe", "DBSPTypeVariant", "DBSPTypeString")
                if boolean(to, "mayBeNull")? && roots.contains(&id) =>
            {
                safe_variant_default(cast, source, to, &mut fresh)?
            }
            _ => return Err(Error("SQL cast outside the measured scalar subset".into())),
        };
        replacements.insert(id, expanded);
    }
    encode_with_inner(circuit, circuit, &definitions, &replacements)
}

fn metadata_casts(
    circuit: &Value,
    definitions: &Definitions<'_>,
    roots: &mut HashSet<u64>,
) -> Result<()> {
    for operator in array(circuit, "allOperators")? {
        if operator["class"] == "DBSPNestedOperator" {
            metadata_casts(operator, definitions, roots)?;
        }
        if let Some(columns) = operator["metadata"]["columnMetadata"].as_array() {
            for field in columns {
                for key in ["defaultValue", "lateness"] {
                    if let Some(root) = field.get(key).filter(|value| !value.is_null()) {
                        let root = definitions.resolve(root)?;
                        if root["class"] == "DBSPCastExpression" {
                            roots.insert(uint(root, "id")?);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn into_nullable_integer(
    cast: &Value,
    source: &Value,
    from: &Value,
    to: &Value,
    definitions: &Definitions<'_>,
    default: bool,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    if boolean(from, "mayBeNull")?
        || !boolean(to, "mayBeNull")?
        || from["width"] != to["width"]
        || from["signed"] != to["signed"]
    {
        return Err(Error(
            "integer cast requires only adding nullability".into(),
        ));
    }
    let id = uint(cast, "id")?;
    let mut ty = from.clone();
    ty["id"] = fresh()?.into();
    ty["mayBeNull"] = true.into();
    if source["class"] == "DBSPI32Literal" {
        let mut literal = source.clone();
        literal["id"] = id.into();
        literal["type"] = ty;
        return Ok(literal);
    }
    if default {
        if from["code"] != "INT32" || source["class"] != "DBSPBinaryExpression" {
            return Err(Error("unsupported nullable integer table default".into()));
        }
        let literal = |name| -> Result<i32> {
            let operand = definitions.resolve(field(source, name)?)?;
            if operand["class"] != "DBSPI32Literal" {
                return Err(Error("integer default operand is not a literal".into()));
            }
            text(operand, "value")?
                .parse()
                .map_err(|_| Error("invalid i32 default operand".into()))
        };
        let left = literal("left")?;
        let right = literal("right")?;
        let result = match text(source, "opcode")? {
            "ADD" => left.checked_add(right),
            "SUB" => left.checked_sub(right),
            "MUL" => left.checked_mul(right),
            _ => return Err(Error("unsupported integer default arithmetic".into())),
        }
        .ok_or_else(|| Error("integer default arithmetic overflows".into()))?;
        ty["sourceEmpty"] = true.into();
        ty.as_object_mut().expect("integer type").remove("position");
        return Ok(json!({
            "class": "DBSPI32Literal", "id": id, "sourceEmpty": true,
            "type": ty, "value": result.to_string(),
        }));
    }
    let mut some = json!({
        "class": "DBSPSomeExpression", "id": id, "type": ty,
        "expression": inner_reference(source)?,
    });
    copy_origin(source, &mut some);
    Ok(some)
}

fn safe_variant_default(
    cast: &Value,
    source: &Value,
    to: &Value,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let result = inner_reference(to)?;
    let mut result_type = json!({
        "class": "DBSPTypeSqlResult", "id": fresh()?, "typeArgs": [result.clone()],
        "name": "SqlResult", "code": "RESULT", "mayBeNull": false,
    });
    copy_origin(to, &mut result_type);
    let mut rust = json!({
        "class": "DBSPCastExpression", "id": fresh()?, "source": inner_reference(source)?,
        "type": result_type, "safe": "RustCast",
    });
    copy_origin(cast, &mut rust);
    let mut handled = json!({
        "class": "DBSPHandleErrorExpression", "id": uint(cast, "id")?,
        "source": rust, "type": result, "runtimeBehavior": "ReturnNone",
        "index": 0, "hasSourcePosition": false,
    });
    copy_origin(cast, &mut handled);
    Ok(handled)
}

fn copy_origin(source: &Value, target: &mut Value) {
    for field in ["sourceEmpty", "position"] {
        if let Some(value) = source.get(field) {
            target[field] = value.clone();
        }
    }
}
