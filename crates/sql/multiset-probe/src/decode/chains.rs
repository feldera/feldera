//! Fuse a non-branching run of maps, indexed maps and filters.

use super::{
    array, encode_rewrite, field, inner_identity, inner_reference, text, uint,
    wire_edits::{operator, WireEdits},
    wire_port,
};
use crate::{provenance::Provenance, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
enum Computation {
    Map,
    MapIndex,
    Filter,
}

fn computation(node: &Value, edits: &WireEdits<'_>) -> Result<Option<Computation>> {
    let kind = match text(node, "class")? {
        "DBSPMapOperator" => Computation::Map,
        "DBSPMapIndexOperator" => Computation::MapIndex,
        "DBSPFilterOperator" => Computation::Filter,
        _ => return Ok(None),
    };
    let id = inner_identity(field(node, "function")?)?;
    let function = edits
        .definitions()
        .inner
        .get(&id)
        .ok_or_else(|| Error("missing chain closure".into()))?;
    if text(function, "class")? != "DBSPClosureExpression" {
        return Ok(None);
    }
    if array(function, "parameters")?.len() != 1 {
        return Err(Error("chain closure must have one parameter".into()));
    }
    Ok(Some(kind))
}

fn barrier(node: &Value) -> Result<bool> {
    Ok(array(node, "annotations")?
        .iter()
        .any(|annotation| annotation["class"] == "NoChain"))
}

struct Prefix {
    input: Value,
    kinds: Vec<Computation>,
    closures: Vec<Value>,
}

fn prefix(node: &Value, edits: &WireEdits<'_>) -> Result<Option<Prefix>> {
    if node["class"] == "DBSPChainOperator" {
        let [input] = array(node, "inputs")? else {
            return Err(Error("chain requires one input".into()));
        };
        let kinds: Vec<Computation> = serde_json::from_value(field(node, "kinds")?.clone())
            .map_err(|error| Error(format!("invalid chain computation: {error}")))?;
        let closures = array(node, "chain")?;
        if kinds.len() != closures.len() || kinds.len() < 2 {
            return Err(Error("inconsistent chain computation table".into()));
        }
        return Ok(Some(Prefix {
            input: input.clone(),
            kinds,
            closures: closures
                .iter()
                .map(inner_reference)
                .collect::<Result<_>>()?,
        }));
    }
    if barrier(node)? {
        return Ok(None);
    }
    let Some(kind) = computation(node, edits)? else {
        return Ok(None);
    };
    let [input] = array(node, "inputs")? else {
        return Err(Error("chain computation requires one input".into()));
    };
    Ok(Some(Prefix {
        input: input.clone(),
        kinds: vec![kind],
        closures: vec![inner_reference(field(node, "function")?)?],
    }))
}

pub(crate) fn fuse(circuit: &Value) -> Result<Value> {
    let original = array(circuit, "allOperators")?;
    let mut fanout = HashMap::new();
    for node in original {
        if node["class"] == "DBSPNestedOperator" {
            return Err(Error("chain fusion requires a flat circuit".into()));
        }
        for input in array(node, "inputs")? {
            *fanout.entry(wire_port(input)?.operator).or_insert(0_usize) += 1;
        }
    }
    let mut origins = Provenance::read(original)?;
    let mut edits = WireEdits::new(circuit)?;
    let mut rewritten: Vec<Value> = Vec::with_capacity(original.len());
    let mut positions: HashMap<u64, usize> = HashMap::new();
    for node in original {
        let id = edits.remember_type(node)?;
        let mut result = edits.retain(node, id)?;
        edits.retype_consumer(&mut result, id)?;
        if let Some(kind) = computation(node, &edits)? {
            let [input] = array(node, "inputs")? else {
                return Err(Error("chain computation requires one input".into()));
            };
            let prior = wire_port(input)?;
            if prior.output == 0 && fanout[&prior.operator] == 1 && !barrier(node)? {
                let predecessor = positions
                    .get(&prior.operator)
                    .map(|&index| &rewritten[index])
                    .ok_or_else(|| Error("chain names an unavailable input".into()))?;
                if let Some(prefix) = prefix(predecessor, &edits)? {
                    let prior_origin = field(predecessor, "provenance")?;
                    let origin = origins.after(field(node, "provenance")?, prior_origin)?;
                    result = fused_node(&mut edits, node, id, prefix, kind, origin)?;
                }
            }
        }
        positions.insert(id, rewritten.len());
        rewritten.push(result);
    }
    encode_rewrite(circuit, &edits.finish(circuit, rewritten))
}

fn fused_node(
    edits: &mut WireEdits<'_>,
    node: &Value,
    id: u64,
    mut prefix: Prefix,
    kind: Computation,
    origin: Value,
) -> Result<Value> {
    prefix.kinds.push(kind);
    prefix
        .closures
        .push(inner_reference(field(node, "function")?)?);
    let input_type = edits.input_type(&prefix.input)?;
    let output_type = chain_output(edits, id, &prefix, &input_type)?;
    let mut result = operator("DBSPChainOperator", id, vec![prefix.input], output_type);
    result["inputType"] = input_type;
    result["chain"] = json!(prefix.closures);
    result["kinds"] = json!(prefix.kinds);
    result["provenance"] = origin;
    edits.replaced(id);
    Ok(result)
}

fn chain_output(
    edits: &mut WireEdits<'_>,
    id: u64,
    prefix: &Prefix,
    input_type: &Value,
) -> Result<Value> {
    let Some((operation, closure)) = prefix
        .kinds
        .iter()
        .zip(&prefix.closures)
        .rev()
        .find(|(kind, _)| **kind != Computation::Filter)
    else {
        edits.set_output(id, uint(input_type, "node")?);
        return Ok(input_type.clone());
    };
    let output_id = edits.inner_id()?;
    let definitions = edits.definitions();
    let closure = definitions
        .inner
        .get(&inner_identity(closure)?)
        .ok_or_else(|| Error("missing output closure for chain".into()))?;
    let function = definitions.resolve(field(closure, "type")?)?;
    let value = definitions.resolve(field(function, "resultType")?)?;
    let mut output = match operation {
        Computation::Map => json!({
            "class": "DBSPTypeZSet", "name": "WSet", "code": "ZSET",
            "elementType": inner_reference(value)?,
        }),
        Computation::MapIndex => {
            let [key, row] = array(value, "tupFields")? else {
                return Err(Error(
                    "indexed chain result requires key and value types".into(),
                ));
            };
            json!({
                "class": "DBSPTypeIndexedZSet", "name": "IndexedWSet", "code": "INDEXED_ZSET",
                "keyType": inner_reference(key)?, "elementType": inner_reference(row)?,
            })
        }
        Computation::Filter => unreachable!("selected the last map"),
    };
    output["id"] = output_id.into();
    output["mayBeNull"] = false.into();
    for field in ["sourceEmpty", "position"] {
        if let Some(source) = value.get(field) {
            output[field] = source.clone();
        }
    }
    edits.remember_generated_output(id, &output)?;
    Ok(output)
}
