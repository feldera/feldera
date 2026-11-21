//! Fuse a direct tuple permutation into a single-consumer stream join.

use super::{array, encode_rewrite, field, inner_reference, text, uint, wire_port, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let operators = array(circuit, "allOperators")?;
    let mut by_id = HashMap::new();
    let mut fanout = HashMap::new();
    for operator in operators {
        by_id.insert(uint(operator, "id")?, operator);
        for input in array(operator, "inputs")? {
            *fanout.entry(wire_port(input)?.operator).or_insert(0) += 1;
        }
    }
    let mut next = definitions.inner.keys().copied().max().unwrap_or_default();
    let mut fresh = || {
        next = next
            .checked_add(1)
            .ok_or_else(|| Error("projection expression identities exhausted".into()))?;
        Ok(next)
    };
    let mut fused = HashMap::new();
    let mut removed = HashSet::new();
    for operator in operators {
        let class = text(operator, "class")?;
        match class {
            "DBSPNestedOperator" => return Err(outside_scope()),
            "DBSPMapOperator"
            | "DBSPMapIndexOperator"
            | "DBSPDeindexOperator"
            | "DBSPApplyOperator" => {}
            _ => continue,
        }
        let [input] = array(operator, "inputs")? else {
            return Err(outside_scope());
        };
        let input = wire_port(input)?;
        let source = *by_id.get(&input.operator).ok_or_else(outside_scope)?;
        match (class, text(source, "class")?) {
            ("DBSPMapOperator", "DBSPStreamJoinOperator")
                if input.output == 0 && fanout[&input.operator] == 1 =>
            {
                let mut join = source.clone();
                join["id"] = field(operator, "id")?.clone();
                join["outputType"] = field(operator, "outputType")?.clone();
                join["function"] = compose(source, operator, &definitions, &mut fresh)?;
                fused.insert(uint(operator, "id")?, join);
                removed.insert(input.operator);
            }
            (
                "DBSPMapOperator",
                "DBSPSourceMultisetOperator"
                | "DBSPSourceMapOperator"
                | "DBSPFilterOperator"
                | "DBSPStreamJoinOperator",
            )
            | ("DBSPMapIndexOperator", "DBSPSourceMultisetOperator")
            | ("DBSPApplyOperator", "DBSPWaterlineOperator") => {}
            _ => return Err(outside_scope()),
        }
    }
    let mut result = circuit.clone();
    let mut rewritten = Vec::with_capacity(operators.len() - removed.len());
    for operator in operators {
        let id = uint(operator, "id")?;
        if let Some(join) = fused.remove(&id) {
            rewritten.push(join);
        } else if !removed.contains(&id) {
            rewritten.push(operator.clone());
        }
    }
    result["allOperators"] = Value::Array(rewritten);
    encode_rewrite(circuit, &result)
}

fn compose<'a>(
    join: &'a Value,
    map: &'a Value,
    defs: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let join = expect(defs, field(join, "function")?, "DBSPClosureExpression")?;
    let map = expect(defs, field(map, "function")?, "DBSPClosureExpression")?;
    let [parameter] = array(map, "parameters")? else {
        return Err(outside_scope());
    };
    let parameter = expect(defs, parameter, "DBSPParameter")?;
    let joined = expect(defs, field(join, "body")?, "DBSPTupleExpression")?;
    let projected = expect(defs, field(map, "body")?, "DBSPTupleExpression")?;
    let joined_fields = array(joined, "fields")?;
    let projected_fields = array(projected, "fields")?;
    if joined_fields.len() != projected_fields.len()
        || defs.resolve(field(joined, "type")?)?["mayBeNull"] != false
        || defs.resolve(field(projected, "type")?)?["mayBeNull"] != false
    {
        return Err(outside_scope());
    }

    let mut ordinals = HashSet::new();
    let mut fields = Vec::with_capacity(projected_fields.len());
    for projection in projected_fields {
        let projection = expect(defs, projection, "DBSPFieldExpression")?;
        let deref = expect(
            defs,
            field(projection, "expression")?,
            "DBSPDerefExpression",
        )?;
        let variable = expect(defs, field(deref, "expression")?, "DBSPVariablePath")?;
        let ordinal = usize::try_from(uint(projection, "fieldNo")?).map_err(|_| outside_scope())?;
        let field = defs.resolve(joined_fields.get(ordinal).ok_or_else(outside_scope)?)?;
        // A permutation retains each evaluation. Limit the join body to simple
        // field accesses, for which changing evaluation order cannot raise an error.
        if variable["variable"] != parameter["name"]
            || !ordinals.insert(ordinal)
            || !is_direct_field(field, defs)?
            || defs.ty(&projection["type"], 0)? != defs.ty(&field["type"], 0)?
        {
            return Err(outside_scope());
        }
        fields.push(inner_reference(field)?);
    }

    let mut body = projected.clone();
    body["id"] = fresh()?.into();
    body["fields"] = Value::Array(fields);
    let mut function_type = expect(defs, field(join, "type")?, "DBSPTypeFunction")?.clone();
    function_type["id"] = fresh()?.into();
    function_type["resultType"] = inner_reference(field(projected, "type")?)?;
    Ok(json!({
        "class": "DBSPClosureExpression", "id": fresh()?, "sourceEmpty": true,
        "type": function_type, "parameters": array(join, "parameters")?, "body": body,
    }))
}

fn is_direct_field<'a>(value: &'a Value, defs: &Definitions<'a>) -> Result<bool> {
    if value["class"] != "DBSPFieldExpression" {
        return Ok(false);
    }
    let deref = defs.resolve(field(value, "expression")?)?;
    Ok(deref["class"] == "DBSPDerefExpression"
        && defs.resolve(field(deref, "expression")?)?["class"] == "DBSPVariablePath")
}

fn expect<'a>(defs: &Definitions<'a>, value: &'a Value, class: &str) -> Result<&'a Value> {
    let value = defs.resolve(value)?;
    if value["class"] != class {
        return Err(outside_scope());
    }
    Ok(value)
}

fn outside_scope() -> Error {
    Error("projection optimization outside the supported stream-join slice".into())
}
