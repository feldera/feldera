//! Move a predicate across a single-consumer tuple projection.

use super::{array, encode_rewrite, field, inner_reference, text, uint, wire_port, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut next_inner = definitions.inner.keys().copied().max().unwrap_or(0);
    let mut fresh = || {
        next_inner = next_inner
            .checked_add(1)
            .ok_or_else(|| Error("filter expression identities exhausted".into()))?;
        Ok(next_inner)
    };
    let mut result = circuit.clone();
    move_filters(circuit, &mut result, &definitions, &mut fresh)?;
    encode_rewrite(circuit, &result)
}

fn move_filters<'a>(
    original: &'a Value,
    result: &mut Value,
    definitions: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<()> {
    let operators = array(original, "allOperators")?;
    let mut locations = HashMap::new();
    let mut consumers = HashMap::new();
    for (index, operator) in operators.iter().enumerate() {
        locations.insert(uint(operator, "id")?, index);
        for input in array(operator, "inputs")? {
            *consumers
                .entry(wire_port(input)?.operator)
                .or_insert(0_usize) += 1;
        }
    }
    let output = result["allOperators"]
        .as_array_mut()
        .ok_or_else(|| Error("missing operator array".into()))?;
    for (index, filter) in operators.iter().enumerate() {
        if filter["class"] == "DBSPNestedOperator" {
            move_filters(filter, &mut output[index], definitions, fresh)?;
        }
        if filter["class"] != "DBSPFilterOperator" {
            continue;
        }
        let source = wire_port(only_input(filter)?)?;
        if source.output != 0 || consumers[&source.operator] != 1 {
            continue;
        }
        let source_index = *locations.get(&source.operator).ok_or_else(outside_scope)?;
        let map = &operators[source_index];
        match text(map, "class")? {
            "DBSPMapOperator" => {
                let map_input = only_input(map)?;
                let input = wire_port(map_input)?;
                let input_index = *locations.get(&input.operator).ok_or_else(outside_scope)?;
                let producer = &operators[input_index];
                // A second rewrite requires the other branches of Java's fixed point.
                if input.output != 0 || pullable(producer) {
                    return Err(outside_scope());
                }
                let mut pulled_filter = filter.clone();
                pulled_filter["id"] = map["id"].clone();
                pulled_filter["inputs"] = json!([map_input]);
                pulled_filter["outputType"] = inner_reference(field(producer, "outputType")?)?;
                pulled_filter["function"] = rebind_filter(filter, map, definitions, fresh)?;

                let mut projection = map.clone();
                projection["id"] = filter["id"].clone();
                projection["inputs"] = json!([only_input(filter)?]);
                output[source_index] = pulled_filter;
                output[index] = projection;
            }
            "DBSPIncrementalConstantOperator" if retains_overflow(map, filter, definitions)? => {}
            _ if pullable(map) => return Err(outside_scope()),
            _ => {}
        }
    }
    Ok(())
}

fn only_input(operator: &Value) -> Result<&Value> {
    let [input] = array(operator, "inputs")? else {
        return Err(outside_scope());
    };
    Ok(input)
}

fn pullable(operator: &Value) -> bool {
    matches!(
        operator["class"].as_str(),
        Some(
            "DBSPAggregateLinearPostprocessOperator"
                | "DBSPAggregateOperator"
                | "DBSPChainAggregateOperator"
                | "DBSPConstantOperator"
                | "DBSPDifferentiateOperator"
                | "DBSPFilterOperator"
                | "DBSPIncrementalConstantOperator"
                | "DBSPIntegrateOperator"
                | "DBSPMapIndexOperator"
                | "DBSPMapOperator"
                | "DBSPNoopOperator"
        )
    )
}

fn rebind_filter<'a>(
    filter: &'a Value,
    map: &'a Value,
    defs: &Definitions<'a>,
    fresh: &mut impl FnMut() -> Result<u64>,
) -> Result<Value> {
    let map_fn = closure(map, defs)?;
    let filter_fn = closure(filter, defs)?;
    let map_param = parameter(map_fn, defs)?;
    let filter_param = parameter(filter_fn, defs)?;
    let tuple = expect(defs, &map_fn["body"], "DBSPTupleExpression")?;
    let projections = array(tuple, "fields")?;
    // Each projection traverses a field, dereference and variable.
    if projections.len() > 32 {
        return Err(outside_scope());
    }
    for projection in projections {
        field_ordinal(defs.resolve(projection)?, map_param, defs)?;
    }

    let body = expect(defs, &filter_fn["body"], "DBSPUnaryExpression")?;
    if body["opcode"] != "WRAP_BOOL" {
        return Err(outside_scope());
    }
    let comparison = expect(defs, &body["source"], "DBSPBinaryExpression")?;
    if !matches!(
        comparison["opcode"].as_str(),
        Some("GT" | "GTE" | "LT" | "LTE" | "EQ" | "NEQ")
    ) {
        return Err(outside_scope());
    }
    let argument = defs.resolve(&comparison["left"])?;
    let ordinal = field_ordinal(argument, filter_param, defs)?;
    let projected = projections.get(ordinal).ok_or_else(outside_scope)?;
    let projected = defs.resolve(projected)?;
    if defs.ty(&argument["type"], 0)? != defs.ty(&projected["type"], 0)? {
        return Err(outside_scope());
    }
    expect(defs, &comparison["right"], "DBSPI32Literal")?;

    let mut comparison = comparison.clone();
    comparison["id"] = fresh()?.into();
    comparison["left"] = inner_reference(projected)?;
    let mut body = body.clone();
    body["id"] = fresh()?.into();
    body["source"] = comparison;
    let function_type = json!({
        "class": "DBSPTypeFunction", "id": fresh()?, "sourceEmpty": true,
        "resultType": inner_reference(&body["type"])?,
        "parameterTypes": [inner_reference(&map_param["type"])?],
        "code": "FUNCTION", "mayBeNull": false,
    });
    Ok(json!({
        "class": "DBSPClosureExpression", "id": fresh()?, "sourceEmpty": true,
        "type": function_type, "parameters": [inner_reference(map_param)?],
        "body": body,
    }))
}

fn closure<'a>(operator: &'a Value, defs: &Definitions<'a>) -> Result<&'a Value> {
    expect(defs, field(operator, "function")?, "DBSPClosureExpression")
}

fn parameter<'a>(closure: &'a Value, defs: &Definitions<'a>) -> Result<&'a Value> {
    let [parameter] = array(closure, "parameters")? else {
        return Err(outside_scope());
    };
    expect(defs, parameter, "DBSPParameter")
}

fn field_ordinal<'a>(value: &'a Value, parameter: &Value, defs: &Definitions<'a>) -> Result<usize> {
    if value["class"] != "DBSPFieldExpression" {
        return Err(outside_scope());
    }
    let deref = expect(defs, &value["expression"], "DBSPDerefExpression")?;
    let variable = expect(defs, &deref["expression"], "DBSPVariablePath")?;
    if variable["variable"] != parameter["name"] {
        return Err(outside_scope());
    }
    usize::try_from(uint(value, "fieldNo")?).map_err(|_| outside_scope())
}

// The Java simplifier leaves the whole incremental constant alone if any
// predicate fails to fold, even when another row would fold to false.
fn retains_overflow<'a>(
    constant: &'a Value,
    filter: &'a Value,
    defs: &Definitions<'a>,
) -> Result<bool> {
    let closure = closure(filter, defs)?;
    let parameter = parameter(closure, defs)?;
    let compare = expect(defs, &closure["body"], "DBSPBinaryExpression")?;
    if compare["opcode"] != "GT" {
        return Ok(false);
    }
    literal_i32(defs, &compare["right"])?;
    let add = expect(defs, &compare["left"], "DBSPBinaryExpression")?;
    if add["opcode"] != "ADD" {
        return Ok(false);
    }
    let index = field_ordinal(defs.resolve(&add["left"])?, parameter, defs)?;
    let increment = literal_i32(defs, &add["right"])?;
    let rows = expect(defs, &constant["function"], "DBSPZSetExpression")?;
    for row in array(rows, "data")? {
        let row = expect(defs, row, "DBSPTupleExpression")?;
        let field = array(row, "fields")?.get(index).ok_or_else(outside_scope)?;
        if literal_i32(defs, field)?.checked_add(increment).is_none() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn literal_i32<'a>(defs: &Definitions<'a>, value: &'a Value) -> Result<i32> {
    text(expect(defs, value, "DBSPI32Literal")?, "value")?
        .parse()
        .map_err(|_| outside_scope())
}

fn expect<'a>(defs: &Definitions<'a>, value: &'a Value, class: &str) -> Result<&'a Value> {
    let value = defs.resolve(value)?;
    if value["class"] != class {
        return Err(outside_scope());
    }
    Ok(value)
}

fn outside_scope() -> Error {
    Error("filter pull-up outside the supported projection slice".into())
}
