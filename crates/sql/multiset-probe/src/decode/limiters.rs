//! Replay the optimizer's supported waterline and error-stream rewrites.

mod clock;
mod lateness;

use super::{
    array, boolean, encode_rewrite, field,
    generated_closure::{advance_name, GENERATED_NAME},
    inner_reference, text, uint,
    wire_edits::{copy_origin, operator, port, WireEdits},
    wire_port,
};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) fn insert_limiters(circuit: &Value) -> Result<Value> {
    let operators = array(circuit, "allOperators")?;
    if input_needs_bounds(operators)? {
        if let Some(reserved_names) = clock::unused_bound_names(circuit)? {
            return wire_error_stream(circuit, operators, reserved_names);
        }
        return lateness::rewrite(circuit);
    }
    wire_error_stream(circuit, operators, 0)
}

pub(crate) fn analyze(circuit: &Value) -> Result<Value> {
    let (kind, name_count) = if !input_needs_bounds(array(circuit, "allOperators")?)? {
        ("unbounded", 0)
    } else if clock::unused_bound_names(circuit)?.is_some() {
        ("unconsumed-clock", 2)
    } else if lateness::uses_distinct_bound(circuit)? {
        ("timestamp-distinct", 3)
    } else {
        return Ok(Value::Null);
    };
    Ok(json!({
        "kind": kind,
        "next_generated_name": advance_name(uint(circuit, GENERATED_NAME)?, name_count)?,
    }))
}

fn input_needs_bounds(operators: &[Value]) -> Result<bool> {
    for operator in operators {
        if needs_bound_analysis(operator)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn wire_error_stream(circuit: &Value, operators: &[Value], reserved_names: usize) -> Result<Value> {
    let by_id = operators
        .iter()
        .map(|node| Ok((uint(node, "id")?, node)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut edits = WireEdits::new(circuit)?;
    for _ in 0..reserved_names {
        edits.generated_name()?;
    }
    let incremental = boolean(circuit, "incremental")?;
    let mut graph = Vec::with_capacity(operators.len() + 1);
    for before in operators {
        let id = edits.remember_type(before)?;
        let mut after = if before["class"] == "DBSPViewOperator"
            && before["viewName"]["name"] == "error_view"
        {
            let [input] = array(before, "inputs")? else {
                return Err(Error("error view requires exactly one input".into()));
            };
            let source = by_id
                .get(&wire_port(input)?.operator)
                .ok_or_else(|| Error("unknown source for the system error view".into()))?;
            if source["class"] != "DBSPSourceMultisetOperator"
                || source["tableName"]["name"] != "feldera_error_table"
            {
                return Err(Error("unexpected source for the system error view".into()));
            }
            let input = add_empty_constant(before, &mut edits, &mut graph, incremental)?;
            edits.retain_with_inputs(before, id, &[input])?
        } else {
            edits.retain(before, id)?
        };
        edits.retype_consumer(&mut after, id)?;
        graph.push(after);
    }
    let rewritten = edits.finish(circuit, graph);
    encode_rewrite(circuit, &rewritten)
}

fn needs_bound_analysis(node: &Value) -> Result<bool> {
    let metadata = &node["metadata"];
    let columns = match text(node, "class")? {
        "DBSPSourceMultisetOperator" => {
            if node["tableName"]["name"] == "now" || metadata["changes"] != "Unrestricted" {
                return Ok(true);
            }
            array(metadata, "columnMetadata")?
        }
        "DBSPViewOperator" | "DBSPSinkOperator" => return view_needs_bounds(metadata),
        "DBSPMapOperator"
        | "DBSPMapIndexOperator"
        | "DBSPDistinctOperator"
        | "DBSPIncrementalConstantOperator"
        | "DBSPConstantOperator"
        | "DBSPSumOperator"
        | "DBSPPositiveOperator"
        | "DBSPNegateOperator"
        | "DBSPFilterOperator" => return Ok(false),
        _ => return Ok(true),
    };
    Ok(columns.iter().any(|column| !column["lateness"].is_null()))
}

fn view_needs_bounds(metadata: &Value) -> Result<bool> {
    Ok(metadata["emitFinalColumn"] != -1
        || array(metadata, "columns")?
            .iter()
            .any(|column| !column["lateness"].is_null()))
}

fn add_empty_constant(
    view: &Value,
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
    incremental: bool,
) -> Result<Value> {
    let ty = edits.definitions().resolve(field(view, "outputType")?)?;
    if ty["class"] != "DBSPTypeZSet" {
        return Err(Error("system error view requires a multiset".into()));
    }
    let element = inner_reference(field(ty, "elementType")?)?;
    let mut output = ty.clone();
    output["elementType"] = element.clone();
    output["id"] = json!(edits.inner_id()?);
    let expression = json!({
        "class": "DBSPZSetExpression", "id": edits.inner_id()?, "sourceEmpty": true,
        "elementType": element, "type": inner_reference(&output)?, "data": [], "weights": [],
    });
    let constant_class = if incremental {
        "DBSPIncrementalConstantOperator"
    } else {
        "DBSPConstantOperator"
    };
    let id = edits.id()?;
    edits.remember_generated_output(id, &output)?;
    let mut constant = operator(constant_class, id, Vec::new(), output);
    constant["function"] = expression;
    copy_origin(view, &mut constant);
    graph.push(constant);
    Ok(port(id))
}
