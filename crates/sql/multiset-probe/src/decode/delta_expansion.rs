//! Expand the distinct and binary join operators consumed by monotonicity.

use super::{
    array, encode_rewrite, field, text, uint,
    wire_edits::{copy_origin, operator, port, WireEdits},
    wire_port,
};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) fn expand(circuit: &Value) -> Result<Value> {
    let operators = array(circuit, "allOperators")?;
    let by_id = operators
        .iter()
        .map(|operator| Ok((uint(operator, "id")?, operator)))
        .collect::<Result<HashMap<_, _>>>()?;
    let mut edits = WireEdits::new(circuit)?;
    let mut graph = Vec::with_capacity(operators.len());
    let mut expansions = Vec::new();
    for before in operators {
        let id = edits.remember_type(before)?;
        let expansion = match text(before, "class")? {
            "DBSPDistinctOperator" | "DBSPPositiveOperator" => {
                let expanded = expand_distinct(before, id, &mut edits, &mut graph)?;
                edits.replaced(id);
                expanded
            }
            "DBSPJoinOperator" => {
                let expanded = expand_join(before, id, &by_id, &mut edits, &mut graph)?;
                edits.replaced(id);
                expanded
            }
            class @ ("DBSPSourceMultisetOperator"
            | "DBSPIncrementalConstantOperator"
            | "DBSPConstantOperator"
            | "DBSPViewOperator"
            | "DBSPSinkOperator"
            | "DBSPSumOperator"
            | "DBSPNegateOperator"
            | "DBSPMapOperator"
            | "DBSPMapIndexOperator"
            | "DBSPFilterOperator"
            | "DBSPChainAggregateOperator"
            | "DBSPNoopOperator"
            | "DBSPStreamJoinOperator") => {
                let mut after = edits.retain(before, id)?;
                if class == "DBSPNoopOperator" && after["function"] == before["function"] {
                    let [input] = array(before, "inputs")? else {
                        return Err(Error("trace noop requires one input".into()));
                    };
                    edits.rebuild_noop(&mut after, input)?;
                    edits.replaced(id);
                }
                edits.retype_consumer(&mut after, id)?;
                graph.push(after);
                if matches!(
                    class,
                    "DBSPIncrementalConstantOperator" | "DBSPConstantOperator"
                ) {
                    continue;
                }
                json!({"original": id, "kind": "replacement", "operator": id})
            }
            class => {
                return Err(Error(format!(
                    "delta expansion outside the measured slice: {class}"
                )))
            }
        };
        expansions.push(expansion);
    }
    let expanded = edits.finish(circuit, graph);
    Ok(json!({"circuit": encode_rewrite(circuit, &expanded)?, "expansions": expansions}))
}

fn expand_distinct(
    source: &Value,
    id: u64,
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
) -> Result<Value> {
    let [input] = array(source, "inputs")? else {
        return Err(Error("distinct expansion requires one input".into()));
    };
    let ty = edits.input_type(input)?;
    let integrator = edits.id()?;
    let mut integrated = operator(
        "DBSPIntegrateOperator",
        integrator,
        vec![input.clone()],
        ty.clone(),
    );
    copy_origin(source, &mut integrated);
    graph.push(integrated);
    let mut distinct = operator(
        "DBSPBinaryDistinctOperator",
        id,
        vec![port(integrator), input.clone()],
        ty,
    );
    distinct["positive"] = (source["class"] == "DBSPPositiveOperator").into();
    copy_origin(source, &mut distinct);
    graph.push(distinct);
    Ok(json!({"original": id, "kind": "distinct", "integrator": integrator, "operator": id}))
}

fn expand_join(
    source: &Value,
    id: u64,
    by_id: &HashMap<u64, &Value>,
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
) -> Result<Value> {
    let [left, right] = array(source, "inputs")? else {
        return Err(Error("join expansion requires two inputs".into()));
    };
    let ty = field(source, "outputType")?;
    let left_integral = delayed(source, left, by_id, edits, graph)?;
    let right_delta = join(
        source,
        ty,
        [port(left_integral), right.clone()],
        edits,
        graph,
    )?;
    let right_integral = delayed(source, right, by_id, edits, graph)?;
    let left_delta = join(
        source,
        ty,
        [left.clone(), port(right_integral)],
        edits,
        graph,
    )?;
    let both = join(source, ty, [left.clone(), right.clone()], edits, graph)?;
    let mut sum = operator(
        "DBSPSumOperator",
        id,
        vec![port(right_delta), port(left_delta), port(both)],
        ty.clone(),
    );
    copy_origin(source, &mut sum);
    graph.push(sum);
    Ok(json!({
        "original": id, "kind": "join", "operator": id,
        "left_integrator": left_integral, "right_integrator": right_integral,
        "left_delta": left_delta, "right_delta": right_delta, "both": both,
    }))
}

fn delayed(
    source: &Value,
    input: &Value,
    by_id: &HashMap<u64, &Value>,
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
) -> Result<u64> {
    let id = edits.id()?;
    let mut delayed = operator(
        "DBSPDelayedIntegralOperator",
        id,
        vec![input.clone()],
        edits.input_type(input)?,
    );
    let producer = by_id
        .get(&wire_port(input)?.operator)
        .ok_or_else(|| Error("unknown join input".into()))?;
    delayed["annotations"] = field(producer, "annotations")?.clone();
    copy_origin(source, &mut delayed);
    graph.push(delayed);
    Ok(id)
}

fn join(
    source: &Value,
    ty: &Value,
    inputs: [Value; 2],
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
) -> Result<u64> {
    let id = edits.id()?;
    let mut join = operator("DBSPStreamJoinOperator", id, inputs.into(), ty.clone());
    join["function"] = field(source, "function")?.clone();
    join["balanced"] = field(source, "balanced")?.clone();
    copy_origin(source, &mut join);
    graph.push(join);
    Ok(id)
}
