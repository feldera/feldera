// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT

//! Push integral boundaries through operators in the flat optimizer subset.

use super::{
    array, encode_rewrite, text,
    wire_edits::{copy_origin, operator, output_type_from_input, port, WireEdits},
    wire_port,
};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

struct Integral<'a> {
    input: Value,
    origin: &'a Value,
}

fn delta_operator(class: &str, no_inc: bool) -> Option<&str> {
    match class {
        "DBSPStreamJoinOperator" => Some("DBSPJoinOperator"),
        "DBSPStreamJoinIndexOperator" => Some("DBSPJoinIndexOperator"),
        "DBSPStreamAntiJoinOperator" => Some("DBSPAntiJoinOperator"),
        "DBSPStreamDistinctOperator" => Some("DBSPDistinctOperator"),
        "DBSPStreamAggregateOperator" => Some("DBSPAggregateOperator"),
        "DBSPSumOperator"
        | "DBSPAtomicSumOperator"
        | "DBSPSubtractOperator"
        | "DBSPDeindexOperator"
        | "DBSPMapOperator"
        | "DBSPFilterOperator"
        | "DBSPNegateOperator"
        | "DBSPFlatMapOperator"
        | "DBSPNoopOperator"
        | "DBSPHopOperator"
        | "DBSPViewOperator"
        | "DBSPPartitionedRollingAggregateOperator"
        | "DBSPChainAggregateOperator"
        | "DBSPMapIndexOperator" => Some(class),
        "DBSPDifferentiateOperator" if no_inc => Some(class),
        _ => None,
    }
}

pub(crate) fn rewrite(wire: &Value) -> Result<Value> {
    let original = array(wire, "allOperators")?;
    let mut edits = WireEdits::new(wire)?;
    let mut aliases: HashMap<u64, Value> = HashMap::new();
    let mut integrals: HashMap<u64, Integral<'_>> = HashMap::new();
    let mut operators = Vec::with_capacity(original.len());

    for node in original {
        let id = edits.remember_type(node)?;
        let class = text(node, "class")?;
        if class == "DBSPNestedOperator" {
            return Err(Error(
                "nested integral movement requires a scoped rewrite".into(),
            ));
        }
        let inputs = array(node, "inputs")?
            .iter()
            .map(|input| {
                let source = wire_port(input)?;
                Ok(aliases.get(&source.operator).unwrap_or(input).clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let upstream_integrals = inputs
            .iter()
            .map(|input| {
                let port = wire_port(input)?;
                Ok(integrals.get(&port.operator))
            })
            .collect::<Result<Vec<_>>>()?;
        let no_inc = array(node, "annotations")?
            .iter()
            .any(|annotation| annotation["class"] == "NoInc");
        if let [Some(integral)] = upstream_integrals.as_slice() {
            if class == "DBSPDifferentiateOperator" && !no_inc {
                aliases.insert(id, integral.input.clone());
                edits.replaced(id);
                continue;
            }
        }
        let all_integrated = !inputs.is_empty() && upstream_integrals.iter().all(Option::is_some);
        if let Some(delta_class) = delta_operator(class, no_inc).filter(|_| all_integrated) {
            let underlying: Vec<_> = upstream_integrals
                .iter()
                .map(|integral| integral.expect("all inputs are integrals"))
                .collect();
            let delta_inputs: Vec<_> = underlying
                .iter()
                .map(|integral| integral.input.clone())
                .collect();
            let delta_id = edits.id()?;
            let mut delta = node.clone();
            delta["class"] = delta_class.into();
            delta["id"] = delta_id.into();
            delta["inputs"] = json!(delta_inputs);
            if delta_class == "DBSPDeindexOperator" {
                edits.rebuild_deindex(&mut delta, delta_id, &delta_inputs[0])?;
            } else {
                let ty = if output_type_from_input(delta_class) {
                    edits.output_type_id(&delta_inputs[0])?
                } else {
                    edits.output_type_id(&port(id))?
                };
                edits.set_output(delta_id, ty);
                if node.get("outputType").is_some() {
                    delta["outputType"] = json!({"node": ty});
                }
            }
            if delta_class == "DBSPNoopOperator" {
                edits.rebuild_noop(&mut delta, &delta_inputs[0])?;
            }
            if delta_class == "DBSPAggregateOperator" {
                copy_origin(underlying[0].origin, &mut delta);
            }
            operators.push(delta);

            let input = port(delta_id);
            edits.set_output(id, edits.output_type_id(&input)?);
            let mut integral = operator(
                "DBSPIntegrateOperator",
                id,
                vec![input.clone()],
                edits.input_type(&input)?,
            );
            copy_origin(node, &mut integral);
            operators.push(integral);
            integrals.insert(
                id,
                Integral {
                    input,
                    origin: node,
                },
            );
            edits.replaced(id);
        } else {
            let mut retained = edits.retain_with_inputs(node, id, &inputs)?;
            edits.retype_consumer(&mut retained, id)?;
            if class == "DBSPIntegrateOperator" {
                let [input] = inputs.as_slice() else {
                    return Err(Error("expected one integral input".into()));
                };
                integrals.insert(
                    id,
                    Integral {
                        input: input.clone(),
                        origin: node,
                    },
                );
            }
            operators.push(retained);
        }
    }
    encode_rewrite(wire, &edits.finish(wire, operators))
}
