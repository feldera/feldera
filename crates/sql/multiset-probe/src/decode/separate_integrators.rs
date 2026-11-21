//! Give each integrating consumer its own trace when the source already has
//! a trace or several inputs would integrate the same source.

use super::{
    array, boolean, encode_rewrite, field, text, uint,
    wire_edits::{copy_origin, operator, port, WireEdits},
    wire_port,
};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn integrates_input(node: &Value, index: usize) -> Result<bool> {
    Ok(match text(node, "class")? {
        "DBSPSinkOperator" => text(field(node, "metadata")?, "viewKind")? == "MATERIALIZED",
        "DBSPAntiJoinOperator" | "DBSPWindowOperator" => index == 0,
        "DBSPAggregateOperator"
        | "DBSPAsofJoinOperator"
        | "DBSPConcreteAsofJoinOperator"
        | "DBSPDistinctOperator"
        | "DBSPIndexedTopKOperator"
        | "DBSPIntegrateOperator"
        | "DBSPJoinFilterMapOperator"
        | "DBSPJoinIndexOperator"
        | "DBSPJoinOperator"
        | "DBSPLagOperator"
        | "DBSPLeftJoinFilterMapOperator"
        | "DBSPLeftJoinIndexOperator"
        | "DBSPLeftJoinOperator"
        | "DBSPPartitionedRollingAggregateOperator"
        | "DBSPPositiveOperator"
        | "DBSPStarJoinFilterMapOperator"
        | "DBSPStarJoinIndexOperator"
        | "DBSPStarJoinOperator"
        | "DBSPStreamJoinIndexOperator"
        | "DBSPStreamJoinOperator" => true,
        _ => false,
    })
}

fn stores_output(node: &Value) -> Result<bool> {
    Ok(match text(node, "class")? {
        "DBSPSourceMultisetOperator" | "DBSPSourceMapOperator" | "DBSPSourceLazyMapOperator" => {
            boolean(field(node, "metadata")?, "materialized")?
        }
        "DBSPAggregateLinearPostprocessOperator"
        | "DBSPAggregateLinearPostprocessRetainKeysOperator"
        | "DBSPAggregateOperator"
        | "DBSPChainAggregateOperator"
        | "DBSPIndexedTopKOperator"
        | "DBSPIntegrateOperator"
        | "DBSPLagOperator"
        | "DBSPPartitionedRollingAggregateOperator"
        | "DBSPPartitionedRollingAggregateWithWaterlineOperator"
        | "DBSPRankOperator"
        | "DBSPRowNumberOperator" => true,
        _ => false,
    })
}

pub(crate) fn rewrite(circuit: &Value) -> Result<Value> {
    let original = array(circuit, "allOperators")?;
    let mut edits = WireEdits::new(circuit)?;
    if !boolean(circuit, "incremental")? {
        return Err(Error(
            "separating integrators requires delta streams".into(),
        ));
    }

    let mut stored = HashSet::new();
    let mut users = HashMap::<u64, usize>::new();
    for node in original {
        match text(node, "class")? {
            "DBSPNestedOperator" | "DBSPDeltaOperator" | "DBSPViewDeclarationOperator" => {
                return Err(Error(
                    "separating integrators requires a flat circuit".into(),
                ));
            }
            "DBSPStreamAggregateOperator" => {
                return Err(Error(
                    "snapshot aggregate remains in a delta circuit".into(),
                ));
            }
            _ => {}
        }
        if stores_output(node)? {
            stored.insert(uint(node, "id")?);
        }
        for (index, input) in array(node, "inputs")?.iter().enumerate() {
            if integrates_input(node, index)? {
                // Count edges. Both inputs of a self-join need separate traces.
                *users.entry(wire_port(input)?.operator).or_default() += 1;
            }
        }
    }

    let mut operators = Vec::with_capacity(original.len());
    for node in original {
        let id = edits.remember_type(node)?;
        let mut inputs = array(node, "inputs")?.to_vec();
        for (index, input) in inputs.iter_mut().enumerate() {
            let source = wire_port(input)?.operator;
            if integrates_input(node, index)? && (stored.contains(&source) || users[&source] > 1) {
                let id = edits.id()?;
                let ty = edits.input_type(input)?;
                let mut noop = operator("DBSPNoopOperator", id, vec![input.clone()], ty);
                copy_origin(node, &mut noop);
                edits.rebuild_noop(&mut noop, input)?;
                operators.push(noop);
                *input = port(id);
            }
        }
        operators.push(edits.retain_with_inputs(node, id, &inputs)?);
    }
    encode_rewrite(circuit, &edits.finish(circuit, operators))
}
