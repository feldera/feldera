//! Lower snapshot join variants that have no implementation in DBSP.

use super::{
    array, encode_rewrite, field, inner_reference,
    wire_edits::{copy_intermediate_origin, copy_origin, operator, port, WireEdits},
};
use crate::{provenance::Provenance, Error, Result};
use serde_json::Value;

fn delta_join_class(operator: &Value) -> Option<&'static str> {
    match operator["class"].as_str()? {
        "DBSPStreamAntiJoinOperator" => Some("DBSPAntiJoinOperator"),
        "DBSPStreamJoinIndexOperator" => Some("DBSPJoinIndexOperator"),
        _ => None,
    }
}

fn reject_nested_snapshot_joins(operators: &[Value]) -> Result<()> {
    let mut pending = vec![(operators, false)];
    while let Some((operators, nested)) = pending.pop() {
        for node in operators {
            if node["class"] == "DBSPNestedOperator" {
                pending.push((array(node, "allOperators")?, true));
            } else if nested && delta_join_class(node).is_some() {
                return Err(Error("snapshot join in a nested delta circuit".into()));
            }
        }
    }
    Ok(())
}

pub(crate) fn rewrite(wire: &Value) -> Result<Value> {
    let original = array(wire, "allOperators")?;
    reject_nested_snapshot_joins(original)?;
    let mut edits = WireEdits::new(wire)?;
    let mut provenance = Provenance::read(original)?;
    let mut operators = Vec::with_capacity(original.len());
    for node in original {
        let id = edits.remember_type(node)?;
        let Some(class) = delta_join_class(node) else {
            operators.push(edits.retain(node, id)?);
            continue;
        };
        let [left, right] = array(node, "inputs")? else {
            return Err(Error("expected two snapshot join inputs".into()));
        };
        let mut differences = Vec::with_capacity(2);
        for input in [left, right] {
            let id = edits.id()?;
            let mut differentiate = operator(
                "DBSPDifferentiateOperator",
                id,
                vec![input.clone()],
                edits.input_type(input)?,
            );
            copy_intermediate_origin(node, &mut differentiate, &mut provenance)?;
            differences.push(port(id));
            operators.push(differentiate);
        }
        let join_id = edits.id()?;
        let output_type = if class == "DBSPAntiJoinOperator" {
            edits.input_type(left)?
        } else {
            inner_reference(field(node, "outputType")?)?
        };
        let mut join = operator(class, join_id, differences, output_type.clone());
        if class == "DBSPJoinIndexOperator" {
            join["function"] = inner_reference(field(node, "function")?)?;
            join["balanced"] = field(node, "balanced")?.clone();
        }
        copy_intermediate_origin(node, &mut join, &mut provenance)?;
        operators.push(join);

        let mut integrate = operator(
            "DBSPIntegrateOperator",
            id,
            vec![port(join_id)],
            output_type,
        );
        copy_origin(node, &mut integrate);
        operators.push(integrate);
        edits.replaced(id);
    }
    encode_rewrite(wire, &edits.finish(wire, operators))
}
