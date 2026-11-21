//! Convert flat snapshot circuits to the initial, unoptimized delta form.

use super::{
    array, encode_rewrite, field, inner_reference, text,
    wire_edits::{copy_origin, operator, port, WireEdits},
};
use crate::{Error, Result};
use serde_json::{json, Value};

fn unary(class: &str, id: u64, input: Value, ty: Value, origin: &Value) -> Value {
    let mut result = operator(class, id, vec![input], ty);
    copy_origin(origin, &mut result);
    result
}

pub(crate) fn rewrite(wire: &Value) -> Result<Value> {
    let original = array(wire, "allOperators")?;
    let mut edits = WireEdits::new(wire)?;
    let mut operators = Vec::with_capacity(original.len());
    for node in original {
        let id = edits.remember_type(node)?;
        match text(node, "class")? {
            "DBSPSourceMultisetOperator"
            | "DBSPSourceMapOperator"
            | "DBSPConstantOperator"
            | "DBSPNowOperator" => {
                let ty = inner_reference(field(node, "outputType")?)?;
                let mut delta = node.clone();
                let mut delta_id = edits.id()?;
                delta["id"] = delta_id.into();
                if node["class"] == "DBSPConstantOperator" {
                    // The first transaction supplies the snapshot constant.
                    delta["class"] = "DBSPIncrementalConstantOperator".into();
                }
                operators.push(delta);
                if node["class"] == "DBSPNowOperator" {
                    let snapshot = delta_id;
                    delta_id = edits.id()?;
                    operators.push(unary(
                        "DBSPDifferentiateOperator",
                        delta_id,
                        port(snapshot),
                        ty.clone(),
                        node,
                    ));
                }
                let mut integral = unary("DBSPIntegrateOperator", id, port(delta_id), ty, node);
                if node["class"] == "DBSPConstantOperator" {
                    integral["annotations"] = field(node, "annotations")?.clone();
                }
                operators.push(integral);
                edits.replaced(id);
            }
            "DBSPSinkOperator" => {
                let [input] = array(node, "inputs")? else {
                    return Err(Error("expected one sink input".into()));
                };
                let ty = edits.input_type(input)?;
                let diff_id = edits.id()?;
                operators.push(unary(
                    "DBSPDifferentiateOperator",
                    diff_id,
                    input.clone(),
                    ty,
                    node,
                ));
                let mut sink = node.clone();
                sink["inputs"] = json!([port(diff_id)]);
                sink["annotations"] = json!([]);
                operators.push(sink);
                edits.replaced(id);
            }
            "DBSPNestedOperator" => {
                return Err(Error(
                    "nested incrementalization requires a scoped rewrite".into(),
                ));
            }
            _ => operators.push(edits.retain(node, id)?),
        }
    }
    let mut result = edits.finish(wire, operators);
    result["incremental"] = true.into();
    encode_rewrite(wire, &result)
}
