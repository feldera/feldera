//! Boundedness of the flat retention and output-distinct slice.

use super::{array, field, nesting, text, uint, wire_port, Definitions, WirePort};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashSet;

pub(crate) fn analyze(circuit: &Value) -> Result<Value> {
    let operators = array(circuit, "allOperators")?;
    if !is_streaming(operators, 0)? {
        return Ok(json!([]));
    }
    let defs = Definitions::read(circuit)?;
    let mut bounded = HashSet::new();
    let mut collected = HashSet::new();
    for node in operators {
        if is_trace_retention(text(node, "class")?) {
            let input = array(node, "inputs")?.first().ok_or_else(outside_scope)?;
            let port = wire_port(input)?;
            bounded.insert(port);
            collected.insert(port.operator);
        }
    }
    let mut warnings = Vec::new();
    for node in operators {
        let id = uint(node, "id")?;
        let class = text(node, "class")?;
        let inputs = array(node, "inputs")?
            .iter()
            .map(wire_port)
            .collect::<Result<Vec<_>>>()?;
        let all_inputs_bounded = inputs.iter().all(|port| bounded.contains(port));
        let mut outputs = 1;
        let known_bounded = match class {
            "DBSPSourceMultisetOperator" => {
                !node["metadata"]["expected_size"].is_null() || node["tableName"]["name"] == "now"
            }
            "DBSPWaterlineOperator"
            | "DBSPConstantOperator"
            | "DBSPIncrementalConstantOperator" => true,
            "DBSPControlledKeyFilterOperator" => {
                outputs = 2;
                all_inputs_bounded
            }
            "DBSPDelayOperator" | "DBSPApplyOperator" | "DBSPViewOperator" | "DBSPSinkOperator"
            | "DBSPMapOperator" | "DBSPFilterOperator" => all_inputs_bounded,
            "DBSPDistinctOperator" => {
                if inputs.len() != 1 {
                    return Err(outside_scope());
                }
                let finite = has_finite_rows(field(node, "outputType")?, &defs)?;
                if !finite && !all_inputs_bounded && !collected.contains(&id) {
                    warnings.push(json!({"operator": id, "inputs": [0]}));
                }
                finite || all_inputs_bounded
            }
            class if is_trace_retention(class) => false,
            _ => return Err(outside_scope()),
        };
        if known_bounded {
            bounded.extend((0..outputs).map(|output| WirePort {
                operator: id,
                output,
            }));
        }
    }
    Ok(warnings.into())
}

fn is_streaming(operators: &[Value], depth: usize) -> Result<bool> {
    nesting(depth)?;
    for node in operators {
        let metadata = &node["metadata"];
        match text(node, "class")? {
            "DBSPSourceMultisetOperator"
            | "DBSPSourceMapOperator"
            | "DBSPSourceLazyMapOperator" => {
                if metadata["changes"] == "AppendOnly"
                    || has_lateness(array(metadata, "columnMetadata")?)
                {
                    return Ok(true);
                }
            }
            "DBSPViewOperator" | "DBSPSinkOperator" => {
                if has_lateness(array(metadata, "columns")?) {
                    return Ok(true);
                }
            }
            "DBSPWindowOperator" => return Ok(true),
            "DBSPNestedOperator" if is_streaming(array(node, "allOperators")?, depth + 1)? => {
                return Ok(true);
            }
            _ => {}
        }
    }
    Ok(false)
}

fn has_lateness(columns: &[Value]) -> bool {
    columns.iter().any(|column| !column["lateness"].is_null())
}

fn has_finite_rows<'a>(ty: &'a Value, defs: &Definitions<'a>) -> Result<bool> {
    let ty = defs.resolve(ty)?;
    if ty["class"] != "DBSPTypeZSet" {
        return Ok(false);
    }
    let row = defs.resolve(field(ty, "elementType")?)?;
    if row["class"] != "DBSPTypeTuple" && row["class"] != "DBSPTypeRawTuple" {
        return Ok(false);
    }
    let mut domain = 1_u64;
    for field in array(row, "tupFields")? {
        let field = defs.resolve(field)?;
        let size = match text(field, "code")? {
            "BOOL" => 2,
            "INT8" | "UINT8" => 256,
            _ => return Ok(false),
        };
        domain *= size + u64::from(field["mayBeNull"] == true);
        if domain > 1024 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn is_trace_retention(class: &str) -> bool {
    matches!(
        class,
        "DBSPIntegrateTraceRetainKeysOperator"
            | "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator"
    )
}

fn outside_scope() -> Error {
    Error("streaming state graph is outside the flat retention slice".into())
}
