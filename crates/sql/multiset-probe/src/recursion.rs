//! Operator legality across nested circuit components.

use crate::{Error, Result};
use serde_json::{json, Value};

pub(crate) fn violation(circuit: &Value) -> Result<Value> {
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    Ok(first_violation(operators(circuit)?, false)?.unwrap_or(Value::Null))
}

pub(crate) fn first_integral(circuit: &Value) -> Result<Option<u64>> {
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    find_integral(operators(circuit)?)
}

fn find_integral(nodes: &[Value]) -> Result<Option<u64>> {
    for node in nodes {
        if node["class"] == "DBSPIntegrateOperator" {
            return node["id"]
                .as_u64()
                .map(Some)
                .ok_or_else(|| Error("integration operator identity is missing".into()));
        }
        if node["class"] == "DBSPNestedOperator" {
            if let Some(id) = find_integral(operators(node)?)? {
                return Ok(Some(id));
            }
        }
    }
    Ok(None)
}

fn operators(node: &Value) -> Result<&[Value]> {
    node["allOperators"]
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| Error("missing recursive circuit operators".into()))
}

fn first_violation(nodes: &[Value], recursive: bool) -> Result<Option<Value>> {
    for node in nodes {
        let class = node["class"]
            .as_str()
            .ok_or_else(|| Error("operator class is missing".into()))?;
        if class == "DBSPNestedOperator" {
            if let Some(nested) = first_violation(operators(node)?, true)? {
                return Ok(Some(nested));
            }
        }
        if recursive {
            if let Some((operation, internal)) = forbidden(class) {
                let id = node["id"]
                    .as_u64()
                    .ok_or_else(|| Error("recursive operator identity is missing".into()))?;
                return Ok(Some(
                    json!({"operator": id, "operation": operation, "internal": internal}),
                ));
            }
        }
    }
    Ok(None)
}

fn forbidden(class: &str) -> Option<(&'static str, bool)> {
    Some(match class {
        "DBSPApplyOperator" => ("apply", true),
        "DBSPApply2Operator" => ("apply2", true),
        "DBSPIndexedTopKOperator" => ("TopK", false),
        "DBSPRankOperator" => ("RANK", false),
        "DBSPRowNumberOperator" => ("ROW_NUMBER", false),
        "DBSPIntegrateTraceRetainKeysOperator"
        | "DBSPIntegrateTraceRetainValuesOperator"
        | "DBSPIntegrateTraceRetainNValuesOperator" => ("GC", true),
        "DBSPLagOperator" => ("LAG", false),
        "DBSPNestedOperator" => ("recursion", true),
        "DBSPNowOperator" => ("NOW", true),
        "DBSPPartitionedRollingAggregateOperator"
        | "DBSPPartitionedRollingAggregateWithWaterlineOperator" => ("OVER", false),
        "DBSPSourceMultisetOperator" | "DBSPSourceMapOperator" => ("input", true),
        "DBSPWaterlineOperator" => ("WATERLINE", false),
        "DBSPWindowOperator" => ("WINDOW", false),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_violations_follow_java_postorder_and_scope() {
        let circuit = json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPWindowOperator", "id": 1},
                {"class": "DBSPNestedOperator", "id": 2, "allOperators": [
                    {"class": "DBSPViewDeclarationOperator", "id": 3},
                    {"class": "DBSPNestedOperator", "id": 4, "allOperators": [
                        {"class": "DBSPLagOperator", "id": 5}
                    ]},
                    {"class": "DBSPRankOperator", "id": 6}
                ]}
            ]
        });
        assert_eq!(
            violation(&circuit).unwrap(),
            json!({"operator": 5, "operation": "LAG", "internal": false}),
        );
        let mut empty_inner = circuit;
        empty_inner["allOperators"][1]["allOperators"][1]["allOperators"] = json!([]);
        assert_eq!(
            violation(&empty_inner).unwrap(),
            json!({"operator": 4, "operation": "recursion", "internal": true}),
        );
    }
}
