//! Validate trace-retention placement in the original optimizer visit order.

use super::{array, boolean, field, nesting, text, uint, wire_port};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Dimension {
    Keys,
    Values,
}

pub(crate) fn duplicate(circuit: &Value) -> Result<Value> {
    duplicate_in(array(circuit, "allOperators")?, 0)
}

pub(crate) fn merge_gc(circuit: &Value) -> Result<Value> {
    check_noop_inputs(array(circuit, "allOperators")?, 0)?;
    if !duplicate(circuit)?.is_null() {
        return Err(Error("multiple retention policies require merging".into()));
    }
    Ok(circuit.clone())
}

fn check_noop_inputs(operators: &[Value], depth: usize) -> Result<()> {
    nesting(depth)?;
    let mut seen = HashSet::new();
    for operator in operators {
        match text(operator, "class")? {
            "DBSPNestedOperator" => check_noop_inputs(array(operator, "allOperators")?, depth + 1)?,
            "DBSPNoopOperator" => {
                let [input] = array(operator, "inputs")? else {
                    return Err(Error("trace noop requires one input".into()));
                };
                if !seen.insert(wire_port(input)?) {
                    return Err(Error(
                        "shared noop input requires retention analysis".into(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn duplicate_in(operators: &[Value], depth: usize) -> Result<Value> {
    nesting(depth)?;
    let mut seen = HashMap::new();
    for operator in operators {
        let id = uint(operator, "id")?;
        let dimension = match text(operator, "class")? {
            "DBSPNestedOperator" => {
                let violation = duplicate_in(array(operator, "allOperators")?, depth + 1)?;
                if !violation.is_null() {
                    return Ok(violation);
                }
                continue;
            }
            "DBSPIntegrateTraceRetainKeysOperator" => Dimension::Keys,
            "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator" => Dimension::Values,
            _ => continue,
        };
        let [data, _] = array(operator, "inputs")? else {
            return Err(Error("trace retention requires data and bounds".into()));
        };
        if let Some(previous) = seen.insert((wire_port(data)?, dimension), id) {
            return Ok(json!({
                "operator": previous, "other": id,
                "dimension": match dimension {
                    Dimension::Keys => "Key",
                    Dimension::Values => "Value",
                }
            }));
        }
    }
    Ok(Value::Null)
}

pub(crate) fn violation(input: &Value) -> Result<Value> {
    let circuit = field(input, "circuit")?;
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    // These are type capabilities, since the wire class names do not encode
    // the Java hierarchy. All edge and retention decisions are made below.
    let mut capabilities = HashMap::new();
    for entry in array(input, "operator_capabilities")? {
        let operator = uint(entry, "operator")?;
        let usable = !boolean(entry, "simple")? || boolean(entry, "contains_integrator")?;
        if capabilities.insert(operator, usable).is_some() {
            return Err(Error("duplicate trace capability entry".into()));
        }
    }
    check_component(array(circuit, "allOperators")?, &capabilities)
}

fn check_component(operators: &[Value], capabilities: &HashMap<u64, bool>) -> Result<Value> {
    let mut classes = HashMap::new();
    let mut has_sibling = HashSet::new();
    for operator in operators {
        let id = uint(operator, "id")?;
        let class = text(operator, "class")?;
        classes.insert(id, class);
        let usable = capabilities
            .get(&id)
            .ok_or_else(|| Error(format!("missing trace capability for operator {id}")))?;
        // Graph omits delta edges, which cross circuit boundaries.
        if *usable && class != "DBSPDeltaOperator" {
            for input in array(operator, "inputs")? {
                has_sibling.insert(wire_port(input)?.operator);
            }
        }
    }
    let mut retainers = HashMap::new();
    for operator in operators {
        let id = uint(operator, "id")?;
        let (dimension, accumulate) = match classes[&id] {
            "DBSPNestedOperator" => {
                let nested = check_component(array(operator, "allOperators")?, capabilities)?;
                if !nested.is_null() {
                    return Ok(nested);
                }
                continue;
            }
            "DBSPIntegrateTraceRetainKeysOperator" => {
                (Dimension::Keys, boolean(operator, "accumulate")?)
            }
            "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator" => (Dimension::Values, true),
            _ => continue,
        };
        let [collection, _waterline] = array(operator, "inputs")? else {
            return Err(Error(
                "trace retainer needs a collection and a waterline".into(),
            ));
        };
        let source = wire_port(collection)?;
        let source_class = classes
            .get(&source.operator)
            .ok_or_else(|| Error(format!("missing trace input operator {}", source.operator)))?;
        let input_table = *source_class == "DBSPInputMapWithWaterlineOperator";
        if input_table == accumulate {
            let reason = if input_table {
                "input_accumulate"
            } else {
                "trace_nonaccumulate"
            };
            return Ok(json!({"operator": id, "reason": reason}));
        }
        if let Some(previous) = retainers.insert((source, dimension), id) {
            return Ok(json!({"operator": id, "reason": "duplicate", "previous": previous}));
        }
        let owns_key_trace = dimension == Dimension::Keys
            && matches!(
                *source_class,
                "DBSPAggregateLinearPostprocessRetainKeysOperator"
                    | "DBSPChainAggregateOperator"
                    | "DBSPAggregateOperator"
                    | "DBSPLagOperator"
            );
        if !input_table && !owns_key_trace && !has_sibling.contains(&source.operator) {
            return Ok(json!({"operator": id, "reason": "missing_sibling"}));
        }
    }
    Ok(Value::Null)
}
