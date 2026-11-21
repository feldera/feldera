//! Identify operators downstream of the compiler's error table.

use super::{array, uint, wire_port, Definitions};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn select(circuit: &Value) -> Result<Vec<u64>> {
    Definitions::read(circuit)?;
    let operators = array(circuit, "allOperators")?;
    let mut reachable = HashMap::with_capacity(operators.len());
    let mut consumers = Vec::new();
    let mut seen_source = false;
    for operator in operators {
        let class = operator["class"].as_str();
        if matches!(
            class,
            Some("DBSPNestedOperator" | "DBSPDeltaOperator" | "DBSPViewDeclarationOperator")
        ) {
            return Err(Error(
                "error consumer selection requires a flat circuit".into(),
            ));
        }
        let id = uint(operator, "id")?;
        let is_source = class == Some("DBSPSourceMultisetOperator")
            && operator["tableName"]["name"] == "feldera_error_table";
        if is_source && std::mem::replace(&mut seen_source, true) {
            return Err(Error("duplicate error table source".into()));
        }

        // Every producer precedes its consumers in a flat circuit.
        let mut downstream = is_source;
        for input in array(operator, "inputs")? {
            let producer = wire_port(input)?.operator;
            downstream |= *reachable.get(&producer).ok_or_else(|| {
                Error(format!(
                    "operator {id} references unavailable producer {producer}"
                ))
            })?;
        }
        if reachable.insert(id, downstream).is_some() {
            return Err(Error(format!("duplicate operator {id}")));
        }
        if downstream {
            consumers.push(id);
        }
    }
    Ok(consumers)
}
