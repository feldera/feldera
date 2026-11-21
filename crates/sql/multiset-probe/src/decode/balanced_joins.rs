//! Select joins whose input traces may be independently balanced.

use super::{array, boolean, text, uint, wire_port};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashSet;

pub(crate) fn select(circuit: &Value) -> Result<Vec<u64>> {
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    let operators = array(circuit, "allOperators")?;
    let mut retained_ports = HashSet::new();
    for operator in operators {
        if matches!(
            text(operator, "class")?,
            "DBSPIntegrateTraceRetainKeysOperator"
                | "DBSPIntegrateTraceRetainValuesOperator"
                | "DBSPIntegrateTraceRetainNValuesOperator"
        ) {
            let [data, _waterline] = array(operator, "inputs")? else {
                return Err(Error("trace retention needs two inputs".into()));
            };
            retained_ports.insert(wire_port(data)?);
        }
    }

    let mut selected = Vec::new();
    // Recursive components cannot use dynamically balanced joins. Only
    // operators in the root component are candidates.
    for join in operators {
        if !matches!(
            text(join, "class")?,
            "DBSPJoinOperator"
                | "DBSPJoinIndexOperator"
                | "DBSPLeftJoinOperator"
                | "DBSPLeftJoinIndexOperator"
                | "DBSPJoinFilterMapOperator"
                | "DBSPLeftJoinFilterMapOperator"
        ) {
            continue;
        }
        boolean(join, "balanced")?;
        let [left, right] = array(join, "inputs")? else {
            return Err(Error("balanced join needs two inputs".into()));
        };
        if !retained_ports.contains(&wire_port(left)?)
            && !retained_ports.contains(&wire_port(right)?)
        {
            selected.push(uint(join, "id")?);
        }
    }
    Ok(selected)
}
