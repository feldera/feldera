//! Identify when the bounded circuit has no shareable operator or constant.

use super::{array, field, text, wire_port, Definitions, WirePort};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

pub(super) fn require_no_duplicates(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut constants: HashMap<&str, Vec<&Value>> = HashMap::new();
    let mut consumers: HashMap<Vec<WirePort>, Vec<&str>> = HashMap::new();
    for node in array(circuit, "allOperators")? {
        let class = text(node, "class")?;
        match class {
            "DBSPNestedOperator" | "DBSPDeltaOperator" => {
                return Err(Error("CSE requires analyzing a recursive scope".into()));
            }
            "DBSPConstantOperator" | "DBSPIncrementalConstantOperator" => {
                let current = definitions.resolve(field(node, "function")?)?;
                let earlier = constants.entry(class).or_default();
                for other in earlier.iter() {
                    if !different_constants(&definitions, current, other)? {
                        return Err(Error("CSE requires comparing equivalent constants".into()));
                    }
                }
                earlier.push(current);
            }
            _ => {
                let inputs = array(node, "inputs")?;
                if inputs.is_empty() {
                    continue;
                }
                let signature = inputs.iter().map(wire_port).collect::<Result<Vec<_>>>()?;
                let earlier = consumers.entry(signature).or_default();
                for other in earlier.iter().copied() {
                    // These two operator classes compute different operations
                    // even when they read the same waterline.
                    if !matches!(
                        (other, class),
                        ("DBSPDelayOperator", "DBSPApplyOperator")
                            | ("DBSPApplyOperator", "DBSPDelayOperator")
                    ) {
                        return Err(Error(format!("CSE requires comparing {other} and {class}")));
                    }
                }
                earlier.push(class);
            }
        }
    }
    Ok(circuit.clone())
}

fn different_constants<'a>(
    definitions: &Definitions<'a>,
    left: &'a Value,
    right: &'a Value,
) -> Result<bool> {
    if left["class"] != "DBSPZSetExpression" || right["class"] != "DBSPZSetExpression" {
        return Ok(false);
    }
    let left_type = definitions.ty(field(left, "elementType")?, 0)?;
    let right_type = definitions.ty(field(right, "elementType")?, 0)?;
    if left_type != right_type {
        return Ok(true);
    }
    let left_rows = array(left, "data")?;
    let right_rows = array(right, "data")?;
    if left_rows.len() != right_rows.len() {
        // The replay codec writes canonical Z-set entries, one per distinct
        // nonzero row. Their cardinalities distinguish these constants.
        return Ok(true);
    }
    Ok(integer_bag(definitions, left)? != integer_bag(definitions, right)?)
}

fn integer_bag<'a>(
    definitions: &Definitions<'a>,
    constant: &'a Value,
) -> Result<BTreeMap<i32, i64>> {
    let rows = array(constant, "data")?;
    let weights = array(constant, "weights")?;
    if rows.len() != weights.len() {
        return Err(Error("inconsistent constant rows and weights".into()));
    }
    let mut bag = BTreeMap::new();
    for (row, weight) in rows.iter().zip(weights) {
        let row = definitions.resolve(row)?;
        let [value] = array(row, "fields")? else {
            return Err(Error("CSE requires a single integer literal field".into()));
        };
        let value = definitions.resolve(value)?;
        if row["class"] != "DBSPTupleExpression" || value["class"] != "DBSPI32Literal" {
            return Err(Error("CSE requires a signed INTEGER tuple literal".into()));
        }
        let value = text(value, "value")?
            .parse::<i32>()
            .map_err(|_| Error("invalid constant integer".into()))?;
        let weight = weight
            .as_i64()
            .filter(|weight| *weight != 0)
            .ok_or_else(|| Error("invalid constant weight".into()))?;
        if bag.insert(value, weight).is_some() {
            return Err(Error("duplicate integer constant row".into()));
        }
    }
    Ok(bag)
}
