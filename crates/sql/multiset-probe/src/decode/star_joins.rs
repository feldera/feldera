//! Account for the candidate closure of each isolated stream join.

use super::{advance_name, array, text, uint, wire_port, GENERATED_NAME};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;

pub(crate) fn analyze(circuit: &Value) -> Result<Value> {
    let operators = array(circuit, "allOperators")?;
    let mut classes = HashMap::new();
    let mut fanout = HashMap::new();
    for operator in operators {
        classes.insert(uint(operator, "id")?, text(operator, "class")?);
        for input in array(operator, "inputs")? {
            *fanout.entry(wire_port(input)?.operator).or_insert(0_usize) += 1;
        }
    }
    let mut next = uint(circuit, GENERATED_NAME)?;
    for operator in operators {
        let class = text(operator, "class")?;
        let inputs = array(operator, "inputs")?;
        if is_stream_join(class) {
            let [_, _] = inputs else {
                return Err(Error("stream join requires two inputs".into()));
            };
            for input in inputs {
                let source = wire_port(input)?.operator;
                if classes.get(&source) == Some(&"DBSPStreamJoinIndexOperator")
                    && fanout[&source] == 1
                {
                    return Err(outside_scope());
                }
            }
            // The original candidate closure binds one key and one value per
            // input even if the isolated binary join cannot become a star.
            next = advance_name(next, 3)?;
        } else if class == "DBSPMapIndexOperator" {
            let [input] = inputs else {
                return Err(Error("index map requires one input".into()));
            };
            let source = wire_port(input)?.operator;
            if classes.get(&source) == Some(&"DBSPStreamJoinOperator") && fanout[&source] == 1 {
                return Err(outside_scope());
            }
        }
    }
    let mut result = circuit.clone();
    result[GENERATED_NAME] = next.into();
    Ok(result)
}

fn is_stream_join(class: &str) -> bool {
    matches!(
        class,
        "DBSPStreamJoinOperator" | "DBSPStreamJoinIndexOperator"
    )
}

fn outside_scope() -> Error {
    Error("nested star-join candidate is outside the isolated join slice".into())
}
