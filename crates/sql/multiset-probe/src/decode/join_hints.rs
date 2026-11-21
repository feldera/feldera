//! Compare hints attached to uses of the same collection.

use super::{array, field, text, uint};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Clone, Copy)]
struct Hint<'a> {
    join: u64,
    annotation: usize,
    input: usize,
    strategy: &'a str,
}

impl Hint<'_> {
    fn address(&self) -> Value {
        json!({"join": self.join, "annotation": self.annotation})
    }
}

fn is_checked_join(class: &str) -> bool {
    matches!(
        class,
        "DBSPStreamJoinOperator"
            | "DBSPJoinIndexOperator"
            | "DBSPStreamJoinIndexOperator"
            | "DBSPJoinOperator"
            | "DBSPConcreteAsofJoinOperator"
            | "DBSPAsofJoinOperator"
            | "DBSPLeftJoinOperator"
            | "DBSPLeftJoinFilterMapOperator"
            | "DBSPJoinFilterMapOperator"
            | "DBSPLeftJoinIndexOperator"
    )
}

pub(crate) fn conflicting_join_hints(circuit: &Value) -> Result<Value> {
    feldera_sql_difftest::compare_circuits(circuit, circuit)
        .map_err(|error| Error(error.to_string()))?;
    let mut hints_by_port = HashMap::new();
    let mut conflicts = Vec::new();
    visit_joins(
        array(circuit, "allOperators")?,
        &mut hints_by_port,
        &mut conflicts,
    )?;
    Ok(Value::Array(conflicts))
}

fn visit_joins<'a>(
    operators: &'a [Value],
    hints_by_port: &mut HashMap<(u64, u64), Vec<Hint<'a>>>,
    conflicts: &mut Vec<Value>,
) -> Result<()> {
    for operator in operators {
        let class = text(operator, "class")?;
        if class == "DBSPNestedOperator" {
            visit_joins(array(operator, "allOperators")?, hints_by_port, conflicts)?;
        }
        if !is_checked_join(class) {
            continue;
        }
        let join = uint(operator, "id")?;
        let annotations = array(operator, "annotations")?;
        for (index, port) in array(operator, "inputs")?.iter().enumerate() {
            let key = (
                uint(field(port, "operator")?, "node")?,
                uint(port, "outputNumber")?,
            );
            for (annotation, node) in annotations.iter().enumerate() {
                if node["class"] != "JoinStrategy" || uint(node, "input")? != index as u64 {
                    continue;
                }
                let strategy = text(node, "strategy")?;
                if !matches!(strategy, "Broadcast" | "Shard" | "Balance") {
                    return Err(Error(format!("unknown join strategy {strategy}")));
                }
                let hint = Hint {
                    join,
                    annotation,
                    input: index,
                    strategy,
                };
                let previous = hints_by_port.entry(key).or_default();
                for earlier in previous.iter() {
                    if earlier.strategy != hint.strategy || earlier.input != hint.input {
                        conflicts.push(json!({
                            "current": hint.address(),
                            "previous": earlier.address(),
                        }));
                    }
                }
                previous.push(hint);
            }
        }
    }
    Ok(())
}
