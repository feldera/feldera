//! Select inexpensive shared maps and filters that may be duplicated.

use super::{array, field, nesting, uint, wire_port, Definitions};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;

const MAX_CHEAP_TERMS: usize = 100;

pub(crate) fn select(circuit: &Value) -> Result<Vec<u64>> {
    let definitions = Definitions::read(circuit)?;
    let mut choices = Vec::new();
    visit_component(array(circuit, "allOperators")?, &definitions, &mut choices)?;
    Ok(choices)
}

fn is_candidate(operator: &Value) -> bool {
    matches!(
        operator["class"].as_str(),
        Some("DBSPMapOperator" | "DBSPMapIndexOperator" | "DBSPFilterOperator")
    )
}

fn visit_component(
    operators: &[Value],
    definitions: &Definitions<'_>,
    choices: &mut Vec<u64>,
) -> Result<()> {
    let mut nodes = HashMap::new();
    let mut fanout = HashMap::<u64, usize>::new();
    for operator in operators {
        nodes.insert(uint(operator, "id")?, operator);
        if operator["class"] != "DBSPDeltaOperator" {
            for input in array(operator, "inputs")? {
                *fanout.entry(wire_port(input)?.operator).or_default() += 1;
            }
        }
    }
    let mut cheap_producers = HashMap::new();
    for operator in operators {
        if operator["class"] == "DBSPNestedOperator" {
            visit_component(array(operator, "allOperators")?, definitions, choices)?;
        }
        if !is_candidate(operator) {
            continue;
        }
        let [input] = array(operator, "inputs")? else {
            return Err(Error("map or filter needs one input".into()));
        };
        let input = wire_port(input)?.operator;
        let producer = nodes.get(&input).ok_or_else(|| {
            Error(format!(
                "consumer references an operator outside its component: {input}"
            ))
        })?;
        if fanout[&input] == 1 || !is_candidate(producer) {
            continue;
        }
        let cheap = match cheap_producers.get(&input) {
            Some(cheap) => *cheap,
            None => {
                let function = field(producer, "function")?;
                let mut budget = MAX_CHEAP_TERMS;
                let cheap = !expensive(definitions, function, &mut budget, 0)?;
                cheap_producers.insert(input, cheap);
                cheap
            }
        };
        if cheap {
            choices.push(uint(operator, "id")?);
        }
    }
    Ok(())
}

fn expensive(
    definitions: &Definitions<'_>,
    node: &Value,
    budget: &mut usize,
    depth: usize,
) -> Result<bool> {
    nesting(depth)?;
    let node = if node
        .as_object()
        .is_some_and(|o| o.len() == 1 && o.contains_key("node"))
    {
        definitions.resolve(node)?
    } else {
        node
    };
    if let Some(class) = node["class"].as_str() {
        if class.starts_with("DBSPType") {
            return Ok(false);
        }
        match class {
            "DBSPApplyExpression" => {
                return Ok(!definitions
                    .call_name(node)?
                    .is_some_and(|name| name.starts_with("tumble_")));
            }
            "DBSPApplyMethodExpression" => return Ok(true),
            "DBSPBinaryExpression" if node["opcode"] == "VARIANT_INDEX" => return Ok(true),
            "DBSPCastExpression" => {
                let source = definitions.resolve(field(node, "source")?)?;
                for expression in [node, source] {
                    if definitions.resolve(field(expression, "type")?)?["class"]
                        == "DBSPTypeVariant"
                    {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
        if *budget == 0 {
            return Ok(true);
        }
        *budget -= 1;
    }
    let children: &mut dyn Iterator<Item = &Value> = match node {
        Value::Array(values) => &mut values.iter(),
        Value::Object(fields) => &mut fields.iter().filter_map(|(name, value)| {
            (!matches!(name.as_str(), "annotations" | "provenance")).then_some(value)
        }),
        _ => return Ok(false),
    };
    for child in children {
        if expensive(definitions, child, budget, depth + 1)? {
            return Ok(true);
        }
    }
    Ok(false)
}
