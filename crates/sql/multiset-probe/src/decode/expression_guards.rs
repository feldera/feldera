//! Check bounded expression rewrites at the optimizer's scheduled placements.

use super::{array, field, nesting, text, Definitions};
use crate::{Error, Result};
use serde_json::Value;

pub(super) fn check(circuit: &Value, pass: &str) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    match pass {
        "DecomposeExpensiveFilters" | "FuseExpensiveMaps" => {
            let class = if pass == "DecomposeExpensiveFilters" {
                "DBSPFilterOperator"
            } else {
                "DBSPMapOperator"
            };
            for operator in array(circuit, "allOperators")? {
                if operator["class"] == "DBSPNestedOperator" {
                    return Err(Error(format!("{pass} needs recursive expression analysis")));
                }
                if operator["class"] == class {
                    require_cheap(&definitions, field(operator, "function")?)?;
                }
            }
        }
        "SimplifyConditionals" => {
            let mut check_condition = |node| require_simple_conditional(&definitions, node);
            visit(&definitions, circuit, &mut check_condition)?;
        }
        "SimplifyWaterline" => {
            for operator in array(circuit, "allOperators")? {
                if operator["class"] == "DBSPNestedOperator" {
                    return Err(Error("SimplifyWaterline needs recursive analysis".into()));
                }
                if array(operator, "annotations")?
                    .iter()
                    .any(|ann| ann["class"] == "Waterline")
                {
                    require_waterline_identity(&definitions, field(operator, "function")?)?;
                }
            }
        }
        _ => return Err(Error(format!("unknown expression guard {pass}"))),
    }
    Ok(circuit.clone())
}

fn require_cheap<'a>(definitions: &Definitions<'a>, function: &'a Value) -> Result<()> {
    let mut remaining = 100;
    visit(definitions, function, &mut |node| {
        let class = text(node, "class")?;
        if matches!(class, "DBSPApplyExpression" | "DBSPApplyMethodExpression")
            || node["opcode"] == "VARIANT_INDEX"
        {
            return Err(Error(
                "expensive expression requires an optimizer rewrite".into(),
            ));
        }
        if class == "DBSPCastExpression" {
            let source = definitions.resolve(field(node, "source")?)?;
            for expression in [node, source] {
                if definitions.resolve(field(expression, "type")?)?["class"] == "DBSPTypeVariant" {
                    return Err(Error(
                        "expensive VARIANT cast requires an optimizer rewrite".into(),
                    ));
                }
            }
        }
        remaining -= 1;
        if remaining == 0 {
            return Err(Error(
                "expression exceeds the bounded inexpensive slice".into(),
            ));
        }
        Ok(())
    })
}

fn require_simple_conditional<'a>(definitions: &Definitions<'a>, node: &'a Value) -> Result<()> {
    if node["class"] != "DBSPIfExpression" {
        return Ok(());
    }
    let condition = definitions.resolve(field(node, "condition")?)?;
    if condition["class"] != "DBSPIsNullExpression" {
        return Err(Error(
            "SimplifyConditionals needs analysis of the branch condition".into(),
        ));
    }
    for branch in ["positive", "negative"] {
        if let Some(value) = node.get(branch) {
            visit(definitions, value, &mut |child| {
                if child["class"] == "DBSPIsNullExpression" {
                    return Err(Error(
                        "SimplifyConditionals needs to compare guarded null tests".into(),
                    ));
                }
                Ok(())
            })?;
        }
    }
    Ok(())
}

fn require_waterline_identity<'a>(definitions: &Definitions<'a>, node: &'a Value) -> Result<()> {
    let closure = definitions.resolve(node)?;
    let [parameter] = array(closure, "parameters")? else {
        return Err(Error("expected one waterline parameter".into()));
    };
    let parameter = definitions.resolve(parameter)?;
    let ty = definitions.resolve(field(parameter, "type")?)?;
    let tuple = definitions.resolve(field(ty, "type")?)?;
    let body = definitions.resolve(field(closure, "body")?)?;
    let value = definitions.resolve(field(body, "expression")?)?;
    let variable = definitions.resolve(field(value, "expression")?)?;
    let elements = array(tuple, "tupFields")?;
    if ty["class"] != "DBSPTypeRef"
        || tuple["class"] != "DBSPTypeTuple"
        || elements.len() != 1
        || definitions.resolve(&elements[0])?["class"] != "DBSPTypeTimestamp"
        || body["class"] != "DBSPCloneExpression"
        || value["class"] != "DBSPDerefExpression"
        || variable["class"] != "DBSPVariablePath"
        || variable["variable"] != parameter["name"]
    {
        return Err(Error(
            "SimplifyWaterline requires lowering a nonidentity waterline".into(),
        ));
    }
    Ok(())
}

fn visit<'a>(
    definitions: &Definitions<'a>,
    value: &'a Value,
    visitor: &mut impl FnMut(&'a Value) -> Result<()>,
) -> Result<()> {
    visit_at(definitions, value, 0, &mut 100_000, visitor)
}

fn visit_at<'a>(
    definitions: &Definitions<'a>,
    value: &'a Value,
    depth: usize,
    budget: &mut usize,
    visitor: &mut impl FnMut(&'a Value) -> Result<()>,
) -> Result<()> {
    nesting(depth)?;
    *budget = budget
        .checked_sub(1)
        .ok_or_else(|| Error("optimizer expression traversal budget exceeded".into()))?;
    match value {
        Value::Array(values) => {
            for value in values {
                visit_at(definitions, value, depth + 1, budget, visitor)?;
            }
        }
        Value::Object(fields) => {
            if value.get("node").is_some() && fields.len() == 1 {
                let defined = definitions.resolve(value)?;
                return visit_at(definitions, defined, depth + 1, budget, visitor);
            }
            if let Some(class) = value["class"].as_str() {
                if class.starts_with("DBSPType") {
                    return Ok(());
                }
                visitor(value)?;
            }
            for (name, child) in fields {
                if !matches!(name.as_str(), "type" | "inputs" | "provenance" | "metadata") {
                    visit_at(definitions, child, depth + 1, budget, visitor)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}
