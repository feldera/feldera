//! Assign runtime cast diagnostics by source position within each operator.

use super::{array, encode_with_inner, field, inner_reference, nesting, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(crate) fn wrap(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let target_count = definitions
        .inner
        .values()
        .filter(|node| needs_wrap(node))
        .count();
    if target_count == 0 {
        return Ok(circuit.clone());
    }
    let mut rewritten = HashMap::with_capacity(target_count);
    operator_functions(circuit, &definitions, &mut rewritten)?;
    if rewritten.len() != target_count {
        return Err(Error(
            "a runtime wrapper lies outside the supported operator functions".into(),
        ));
    }
    encode_with_inner(circuit, circuit, &definitions, &rewritten)
}

fn needs_wrap(node: &Value) -> bool {
    node["class"] == "DBSPCastExpression"
        && matches!(
            node["safe"].as_str(),
            Some("UnwrapResultSafe" | "UnwrapResultUnsafe")
        )
        || node["class"] == "DBSPUnwrapExpression"
            && node["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty())
}

fn operator_functions(
    circuit: &Value,
    definitions: &Definitions<'_>,
    rewritten: &mut HashMap<u64, Value>,
) -> Result<()> {
    for operator in array(circuit, "allOperators")? {
        if operator["class"] == "DBSPNestedOperator" {
            operator_functions(operator, definitions, rewritten)?;
        }
        if let Some(function) = operator
            .get("function")
            .filter(|function| !function.is_null())
        {
            let mut visitor = OperatorWrappers {
                definitions,
                rewritten,
                visited: HashSet::new(),
                positions: HashMap::new(),
            };
            visitor.visit(function, 0)?;
        }
    }
    Ok(())
}

struct OperatorWrappers<'a, 'b> {
    definitions: &'b Definitions<'a>,
    rewritten: &'b mut HashMap<u64, Value>,
    visited: HashSet<u64>,
    positions: HashMap<(u64, u64), usize>,
}

impl<'a> OperatorWrappers<'a, '_> {
    fn visit(&mut self, expression: &'a Value, depth: usize) -> Result<()> {
        nesting(depth)?;
        let expression = self.definitions.resolve(expression)?;
        let id = uint(expression, "id")?;
        if !self.visited.insert(id) {
            return Ok(());
        }
        let children: &[&str] = match text(expression, "class")? {
            "DBSPClosureExpression" => &["body"],
            "DBSPTupleExpression" | "DBSPRawTupleExpression" => &["fields"],
            "DBSPBinaryExpression" | "DBSPTimeAddSub" | "DBSPConditionalIncrementExpression" => {
                &["left", "right"]
            }
            "DBSPCastExpression" | "DBSPUnaryExpression" => &["source"],
            "DBSPCloneExpression"
            | "DBSPDerefExpression"
            | "DBSPBorrowExpression"
            | "DBSPFieldExpression"
            | "DBSPSomeExpression"
            | "DBSPUnwrapExpression"
            | "DBSPReturnExpression"
            | "DBSPExpressionStatement" => &["expression"],
            "DBSPIfExpression" => &["condition", "positive", "negative"],
            "DBSPApplyExpression" => &["function", "arguments"],
            "DBSPApplyMethodExpression" => &["self", "arguments"],
            "DBSPBlockExpression" => &["contents", "lastExpression"],
            "DBSPLetStatement" => &["initializer"],
            _ => &[],
        };
        for name in children {
            let Some(child) = expression.get(name).filter(|child| !child.is_null()) else {
                continue;
            };
            if let Some(values) = child.as_array() {
                for value in values {
                    self.visit(value, depth + 1)?;
                }
            } else {
                self.visit(child, depth + 1)?;
            }
        }
        if needs_wrap(expression) {
            if expression["class"] != "DBSPCastExpression" {
                return Err(Error(
                    "fallible expression unwrap is outside this slice".into(),
                ));
            }
            let handler = self.cast_handler(expression)?;
            if self.rewritten.insert(id, handler).is_some() {
                return Err(Error(
                    "a fallible cast is shared by distinct operators".into(),
                ));
            }
        }
        Ok(())
    }

    fn cast_handler(&mut self, cast: &'a Value) -> Result<Value> {
        let source = self.definitions.resolve(field(cast, "source")?)?;
        let source_type = self.definitions.resolve(field(source, "type")?)?;
        let [result] = array(source_type, "typeArgs")? else {
            return Err(Error(
                "runtime cast expects one SqlResult type argument".into(),
            ));
        };
        let ty = field(cast, "type")?;
        if source["class"] != "DBSPCastExpression"
            || source["safe"] != "RustCast"
            || source_type["class"] != "DBSPTypeSqlResult"
            || inner_reference(result)? != inner_reference(ty)?
        {
            return Err(Error(
                "runtime cast must unwrap the matching Rust result".into(),
            ));
        }
        let start = coordinate(cast, "start_line_number", "start_column");
        let end = coordinate(cast, "end_line_number", "end_column");
        let index = if let Some(start) = start {
            let next = self.positions.len();
            *self.positions.entry(start).or_insert(next)
        } else {
            0
        };
        let behavior = if cast["safe"] == "UnwrapResultSafe" {
            "ReturnNone"
        } else if start.is_some() && end.is_some() {
            "PanicWithSource"
        } else {
            "Panic"
        };
        let mut handler = json!({
            "class": "DBSPHandleErrorExpression", "id": uint(cast, "id")?,
            "source": inner_reference(source)?, "type": inner_reference(ty)?,
            "runtimeBehavior": behavior, "index": index, "hasSourcePosition": true,
        });
        for key in ["position", "sourceEmpty"] {
            if let Some(value) = cast.get(key) {
                handler[key] = value.clone();
            }
        }
        Ok(handler)
    }
}

fn coordinate(cast: &Value, line: &str, column: &str) -> Option<(u64, u64)> {
    let position = &cast["position"];
    let line = position[line].as_u64()?;
    let column = position[column].as_u64()?;
    (line > 0 && column > 0).then_some((line, column))
}
