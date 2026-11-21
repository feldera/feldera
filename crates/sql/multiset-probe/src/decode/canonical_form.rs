//! Canonicalize variable bindings in the measured circuit closure subset.

use super::{array, encode_with_inner, field, inner_reference, nesting, text, uint, Definitions};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(crate) fn canonicalize(circuit: &Value) -> Result<Value> {
    let definitions = Definitions::read(circuit)?;
    let mut rewritten = circuit.clone();
    let mut state = Rewriter {
        definitions: &definitions,
        next_id: definitions.inner.keys().copied().max().unwrap_or_default(),
        next_name: 0,
        new_nodes: HashMap::new(),
        scopes: Vec::new(),
    };
    state.operators(circuit, &mut rewritten, 0)?;
    encode_with_inner(circuit, &rewritten, &definitions, &state.new_nodes)
}

struct Rewriter<'a, 'b> {
    definitions: &'b Definitions<'a>,
    next_id: u64,
    next_name: usize,
    new_nodes: HashMap<u64, Value>,
    scopes: Vec<HashMap<String, Binding>>,
}

#[derive(Clone)]
enum Binding {
    Parameter { name: String, ty: Value },
    Local { name: String, variable: Value },
}

impl<'a> Rewriter<'a, '_> {
    fn operators(
        &mut self,
        original: &'a Value,
        rewritten: &mut Value,
        depth: usize,
    ) -> Result<()> {
        nesting(depth)?;
        let operators = rewritten["allOperators"]
            .as_array_mut()
            .ok_or_else(|| Error("missing operator array".into()))?;
        let mut reconstructed = HashSet::new();
        for (before, after) in array(original, "allOperators")?.iter().zip(operators) {
            let class = text(before, "class")?;
            let names: &[&str] = match class {
                "DBSPNestedOperator" => {
                    self.operators(before, after, depth + 1)?;
                    continue;
                }
                "DBSPWaterlineOperator" => &["function", "init", "extractTs"],
                "DBSPChainAggregateOperator" => &["function", "init"],
                "DBSPControlledKeyFilterOperator" | "DBSPControlledFilterOperator" => {
                    &["function", "error"]
                }
                "DBSPMapOperator"
                | "DBSPMapIndexOperator"
                | "DBSPApplyOperator"
                | "DBSPApply2Operator"
                | "DBSPApplyNOperator"
                | "DBSPJoinOperator"
                | "DBSPJoinIndexOperator"
                | "DBSPStreamJoinOperator"
                | "DBSPStreamJoinIndexOperator"
                | "DBSPFilterOperator"
                | "DBSPDelayOperator"
                | "DBSPFlatMapOperator"
                | "DBSPFlatMapIndexOperator"
                | "DBSPIntegrateTraceRetainKeysOperator"
                | "DBSPIntegrateTraceRetainValuesOperator"
                | "DBSPIntegrateTraceRetainNValuesOperator" => &["function"],
                _ if before
                    .get("function")
                    .is_some_and(|function| !function.is_null()) =>
                {
                    let function = self.definitions.resolve(field(before, "function")?)?;
                    if function["class"] != "DBSPZSetExpression" {
                        return Err(Error(format!(
                            "canonical form does not cover {}",
                            text(before, "class")?
                        )));
                    }
                    &[]
                }
                _ => &[],
            };
            let mut changed = false;
            if let Some(inputs) = before.get("inputs").and_then(Value::as_array) {
                for input in inputs {
                    let id = uint(field(input, "operator")?, "node")?;
                    changed |= reconstructed.contains(&id);
                }
            }
            for name in names {
                if let Some(root) = before.get(name).filter(|root| !root.is_null()) {
                    // Java starts a fresh binding scope for each operator expression.
                    self.next_name = 0;
                    let replacement = self.expression(root, 0)?;
                    changed |= replacement != inner_reference(root)?;
                    after[*name] = replacement;
                }
            }
            if changed {
                reconstructed.insert(uint(before, "id")?);
                // CircuitRewriter rebuilds these operators without their annotations.
                if matches!(
                    class,
                    "DBSPApplyOperator" | "DBSPApply2Operator" | "DBSPApplyNOperator"
                ) {
                    after["annotations"] = json!([]);
                }
            }
        }
        Ok(())
    }

    fn expression(&mut self, node: &'a Value, depth: usize) -> Result<Value> {
        nesting(depth)?;
        let node = self.definitions.resolve(node)?;
        let class = text(node, "class")?;
        match class {
            "DBSPClosureExpression" => return self.closure(node, depth + 1),
            "DBSPVariablePath" => return self.variable(node),
            "DBSPBlockExpression" => return self.block(node, depth + 1),
            _ => {}
        }
        let children: &[&str] = match class {
            "DBSPTupleExpression" | "DBSPRawTupleExpression" => &["fields"],
            "DBSPFieldExpression"
            | "DBSPDerefExpression"
            | "DBSPBorrowExpression"
            | "DBSPCloneExpression"
            | "DBSPSomeExpression"
            | "DBSPIsNullExpression"
            | "DBSPUnwrapExpression"
            | "DBSPReturnExpression"
            | "DBSPExpressionStatement" => &["expression"],
            "DBSPUnaryExpression" | "DBSPCastExpression" => &["source"],
            "DBSPBinaryExpression" | "DBSPConditionalIncrementExpression" | "DBSPTimeAddSub" => {
                &["left", "right"]
            }
            "DBSPIfExpression" => &["condition", "positive", "negative"],
            "DBSPApplyExpression" => &["function", "arguments"],
            "DBSPApplyMethodExpression" => &["self", "arguments"],
            "DBSPPathExpression" | "DBSPZSetExpression" => return inner_reference(node),
            _ if class.ends_with("Literal") => return inner_reference(node),
            _ => {
                return Err(Error(format!(
                    "canonical form does not cover expression {class}"
                )))
            }
        };
        let mut changed = false;
        let mut rewritten = node.clone();
        for name in children {
            let Some(child) = node.get(name).filter(|child| !child.is_null()) else {
                continue;
            };
            let replacement = if let Some(items) = child.as_array() {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    let value = self.expression(item, depth + 1)?;
                    changed |= value != inner_reference(item)?;
                    values.push(value);
                }
                Value::Array(values)
            } else {
                let value = self.expression(child, depth + 1)?;
                changed |= value != inner_reference(child)?;
                value
            };
            rewritten[*name] = replacement;
        }
        if !changed {
            return inner_reference(node);
        }
        if class == "DBSPBorrowExpression" {
            let old_type = self.definitions.resolve(field(node, "type")?)?;
            let mut reference = old_type.clone();
            reference["type"] = self.result_type(&rewritten["expression"])?;
            rewritten["type"] = self.insert(reference)?;
        }
        self.insert(rewritten)
    }

    fn closure(&mut self, node: &'a Value, depth: usize) -> Result<Value> {
        self.scopes.push(HashMap::new());
        let mut parameters = Vec::new();
        let mut types = Vec::new();
        let mut changed = false;
        for parameter in array(node, "parameters")? {
            let parameter = self.definitions.resolve(parameter)?;
            let name = self.allocate_name();
            let previous = text(parameter, "name")?;
            let ty = inner_reference(field(parameter, "type")?)?;
            types.push(ty.clone());
            self.bind(
                previous,
                Binding::Parameter {
                    name: name.clone(),
                    ty,
                },
            );
            let replacement = if name == previous {
                inner_reference(parameter)?
            } else {
                changed = true;
                let mut renamed = parameter.clone();
                renamed["name"] = name.into();
                self.insert(renamed)?
            };
            parameters.push(replacement);
        }
        let input = field(node, "body")?;
        let body = self.expression(input, depth)?;
        self.scopes.pop();
        changed |= body != inner_reference(input)?;
        if !changed {
            return inner_reference(node);
        }
        let mut rewritten = node.clone();
        let result = self.result_type(&body)?;
        rewritten["type"] = self.insert(json!({
            "class": "DBSPTypeFunction",
            "sourceEmpty": true,
            "resultType": result,
            "parameterTypes": types,
            "code": "FUNCTION",
            "mayBeNull": false,
        }))?;
        rewritten["parameters"] = parameters.into();
        rewritten["body"] = body;
        self.insert(rewritten)
    }

    fn block(&mut self, node: &'a Value, depth: usize) -> Result<Value> {
        self.scopes.push(HashMap::new());
        let mut statements = Vec::new();
        let mut changed = false;
        for statement in array(node, "contents")? {
            let statement = self.definitions.resolve(statement)?;
            let replacement = if statement["class"] == "DBSPLetStatement" {
                self.let_statement(statement, depth)?
            } else {
                self.expression(statement, depth)?
            };
            changed |= replacement != inner_reference(statement)?;
            statements.push(replacement);
        }
        let mut block = node.clone();
        if let Some(last) = node.get("lastExpression").filter(|last| !last.is_null()) {
            let value = self.expression(last, depth)?;
            changed |= value != inner_reference(last)?;
            block["type"] = self.result_type(&value)?;
            block["lastExpression"] = value;
        }
        self.scopes.pop();
        if !changed {
            return inner_reference(node);
        }
        block["contents"] = statements.into();
        self.insert(block)
    }

    fn let_statement(&mut self, statement: &'a Value, depth: usize) -> Result<Value> {
        let previous = text(statement, "variable")?;
        let initializer = statement
            .get("initializer")
            .filter(|value| !value.is_null());
        let mut replacement = statement.clone();
        let ty = inner_reference(field(statement, "type")?)?;
        let mut changed = false;
        if let Some(initializer) = initializer {
            let value = self.expression(initializer, depth)?;
            changed = value != inner_reference(initializer)?;
            replacement["type"] = self.result_type(&value)?;
            replacement["initializer"] = value;
        }
        let name = self.allocate_name();
        let mut variable = json!({"class":"DBSPVariablePath", "type":ty, "variable":name});
        for origin in ["sourceEmpty", "position"] {
            if let Some(value) = statement.get(origin) {
                variable[origin] = value.clone();
            }
        }
        let variable = self.insert(variable)?;
        self.bind(
            previous,
            Binding::Local {
                name: name.clone(),
                variable,
            },
        );
        if name == previous && !changed {
            return inner_reference(statement);
        }
        replacement["variable"] = name.into();
        self.insert(replacement)
    }

    fn variable(&mut self, node: &Value) -> Result<Value> {
        let name = text(node, "variable")?;
        let binding = self.scopes.iter().rev().find_map(|scope| scope.get(name));
        match binding.cloned() {
            Some(Binding::Local { name: canonical, variable }) if canonical != name => Ok(variable),
            Some(Binding::Parameter { name: canonical, ty }) if canonical != name => self.insert(
                json!({"class":"DBSPVariablePath", "sourceEmpty":true, "type":ty, "variable":canonical}),
            ),
            _ => inner_reference(node),
        }
    }

    fn result_type(&self, reference: &Value) -> Result<Value> {
        let id = uint(reference, "node")?;
        let node = self
            .new_nodes
            .get(&id)
            .or_else(|| self.definitions.inner.get(&id).copied())
            .ok_or_else(|| Error("canonical expression has no definition".into()))?;
        inner_reference(field(node, "type")?)
    }

    fn allocate_name(&mut self) -> String {
        let name = format!("p{}", self.next_name);
        self.next_name += 1;
        name
    }

    fn bind(&mut self, name: &str, binding: Binding) {
        self.scopes
            .last_mut()
            .expect("binding has an enclosing scope")
            .insert(name.into(), binding);
    }

    fn insert(&mut self, mut node: Value) -> Result<Value> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| Error("canonical form exhausted node identities".into()))?;
        node["id"] = self.next_id.into();
        self.new_nodes.insert(self.next_id, node);
        Ok(json!({"node":self.next_id}))
    }
}
