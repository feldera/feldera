//! Replace a repeated Boolean test with its known value inside a guarded branch.
use crate::{
    expression_visitor::{rewrite_expressions, Context},
    terms::Terms,
};
use feldera_sql_expr::{ensure_expression_tree, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

fn children(node: &Node) -> Vec<NodeId> {
    let mut children = Vec::new();
    if !matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
        for (name, field) in node.fields() {
            if !matches!(name, "type" | "originalStruct") {
                field.collect_inner_edges(&mut children);
            }
        }
    }
    children
}

fn contains_if(edit: &CircuitEditor, root: NodeId) -> Result<bool, Error> {
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if seen.insert(id) {
            let node = edit.node(id)?;
            if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::If))) {
                return Ok(true);
            }
            pending.extend(children(node));
        }
    }
    Ok(false)
}

fn binding(node: &Node, field: &str) -> Result<String, Error> {
    node.data(field)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("missing variable name in conditional"))
}

#[derive(Default)]
struct GuardedTests {
    terms: Terms,
    // Each branch adds the condition and the value that selected it.
    guards: Vec<(usize, bool)>,
    scope: Vec<String>,
    values: HashMap<NodeId, bool>,
}

impl GuardedTests {
    fn visit(&mut self, edit: &CircuitEditor, id: NodeId, depth: usize) -> Result<(), Error> {
        if depth > 128 {
            return Err(Error::new(
                "conditional exceeds the expression nesting limit",
            ));
        }
        let node = edit.node(id)?;
        let kind = node.inner_kind();
        let outer = self.scope.len();
        match kind {
            Some(InnerKind::Expression(Expr::If)) => {
                let condition = node.edge("condition")?;
                self.visit(edit, condition, depth + 1)?;
                let shape =
                    self.terms
                        .field(edit.circuit(), &Field::Reference(condition), &self.scope);
                for (branch, value) in [("positive", true), ("negative", false)] {
                    if let Some(child) = node.field(branch).and_then(Field::node_id) {
                        self.guards.push((shape, value));
                        self.visit(edit, child, depth + 1)?;
                        self.guards.pop();
                    }
                }
            }
            Some(InnerKind::Expression(Expr::Closure)) => {
                for param in node.edges("parameters")? {
                    self.scope.push(binding(edit.node(param)?, "name")?);
                }
                self.visit(edit, node.edge("body")?, depth + 1)?;
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                self.visit(edit, node.edge("initializer")?, depth + 1)?;
                self.scope
                    .push(binding(edit.node(node.edge("variable")?)?, "variable")?);
                self.visit(edit, node.edge("consumer")?, depth + 1)?;
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                for statement in node.edges("contents")? {
                    self.visit(edit, statement, depth + 1)?;
                    if edit.node(statement)?.class() == "DBSPLetStatement" {
                        self.scope.push(binding(edit.node(statement)?, "variable")?);
                    }
                }
                if let Some(last) = node.field("lastExpression").and_then(Field::node_id) {
                    self.visit(edit, last, depth + 1)?;
                }
            }
            Some(InnerKind::Expression(Expr::For)) => {
                self.visit(edit, node.edge("iterated")?, depth + 1)?;
                self.scope.push(binding(node, "variable")?);
                self.visit(edit, node.edge("block")?, depth + 1)?;
            }
            _ => {
                for child in children(node) {
                    self.visit(edit, child, depth + 1)?;
                }
            }
        }
        self.scope.truncate(outer);
        if !self.guards.is_empty()
            && matches!(
                kind,
                Some(InnerKind::Expression(
                    Expr::VariablePath
                        | Expr::Apply
                        | Expr::ApplyMethod
                        | Expr::Binary
                        | Expr::Cast
                        | Expr::Deref
                        | Expr::Field
                        | Expr::IsNull
                        | Expr::Path
                        | Expr::Unary
                        | Expr::Unwrap
                ))
            )
        {
            let ty = edit.expression_type(edit.expression(id)?)?;
            if edit.type_kind(ty)? == TypeKind::Bool && !edit.nullable(ty)? {
                let shape = self
                    .terms
                    .field(edit.circuit(), &Field::Reference(id), &self.scope);
                if let Some((_, value)) =
                    self.guards.iter().rev().find(|(guard, _)| *guard == shape)
                {
                    self.values.insert(id, *value);
                }
            }
        }
        Ok(())
    }
}

struct ReplaceKnown(HashMap<NodeId, bool>);

impl Transformation for ReplaceKnown {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if let Some(&value) = self.0.get(&id) {
            let ty = edit.expression_type(edit.expression(id)?)?;
            let literal = edit.make_expr(
                id,
                Expr::BoolLiteral,
                ty,
                [("value", Field::Data(value.into()))],
            )?;
            Ok(Visit::Replace(literal.into()))
        } else if matches!(edit.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
            Ok(Visit::Keep)
        } else {
            Ok(Visit::Children)
        }
    }
}

pub fn simplify_conditional_root(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    if edit.expression(root).is_err() || !contains_if(edit, root)? {
        return Ok(root);
    }
    // A shared expression may be true in one branch and false in another.
    let root = ensure_expression_tree(edit, root)?;
    let mut facts = GuardedTests::default();
    facts.visit(edit, root, 0)?;
    Rewrite::translating_expressions(ReplaceKnown(facts.values)).apply(edit, root)
}

pub fn simplify_conditionals(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    rewrite_expressions(circuit, next, |edit, root, context, _| {
        if matches!(context, Context::Declaration) {
            Ok(root)
        } else {
            simplify_conditional_root(edit, root)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_let_reads_outer_guard_in_initializer_and_shadows_it_in_body() {
        let boolean = json!({"class":"DBSPTypeBool","id":1,"code":"BOOL","mayBeNull":false});
        let path =
            |id| json!({"class":"DBSPVariablePath","id":id,"variable":"x","type":{"node":1}});
        let closure = json!({
            "class":"DBSPClosureExpression","id":2,
            "parameters":[{"class":"DBSPParameter","id":3,"type":boolean,"name":"x"}],
            "body":{"class":"DBSPIfExpression","id":4,"type":{"node":1},"condition":path(5),
                "positive":{"class":"DBSPLetExpression","id":6,"type":{"node":1},
                    "variable":path(7),"initializer":path(8),"consumer":path(9)},
                "negative":path(10)}
        });
        let circuit = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,
            "declarations":[],"allOperators":[{"class":"DBSPMapOperator","id":1,
            "inputs":[],"function":closure}]}))
        .unwrap();
        let edit = CircuitEditor::new(circuit);
        let mut tests = GuardedTests::default();
        tests.visit(&edit, NodeId::Inner(2), 0).unwrap();
        assert_eq!(
            tests.values,
            HashMap::from([(NodeId::Inner(8), true), (NodeId::Inner(10), false)])
        );
    }
}
