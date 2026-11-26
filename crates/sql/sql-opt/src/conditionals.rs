//! Replace a repeated Boolean test with its known value inside a guarded branch.
use crate::{
    expression_visitor::{rewrite_expressions, Context},
    lexical_scope::expression_children,
    terms::Terms,
};
use feldera_sql_expr::{ensure_expression_tree, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

fn contains_if(edit: &CircuitEditor, root: NodeId) -> Result<bool, Error> {
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if seen.insert(id) {
            let node = edit.node(id)?;
            if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::If))) {
                return Ok(true);
            }
            pending.extend(expression_children(node));
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

enum Step {
    Visit(NodeId),
    Bind(String),
    RestoreScope(usize),
    EnterBranch(usize, bool),
    LeaveBranch,
}

impl GuardedTests {
    fn visit(&mut self, edit: &CircuitEditor, root: NodeId) -> Result<(), Error> {
        let mut pending = vec![Step::Visit(root)];
        while let Some(step) = pending.pop() {
            let id = match step {
                Step::Visit(id) => id,
                Step::Bind(name) => {
                    self.scope.push(name);
                    continue;
                }
                Step::RestoreScope(size) => {
                    self.scope.truncate(size);
                    continue;
                }
                Step::EnterBranch(guard, value) => {
                    self.guards.push((guard, value));
                    continue;
                }
                Step::LeaveBranch => {
                    self.guards.pop();
                    continue;
                }
            };
            let node = edit.node(id)?;
            self.record_guarded_value(edit, id, node)?;
            // Queue children and scope changes in evaluation order, then reverse
            // this segment so the first child runs next.
            let start = pending.len();
            match node.inner_kind() {
                Some(InnerKind::Expression(Expr::If)) => {
                    let condition = node.edge("condition")?;
                    let shape =
                        self.terms
                            .field(edit.circuit(), &Field::Reference(condition), &self.scope);
                    pending.push(Step::Visit(condition));
                    for (branch, value) in [("positive", true), ("negative", false)] {
                        if let Some(child) = node.field(branch).and_then(Field::node_id) {
                            pending.extend([
                                Step::EnterBranch(shape, value),
                                Step::Visit(child),
                                Step::LeaveBranch,
                            ]);
                        }
                    }
                }
                Some(InnerKind::Expression(Expr::Closure)) => {
                    for param in node.edges("parameters")? {
                        pending.push(Step::Bind(binding(edit.node(param)?, "name")?));
                    }
                    pending.push(Step::Visit(node.edge("body")?));
                    pending.push(Step::RestoreScope(self.scope.len()));
                }
                Some(InnerKind::Expression(Expr::Let)) => {
                    let variable = edit.node(node.edge("variable")?)?;
                    pending.extend([
                        Step::Visit(node.edge("initializer")?),
                        Step::Bind(binding(variable, "variable")?),
                        Step::Visit(node.edge("consumer")?),
                        Step::RestoreScope(self.scope.len()),
                    ]);
                }
                Some(InnerKind::Expression(Expr::Block)) => {
                    for statement in node.edges("contents")? {
                        pending.push(Step::Visit(statement));
                        let statement = edit.node(statement)?;
                        if statement.class() == "DBSPLetStatement" {
                            pending.push(Step::Bind(binding(statement, "variable")?));
                        }
                    }
                    if let Some(last) = node.field("lastExpression").and_then(Field::node_id) {
                        pending.push(Step::Visit(last));
                    }
                    pending.push(Step::RestoreScope(self.scope.len()));
                }
                Some(InnerKind::Expression(Expr::For)) => {
                    pending.extend([
                        Step::Visit(node.edge("iterated")?),
                        Step::Bind(binding(node, "variable")?),
                        Step::Visit(node.edge("block")?),
                        Step::RestoreScope(self.scope.len()),
                    ]);
                }
                _ => pending.extend(expression_children(node).into_iter().map(Step::Visit)),
            }
            pending[start..].reverse();
        }
        Ok(())
    }

    fn record_guarded_value(
        &mut self,
        edit: &CircuitEditor,
        id: NodeId,
        node: &Node,
    ) -> Result<(), Error> {
        if !self.guards.is_empty()
            && matches!(
                node.inner_kind(),
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
    facts.visit(edit, root)?;
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
    fn a_deep_branch_still_simplifies_its_guard() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let boolean = edit.bool_type(site, false)?;
        let parameter = edit.named_variable(boolean, "flag")?;
        let condition = edit.named_variable(boolean, "flag")?;
        let read = edit.named_variable(boolean, "flag")?;
        let negative = edit.named_variable(boolean, "flag")?;
        let mut branch = read;
        for _ in 0..256 {
            branch = edit.make_expr(
                site,
                Expr::Unary,
                boolean,
                [
                    ("opcode", Field::Data("NOT".into())),
                    ("source", Field::Reference(branch.into())),
                ],
            )?;
        }
        let body = edit.make_expr(
            site,
            Expr::If,
            boolean,
            [
                ("condition", Field::Reference(condition.into())),
                ("positive", Field::Reference(branch.into())),
                ("negative", Field::Reference(negative.into())),
            ],
        )?;
        let closure = edit.closure_from_variables(body, &[parameter])?;
        let rewritten = simplify_conditional_root(&mut edit, closure.into())?;
        let body = edit.node(edit.node(rewritten)?.edge("body")?)?;
        assert_eq!(body.edge("condition")?, condition.into());
        assert_eq!(
            edit.node(body.edge("negative")?)?.data("value"),
            Some(&json!(false))
        );
        let mut current = body.edge("positive")?;
        for _ in 0..256 {
            current = edit.node(current)?.edge("source")?;
        }
        assert_eq!(edit.node(current)?.data("value"), Some(&json!(true)));
        Ok(())
    }

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
        tests.visit(&edit, NodeId::Inner(2)).unwrap();
        assert_eq!(
            tests.values,
            HashMap::from([(NodeId::Inner(8), true), (NodeId::Inner(10), false)])
        );
    }
}
