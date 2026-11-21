//! Give closure parameters and local bindings deterministic names within each expression root.
use super::{
    constant_rows, copy_expression, rebuild_aggregate_cache, Rewrite, Transformation, Visit,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId};
use std::collections::HashMap;

enum Binding {
    Parameter(NodeId),
    Local(NodeId),
}

#[derive(Default)]
struct Names {
    next: usize,
    depth: usize,
    aggregate_row: Option<(String, String)>,
    scopes: Vec<HashMap<String, Binding>>,
}

impl Names {
    fn fresh(&mut self) -> String {
        let name = format!("p{}", self.next);
        self.next += 1;
        name
    }

    fn rewrite(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        if self.depth >= 128 {
            return Err(Error::new("canonical expression nesting exceeds 128"));
        }
        self.depth += 1;
        let result = Rewrite::new(&mut *self).apply(edit, id);
        self.depth -= 1;
        result
    }

    fn bind(&mut self, original: String, binding: Binding) {
        self.scopes
            .last_mut()
            .expect("lexical scope")
            .insert(original, binding);
    }

    /// Visit operands in constructor order. Calls allocate argument bindings before
    /// the callee; blocks introduce each statement's locals before the final value.
    fn ordered_children(
        &mut self,
        edit: &mut CircuitEditor,
        id: NodeId,
        fields: &[&str],
    ) -> Result<NodeId, Error> {
        let mut changes = Vec::new();
        for &key in fields {
            if let Some(mut field) = edit.node(id)?.field(key).cloned() {
                if field.map_inner_edges(&mut |child| self.rewrite(edit, child))? {
                    changes.push((key, field));
                }
            }
        }
        if changes.is_empty() {
            return Ok(id);
        }
        if matches!(
            edit.node(id)?.inner_kind(),
            Some(InnerKind::Expression(Expr::Block))
        ) {
            let block = edit.derive_unlocated_inner(id, changes)?;
            return super::rebuild_result_type(edit, block, Some(Expr::Block), false);
        }
        edit.derive_inner(id, changes)
    }

    fn closure(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = edit.node(id)?;
        let body = node.edge("body")?;
        let parameters = node.edges("parameters")?;
        let mut rewritten = Vec::with_capacity(parameters.len());
        self.scopes.push(HashMap::new());
        for &param in &parameters {
            let old_name = name(edit, param, "name")?;
            let canonical = match &self.aggregate_row {
                Some((row, canonical)) if *row == old_name => canonical.clone(),
                _ => self.fresh(),
            };
            let new = if old_name == canonical {
                param
            } else {
                edit.derive_inner(param, [("name", Field::Data(canonical.into()))])?
            };
            self.bind(old_name, Binding::Parameter(new));
            rewritten.push(new);
        }
        let result = self.rewrite(edit, body)?;
        self.scopes.pop();
        if body == result && parameters == rewritten {
            return Ok(id);
        }
        let types = rewritten
            .iter()
            .map(|&p| edit.node(p)?.edge("type"))
            .collect::<Result<Vec<_>, _>>()?;
        let ty = edit.closure_type(edit.expression(result)?, &types)?;
        edit.derive_inner(
            id,
            [
                (
                    "parameters",
                    Field::Array(rewritten.into_iter().map(Field::Reference).collect()),
                ),
                ("body", Field::Reference(result)),
                ("type", Field::Reference(ty)),
            ],
        )
    }

    fn aggregate(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let row = edit.node(id)?.edge("rowVar")?;
        let old_name = name(edit, row, "variable")?;
        let canonical = self.fresh();
        let path = edit.derive_inner(row, [("variable", Field::Data(canonical.clone().into()))])?;
        self.aggregate_row = Some((old_name, canonical));
        let mut parts = Vec::new();
        for part in edit.node(id)?.edges("aggregates")? {
            // Each aggregate owns its bindings, including uses of the shared weight variable.
            let kind = match edit.expr_kind(edit.expression(part)?)? {
                Expr::LinearAggregate => Expr::LinearAggregate,
                Expr::MinMaxAggregate | Expr::NonLinearAggregate => Expr::NonLinearAggregate,
                other => {
                    return Err(Error::new(format!(
                        "unexpected aggregate implementation {other:?}"
                    )))
                }
            };
            let fields: &[&'static str] = if kind == Expr::LinearAggregate {
                &["map", "postProcess", "emptySetResult"]
            } else {
                // MinMaxAggregate inherits NonLinearAggregate.deepCopy().
                &[
                    "semigroup",
                    "zero",
                    "increment",
                    "postProcess",
                    "emptySetResult",
                ]
            };
            let mut copied = Vec::new();
            for &field in fields {
                if let Some(child) = edit.node(part)?.field(field).and_then(Field::node_id) {
                    let transformed = if field == "semigroup" {
                        child
                    } else {
                        let copy = copy_expression(edit, child)?;
                        self.rewrite(edit, copy)?
                    };
                    copied.push((field, Field::Reference(transformed)));
                    if field == "emptySetResult" {
                        copied.push((
                            "type",
                            Field::Reference(edit.node(transformed)?.edge("type")?),
                        ));
                    }
                }
            }
            let part = edit.inner_at(part, InnerKind::Expression(kind), copied)?;
            parts.push(Field::Reference(part));
        }
        self.aggregate_row = None;
        let list = edit.derive_inner(
            id,
            [
                ("rowVar", Field::Reference(path)),
                ("aggregates", Field::Array(parts)),
            ],
        )?;
        rebuild_aggregate_cache(edit, list)
    }

    fn path(&self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let variable = name(edit, id, "variable")?;
        match self
            .scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(&variable))
        {
            Some(Binding::Local(path)) => Ok(*path),
            Some(Binding::Parameter(param)) => {
                let canonical = name(edit, *param, "name")?;
                let ty = edit.node(*param)?.edge("type")?;
                if canonical == variable && edit.node(id)?.edge("type")? == ty {
                    return Ok(id);
                }
                Ok(edit.named_variable(edit.type_id(ty)?, &canonical)?.into())
            }
            None => Ok(id),
        }
    }

    fn local(
        &mut self,
        edit: &mut CircuitEditor,
        id: NodeId,
        is_statement: bool,
    ) -> Result<NodeId, Error> {
        let node = edit.node(id)?;
        let initializer = node.field("initializer").and_then(Field::node_id);
        let declaration = if is_statement {
            id
        } else {
            node.edge("variable")?
        };
        let variable = name(edit, declaration, "variable")?;
        let ty = edit.type_id(edit.node(declaration)?.edge("type")?)?;
        let initializer = initializer.map(|i| self.rewrite(edit, i)).transpose()?;
        let canonical = self.fresh();
        let path = edit.make_expr(
            id,
            Expr::VariablePath,
            ty,
            [("variable", Field::Data(canonical.clone().into()))],
        )?;
        self.bind(variable, Binding::Local(path.into()));
        if is_statement {
            let mut fields = vec![("variable", Field::Data(canonical.into()))];
            if let Some(init) = initializer {
                fields.push(("initializer", Field::Reference(init)));
                fields.push(("type", Field::Reference(edit.node(init)?.edge("type")?)));
            }
            return edit.derive_inner(id, fields);
        }
        let consumer = self.rewrite(edit, edit.node(id)?.edge("consumer")?)?;
        let initializer =
            initializer.ok_or_else(|| Error::new("let expression requires an initializer"))?;
        edit.inner_at(
            initializer,
            InnerKind::Expression(Expr::Let),
            [
                ("variable", Field::Reference(path.into())),
                ("initializer", Field::Reference(initializer)),
                ("consumer", Field::Reference(consumer)),
                ("type", Field::Reference(edit.node(consumer)?.edge("type")?)),
            ],
        )
    }
}

fn name(edit: &CircuitEditor, id: NodeId, field: &str) -> Result<String, Error> {
    edit.node(id)?
        .data(field)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new(format!("missing {field} in canonical binding")))
}

impl Transformation for &mut Names {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        Ok(match node.inner_kind() {
            Some(InnerKind::Type(_)) => Visit::Keep,
            Some(InnerKind::AggregateList) => Visit::Replace(self.aggregate(edit, id)?),
            Some(InnerKind::Expression(Expr::Closure)) => Visit::Replace(self.closure(edit, id)?),
            Some(InnerKind::Expression(Expr::VariablePath)) => Visit::Replace(self.path(edit, id)?),
            Some(InnerKind::Expression(Expr::Apply | Expr::ApplyMethod)) => Visit::Replace(
                self.ordered_children(edit, id, &["arguments", "function", "self"])?,
            ),
            Some(InnerKind::Expression(Expr::Block)) => {
                self.scopes.push(HashMap::new());
                let block = self.ordered_children(edit, id, &["contents", "lastExpression"])?;
                self.scopes.pop();
                Visit::Replace(block)
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                self.scopes.push(HashMap::new());
                let result = self.local(edit, id, false)?;
                self.scopes.pop();
                Visit::Replace(result)
            }
            _ if node.class() == "DBSPLetStatement" => Visit::Replace(self.local(edit, id, true)?),
            _ => Visit::Children,
        })
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        let kind = edit.node(original)?.inner_kind();
        if original != rebuilt && matches!(kind, Some(InnerKind::Expression(Expr::RawTuple))) {
            return edit.derive_unlocated_inner(rebuilt, []);
        }
        if original != rebuilt && matches!(kind, Some(InnerKind::Expression(Expr::Lazy))) {
            let node = edit.node(rebuilt)?;
            let child = node.edge("expression")?;
            return edit.inner_at(
                child,
                InnerKind::Expression(Expr::Lazy),
                [
                    ("expression", Field::Reference(child)),
                    ("type", Field::Reference(node.edge("type")?)),
                ],
            );
        }
        if matches!(kind, Some(InnerKind::Expression(Expr::ZSet))) {
            let rows = constant_rows::entries(edit, edit.expression(rebuilt)?)?;
            let mut consolidated = Vec::with_capacity(rows.len());
            for &entry in &rows {
                constant_rows::append(edit, &mut consolidated, entry)?;
            }
            if rows != consolidated || original != rebuilt {
                let ty = edit.type_id(edit.node(original)?.edge("elementType")?)?;
                return Ok(constant_rows::multiset(edit, ty, consolidated)?.into());
            }
        }
        Ok(rebuilt)
    }
}

pub fn canonicalize_names(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    let mut names = Names::default();
    names.scopes.push(HashMap::new());
    names.rewrite(edit, root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, TypeKind};
    use serde_json::json;

    #[test]
    fn closure_arguments_receive_names_before_the_callee() {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))
        .unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let integer = edit
            .inner(
                InnerKind::Type(TypeKind::Integer),
                [
                    ("code", Field::Data("INT32".into())),
                    ("width", Field::Data(32.into())),
                    ("signed", Field::Data(true.into())),
                    ("mayBeNull", Field::Data(false.into())),
                ],
            )
            .unwrap();
        let value = edit
            .named_variable(edit.type_id(integer).unwrap(), "value")
            .unwrap();
        let argument = edit.closure_from_variables(value, &[value]).unwrap();
        let function_type = edit.expression_type(argument).unwrap();
        let function = edit.named_variable(function_type, "function").unwrap();
        let callee = edit.closure_from_variables(function, &[function]).unwrap();
        let call = edit
            .make_expr(
                callee.into(),
                Expr::Apply,
                function_type,
                [
                    ("function", Field::Reference(callee.into())),
                    (
                        "arguments",
                        Field::Array(vec![Field::Reference(argument.into())]),
                    ),
                ],
            )
            .unwrap();

        let result = canonicalize_names(&mut edit, call.into()).unwrap();
        let result = edit.node(result).unwrap();
        let argument = result.edges("arguments").unwrap()[0];
        let callee = result.edge("function").unwrap();
        for (closure, expected) in [(argument, "p0"), (callee, "p1")] {
            let closure = edit.node(closure).unwrap();
            let parameter = closure.edges("parameters").unwrap()[0];
            assert_eq!(name(&edit, parameter, "name").unwrap(), expected);
            assert_eq!(
                name(&edit, closure.edge("body").unwrap(), "variable").unwrap(),
                expected
            );
        }
    }
}
