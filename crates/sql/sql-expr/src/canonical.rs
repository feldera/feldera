//! Give closure parameters and local bindings deterministic names within each expression root.
use super::{
    constant_rows, copy_expression, rebuild_aggregate_cache, rebuild_result_type, Rewrite,
    Transformation, Visit,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId};
use std::collections::{HashMap, HashSet};

enum Binding {
    Parameter(NodeId),
    Local(NodeId),
}

#[derive(Default)]
struct Names {
    next: usize,
    aggregate_row: Option<(String, String)>,
    scopes: Vec<HashMap<String, Binding>>,
    changed_static_initializer: bool,
}

impl Names {
    fn fresh(&mut self) -> String {
        let name = format!("p{}", self.next);
        self.next += 1;
        name
    }

    fn bind(&mut self, original: String, binding: Binding) {
        self.scopes
            .last_mut()
            .expect("lexical scope")
            .insert(original, binding);
    }

    fn parameter(&mut self, edit: &mut CircuitEditor, param: NodeId) -> Result<NodeId, Error> {
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
        Ok(new)
    }

    fn aggregate(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        if self.aggregate_row.is_some() {
            return Err(Error::new("aggregate lists do not nest"));
        }
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
                        Rewrite::new(&mut *self).apply(edit, copy)?
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

    fn declare_local(
        &mut self,
        edit: &mut CircuitEditor,
        site: NodeId,
        declaration: NodeId,
    ) -> Result<NodeId, Error> {
        let variable = name(edit, declaration, "variable")?;
        let ty = edit.type_id(edit.node(declaration)?.edge("type")?)?;
        let canonical = self.fresh();
        let path = edit.make_expr(
            site,
            Expr::VariablePath,
            ty,
            [("variable", Field::Data(canonical.into()))],
        )?;
        self.bind(variable, Binding::Local(path.into()));
        Ok(path.into())
    }
}

fn name(edit: &CircuitEditor, id: NodeId, field: &str) -> Result<String, Error> {
    edit.node(id)?
        .data(field)
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::new(format!("missing {field} in canonical binding")))
}

/// CanonicalForm rebuilds these expressions even when their children keep their identity.
pub fn canonical_constructor_simplifies(
    edit: &CircuitEditor,
    id: NodeId,
    kind: Expr,
) -> Result<bool, Error> {
    let operand = |field| edit.expression(edit.node(id)?.edge(field)?);
    match kind {
        Expr::Field => edit.field_read_simplifies(operand("expression")?),
        Expr::IsNull => {
            let value = operand("expression")?;
            Ok(edit.expr_kind(value)? == Expr::Clone || edit.known_nullness(value)?.is_some())
        }
        Expr::Cast => edit.same_type(
            edit.expression_type(operand("source")?)?,
            edit.type_id(edit.node(id)?.edge("type")?)?,
        ),
        _ => Ok(false),
    }
}

impl Transformation for &mut Names {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        Ok(match node.inner_kind() {
            Some(InnerKind::Type(_)) => Visit::Keep,
            Some(InnerKind::AggregateList) => Visit::Replace(self.aggregate(edit, id)?),
            Some(InnerKind::Expression(Expr::Closure)) => {
                self.scopes.push(HashMap::new());
                Visit::IndependentFields(&["parameters", "body"])
            }
            Some(InnerKind::Expression(Expr::VariablePath)) => Visit::Replace(self.path(edit, id)?),
            Some(InnerKind::Expression(Expr::Apply | Expr::ApplyMethod)) => {
                Visit::IndependentFields(&["arguments", "function", "self"])
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                self.scopes.push(HashMap::new());
                Visit::IndependentFields(&["contents", "lastExpression"])
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                self.scopes.push(HashMap::new());
                Visit::IndependentFields(&["initializer", "variable", "consumer"])
            }
            _ if node.class() == "DBSPLetStatement" => Visit::IndependentFields(&["initializer"]),
            _ => Visit::Children,
        })
    }

    fn independent_child(
        &mut self,
        edit: &mut CircuitEditor,
        parent: NodeId,
        field: &str,
        child: NodeId,
    ) -> Result<Option<NodeId>, Error> {
        match field {
            "parameters" => self.parameter(edit, child).map(Some),
            "variable" => self.declare_local(edit, parent, child).map(Some),
            _ => Ok(None),
        }
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        let node = edit.node(original)?;
        if node.class() == "DBSPLetStatement" {
            // The initializer sees the previous binding. Later statements see this one.
            let path = self.declare_local(edit, original, original)?;
            let mut fields = vec![(
                "variable",
                Field::Data(name(edit, path, "variable")?.into()),
            )];
            if let Some(init) = edit
                .node(rebuilt)?
                .field("initializer")
                .and_then(Field::node_id)
            {
                fields.push(("type", Field::Reference(edit.node(init)?.edge("type")?)));
            }
            return edit.derive_inner(rebuilt, fields);
        }
        let kind = node.inner_kind();
        match kind {
            Some(InnerKind::Expression(Expr::Static)) => {
                // The name hashes the original initializer. The reference reports
                // an error if canonicalization would change it.
                self.changed_static_initializer |= original != rebuilt;
            }
            Some(InnerKind::Expression(kind @ (Expr::Field | Expr::IsNull | Expr::Cast)))
                if original == rebuilt || *kind == Expr::Cast =>
            {
                if canonical_constructor_simplifies(edit, rebuilt, *kind)? {
                    if *kind == Expr::Cast {
                        return edit.node(rebuilt)?.edge("source");
                    }
                    return rebuild_result_type(edit, rebuilt, Some(*kind), false);
                }
            }
            Some(InnerKind::Expression(Expr::Closure)) => {
                self.scopes.pop();
                if original != rebuilt {
                    return rebuild_result_type(edit, rebuilt, Some(Expr::Closure), false);
                }
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                self.scopes.pop();
                let node = edit.node(rebuilt)?;
                return edit
                    .let_expression(
                        edit.expression(node.edge("variable")?)?,
                        edit.expression(node.edge("initializer")?)?,
                        edit.expression(node.edge("consumer")?)?,
                    )
                    .map(Into::into);
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                self.scopes.pop();
            }
            _ => {}
        }
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
    Rewrite::new(&mut names).apply(edit, root)
}

/// Canonicalize a closed projection for comparison by its printed value. Reuse
/// rewritten operands even across call arguments when every read has one binding.
/// A declaration reached twice would allocate a fresh name on its second visit.
pub fn canonicalize_projection(
    edit: &mut CircuitEditor,
    closure: NodeId,
) -> Result<Option<NodeId>, Error> {
    use crate::lexical_scope::{lexical_references, push_expression_children, Resolution};

    let mut seen = HashSet::new();
    let mut repeated = HashSet::new();
    let mut pending = vec![closure];
    while let Some(id) = pending.pop() {
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            continue;
        }
        if !seen.insert(id) {
            if !repeated.insert(id) {
                continue;
            }
            if matches!(
                node.inner_kind(),
                Some(
                    InnerKind::Parameter
                        | InnerKind::Expression(Expr::Let)
                        | InnerKind::AggregateList
                )
            ) || node.class() == "DBSPLetStatement"
            {
                return Ok(None);
            }
        }
        // Propagate a second visit to descendants, including declarations inside
        // a shared compound expression. Each input edge is examined at most twice.
        push_expression_children(node, &mut pending);
    }
    if !matches!(
        lexical_references(edit.circuit(), closure)?,
        Resolution::Bound(_)
    ) {
        return Ok(None);
    }
    let mut names = Names::default();
    names.scopes.push(HashMap::new());
    let result = Rewrite::bound_projection(&mut names).apply(edit, closure)?;
    Ok((!names.changed_static_initializer).then_some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, TypeKind};
    use serde_json::json;

    #[test]
    fn a_projection_preserves_the_initializer_of_a_named_static() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.bool_type(site, false)?;
        let row = edit.named_variable(ty, "row")?;
        let literal = edit.make_expr(
            site,
            Expr::BoolLiteral,
            ty,
            [("value", Field::Data(true.into()))],
        )?;
        for (initializer, allowed) in [(row, false), (literal, true)] {
            let value = edit.make_expr(
                site,
                Expr::Static,
                ty,
                [
                    ("initializer", Field::Reference(initializer.into())),
                    ("name", Field::Data("named_static".into())),
                ],
            )?;
            let closure = edit.closure_from_variables(value, &[row])?;
            assert_eq!(
                canonicalize_projection(&mut edit, closure.into())?.is_some(),
                allowed
            );
        }
        Ok(())
    }

    #[test]
    fn deep_calls_in_blocks_keep_the_outer_parameter() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.bool_type(site, false)?;
        let input = edit.named_variable(ty, "input")?;
        let function = edit.function_path(site, "f")?;
        let mut body = input;
        for _ in 0..180 {
            let call = edit.make_expr(
                site,
                Expr::Apply,
                ty,
                [
                    ("function", Field::Reference(function.into())),
                    (
                        "arguments",
                        Field::Array(vec![Field::Reference(body.into())]),
                    ),
                ],
            )?;
            body = edit.make_expr(
                site,
                Expr::Block,
                ty,
                [
                    ("contents", Field::Array(vec![])),
                    ("lastExpression", Field::Reference(call.into())),
                ],
            )?;
        }
        let closure = edit.closure_from_variables(body, &[input])?;
        let canonical = canonicalize_names(&mut edit, closure.into())?;
        let mut current = edit.node(canonical)?.edge("body")?;
        for _ in 0..180 {
            let call = edit.node(current)?.edge("lastExpression")?;
            assert_eq!(edit.node(call)?.edge("function")?, function.into());
            current = edit.node(call)?.edges("arguments")?[0];
        }
        assert_eq!(name(&edit, current, "variable")?, "p0");
        assert_eq!(name(&edit, input.into(), "variable")?, "input");
        Ok(())
    }

    #[test]
    fn nested_closures_and_let_initializers_bind_in_evaluation_order() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.bool_type(site, false)?;
        let value = edit.named_variable(ty, "value")?;
        let mut body = value;
        const DEPTH: usize = 160;
        for _ in 0..DEPTH {
            let lambda = edit.closure_from_variables(body, &[value])?;
            let call = edit.make_expr(
                site,
                Expr::Apply,
                ty,
                [
                    ("function", Field::Reference(lambda.into())),
                    (
                        "arguments",
                        Field::Array(vec![Field::Reference(value.into())]),
                    ),
                ],
            )?;
            body = edit.let_expression(value, call, value)?;
        }
        let root = edit.closure_from_variables(body, &[value])?;
        let canonical = canonicalize_names(&mut edit, root.into())?;
        let mut current = edit.node(canonical)?.edge("body")?;
        for level in 0..DEPTH {
            let binding = edit.node(current)?;
            assert_eq!(binding.edge("consumer")?, binding.edge("variable")?);
            assert_eq!(
                name(&edit, binding.edge("variable")?, "variable")?,
                format!("p{}", 2 * DEPTH - level)
            );
            let call = edit.node(binding.edge("initializer")?)?;
            assert_eq!(
                name(&edit, call.edges("arguments")?[0], "variable")?,
                format!("p{level}")
            );
            let lambda = edit.node(call.edge("function")?)?;
            assert_eq!(
                name(&edit, lambda.edges("parameters")?[0], "name")?,
                format!("p{}", level + 1)
            );
            current = lambda.edge("body")?;
        }
        assert_eq!(name(&edit, current, "variable")?, format!("p{DEPTH}"));
        Ok(())
    }

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
