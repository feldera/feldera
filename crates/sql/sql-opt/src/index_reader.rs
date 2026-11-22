//! Rebind shared-index consumers to their widened rows.
use feldera_sql_expr::{parameter_paths, Rewrite, Transformation, Visit};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId};
use std::collections::HashMap;

/// One closure parameter, its replacement variable, and the position of each
/// narrow column in the wide row. Parameters are identified by declaration.
pub struct IndexParameter {
    pub declaration: NodeId,
    pub variable: NodeId,
    pub columns: Vec<usize>,
}

/// Rebind a join closure, rolling partition function, or aggregate component.
/// Captured references follow the declaring parameter through nested scopes.
/// Every direct field access gets a fresh variable path and dereference.
pub fn rewrite_index_reader(
    edit: &mut CircuitEditor,
    root: NodeId,
    parameters: &[IndexParameter],
) -> Result<NodeId, Error> {
    let mut declarations = HashMap::new();
    for parameter in parameters {
        if declarations
            .insert(parameter.declaration, parameter)
            .is_some()
        {
            return Err(Error::new("duplicate shared-index parameter"));
        }
        if !matches!(
            edit.node(parameter.declaration)?.inner_kind(),
            Some(InnerKind::Parameter)
        ) || edit.expr_kind(edit.expression(parameter.variable)?)? != Expr::VariablePath
        {
            return Err(Error::new("expected a shared-index parameter and variable"));
        }
    }
    let rewrite = IndexReader {
        declarations,
        paths: HashMap::new(),
    };
    Rewrite::translating_expressions(rewrite).apply(edit, root)
}

struct IndexReader<'a> {
    declarations: HashMap<NodeId, &'a IndexParameter>,
    paths: HashMap<NodeId, &'a IndexParameter>,
}

impl Transformation for IndexReader<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Parameter) => return Ok(Visit::Keep),
            Some(InnerKind::Expression(Expr::Closure)) => {
                let body = node.edge("body")?;
                for decl in node.edges("parameters")? {
                    if let Some(&remap) = self.declarations.get(&decl) {
                        for path in parameter_paths(edit, body, decl)? {
                            self.paths.insert(path, remap);
                        }
                    }
                }
            }
            Some(InnerKind::Expression(Expr::VariablePath)) => {
                if let Some(remap) = self.paths.get(&id) {
                    return Ok(Visit::Replace(edit.derive_inner(remap.variable, [])?));
                }
            }
            Some(InnerKind::Expression(Expr::Field)) => {
                let base = edit.node(node.edge("expression")?)?;
                if matches!(base.inner_kind(), Some(InnerKind::Expression(Expr::Deref))) {
                    if let Some(remap) = self.paths.get(&base.edge("expression")?) {
                        let index = node
                            .data("fieldNo")
                            .and_then(|n| n.as_u64())
                            .and_then(|n| usize::try_from(n).ok())
                            .ok_or_else(|| Error::new("invalid shared-index field"))?;
                        let column = remap.columns.get(index).copied().unwrap_or(index);
                        let copy = edit.derive_inner(remap.variable, [])?;
                        let row = edit.dereference(edit.expression(copy)?)?;
                        return Ok(Visit::Replace(edit.tuple_field(row, column)?.into()));
                    }
                }
            }
            _ => {}
        }
        Ok(Visit::Children)
    }

    fn rebuild_null_test(
        &mut self,
        edit: &mut CircuitEditor,
        node: NodeId,
    ) -> Result<NodeId, Error> {
        edit.rebuild_written_null_test(node)
    }

    fn postorder_rebuilt(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
    ) -> Result<NodeId, Error> {
        if !matches!(
            edit.node(original)?.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) {
            return Ok(rebuilt);
        }
        let mut parameters = edit.node(original)?.edges("parameters")?;
        if !parameters.iter().any(|p| self.declarations.contains_key(p)) {
            return Ok(rebuilt);
        }
        for parameter in &mut parameters {
            if let Some(remap) = self.declarations.get(parameter) {
                let var = edit.node(remap.variable)?;
                let name = var
                    .data("variable")
                    .cloned()
                    .ok_or_else(|| Error::new("unnamed shared-index variable"))?;
                *parameter = edit.inner_at(
                    remap.variable,
                    InnerKind::Parameter,
                    [
                        ("name", Field::Data(name)),
                        ("type", Field::Reference(var.edge("type")?)),
                    ],
                )?;
            }
        }
        let body = edit.expression(edit.node(rebuilt)?.edge("body")?)?;
        let old_body = edit.expression(edit.node(original)?.edge("body")?)?;
        if !edit.same_type(edit.expression_type(body)?, edit.expression_type(old_body)?)? {
            return Err(Error::new("shared-index reader changed its result type"));
        }
        let types = parameters
            .iter()
            .map(|&p| edit.node(p)?.edge("type"))
            .collect::<Result<Vec<_>, _>>()?;
        let signature = edit.closure_type(body, &types)?;
        edit.derive_inner(
            rebuilt,
            [
                (
                    "parameters",
                    Field::Array(parameters.into_iter().map(Field::Reference).collect()),
                ),
                ("type", Field::Reference(signature)),
            ],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn nested_closures_keep_their_own_row_and_capture_wide_columns() -> Result<(), Error> {
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"width":32,"signed":true,"mayBeNull":true,"code":"INT32"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let narrow = edit.tuple_type(roots[0], false, [roots[0]])?;
        let wide = edit.tuple_type(roots[0], false, [roots[0], roots[0]])?;
        let reference = edit.reference_type(narrow)?;
        let wide_ref = edit.reference_type(wide)?;
        let row = edit.named_variable(reference, "row")?;
        let replacement = edit.named_variable(wide_ref, "wide")?;
        let deref = edit.dereference(row)?;
        let read = edit.tuple_field(deref, 0)?;
        let shadow_var = edit.named_variable(reference, "row")?;
        let inner_deref = edit.dereference(shadow_var)?;
        let inner_read = edit.tuple_field(inner_deref, 0)?;
        let shadow = edit.closure_from_variables(inner_read, &[shadow_var])?;
        let capture = edit.closure_from_variables(read, &[])?;
        let null_test = edit.null_test(read)?;
        let position =
            json!({"startLineNumber": 2, "startColumn": 8, "endLineNumber": 2, "endColumn": 14});
        let located = edit.derive_inner(
            null_test.into(),
            [
                ("sourceEmpty", Field::Data(false.into())),
                ("position", Field::Data(position.clone())),
            ],
        )?;
        let body = edit.tuple_expr(
            read.into(),
            true,
            &[capture, shadow, edit.expression(located)?],
        )?;
        let closure = edit.closure_from_variables(body, &[row])?;
        let parameter = edit.node(closure.into())?.edges("parameters")?[0];
        let rewritten = rewrite_index_reader(
            &mut edit,
            closure.into(),
            &[IndexParameter {
                declaration: parameter,
                variable: replacement.into(),
                columns: vec![1],
            }],
        )?;
        let body = edit.node(edit.node(rewritten)?.edge("body")?)?;
        let fields = body.edges("fields")?;
        assert_eq!(fields[1], shadow.into());
        assert_eq!(edit.node(fields[2])?.data("position"), Some(&position));
        assert_ne!(
            edit.node(fields[2])?.edge("type")?,
            edit.node(located)?.edge("type")?
        );
        let read = edit.node(edit.node(fields[0])?.edge("body")?)?;
        assert_eq!(read.data("fieldNo"), Some(&json!(1)));
        let var = edit.node(edit.node(read.edge("expression")?)?.edge("expression")?)?;
        assert_eq!(var.data("variable"), Some(&json!("wide")));
        Ok(())
    }
}
