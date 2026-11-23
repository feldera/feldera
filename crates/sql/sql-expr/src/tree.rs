//! Copy scalar expression occurrences while retaining their type and binding identities.
use super::rebuild_result_type;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
};
use std::collections::HashSet;

/// Flatten a SQL tuple into owned fields. Each field reads a separate copy of
/// the input expression, as required by the circuit's expression-tree passes.
pub fn flatten_tuple_fields(
    edit: &mut CircuitEditor,
    value: ExpressionId,
) -> Result<Vec<ExpressionId>, Error> {
    let ty = edit.expression_type(value)?;
    let width = edit.node(ty.into())?.edges("tupFields")?.len();
    (0..width)
        .map(|index| {
            let copy = copy_expression(edit, value.into())?;
            let field = edit.tuple_field(edit.expression(copy)?, index)?;
            edit.clone_if_needed(field)
        })
        .collect()
}

/// SQL types may be shared. Each expression in a tree has exactly one occurrence.
pub fn is_expression_tree(edit: &CircuitEditor, root: NodeId) -> Result<bool, Error> {
    Ok(first_repeated_expression(edit, root)?.is_none())
}

/// The path from the body root to the first repeated expression in visitor order.
/// The live compiler uses these identities to print the original enclosing expressions.
pub fn first_repeated_expression(
    edit: &CircuitEditor,
    root: NodeId,
) -> Result<Option<Vec<NodeId>>, Error> {
    let mut pending = vec![(root, 0)];
    let mut path = Vec::new();
    let mut seen = HashSet::new();
    let mut children = Vec::new();
    while let Some((id, depth)) = pending.pop() {
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            continue;
        }
        path.truncate(depth);
        path.push(id);
        if matches!(node.inner_kind(), Some(InnerKind::Expression(_))) && !seen.insert(id) {
            return Ok(Some(path));
        }
        for (key, field) in node.fields() {
            if !matches!(key, "type" | "originalStruct" | "cachedEmptySetResult") {
                field.collect_inner_edges(&mut children);
            }
        }
        pending.extend(children.drain(..).rev().map(|child| (child, depth + 1)));
    }
    Ok(None)
}

/// A filter closure that is already a tree must retain its original identity.
/// In a copied tree, separate uses of a nested closure need distinct parameters.
pub fn ensure_expression_tree(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    if is_expression_tree(edit, root)? {
        Ok(root)
    } else {
        copy(edit, root, Parameters::KeepRoot(root))
    }
}

/// Each use gets a fresh expression. Parameters belong to the closure that
/// binds them and types retain the identities chosen by their constructors.
pub fn copy_expression(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    copy(edit, root, Parameters::KeepAll)
}

/// Substituting an argument into multiple sites gives each copy its own declarations.
pub(super) fn copy_argument(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    copy(edit, root, Parameters::Fresh)
}

#[derive(Clone, Copy)]
enum Parameters {
    KeepAll,
    KeepRoot(NodeId),
    Fresh,
}

/// A shared child is copied separately at each use. Only ancestors are active,
/// so two siblings can refer to the same expression without forming a cycle.
enum CopyStep {
    Visit(NodeId),
    Rebuild {
        id: NodeId,
        kind: Option<Expr>,
        fields: Vec<(String, Field)>,
        first_child: usize,
    },
}

fn copy(edit: &mut CircuitEditor, root: NodeId, parameters: Parameters) -> Result<NodeId, Error> {
    let mut stack = vec![CopyStep::Visit(root)];
    let mut copies = Vec::new();
    let mut active = HashSet::new();
    let mut children = Vec::new();
    while let Some(step) = stack.pop() {
        match step {
            CopyStep::Visit(id) => {
                let node = edit.node(id)?;
                let kind = match node.inner_kind() {
                    Some(InnerKind::Expression(kind)) => Some(*kind),
                    Some(InnerKind::Other(name)) if name.ends_with("Statement") => None,
                    _ => {
                        copies.push(id);
                        continue;
                    }
                };
                if matches!(kind, Some(Expr::NullLiteral | Expr::VariantNullLiteral))
                    || (kind == Some(Expr::Variant)
                        && node.data("isSqlNull").and_then(|v| v.as_bool()) == Some(true))
                {
                    copies.push(id);
                    continue;
                }
                if !active.insert(id) {
                    return Err(Error::new("cycle while copying an expression"));
                }
                let fields = copy_fields(node, kind);
                children.clear();
                for (_, field) in &fields {
                    field.collect_inner_edges(&mut children);
                }
                stack.push(CopyStep::Rebuild {
                    id,
                    kind,
                    fields,
                    first_child: copies.len(),
                });
                stack.extend(children.iter().rev().copied().map(CopyStep::Visit));
            }
            CopyStep::Rebuild {
                id,
                kind,
                mut fields,
                first_child,
            } => {
                let mut children = copies.drain(first_child..);
                for (_, field) in &mut fields {
                    field.map_inner_edges(&mut |_| {
                        children
                            .next()
                            .ok_or_else(|| Error::new("missing copied child"))
                    })?;
                }
                debug_assert!(children.next().is_none());
                drop(children);
                let result = finish_copy(edit, id, kind, fields, parameters)?;
                active.remove(&id);
                copies.push(result);
            }
        }
    }
    copies
        .pop()
        .ok_or_else(|| Error::new("missing copied expression"))
}

fn copy_fields(node: &feldera_sql_ir::Node, kind: Option<Expr>) -> Vec<(String, Field)> {
    node.fields()
        .filter(|(key, _)| match node.class() {
            "DBSPExpressionStatement" => *key == "expression",
            "DBSPLetStatement" if node.field("initializer").and_then(Field::node_id).is_some() => {
                *key == "initializer"
            }
            _ => true,
        })
        .filter(|(key, _)| !matches!(*key, "type" | "originalStruct" | "parameters"))
        .filter(|(key, _)| {
            !matches!(
                (kind, *key),
                (Some(Expr::Return), "argument")
                    | (Some(Expr::For), "variable")
                    | (Some(Expr::Variant), "value")
                    | (Some(Expr::Flatmap | Expr::AsymmetricFieldComparator), _)
            )
        })
        .map(|(key, field)| (key.to_owned(), field.clone()))
        .collect()
}

fn finish_copy(
    edit: &mut CircuitEditor,
    id: NodeId,
    kind: Option<Expr>,
    mut fields: Vec<(String, Field)>,
    parameters: Parameters,
) -> Result<NodeId, Error> {
    let node = edit.node(id)?;
    if node.class() == "DBSPExpressionStatement" {
        let expression = fields
            .first()
            .and_then(|(_, f)| f.node_id())
            .ok_or_else(|| Error::new("expression statement requires a value"))?;
        return edit.inner_at(
            expression,
            InnerKind::Other("DBSPExpressionStatement".into()),
            [("expression", Field::Reference(expression))],
        );
    }
    if node.class() == "DBSPLetStatement"
        && node.field("initializer").and_then(Field::node_id).is_some()
    {
        let initializer = fields
            .first()
            .and_then(|(_, f)| f.node_id())
            .ok_or_else(|| Error::new("missing let initializer"))?;
        let variable = node
            .data("variable")
            .cloned()
            .ok_or_else(|| Error::new("let statement requires a variable"))?;
        let mutable = node
            .data("mutable")
            .cloned()
            .ok_or_else(|| Error::new("let statement requires mutability"))?;
        let ty = edit.node(initializer)?.edge("type")?;
        return edit.inner_at(
            initializer,
            InnerKind::Other("DBSPLetStatement".into()),
            [
                ("variable", Field::Data(variable)),
                ("mutable", Field::Data(mutable)),
                ("type", Field::Reference(ty)),
                ("initializer", Field::Reference(initializer)),
            ],
        );
    }
    if kind == Some(Expr::Closure)
        && match parameters {
            Parameters::KeepAll => false,
            Parameters::KeepRoot(root) => root != id,
            Parameters::Fresh => true,
        }
    {
        let params = edit.node(id)?.edges("parameters")?;
        let mut fresh = Vec::with_capacity(params.len());
        for param in params {
            let parameter = edit.node(param)?;
            let name = parameter
                .field("name")
                .ok_or_else(|| Error::new("closure parameter requires a name"))?
                .clone();
            let ty = parameter.edge("type")?;
            let new = edit.inner_at(
                ty,
                InnerKind::Parameter,
                [("type", Field::Reference(ty)), ("name", name)],
            )?;
            fresh.push(Field::Reference(new));
        }
        fields.push(("parameters".to_owned(), Field::Array(fresh)));
    }
    let raw_tuple_fields = kind == Some(Expr::RawTuple)
        && matches!(edit.node(id)?.field("fields"), Some(Field::Array(_)));
    let changes = fields
        .iter()
        .map(|(name, value)| (name.as_str(), value.clone()));
    let copied = if matches!(
        kind,
        Some(Expr::VariablePath | Expr::Apply | Expr::ApplyMethod | Expr::Block)
    ) || raw_tuple_fields
    {
        edit.derive_unlocated_inner(id, changes)?
    } else {
        edit.derive_inner(id, changes)?
    };
    if matches!(
        kind,
        Some(Expr::DirectComparator | Expr::FieldComparator | Expr::NoComparator)
    ) {
        let ty = edit.derive_inner(edit.node(id)?.edge("type")?, [])?;
        return edit.derive_inner(copied, [("type", Field::Reference(ty))]);
    }
    // deepCopy uses the explicit IsNull constructor. The expression keeps
    // its own location and gets a fresh boolean type even for a literal.
    if kind == Some(Expr::IsNull) {
        if !matches!(parameters, Parameters::KeepAll) {
            // Freshening nested parameters visits the whole copied tree and
            // calls is_null(), which folds tests on constructors and clones.
            let operand = edit.node(copied)?.edge("expression")?;
            let test = edit.null_test(edit.expression(operand)?)?;
            if edit
                .node(test.into())?
                .field("expression")
                .and_then(Field::node_id)
                != Some(operand)
            {
                return Ok(test.into());
            }
        }
        let ty = edit.empty_origin_node(
            id,
            InnerKind::Type(feldera_sql_ir::TypeKind::Bool),
            [
                ("code", Field::Data("BOOL".into())),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        return edit.derive_inner(copied, [("type", Field::Reference(ty))]);
    }
    // A field constructor derives its type but keeps the explicit source of the field access.
    if kind == Some(Expr::Field) {
        let field = edit.node(copied)?;
        let row = edit.expression(field.edge("expression")?)?;
        let index = field
            .data("fieldNo")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| Error::new("invalid field number"))?;
        let ty = edit.tuple_field_type(edit.expression_type(row)?, index as usize)?;
        return edit.derive_inner(copied, [("type", Field::Reference(ty.into()))]);
    }
    if kind == Some(Expr::RawTuple)
        && !matches!(edit.node(copied)?.field("fields"), Some(Field::Array(_)))
    {
        return Ok(copied);
    }
    rebuild_result_type(edit, copied, kind, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;

    #[test]
    fn deep_shared_operands_become_independent_trees() -> Result<(), Error> {
        let circuit = Circuit::from_json(serde_json::json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.bool_type(site, false)?;
        let leaf = edit.named_variable(ty, "value")?;
        let mut value = leaf;
        for _ in 0..512 {
            value = edit.make_expr(
                site,
                Expr::Clone,
                ty,
                [("expression", Field::Reference(value.into()))],
            )?;
        }
        let tuple = edit.tuple_expr(site, false, &[value, value])?;
        let copied = ensure_expression_tree(&mut edit, tuple.into())?;
        let fields = edit.node(copied)?.edges("fields")?;
        let (mut left, mut right, mut original) = (fields[0], fields[1], NodeId::from(value));
        for _ in 0..512 {
            assert_ne!(left, right);
            assert_ne!(left, original);
            assert_ne!(right, original);
            left = edit.node(left)?.edge("expression")?;
            right = edit.node(right)?.edge("expression")?;
            original = edit.node(original)?.edge("expression")?;
        }
        assert_eq!(original, leaf.into());
        assert_ne!(left, right);
        assert_eq!(edit.expression_type(edit.expression(left)?)?, ty);
        assert_eq!(edit.expression_type(edit.expression(right)?)?, ty);
        assert_eq!(ensure_expression_tree(&mut edit, copied)?, copied);
        Ok(())
    }
}
