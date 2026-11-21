//! Copy scalar expression occurrences while retaining their type and binding identities.
use super::rebuild_result_type;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId};
use std::collections::HashSet;

/// A filter closure that is already a tree must retain its original identity.
/// In a copied tree, separate uses of a nested closure need distinct parameters.
pub fn ensure_expression_tree(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    let mut pending = vec![root];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            continue;
        }
        if matches!(node.inner_kind(), Some(InnerKind::Expression(_))) && !seen.insert(id) {
            return copy(edit, root, Some(root), 0);
        }
        for (key, field) in node.fields() {
            if !matches!(key, "type" | "originalStruct") {
                field.collect_inner_edges(&mut pending);
            }
        }
    }
    Ok(root)
}

/// Each use gets a fresh expression. Parameters belong to the closure that
/// binds them and types retain the identities chosen by their constructors.
pub fn copy_expression(edit: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
    copy(edit, root, None, 0)
}

fn copy(
    edit: &mut CircuitEditor,
    id: NodeId,
    tree_root: Option<NodeId>,
    depth: usize,
) -> Result<NodeId, Error> {
    if depth > 128 {
        return Err(Error::new("expression exceeds the circuit nesting limit"));
    }
    let node = edit.node(id)?;
    let kind = match node.inner_kind() {
        Some(InnerKind::Expression(kind)) => Some(*kind),
        Some(InnerKind::Other(name)) if name.ends_with("Statement") => None,
        _ => return Ok(id),
    };
    if matches!(kind, Some(Expr::NullLiteral | Expr::VariantNullLiteral))
        || (kind == Some(Expr::Variant)
            && node.data("isSqlNull").and_then(|v| v.as_bool()) == Some(true))
    {
        return Ok(id);
    }
    if node.class() == "DBSPExpressionStatement" {
        let expression = copy(edit, node.edge("expression")?, tree_root, depth + 1)?;
        return edit.inner_at(
            expression,
            InnerKind::Other("DBSPExpressionStatement".into()),
            [("expression", Field::Reference(expression))],
        );
    }
    if node.class() == "DBSPLetStatement" {
        if let Some(initializer) = node.field("initializer").and_then(Field::node_id) {
            let variable = node
                .data("variable")
                .cloned()
                .ok_or_else(|| Error::new("let statement requires a variable"))?;
            let mutable = node
                .data("mutable")
                .cloned()
                .ok_or_else(|| Error::new("let statement requires mutability"))?;
            let initializer = copy(edit, initializer, tree_root, depth + 1)?;
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
    }
    let mut fields: Vec<_> = node
        .fields()
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
        .collect();
    for (_, field) in &mut fields {
        field.map_inner_edges(&mut |child| copy(edit, child, tree_root, depth + 1))?;
    }
    if kind == Some(Expr::Closure) && tree_root.is_some_and(|root| root != id) {
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
        if tree_root.is_some() {
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
