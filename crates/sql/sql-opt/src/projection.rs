//! Recognize tuple projections and track their input field indexes.
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId, TypeKind as Ty};

pub(super) enum Projection {
    Other,
    Nested,
    Fields(Vec<usize>),
}

/// A tuple of simple field accesses records its input columns. Nested tuples,
/// literals and computed field paths may still project without such a mapping.
pub(super) fn projection(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<Projection, Error> {
    let name = edit.node(parameter)?.data("name");
    let mut pending = vec![body];
    let mut fields = Some(Vec::new());
    while let Some(id) = pending.pop() {
        let node = edit.node(id)?;
        let kind = edit.expr_kind(edit.expression(id)?)?;
        match kind {
            Expr::VariablePath if node.data("variable") == name => {}
            Expr::RawTuple | Expr::Tuple => {
                if node.field("fields").is_none() {
                    continue;
                }
                let items = node.edges("fields")?;
                let mut nested = false;
                for &item in &items {
                    let ty = edit.expression_type(edit.expression(item)?)?;
                    nested |= matches!(edit.type_kind(ty)?, Ty::RawTuple | Ty::Tuple);
                }
                if nested {
                    fields = None;
                } else {
                    pending.extend(items.into_iter().rev());
                }
            }
            Expr::Block if node.edges("contents")?.is_empty() => {
                pending.extend(node.field("lastExpression").and_then(Field::node_id));
            }
            Expr::Field
            | Expr::CustomOrdField
            | Expr::UnwrapCustomOrd
            | Expr::Deref
            | Expr::Clone
            | Expr::Unwrap => {
                let source = node.edge("expression")?;
                let source_kind = edit.expr_kind(edit.expression(source)?)?;
                match kind {
                    Expr::Field | Expr::CustomOrdField => {
                        if kind == Expr::CustomOrdField && source_kind != Expr::VariablePath {
                            return Ok(Projection::Other);
                        }
                        if kind == Expr::Field
                            && !matches!(source_kind, Expr::Deref | Expr::UnwrapCustomOrd)
                        {
                            fields = None;
                        }
                        if let Some(indices) = &mut fields {
                            let base = if source_kind == Expr::Deref {
                                edit.node(source)?.edge("expression")?
                            } else {
                                source
                            };
                            if edit.expr_kind(edit.expression(base)?)? != Expr::VariablePath {
                                return Ok(Projection::Other);
                            }
                            let field = node
                                .data("fieldNo")
                                .and_then(|v| v.as_u64())
                                .and_then(|v| usize::try_from(v).ok())
                                .ok_or_else(|| Error::new("invalid projection index"))?;
                            indices.push(field);
                        }
                    }
                    Expr::Deref
                        if !(source_kind == Expr::VariablePath
                            || (source_kind == Expr::Field
                                && edit.expr_kind(
                                    edit.expression(edit.node(source)?.edge("expression")?)?,
                                )? == Expr::VariablePath)) =>
                    {
                        fields = None
                    }
                    Expr::UnwrapCustomOrd if source_kind != Expr::Deref => fields = None,
                    _ => {}
                }
                pending.push(source);
            }
            Expr::Cast => {
                let source = node.edge("source")?;
                if !edit.same_type_ignoring_nullability(
                    edit.expression_type(edit.expression(source)?)?,
                    edit.expression_type(edit.expression(id)?)?,
                )? {
                    return Ok(Projection::Other);
                }
                pending.push(source);
            }
            _ if node.class().ends_with("Literal") => fields = None,
            _ => return Ok(Projection::Other),
        }
    }
    Ok(fields.map_or(Projection::Nested, Projection::Fields))
}
