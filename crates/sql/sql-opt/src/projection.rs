//! Recognize tuple projections and track their input field indexes.
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId, TypeKind as Ty};

pub(super) enum Projection {
    Other,
    Nested,
    /// (closure parameter, input field) for each output field.
    Fields(Vec<(usize, usize)>),
}

pub(super) fn simple_tuple(edit: &CircuitEditor, ty: NodeId) -> Result<bool, Error> {
    let kind = edit.type_kind(edit.type_id(ty)?)?;
    if !matches!(kind, Ty::Tuple | Ty::RawTuple) {
        return Ok(false);
    }
    let fields = edit.node(ty)?.edges("tupFields")?;
    if kind == Ty::RawTuple {
        if fields.len() != 2 {
            return Ok(false);
        }
        for field in fields {
            if edit.type_kind(edit.type_id(field)?)? != Ty::Tuple || !simple_tuple(edit, field)? {
                return Ok(false);
            }
        }
    } else {
        for field in fields {
            if matches!(
                edit.type_kind(edit.type_id(field)?)?,
                Ty::RawTuple | Ty::Tuple
            ) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// A tuple of simple field accesses records its input columns. Nested tuples,
/// literals and computed field paths may still project without such a mapping.
pub(super) fn projection(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<Projection, Error> {
    classify(edit, body, &[parameter], true)
}

pub(super) fn join_projection(
    edit: &CircuitEditor,
    closure: NodeId,
) -> Result<Vec<(usize, usize)>, Error> {
    let node = edit.node(closure)?;
    let params = node.edges("parameters")?;
    let Projection::Fields(fields) = classify(edit, node.edge("body")?, &params, true)? else {
        return Err(Error::new("join requires a projection of its input fields"));
    };
    Ok(fields)
}

/// The strict projection visitor requires a flat result and rejects casts,
/// including casts that only change nullability.
pub(super) fn strict_projection(
    edit: &CircuitEditor,
    body: NodeId,
    parameter: NodeId,
) -> Result<bool, Error> {
    if !simple_tuple(edit, edit.node(body)?.edge("type")?)? {
        return Ok(false);
    }
    Ok(!matches!(
        classify(edit, body, &[parameter], false)?,
        Projection::Other
    ))
}

/// Classification of a live map function after resolving its declarations.
pub enum MapProjection {
    Accepted,
    Rejected,
    /// Render the failed arity or type check against the original result type.
    InvalidFieldMap {
        fields: usize,
    },
}

fn classify_function(
    edit: &CircuitEditor,
    function: NodeId,
    allow_nested_and_casts: bool,
) -> Result<Projection, Error> {
    if edit.expr_kind(edit.expression(function)?)? != Expr::Closure {
        return classify(edit, function, &[], allow_nested_and_casts);
    }
    let node = edit.node(function)?;
    let parameters = node.edges("parameters")?;
    let body = node.edge("body")?;
    let ty = edit.expression_type(edit.expression(body)?)?;
    if parameters.is_empty() || (!allow_nested_and_casts && !simple_tuple(edit, ty.into())?) {
        return Ok(Projection::Other);
    }
    classify(edit, body, &parameters, allow_nested_and_casts)
}

/// Projection.endVisit checks a row's width or the combined widths of an indexed
/// key and value. Scalar results have no field-count constraint.
fn valid_field_count(edit: &CircuitEditor, body: NodeId, count: usize) -> Result<bool, Error> {
    let ty = edit.expression_type(edit.expression(body)?)?;
    match edit.type_kind(ty)? {
        Ty::Tuple => Ok(edit.node(ty.into())?.edges("tupFields")?.len() == count),
        Ty::RawTuple => {
            let members = edit.node(ty.into())?.edges("tupFields")?;
            if members.len() != 2 {
                return Ok(false);
            }
            let mut total = 0;
            for member in members {
                if !matches!(
                    edit.type_kind(edit.type_id(member)?)?,
                    Ty::RawTuple | Ty::Tuple
                ) {
                    return Ok(false);
                }
                total += edit.node(member)?.edges("tupFields")?.len();
            }
            Ok(total == count)
        }
        _ => Ok(true),
    }
}

pub fn map_projection(
    edit: &CircuitEditor,
    function: NodeId,
    allow_nested_and_casts: bool,
) -> Result<MapProjection, Error> {
    match classify_function(edit, function, allow_nested_and_casts)? {
        Projection::Other => Ok(MapProjection::Rejected),
        Projection::Fields(fields)
            if edit.expr_kind(edit.expression(function)?)? == Expr::Closure =>
        {
            let count = fields.len();
            Ok(
                if valid_field_count(edit, edit.node(function)?.edge("body")?, count)? {
                    MapProjection::Accepted
                } else {
                    MapProjection::InvalidFieldMap { fields: count }
                },
            )
        }
        _ => Ok(MapProjection::Accepted),
    }
}

/// Test the shape alone when looking for potentially shareable indexes.
pub fn is_map_projection(
    edit: &CircuitEditor,
    function: NodeId,
    allow_nested_and_casts: bool,
) -> Result<bool, Error> {
    Ok(!matches!(
        classify_function(edit, function, allow_nested_and_casts)?,
        Projection::Other
    ))
}

fn classify(
    edit: &CircuitEditor,
    body: NodeId,
    parameters: &[NodeId],
    allow_noop_casts: bool,
) -> Result<Projection, Error> {
    let parameter_index = |var: &feldera_sql_ir::Node| {
        let name = var.data("variable")?;
        parameters.iter().position(|id| {
            edit.circuit()
                .node(*id)
                .and_then(|parameter| parameter.data("name"))
                == Some(name)
        })
    };
    enum Visit {
        Expression(NodeId),
        RecordField(NodeId),
    }
    let mut pending = vec![Visit::Expression(body)];
    let mut fields = Some(Vec::new());
    while let Some(visit) = pending.pop() {
        let id = match visit {
            Visit::Expression(id) => id,
            Visit::RecordField(id) => {
                // An indexed access such as (*pair.1).2 is a projection, but
                // visiting pair.1 discards the flat field map before we get here.
                if let Some(indices) = &mut fields {
                    let field = edit.node(id)?;
                    let mut base = field.edge("expression")?;
                    if edit.expr_kind(edit.expression(base)?)? == Expr::Deref {
                        base = edit.node(base)?.edge("expression")?;
                    }
                    if edit.expr_kind(edit.expression(base)?)? != Expr::VariablePath {
                        return Ok(Projection::Other);
                    }
                    let Some(input) = parameter_index(edit.node(base)?) else {
                        return Ok(Projection::Other);
                    };
                    let column = field
                        .data("fieldNo")
                        .and_then(|v| v.as_u64())
                        .and_then(|v| usize::try_from(v).ok())
                        .ok_or_else(|| Error::new("invalid projection index"))?;
                    indices.push((input, column));
                }
                continue;
            }
        };
        let node = edit.node(id)?;
        let kind = edit.expr_kind(edit.expression(id)?)?;
        match kind {
            Expr::VariablePath if parameter_index(node).is_some() => {}
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
                    pending.extend(items.into_iter().rev().map(Visit::Expression));
                }
            }
            Expr::Block if node.edges("contents")?.is_empty() => {
                pending.extend(
                    node.field("lastExpression")
                        .and_then(Field::node_id)
                        .map(Visit::Expression),
                );
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
                        pending.push(Visit::RecordField(id));
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
                pending.push(Visit::Expression(source));
            }
            Expr::Cast if allow_noop_casts => {
                let source = node.edge("source")?;
                if !edit.same_type_ignoring_nullability(
                    edit.expression_type(edit.expression(source)?)?,
                    edit.expression_type(edit.expression(id)?)?,
                )? {
                    return Ok(Projection::Other);
                }
                pending.push(Visit::Expression(source));
            }
            _ if node.class().ends_with("Literal") => fields = None,
            _ => return Ok(Projection::Other),
        }
    }
    Ok(fields.map_or(Projection::Nested, Projection::Fields))
}
