//! Decide whether composing a projection would duplicate an expensive computation.
use crate::{
    fanout::expensive,
    projection::{projection, strict_projection, Projection},
};
use feldera_sql_expr::{compose_closures, parameter_references, SimplifyWarning};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId};
use std::collections::HashSet;

pub(crate) fn should_inline(
    edit: &CircuitEditor,
    after: NodeId,
    before: NodeId,
) -> Result<bool, Error> {
    if !expensive(edit.circuit(), before)? {
        return Ok(true);
    }
    let op = edit.node(after)?;
    let params = op.edges("parameters")?;
    let [parameter] = params.as_slice() else {
        return Err(Error::new("projection composition requires one parameter"));
    };
    let body = op.edge("body")?;
    let produced = edit.node(before)?.edge("body")?;
    if matches!(
        edit.expr_kind(edit.expression(produced)?)?,
        Expr::Tuple | Expr::RawTuple
    ) {
        match projection(edit, body, *parameter)? {
            Projection::Nested => return Ok(true),
            Projection::Fields(indices) => {
                let row = edit.node(produced)?;
                if row.field("fields").is_some() {
                    let items = row.edges("fields")?;
                    let mut used = HashSet::new();
                    for index in indices {
                        if !used.insert(index)
                            && expensive(
                                edit.circuit(),
                                *items
                                    .get(index)
                                    .ok_or_else(|| Error::new("projection outside source row"))?,
                            )?
                        {
                            return Ok(false);
                        }
                    }
                }
                return Ok(true);
            }
            Projection::Other => {}
        }
    }
    Ok(parameter_references(edit, body, *parameter)? <= 1)
}

/// Inline a projection after its producer. During map fusion, a consumer that
/// would duplicate expensive work keeps its own operator.
pub fn compose_projection(
    edit: &mut CircuitEditor,
    before: NodeId,
    after: NodeId,
    check_cost: bool,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    if check_cost && !should_inline(edit, after, before)? {
        return Ok(None);
    }
    let before = edit.expression(before)?;
    compose_closures(edit, before, false, after, Some(warnings)).map(Some)
}

fn bind(
    edit: &mut CircuitEditor,
    variable: ExpressionId,
    initializer: ExpressionId,
    consumer: ExpressionId,
) -> Result<ExpressionId, Error> {
    edit.make_expr(
        initializer.into(),
        Expr::Let,
        edit.expression_type(consumer)?,
        [
            ("variable", Field::Reference(variable.into())),
            ("initializer", Field::Reference(initializer.into())),
            ("consumer", Field::Reference(consumer.into())),
        ],
    )
}

/// Fuse two indexed maps. Expensive producers must run once per input even
/// when the consumer needs both their key and their value.
pub fn compose_indexed_projection(
    edit: &mut CircuitEditor,
    before: NodeId,
    after: NodeId,
    only_projections: bool,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ExpressionId>, Error> {
    let producer = edit.expression(edit.node(before)?.edge("body")?)?;
    let consumer = edit.expression(edit.node(after)?.edge("body")?)?;
    let [parameter] = edit
        .node(after)?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("indexed map requires one parameter"))?;
    if !expensive(edit.circuit(), before)?
        || (matches!(edit.expr_kind(producer)?, Expr::Tuple | Expr::RawTuple)
            && strict_projection(edit, consumer.into(), parameter)?)
    {
        let before = edit.expression(before)?;
        return compose_closures(edit, before, true, after, Some(warnings)).map(Some);
    }
    if only_projections {
        return Ok(None);
    }
    let pair = edit.variable(edit.expression_type(producer)?, next)?;
    let key = edit.tuple_field(pair, 0)?;
    let value = edit.tuple_field(pair, 1)?;
    let fields = [edit.borrow_value(key)?, edit.borrow_value(value)?];
    let arguments = edit.tuple_expr(pair.into(), true, &fields)?;
    let parameter = edit.node(parameter)?;
    let name = parameter
        .data("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::new("indexed map parameter is missing its name"))?
        .to_owned();
    let parameter_type = edit.type_id(parameter.edge("type")?)?;
    let parameter = edit.named_variable(parameter_type, &name)?;
    let apply = bind(edit, parameter, arguments, consumer)?;
    let body = bind(edit, pair, producer, apply)?;
    let parameters = edit.node(before)?.edges("parameters")?;
    edit.closure_with_parameters(body, &parameters).map(Some)
}
