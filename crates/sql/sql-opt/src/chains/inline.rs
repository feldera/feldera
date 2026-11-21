//! Decide whether composing a projection would duplicate an expensive computation.
use crate::{
    fanout::expensive,
    projection::{projection, Projection},
};
use feldera_sql_expr::parameter_references;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, NodeId};
use std::collections::HashSet;

pub(super) fn should_inline(
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
        return Err(Error::new("chain composition requires one parameter"));
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
