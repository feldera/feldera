//! Compare projected index keys under the bindings of their two row parameters.
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId};
use std::collections::HashSet;

fn same_type(edit: &CircuitEditor, left: &Node, right: &Node) -> Result<bool, Error> {
    edit.same_type(
        edit.type_id(left.edge("type")?)?,
        edit.type_id(right.edge("type")?)?,
    )
}

fn compare_children(
    left: &Node,
    right: &Node,
    field: &str,
    pending: &mut Vec<(NodeId, NodeId)>,
) -> Result<bool, Error> {
    if left.field(field).is_none() || right.field(field).is_none() {
        return Ok(left.field(field).is_none() && right.field(field).is_none());
    }
    let l = left.edges(field)?;
    let r = right.edges(field)?;
    if l.len() != r.len() {
        return Ok(false);
    }
    pending.extend(l.into_iter().zip(r).rev());
    Ok(true)
}

fn same_path(edit: &CircuitEditor, left: &Node, right: &Node) -> Result<Option<bool>, Error> {
    let l = edit.node(left.edge("path")?)?.edges("components")?;
    let r = edit.node(right.edge("path")?)?.edges("components")?;
    if l.len() != r.len() {
        return Ok(Some(false));
    }
    for (l, r) in l.into_iter().zip(r) {
        let l = edit.node(l)?;
        let r = edit.node(r)?;
        if l.class() != "DBSPSimplePathSegment" || r.class() != "DBSPSimplePathSegment" {
            return Ok(None);
        }
        let lt = l.edges("genericArgs")?;
        let rt = r.edges("genericArgs")?;
        if l.data("identifier") != r.data("identifier") || lt.len() != rt.len() {
            return Ok(Some(false));
        }
        for (a, b) in lt.into_iter().zip(rt) {
            if !edit.same_type(edit.type_id(a)?, edit.type_id(b)?)? {
                return Ok(Some(false));
            }
        }
    }
    Ok(Some(true))
}

/// Some expression classes have additional equivalence rules on the Java side.
/// Return an undecided comparison when one of those rules is needed.
pub(super) fn equivalent(
    edit: &CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<Option<bool>, Error> {
    let left = edit.node(left.into())?;
    let right = edit.node(right.into())?;
    let [lp] = left
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected one index key parameter"))?;
    let [rp] = right
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected one index key parameter"))?;
    let lp = edit.node(lp)?;
    let rp = edit.node(rp)?;
    if !edit.same_type(
        edit.type_id(lp.edge("type")?)?,
        edit.type_id(rp.edge("type")?)?,
    )? {
        return Ok(Some(false));
    }
    let mut pending = vec![(left.edge("body")?, right.edge("body")?)];
    let mut seen = HashSet::new();
    while let Some((a, b)) = pending.pop() {
        if !seen.insert((a, b)) {
            continue;
        }
        let left = edit.node(a)?;
        let right = edit.node(b)?;
        let kind = edit.expr_kind(edit.expression(a)?)?;
        if left.class() != right.class() {
            return Ok(Some(false));
        }
        let children: &[&str] = match kind {
            Expr::VariablePath => {
                if a != b
                    && (left.data("variable") != lp.data("name")
                        || right.data("variable") != rp.data("name"))
                {
                    return Ok(None);
                }
                &[]
            }
            Expr::Tuple | Expr::RawTuple => {
                if !same_type(edit, left, right)?
                    || !compare_children(left, right, "fields", &mut pending)?
                {
                    return Ok(Some(false));
                }
                &[]
            }
            Expr::Path => match same_path(edit, left, right)? {
                Some(true) => &[],
                result => return Ok(result),
            },
            Expr::Apply | Expr::Constructor | Expr::ApplyMethod => {
                if !compare_children(left, right, "arguments", &mut pending)? {
                    return Ok(Some(false));
                }
                if kind == Expr::ApplyMethod {
                    &["self", "function"]
                } else {
                    &["function"]
                }
            }
            kind if kind.wire_name().ends_with("Literal") => {
                if !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                match feldera_sql_expr::same_literal_value(
                    edit,
                    edit.expression(a)?,
                    edit.expression(b)?,
                )? {
                    Some(true) => &[],
                    result => return Ok(result),
                }
            }
            Expr::Field | Expr::CustomOrdField => {
                if left.data("fieldNo") != right.data("fieldNo") {
                    return Ok(Some(false));
                }
                &["expression"]
            }
            Expr::Borrow => {
                if left.data("mut") != right.data("mut") {
                    return Ok(Some(false));
                }
                &["expression"]
            }
            Expr::Cast => {
                if left.data("safe") != right.data("safe") || !same_type(edit, left, right)? {
                    return Ok(Some(false));
                }
                &["source"]
            }
            Expr::TimeAddSub => {
                if ["opcode", "shortUnits", "longUnits"]
                    .iter()
                    .any(|name| left.data(name) != right.data(name))
                {
                    return Ok(Some(false));
                }
                &["left", "right"]
            }
            Expr::UnsignedWrap | Expr::UnsignedUnwrap => {
                if left.data("ascending") != right.data("ascending")
                    || left.data("nullsLast") != right.data("nullsLast")
                {
                    return Ok(Some(false));
                }
                &["source"]
            }
            Expr::Binary | Expr::Unary => {
                if left.data("opcode") != right.data("opcode") {
                    return Ok(Some(false));
                }
                if kind == Expr::Unary {
                    &["source"]
                } else {
                    &["left", "right"]
                }
            }
            Expr::Clone
            | Expr::Deref
            | Expr::IsNull
            | Expr::Some
            | Expr::Unwrap
            | Expr::UnwrapCustomOrd => &["expression"],
            Expr::If => &["condition", "positive", "negative"],
            Expr::Static => {
                if a != b {
                    return Ok(Some(false));
                }
                &[]
            }
            _ => return Ok(None),
        };
        for name in children.iter().rev() {
            pending.push((left.edge(name)?, right.edge(name)?));
        }
    }
    Ok(Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{Circuit, Field};
    use serde_json::json;

    #[test]
    fn ordering_keys_bind_the_row_by_position_and_keep_null_order() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"width":32,"signed":true,"mayBeNull":true,"code":"INT32"
        }),json!({
            "class":"DBSPTypeInteger","id":2,"width":64,"signed":false,"mayBeNull":false,"code":"UINT64"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.type_id(roots[0])?;
        let unsigned = edit.type_id(roots[1])?;
        let mut key = |name, nulls_last| -> Result<_, Error> {
            let row = edit.named_variable(ty, name)?;
            let body = edit.make_expr(
                row.into(),
                Expr::UnsignedWrap,
                unsigned,
                [
                    ("source", Field::Reference(row.into())),
                    ("ascending", Field::Data(true.into())),
                    (
                        "nullsLast",
                        Field::Data(serde_json::Value::Bool(nulls_last)),
                    ),
                ],
            )?;
            edit.closure_from_variables(body, &[row])
        };
        let left = key("l", true)?;
        let right = key("r", true)?;
        let nulls_first = key("r", false)?;
        assert_eq!(equivalent(&edit, left, right)?, Some(true));
        assert_eq!(equivalent(&edit, left, nulls_first)?, Some(false));
        Ok(())
    }
}
