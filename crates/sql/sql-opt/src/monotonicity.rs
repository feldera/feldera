//! Transfer almost-monotone columns through identity operators and filter comparisons.
pub mod binary;
pub mod functions;
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Node, NodeId, TypeId,
    TypeKind,
};

/// The transfer function takes a row reference, or a pair of references to an indexed row.
pub fn monotone_identity(
    edit: &mut CircuitEditor,
    row: TypeId,
    indexed: bool,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    if !indexed {
        let borrowed = edit.reference_type(row)?;
        let var = edit.variable(borrowed, next)?;
        let body = edit.dereference(var)?;
        return edit.closure_from_variables(body, &[var]);
    }
    let [key, value] = edit
        .node(row.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("indexed monotonicity requires a key and value"))?;
    let key_ref = edit.reference_type(edit.type_id(key)?)?;
    let value_ref = edit.reference_type(edit.type_id(value)?)?;
    let raw = edit.type_kind(row)? == TypeKind::RawTuple;
    let pair = edit.tuple_type(row.into(), raw, [key_ref.into(), value_ref.into()])?;
    let pair = edit.with_nullability(pair, !raw && edit.nullable(row)?)?;
    let var = edit.variable(pair, next)?;
    let key = edit.tuple_field(var, 0)?;
    let key = edit.dereference(key)?;
    // The two reads are distinct expressions with a shared declaration.
    let second = edit.derive_inner(var.into(), [])?;
    let value = edit.tuple_field(edit.expression(second)?, 1)?;
    let value = edit.dereference(value)?;
    let body = edit.tuple_with_type(row, &[key, value])?;
    edit.closure_from_variables(body, &[var])
}

pub struct FilterBound {
    pub column: usize,
    pub comparison: BinaryOp,
    pub value: ExpressionId,
}

fn opcode(node: &Node) -> Option<&str> {
    node.data("opcode")?.as_str()
}

fn column(edit: &CircuitEditor, id: NodeId, parameter: &str) -> Result<Option<usize>, Error> {
    let field = edit.node(id)?;
    if field.class() != "DBSPFieldExpression" {
        return Ok(None);
    }
    let deref = edit.node(field.edge("expression")?)?;
    if deref.class() != "DBSPDerefExpression" {
        return Ok(None);
    }
    let var = edit.node(deref.edge("expression")?)?;
    if var.class() != "DBSPVariablePath"
        || var.data("variable").and_then(|v| v.as_str()) != Some(parameter)
    {
        return Ok(None);
    }
    field
        .data("fieldNo")
        .and_then(|n| n.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .map(Some)
        .ok_or_else(|| Error::new("filter column requires a field index"))
}

fn offset_bound(
    edit: &mut CircuitEditor,
    parameter: &str,
    larger: NodeId,
    smaller: NodeId,
) -> Result<Option<(usize, ExpressionId)>, Error> {
    let sum = edit.node(larger)?;
    if !matches!(sum.class(), "DBSPBinaryExpression" | "DBSPTimeAddSub") {
        return Ok(None);
    }
    let inverse = match opcode(sum) {
        Some("ADD") => BinaryOp::Sub,
        Some("SUB") => BinaryOp::Add,
        _ => return Ok(None),
    };
    let left = sum.edge("left")?;
    let right = sum.edge("right")?;
    let term = if edit.node(right)?.class().ends_with("Literal") {
        column(edit, left, parameter)?.map(|col| (col, right))
    } else if inverse == BinaryOp::Sub && edit.node(left)?.class().ends_with("Literal") {
        column(edit, right, parameter)?.map(|col| (col, left))
    } else {
        None
    };
    let Some((col, offset)) = term else {
        return Ok(None);
    };
    let ty = edit.expression_type(edit.expression(larger)?)?;
    let bound = edit.binary_expression(
        larger,
        ty,
        inverse,
        edit.expression(smaller)?,
        edit.expression(offset)?,
    )?;
    Ok(Some((col, bound)))
}

fn comparison_bound(
    edit: &mut CircuitEditor,
    parameter: &str,
    sides: &[(NodeId, NodeId)],
) -> Result<Option<(usize, ExpressionId)>, Error> {
    for &(larger, smaller) in sides {
        if let Some(col) = column(edit, larger, parameter)? {
            return Ok(Some((col, edit.expression(smaller)?)));
        }
    }
    for &(larger, smaller) in sides {
        if let Some(bound) = offset_bound(edit, parameter, larger, smaller)? {
            return Ok(Some(bound));
        }
    }
    Ok(None)
}

/// Collect lower bounds in conjunct order. For equality, a direct column on either side
/// takes precedence over moving a literal offset to the other side.
pub fn filter_lower_bounds(
    edit: &mut CircuitEditor,
    closure: NodeId,
) -> Result<Vec<FilterBound>, Error> {
    let function = edit.node(closure)?;
    let [param] = function
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("filter requires one parameter"))?;
    let parameter = edit
        .node(param)?
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("filter parameter requires a name"))?
        .to_owned();
    let mut root = function.edge("body")?;
    while edit.expr_kind(edit.expression(root)?)? == Expr::Unary
        && opcode(edit.node(root)?) == Some("WRAP_BOOL")
    {
        root = edit.node(root)?.edge("source")?;
    }
    let mut pending = vec![root];
    let mut bounds = Vec::new();
    while let Some(id) = pending.pop() {
        let node = edit.node(id)?;
        if node.class() != "DBSPBinaryExpression" {
            continue;
        }
        let left = node.edge("left")?;
        let right = node.edge("right")?;
        let (comparison, larger, smaller) = match opcode(node) {
            Some("AND") => {
                pending.extend([right, left]);
                continue;
            }
            Some("EQ") => (BinaryOp::Eq, right, left),
            Some("LT") => (BinaryOp::Gte, right, left),
            Some("LTE") => (BinaryOp::Gt, right, left),
            Some("GT") => (BinaryOp::Gt, left, right),
            Some("GTE") => (BinaryOp::Gte, left, right),
            _ => continue,
        };
        let both = [(larger, smaller), (smaller, larger)];
        let sides = if comparison == BinaryOp::Eq {
            &both[..]
        } else {
            &both[..1]
        };
        let bound = comparison_bound(edit, &parameter, sides)?;
        if let Some((column, value)) = bound {
            bounds.push(FilterBound {
                column,
                comparison,
                value,
            });
        }
    }
    Ok(bounds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn filter_bounds_preserve_comparison_order_and_offset_direction() -> Result<(), Error> {
        let c = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let (c, roots) = c.with_inner_roots([json!({
            "class":"DBSPI32Literal","id":1,"value":9,"type":{
                "class":"DBSPTypeInteger","id":2,"width":32,"signed":true,"mayBeNull":false,"code":"INT32"
            }
        })])?;
        let mut edit = CircuitEditor::new(c);
        let nine = edit.expression(roots[0])?;
        let integer = edit.expression_type(nine)?;
        let row = edit.tuple_type(nine.into(), false, [integer.into(), integer.into()])?;
        let row_ref = edit.reference_type(row)?;
        let var = edit.named_variable(row_ref, "r")?;
        let read = edit.dereference(var)?;
        let a = edit.tuple_field(read, 0)?;
        let b = edit.tuple_field(read, 1)?;
        let boolean = edit.bool_type(nine.into(), false)?;

        // 9 < 9+a AND a+9 = b AND 9-a >= 9. The last term supplies no lower bound.
        let sum = edit.binary_expression(a.into(), integer, BinaryOp::Add, nine, a)?;
        let lower = edit.binary_expression(a.into(), boolean, BinaryOp::Lt, nine, sum)?;
        let offset = edit.binary_expression(a.into(), integer, BinaryOp::Add, a, nine)?;
        let equal = edit.binary_expression(a.into(), boolean, BinaryOp::Eq, offset, b)?;
        let difference = edit.binary_expression(a.into(), integer, BinaryOp::Sub, nine, a)?;
        let ignored = edit.binary_expression(a.into(), boolean, BinaryOp::Gte, difference, nine)?;
        let first = edit.binary_expression(a.into(), boolean, BinaryOp::And, lower, equal)?;
        let all = edit.binary_expression(a.into(), boolean, BinaryOp::And, first, ignored)?;
        let closure = edit.closure_from_variables(all, &[var])?;
        let bounds = filter_lower_bounds(&mut edit, closure.into())?;
        assert_eq!(bounds.len(), 2);
        assert_eq!((bounds[0].column, bounds[0].comparison), (0, BinaryOp::Gte));
        let adjusted = edit.node(bounds[0].value.into())?;
        assert_eq!(opcode(adjusted), Some("SUB"));
        assert_eq!(adjusted.edge("left")?, nine.into());
        assert_eq!(adjusted.edge("right")?, nine.into());
        assert_eq!((bounds[1].column, bounds[1].comparison), (1, BinaryOp::Eq));
        assert_eq!(bounds[1].value, offset);
        Ok(())
    }
}
