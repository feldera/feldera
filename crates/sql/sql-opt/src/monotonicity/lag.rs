//! Transfer waterlines through LAG and LEAD in partition order.
use super::{indexed_parameter, unknown};
use feldera_sql_expr::{
    ensure_expression_tree, reduce_closure_body_with_warnings, SimplifyWarning,
};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, NodeId, TypeId};
use std::collections::HashSet;

/// The innermost field comparator is the first SQL sort key. A late change can
/// affect subsequent rows for LAG and preceding rows for LEAD. A NULL change
/// can affect arbitrarily small non-NULL keys if it sorts on the wrong side.
fn preserved_field(
    edit: &CircuitEditor,
    mut comparator: ExpressionId,
    fields: &[NodeId],
    offset: i32,
) -> Result<Option<usize>, Error> {
    let mut first = None;
    let mut visited = HashSet::new();
    while edit.expr_kind(comparator)? == Expr::FieldComparator {
        if !visited.insert(comparator) {
            return Err(Error::new("cyclic LAG sort comparator"));
        }
        first = Some(comparator);
        comparator = edit.expression(edit.node(comparator.into())?.edge("source")?)?;
    }
    let Some(first) = first else { return Ok(None) };
    let key = edit.node(first.into())?;
    let index = key
        .data("fieldNo")
        .and_then(|n| n.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| Error::new("LAG sort key requires a field index"))?;
    let ty = *fields
        .get(index)
        .ok_or_else(|| Error::new("LAG sort key outside input row"))?;
    let flag = |name| {
        key.data(name)
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new(format!("missing {name} on LAG sort key")))
    };
    let lag = offset >= 0;
    let preserves = lag == flag("ascending")?
        && (!edit.nullable(edit.type_id(ty)?)? || lag != flag("nullsFirst")?);
    Ok(preserves.then_some(index))
}

/// Apply the output closure to the current row and an unknown offset row.
/// Only the partition key and the preserved sort column carry input bounds.
pub fn lag_transfer(
    edit: &mut CircuitEditor,
    function: ExpressionId,
    comparator: ExpressionId,
    input: TypeId,
    offset: i32,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let indexed = edit.node(input.into())?;
    let key = edit.type_id(indexed.edge("keyType")?)?;
    let value = edit.type_id(indexed.edge("elementType")?)?;
    let fields = edit.node(value.into())?.edges("tupFields")?;
    let preserved = preserved_field(edit, comparator, &fields, offset)?;
    let row = indexed_parameter(edit, key, value, next)?;

    let [_, delayed] = edit
        .node(function.into())?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("LAG output closure requires current and offset row parameters"))?;
    let delayed = unknown(edit, edit.node(delayed)?.edge("type")?)?;
    let mut columns = Vec::with_capacity(fields.len());
    for (i, &ty) in fields.iter().enumerate() {
        let column = if Some(i) == preserved {
            let value = edit.tuple_field(row, 1)?;
            let value = edit.dereference(value)?;
            edit.tuple_field(value, i)?
        } else {
            unknown(edit, ty)?
        };
        columns.push(column);
    }
    let current = edit.tuple_type(input.into(), false, fields)?;
    let current = edit.with_nullability(current, edit.nullable(value)?)?;
    let current = edit.tuple_with_type(current, &columns)?;
    let current = edit.borrow_value(current)?;
    let data = edit.call_closure(function, &[current, delayed])?;
    let key = edit.tuple_field(row, 0)?;
    let key = edit.dereference(key)?;
    let output = edit.tuple_expr(input.into(), true, &[key, data])?;
    let closure = edit.closure_from_variables(output, &[row])?;
    let reduced =
        reduce_closure_body_with_warnings(edit, closure.into(), &[], &[], Some(warnings))?;
    let tree = ensure_expression_tree(edit, reduced)?;
    edit.expression(tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn only_the_leading_sort_key_can_propagate_its_waterline() -> Result<(), Error> {
        for (offset, ascending, nulls_first, nullable, expected) in [
            (0, true, false, true, Some(1)),
            (1, true, true, false, Some(1)),
            (1, true, true, true, None),
            (1, false, false, false, None),
            (-1, false, true, true, Some(1)),
            (-1, false, false, false, Some(1)),
            (-1, false, false, true, None),
            (-1, true, true, false, None),
        ] {
            let graph = Circuit::from_json(json!({
                "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
            }))?;
            let (graph, roots) = graph.with_inner_roots([
                json!({"class":"DBSPTypeInteger","id":1,"mayBeNull":nullable,"code":"INT32","width":32,"signed":true}),
                json!({"class":"DBSPFieldComparatorExpression","id":3,"fieldNo":0,"ascending":!ascending,
                    "nullsFirst":!nulls_first,"source":{
                        "class":"DBSPFieldComparatorExpression","id":4,"fieldNo":1,"ascending":ascending,
                        "nullsFirst":nulls_first,"source":{"class":"DBSPNoComparatorExpression","id":5}
                    }}),
            ])?;
            let edit = CircuitEditor::new(graph);
            let fields = [roots[0], roots[0]];
            let comparator = edit.expression(roots[1])?;
            assert_eq!(
                preserved_field(&edit, comparator, &fields, offset)?,
                expected
            );
            let no_order = edit.expression(NodeId::Inner(5))?;
            assert_eq!(preserved_field(&edit, no_order, &fields, offset)?, None);
        }
        Ok(())
    }
}
