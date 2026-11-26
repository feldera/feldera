//! Group output columns whose expressions compute the same value in a closure.
use super::{text, Column, OutputShape};
use crate::index_key_equivalence::{compare_keys, speculative, KeyComparisonError};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId};

fn tuple_fields(
    edit: &CircuitEditor,
    mut expression: NodeId,
    width: usize,
) -> Result<Vec<NodeId>, Error> {
    if edit.expr_kind(edit.expression(expression)?)? == Expr::Some {
        expression = edit.node(expression)?.edge("expression")?;
    }
    let node = edit.node(expression)?;
    if matches!(
        edit.expr_kind(edit.expression(expression)?)?,
        Expr::Tuple | Expr::RawTuple
    ) && matches!(node.field("fields"), Some(Field::Array(fields)) if fields.len() == width)
    {
        return node.edges("fields");
    }
    Ok(Vec::new())
}

/// Indexed closures expose the key and value as separate tuples. Either half
/// can be opaque. Preserve the original column offsets when one is missing.
pub fn equal_computations(
    edit: &CircuitEditor,
    closure: NodeId,
    shape: &OutputShape,
) -> Result<Option<Vec<Vec<Column>>>, Error> {
    speculative(checked_equal_computations(edit, closure, shape))
}

pub fn checked_equal_computations(
    edit: &CircuitEditor,
    closure: NodeId,
    shape: &OutputShape,
) -> Result<Option<Vec<Vec<Column>>>, KeyComparisonError> {
    let closure = edit.node(closure)?;
    let body = closure.edge("body")?;
    let mut fields = Vec::new();
    let mut append = |tuple, offset, width| -> Result<(), Error> {
        for (i, expression) in tuple_fields(edit, tuple, width)?.into_iter().enumerate() {
            fields.push((expression, shape.column(offset + i)));
        }
        Ok(())
    };
    if let Some(index) = shape.index_fields {
        let values = shape
            .width
            .checked_sub(index)
            .ok_or_else(|| Error::new("index exceeds the collection width"))?;
        if edit.expr_kind(edit.expression(body)?)? == Expr::RawTuple {
            if let [key, value] = tuple_fields(edit, body, 2)?.as_slice() {
                append(*key, 0, index)?;
                append(*value, index, values)?;
            }
        }
    } else {
        append(body, 0, shape.width)?;
    }

    let parameters = closure
        .edges("parameters")?
        .into_iter()
        .map(|p| text(edit.node(p)?, "name"))
        .collect::<Result<Vec<_>, _>>()?;
    let mut groups = Vec::<(NodeId, Vec<Column>)>::new();
    'fields: for (expression, column) in fields {
        for (representative, columns) in &mut groups {
            let bindings = parameters.iter().map(|&name| (name, name));
            match compare_keys(edit, *representative, expression, Some(bindings))? {
                Some(true) => {
                    columns.push(column);
                    continue 'fields;
                }
                Some(false) => {}
                None => return Ok(None),
            }
        }
        groups.push((expression, vec![column]));
    }
    Ok(Some(
        groups
            .into_iter()
            .map(|(_, columns)| columns)
            .filter(|columns| columns.len() > 1)
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{BinaryOp, Circuit};
    use serde_json::json;

    #[test]
    fn indexed_columns_compare_computations_under_all_closure_parameters() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let site = edit.circuit().root_id();
        let ty = edit.bool_type(site, false)?;
        let left = edit.named_variable(ty, "l")?;
        let right = edit.named_variable(ty, "r")?;
        let sum = edit.binary_expression(site, ty, BinaryOp::And, left, right)?;
        let fresh_left = edit.named_variable(ty, "l")?;
        let fresh_right = edit.named_variable(ty, "r")?;
        let repeated = edit.binary_expression(site, ty, BinaryOp::And, fresh_left, fresh_right)?;
        let reversed = edit.binary_expression(site, ty, BinaryOp::And, fresh_right, fresh_left)?;
        let key = edit.tuple_expr(site, false, &[sum])?;
        let value = edit.tuple_expr(site, false, &[reversed, repeated])?;
        let pair = edit.tuple_expr(site, true, &[key, value])?;
        let closure = edit.closure_from_variables(pair, &[left, right])?;
        let shape = OutputShape {
            width: 3,
            index_fields: Some(1),
        };
        assert_eq!(
            equal_computations(&edit, closure.into(), &shape)?,
            Some(vec![vec![shape.column(0), shape.column(2)]])
        );
        // The opaque key still occupies its slot in the indexed layout.
        let duplicates = edit.tuple_expr(site, false, &[repeated, sum])?;
        let pair = edit.tuple_expr(site, true, &[left, duplicates])?;
        let closure = edit.closure_from_variables(pair, &[left, right])?;
        assert_eq!(
            equal_computations(&edit, closure.into(), &shape)?,
            Some(vec![vec![shape.column(1), shape.column(2)]])
        );
        Ok(())
    }
}
