//! Recognize chains whose initializer and updates all minimize the same row expression.
use super::aggregate::first_leaf;
use crate::{index_key_equivalence, lexical_scope};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId};

fn tuple_updates(edit: &CircuitEditor, function: NodeId) -> Result<Option<Vec<NodeId>>, Error> {
    let body = edit.node(function)?.edge("body")?;
    let kind = edit.expr_kind(edit.expression(body)?)?;
    if !matches!(kind, ExpressionKind::Tuple | ExpressionKind::RawTuple)
        || !matches!(edit.node(body)?.field("fields"), Some(Field::Array(_)))
    {
        return Ok(None);
    }
    Ok(Some(edit.node(body)?.edges("fields")?))
}

fn compared_column(
    edit: &mut CircuitEditor,
    update: NodeId,
    row: NodeId,
) -> Result<Option<ExpressionId>, Error> {
    let node = edit.node(update)?;
    if edit.expr_kind(edit.expression(update)?)? != ExpressionKind::ConditionalIncrement
        || node.field("condition").is_some()
        || !matches!(
            node.data("opcode").and_then(|op| op.as_str()),
            Some("AGG_MIN" | "AGG_MIN1")
        )
    {
        return Ok(None);
    }
    let right = edit.expression(node.edge("right")?)?;
    let field = first_leaf(edit, right)?;
    edit.closure_with_parameters(field, &[row]).map(Some)
}

/// A missing decision means that a compared expression needs an equivalence
/// rule supplied by the Java expression implementation.
pub fn is_minimum_chain(
    edit: &mut CircuitEditor,
    init: NodeId,
    update: NodeId,
) -> Result<Option<bool>, Error> {
    let init_params = edit.node(init)?.edges("parameters")?;
    let update_params = edit.node(update)?.edges("parameters")?;
    let (&[init_row, _], &[_, update_row, _]) = (init_params.as_slice(), update_params.as_slice())
    else {
        return Ok(Some(false));
    };
    let (Some(initial), Some(updates)) = (tuple_updates(edit, init)?, tuple_updates(edit, update)?)
    else {
        return Ok(Some(false));
    };
    if initial.len() != updates.len() {
        return Ok(Some(false));
    }
    let mut first = None;
    for (initial, update) in initial.into_iter().zip(updates) {
        if edit.node(initial)?.data("opcode") != edit.node(update)?.data("opcode") {
            return Ok(Some(false));
        }
        let Some(column) = compared_column(edit, update, update_row)? else {
            return Ok(Some(false));
        };
        if lexical_scope::has_free_variables(edit.circuit(), column.into())? {
            return Ok(Some(false));
        }
        if let Some(bound) = first {
            let same = index_key_equivalence::equivalent(edit, bound, column)?;
            if same != Some(true) {
                return Ok(same);
            }
        }
        let bound = *first.get_or_insert(column);
        let Some(initial_column) = compared_column(edit, initial, init_row)? else {
            return Ok(Some(false));
        };
        let same = index_key_equivalence::equivalent(edit, bound, initial_column)?;
        if same != Some(true) {
            return Ok(same);
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
    fn initializer_and_update_must_minimize_the_same_row_field() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"width":32,"signed":true,"mayBeNull":false,"code":"INT32"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.type_id(roots[0])?;
        let pair = edit.tuple_type(roots[0], false, [roots[0], roots[0]])?;
        let make = |edit: &mut CircuitEditor, init, name, index, read_weight| {
            let row = edit.named_variable(pair, name)?;
            let weight = edit.named_variable(ty, "weight")?;
            let acc = edit.named_variable(ty, "acc")?;
            let column = edit.tuple_field(row, index)?;
            let right = if read_weight { weight } else { column };
            let left = if init { column } else { acc };
            let minimum = edit.make_expr(
                roots[0],
                ExpressionKind::ConditionalIncrement,
                ty,
                [
                    ("left", Field::Reference(left.into())),
                    ("right", Field::Reference(right.into())),
                    ("opcode", Field::Data("AGG_MIN".into())),
                ],
            )?;
            let body = edit.tuple_expr(roots[0], false, &[minimum])?;
            if init {
                edit.closure_from_variables(body, &[row, weight])
            } else {
                edit.closure_from_variables(body, &[acc, row, weight])
            }
        };
        let init = make(&mut edit, true, "initial", 0, false)?;
        for (column, read_weight, expected) in
            [(0, false, true), (1, false, false), (0, true, false)]
        {
            let update = make(&mut edit, false, "incoming", column, read_weight)?;
            assert_eq!(
                is_minimum_chain(&mut edit, init.into(), update.into())?,
                Some(expected)
            );
        }
        Ok(())
    }
}
