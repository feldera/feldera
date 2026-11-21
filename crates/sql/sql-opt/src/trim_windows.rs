//! Move a single reader's value projection before a window. The key and the
//! control stream retain their original values and window bounds.
use crate::rebuild::ProjectionNames;
use crate::row_projection::RowProjection;
use crate::stream_rewrites::{append_map, body_members, reader_counts};
use feldera_sql_expr::input_field_paths;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort,
};
use std::collections::{BTreeMap, HashMap};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn closures(
    edit: &mut CircuitEditor,
    function: NodeId,
    next: &mut u64,
) -> Result<Option<(ExpressionId, ExpressionId)>, Error> {
    let mut paths = input_field_paths(edit.circuit(), function)?;
    paths.insert(vec![0]); // Window membership depends on the complete key.
    let layout = RowProjection::new(edit, function, &paths)?;
    if !layout.has_unused_fields() {
        return Ok(None);
    }
    Ok(Some((
        layout.project(edit, next, true)?,
        layout.rebind(edit, function)?,
    )))
}

pub fn trim_windows(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let parents: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    for id in order {
        let reader = edit.node(id)?;
        projections.visit(id, reader, &mut names)?;
        if reader.operator_kind() != Some(Op::MapIndex) {
            continue;
        }
        let window = reader
            .inputs()
            .next()
            .ok_or_else(|| Error::new("window reader requires an input"))?
            .operator;
        if readers.get(&window) != Some(&1)
            || edit.node(window)?.operator_kind() != Some(Op::Window)
        {
            continue;
        }
        let function = reader.edge("function")?;
        let Some((pre, post)) = closures(&mut edit, function, &mut names)? else {
            continue;
        };
        let op = edit.node(window)?;
        let [data, control] = op
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("window requires two inputs"))?;
        let mut options = Vec::new();
        for key in ["lowerInclusive", "upperInclusive", "lowerUnbounded"] {
            options.push((
                key,
                op.field(key)
                    .ok_or_else(|| Error::new("missing window bound option"))?
                    .clone(),
            ));
        }
        let body = parents[&id];
        let (index, stream) = append_map(&mut edit, body, window, Op::MapIndex, data, pre)?;
        options.push(("outputType", link(stream)));
        let narrowed =
            edit.append_operator(body, window, Op::Window, &[output(index), control], options)?;
        let (post, _) = append_map(&mut edit, body, id, Op::MapIndex, output(narrowed), post)?;
        edit.place_before(body, index, id)?;
        replacements.insert(id, output(post));
        projections.replaced(id);
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(circuit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::ExpressionKind as Expr;
    use serde_json::json;
    use std::collections::BTreeSet;

    #[test]
    fn nullable_window_values_retain_empty_rows_and_rebind_sparse_columns() {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[],
            "type":{"class":"DBSPTypeInteger","id":1,"code":"INT32","mayBeNull":false}
        }))
        .unwrap();
        let mut edit = CircuitEditor::new(graph);
        let integer = edit.type_id(NodeId::Inner(1)).unwrap();
        let empty = edit.tuple_type(integer.into(), false, []).unwrap();
        let value = edit
            .tuple_type(
                integer.into(),
                false,
                [integer.into(), empty.into(), integer.into()],
            )
            .unwrap();
        let value = edit.with_nullability(value, true).unwrap();
        let key = edit.reference_type(integer).unwrap();
        let value = edit.reference_type(value).unwrap();
        let pair = edit
            .tuple_type(value.into(), true, [key.into(), value.into()])
            .unwrap();
        let mut next = 0;
        let row = edit.variable(pair, &mut next).unwrap();
        let key = edit.tuple_field(row, 0).unwrap();
        let key = edit.dereference(key).unwrap();
        let value = edit.tuple_field(row, 1).unwrap();
        let value = edit.dereference(value).unwrap();
        let last = edit.tuple_field(value, 2).unwrap();
        let result = edit.tuple_expr(row.into(), true, &[key, last]).unwrap();
        let reader = edit.closure_from_variables(result, &[row]).unwrap();

        let (pre, post) = closures(&mut edit, reader.into(), &mut next)
            .unwrap()
            .unwrap();
        assert_eq!(next, 2);
        let projected = edit.node(pre.into()).unwrap().edge("body").unwrap();
        let value = edit.node(projected).unwrap().edges("fields").unwrap()[1];
        assert_eq!(
            edit.expr_kind(edit.expression(value).unwrap()).unwrap(),
            Expr::If
        );
        let projected_type = edit
            .expression_type(edit.expression(value).unwrap())
            .unwrap();
        assert!(edit.nullable(projected_type).unwrap());
        assert_eq!(
            edit.node(projected_type.into())
                .unwrap()
                .edges("tupFields")
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            input_field_paths(edit.circuit(), post.into()).unwrap(),
            BTreeSet::from([vec![0], vec![1, 1]])
        );
        assert!(
            closures(&mut edit, post.into(), &mut next)
                .unwrap()
                .is_none()
        );
        assert_eq!(next, 2);
    }
}
