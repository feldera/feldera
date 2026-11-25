//! Move a single reader's value projection before a window. The key and the
//! control stream retain their original values and window bounds.
use crate::rebuild::ProjectionNames;
use crate::row_projection::RowProjection;
use crate::stream_rewrites::{append_map, body_members, reader_counts};
use crate::{FieldRewriteError, limiters::WindowBounds};
use feldera_sql_expr::{input_field_paths, lexical_scope::inspect_references};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

pub struct TrimmedWindow {
    pre: ExpressionId,
    post: ExpressionId,
}

impl TrimmedWindow {
    /// The first two operators inherit the window site. The final projection
    /// inherits the reader site and preserves the reader's original result.
    pub fn append(
        self,
        edit: &mut CircuitEditor,
        body: NodeId,
        [window, reader]: [NodeId; 2],
        [data, control]: [OutputPort; 2],
        bounds: WindowBounds,
    ) -> Result<[NodeId; 3], Error> {
        let (index, stream) = append_map(edit, body, window, Op::MapIndex, data, self.pre)?;
        let narrowed = edit.append_operator(
            body,
            window,
            Op::Window,
            &[output(index), control],
            [
                ("outputType", link(stream)),
                ("lowerInclusive", Field::Data(bounds.lower_inclusive.into())),
                ("upperInclusive", Field::Data(bounds.upper_inclusive.into())),
                ("lowerUnbounded", Field::Data(bounds.lower_unbounded.into())),
            ],
        )?;
        let (post, _) = append_map(
            edit,
            body,
            reader,
            Op::MapIndex,
            output(narrowed),
            self.post,
        )?;
        Ok([index, narrowed, post])
    }
}

pub fn trim_window_fields<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    function: NodeId,
    mut paths: BTreeSet<Vec<usize>>,
    next: &mut u64,
) -> Result<Option<TrimmedWindow>, FieldRewriteError<'a>> {
    paths.insert(vec![0]); // Window membership depends on the complete key.
    let layout = RowProjection::new(edit, function, &paths)?;
    if !layout.has_unused_fields() {
        return Ok(None);
    }
    let pre = layout.project(edit, next, true)?;
    if let Some(issue) = inspect_references(original, function, false)?.diagnostic {
        return Err(FieldRewriteError::Binding(issue));
    }
    let post = layout.rebind(edit, function)?;
    Ok(Some(TrimmedWindow { pre, post }))
}

/// Find exclusive readers from circuit topology before inspecting any closure.
pub fn window_trim_readers(circuit: &Circuit) -> Result<BTreeSet<NodeId>, Error> {
    if !circuit
        .operators()
        .any(|(_, op)| op.operator_kind() == Some(Op::Window))
    {
        return Ok(BTreeSet::new());
    }
    let members = body_members(circuit)?;
    let counts = reader_counts(circuit, &members);
    let mut selected = BTreeSet::new();
    for (_, id) in members {
        let reader = circuit.node(id).expect("validated circuit member");
        if reader.operator_kind() == Some(Op::MapIndex) {
            let input = reader
                .inputs()
                .next()
                .ok_or_else(|| Error::new("window reader requires an input"))?;
            if counts.get(&input.operator) == Some(&1)
                && circuit.node(input.operator).and_then(|n| n.operator_kind()) == Some(Op::Window)
            {
                selected.insert(id);
            }
        }
    }
    Ok(selected)
}

pub fn trim_windows(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let selected = window_trim_readers(&circuit)?;
    if selected.is_empty() {
        return Ok(circuit);
    }
    let parents: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit.clone());
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    for id in order {
        let reader = edit.node(id)?;
        projections.visit(id, reader, &mut names)?;
        if !selected.contains(&id) {
            continue;
        }
        let window = reader
            .inputs()
            .next()
            .ok_or_else(|| Error::new("window reader requires an input"))?
            .operator;
        let function = reader.edge("function")?;
        let paths = input_field_paths(&circuit, function)?;
        let Some(trim) = trim_window_fields(&circuit, &mut edit, function, paths, &mut names)
            .map_err(|e| match e {
                FieldRewriteError::InvalidIr(error) => error,
                FieldRewriteError::Binding(issue) => {
                    Error::new(format!("invalid window reader binding: {issue:?}"))
                }
            })?
        else {
            continue;
        };
        let op = edit.node(window)?;
        let inputs = op
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("window requires two inputs"))?;
        let flag = |name| {
            op.data(name)
                .and_then(|v| v.as_bool())
                .ok_or_else(|| Error::new("missing window bound option"))
        };
        let bounds = WindowBounds {
            lower_inclusive: flag("lowerInclusive")?,
            upper_inclusive: flag("upperInclusive")?,
            lower_unbounded: flag("lowerUnbounded")?,
        };
        let body = parents[&id];
        let [index, _, post] = trim.append(&mut edit, body, [window, id], inputs, bounds)?;
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

        let original = edit.circuit().clone();
        let paths = input_field_paths(&original, reader.into()).unwrap();
        let TrimmedWindow { pre, post } =
            trim_window_fields(&original, &mut edit, reader.into(), paths, &mut next)
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
        let original_for_post = edit.circuit().clone();
        assert!(
            trim_window_fields(
                &original_for_post,
                &mut edit,
                post.into(),
                input_field_paths(&original_for_post, post.into()).unwrap(),
                &mut next
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(next, 2);
    }
}
