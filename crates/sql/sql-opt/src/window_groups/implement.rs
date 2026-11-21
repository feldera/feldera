//! Build one index per selected window group and restore each window's original row shape.
use crate::{rebuild::rebuild_input_closures, stream_rewrites::body_members};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort, TypeId,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};

#[derive(Deserialize)]
pub struct WindowProjection {
    window: u64,
    fields: Vec<usize>,
}

impl WindowProjection {
    fn operator(&self) -> NodeId {
        NodeId::Outer(self.window)
    }
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

struct WindowIndex {
    source: OutputPort,
    parameter: NodeId,
    key: NodeId,
    fields: Vec<NodeId>,
}

impl WindowIndex {
    fn read(edit: &CircuitEditor, window: NodeId) -> Result<Self, Error> {
        let window = edit.node(window)?;
        if window.operator_kind() != Some(OperatorKind::Window) {
            return Err(Error::new("window group contains a non-window operator"));
        }
        let left = window
            .inputs()
            .next()
            .ok_or_else(|| Error::new("window has no left input"))?;
        let index = edit.node(left.operator)?;
        if index.operator_kind() != Some(OperatorKind::MapIndex) {
            return Err(Error::new("shared window requires an indexed projection"));
        }
        let source = index
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing window index input"))?;
        let closure = edit.node(index.edge("function")?)?;
        let [parameter] = closure
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("window index requires one parameter"))?;
        let [key, value] = edit
            .node(closure.edge("body")?)?
            .edges("fields")?
            .try_into()
            .map_err(|_| Error::new("window index requires a key and a value"))?;
        Ok(Self {
            source,
            parameter,
            key,
            fields: edit.node(value)?.edges("fields")?,
        })
    }
}

fn apply_field(
    edit: &mut CircuitEditor,
    field: NodeId,
    parameter: NodeId,
    row: ExpressionId,
) -> Result<ExpressionId, Error> {
    let value = reduce_closure_body(edit, field, &[parameter], &[row.into()])?;
    edit.expression(value)
}

fn shared_index(
    edit: &mut CircuitEditor,
    body: NodeId,
    group: &[WindowProjection],
    next: &mut u64,
) -> Result<(NodeId, TypeId), Error> {
    let first = group
        .first()
        .ok_or_else(|| Error::new("empty window group"))?
        .operator();
    let index = WindowIndex::read(edit, first)?;
    let argument_type = edit.type_id(edit.node(index.parameter)?.edge("type")?)?;
    let row = edit.variable(argument_type, next)?;
    let key = apply_field(edit, index.key, index.parameter, row)?;
    let mut fields = Vec::new();
    for member in group {
        let member_index = WindowIndex::read(edit, member.operator())?;
        if member_index.source != index.source || member_index.fields.len() != member.fields.len() {
            return Err(Error::new(
                "window group disagrees with its input projections",
            ));
        }
        for (&column, field) in member.fields.iter().zip(member_index.fields) {
            if column == fields.len() {
                fields.push(apply_field(edit, field, member_index.parameter, row)?);
            } else if column > fields.len() {
                return Err(Error::new(
                    "shared window columns must follow first-use order",
                ));
            }
        }
    }
    let value = edit.tuple_expr(first, false, &fields)?;
    let pair = edit.tuple_expr(first, true, &[key, value])?;
    let function = edit.closure_from_variables(pair, &[row])?;
    let stream = edit.indexed_zset_from_pair(edit.expression_type(pair)?)?;
    let id = edit.append_operator(
        body,
        first,
        OperatorKind::MapIndex,
        &[index.source],
        [
            ("outputType", Field::Reference(stream.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    Ok((id, stream))
}

fn project_back(
    edit: &mut CircuitEditor,
    stream: TypeId,
    columns: &[usize],
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let index = edit.node(stream.into())?;
    let key = edit.type_id(index.edge("keyType")?)?;
    let value = edit.type_id(index.edge("elementType")?)?;
    let key_ref = edit.reference_type(key)?;
    let value_ref = edit.reference_type(value)?;
    let pair = edit.tuple_type(stream.into(), true, [key_ref.into(), value_ref.into()])?;
    let row = edit.variable(pair, next)?;
    let key = edit.tuple_field(row, 0)?;
    let key = edit.dereference(key)?;
    let key = edit.clone_if_needed(key)?;
    let mut fields = Vec::with_capacity(columns.len());
    for &column in columns {
        let value = edit.tuple_field(row, 1)?;
        let value = edit.dereference(value)?;
        let value = edit.tuple_field(value, column)?;
        fields.push(edit.clone_if_needed(value)?);
    }
    let values = edit.tuple_expr(stream.into(), false, &fields)?;
    let result = edit.tuple_expr(stream.into(), true, &[key, values])?;
    edit.closure_from_variables(result, &[row])
}

pub fn implement_window_groups(
    circuit: Circuit,
    groups: &[Vec<WindowProjection>],
    next: &mut u64,
) -> Result<Circuit, Error> {
    if groups.is_empty() {
        return Ok(circuit);
    }
    let mut members = HashMap::new();
    for (group, windows) in groups.iter().enumerate() {
        for member in windows {
            if members
                .insert(member.operator(), (group, &member.fields))
                .is_some()
            {
                return Err(Error::new("a window belongs to multiple projections"));
            }
        }
    }
    let order = body_members(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut indexes = HashMap::new();
    let mut replacements = BTreeMap::new();
    let mut names = *next;
    for (body, id) in order {
        let Some((group, fields)) = members.remove(&id) else {
            continue;
        };
        let mut insert_before = None;
        let (shared, stream) = *match indexes.entry(group) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let shared = shared_index(&mut edit, body, &groups[group], &mut names)?;
                insert_before = Some(shared.0);
                entry.insert(shared)
            }
        };
        let window = edit.node(id)?;
        let control = window
            .inputs()
            .nth(1)
            .ok_or_else(|| Error::new("window has no control input"))?;
        let original_type = window.edge("outputType")?;
        let mut bounds = Vec::new();
        for field in ["lowerInclusive", "upperInclusive", "lowerUnbounded"] {
            bounds.push((
                field,
                window
                    .field(field)
                    .cloned()
                    .ok_or_else(|| Error::new("missing window bound option"))?,
            ));
        }
        bounds.push(("outputType", Field::Reference(stream.into())));
        let replacement = edit.append_operator(
            body,
            id,
            OperatorKind::Window,
            &[output(shared), control],
            bounds,
        )?;
        let function = project_back(&mut edit, stream, fields, &mut names)?;
        let narrow = edit.append_operator(
            body,
            id,
            OperatorKind::MapIndex,
            &[output(replacement)],
            [
                ("outputType", Field::Reference(original_type)),
                ("function", Field::Reference(function.into())),
            ],
        )?;
        edit.place_before(body, insert_before.unwrap_or(replacement), id)?;
        replacements.insert(id, output(narrow));
    }
    if !members.is_empty() {
        return Err(Error::new("window group contains an unknown operator"));
    }
    let circuit = rebuild_input_closures(edit.finish_forwarding(&replacements)?, &mut names)?;
    *next = names;
    Ok(circuit)
}
