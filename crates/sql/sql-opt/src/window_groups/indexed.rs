//! Plan shared indexes after temporal predicates have become window operators.
use crate::projection_key::{row_parameter, SourceRows, ProjectionKey};
use super::{qualifying_window_groups, shared_window_fields, window_candidates, WindowFields};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, NodeId, OperatorKind, OutputPort};

pub(super) struct WindowIndex {
    pub(super) source: OutputPort,
    pub(super) parameter: NodeId,
    pub(super) key: NodeId,
    pub(super) fields: Vec<NodeId>,
}

impl WindowIndex {
    pub(super) fn read(edit: &CircuitEditor, window: NodeId) -> Result<Self, Error> {
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

/// Selection order and field ordinals are consumed by the graph rewriter.
#[derive(serde::Serialize)]
pub struct IndexedWindowGroup {
    windows: Vec<u64>,
    fields: WindowFields,
}

pub enum IndexedWindows {
    Groups(Vec<IndexedWindowGroup>),
    Candidates(Vec<NodeId>),
}

pub fn indexed_window_groups(circuit: Circuit, threshold: usize) -> Result<IndexedWindows, Error> {
    let candidates = window_candidates(&circuit)?;
    if candidates.is_empty() || threshold == 0 {
        return Ok(IndexedWindows::Groups(Vec::new()));
    }
    let edit = CircuitEditor::new(circuit);
    let mut inputs = SourceRows::default();
    let mut keys = Vec::with_capacity(candidates.len());
    let mut indexes = Vec::with_capacity(candidates.len());
    for &window in &candidates {
        let index = WindowIndex::read(&edit, window)?;
        let parameter = row_parameter(&edit, index.parameter)?;
        let Some(row) = inputs.representative(&edit, index.source, parameter.ty)? else {
            return Ok(IndexedWindows::Candidates(candidates));
        };
        let Some(key) = ProjectionKey::read(&edit, edit.expression(index.key)?, parameter)? else {
            return Ok(IndexedWindows::Candidates(candidates));
        };
        keys.push((index.source, row, key));
        indexes.push((index, parameter));
    }

    let mut groups = Vec::new();
    for members in qualifying_window_groups(&keys, threshold) {
        let mut fields = Vec::with_capacity(members.len());
        let mut windows = Vec::with_capacity(members.len());
        for member in members {
            let (index, parameter) = &indexes[member];
            let mut projections = Vec::with_capacity(index.fields.len());
            for &field in &index.fields {
                let Some(field) = ProjectionKey::read(&edit, edit.expression(field)?, *parameter)?
                else {
                    return Ok(IndexedWindows::Candidates(candidates));
                };
                projections.push(field);
            }
            fields.push(projections);
            let NodeId::Outer(window) = candidates[member] else {
                return Err(Error::new("expected window operator"));
            };
            windows.push(window);
        }
        groups.push(IndexedWindowGroup {
            windows,
            fields: shared_window_fields(&fields),
        });
    }
    Ok(IndexedWindows::Groups(groups))
}
