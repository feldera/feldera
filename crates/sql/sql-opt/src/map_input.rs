//! Split a map that reads a subset of its input into a projection and a narrower reader.
use crate::{row_projection::RowProjection, terms::Terms};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, Field, NodeId};
use std::collections::BTreeSet;

pub enum MapOutput {
    Row,
    Indexed,
}

pub fn split_map_input(
    edit: &mut CircuitEditor,
    reader: NodeId,
    paths: &BTreeSet<Vec<usize>>,
    output: MapOutput,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 2]>, Error> {
    let layout = RowProjection::new(edit, reader, paths)?;
    let indexed_input = layout.is_indexed();
    let indexed_output = matches!(output, MapOutput::Indexed);
    let mut terms = Terms::default();
    let original = terms.field(edit.circuit(), &Field::Reference(reader), &[]);
    let mut equivalent = |edit: &CircuitEditor, f: ExpressionId| {
        terms.field(edit.circuit(), &Field::Reference(f.into()), &[]) == original
    };
    // An indexed-to-row map checks its reader before allocating a name for the projection.
    if indexed_input && !indexed_output {
        let rebound = layout.rebind(edit, reader)?;
        if equivalent(edit, rebound) {
            return Ok(None);
        }
        return Ok(Some([
            layout.project(edit, next, !paths.is_empty())?,
            rebound,
        ]));
    }
    let projection = layout.project(edit, next, !paths.is_empty())?;
    if indexed_input == indexed_output && equivalent(edit, projection) {
        return Ok(None);
    }
    let rebound = layout.rebind(edit, reader)?;
    if !indexed_input && equivalent(edit, rebound) {
        return Ok(None);
    }
    Ok(Some([projection, rebound]))
}
