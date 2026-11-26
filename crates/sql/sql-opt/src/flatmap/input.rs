//! Narrow the collection reader and the columns UNNEST copies from its input row.
use crate::{
    compressed_field_type, inspect_references, project_used_fields, rebind_projected_reader,
    row_fields::RowFields, FieldRewriteError,
};
use feldera_sql_expr::{replace_flatmap_input, FlatmapColumns};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId};
use std::collections::BTreeSet;

/// Return the projection that precedes UNNEST and the UNNEST over its output.
/// Collection reads come from field analysis on its expression tree. Rebinding
/// uses the original reader after allocating the input projection.
pub fn trim_flatmap_input<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    flatmap: NodeId,
    paths: &BTreeSet<Vec<usize>>,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 2]>, FieldRewriteError<'a>> {
    let node = edit.node(flatmap)?;
    let collection = node.edge("collectionExpression")?;
    let passthrough = FlatmapColumns::read(node)?.passthrough;
    let [parameter] = edit
        .node(collection)?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("UNNEST collection reader needs one parameter"))?;
    let reference = edit.type_id(edit.node(parameter)?.edge("type")?)?;
    let row = edit.type_id(edit.node(reference.into())?.edge("type")?)?;
    let columns = edit.node(row.into())?.edges("tupFields")?;
    let mut used = RowFields::new(original).column_mask(&columns, &[], paths)?;
    for &index in &passthrough {
        *used
            .get_mut(index)
            .ok_or_else(|| Error::new("UNNEST passthrough exceeds input arity"))? = true;
    }
    if !used.contains(&false) {
        return Ok(None);
    }
    // Keep repeated passthrough columns in their original output positions.
    let passthrough = passthrough
        .into_iter()
        .map(|i| used[..i].iter().filter(|&&keep| keep).count())
        .collect();
    let borrowed = edit.reference_type(row)?;
    let projection = project_used_fields(edit, borrowed, used.clone(), next)?;
    if let Some(issue) = inspect_references(original, collection, false)?.diagnostic {
        return Err(FieldRewriteError::Binding(issue));
    }
    let reader_type = compressed_field_type(edit, reference, 1, &used)?;
    let declaration =
        edit.derive_inner(parameter, [("type", Field::Reference(reader_type.into()))])?;
    let row = compressed_field_type(edit, row, 1, &used)?;
    let reader = rebind_projected_reader(edit, collection, &[declaration], &[used])?;
    let flatmap = replace_flatmap_input(edit, flatmap, row.into(), reader.into(), passthrough)?;
    Ok(Some([projection, edit.expression(flatmap)?]))
}
