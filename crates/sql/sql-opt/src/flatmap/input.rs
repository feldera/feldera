//! Narrow the collection reader and the columns UNNEST copies from its input row.
use crate::{compressed_field_type, project_used_fields, rebind_projected_reader};
use feldera_sql_expr::{replace_flatmap_input, FlatmapColumns};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, Field, NodeId};

/// Return the projection that precedes UNNEST and the UNNEST over its output.
/// The mask initially covers only reads of the collection-producing closure.
pub fn trim_flatmap_input(
    edit: &mut CircuitEditor,
    flatmap: NodeId,
    mut used: Vec<bool>,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 2]>, Error> {
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
    if used.len() != edit.node(row.into())?.edges("tupFields")?.len() {
        return Err(Error::new("UNNEST field mask differs from its input arity"));
    }
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
    let reader_type = compressed_field_type(edit, reference, 1, &used)?;
    let declaration =
        edit.derive_inner(parameter, [("type", Field::Reference(reader_type.into()))])?;
    let row = compressed_field_type(edit, row, 1, &used)?;
    let reader = rebind_projected_reader(edit, collection, &[declaration], &[used])?;
    let flatmap = replace_flatmap_input(edit, flatmap, row.into(), reader.into(), passthrough)?;
    Ok(Some([projection, edit.expression(flatmap)?]))
}
