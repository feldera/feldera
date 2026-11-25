//! Project one indexed input for all components of a stream aggregate.
use crate::{
    FieldRewriteError, aggregate_groups::row_parameters, compressed_field_type, inspect_references,
    project_indexed_value, project_used_fields, rebind_projected_aggregate, row_fields::RowFields,
};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, TypeId};
use std::collections::{BTreeSet, HashMap};

pub struct AggregateInput<'a> {
    pub pair: TypeId,
    pub row: TypeId,
    pub components: &'a [NodeId],
}

pub struct ProjectedAggregateInput {
    pub projection: ExpressionId,
    pub row: TypeId,
    pub components: Vec<ExpressionId>,
}

/// All components consume the same projected row. Within one component, two
/// closures that share a row declaration must receive the same replacement.
pub fn trim_aggregate_input<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    input: AggregateInput<'_>,
    reads: Vec<BTreeSet<Vec<usize>>>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<ProjectedAggregateInput>, FieldRewriteError<'a>> {
    if reads.len() != input.components.len() {
        return Err(Error::new("expected field reads for each aggregate component").into());
    }
    let paths = reads.into_iter().flatten().collect();
    let Some(used) = RowFields::new(original).projection_mask(input.row.into(), &paths)? else {
        return Ok(None);
    };
    let mut components = Vec::with_capacity(input.components.len());
    for &part in input.components {
        if let Some(issue) = inspect_references(original, part, false)?.diagnostic {
            return Err(FieldRewriteError::Binding(issue));
        }
        let mut replacements = Vec::new();
        let mut declarations = HashMap::new();
        for &(field, slot) in row_parameters(edit.node(part)?)? {
            let closure = edit.node(part)?.edge(field)?;
            let params = edit.node(closure)?.edges("parameters")?;
            let param = *params
                .get(slot)
                .ok_or_else(|| Error::new("missing aggregate row parameter"))?;
            let replacement = match declarations.entry(param) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let ty = compressed_field_type(edit, input.row, 1, &used)?;
                    *entry
                        .insert(edit.derive_inner(param, [("type", Field::Reference(ty.into()))])?)
                }
            };
            replacements.push(replacement);
        }
        let result = rebind_projected_aggregate(edit, part, &replacements, &used)?;
        // Each occurrence in the new list has its own aggregate descriptor.
        components.push(if NodeId::from(result) == part {
            let fresh = edit.derive_inner(part, [])?;
            edit.expression(fresh)?
        } else {
            result
        });
    }
    let row = compressed_field_type(edit, input.row, 1, &used)?;
    let value = project_used_fields(edit, input.row, used, next)?;
    let projection = project_indexed_value(edit, input.pair, value, next, warnings)?;
    Ok(Some(ProjectedAggregateInput {
        projection,
        row,
        components,
    }))
}
