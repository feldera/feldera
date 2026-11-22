//! Transfer the lower bound unchanged through a row or an indexed identity operator.
use super::bindings::waterline_bindings;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, TypeId, TypeKind};

pub fn identity_bounds(
    edit: &mut CircuitEditor,
    row: TypeId,
    projected: TypeId,
    indexed: bool,
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let name = CircuitEditor::next_variable_name(next)?;
    if indexed {
        return indexed_bounds(edit, row, projected, &name);
    }
    let reference = edit.reference_type(row)?;
    let variable = edit.named_variable(reference, &name)?;
    let body = edit.dereference(variable)?;
    let original = edit.closure_from_variables(body, &[variable])?;

    let reference = edit.reference_type(projected)?;
    let variable = edit.named_variable(reference, &name)?;
    let body = edit.dereference(variable)?;
    let reduced = edit.closure_from_variables(body, &[variable])?;
    Ok([original, reduced])
}

/// The data operator takes (&K, &V). Its waterline takes a reference to the
/// compressed tuple containing just the key and value components with bounds.
fn indexed_bounds(
    edit: &mut CircuitEditor,
    row: TypeId,
    projected: TypeId,
    name: &str,
) -> Result<[ExpressionId; 2], Error> {
    if edit.node(row.into())?.edges("tupFields")?.len() != 2 {
        return Err(Error::new("indexed monotonicity requires a key and value"));
    }
    let parameter_type = tuple_of_references(edit, row)?;
    let variable = edit.named_variable(parameter_type, name)?;
    // Each field read in the original closure is a separate variable occurrence.
    let key = edit.tuple_field(variable, 0)?;
    let key = edit.dereference(key)?;
    let second = edit.named_variable(parameter_type, name)?;
    let value = edit.tuple_field(second, 1)?;
    let value = edit.dereference(value)?;
    let body = edit.tuple_with_type(row, &[key, value])?;
    let original = edit.closure_from_variables(body, &[variable])?;

    let bindings = waterline_bindings(edit, &[(name, projected)])?;
    let projected_refs = tuple_of_references(edit, projected)?;
    // The transfer analysis resolves all reduced reads to the same bound variable.
    let bound = edit.named_variable(projected_refs, name)?;
    let arity = edit.node(projected.into())?.edges("tupFields")?.len();
    let mut fields = Vec::with_capacity(arity);
    for index in 0..arity {
        let field = edit.tuple_field(bound, index)?;
        fields.push(edit.dereference(field)?);
    }
    let raw = edit.type_kind(projected)? == TypeKind::RawTuple;
    let body = edit.tuple_expr(projected.into(), raw, &fields)?;
    let reduced = bindings.closure(edit, body)?;
    Ok([original, reduced])
}

fn tuple_of_references(edit: &mut CircuitEditor, row: TypeId) -> Result<TypeId, Error> {
    let mut references = Vec::new();
    for field in edit.node(row.into())?.edges("tupFields")? {
        references.push(edit.reference_type(edit.type_id(field)?)?.into());
    }
    let raw = edit.type_kind(row)? == TypeKind::RawTuple;
    let pair = edit.tuple_type(row.into(), raw, references)?;
    edit.with_nullability(pair, edit.nullable(row)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn indexed_bounds_bind_only_retained_components_and_preserve_read_identity() -> Result<(), Error>
    {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let mut edit = CircuitEditor::new(circuit);
        let sample = edit.circuit().root_id();
        let key = edit.bool_type(sample, false)?;
        let value = edit.tuple_type(sample, false, [key.into()])?;
        let row = edit.tuple_type(sample, true, [key.into(), value.into()])?;
        for retained in [vec![key], vec![value], vec![key, value]] {
            let projected =
                edit.tuple_type(sample, true, retained.iter().copied().map(Into::into))?;
            let mut next = 5;
            let [original, reduced] = identity_bounds(&mut edit, row, projected, true, &mut next)?;
            assert_eq!(next, 6);
            let body = edit.node(original.into())?.edge("body")?;
            let reads = |body| -> Result<Vec<_>, Error> {
                edit.node(body)?
                    .edges("fields")?
                    .into_iter()
                    .map(|read| {
                        let field = edit.node(read)?.edge("expression")?;
                        edit.node(field)?.edge("expression")
                    })
                    .collect()
            };
            let wide = reads(body)?;
            assert_ne!(wide[0], wide[1]);
            let block = edit.node(edit.node(reduced.into())?.edge("body")?)?;
            let result = block.edge("lastExpression")?;
            let bound_reads = reads(result)?;
            assert!(bound_reads.iter().all(|&id| id == bound_reads[0]));
            let bound_type = edit.node(result)?.edge("type")?;
            let types = edit.node(bound_type)?.edges("tupFields")?;
            assert_eq!(
                types,
                retained.into_iter().map(Into::into).collect::<Vec<_>>()
            );
            assert_eq!(block.edges("contents")?.len(), 1);
        }
        Ok(())
    }
}
