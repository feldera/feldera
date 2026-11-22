//! UNNEST preserves waterlines only on columns copied from its input row.
use super::unknown;
use feldera_sql_expr::{FlatmapColumns, copy_expression, flatmap_generated_types};
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, NodeId};

pub fn flatmap_waterline(
    edit: &mut CircuitEditor,
    site: NodeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let flatmap = edit.node(site)?;
    let columns = FlatmapColumns::read(flatmap)?;
    let row = edit.type_id(flatmap.edge("inputRowType")?)?;
    let generated = flatmap_generated_types(edit, site)?;
    if columns.passthrough.len() + generated.len() != columns.width {
        return Err(Error::new(
            "UNNEST column count differs from the shuffle input width",
        ));
    }
    let borrowed = edit.reference_type(row)?;
    let parameter = edit.variable_at(site, borrowed, next)?;
    let mut fields = Vec::with_capacity(columns.width);
    for index in columns.passthrough {
        let read = copy_expression(edit, parameter.into())?;
        let row = edit.dereference(edit.expression(read)?)?;
        fields.push(edit.tuple_field(row, index)?);
    }
    for ty in generated {
        fields.push(unknown(edit, ty)?);
    }
    let shuffled: Vec<_> = columns.output.iter().map(|&i| fields[i]).collect();
    let body = edit.tuple_expr(site, false, &shuffled)?;
    edit.closure_from_variables(body, &[parameter])
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn shuffled_passthrough_reads_share_a_binding_and_drop_its_position() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "allOperators": [], "declarations": []
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class": "DBSPFlatmap", "id": 1, "sourceEmpty": false,
            "position": {"start_line_number": 2, "start_column": 3, "end_line_number": 2, "end_column": 8},
            "inputRowType": {"class": "DBSPTypeTuple", "id": 2, "mayBeNull": false,
                "tupFields": [{"class": "DBSPTypeInteger", "id": 3, "code": "INT32",
                    "width": 32, "signed": true, "mayBeNull": false}]},
            "collectionExpression": {"class": "DBSPClosureExpression", "id": 4,
                "type": {"class": "DBSPTypeFunction", "id": 5,
                    "resultType": {"class": "DBSPTypeArray", "id": 6, "typeArgs": [{"node": 3}]}}},
            "passthroughIndexes": [0, 0],
            "shuffle": {"class": "ExplicitShuffle", "inputLength": 3, "indexes": [0, 2, 1]}
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let mut next = 4;
        let closure = flatmap_waterline(&mut edit, roots[0], &mut next)?;
        let closure = edit.node(closure.into())?;
        let parameter = edit.node(closure.edges("parameters")?[0])?;
        assert!(parameter.field("position").is_some());
        assert_eq!(next, 5);

        let fields = edit.node(closure.edge("body")?)?.edges("fields")?;
        assert_eq!(edit.node(fields[1])?.class(), "NoExpression");
        let read = |id| {
            edit.node(edit.node(id)?.edge("expression")?)?
                .edge("expression")
        };
        let first = read(fields[0])?;
        let second = read(fields[2])?;
        assert_ne!(first, second);
        for occurrence in [first, second] {
            let var = edit.node(occurrence)?;
            assert_eq!(var.data("variable"), parameter.data("name"));
            assert_eq!(var.edge("type")?, parameter.edge("type")?);
            assert!(var.field("position").is_none());
        }
        Ok(())
    }
}
