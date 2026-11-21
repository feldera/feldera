use feldera_sql_ir::{Circuit, CircuitEditor, NodeId};
use serde_json::{json, Value};
use std::error::Error;

/// Return the join edges, shared output types and closures for one delta term.
pub(super) fn delta_term(
    arena: Circuit,
    roots: &[NodeId],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let [function, key, rows @ ..] = roots else {
        return Err("missing star join function or key".into());
    };
    let mut edit = CircuitEditor::new(arena.clone());
    let key = edit.type_id(*key)?;
    let rows = rows
        .iter()
        .map(|&id| edit.type_id(id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut warnings = Vec::new();
    let plan = feldera_sql_opt::expand_star_term(
        &mut edit,
        *function,
        key,
        &rows,
        &mut next,
        &mut warnings,
    )?;
    let projection = feldera_sql_expr::ensure_expression_tree(&mut edit, plan.projection.into())?;
    let definitions = plan
        .joins
        .iter()
        .flat_map(|join| [join.output_type.into(), join.function.into()])
        .chain(std::iter::once(projection));
    let mut encoded = edit
        .circuit()
        .additional_inner_nodes(&arena, definitions)?
        .into_iter();
    let joins: Vec<_> = plan
        .joins
        .iter()
        .map(|join| {
            json!({
                "inputs": join.inputs,
                "type": encoded.next().expect("one indexed type per join"),
                "function": encoded.next().expect("one function per join"),
            })
        })
        .collect();
    Ok(json!({
        "joins": joins, "projection": encoded.next().expect("final projection"), "next_generated_name": next,
        "warnings": warnings.into_iter().map(|w| json!({
            "range": w.position, "error_type": w.kind, "message": w.message,
        })).collect::<Vec<_>>()
    }))
}
