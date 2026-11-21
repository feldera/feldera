use crate::rebuild::rebuild_input_closures;
use feldera_sql_expr::{expand_sql_casts, prepare_column_default, CastSafety};
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, OperatorKind as Op};

/// The final stage of ExpandCasts also prepares expressions embedded in table metadata.
pub fn expand_metadata_casts(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut names = *next;
    for id in order {
        if matches!(
            edit.node(id)?.operator_kind(),
            Some(
                Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap | Op::InputMapWithWaterline
            )
        ) {
            edit.rewrite_column_initializers(id, |edit, value| {
                prepare_column_default(edit, value, &mut names)
            })?;
        }
    }
    let result = rebuild_input_closures(edit.finish_with_rewrites()?, &mut names)?;
    *next = names;
    Ok(result)
}

/// One iteration of the expression stage inside the optimizer's ExpandCasts group.
pub fn expand_circuit_casts(
    circuit: Circuit,
    next: &mut u64,
    safety: CastSafety,
) -> Result<Circuit, Error> {
    crate::expression_visitor::rewrite_expressions(circuit, next, |editor, root, _, names| {
        expand_sql_casts(editor, root, safety, names)
    })
}
