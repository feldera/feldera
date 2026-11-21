use feldera_sql_expr::{Rewrite, Rule};
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, OperatorKind};

/// Java Gen2Passes unboxes window and waterline values before removing Rust clones.
pub fn gen2_passes(circuit: Circuit) -> Result<Circuit, Error> {
    if circuit
        .operator_order(BodyOrder::Preorder)?
        .into_iter()
        .any(|id| {
            circuit.node(id).and_then(|op| op.operator_kind())
                == Some(OperatorKind::InputMapWithWaterline)
        })
    {
        return Err(Error::new(
            "Cannot unbox the waterline output of a keyed input map",
        ));
    }
    let mut circuit = circuit;
    for rule in [Rule::RemoveTypedBox, Rule::RemoveClones] {
        let mut rewrite = Rewrite::new(rule);
        circuit = CircuitEditor::new(circuit)
            .rewrite_inner_roots(|editor, id| rewrite.apply(editor, id))?;
    }
    Ok(circuit)
}
