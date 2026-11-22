use feldera_sql_expr::{Rewrite, Rule};
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, OperatorKind};

/// Java Gen2Passes unboxes window and waterline values before removing Rust clones.
pub fn gen2_passes(circuit: Circuit) -> Result<Circuit, Error> {
    remove_clones(remove_typed_boxes(circuit)?)
}

pub fn remove_typed_boxes(circuit: Circuit) -> Result<Circuit, Error> {
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
    rewrite(circuit, Rule::RemoveTypedBox)
}

pub fn remove_clones(circuit: Circuit) -> Result<Circuit, Error> {
    rewrite(circuit, Rule::RemoveClones)
}

fn rewrite(circuit: Circuit, rule: Rule) -> Result<Circuit, Error> {
    let mut rewrite = Rewrite::new(rule);
    CircuitEditor::new(circuit).rewrite_inner_roots(|editor, id| rewrite.apply(editor, id))
}
