//! Replace filters with operator groups while preserving circuit and generated-name order.
use crate::{rebuild::ProjectionNames, stream_rewrites::body_members};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::{BTreeMap, HashMap};

pub(super) fn rewrite_filters(
    circuit: Circuit,
    next: &mut u64,
    mut rewrite: impl FnMut(
        &mut CircuitEditor,
        NodeId,
        NodeId,
        &mut u64,
        &mut Vec<SimplifyWarning>,
    ) -> Result<Option<(NodeId, OutputPort)>, Error>,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut placements = Vec::new();
    let mut warnings = Vec::new();
    let mut names = *next;
    let mut projections = ProjectionNames::default();
    for id in order {
        let node = edit.node(id)?;
        projections.visit(id, node, &mut names)?;
        if node.operator_kind() == Some(Op::Filter) {
            let body = bodies[&id];
            if let Some((first, output)) = rewrite(&mut edit, body, id, &mut names, &mut warnings)?
            {
                placements.push((body, first, id));
                replacements.insert(id, output);
                projections.replaced(id);
            }
        }
    }
    // Keep original provenance definitions ahead of new references while building
    // expressions. Each placement moves the remaining suffix of its body.
    for (body, first, original) in placements.into_iter().rev() {
        edit.place_before(body, first, original)?;
    }
    let result = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((result, warnings))
}
