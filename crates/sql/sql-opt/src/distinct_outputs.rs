//! Enforce set semantics at output sinks using the preceding multiset analysis.

use crate::rebuild::{input_type, rebuild_input_closures};
use crate::stream_rewrites::body_members;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind, OutputPort};
use std::collections::HashSet;

/// Each selected sink gets its own snapshot DISTINCT, including sinks that
/// read the same input. Other consumers continue to see the original weights.
pub fn ensure_distinct_outputs(
    circuit: Circuit,
    multiset_sinks: &[u64],
    next: &mut u64,
) -> Result<Circuit, Error> {
    if multiset_sinks.is_empty() {
        return Ok(circuit);
    }
    let mut selected: HashSet<_> = multiset_sinks.iter().copied().map(NodeId::Outer).collect();
    if selected.len() != multiset_sinks.len() {
        return Err(Error::new("duplicate sink in multiset analysis"));
    }
    let mut origins = circuit.intermediate_origins()?;
    let mut sites = Vec::with_capacity(selected.len());
    for (body, id) in body_members(&circuit)? {
        if selected.remove(&id) {
            let sink = circuit.node(id).expect("validated circuit member");
            if sink.operator_kind() != Some(OperatorKind::Sink) {
                return Err(Error::new("multiset output analysis requires a sink"));
            }
            let mut inputs = sink.inputs();
            let input = inputs
                .next()
                .filter(|_| inputs.next().is_none())
                .ok_or_else(|| Error::new("output sink must have one input"))?;
            let origin = origins.derive::<1>(id)?.map(|[origin]| origin);
            sites.push((body, id, input, origin));
        }
    }
    if !selected.is_empty() {
        return Err(Error::new("unknown sink in multiset analysis"));
    }

    let mut edit = CircuitEditor::new(circuit);
    for (body, sink, input, origin) in sites {
        let ty = input_type(&edit, input)?;
        let distinct = edit.append_operator(
            body,
            sink,
            OperatorKind::StreamDistinct,
            &[input],
            [("outputType", Field::Reference(ty.into()))],
        )?;
        edit.set_intermediate_origin(distinct, origin)?;
        edit.place_before(body, distinct, sink)?;
        edit.rewire_inputs(
            sink,
            &[OutputPort {
                operator: distinct,
                number: 0,
            }],
        )?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}
