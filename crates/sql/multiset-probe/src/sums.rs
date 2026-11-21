//! Ordered sum and negate rewrites on the typed operator graph.

use crate::model::{Circuit, CollectionKind, Edit, OperatorKind, Port};
use crate::{Error, Result};
use std::collections::HashMap;

pub(crate) fn merge(circuit: &Circuit) -> Result<Vec<Edit>> {
    // A nested sum can be consumed many times. Limit the total rewritten
    // edges before flattening can make a small input produce a huge output.
    const MAX_EDGES: usize = 1_000_000;
    let mut remaining_edges = MAX_EDGES;
    let mut replacements: Vec<Option<Port>> = Vec::with_capacity(circuit.operators.len());
    let mut edits = Vec::with_capacity(circuit.operators.len());

    for operator in &circuit.operators {
        let inputs: Vec<_> = operator
            .inputs
            .iter()
            .map(|port| replacements[port.operator.0].unwrap_or(*port))
            .collect();

        let negated_input = |port: Port| match &edits[port.operator.0] {
            Edit::Retain(inputs)
                if matches!(
                    circuit.operators[port.operator.0].kind,
                    OperatorKind::Negate
                ) =>
            {
                Some(inputs[0])
            }
            _ => None,
        };

        let (replacement, edit) = match operator.kind {
            OperatorKind::Negate => match negated_input(inputs[0]) {
                Some(port) => (Some(port), Edit::Bypass),
                None => (None, Edit::Retain(inputs)),
            },
            OperatorKind::Sum => match cancel(inputs, negated_input).as_slice() {
                [] => (None, Edit::Empty(CollectionKind::Snapshot)),
                [port] => (Some(*port), Edit::Bypass),
                sources => {
                    let mut flattened = Vec::new();
                    for &source in sources {
                        let inputs = match &edits[source.operator.0] {
                            Edit::Retain(inputs)
                                if matches!(
                                    circuit.operators[source.operator.0].kind,
                                    OperatorKind::Sum
                                ) =>
                            {
                                inputs.as_slice()
                            }
                            _ => std::slice::from_ref(&source),
                        };
                        reserve_edges(&mut remaining_edges, inputs.len())?;
                        flattened.extend_from_slice(inputs);
                    }
                    edits.push(Edit::Retain(flattened));
                    replacements.push(None);
                    continue;
                }
            },
            _ => (None, Edit::Retain(inputs)),
        };
        if let Edit::Retain(inputs) = &edit {
            reserve_edges(&mut remaining_edges, inputs.len())?;
        }
        edits.push(edit);
        replacements.push(replacement);
    }
    Ok(edits)
}

fn reserve_edges(remaining: &mut usize, count: usize) -> Result<()> {
    *remaining = remaining
        .checked_sub(count)
        .ok_or_else(|| Error("merge-sums output edge budget exceeded".into()))?;
    Ok(())
}

fn cancel(inputs: Vec<Port>, negated_input: impl Fn(Port) -> Option<Port>) -> Vec<Port> {
    let mut positive = Vec::new();
    let mut negative = Vec::new();
    let mut counts = HashMap::<Port, usize>::new();
    for port in inputs {
        if let Some(original) = negated_input(port) {
            negative.push((port, original));
        } else {
            *counts.entry(port).or_default() += 1;
            positive.push(port);
        }
    }

    // Java visits negations last and removes the first matching occurrence.
    // Keep counts separate from the vectors so hash order cannot affect output.
    let mut canceled = HashMap::<Port, usize>::new();
    let mut unmatched = Vec::new();
    for (port, original) in negative {
        match counts.get_mut(&original) {
            Some(count) if *count > 0 => {
                *count -= 1;
                *canceled.entry(original).or_default() += 1;
            }
            _ => unmatched.push(port),
        }
    }
    positive.retain(|port| match canceled.get_mut(port) {
        Some(count) if *count > 0 => {
            *count -= 1;
            false
        }
        _ => true,
    });
    positive.extend(unmatched);
    positive
}
