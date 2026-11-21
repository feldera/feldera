//! Simplify differentiation and integration in flat circuits.

use crate::model::{Circuit, CollectionKind, Edit, OperatorKind, Port};

pub(crate) fn remove_redundant(circuit: &Circuit) -> Vec<Edit> {
    let mut aliases = Vec::<Option<Port>>::with_capacity(circuit.operators.len());
    let mut edits = Vec::with_capacity(circuit.operators.len());

    for operator in &circuit.operators {
        let inputs: Vec<_> = operator
            .inputs
            .iter()
            .map(|port| aliases[port.operator.0].unwrap_or(*port))
            .collect();
        let mut alias = None;
        let edit = if matches!(operator.kind, OperatorKind::Integrate) {
            let source = inputs[0];
            let producer = source.operator.0;
            match (&circuit.operators[producer].kind, &edits[producer]) {
                (OperatorKind::Differentiate, Edit::Retain(inputs)) => {
                    alias = Some(inputs[0]);
                    Edit::Bypass
                }
                (
                    OperatorKind::Constant {
                        stream: CollectionKind::Delta,
                        ..
                    },
                    _,
                ) => Edit::SnapshotConstant(source),
                _ => Edit::Retain(inputs),
            }
        } else {
            Edit::Retain(inputs)
        };
        aliases.push(alias);
        edits.push(edit);
    }
    edits
}

pub(crate) fn push_differentials(circuit: &Circuit) -> Vec<Edit> {
    let mut fanout = vec![0usize; circuit.operators.len()];
    for operator in &circuit.operators {
        for input in &operator.inputs {
            fanout[input.operator.0] += 1;
        }
    }
    circuit
        .operators
        .iter()
        .map(|operator| {
            if matches!(operator.kind, OperatorKind::Differentiate) {
                let source = operator.inputs[0].operator;
                let map = &circuit.operators[source.0];
                if fanout[source.0] == 1 && matches!(map.kind, OperatorKind::Map(_)) {
                    return Edit::MoveDifferentiate {
                        map: source,
                        input: map.inputs[0],
                    };
                }
            }
            Edit::Retain(operator.inputs.clone())
        })
        .collect()
}
