use std::collections::HashMap;

use crate::model::{Circuit, Edit, OperatorId, OperatorKind, Port};

#[derive(Clone, Copy)]
pub(crate) enum Retention {
    Keys = 1,
    Values = 2,
}

pub(crate) fn remove_identity_maps(
    circuit: &Circuit,
    identity: &[bool],
    retention: &[Option<Retention>],
) -> Vec<Edit> {
    let mut claimed = HashMap::<Port, u8>::new();
    for (operator, kind) in circuit.operators.iter().zip(retention) {
        if let Some(kind) = kind {
            // Only the collection input carries retention. The second input
            // supplies its waterline.
            *claimed.entry(operator.inputs[0]).or_default() |= *kind as u8;
        }
    }
    let mut aliases: Vec<Option<Port>> = vec![None; circuit.operators.len()];
    circuit
        .operators
        .iter()
        .enumerate()
        .map(|(index, operator)| {
            let inputs: Vec<_> = operator
                .inputs
                .iter()
                .map(|port| aliases[port.operator.0].unwrap_or(*port))
                .collect();
            if !identity[index] {
                return Edit::Retain(inputs);
            }
            let output = Port {
                operator: OperatorId(index),
                output: 0,
            };
            let moving = claimed.get(&output).copied().unwrap_or_default();
            // The Java decision is based on original ports. Earlier sibling
            // candidates can claim additional dimensions on this input.
            if moving != 0 {
                let taken = claimed.entry(operator.inputs[0]).or_default();
                if *taken & moving != 0 {
                    return Edit::ReplaceIdentity(inputs[0]);
                }
                *taken |= moving;
            }
            aliases[index] = Some(inputs[0]);
            Edit::Bypass
        })
        .collect()
}

pub(crate) fn bypass_views(circuit: &Circuit, all: bool) -> Vec<Edit> {
    bypass_unary(
        circuit,
        |_, kind| matches!(kind, OperatorKind::View { preserve } if all || !preserve),
    )
}

pub(crate) fn bypass_noops(circuit: &Circuit) -> Vec<Edit> {
    let mut feeds_retention = vec![false; circuit.operators.len()];
    for operator in &circuit.operators {
        if matches!(operator.kind, OperatorKind::RetainTrace) {
            for port in &operator.inputs {
                feeds_retention[port.operator.0] = true;
            }
        }
    }
    bypass_unary(circuit, |index, kind| {
        matches!(kind, OperatorKind::Noop) && !feeds_retention[index]
    })
}

fn bypass_unary(
    circuit: &Circuit,
    should_bypass: impl Fn(usize, &OperatorKind) -> bool,
) -> Vec<Edit> {
    // Bypassed operators have one output. Every input port names an earlier
    // operator, so a replacement already points directly to a retained one.
    let mut replacements: Vec<Option<Port>> = Vec::with_capacity(circuit.operators.len());
    let mut edits = Vec::with_capacity(circuit.operators.len());
    for (index, operator) in circuit.operators.iter().enumerate() {
        let resolved = operator
            .inputs
            .iter()
            .map(|port| replacements[port.operator.0].unwrap_or(*port))
            .collect::<Vec<_>>();
        if should_bypass(index, &operator.kind) {
            replacements.push(Some(resolved[0]));
            edits.push(Edit::Bypass);
        } else {
            replacements.push(None);
            edits.push(Edit::Retain(resolved));
        }
    }
    edits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Operator;

    #[test]
    fn identity_maps_claim_retention_dimensions_independently() {
        let port = |index| Port {
            operator: OperatorId(index),
            output: 0,
        };
        let source = Operator {
            inputs: vec![],
            kind: OperatorKind::Now,
        };
        let operators = [0, 0, 0].into_iter().map(|input| Operator {
            inputs: vec![port(input)],
            kind: OperatorKind::Noop,
        });
        let retention = [1, 2, 3].into_iter().map(|input| Operator {
            inputs: vec![port(input), port(0)],
            kind: OperatorKind::RetainTrace,
        });
        let circuit = Circuit {
            operators: std::iter::once(source)
                .chain(operators)
                .chain(retention)
                .collect(),
        };
        let edits = remove_identity_maps(
            &circuit,
            &[false, true, true, true, false, false, false],
            &[
                None,
                None,
                None,
                None,
                Some(Retention::Keys),
                Some(Retention::Values),
                Some(Retention::Values),
            ],
        );
        assert!(matches!(
            edits[1..4],
            [Edit::Bypass, Edit::Bypass, Edit::ReplaceIdentity(input)] if input == port(0)
        ));
        assert!(matches!(&edits[6], Edit::Retain(inputs) if inputs == &[port(3), port(0)]));
    }
}
