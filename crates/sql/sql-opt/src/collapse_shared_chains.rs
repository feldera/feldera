//! Collapse map chains with a common upstream port before comparing their indexes.
use crate::fanout::expensive;
use crate::rebuild::rebuild_input_closures;
use feldera_sql_expr::compose_closures;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::HashMap;

fn input(circuit: &Circuit, operator: NodeId) -> Result<OutputPort, Error> {
    circuit
        .node(operator)
        .and_then(|n| n.inputs().next())
        .ok_or_else(|| Error::new("missing map chain input"))
}

pub(super) fn compose(
    edit: &mut CircuitEditor,
    before: ExpressionId,
    indexed: bool,
    after: NodeId,
) -> Result<ExpressionId, Error> {
    compose_closures(edit, before, indexed, after, None)
}

pub struct CollapsedIndex {
    pub tail: NodeId,
    pub function: ExpressionId,
    pub input: OutputPort,
}

/// Chains are ordered downstream first, as FindMapChains publishes them.
/// Read the original closures even where a chain overlaps another replacement.
pub fn collapsed_indexes(
    edit: &mut CircuitEditor,
    chains: &[Vec<NodeId>],
) -> Result<Vec<CollapsedIndex>, Error> {
    let circuit = edit.circuit();
    let mut by_source: HashMap<_, Vec<&[NodeId]>> = HashMap::new();
    for chain in chains {
        let head = *chain.last().ok_or_else(|| Error::new("empty map chain"))?;
        by_source
            .entry(input(circuit, head)?)
            .or_default()
            .push(chain);
    }
    let tails: HashMap<_, _> = by_source
        .into_values()
        .filter(|siblings| siblings.len() >= 2)
        .flatten()
        .filter(|chain| chain.len() > 1)
        .map(|chain| (chain[0], chain))
        .collect();
    if tails.is_empty() {
        return Ok(Vec::new());
    }
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut replacements = Vec::new();
    for tail in order {
        let Some(&chain) = tails.get(&tail) else {
            continue;
        };
        if edit.node(tail)?.operator_kind() != Some(Op::MapIndex) {
            continue;
        }
        let mut maps = chain.iter().rev().copied();
        let first = maps.next().expect("nontrivial chain");
        let upstream = input(edit.circuit(), first)?;
        let mut before = first;
        let mut closure = edit.expression(edit.node(first)?.edge("function")?)?;
        let mut collapsible = true;
        for after in maps {
            if expensive(edit.circuit(), closure.into())? {
                collapsible = false;
                break;
            }
            let indexed = edit.node(before)?.operator_kind() == Some(Op::MapIndex);
            let function = edit.node(after)?.edge("function")?;
            closure = compose(edit, closure, indexed, function)?;
            before = after;
        }
        if collapsible {
            replacements.push(CollapsedIndex {
                tail,
                function: closure,
                input: upstream,
            });
        }
    }
    Ok(replacements)
}

pub fn collapse_shared_chains(
    circuit: Circuit,
    chains: &[Vec<NodeId>],
    next: &mut u64,
) -> Result<Circuit, Error> {
    let mut edit = CircuitEditor::new(circuit);
    let replacements = collapsed_indexes(&mut edit, chains)?;
    for index in replacements {
        edit.set_operator_field(
            index.tail,
            "function",
            feldera_sql_ir::Field::Reference(index.function.into()),
        )?;
        edit.clear_annotations(index.tail)?;
        edit.rewire_inputs(index.tail, &[index.input])?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}
