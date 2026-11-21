//! Replace a maximal tree of snapshot joins with one star join.
mod analysis;
mod expand;
pub use expand::{expand_star_join_layer, remove_star_joins};
use crate::{
    rebuild::{input_type, rebuild_input_closures},
    stream_rewrites::body_members,
};
pub use analysis::{find_join_trees, names_after_isolated_joins};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// The preceding analysis supplies the ordered leaves and a closure over their shared key.
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StarJoinTree {
    root: u64,
    inputs: Vec<(u64, usize)>,
    function: Value,
}

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn replace_tree(
    edit: &mut CircuitEditor,
    body: NodeId,
    root: NodeId,
    inputs: Vec<OutputPort>,
    closure: NodeId,
) -> Result<OutputPort, Error> {
    let node = edit.node(root)?;
    let join_kind = match node.operator_kind() {
        Some(Op::StreamJoin) => Op::StarJoin,
        Some(Op::StreamJoinIndex) => Op::StarJoinIndex,
        _ => return Err(Error::new("star join root must be a snapshot join")),
    };
    let result_type = node.edge("outputType")?;
    if inputs.len() < 3 || edit.node(closure)?.edges("parameters")?.len() != inputs.len() + 1 {
        return Err(Error::new(
            "star join closure requires a key and one argument for each of at least three inputs",
        ));
    }
    let fields = [
        ("outputType", Field::Reference(result_type)),
        ("function", Field::Reference(closure)),
    ];
    let mut differences = Vec::with_capacity(inputs.len());
    for input in inputs {
        let ty = input_type(edit, input)?;
        let diff = edit.append_operator(
            body,
            root,
            Op::Differentiate,
            &[input],
            [("outputType", Field::Reference(ty.into()))],
        )?;
        differences.push(output(diff));
    }
    let join = edit.append_operator(body, root, join_kind, &differences, fields)?;
    let integral = edit.append_operator(
        body,
        root,
        Op::Integrate,
        &[output(join)],
        [("outputType", Field::Reference(result_type))],
    )?;
    edit.place_before(body, differences[0].operator, root)?;
    Ok(output(integral))
}

pub fn replace_star_joins(
    circuit: Circuit,
    trees: Vec<StarJoinTree>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if trees.is_empty() {
        return Ok(circuit);
    }
    let (sites, functions): (Vec<_>, Vec<_>) = trees
        .into_iter()
        .map(|tree| ((NodeId::Outer(tree.root), tree.inputs), tree.function))
        .unzip();
    let (circuit, closures) = circuit.with_inner_roots(functions)?;
    let mut plans = BTreeMap::new();
    for ((root, ports), closure) in sites.into_iter().zip(closures) {
        let inputs = ports
            .into_iter()
            .map(|(id, number)| OutputPort {
                operator: NodeId::Outer(id),
                number,
            })
            .collect();
        if plans.insert(root, (inputs, closure)).is_some() {
            return Err(Error::new("duplicate star join root"));
        }
    }
    let members = body_members(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    for (body, root) in members {
        if let Some((inputs, closure)) = plans.remove(&root) {
            let port = replace_tree(&mut edit, body, root, inputs, closure)?;
            replacements.insert(root, port);
        }
    }
    if !plans.is_empty() {
        return Err(Error::new("star join plan contains an unknown root"));
    }
    rebuild_input_closures(edit.finish_forwarding(&replacements)?, next)
}
