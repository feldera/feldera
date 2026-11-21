//! Fuse runs of unary maps, indexed maps and filters with a single downstream reader.
mod inline;
mod lower;
pub use lower::implement_chains;

use crate::rebuild::{input_type, rebuild_input_closures};
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, InnerKind, Node, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, PartialEq)]
enum Computation {
    Map,
    MapIndex,
    Filter,
}

impl Computation {
    fn name(self) -> &'static str {
        match self {
            Self::Map => "Map",
            Self::MapIndex => "MapIndex",
            Self::Filter => "Filter",
        }
    }

    fn read(name: &Field) -> Result<Self, Error> {
        match name {
            Field::Data(v) if v == "Map" => Ok(Self::Map),
            Field::Data(v) if v == "MapIndex" => Ok(Self::MapIndex),
            Field::Data(v) if v == "Filter" => Ok(Self::Filter),
            _ => Err(Error::new("unknown chain computation")),
        }
    }
}

fn barrier(n: &Node) -> bool {
    n.data("annotations")
        .and_then(|v| v.as_array())
        .is_some_and(|items| items.iter().any(|a| a["class"] == "NoChain"))
}

/// The topology projection omits closure bodies. A candidate map still needs
/// the full snapshot to establish whether its function is a closure.
pub(super) fn has_chain_pattern(circuit: &Circuit) -> Result<bool, Error> {
    let members = body_members(circuit)?;
    let readers = reader_counts(circuit, &members);
    let stage = |op: &Node| {
        matches!(
            op.operator_kind(),
            Some(Op::Map | Op::MapIndex | Op::Filter)
        ) && !barrier(op)
    };
    for (_, id) in members {
        let op = circuit.node(id).expect("validated member");
        if stage(op) {
            let port = unary_input(op)?;
            let input = circuit.node(port.operator).expect("validated producer");
            if readers.get(&port.operator) == Some(&1)
                && (input.operator_kind() == Some(Op::Chain) || stage(input))
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn computation(edit: &CircuitEditor, op: &Node) -> Result<Option<(Computation, NodeId)>, Error> {
    let kind = match op.operator_kind() {
        Some(Op::Map) => Computation::Map,
        Some(Op::MapIndex) => Computation::MapIndex,
        Some(Op::Filter) => Computation::Filter,
        _ => return Ok(None),
    };
    let closure = op.edge("function")?;
    if !matches!(
        edit.node(closure)?.inner_kind(),
        Some(InnerKind::Expression(ExpressionKind::Closure))
    ) {
        if kind == Computation::Map {
            return Ok(None);
        }
        return Err(Error::new("expected a closure in a filter or indexed map"));
    }
    if edit.node(closure)?.edges("parameters")?.len() != 1 {
        return Err(Error::new("chain computation requires a unary closure"));
    }
    Ok(Some((kind, closure)))
}

fn chain_prefix(edit: &CircuitEditor, op: &Node) -> Result<Vec<(Computation, NodeId)>, Error> {
    if op.operator_kind() == Some(Op::Chain) {
        let functions = op.edges("chain")?;
        let Some(Field::Array(kinds)) = op.field("kinds") else {
            return Err(Error::new("missing chain computations"));
        };
        if functions.len() != kinds.len() {
            return Err(Error::new("inconsistent chain computations and closures"));
        }
        return kinds
            .iter()
            .zip(functions)
            .map(|(kind, f)| Ok((Computation::read(kind)?, f)))
            .collect();
    }
    if barrier(op) {
        return Ok(Vec::new());
    }
    Ok(computation(edit, op)?.into_iter().collect())
}

fn chain_output(
    edit: &mut CircuitEditor,
    stages: &[(Computation, NodeId)],
    input: TypeId,
) -> Result<TypeId, Error> {
    let Some(&(kind, closure)) = stages
        .iter()
        .rev()
        .find(|(kind, _)| *kind != Computation::Filter)
    else {
        return Ok(input);
    };
    let function_type = edit.node(closure)?.edge("type")?;
    let row = edit.type_id(edit.node(function_type)?.edge("resultType")?)?;
    if kind == Computation::Map {
        return edit.zset_type(row);
    }
    edit.indexed_zset_from_pair(row)
}

fn unary_input(op: &Node) -> Result<OutputPort, Error> {
    let mut inputs = op.inputs();
    inputs
        .next()
        .filter(|_| inputs.next().is_none())
        .ok_or_else(|| Error::new("chain computation requires a single input"))
}

fn fuse_once(circuit: Circuit, next: &mut u64) -> Result<(Circuit, bool), Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let original = circuit.clone();
    // Many programs have no chains. Read provenance only at the first fusion.
    let mut origins = None;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::<NodeId, OutputPort>::new();
    for (body, id) in members {
        let node = edit.node(id)?;
        if barrier(node) {
            continue;
        }
        let Some(stage) = computation(&edit, node)? else {
            continue;
        };
        let link = unary_input(node)?;
        if readers.get(&link.operator) != Some(&1) {
            continue;
        }
        let prior = replacements.get(&link.operator).copied().unwrap_or(link);
        let predecessor = edit.node(prior.operator)?;
        let mut stages = chain_prefix(&edit, predecessor)?;
        if stages.is_empty() {
            continue;
        }
        // Extending a chain preserves the input type recorded at its creation.
        // A new chain resolves this field after upstream operators are rebuilt.
        let prefix_input = predecessor
            .field("inputType")
            .cloned()
            .filter(|_| predecessor.operator_kind() == Some(Op::Chain))
            .unwrap_or(Field::Data(serde_json::Value::Null));
        let base = unary_input(predecessor)?;
        let base = replacements.get(&base.operator).copied().unwrap_or(base);
        stages.push(stage);
        let input = match prefix_input.node_id() {
            Some(id) => edit.type_id(id)?,
            None => input_type(&edit, base)?,
        };
        let output = chain_output(&mut edit, &stages, input)?;
        let new = edit.append_operator(
            body,
            id,
            Op::Chain,
            &[base],
            [
                ("outputType", Field::Reference(output.into())),
                ("inputType", prefix_input),
                (
                    "chain",
                    Field::Array(stages.iter().map(|(_, f)| Field::Reference(*f)).collect()),
                ),
                (
                    "kinds",
                    Field::Array(
                        stages
                            .iter()
                            .map(|(k, _)| Field::Data(k.name().into()))
                            .collect(),
                    ),
                ),
            ],
        )?;
        let table = match &mut origins {
            Some(table) => table,
            None => origins.insert(original.provenance()?),
        };
        let origin = table.fuse_origins(new, prior.operator, id)?;
        edit.set_intermediate_origin(new, origin)?;
        edit.place_before(body, new, id)?;
        replacements.insert(
            id,
            OutputPort {
                operator: new,
                number: 0,
            },
        );
    }
    let changed = !replacements.is_empty();
    let result = rebuild_input_closures(edit.finish_forwarding(&replacements)?, next)?;
    Ok((result, changed))
}

pub fn fuse_chains(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    Ok(fuse_once(circuit, next)?.0)
}

pub fn optimize_chains(mut circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    loop {
        let before = circuit.operator_order(BodyOrder::Preorder)?.len();
        let (fused, changed) = fuse_once(circuit, next)?;
        circuit = crate::remove_dead_operators(fused, next)?;
        let after = circuit.operator_order(BodyOrder::Preorder)?.len();
        if !changed && before == after {
            return Ok(circuit);
        }
        if after >= before {
            return Err(Error::new(
                "chain fusion failed to reduce the number of operators",
            ));
        }
    }
}
