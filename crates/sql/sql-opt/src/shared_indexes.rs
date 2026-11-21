//! Give each join input its own copy of a shared MapIndex before widening indexes.
use crate::rebuild::rebuild_input_closures;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort,
};

struct JoinInputs {
    body: NodeId,
    join: NodeId,
    shared_slots: Vec<usize>,
}

fn is_indexed_join(kind: Option<Op>) -> bool {
    matches!(
        kind,
        Some(
            Op::Join
                | Op::JoinIndex
                | Op::JoinFilterMap
                | Op::LeftJoin
                | Op::LeftJoinIndex
                | Op::LeftJoinFilterMap
                | Op::StreamJoin
                | Op::StreamJoinIndex
                | Op::StarJoin
                | Op::StarJoinIndex
                | Op::StarJoinFilterMap
        )
    )
}

fn shares_input_integral(node: &Node, slot: usize) -> Result<bool, Error> {
    let kind = node.operator_kind();
    Ok(match kind {
        Some(Op::Window) => slot == 0,
        Some(Op::Sink) => {
            crate::recursive_views::metadata_field(node, "viewKind")? == "MATERIALIZED"
        }
        Some(
            Op::AntiJoin
            | Op::PartitionedRollingAggregate
            | Op::Aggregate
            | Op::StreamAggregate
            | Op::Distinct
            | Op::IndexedTopK
            | Op::Lag,
        ) => true,
        _ => is_indexed_join(kind),
    })
}

/// Follow each shareable input back to its first non-projection producer.
/// Preserve one chain per input, in visitor order, including repeated inputs.
pub fn find_map_chains(circuit: &Circuit) -> Result<Vec<Vec<NodeId>>, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let limit = order.len();
    let mut chains = Vec::new();
    for id in order {
        let reader = circuit.node(id).expect("validated operator");
        for (slot, mut port) in reader.inputs().enumerate() {
            if !shares_input_integral(reader, slot)? {
                continue;
            }
            let mut chain = Vec::new();
            loop {
                let producer = circuit.node(port.operator).expect("validated input");
                if !matches!(producer.operator_kind(), Some(Op::Map | Op::MapIndex)) {
                    break;
                }
                if chain.len() == limit {
                    return Err(Error::new("cycle in map chain"));
                }
                chain.push(port.operator);
                port = producer
                    .inputs()
                    .next()
                    .ok_or_else(|| Error::new("map chain requires a unary projection"))?;
            }
            if !chain.is_empty() {
                chains.push(chain);
            }
        }
    }
    Ok(chains)
}

fn shared_inputs(circuit: &Circuit) -> Result<Vec<JoinInputs>, Error> {
    let members = body_members(circuit)?;
    let readers = reader_counts(circuit, &members);
    let mut sites = Vec::new();
    for (body, join) in members {
        let node = circuit.node(join).expect("validated operator");
        if !is_indexed_join(node.operator_kind()) {
            continue;
        }
        let shared_slots: Vec<_> = node
            .inputs()
            .enumerate()
            .filter_map(|(slot, port)| {
                let source = circuit.node(port.operator).expect("validated input");
                (source.operator_kind() == Some(Op::MapIndex) && readers[&port.operator] > 1)
                    .then_some(slot)
            })
            .collect();
        if !shared_slots.is_empty() {
            sites.push(JoinInputs {
                body,
                join,
                shared_slots,
            });
        }
    }
    Ok(sites)
}

pub(super) fn has_shared_join_indexes(circuit: &Circuit) -> Result<bool, Error> {
    Ok(!shared_inputs(circuit)?.is_empty())
}

/// Count readers on entry. Two inputs of the same join that read the same
/// index receive separate copies in input order, just like two separate joins.
pub fn duplicate_shared_indexes(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let sites = shared_inputs(&circuit)?;
    if sites.is_empty() {
        return Ok(circuit);
    }
    let mut edit = CircuitEditor::new(circuit);
    for site in sites {
        let mut inputs: Vec<_> = edit.node(site.join)?.inputs().collect();
        for slot in site.shared_slots {
            let index = inputs[slot].operator;
            let source = edit.node(index)?;
            let upstream = source
                .inputs()
                .next()
                .ok_or_else(|| Error::new("MapIndex requires an input"))?;
            let fields = [
                ("outputType", Field::Reference(source.edge("outputType")?)),
                ("function", Field::Reference(source.edge("function")?)),
            ];
            let copy = edit.append_operator(site.body, index, Op::MapIndex, &[upstream], fields)?;
            edit.place_before(site.body, copy, site.join)?;
            inputs[slot] = OutputPort {
                operator: copy,
                number: 0,
            };
        }
        edit.rewire_inputs(site.join, &inputs)?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}
