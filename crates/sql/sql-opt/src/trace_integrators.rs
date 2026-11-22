//! Give integrating consumers distinct input traces before monotonicity analysis.
use crate::rebuild::{input_type, rebuild_input_closures};
use crate::recursive_views::metadata_field;
use crate::stream_rewrites::body_members;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Node, NodeId, OperatorKind as Op, OutputPort};
use std::collections::HashMap;

pub(super) fn integrates_input(kind: Op, slot: usize) -> bool {
    match kind {
        Op::AntiJoin | Op::Window => slot == 0,
        Op::Aggregate
        | Op::AsofJoin
        | Op::ConcreteAsofJoin
        | Op::Distinct
        | Op::IndexedTopK
        | Op::Integrate
        | Op::Join
        | Op::JoinFilterMap
        | Op::JoinIndex
        | Op::Lag
        | Op::LeftJoin
        | Op::LeftJoinFilterMap
        | Op::LeftJoinIndex
        | Op::PartitionedRollingAggregate
        | Op::Positive
        | Op::StarJoin
        | Op::StarJoinFilterMap
        | Op::StarJoinIndex
        | Op::StreamJoin
        | Op::StreamJoinIndex => true,
        _ => false,
    }
}

fn integrates_output(kind: Op) -> bool {
    matches!(
        kind,
        Op::AggregateLinearPostprocess
            | Op::AggregateLinearPostprocessRetainKeys
            | Op::Aggregate
            | Op::ChainAggregate
            | Op::IndexedTopK
            | Op::Integrate
            | Op::Lag
            | Op::PartitionedRollingAggregate
            | Op::PartitionedRollingAggregateWithWaterline
            | Op::Rank
            | Op::RowNumber
    )
}

/// StrayGC accepts both simple operators with internal integrators and
/// multi-output operators, whose state ownership it cannot determine.
pub(super) fn can_collect_input(kind: Op) -> bool {
    integrates_input(kind, 0)
        || integrates_output(kind)
        || matches!(
            kind,
            Op::BinaryDistinct
                | Op::DelayedIntegral
                | Op::PrimitiveAggregate
                | Op::SourceMap
                | Op::SourceLazyMap
                | Op::UpsertFeedback
                | Op::Nested
                | Op::ControlledKeyFilter
                | Op::InputMapWithWaterline
        )
}

fn stores_output(node: &Node) -> Result<bool, Error> {
    match node.operator_kind() {
        Some(Op::SourceMap | Op::SourceLazyMap | Op::SourceMultiset) => {
            metadata_field(node, "materialized")?
                .as_bool()
                .ok_or_else(|| Error::new("table materialization flag must be boolean"))
        }
        Some(kind) => Ok(integrates_output(kind)),
        None => Err(Error::new("expected a trace producer")),
    }
}

struct Consumer {
    body: NodeId,
    reader: NodeId,
    slots: Vec<usize>,
}

fn shared_traces(circuit: &Circuit) -> Result<Vec<Consumer>, Error> {
    let mut users = HashMap::<NodeId, usize>::new();
    let mut consumers = Vec::new();
    for (body, reader) in body_members(circuit)? {
        let op = circuit.node(reader).expect("validated operator");
        let kind = op.operator_kind().expect("body contains operators");
        let materialized = kind == Op::Sink && metadata_field(op, "viewKind")? == "MATERIALIZED";
        let mut slots = Vec::new();
        for (slot, port) in op.inputs().enumerate() {
            if materialized || integrates_input(kind, slot) {
                *users.entry(port.operator).or_default() += 1;
                slots.push(slot);
            }
        }
        if !slots.is_empty() {
            consumers.push(Consumer {
                body,
                reader,
                slots,
            });
        }
    }
    let mut shared = Vec::new();
    for mut consumer in consumers {
        let inputs: Vec<_> = circuit
            .node(consumer.reader)
            .expect("validated reader")
            .inputs()
            .collect();
        let mut slots = Vec::new();
        for slot in consumer.slots {
            let source = inputs[slot].operator;
            if users[&source] > 1 || stores_output(circuit.node(source).expect("validated source"))?
            {
                slots.push(slot);
            }
        }
        if !slots.is_empty() {
            consumer.slots = slots;
            shared.push(consumer);
        }
    }
    Ok(shared)
}

pub(super) fn needs_separation(circuit: &Circuit) -> Result<bool, Error> {
    Ok(!shared_traces(circuit)?.is_empty())
}

/// Input slots where the live compiler must insert a no-op to isolate a trace.
pub fn separate_trace_inputs(circuit: &Circuit) -> Result<Vec<(NodeId, Vec<usize>)>, Error> {
    Ok(shared_traces(circuit)?
        .into_iter()
        .map(|consumer| (consumer.reader, consumer.slots))
        .collect())
}

/// The original visitor rejects the first snapshot aggregate in constructor order.
pub fn snapshot_aggregate(circuit: &Circuit) -> Result<Option<NodeId>, Error> {
    Ok(circuit
        .operator_order(feldera_sql_ir::BodyOrder::Postorder)?
        .into_iter()
        .find(|&id| circuit.node(id).and_then(Node::operator_kind) == Some(Op::StreamAggregate)))
}

pub fn separate_integrators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    if snapshot_aggregate(&circuit)?.is_some() {
        return Err(Error::new(
            "StreamAggregate operator should have been removed",
        ));
    }
    let consumers = shared_traces(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    for consumer in consumers {
        let mut inputs: Vec<_> = edit.node(consumer.reader)?.inputs().collect();
        for slot in consumer.slots {
            let input = inputs[slot];
            let ty = input_type(&edit, input)?;
            let noop = edit.insert_trace_noop(consumer.body, consumer.reader, input, ty)?;
            inputs[slot] = OutputPort {
                operator: noop,
                number: 0,
            };
        }
        edit.rewire_inputs(consumer.reader, &inputs)?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_integrating_slot_gets_its_own_trace() -> Result<(), Error> {
        let port = |id| json!({"operator": {"node": id}, "outputNumber": 0});
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [],
                 "metadata": {"materialized": false}},
                {"class": "DBSPSourceMultisetOperator", "id": 2, "inputs": [],
                 "metadata": {"materialized": true}},
                {"class": "DBSPJoinOperator", "id": 3, "inputs": [port(1), port(1)]},
                {"class": "DBSPWindowOperator", "id": 4, "inputs": [port(2), port(1)]},
                {"class": "DBSPDistinctOperator", "id": 5, "inputs": [port(3)]}
            ]
        }))?;
        assert_eq!(
            separate_trace_inputs(&graph)?,
            [(NodeId::Outer(3), vec![0, 1]), (NodeId::Outer(4), vec![0])]
        );
        Ok(())
    }
}
