//! Inputs to the state-boundedness analysis used for streaming diagnostics.
use crate::{recursive_views::metadata_field, trace_retention::retention_slot};
use feldera_sql_ir::{BodyOrder, Circuit, Error, Field, Node, OperatorKind as Op, OutputPort};
use std::collections::BTreeSet;

mod propagate;
pub use propagate::propagate_bounds;

use crate::{rebuild::input_type, stream_rewrites::body_members};
use feldera_sql_ir::{CircuitEditor, NodeId, OutputClocks, StreamKind, TypeId, TypeKind as Ty};
use serde::Serialize;
use std::collections::HashMap;

fn has_lateness(node: &Node, column_field: &str) -> Result<bool, Error> {
    let Some(Field::Object(metadata)) = node.field("metadata") else {
        return Err(Error::new("missing relation metadata"));
    };
    let Some(Field::Array(columns)) = metadata.get(column_field) else {
        return Err(Error::new("missing relation columns"));
    };
    for column in columns {
        let Field::Object(fields) = column else {
            return Err(Error::new("expected column metadata"));
        };
        if fields.contains_key("lateness") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn detects_streaming(circuit: &Circuit) -> Result<bool, Error> {
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("ordered operator");
        let streaming = match node.operator_kind() {
            Some(Op::SourceMultiset | Op::SourceMap | Op::SourceLazyMap | Op::ViewDeclaration) => {
                metadata_field(node, "changes")? == "AppendOnly"
                    || has_lateness(node, "columnMetadata")?
            }
            Some(Op::Sink | Op::View) => has_lateness(node, "columns")?,
            Some(Op::Window) => true,
            _ => false,
        };
        if streaming {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A retention operator bounds its first input. Its waterline input may
/// be shared by policies that retain entirely different traces.
pub fn pruned_streams(circuit: &Circuit) -> Result<BTreeSet<OutputPort>, Error> {
    let mut pruned = BTreeSet::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("ordered operator");
        if retention_slot(node.operator_kind()).is_some() {
            let input = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("retention requires a data input"))?;
            pruned.insert(input);
        }
    }
    Ok(pruned)
}

fn small_tuple_domain(edit: &CircuitEditor, tuple: TypeId) -> Result<bool, Error> {
    if !matches!(edit.type_kind(tuple)?, Ty::Tuple | Ty::RawTuple) {
        return Ok(false);
    }
    let mut values = 1;
    for field in edit.node(tuple.into())?.edges("tupFields")? {
        let ty = edit.type_id(field)?;
        let width = match edit.type_kind(ty)? {
            Ty::Bool => 2,
            Ty::Integer if edit.node(field)?.data("width").and_then(|v| v.as_u64()) == Some(8) => {
                256
            }
            _ => return Ok(false),
        };
        values *= width + i32::from(edit.nullable(ty)?);
        if values > 1024 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn finite_input(edit: &CircuitEditor, op: &Node, keys: bool) -> Result<bool, Error> {
    let stream = if let Some(ty) = op.field("domainInputType").and_then(Field::node_id) {
        edit.type_id(ty)?
    } else {
        let port = op
            .inputs()
            .next()
            .ok_or_else(|| Error::new("domain check requires an input"))?;
        let source = edit.node(port.operator)?;
        if port.number == 1 && source.operator_kind() == Some(Op::InputMapWithWaterline) {
            // This source serializes the error closure in place of the derived Z-set type.
            if keys {
                return Ok(false);
            }
            let body = edit.node(source.edge("error")?)?.edge("body")?;
            return small_tuple_domain(edit, edit.expression_type(edit.expression(body)?)?);
        }
        input_type(edit, port)?
    };
    let row = match (edit.type_kind(stream)?, keys) {
        (Ty::IndexedZSet, true) => "keyType",
        (Ty::ZSet, false) => "elementType",
        _ => return Ok(false),
    };
    small_tuple_domain(edit, edit.type_id(edit.node(stream.into())?.edge(row)?)?)
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct UnboundedOperator {
    operator: u64,
    inputs: Vec<usize>,
}

/// Identify state that can grow indefinitely, in the same order in which the
/// warning renderer visits the circuit. Recursive steps retain intermediate
/// state even for operators whose top-level state is bounded.
pub fn unbounded_operators(
    circuit: &Circuit,
    bounded: &BTreeSet<OutputPort>,
) -> Result<Vec<UnboundedOperator>, Error> {
    let parents: HashMap<_, _> = body_members(circuit)?
        .into_iter()
        .map(|(body, op)| (op, body))
        .collect();
    let collected = pruned_streams(circuit)?;
    let pruned: BTreeSet<_> = collected.iter().map(|p| p.operator).collect();
    let mut clocks = OutputClocks::new(circuit)?;
    let mut is_bounded = |p| {
        Ok::<_, Error>(
            bounded.contains(&p)
                || collected.contains(&p)
                || clocks.get(p)? == StreamKind::Waterline,
        )
    };
    let edit = CircuitEditor::new(circuit.clone());
    let mut findings = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = edit.node(id)?;
        let kind = node.operator_kind().expect("ordered operator");
        if !kind.is_stateful() || pruned.contains(&id) {
            continue;
        }
        let recursive = parents[&id] != circuit.root_id();
        let bounded_state = match kind {
            Op::Delay
            | Op::Differentiate
            | Op::Waterline
            | Op::AggregateLinearPostprocessRetainKeys
            | Op::PartitionedRollingAggregateWithWaterline => !recursive,
            Op::Window => {
                !recursive
                    && !node
                        .data("lowerUnbounded")
                        .and_then(|v| v.as_bool())
                        .ok_or_else(|| Error::new("window missing lower bound flag"))?
            }
            Op::Distinct | Op::BinaryDistinct => finite_input(&edit, node, false)?,
            Op::AggregateLinearPostprocess | Op::ChainAggregate => finite_input(&edit, node, true)?,
            _ => false,
        };
        if bounded_state {
            continue;
        }
        let mut inputs = Vec::new();
        for (i, port) in node.inputs().enumerate() {
            if !is_bounded(port)? {
                inputs.push(i);
            }
        }
        if !inputs.is_empty()
            || (node.inputs().next().is_none()
                && !is_bounded(OutputPort {
                    operator: id,
                    number: 0,
                })?)
        {
            let NodeId::Outer(operator) = id else {
                unreachable!("operator has outer identity")
            };
            findings.push(UnboundedOperator { operator, inputs });
        }
    }
    Ok(findings)
}
