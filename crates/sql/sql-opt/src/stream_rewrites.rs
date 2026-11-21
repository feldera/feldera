//! Scheduled stream rewrites that insert operators in their enclosing circuit body.
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort, TypeId,
};
use std::collections::HashMap;

/// Original body slots in constructor order. Graph edits can append new operators.
pub(super) fn body_members(circuit: &Circuit) -> Result<Vec<(NodeId, NodeId)>, Error> {
    let mut members = Vec::new();
    for body in
        std::iter::once(circuit.root_id()).chain(circuit.operator_order(BodyOrder::Preorder)?)
    {
        let node = circuit.node(body).expect("validated body member");
        if node.field("allOperators").is_some() {
            members.extend(node.edges("allOperators")?.into_iter().map(|id| (body, id)));
        }
    }
    Ok(members)
}

/// Append a projection whose closure result determines its output stream type.
pub(super) fn append_map(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    kind: Op,
    input: OutputPort,
    function: ExpressionId,
) -> Result<(NodeId, TypeId), Error> {
    let result = edit.expression(edit.node(function.into())?.edge("body")?)?;
    let ty = edit.expression_type(result)?;
    let stream = match kind {
        Op::Map => edit.zset_type(ty)?,
        Op::MapIndex => edit.indexed_zset_from_pair(ty)?,
        _ => {
            return Err(Error::new(
                "projection requires a map or map-index operator",
            ));
        }
    };
    // The Java decoder resolves outputType before the closure that shares its result type.
    let id = edit.append_operator(
        body,
        site,
        kind,
        &[input],
        [
            ("outputType", Field::Reference(stream.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    Ok((id, stream))
}

pub fn insert_weight_validation(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let members = body_members(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    for (body, id) in members {
        if edit.node(id)?.operator_kind() == Some(Op::SourceMultiset) {
            edit.validate_source_weights(body, id)?;
        }
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

/// Lower the snapshot join forms that have only incremental implementations in DBSP.
pub fn implement_joins(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let sites: Vec<_> = body_members(&circuit)?
        .into_iter()
        .filter(|&(_, id)| {
            matches!(
                circuit.node(id).and_then(|n| n.operator_kind()),
                Some(Op::StreamAntiJoin | Op::StreamJoinIndex)
            )
        })
        .collect();
    if sites.is_empty() {
        return Ok(circuit);
    }
    let mut origins = circuit.intermediate_origins()?;
    let sites = sites
        .into_iter()
        .map(|(body, id)| Ok((body, id, origins.derive(id)?)))
        .collect::<Result<Vec<_>, Error>>()?;
    let mut edit = CircuitEditor::new(circuit);
    for (body, id, origins) in sites {
        let ports: Vec<_> = edit.node(id)?.inputs().collect();
        let [left, right] = ports.as_slice() else {
            return Err(Error::new("snapshot join requires two inputs"));
        };
        let types = [input_type(&edit, *left)?, input_type(&edit, *right)?];
        edit.expand_snapshot_join(body, id, types, origins)?;
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

pub(super) fn reader_counts(
    circuit: &Circuit,
    members: &[(NodeId, NodeId)],
) -> HashMap<NodeId, usize> {
    let mut readers = HashMap::<NodeId, usize>::new();
    for &(_, id) in members {
        let node = circuit.node(id).expect("body member");
        // Graph counts the recursive region's inputs once at its outer boundary.
        if node.operator_kind() != Some(Op::Delta) {
            for input in node.inputs() {
                *readers.entry(input.operator).or_default() += 1;
            }
        }
    }
    readers
}

fn push_once(circuit: Circuit, next: &mut u64) -> Result<(Circuit, bool), Error> {
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let mut edit = CircuitEditor::new(circuit);
    let mut changed = false;
    for (body, id) in members {
        let node = edit.node(id)?;
        if node.operator_kind() != Some(Op::Differentiate) {
            continue;
        }
        let map = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("differentiator requires an input"))?
            .operator;
        let source = edit.node(map)?;
        if readers.get(&map) == Some(&1)
            && matches!(source.operator_kind(), Some(Op::Map | Op::MapIndex))
        {
            let input = source
                .inputs()
                .next()
                .ok_or_else(|| Error::new("map requires an input"))?;
            let ty = input_type(&edit, input)?;
            edit.differentiate_before_map(body, id, map, ty)?;
            changed = true;
        }
    }
    Ok((
        rebuild_input_closures(edit.finish_with_rewrites()?, next)?,
        changed,
    ))
}

/// A single PushDifferentialsUp invocation retains the original map.
pub fn push_differentials_up(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    Ok(push_once(circuit, next)?.0)
}

/// The top-level placement also eliminates dead operators after each iteration.
pub fn optimize_differentials(mut circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut size = circuit.operator_order(BodyOrder::Preorder)?.len();
    loop {
        let (pushed, changed) = push_once(circuit, next)?;
        circuit = crate::remove_dead_operators(pushed, next)?;
        let remaining = circuit.operator_order(BodyOrder::Preorder)?.len();
        if !changed && remaining == size {
            return Ok(circuit);
        }
        size = remaining;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn map_defines_its_shared_result_type_before_the_closure() {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [
                {"class": "DBSPSourceMultisetOperator", "id": 1, "inputs": [], "outputType":
                    {"class": "DBSPTypeZSet", "id": 1, "elementType":
                        {"class": "DBSPTypeInteger", "id": 2, "code": "INT32", "mayBeNull": false}}}
            ]
        }))
        .unwrap();
        let body = circuit.root_id();
        let source = NodeId::Outer(1);
        let input = OutputPort {
            operator: source,
            number: 0,
        };
        let mut edit = CircuitEditor::new(circuit);
        let integer = edit.type_id(NodeId::Inner(2)).unwrap();
        let reference = edit.reference_type(integer).unwrap();
        let variable = edit.variable(reference, &mut 0).unwrap();
        let value = edit.dereference(variable).unwrap();
        let row = edit.tuple_expr(variable.into(), false, &[value]).unwrap();
        let function = edit.closure_from_variables(row, &[variable]).unwrap();
        append_map(&mut edit, body, source, Op::Map, input, function).unwrap();

        let wire = edit.finish_with_rewrites().unwrap().circuit.to_json();
        let map = &wire["allOperators"][1];
        let result_type = &map["outputType"]["elementType"];
        assert_eq!(result_type["class"], "DBSPTypeTuple");
        let closure_type = &map["function"]["type"];
        assert_eq!(closure_type["resultType"]["node"], result_type["id"]);
    }
}
