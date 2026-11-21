//! Find the input on which temporal filters can share a window index.
use crate::{
    function_calls::contains_now,
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, InnerKind, Node, NodeId,
    OperatorKind, OutputPort, TypeKind,
};
use std::collections::HashMap;

pub struct TemporalFilterSource {
    pub filter: NodeId,
    pub source: OutputPort,
    /// Unary maps in composition order, starting at the filter input.
    pub maps: Vec<NodeId>,
}

fn unary_closure(edit: &CircuitEditor, op: &Node) -> Result<Option<(NodeId, NodeId)>, Error> {
    let Some(function) = op.field("function").and_then(Field::node_id) else {
        return Ok(None);
    };
    let closure = edit.node(function)?;
    if !matches!(
        closure.inner_kind(),
        Some(InnerKind::Expression(ExpressionKind::Closure))
    ) {
        return Ok(None);
    }
    Ok(match closure.edges("parameters")?.as_slice() {
        [parameter] => Some((function, edit.node(*parameter)?.edge("type")?)),
        _ => None,
    })
}

fn source_path(
    edit: &CircuitEditor,
    filter: NodeId,
    readers: &HashMap<NodeId, usize>,
) -> Result<Option<TemporalFilterSource>, Error> {
    let node = edit.node(filter)?;
    let Some((predicate, mut parameter)) = unary_closure(edit, node)? else {
        return Ok(None);
    };
    if !contains_now(edit.circuit(), &[predicate])? {
        return Ok(None);
    }
    let input = |node: &Node| {
        node.inputs()
            .next()
            .ok_or_else(|| Error::new("expected a unary input"))
    };
    let mut source = input(node)?;
    let mut maps = Vec::new();
    loop {
        let map = edit.node(source.operator)?;
        if !matches!(
            map.operator_kind(),
            Some(OperatorKind::Map | OperatorKind::MapIndex)
        ) || readers.get(&source.operator) != Some(&1)
        {
            break;
        }
        let Some((function, next_parameter)) = unary_closure(edit, map)? else {
            return Ok(None);
        };
        let parameter_type = edit.type_id(parameter)?;
        let signature = edit.node(function)?.edge("type")?;
        let result = edit.type_id(edit.node(signature)?.edge("resultType")?)?;
        if edit.type_kind(parameter_type)? != TypeKind::Ref
            || edit.nullable(parameter_type)?
            || !edit.same_type(edit.type_id(edit.node(parameter)?.edge("type")?)?, result)?
        {
            return Ok(None);
        }
        maps.push(source.operator);
        source = input(map)?;
        parameter = next_parameter;
    }
    Ok(Some(TemporalFilterSource {
        filter,
        source,
        maps,
    }))
}

pub fn temporal_filter_sources(circuit: Circuit) -> Result<Vec<TemporalFilterSource>, Error> {
    let readers = reader_counts(&circuit, &body_members(&circuit)?);
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let edit = CircuitEditor::new(circuit);
    let mut sources = Vec::new();
    for id in order {
        if edit.node(id)?.operator_kind() != Some(OperatorKind::Filter) {
            continue;
        }
        if let Some(path) = source_path(&edit, id, &readers)? {
            sources.push(path);
        }
    }
    Ok(sources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn source_stops_at_fanout_and_rejects_incompatible_map_results() -> Result<(), Error> {
        let port = |id| json!({"operator":{"node":id},"outputNumber":0});
        let row = json!({"class":"DBSPTypeInteger","id":10,"width":32,"signed":true,"code":"INT32","mayBeNull":false});
        let predicate = json!({
            "class":"DBSPClosureExpression","id":20,
            "parameters":[{"class":"DBSPParameter","id":21,"name":"r","type":{
                "class":"DBSPTypeRef","id":22,"code":"REF","mayBeNull":false,"type":row}}],
            "body":{"class":"DBSPApplyExpression","id":23,"arguments":[],"function":{
                "class":"DBSPPathExpression","id":24,"path":{"class":"DBSPPath","id":25,
                "components":[{"class":"DBSPSimplePathSegment","id":26,"identifier":"now","genericArgs":[]}]}}}
        });
        let map = |id, signature, result| {
            json!({
                "class":"DBSPClosureExpression","id":id,"parameters":[{"node":21}],
                "type":{"class":"DBSPTypeFunction","id":signature,"resultType":result}
            })
        };
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPFilterOperator","id":2,"function":predicate,"inputs":[port(1)]},
                {"class":"DBSPMapOperator","id":3,"function":map(30,31,json!({"node":10})),"inputs":[port(2)]},
                {"class":"DBSPMapOperator","id":4,"function":{"node":30},"inputs":[port(3)]},
                {"class":"DBSPFilterOperator","id":5,"function":{"node":20},"inputs":[port(4)]},
                {"class":"DBSPFilterOperator","id":6,"function":{"node":20},"inputs":[port(3)]},
                {"class":"DBSPMapIndexOperator","id":7,"function":map(40,41,json!({
                    "class":"DBSPTypeBool","id":42,"mayBeNull":false,"code":"BOOL"
                })),"inputs":[port(2)]},
                {"class":"DBSPFilterOperator","id":8,"function":{"node":20},"inputs":[port(7)]}
            ]
        }))?;
        let paths = temporal_filter_sources(circuit)?;
        let paths: Vec<_> = paths
            .into_iter()
            .map(|p| (p.filter, p.source.operator, p.maps))
            .collect();
        use NodeId::Outer;
        assert_eq!(
            paths,
            [
                (Outer(2), Outer(1), vec![]),
                (Outer(5), Outer(3), vec![Outer(4)]),
                (Outer(6), Outer(3), vec![]),
            ]
        );
        Ok(())
    }
}
