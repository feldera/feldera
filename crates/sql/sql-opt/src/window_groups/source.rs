//! Find the input on which temporal filters can share a window index.
use crate::{
    function_calls::contains_now,
    projection_composition::compose_projection,
    stream_rewrites::{body_members, reader_counts},
};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, Node,
    NodeId, OperatorKind, OutputPort, TypeKind,
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
    filter_sources(&CircuitEditor::new(circuit))
}

pub(super) fn filter_sources(edit: &CircuitEditor) -> Result<Vec<TemporalFilterSource>, Error> {
    let circuit = edit.circuit();
    let readers = reader_counts(circuit, &body_members(circuit)?);
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut sources = Vec::new();
    for id in order {
        if edit.node(id)?.operator_kind() != Some(OperatorKind::Filter) {
            continue;
        }
        if let Some(path) = source_path(edit, id, &readers)? {
            sources.push(path);
        }
    }
    Ok(sources)
}

/// Express the timestamp in terms of the stream at the start of the map chain.
pub fn compose_timestamp(
    edit: &mut CircuitEditor,
    mut timestamp: ExpressionId,
    maps: &[NodeId],
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    for &map in maps {
        if let Some(composed) = compose_projection(edit, map, timestamp.into(), true, warnings)
            .map_err(|error| Error::new(error.to_string()))? {
            timestamp = composed;
            continue;
        }

        // Bind an expensive map once when the timestamp reads its result more than once.
        let reader = edit.node(timestamp.into())?;
        let [parameter] = reader
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("timestamp closure must have one parameter"))?;
        let body = edit.expression(reader.edge("body")?)?;
        let parameter = edit.node(parameter)?;
        let ty = edit.type_id(parameter.edge("type")?)?;
        let name = parameter
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("timestamp parameter has no name"))?
            .to_owned();
        let variable = edit.named_variable(ty, &name)?;
        let map_body = edit.expression(edit.node(map)?.edge("body")?)?;
        let value = edit.borrow_value(map_body)?;
        let bound = edit.let_expression(variable, value, body)?;
        let parameters = edit.node(map)?.edges("parameters")?;
        timestamp = edit.closure_with_parameters(bound, &parameters)?;
    }
    Ok(timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn timestamp_binds_a_reused_expensive_input_once() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":10,"width":32,"signed":true,"code":"INT32","mayBeNull":false
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let ty = edit.type_id(roots[0])?;
        let mut next = 0;
        let input = edit.ref_parameter(ty, &mut next)?;
        let value = edit.deref_parameter(&input)?;
        let producer = edit.closure(value, &[input])?;
        let argument = edit.ref_parameter(ty, &mut next)?;
        let use_a = edit.deref_parameter(&argument)?;
        let second_read = edit.derive_inner(argument.path.into(), [])?;
        let use_b = edit.dereference(edit.expression(second_read)?)?;
        let sum =
            edit.binary_expression(roots[0], ty, feldera_sql_ir::BinaryOp::Add, use_a, use_b)?;
        let reader = edit.closure(sum, &[argument])?;

        // A call in the map makes its body expensive, even when the callee is a short closure.
        let input = edit.ref_parameter(ty, &mut next)?;
        let call = edit.call_closure(producer, &[input.path])?;
        let map = edit.closure(call, &[input])?;
        let mut warnings = Vec::new();
        let composed = compose_timestamp(&mut edit, reader, &[map.into()], &mut warnings)?;
        let body = edit.node(composed.into())?.edge("body")?;
        assert_eq!(edit.expr_kind(edit.expression(body)?)?, ExpressionKind::Let);
        let initializer = edit.node(body)?.edge("initializer")?;
        assert_eq!(edit.node(initializer)?.edge("expression")?, call.into());
        assert_eq!(edit.node(body)?.edge("consumer")?, sum.into());
        assert_eq!(
            edit.node(composed.into())?.edges("parameters")?,
            edit.node(map.into())?.edges("parameters")?
        );
        Ok(())
    }

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
