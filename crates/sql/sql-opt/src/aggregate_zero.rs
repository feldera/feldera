//! Expand the empty-input row that a global aggregate emits. A recursive
//! body imports its seed from the enclosing circuit through a delta operator.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort, TypeId,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn seed(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    origin: Option<Value>,
) -> Result<(NodeId, TypeId), Error> {
    let row = edit.expression(edit.node(site)?.edge("function")?)?;
    let value = edit.singleton_zset(row)?;
    let ty = edit.expression_type(value)?;
    let seed = edit.append_operator(
        body,
        site,
        Op::Constant,
        &[],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(value.into())),
        ],
    )?;
    edit.set_intermediate_origin(seed, origin)?;
    Ok((seed, ty))
}

/// Nested constants are hoisted in the Java pass's map iteration order. The caller supplies that order when the JVM owns the map.
pub fn expand_aggregate_zero(
    circuit: Circuit,
    next: &mut u64,
    hoist_order: &[NodeId],
) -> Result<Circuit, Error> {
    let root = circuit.root_id();
    let mut bodies = BTreeMap::new();
    for (body, id) in crate::stream_rewrites::body_members(&circuit)? {
        if circuit.node(id).and_then(|n| n.operator_kind()) == Some(Op::AggregateZero) {
            bodies.insert(id, body);
        }
    }
    let nested: BTreeSet<_> = bodies
        .iter()
        .filter_map(|(&id, &body)| (body != root).then_some(id))
        .collect();
    if hoist_order.len() != nested.len()
        || hoist_order.iter().copied().collect::<BTreeSet<_>>() != nested
    {
        return Err(Error::new(
            "aggregate-zero hoist order must contain each nested site once",
        ));
    }
    if bodies.is_empty() {
        return Ok(circuit);
    }

    let mut origins = circuit.intermediate_origins()?;
    let mut hoists = Vec::with_capacity(nested.len());
    for &id in hoist_order {
        hoists.push((id, origins.derive::<1>(id)?.map(|[origin]| origin)));
    }
    let mut sites = Vec::with_capacity(bodies.len());
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        if let Some(&body) = bodies.get(&id) {
            let [map, negate, constant] = origins
                .derive::<3>(id)?
                .map(|copies| copies.map(Some))
                .unwrap_or_default();
            let fourth = if body != root {
                origins.derive::<1>(id)?.map(|[origin]| origin)
            } else {
                None
            };
            sites.push((body, id, [map, negate, constant, fourth]));
        }
    }
    let first = circuit.root().edges("allOperators")?[0];
    let mut edit = CircuitEditor::new(circuit);
    let mut constants = BTreeMap::new();
    for (site, origin) in hoists {
        let (seed, ty) = seed(&mut edit, root, site, origin)?;
        edit.place_before(root, seed, first)?;
        constants.insert(site, (seed, ty));
    }

    let mut counter = *next;
    for (region, (body, site, [map_origin, negate_origin, seed_origin, integral_origin])) in
        sites.into_iter().enumerate()
    {
        let op = edit.node(site)?;
        let input = {
            let mut inputs = op.inputs();
            inputs
                .next()
                .filter(|_| inputs.next().is_none())
                .ok_or_else(|| Error::new("aggregate zero requires one input"))?
        };
        let row = edit.expression(op.edge("function")?)?;
        let ty = edit.expression_type(row)?;
        let arg = edit.ref_parameter(ty, &mut counter)?;
        let function = edit.closure(row, &[arg])?;
        let map_type = edit.zset_type(ty)?;
        let map = edit.append_operator(
            body,
            site,
            Op::Map,
            &[input],
            [
                ("outputType", Field::Reference(map_type.into())),
                ("function", Field::Reference(function.into())),
            ],
        )?;
        edit.set_intermediate_origin(map, map_origin)?;
        let negate = edit.append_operator(
            body,
            site,
            Op::Negate,
            &[output(map)],
            [("outputType", Field::Reference(map_type.into()))],
        )?;
        edit.set_intermediate_origin(negate, negate_origin)?;
        let annotation = json!({"class":"GlobalAggregate","id":region});
        let (constant, ty) = if let Some((hoisted, ty)) = constants.remove(&site) {
            let delta = edit.append_operator(
                body,
                site,
                Op::Delta,
                &[output(hoisted)],
                [("outputType", Field::Reference(ty.into()))],
            )?;
            edit.set_intermediate_origin(delta, seed_origin)?;
            edit.annotate(delta, annotation.clone())?;
            let integral = edit.append_operator(
                body,
                site,
                Op::Integrate,
                &[output(delta)],
                [("outputType", Field::Reference(ty.into()))],
            )?;
            edit.set_intermediate_origin(integral, integral_origin)?;
            (integral, ty)
        } else {
            seed(&mut edit, body, site, seed_origin)?
        };
        edit.place_before(body, map, site)?;
        edit.aggregate_zero_to_sum(site, [output(constant), output(negate), input], ty)?;
        for id in [map, negate, constant, site] {
            edit.annotate(id, annotation.clone())?;
        }
    }
    let result = rebuild_input_closures(edit.finish_with_rewrites()?, &mut counter)?;
    *next = counter;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_seeds_follow_the_hoist_order_and_regions_follow_the_body_order() {
        let port = |id| json!({"operator":{"node":id},"outputNumber":0});
        let aggregate = |id: u64, value| {
            json!({
                "class":"DBSPAggregateZeroOperator","id":id,"inputs":[port(3)],"annotations":[],
                "outputType":{"node":1},
                "function":{"class":"DBSPI32Literal","id":10+id,"type":{"node":2},"value":value}
            })
        };
        let wire = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"annotations":[],
             "outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeInteger","id":2}}},
            {"class":"DBSPNestedOperator","id":2,"inputs":[port(1)],"annotations":[],
             "allOperators":[
                 {"class":"DBSPDeltaOperator","id":3,"inputs":[port(1)],"annotations":[],"outputType":{"node":1}},
                 aggregate(4,10),aggregate(5,20)
             ],"internalOutputs":[port(4),port(5)]}
        ]});
        let circuit = Circuit::from_json(wire.clone()).unwrap();
        let mut next = 7;
        assert!(expand_aggregate_zero(
            Circuit::from_json(wire).unwrap(),
            &mut next,
            &[NodeId::Outer(4); 2]
        )
        .is_err());
        let circuit =
            expand_aggregate_zero(circuit, &mut next, &[NodeId::Outer(5), NodeId::Outer(4)])
                .unwrap();
        let members: Vec<_> = circuit.operators().collect();
        for ((_, seed), value) in members[..2].iter().zip([20, 10]) {
            let expression = circuit.node(seed.edge("function").unwrap()).unwrap();
            let row = circuit.node(expression.edges("data").unwrap()[0]).unwrap();
            assert_eq!(row.data("value"), Some(&json!(value)));
            assert_eq!(seed.data("annotations"), Some(&json!([])));
        }
        let nested = circuit.node(NodeId::Outer(2)).unwrap();
        let imported: Vec<_> = nested.inputs().map(|p| p.operator).collect();
        assert_eq!(imported, [NodeId::Outer(1), members[1].0, members[0].0]);
        for (id, region) in [(4, 0), (5, 1)] {
            let sum = circuit.node(NodeId::Outer(id)).unwrap();
            assert_eq!(sum.operator_kind(), Some(Op::AtomicSum));
            assert_eq!(
                sum.data("annotations"),
                Some(&json!([{"class":"GlobalAggregate","id":region}]))
            );
        }
        assert_eq!(next, 9);
    }
}
