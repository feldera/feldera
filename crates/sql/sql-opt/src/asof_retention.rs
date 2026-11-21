//! Rebind ASOF retention predicates to the concrete join's ordered input values.
use crate::consumer_graph::successors;
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, NodeId, OperatorKind, TypeKind,
};
use std::collections::BTreeMap;

/// Match each logical ASOF join to the retention operators on its two data ports.
/// Consumer edges are body-local. One producer can expose several distinct ports.
pub fn asof_retainers(circuit: &Circuit) -> Result<Vec<(u64, u64)>, Error> {
    let mut matches = Vec::new();
    for body in
        std::iter::once(circuit.root_id()).chain(circuit.operator_order(BodyOrder::Preorder)?)
    {
        let node = circuit.node(body).expect("validated body member");
        if node.field("allOperators").is_none() {
            continue;
        }
        let members = node.edges("allOperators")?;
        let operator = |id| circuit.node(id).expect("validated operator");
        let mut joins = members
            .iter()
            .copied()
            .filter(|&id| operator(id).operator_kind() == Some(OperatorKind::AsofJoin))
            .peekable();
        if joins.peek().is_none() {
            continue;
        }
        let readers = successors(circuit, body)?;
        let positions: BTreeMap<_, _> = members.iter().copied().zip(0..).collect();
        for join in joins {
            let [left, right] = operator(join)
                .inputs()
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| Error::new("ASOF join requires two inputs"))?;
            for (data, kind) in [
                (left, OperatorKind::IntegrateTraceRetainValues),
                (right, OperatorKind::IntegrateTraceRetainNValues),
            ] {
                let source = positions
                    .get(&data.operator)
                    .ok_or_else(|| Error::new("ASOF input crosses a circuit body"))?;
                for &(reader, _) in &readers[*source] {
                    let retainer = members[reader];
                    let node = operator(retainer);
                    if node.operator_kind() == Some(kind) && node.inputs().next() == Some(data) {
                        let (NodeId::Outer(join), NodeId::Outer(retainer)) = (join, retainer)
                        else {
                            unreachable!("circuit members have outer IDs")
                        };
                        matches.push((join, retainer));
                    }
                }
            }
        }
    }
    Ok(matches)
}

pub fn move_asof_retention(
    circuit: Circuit,
    mut joins: BTreeMap<u64, u64>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if joins.is_empty() {
        return Ok(circuit);
    }
    // Allocate predicate parameters in visitor order, independent of operator IDs.
    let operators = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut names = *next;
    for id in operators {
        let NodeId::Outer(number) = id else {
            return Err(Error::new("expected an ASOF circuit operator"));
        };
        let Some(join) = joins.remove(&number) else {
            continue;
        };
        let retain = edit.node(id)?;
        let side = match retain.operator_kind() {
            Some(OperatorKind::IntegrateTraceRetainValues) => 0,
            Some(OperatorKind::IntegrateTraceRetainNValues) => 1,
            _ => return Err(Error::new("expected an ASOF value retention operator")),
        };
        let control = retain
            .inputs()
            .nth(1)
            .ok_or_else(|| Error::new("missing ASOF retention waterline"))?;
        let predicate = edit.node(retain.edge("function")?)?;
        let body = predicate.edge("body")?;
        let parameters: [NodeId; 2] = predicate
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("ASOF retention predicate needs two parameters"))?;
        let bound_type = edit.type_id(edit.node(parameters[1])?.edge("type")?)?;
        let join = edit.node(NodeId::Outer(join))?;
        if join.operator_kind() != Some(OperatorKind::ConcreteAsofJoin) {
            return Err(Error::new("ASOF retention requires a concrete join"));
        }
        let data = join
            .inputs()
            .nth(side)
            .ok_or_else(|| Error::new("missing ordered ASOF input"))?;
        let collection = input_type(&edit, data)?;
        let wrapper = edit.type_id(edit.node(collection.into())?.edge("elementType")?)?;
        if edit.type_kind(wrapper)? != TypeKind::WithCustomOrd {
            return Err(Error::new("ASOF value must carry its custom ordering"));
        }
        let [row, _comparator] = edit
            .node(wrapper.into())?
            .edges("typeArgs")?
            .try_into()
            .map_err(|_| Error::new("ASOF ordering requires row and comparator types"))?;
        let reference = edit.reference_type(wrapper)?;
        let ordered = edit.variable(reference, &mut names)?;
        let bound = edit.variable(bound_type, &mut names)?;
        let value = edit.dereference(ordered)?;
        let row = edit.make_expr(
            value.into(),
            ExpressionKind::UnwrapCustomOrd,
            edit.type_id(row)?,
            [("expression", Field::Reference(value.into()))],
        )?;
        let row = edit.borrow_value(row)?;
        let reduced =
            reduce_closure_body(&mut edit, body, &parameters, &[row.into(), bound.into()])?;
        let predicate =
            edit.closure_from_variables(edit.expression(reduced)?, &[ordered, bound])?;
        edit.set_operator_field(id, "function", Field::Reference(predicate.into()))?;
        edit.set_operator_field(id, "outputType", Field::Reference(collection.into()))?;
        edit.clear_annotations(id)?;
        edit.rewire_inputs(id, &[data, control])?;
    }
    if !joins.is_empty() {
        return Err(Error::new(
            "ASOF retention plan references an absent operator",
        ));
    }
    let circuit = rebuild_input_closures(edit.finish_with_rewrites()?, &mut names)?;
    *next = names;
    Ok(circuit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_the_data_port_and_the_retention_kind_on_each_side() {
        let port = |id, output| json!({"operator": {"node": id}, "outputNumber": output});
        let source =
            |id| json!({"class":"DBSPInputMapWithWaterlineOperator", "id":id, "inputs":[]});
        let keep = |kind, id, data, control| json!({"class":kind,"id":id,"inputs":[data, control]});
        let values = "DBSPIntegrateTraceRetainValuesOperator";
        let latest = "DBSPIntegrateTraceRetainNValuesOperator";
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[
                source(1), source(2), source(3),
                {"class":"DBSPAsofJoinOperator","id":4,"inputs":[port(1,0),port(2,0)]},
                keep(values, 5, port(1,0), port(3,0)), keep(latest, 6, port(2,0), port(3,0)),
                keep(values, 7, port(1,1), port(3,0)), keep(values, 8, port(2,0), port(3,0)),
                keep(latest, 9, port(3,0), port(2,0))
            ]
        }))
        .unwrap();
        assert_eq!(asof_retainers(&circuit).unwrap(), [(4, 5), (4, 6)]);
    }
}
