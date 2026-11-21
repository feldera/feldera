//! Rebind ASOF retention predicates to the concrete join's ordered input values.
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, NodeId, OperatorKind, TypeKind,
};
use std::collections::BTreeMap;

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
