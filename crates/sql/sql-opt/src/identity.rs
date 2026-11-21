//! Remove identity projections while keeping at most one retainer of each kind on a trace.
use crate::rebuild::{input_type, rebuild_input_closures};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, OutputPort, TypeId, TypeKind as Ty,
};
use std::collections::{BTreeMap, HashMap};

/// Match the access from a row or one side of a (key, value) input. The
/// Java reference closure clones a field exactly when its type requires it.
fn reads_input(
    edit: &CircuitEditor,
    mut id: NodeId,
    name: &str,
    slot: Option<usize>,
    field: Option<usize>,
    clone: bool,
) -> Result<bool, Error> {
    let mut peel = |kind, index| -> Result<bool, Error> {
        let node = edit.node(id)?;
        if !matches!(node.inner_kind(), Some(InnerKind::Expression(actual)) if *actual == kind) {
            return Ok(false);
        }
        if let Some(index) = index {
            if node.data("fieldNo").and_then(|v| v.as_u64()) != Some(index as u64) {
                return Ok(false);
            }
        }
        id = node.edge("expression")?;
        Ok(true)
    };
    if clone && !peel(Expr::Clone, None)? {
        return Ok(false);
    }
    if field.is_some() && !peel(Expr::Field, field)? {
        return Ok(false);
    }
    if !peel(Expr::Deref, None)? {
        return Ok(false);
    }
    if slot.is_some() && !peel(Expr::Field, slot)? {
        return Ok(false);
    }
    let variable = edit.node(id)?;
    Ok(matches!(
        variable.inner_kind(),
        Some(InnerKind::Expression(Expr::VariablePath))
    ) && variable.data("variable").and_then(|v| v.as_str()) == Some(name))
}

fn projects_row(
    edit: &CircuitEditor,
    expr: NodeId,
    row: TypeId,
    name: &str,
    slot: Option<usize>,
) -> Result<bool, Error> {
    let row_kind = edit.type_kind(row)?;
    if slot.is_none() && !matches!(row_kind, Ty::Tuple | Ty::RawTuple) {
        return reads_input(edit, expr, name, None, None, edit.requires_clone(row)?);
    }
    // The indexed identity constructs non-null TupN values for both halves.
    // A raw tuple or a scalar half cannot equal that constructor's result type.
    if row_kind != Ty::Tuple || (slot.is_some() && edit.nullable(row)?) {
        return Ok(false);
    }
    let tuple = edit.node(expr)?;
    if !matches!(tuple.inner_kind(), Some(InnerKind::Expression(Expr::Tuple)))
        || !matches!(tuple.field("fields"), Some(Field::Array(_)))
    {
        return Ok(false);
    }
    let values = tuple.edges("fields")?;
    let fields = edit.node(row.into())?.edges("tupFields")?;
    if fields.len() != values.len() {
        return Ok(false);
    }
    for (index, (field, value)) in fields.into_iter().zip(values).enumerate() {
        let field = edit.type_id(field)?;
        // Projecting from a nullable row lifts each of its fields.
        if edit.nullable(row)? && !edit.nullable(field)? {
            return Ok(false);
        }
        if !reads_input(
            edit,
            value,
            name,
            slot,
            Some(index),
            edit.requires_clone(field)?,
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn identity_closure(
    edit: &CircuitEditor,
    closure: NodeId,
    next: &mut u64,
) -> Result<bool, Error> {
    let closure = edit.node(closure)?;
    if !matches!(
        closure.inner_kind(),
        Some(InnerKind::Expression(Expr::Closure))
    ) {
        return Ok(false);
    }
    let params = closure.edges("parameters")?;
    let [param] = params.as_slice() else {
        return Ok(false);
    };
    let param = edit.node(*param)?;
    let name = param
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing projection parameter name"))?;
    let argument = edit.type_id(param.edge("type")?)?;
    let body = closure.edge("body")?;
    let result = edit.expression_type(edit.expression(body)?)?;
    // Java allocates a variable after checking the signature, including when
    // the body then fails to match. Later closures expose that counter.
    let reserve_name = |next: &mut u64| -> Result<(), Error> {
        *next = next
            .checked_add(1)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        Ok(())
    };
    match edit.type_kind(argument)? {
        Ty::Ref => {
            let row = edit.type_id(edit.node(argument.into())?.edge("type")?)?;
            if !edit.same_type(row, result)? {
                return Ok(false);
            }
            reserve_name(next)?;
            projects_row(edit, body, row, name, None)
        }
        Ty::RawTuple if edit.type_kind(result)? == Ty::RawTuple => {
            let arguments = edit.node(argument.into())?.edges("tupFields")?;
            let outputs = edit.node(result.into())?.edges("tupFields")?;
            if arguments.len() != 2 || outputs.len() != 2 {
                return Ok(false);
            }
            for (&arg, &out) in arguments.iter().zip(&outputs) {
                if edit.type_kind(edit.type_id(arg)?)? != Ty::Ref
                    || !edit.same_type(
                        edit.type_id(edit.node(arg)?.edge("type")?)?,
                        edit.type_id(out)?,
                    )?
                {
                    return Ok(false);
                }
            }
            reserve_name(next)?;
            let body = edit.node(body)?;
            if !matches!(
                body.inner_kind(),
                Some(InnerKind::Expression(Expr::RawTuple))
            ) {
                return Ok(false);
            }
            let values = body.edges("fields")?;
            if values.len() != 2 {
                return Ok(false);
            }
            for (slot, (out, value)) in outputs.into_iter().zip(values).enumerate() {
                if !projects_row(edit, value, edit.type_id(out)?, name, Some(slot))? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[derive(Default, Copy, Clone)]
struct Retention {
    keys: bool,
    values: bool,
}

impl Retention {
    fn claim(&mut self, other: Self) -> bool {
        if (self.keys && other.keys) || (self.values && other.values) {
            return false;
        }
        self.keys |= other.keys;
        self.values |= other.values;
        true
    }
}

pub fn remove_identity_operators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut attached = HashMap::<OutputPort, Retention>::new();
    for &id in &order {
        let op = edit.node(id)?;
        let keys = match op.operator_kind() {
            Some(Op::IntegrateTraceRetainKeys) => true,
            Some(Op::IntegrateTraceRetainValues | Op::IntegrateTraceRetainNValues) => false,
            _ => continue,
        };
        let input = op
            .inputs()
            .next()
            .ok_or_else(|| Error::new("retainer needs a trace input"))?;
        let retention = attached.entry(input).or_default();
        if keys {
            retention.keys = true;
        } else {
            retention.values = true;
        }
    }

    let mut counter = *next;
    let mut forwarded = BTreeMap::new();
    let mut claimed = attached.clone();
    for id in order {
        let node = edit.node(id)?;
        if !matches!(node.operator_kind(), Some(Op::Map | Op::MapIndex)) {
            continue;
        }
        let input = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("map needs an input"))?;
        let Some(closure) = node.field("function").and_then(Field::node_id) else {
            continue;
        };
        if !edit.same_type(
            input_type(&edit, input)?,
            edit.type_id(node.edge("outputType")?)?,
        )? || !identity_closure(&edit, closure, &mut counter)?
        {
            continue;
        }
        let moving = attached
            .get(&OutputPort {
                operator: id,
                number: 0,
            })
            .copied()
            .unwrap_or_default();
        if claimed.entry(input).or_default().claim(moving) {
            forwarded.insert(id, forwarded.get(&input.operator).copied().unwrap_or(input));
        } else {
            edit.identity_map_as_noop(id)?;
        }
    }
    let output = rebuild_input_closures(edit.finish_forwarding(&forwarded)?, &mut counter)?;
    *next = counter;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_one_projection_can_move_each_retention_kind_to_its_input() {
        let port = |id| json!({"operator":{"node":id},"outputNumber":0});
        let row = json!({"class":"DBSPTypeInteger","id":1,"signed":true,"width":32,"mayBeNull":false,"code":"INT32"});
        let stream = json!({"class":"DBSPTypeZSet","id":2,"elementType":row,"mayBeNull":false});
        let reference = json!({"class":"DBSPTypeRef","id":3,"type":{"node":1},"mayBeNull":false});
        let variable = json!({"class":"DBSPVariablePath","id":4,"variable":"x","type":{"node":3}});
        let closure = json!({"class":"DBSPClosureExpression","id":5,
            "parameters":[{"class":"DBSPParameter","id":6,"name":"x","type":reference}],
            "body":{"class":"DBSPDerefExpression","id":7,"expression":variable,"type":{"node":1}}});
        let map = |id, function| {
            json!({"class":"DBSPMapOperator","id":id,"inputs":[port(1)],
            "annotations":[{"class":"IsProjection","value":1}],"outputType":{"node":2},"function":function})
        };
        let retainer =
            |id, source, class| json!({"class":class,"id":id,"inputs":[port(source),port(1)]});
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[], "allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"outputType":stream},
            map(2,closure), map(3,json!({"node":5})), map(4,json!({"node":5})),
            retainer(5,2,"DBSPIntegrateTraceRetainValuesOperator"),
            retainer(6,3,"DBSPIntegrateTraceRetainNValuesOperator"),
            retainer(7,4,"DBSPIntegrateTraceRetainKeysOperator")
        ]});
        let mut next = 10;
        let circuit =
            remove_identity_operators(Circuit::from_json(graph).unwrap(), &mut next).unwrap();
        assert_eq!(next, 14); // Three comparisons and one replacement Noop closure.
        assert!(circuit.node(NodeId::Outer(2)).is_none());
        assert!(circuit.node(NodeId::Outer(4)).is_none());
        let noop = circuit.node(NodeId::Outer(3)).unwrap();
        assert_eq!(noop.operator_kind(), Some(Op::Noop));
        assert_eq!(noop.data("annotations"), Some(&json!([])));
        for (reader, expected) in [(5, 1), (6, 3), (7, 1)] {
            let trace = circuit
                .node(NodeId::Outer(reader))
                .unwrap()
                .inputs()
                .next()
                .unwrap();
            assert_eq!(trace.operator, NodeId::Outer(expected));
        }
    }
}
