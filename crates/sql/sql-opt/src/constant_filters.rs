//! Fold literal predicates after expression simplification.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, InnerKind, OperatorKind, OutputClocks,
};
use std::collections::BTreeMap;

pub fn remove_constant_filters(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let mut forwarded = BTreeMap::new();
    let mut empty = Vec::new();
    let mut clocks = None;
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        if node.operator_kind() != Some(OperatorKind::Filter) {
            continue;
        }
        let closure = circuit
            .node(node.edge("function")?)
            .expect("validated closure edge");
        let body = circuit
            .node(closure.edge("body")?)
            .expect("validated body edge");
        if !matches!(
            body.inner_kind(),
            Some(InnerKind::Expression(ExpressionKind::BoolLiteral))
        ) {
            continue;
        }
        let mut inputs = node.inputs();
        let input = inputs
            .next()
            .filter(|_| inputs.next().is_none())
            .ok_or_else(|| Error::new("filter requires one input"))?;
        match body.data("value").and_then(|v| v.as_bool()) {
            Some(true) => {
                forwarded.insert(id, forwarded.get(&input.operator).copied().unwrap_or(input));
            }
            Some(false) => {
                let clocks = match &mut clocks {
                    Some(clocks) => clocks,
                    slot @ None => slot.insert(OutputClocks::new(&circuit)?),
                };
                empty.push((id, clocks.get(input)?));
            }
            None => return Err(Error::new("constant filter predicate must be non-null")),
        }
    }
    let mut edit = CircuitEditor::new(circuit);
    for (filter, clock) in empty {
        edit.replace_with_empty_stream(filter, clock)?;
    }
    rebuild_input_closures(edit.finish_forwarding(&forwarded)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::NodeId;
    use serde_json::json;

    #[test]
    fn true_filters_forward_and_false_filters_preserve_the_clock() {
        let port = |n| json!({"operator":{"node":n},"outputNumber":0});
        for (incremental, expected) in [
            (false, OperatorKind::Constant),
            (true, OperatorKind::IncrementalConstant),
        ] {
            let graph = json!({"class":"DBSPCircuit","id":0,"incremental":incremental,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeInteger","id":2}}},
                {"class":"DBSPFilterOperator","id":2,"inputs":[port(1)],"outputType":{"node":1},"function":{"class":"DBSPClosureExpression","id":3,"body":{"class":"DBSPBoolLiteral","id":4,"value":true}}},
                {"class":"DBSPFilterOperator","id":3,"inputs":[port(2)],"outputType":{"node":1},"function":{"node":3}},
                {"class":"DBSPFilterOperator","id":4,"inputs":[port(3)],"outputType":{"node":1},"function":{"class":"DBSPClosureExpression","id":5,"body":{"class":"DBSPBoolLiteral","id":6,"value":false}},"annotations":[{"class":"Recursive"}]},
                {"class":"DBSPSinkOperator","id":5,"inputs":[port(3)],"outputType":{"node":1}},
                {"class":"DBSPSinkOperator","id":6,"inputs":[port(4)],"outputType":{"node":1}}
            ]});
            let mut next = 31;
            let circuit =
                remove_constant_filters(Circuit::from_json(graph).unwrap(), &mut next).unwrap();
            let output = circuit.node(NodeId::Outer(4)).unwrap();
            assert_eq!(output.operator_kind(), Some(expected));
            assert_eq!(output.inputs().count(), 0);
            assert_eq!(
                circuit
                    .node(NodeId::Outer(5))
                    .unwrap()
                    .inputs()
                    .next()
                    .unwrap()
                    .operator,
                NodeId::Outer(1)
            );
            assert_eq!(
                circuit
                    .node(NodeId::Outer(6))
                    .unwrap()
                    .inputs()
                    .next()
                    .unwrap()
                    .operator,
                NodeId::Outer(4)
            );
            assert_eq!(next, 31);
        }
    }
}
