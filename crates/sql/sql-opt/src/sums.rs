//! Simplify ordered sums of streams in circuit construction order.
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, OperatorKind as Op, OutputPort};
use std::collections::{BTreeMap, HashMap};

/// Java visits positive terms first. Each negative term cancels the earliest
/// remaining occurrence of its operand, including its output port number.
fn cancel_terms(
    inputs: &[OutputPort],
    negations: &BTreeMap<OutputPort, OutputPort>,
) -> Vec<OutputPort> {
    let mut positive = Vec::new();
    let mut unmatched = Vec::new();
    let mut available = HashMap::new();
    let mut canceled = HashMap::new();
    for &port in inputs {
        if !negations.contains_key(&port) {
            positive.push(port);
            *available.entry(port).or_insert(0usize) += 1;
        }
    }
    for &port in inputs {
        if let Some(operand) = negations.get(&port) {
            let count = available.entry(*operand).or_default();
            if *count > 0 {
                *count -= 1;
                *canceled.entry(*operand).or_insert(0usize) += 1;
            } else {
                unmatched.push(port);
            }
        }
    }
    positive.retain(|port| {
        let Some(count) = canceled.get_mut(port).filter(|count| **count > 0) else {
            return true;
        };
        *count -= 1;
        false
    });
    positive.extend(unmatched);
    positive
}

pub fn merge_sums(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut forwarded = BTreeMap::new();
    let mut negations = BTreeMap::new();
    let mut remaining_edges = 1_000_000usize;
    for id in order {
        let op = edit.node(id)?;
        let kind = op.operator_kind();
        if !matches!(kind, Some(Op::Sum | Op::Negate)) {
            continue;
        }
        let inputs: Vec<_> = op
            .inputs()
            .map(|p| forwarded.get(&p.operator).copied().unwrap_or(p))
            .collect();
        if kind == Some(Op::Negate) {
            let [operand] = inputs.as_slice() else {
                return Err(Error::new("negation requires one operand"));
            };
            if let Some(original) = negations.get(operand) {
                forwarded.insert(id, *original);
            } else {
                negations.insert(
                    OutputPort {
                        operator: id,
                        number: 0,
                    },
                    *operand,
                );
            }
            continue;
        }
        match cancel_terms(&inputs, &negations).as_slice() {
            [] => edit.cancel_sum(id)?,
            [only] => {
                forwarded.insert(id, *only);
            }
            terms => {
                let mut flattened = Vec::new();
                for &term in terms {
                    let producer = edit.node(term.operator)?;
                    if producer.operator_kind() == Some(Op::Sum) {
                        remaining_edges = remaining_edges
                            .checked_sub(producer.inputs().count())
                            .ok_or_else(|| Error::new("merge-sums output edge budget exceeded"))?;
                        flattened.extend(producer.inputs());
                    } else {
                        remaining_edges = remaining_edges
                            .checked_sub(1)
                            .ok_or_else(|| Error::new("merge-sums output edge budget exceeded"))?;
                        flattened.push(term);
                    }
                }
                let op = edit.node(id)?;
                if !op.inputs().eq(flattened.iter().copied()) {
                    let ty = edit.type_id(op.edge("outputType")?)?;
                    edit.replace_stream_inputs(id, &flattened, ty)?;
                }
            }
        }
    }
    crate::rebuild::rebuild_input_closures(edit.finish_forwarding(&forwarded)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::NodeId;
    use serde_json::json;

    #[test]
    fn complements_remove_the_earliest_positive_and_append_unmatched_negations() {
        let port = |id, number| OutputPort {
            operator: NodeId::Outer(id),
            number,
        };
        let (a, b, c) = (port(1, 0), port(1, 1), port(2, 0));
        let (minus_a, minus_c) = (port(3, 0), port(4, 0));
        let negations = BTreeMap::from([(minus_a, a), (minus_c, c)]);
        assert_eq!(
            cancel_terms(&[minus_c, a, b, a, minus_a], &negations),
            vec![b, a, minus_c]
        );
    }

    #[test]
    fn completely_canceled_sum_builds_the_correct_empty_literal() {
        for (collection, expression) in [
            ("DBSPTypeZSet", "DBSPZSetExpression"),
            ("DBSPTypeIndexedZSet", "DBSPIndexedZSetExpression"),
        ] {
            let mut ty = json!({"class":collection,"id":1,"sourceEmpty":true,"elementType":{"class":"DBSPTypeInteger","id":2,"sourceEmpty":true}});
            if collection == "DBSPTypeIndexedZSet" {
                ty["keyType"] = json!({"node":2});
            }
            let port = |n| json!({"operator":{"node":n},"outputNumber":0});
            let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPConstantOperator","id":1,"outputType":ty,"inputs":[],"annotations":[]},
                {"class":"DBSPNegateOperator","id":2,"outputType":{"node":1},"inputs":[port(1)],"annotations":[]},
                {"class":"DBSPSumOperator","id":3,"outputType":{"node":1},"inputs":[port(1),port(2)],"annotations":[{"class":"Recursive"}]}
            ]});
            let output = merge_sums(Circuit::from_json(graph).unwrap(), &mut 0).unwrap();
            let constant = output.node(NodeId::Outer(3)).unwrap();
            assert_eq!(constant.operator_kind(), Some(Op::Constant));
            assert_eq!(constant.inputs().count(), 0);
            let literal = output.node(constant.edge("function").unwrap()).unwrap();
            assert_eq!(literal.class(), expression);
            assert_eq!(
                literal.edge("type").unwrap(),
                constant.edge("outputType").unwrap()
            );
            assert_eq!(
                literal.edge("type").unwrap() == NodeId::Inner(1),
                collection == "DBSPTypeIndexedZSet"
            );
        }
    }
}
