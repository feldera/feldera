//! Select adaptive joins outside recursive circuits. Adaptive joins keep their
//! own input integrals, so they cannot share a stream that has a GC policy.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};
use std::collections::BTreeSet;

pub(super) fn eligible_joins(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    // Java builds a separate graph for each recursive component and disables
    // balancing there. Only direct edges in the outer circuit participate.
    let mut retained = BTreeSet::new();
    for (_, node) in circuit.operators() {
        if matches!(
            node.operator_kind(),
            Some(
                Op::IntegrateTraceRetainKeys
                    | Op::IntegrateTraceRetainValues
                    | Op::IntegrateTraceRetainNValues
            )
        ) {
            let data = node
                .inputs()
                .next()
                .ok_or_else(|| Error::new("trace retention requires a data port"))?;
            retained.insert(data);
        }
    }
    Ok(circuit
        .operators()
        .filter_map(|(id, node)| {
            (node.operator_kind().is_some_and(Op::supports_balanced_join)
                && node.inputs().all(|port| !retained.contains(&port)))
            .then_some(id)
        })
        .collect())
}

pub fn balance_joins(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let joins = eligible_joins(&circuit)?;
    if joins.is_empty() {
        return Ok(circuit);
    }
    let mut editor = CircuitEditor::new(circuit);
    for join in joins {
        editor.balance_join(join)?;
    }
    rebuild_input_closures(editor.finish_with_rewrites()?, next)
}

/// The scheduled OptimizeWithGraph group runs exactly one iteration.
pub fn optimize_balanced_joins(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    crate::remove_dead_operators(balance_joins(circuit, next)?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_the_retained_data_port_excludes_a_join() {
        let port = |id, number| json!({"operator":{"node":id},"outputNumber":number});
        let join = |id, number| json!({"class":"DBSPJoinOperator","id":id,"inputs":[port(2,number),port(1,0)],"balanced":false});
        let input = Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
            {"class":"DBSPControlledKeyFilterOperator","id":2,"inputs":[port(1,0),port(1,0)]},
            join(3,0),join(4,1),
            {"class":"DBSPIntegrateTraceRetainKeysOperator","id":5,"inputs":[port(2,0),port(1,0)]}
        ]})).unwrap();
        assert_eq!(eligible_joins(&input).unwrap(), [NodeId::Outer(4)]);
    }
}
