//! Check operator legality at the corresponding point in the optimizer schedule.
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct RecursiveViolation {
    pub operator: u64,
    pub operation: &'static str,
    pub internal: bool,
}

/// The incrementalization invariant applies to all circuit bodies.
pub fn first_integral(circuit: &Circuit) -> Result<Option<NodeId>, Error> {
    Ok(circuit
        .operator_order(BodyOrder::Preorder)?
        .into_iter()
        .find(|&id| {
            circuit
                .node(id)
                .is_some_and(|op| op.operator_kind() == Some(Op::Integrate))
        }))
}

/// Inspect children before reporting the enclosing recursive component.
/// A recursive view declaration is a legal input to its own component.
pub fn recursive_violation(circuit: &Circuit) -> Result<Option<RecursiveViolation>, Error> {
    let top_level: HashSet<_> = circuit.root().edges("allOperators")?.into_iter().collect();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        if top_level.contains(&id) {
            continue;
        }
        let op = circuit.node(id).expect("validated circuit body member");
        let (operation, internal) = match op.operator_kind() {
            Some(Op::Apply) => ("apply", true),
            Some(Op::Apply2) => ("apply2", true),
            Some(Op::IndexedTopK) => ("TopK", false),
            Some(Op::Rank) => ("RANK", false),
            Some(Op::RowNumber) => ("ROW_NUMBER", false),
            Some(
                Op::IntegrateTraceRetainKeys
                | Op::IntegrateTraceRetainValues
                | Op::IntegrateTraceRetainNValues,
            ) => ("GC", true),
            Some(Op::Lag) => ("LAG", false),
            Some(Op::Nested) => ("recursion", true),
            Some(Op::Now) => ("NOW", true),
            Some(
                Op::PartitionedRollingAggregate | Op::PartitionedRollingAggregateWithWaterline,
            ) => ("OVER", false),
            Some(Op::SourceMap | Op::SourceMultiset) => ("input", true),
            Some(Op::Waterline) => ("WATERLINE", false),
            Some(Op::Window) => ("WINDOW", false),
            _ => continue,
        };
        let NodeId::Outer(operator) = id else {
            unreachable!("body members are operators");
        };
        return Ok(Some(RecursiveViolation {
            operator,
            operation,
            internal,
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_diagnostics_preserve_java_postorder_and_error_class() {
        let mut graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPWindowOperator","id":1,"inputs":[]},
            {"class":"DBSPNestedOperator","id":2,"inputs":[],"internalOutputs":[],"allOperators":[
                {"class":"DBSPViewDeclarationOperator","id":3,"inputs":[]},
                {"class":"DBSPNestedOperator","id":4,"inputs":[],"internalOutputs":[],"allOperators":[
                    {"class":"DBSPLagOperator","id":5,"inputs":[]}
                ]},
                {"class":"DBSPIntegrateOperator","id":6,"inputs":[]}
            ]}
        ]});
        let circuit = Circuit::from_json(graph.clone()).unwrap();
        assert_eq!(first_integral(&circuit).unwrap(), Some(NodeId::Outer(6)));
        assert_eq!(
            recursive_violation(&circuit).unwrap(),
            Some(RecursiveViolation {
                operator: 5,
                operation: "LAG",
                internal: false
            })
        );
        graph["allOperators"][1]["allOperators"][1]["allOperators"] = json!([]);
        assert_eq!(
            recursive_violation(&Circuit::from_json(graph).unwrap()).unwrap(),
            Some(RecursiveViolation {
                operator: 4,
                operation: "recursion",
                internal: true
            })
        );
    }
}
