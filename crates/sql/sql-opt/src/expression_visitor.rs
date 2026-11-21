//! Apply an expression transformation at the roots visited by Java's CircuitRewriter.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{BodyOrder, Circuit, CircuitEditor, Error, NodeId, OperatorKind as Op};

pub(super) enum Context {
    Declaration,
    Operator(NodeId),
    /// An aggregate implementation inherits the last visited expression's operator.
    Inherited,
}

/// CircuitRewriter visits expressions in constructor order, which can differ
/// from their serialization order. The first use of each position gets its index.
fn expression_fields(operator: Op) -> &'static [&'static str] {
    match operator {
        Op::Aggregate | Op::StreamAggregate => &["function", "aggregate"],
        Op::AggregateLinearPostprocess => &["function", "postProcess"],
        Op::AggregateLinearPostprocessRetainKeys => {
            &["function", "postProcess", "retainKeysFunction"]
        }
        Op::AsofJoin => &["function", "comparator"],
        Op::Chain => &["chain"],
        Op::ChainAggregate => &["function", "init"],
        Op::ConcreteAsofJoin => &["function", "comparator", "leftTimestamp", "rightTimestamp"],
        Op::ControlledKeyFilter => &["function", "error"],
        Op::IndexedTopK => &["function", "outputProducer", "limit", "equalityComparator"],
        Op::InputMapWithWaterline => &[
            "initializer",
            "timestamp",
            "lub",
            "filter",
            "keyFilter",
            "error",
            "keyError",
        ],
        Op::JoinFilterMap | Op::LeftJoinFilterMap | Op::StarJoinFilterMap => {
            &["function", "filter", "map"]
        }
        Op::Lag => &["function", "comparator", "projection"],
        Op::PartitionedRollingAggregate => &[
            "function",
            "partitioningFunction",
            "aggregate",
            "lower",
            "upper",
        ],
        Op::PartitionedRollingAggregateWithWaterline => &[
            "function",
            "partitioningFunction",
            "aggregateList",
            "lower",
            "upper",
        ],
        Op::Rank => &[
            "function",
            "outputProducer",
            "projectionFunc",
            "rankCmpFunc",
        ],
        Op::RowNumber => &["function", "outputProducer"],
        Op::Waterline => &["function", "init", "extractTs"],
        _ => &["function"],
    }
}

pub(super) fn rewrite_expressions(
    circuit: Circuit,
    next: &mut u64,
    mut rewrite: impl FnMut(&mut CircuitEditor, NodeId, Context, &mut u64) -> Result<NodeId, Error>,
) -> Result<Circuit, Error> {
    let operators = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut names = *next;
    for id in edit.circuit().root().edges("declarations")? {
        edit.rewrite_field_roots(id, "item", |editor, root| {
            rewrite(editor, root, Context::Declaration, &mut names)
        })?;
    }
    let mut changed_applications = Vec::new();
    for id in operators {
        let operator = edit
            .node(id)?
            .operator_kind()
            .ok_or_else(|| Error::new("expected an operator"))?;
        for &field in expression_fields(operator) {
            edit.rewrite_field_roots(id, field, |editor, root| {
                let context = if matches!(field, "aggregate" | "aggregateList") {
                    Context::Inherited
                } else {
                    Context::Operator(id)
                };
                let rewritten = rewrite(editor, root, context, &mut names)?;
                if root != rewritten && matches!(operator, Op::Apply | Op::Apply2 | Op::ApplyN) {
                    changed_applications.push(id);
                }
                Ok(rewritten)
            })?;
        }
    }
    let mut rewritten = edit.finish_with_rewrites()?;
    // CircuitRewriter constructs fresh Apply operators when their function or inputs
    // change. Those three constructors drop the source operator's annotations.
    for &id in &rewritten.rebuilt_inputs {
        if matches!(
            rewritten
                .circuit
                .node(id)
                .and_then(|node| node.operator_kind()),
            Some(Op::Apply | Op::Apply2 | Op::ApplyN)
        ) {
            changed_applications.push(id);
        }
    }
    for id in changed_applications {
        rewritten.circuit.annotations_mut(id)?.clear();
    }
    let result = rebuild_input_closures(rewritten, &mut names)?;
    *next = names;
    Ok(result)
}

/// Normalize each expression root independently before monotonicity analysis.
/// A closure's parameter identities survive, so facts keyed by its bindings
/// still address the same declarations.
pub fn ensure_expression_trees(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    rewrite_expressions(circuit, next, |edit, root, _, _| {
        if matches!(
            edit.node(root)?.inner_kind(),
            Some(feldera_sql_ir::InnerKind::Expression(_))
        ) {
            feldera_sql_expr::ensure_expression_tree(edit, root)
        } else {
            Ok(root)
        }
    })
}

/// Canonicalize the bindings in each operator expression independently.
pub fn canonicalize_names(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    rewrite_expressions(circuit, next, |edit, root, context, _| match context {
        Context::Declaration => Ok(root),
        _ => {
            let result = feldera_sql_expr::canonicalize_names(edit, root)?;
            if let Context::Operator(id) = context {
                if root != result
                    && matches!(
                        edit.node(id)?.operator_kind(),
                        Some(Op::Constant | Op::IncrementalConstant)
                    )
                {
                    let ty = edit.node(result)?.edge("type")?;
                    edit.set_operator_field(
                        id,
                        "outputType",
                        feldera_sql_ir::Field::Reference(ty),
                    )?;
                }
            }
            Ok(result)
        }
    })
}

/// Apply one scalar simplification pass to each declaration and operator expression.
pub fn simplify_expressions(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<feldera_sql_expr::SimplifyWarning>), Error> {
    let mut warnings = Vec::new();
    let circuit = rewrite_expressions(circuit, next, |edit, root, _, _| {
        feldera_sql_expr::simplify_closure_with_warnings(edit, root, Some(&mut warnings))
    })?;
    Ok((circuit, warnings))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn double_negation_preserves_the_original_nullable_value() {
        let ty = json!({"class":"DBSPTypeBool","id":10,"code":"BOOL","mayBeNull":true});
        let variable = json!({"class":"DBSPVariablePath","id":11,"type":ty,"variable":"b"});
        let not = |id, source| {
            json!({
                "class":"DBSPUnaryExpression","id":id,"type":{"node":10},"opcode":"NOT","source":source
            })
        };
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[{
                "class":"DBSPConstantOperator","id":1,"inputs":[],
                "function":not(13, not(12, variable))
            }]
        }))
        .unwrap();
        let (result, warnings) = simplify_expressions(circuit, &mut 0).unwrap();
        assert!(warnings.is_empty());
        assert_eq!(
            result
                .node(NodeId::Outer(1))
                .unwrap()
                .edge("function")
                .unwrap(),
            NodeId::Inner(11)
        );
    }

    #[test]
    fn folded_integer_uses_the_original_operand_location() {
        let integer = |id, width| {
            json!({
                "class":"DBSPTypeInteger","id":id,"code":format!("INT{width}"),
                "width":width,"mayBeNull":false,"signed":true
            })
        };
        let position =
            json!({"start_line_number":1,"start_column":3,"end_line_number":1,"end_column":9});
        let value = json!({"class":"DBSPI32Literal","id":11,"sourceEmpty":true,"type":integer(10,32),"value":"10"});
        let inner = json!({"class":"DBSPCastExpression","id":12,"sourceEmpty":false,"position":position,
            "source":value,"type":integer(13,64),"safe":"SqlUnsafe"});
        let outer = json!({"class":"DBSPCastExpression","id":14,"sourceEmpty":true,
            "source":inner,"type":integer(15,128),"safe":"SqlUnsafe"});
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[{
                "class":"DBSPConstantOperator","id":1,"inputs":[],"function":outer
            }]
        }))
        .unwrap();
        let (result, warnings) = simplify_expressions(circuit, &mut 0).unwrap();
        assert!(warnings.is_empty());
        let root = result
            .node(NodeId::Outer(1))
            .unwrap()
            .edge("function")
            .unwrap();
        let literal = result.node(root).unwrap();
        assert_eq!(literal.class(), "DBSPI128Literal");
        assert_eq!(literal.data("value"), Some(&json!("10")));
        assert_eq!(
            literal
                .field("position")
                .unwrap()
                .source_range()
                .unwrap()
                .start_column,
            3
        );
    }
}
