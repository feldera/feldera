//! Split temporal conjunctions and disjunctions before implementing NOW windows.
use crate::{filters::rewrite_filters, function_calls::contains_now};
use feldera_sql_expr::{SimplifyWarning, simplify_closure_with_warnings};
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op, OutputPort,
};

/// Combine two compatible temporal bounds on a single row. Both calls use the
/// same parameter. Reducing the completed closure preserves shared expressions.
pub fn merge_temporal_predicates(
    edit: &mut CircuitEditor,
    source: ExpressionId,
    filter: ExpressionId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let [parameter] = edit
        .node(source.into())?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("a temporal filter requires one row parameter"))?;
    let input = edit.type_id(edit.node(parameter)?.edge("type")?)?;
    let row = edit.variable(input, next)?;
    let left = edit.call_closure(source, &[row])?;
    let right = edit.call_closure(filter, &[row])?;
    let result = edit.expression_type(right)?;
    let both = edit.binary_expression(source.into(), result, BinaryOp::And, left, right)?;
    let closure = edit.closure_from_variables(both, &[row])?;
    let reduced = feldera_sql_expr::reduce_closure_body_with_warnings(
        edit,
        closure.into(),
        &[],
        &[],
        Some(warnings),
    )?;
    edit.expression(reduced)
}

pub fn break_temporal_filters(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    rewrite_filters(circuit, next, |edit, body, id, _, warnings| {
        split_filter(edit, body, id, warnings)
    })
}

fn split_filter(
    edit: &mut CircuitEditor,
    body: NodeId,
    id: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<(NodeId, OutputPort)>, Error> {
    let node = edit.node(id)?;
    let function = node.edge("function")?;
    if !contains_now(edit.circuit(), &[function])? {
        return Ok(None);
    }
    let input = node
        .inputs()
        .next()
        .ok_or_else(|| Error::new("filter requires an input"))?;
    let output_type = node.edge("outputType")?;
    let function = simplify_closure_with_warnings(edit, function, Some(warnings))?;
    let parameters = edit.node(function)?.edges("parameters")?;
    let mut predicate = edit.node(function)?.edge("body")?;
    while edit.expr_kind(edit.expression(predicate)?)? == Expr::Unary
        && edit
            .node(predicate)?
            .data("opcode")
            .and_then(|v| v.as_str())
            == Some("WRAP_BOOL")
    {
        predicate = edit.node(predicate)?.edge("source")?;
    }
    let binary = edit.node(predicate)?;
    if edit.expr_kind(edit.expression(predicate)?)? != Expr::Binary {
        return Ok(None);
    }
    let disjunction = match binary.data("opcode").and_then(|v| v.as_str()) {
        Some("AND") => false,
        Some("OR") => true,
        _ => return Ok(None),
    };
    let left = edit.expression(binary.edge("left")?)?;
    let right = edit.expression(binary.edge("right")?)?;
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    let collection = || ("outputType", Field::Reference(output_type));
    let filter = |edit: &mut CircuitEditor, condition, input| {
        let condition = edit.wrap_bool_if_needed(condition)?;
        let closure = edit.closure_with_parameters(condition, &parameters)?;
        edit.append_operator(
            body,
            id,
            Op::Filter,
            &[input],
            [collection(), ("function", Field::Reference(closure.into()))],
        )
        .map(port)
    };
    let first = filter(edit, left, input)?;
    let second = filter(edit, right, if disjunction { input } else { first })?;
    if !disjunction {
        return Ok(Some((first.operator, second)));
    }

    // Rows accepted by both predicates occur twice in the sum. Subtract that
    // intersection once to preserve the multiplicity of every input row.
    let sum = edit.append_operator(body, id, Op::Sum, &[first, second], [collection()])?;
    let first_closure = edit.node(first.operator)?.edge("function")?;
    let nullable = edit.nullable(edit.expression_type(left)?)?
        || edit.nullable(edit.expression_type(right)?)?;
    let ty = edit.bool_type(first_closure, nullable)?;
    let both = edit.make_expr(
        first_closure,
        Expr::Binary,
        ty,
        [
            ("left", Field::Reference(left.into())),
            ("right", Field::Reference(right.into())),
            ("opcode", Field::Data("AND".into())),
        ],
    )?;
    let intersection = filter(edit, both, input)?;
    let result = edit.append_operator(
        body,
        id,
        Op::Subtract,
        &[port(sum), intersection],
        [collection()],
    )?;
    Ok(Some((first.operator, port(result))))
}
