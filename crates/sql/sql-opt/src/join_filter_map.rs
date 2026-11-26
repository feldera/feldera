//! Lower a join, predicate and optional projection to a single closure returning a nullable row.
use feldera_sql_expr::{reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, NodeId,
};

fn apply_to_row(
    edit: &mut CircuitEditor,
    function: NodeId,
    row: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let argument = edit.borrow_value(row)?;
    let closure = edit.node(function)?;
    let reduced = reduce_closure_body_with_warnings(
        edit,
        closure.edge("body")?,
        &closure.edges("parameters")?,
        &[argument.into()],
        Some(warnings),
    )?;
    edit.expression(reduced)
}

/// Keep the join's parameter declarations and the source positions of the reduced closures.
pub fn lower_join_filter_map(
    edit: &mut CircuitEditor,
    join: NodeId,
    filter: NodeId,
    map: Option<NodeId>,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let closure = edit.node(join)?;
    let params = closure.edges("parameters")?;
    let row = edit.expression(closure.edge("body")?)?;
    let row_type = edit.expression_type(row)?;
    let (condition, value, binding) = if let Some(map) = map {
        let condition = apply_to_row(edit, filter, row, warnings)?;
        (condition, apply_to_row(edit, map, row, warnings)?, None)
    } else {
        // The unprojected row escapes the filter. Evaluate the join once and borrow the local.
        let binding = edit.inner_at(
            row.into(),
            InnerKind::Other("DBSPLetStatement".into()),
            [
                ("variable", Field::Data("tmp".into())),
                ("type", Field::Reference(row_type.into())),
                ("initializer", Field::Reference(row.into())),
                ("mutable", Field::Data(false.into())),
            ],
        )?;
        let argument = edit.named_variable(row_type, "tmp")?;
        let condition = apply_to_row(edit, filter, argument, warnings)?;
        (
            condition,
            edit.named_variable(row_type, "tmp")?,
            Some(binding),
        )
    };
    let some = edit.some_at(value.into(), value)?;
    let nullable = edit.with_nullability(edit.expression_type(value)?, true)?;
    let none = edit.typed_null(nullable)?;
    let result_type = edit.expression_type(some)?;
    let branch = edit.make_expr(
        join,
        ExpressionKind::If,
        result_type,
        [
            ("condition", Field::Reference(condition.into())),
            ("positive", Field::Reference(some.into())),
            ("negative", Field::Reference(none.into())),
        ],
    )?;
    let body = if let Some(binding) = binding {
        let block = edit.empty_origin_node(
            join,
            InnerKind::Expression(ExpressionKind::Block),
            [
                ("type", Field::Reference(result_type.into())),
                ("contents", Field::Array(vec![Field::Reference(binding)])),
                ("lastExpression", Field::Reference(branch.into())),
            ],
        )?;
        edit.expression(block)?
    } else {
        branch
    };
    edit.closure_with_parameters(body, &params)
}
