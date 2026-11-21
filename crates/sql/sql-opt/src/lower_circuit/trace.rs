//! Emit the LowerCircuitVisitor debug print at the start of each apply closure.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, TypeKind,
};

pub(super) fn instrument_apply(
    edit: &mut CircuitEditor,
    id: NodeId,
) -> Result<ExpressionId, Error> {
    let op = edit.node(id)?;
    let function = edit.node(op.edge("function")?)?;
    let parameters = function.edges("parameters")?;
    let body = edit.expression(function.edge("body")?)?;
    let format = match op.operator_kind() {
        Some(Op::Apply) if parameters.len() == 1 => "{:?}".to_owned(),
        Some(Op::Apply2) if parameters.len() == 2 => "({:?},{:?})".to_owned(),
        Some(Op::ApplyN) => format!("({})", "{:?}, ".repeat(parameters.len())),
        _ => {
            return Err(Error::new(
                "apply trace parameter count differs from its operator",
            ))
        }
    };
    let NodeId::Outer(number) = id else {
        return Err(Error::new("expected an apply operator"));
    };
    let string = edit.standard_type(body.into(), TypeKind::Str)?;
    let message = edit.make_expr(
        string.into(),
        Expr::StrLiteral,
        string,
        [
            ("value", Field::Data(format!("{number}={format}").into())),
            ("raw", Field::Data(false.into())),
        ],
    )?;
    let mut arguments = vec![Field::Reference(message.into())];
    for parameter in &parameters {
        let parameter = edit.node(*parameter)?;
        let ty = edit.type_id(parameter.edge("type")?)?;
        let name = parameter
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("apply trace parameter lacks a name"))?
            .to_owned();
        arguments.push(Field::Reference(edit.named_variable(ty, &name)?.into()));
    }
    let any = edit.standard_type(body.into(), TypeKind::Any)?;
    let macro_path = edit.function_path(any.into(), "println!")?;
    let print = edit.make_expr(
        any.into(),
        Expr::Apply,
        any,
        [
            ("function", Field::Reference(macro_path.into())),
            ("arguments", Field::Array(arguments)),
        ],
    )?;
    let statement = edit.inner_at(
        print.into(),
        InnerKind::Other("DBSPExpressionStatement".into()),
        [("expression", Field::Reference(print.into()))],
    )?;
    let block = edit.empty_origin_node(
        body.into(),
        InnerKind::Expression(Expr::Block),
        [
            ("contents", Field::Array(vec![Field::Reference(statement)])),
            ("lastExpression", Field::Reference(body.into())),
            ("type", Field::Reference(edit.expression_type(body)?.into())),
        ],
    )?;
    edit.closure_with_parameters(edit.expression(block)?, &parameters)
}
