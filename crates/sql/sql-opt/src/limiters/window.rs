//! Derive a temporal window's output waterline from its endpoints and input.
use super::{filter::arg, minimum_pair};
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, TypeId};

pub struct WindowWaterline {
    /// Applied to the clock input that drives the window bounds.
    pub endpoint: ExpressionId,
    /// An unbounded lower endpoint also needs the waterline on incoming keys.
    pub combine: Option<ExpressionId>,
}

pub fn window_waterline(
    edit: &mut CircuitEditor,
    bounds: ExpressionId,
    input_bound: Option<TypeId>,
    retain_empty_value: bool,
    next: &mut u64,
) -> Result<WindowWaterline, Error> {
    let closure = edit.node(bounds.into())?;
    let parameters = closure.edges("parameters")?;
    let pair = edit.expression(closure.edge("body")?)?;
    if edit.expr_kind(pair)? != Expr::RawTuple {
        return Err(Error::new("window endpoints must be a raw tuple"));
    }
    let endpoints = edit.node(pair.into())?.edges("fields")?;
    let [lower, upper] = endpoints.as_slice() else {
        return Err(Error::new("window bounds need two endpoints"));
    };
    let unbox = |id| {
        let boxed = edit.node(id)?;
        if !matches!(
            boxed.inner_kind(),
            Some(feldera_sql_ir::InnerKind::Expression(Expr::Unary))
        ) || boxed.data("opcode").and_then(|v| v.as_str()) != Some("TYPEDBOX")
        {
            return Err(Error::new("expected a boxed window endpoint"));
        }
        boxed.edge("source")
    };
    let lower = unbox(*lower)?;
    let upper = unbox(*upper)?;
    let pack = |edit: &mut CircuitEditor, key: ExpressionId| {
        let mut fields = vec![key];
        if retain_empty_value {
            fields.push(edit.tuple_expr(key.into(), false, &[])?);
        }
        edit.tuple_expr(key.into(), true, &fields)
    };
    let source = if input_bound.is_some() { upper } else { lower };
    let copy = copy_expression(edit, source)?;
    let edge = edit.expression(copy)?;
    let Some(input_type) = input_bound else {
        // Once the lower endpoint advances, no future change can have a smaller key.
        let projected = pack(edit, edge)?;
        return Ok(WindowWaterline {
            endpoint: edit.closure_with_parameters(projected, &parameters)?,
            combine: None,
        });
    };

    // A row can enter through the advancing upper endpoint or through the data input.
    let endpoint = edit.closure_with_parameters(edge, &parameters)?;
    let upper_type = edit.expression_type(edit.expression(upper)?)?;
    let input = arg(edit, input_type, next)?;
    let clock = arg(edit, upper_type, next)?;
    let row = edit.dereference(input)?;
    let key = edit.tuple_field(row, 0)?;
    let key = edit.clone_if_needed(key)?;
    let upper = edit.dereference(clock)?;
    let upper = edit.clone_if_needed(upper)?;
    let minimum = minimum_pair(edit, key, upper)?;
    let projected = pack(edit, minimum)?;
    Ok(WindowWaterline {
        endpoint,
        combine: Some(edit.closure_from_variables(projected, &[input, clock])?),
    })
}
