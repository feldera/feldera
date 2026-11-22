//! Derive a temporal window's output waterline from its endpoints and input.
use super::{Bound, Projection, filter::arg, minimum_pair};
use feldera_sql_expr::copy_expression;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, NodeId, OperatorKind as Op,
    OutputPort, TypeId,
};

pub struct WindowWaterline {
    /// Applied to the clock input that drives the window bounds.
    pub endpoint: ExpressionId,
    /// An unbounded lower endpoint also needs the waterline on incoming keys.
    pub combine: Option<ExpressionId>,
}

/// Place the bound immediately before the window, where the Java visitor
/// emits it. The right input of the window boxes the two clock endpoints.
pub(super) fn insert_window_bound(
    edit: &mut CircuitEditor,
    site: NodeId,
    projection: &Projection,
    input_bound: Option<Bound>,
    remap: impl Fn(OutputPort) -> OutputPort,
    next: &mut u64,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<Bound, Error> {
    let op = edit.node(site)?;
    let [_, clock]: [OutputPort; 2] = op
        .inputs()
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| Error::new("window requires data and clock inputs"))?;
    let Projection::Tuple { fields, .. } = projection else {
        return Err(Error::new("expected a key/value window projection"));
    };
    let [_, value] = fields.as_slice() else {
        return Err(Error::new("window projection requires two fields"));
    };
    let lower_unbounded = op
        .data("lowerUnbounded")
        .and_then(|x| x.as_bool())
        .ok_or_else(|| Error::new("window requires lowerUnbounded"))?;
    let input = if lower_unbounded {
        Some(input_bound.ok_or_else(|| Error::new("missing waterline on unbounded window input"))?)
    } else {
        None
    };
    let apply = edit.node(clock.operator)?;
    if apply.operator_kind() != Some(Op::Apply) {
        return Err(Error::new(
            "window clock requires boxed endpoints from Apply",
        ));
    }
    let endpoints = edit.expression(apply.edge("function")?)?;
    let clock = remap(
        apply
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing window clock stream"))?,
    );
    let functions = window_waterline(edit, endpoints, input.map(|b| b.ty), value.monotone(), next)?;
    let body = edit.circuit().root_id();
    let clock = Bound {
        port: clock,
        ty: crate::rebuild::input_type(edit, clock)?,
    };
    let endpoint = clock.apply(edit, body, site, functions.endpoint)?;
    let result = if let (Some(input), Some(combine)) = (input, functions.combine) {
        let function = super::apply_waterlines(
            edit,
            combine.into(),
            &[input.ty, endpoint.ty],
            next,
            warnings,
        )?;
        super::pass::apply_bound_pair(edit, [input, endpoint], function)?
    } else {
        endpoint
    };
    edit.move_origin_definition(site, endpoint.port.operator)?;
    edit.place_before(body, endpoint.port.operator, site)?;
    Ok(result)
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
