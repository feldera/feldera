//! Waterline operators built from monotonicity facts on the expanded circuit.

mod filter;
mod join;
mod keyed;
mod pass;
pub use join::join_waterline;
pub use pass::{insert_limiters, LimiterInputs};
mod projection;
mod source;
mod streams;
pub use projection::Projection;
pub use streams::{connect_error_view, window, Stream, StreamKind, WindowBounds};

use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort,
    TypeId, TypeKind,
};

/// An operator in the circuit being transformed and its scalar bound type.
/// The type is explicit because some operators have multiple output ports.
#[derive(Clone, Copy)]
pub struct Bound {
    pub port: OutputPort,
    pub ty: TypeId,
}

/// Bind a limiter closure to the actual waterline input types. The caller supplies
/// an input for every formal parameter, in port order.
pub fn apply_waterlines(
    edit: &mut CircuitEditor,
    function: NodeId,
    input_types: &[TypeId],
    next: &mut u64,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let mut names = *next;
    let mut arguments = Vec::with_capacity(input_types.len());
    for &ty in input_types {
        let reference = edit.reference_type(ty)?;
        arguments.push(edit.variable(reference, &mut names)?);
    }
    let body = feldera_sql_expr::apply_closure(edit, function, &arguments, warnings)?;
    let closure = edit.closure_from_variables(body, &arguments)?;
    *next = names;
    Ok(closure)
}

fn minimum_pair(
    edit: &mut CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<ExpressionId, Error> {
    let ty = edit.expression_type(left)?;
    if !matches!(edit.type_kind(ty)?, TypeKind::Tuple | TypeKind::RawTuple) {
        return edit.binary_expression(left.into(), ty, BinaryOp::AggMin, left, right);
    }
    let width = edit.node(ty.into())?.edges("tupFields")?.len();
    let mut fields = Vec::with_capacity(width);
    for index in 0..width {
        let left_field = edit.tuple_field(left, index)?;
        let right_field = edit.tuple_field(right, index)?;
        fields.push(minimum_pair(edit, left_field, right_field)?);
    }
    edit.tuple_with_type(ty, &fields)
}

/// Compute a pointwise minimum in a balanced tree. Its intermediate expressions
/// share the first bound's type. Rebinding the closure gives each parameter its
/// own input type, including when equal types have distinct circuit identities.
pub fn minimum_waterline(
    edit: &mut CircuitEditor,
    inputs: &[TypeId],
    next: &mut u64,
    warnings: &mut Vec<feldera_sql_expr::SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let first = *inputs
        .first()
        .ok_or_else(|| Error::new("a minimum requires at least one bound"))?;
    let mut names = *next;
    let mut parameters = Vec::with_capacity(inputs.len());
    let mut level = Vec::with_capacity(inputs.len());
    for &ty in inputs {
        if !edit.same_type(first, ty)? {
            return Err(Error::new("cannot combine bounds of different types"));
        }
        let param = edit.ref_parameter(first, &mut names)?;
        level.push(edit.deref_parameter(&param)?);
        parameters.push(param);
    }
    while level.len() > 1 {
        let (pairs, last) = level.as_chunks::<2>();
        let mut reduced = Vec::with_capacity(level.len().div_ceil(2));
        for &[left, right] in pairs {
            reduced.push(minimum_pair(edit, left, right)?);
        }
        reduced.extend_from_slice(last);
        level = reduced;
    }
    let closure = edit.closure(level[0], &parameters)?;
    let result = apply_waterlines(edit, closure.into(), inputs, &mut names, warnings)?;
    *next = names;
    Ok(result)
}

pub fn minimum_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    bounds: &[Bound],
    next: &mut u64,
) -> Result<Bound, Error> {
    let mut names = *next;
    let types: Vec<_> = bounds.iter().map(|b| b.ty).collect();
    let function = minimum_waterline(edit, &types, &mut names, &mut Vec::new())?;
    let value = edit.node(function.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(value)?)?;
    let inputs = bounds.iter().map(|b| b.port).collect::<Vec<_>>();
    let id = edit.append_operator(
        body,
        bounds[0].port.operator,
        OperatorKind::ApplyN,
        &inputs,
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    edit.annotate(id, serde_json::json!({"class":"Waterline"}))?;
    *next = names;
    Ok(Bound {
        port: OutputPort {
            operator: id,
            number: 0,
        },
        ty,
    })
}
