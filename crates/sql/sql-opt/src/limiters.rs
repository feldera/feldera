//! Waterline operators built from monotonicity facts on the expanded circuit.

mod filter;
mod keyed;
mod pass;
pub use pass::{insert_limiters, LimiterInputs};
mod projection;
mod source;
mod streams;
pub use projection::Projection;
pub use streams::{connect_error_view, window, Stream, StreamKind, WindowBounds};

use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind, OutputPort,
    RefParameter, TypeId, TypeKind,
};

/// An operator in the circuit being transformed and its scalar bound type.
/// The type is explicit because some operators have multiple output ports.
#[derive(Clone, Copy)]
pub struct Bound {
    pub port: OutputPort,
    pub ty: TypeId,
}

fn min_component(
    edit: &mut CircuitEditor,
    params: &[RefParameter],
    ty: TypeId,
    path: &mut Vec<usize>,
) -> Result<ExpressionId, Error> {
    let kind = edit.type_kind(ty)?;
    if matches!(kind, TypeKind::Tuple | TypeKind::RawTuple) {
        let fields = edit.node(ty.into())?.edges("tupFields")?;
        let mut components = Vec::with_capacity(fields.len());
        for (index, field) in fields.into_iter().enumerate() {
            path.push(index);
            components.push(min_component(edit, params, edit.type_id(field)?, path)?);
            path.pop();
        }
        // Beta-reduction rebuilds raw tuples with a new inferred type. SQL
        // tuple constructors keep the declared result type of their input.
        return if kind == TypeKind::RawTuple {
            edit.tuple_expr(ty.into(), true, &components)
        } else {
            edit.tuple_with_type(ty, &components)
        };
    }

    let mut values = Vec::with_capacity(params.len());
    for param in params {
        let mut read = edit.deref_parameter(param)?;
        for &field in path.iter() {
            read = edit.tuple_field(read, field)?;
        }
        values.push(read);
    }
    edit.binary_expressions(values[0].into(), ty, BinaryOp::AggMin, &values)
}

/// Combine projected bounds in adjacent pairs at each level. The Java
/// createApplyN helper substitutes a second set of parameters and simplifies
/// the closure. Generate the reduced tree directly with those same names.
pub fn minimum_bound(
    edit: &mut CircuitEditor,
    body: NodeId,
    bounds: &[Bound],
    next: &mut u64,
) -> Result<Bound, Error> {
    let first = *bounds
        .first()
        .ok_or_else(|| Error::new("a minimum requires at least one bound"))?;
    let mut names = next
        .checked_add(bounds.len() as u64)
        .ok_or_else(|| Error::new("generated names exhausted"))?;
    let mut params = Vec::with_capacity(bounds.len());
    for &bound in bounds {
        if !edit.same_type(first.ty, bound.ty)? {
            return Err(Error::new("cannot combine bounds of different types"));
        }
        params.push(edit.ref_parameter(bound.ty, &mut names)?);
    }
    let value = if let [one] = params.as_slice() {
        edit.deref_parameter(one)?
    } else {
        min_component(edit, &params, first.ty, &mut Vec::new())?
    };
    let ty = edit.expression_type(value)?;
    let function = edit.closure(value, &params)?;
    let inputs = bounds.iter().map(|b| b.port).collect::<Vec<_>>();
    let id = edit.append_operator(
        body,
        first.port.operator,
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
