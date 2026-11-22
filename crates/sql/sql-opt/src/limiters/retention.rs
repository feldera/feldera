//! Delay an inferred waterline by one step before using it to discard trace keys.
use super::filter::{arg, tuple_compare};
use super::source::link;
use super::{Bound, Projection};
use crate::rebuild::input_type;
use feldera_sql_ir::{
    CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort, TypeKind as Ty,
};

/// DBSP checkpoints can store a typed tuple directly. A raw tuple or scalar
/// travels through the delay as a single field of a typed tuple.
pub(super) fn delay(
    edit: &mut CircuitEditor,
    body: NodeId,
    bound: Bound,
    next: &mut u64,
) -> Result<(NodeId, Bound), Error> {
    let site = bound.port.operator;
    let wrap = edit.type_kind(bound.ty)? != Ty::Tuple;
    let mut input = bound;
    let minimum = edit.minimum_value(bound.ty)?;
    let initial = if wrap {
        let parameter = arg(edit, bound.ty, next)?;
        let read = edit.dereference(parameter)?;
        let value = edit.clone_if_needed(read)?;
        let tuple = edit.tuple_expr(value.into(), false, &[value])?;
        let function = edit.closure_from_variables(tuple, &[parameter])?;
        input = bound.apply(edit, body, site, function)?;
        edit.tuple_expr(minimum.into(), false, &[minimum])?
    } else {
        minimum
    };
    let operator = edit.append_operator(
        body,
        site,
        Op::Delay,
        &[input.port],
        [("outputType", link(input.ty)), ("function", link(initial))],
    )?;
    let delayed = Bound {
        port: OutputPort {
            operator,
            number: 0,
        },
        ty: input.ty,
    };
    if !wrap {
        return Ok((operator, delayed));
    }
    let parameter = arg(edit, input.ty, next)?;
    let read = edit.dereference(parameter)?;
    let field = edit.tuple_field(read, 0)?;
    let value = edit.clone_if_needed(field)?;
    let function = edit.closure_from_variables(value, &[parameter])?;
    Ok((
        input.port.operator,
        delayed.apply(edit, body, site, function)?,
    ))
}

pub(super) struct KeyRetention {
    /// The delayed control is appended even for a key that cannot be pruned.
    pub first: NodeId,
    retainer: Option<NodeId>,
}

impl KeyRetention {
    pub fn place_before_owner(
        self,
        edit: &mut CircuitEditor,
        body: NodeId,
        owner: NodeId,
    ) -> Result<(), Error> {
        // The delay inherits its input's origin. Only the trace retainer inherits
        // the owner's origin and can become that origin's first use.
        if let Some(retainer) = self.retainer {
            edit.move_origin_definition(owner, retainer)?;
        }
        edit.place_before(body, self.first, owner)
    }
}

pub(super) fn retain_keys(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    data: OutputPort,
    projection: &Projection,
    bound: Bound,
    next: &mut u64,
) -> Result<KeyRetention, Error> {
    let (first, control) = delay(edit, body, bound, next)?;
    let mut retention = KeyRetention {
        first,
        retainer: None,
    };
    let control_var = arg(edit, control.ty, next)?;
    let stream = input_type(edit, data)?;
    let indexed = edit.type_kind(stream)? == Ty::IndexedZSet;
    let key = edit.type_id(edit.node(stream.into())?.edge(if indexed {
        "keyType"
    } else {
        "elementType"
    })?)?;
    if indexed
        && edit.type_kind(key)? == Ty::Tuple
        && edit.node(key.into())?.edges("tupFields")?.is_empty()
        && !edit.nullable(key)?
    {
        return Ok(retention);
    }
    let data_var = arg(edit, key, next)?;
    let projected = if indexed {
        let Projection::Tuple { fields, .. } = projection else {
            return Err(Error::new("indexed trace requires a key/value projection"));
        };
        fields
            .first()
            .ok_or_else(|| Error::new("indexed trace has no key projection"))?
    } else {
        projection
    };
    if !projected.monotone() {
        return Ok(retention);
    }
    let read = edit.dereference(data_var)?;
    let left = projected.project_data(edit, read)?;
    let mut right = edit.dereference(control_var)?;
    if indexed {
        right = edit.tuple_field(right, 0)?;
    }
    let predicate = tuple_compare(edit, left, right)?;
    let key_ref = edit.expression_type(data_var)?;
    let data_param = edit.parameter_at(key_ref.into(), data_var)?;
    let control_param = edit.parameter_at(control_var.into(), control_var)?;
    let function = edit.closure_with_parameters(predicate, &[data_param, control_param])?;
    retention.retainer = Some(edit.append_operator(
        body,
        site,
        Op::IntegrateTraceRetainKeys,
        &[data, control.port],
        [
            ("outputType", link(stream)),
            ("function", link(function)),
            ("accumulate", Field::Data(true.into())),
        ],
    )?);
    Ok(retention)
}
