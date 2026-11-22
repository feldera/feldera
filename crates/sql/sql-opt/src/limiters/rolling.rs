//! Bound a rolling aggregate by its ordering key. The trace consumes a delayed,
//! boxed key, while a frame ending at or before the row also bounds its output.
use super::pass::InsertionSite;
use super::{Bound, Projection, pass::apply_bound, retention::delay, source::link};
use crate::monotonicity::bindings::waterline_bindings;
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, OutputPort, TypeId,
};

struct RollingKey<'a> {
    name: String,
    fields: [(TypeId, &'a Projection); 2],
}

impl RollingKey<'_> {
    /// Read the key from a tuple of references bound inside an Apply closure.
    fn transfer(
        &self,
        edit: &mut CircuitEditor,
        output: Option<bool>,
        incoming: Bound,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<ExpressionId, Error> {
        let mut types = Vec::new();
        let mut references = Vec::new();
        for (ty, shape) in self.fields {
            if shape.monotone() {
                let projected = shape.projected_type(edit, ty)?;
                types.push(projected.into());
                references.push(edit.reference_type(projected)?.into());
            }
        }
        let row = edit.tuple_type(incoming.ty.into(), true, types)?;
        let pair = edit.tuple_type(row.into(), true, references)?;
        let variable = edit.named_variable(pair, &self.name)?;
        let key = edit.tuple_field(variable, 0)?;
        let mut result = edit.dereference(key)?;
        if let Some(empty_partition) = output {
            let mut fields = Vec::new();
            if empty_partition {
                fields.push(edit.tuple_expr(row.into(), false, &[])?);
            }
            fields.push(edit.tuple_expr(row.into(), false, &[result])?);
            result = edit.tuple_expr(row.into(), true, &fields)?;
        }
        let bindings = waterline_bindings(edit, &[(&self.name, row)])?;
        let closure = bindings.closure(edit, result)?;
        super::apply_waterlines(edit, closure.into(), &[incoming.ty], next, warnings)
    }
}

pub(super) struct RollingInput<'a> {
    pub data: OutputPort,
    pub bound: Bound,
    pub projection: &'a Projection,
    pub analyzed_type: TypeId,
}

pub(super) fn insert(
    edit: &mut CircuitEditor,
    InsertionSite {
        body,
        operator: owner,
    }: InsertionSite,
    input: RollingInput<'_>,
    bounds_output: bool,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Bound>, Error> {
    let RollingInput {
        data,
        bound: input,
        projection: shape,
        analyzed_type,
    } = input;
    let Projection::Tuple { fields, .. } = shape else {
        return Err(Error::new(
            "rolling aggregate requires an indexed projection",
        ));
    };
    let [key_shape, value_shape] = fields.as_slice() else {
        return Err(Error::new("rolling aggregate requires a key and value"));
    };
    if !key_shape.monotone() {
        return Err(Error::new("rolling trace requires a bounded ordering key"));
    }
    let pair = edit.node(analyzed_type.into())?.edges("tupFields")?;
    let &[key_type, value_type] = pair.as_slice() else {
        return Err(Error::new(
            "rolling input analysis requires a key and value type",
        ));
    };
    let key = RollingKey {
        name: format!("t_{}", *next),
        fields: [
            (edit.type_id(key_type)?, key_shape),
            (edit.type_id(value_type)?, value_shape),
        ],
    };
    *next = next
        .checked_add(1)
        .ok_or_else(|| Error::new("generated names exhausted"))?;
    let extract = key.transfer(edit, None, input, next, warnings)?;
    let control = apply_bound(edit, body, input, extract)?;
    let result = if bounds_output {
        let output = edit.node(owner)?.edge("outputType")?;
        let partition = edit.node(output)?.edge("keyType")?;
        let empty = edit.node(partition)?.field("tupFields").is_some()
            && edit.node(partition)?.edges("tupFields")?.is_empty();
        let project = key.transfer(edit, Some(empty), input, next, warnings)?;
        Some(apply_bound(edit, body, input, project)?)
    } else {
        None
    };
    let parameter = super::filter::arg(edit, control.ty, next)?;
    let read = edit.dereference(parameter)?;
    let value = edit.clone_if_needed(read)?;
    let ty = edit.typed_box_type(control.ty)?;
    let boxed = edit.make_expr(
        value.into(),
        Expr::Unary,
        ty,
        [
            ("source", link(value)),
            ("opcode", Field::Data("TYPEDBOX".into())),
        ],
    )?;
    let function = edit.closure_from_variables(boxed, &[parameter])?;
    let (_, delayed) = delay(edit, body, control, next)?;
    let boxed = delayed.apply(edit, body, owner, function)?;
    edit.move_origin_definition(owner, boxed.port.operator)?;
    edit.place_before(body, control.port.operator, owner)?;
    edit.bound_rolling_aggregate(owner, data, boxed.port)?;
    Ok(result)
}
