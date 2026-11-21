//! Build the timestamp extractors and row projection of a concrete ASOF join.
use feldera_sql_expr::{SimplifyWarning, apply_closure, flatten_tuple_fields};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId, TypeId, TypeKind,
};

/// Materialize an index row with a timestamp-ordered value. The left map
/// reconstructs the key tuple. The right map forwards its dereferenced key.
pub fn asof_index(
    edit: &mut CircuitEditor,
    pair: ExpressionId,
    comparator: ExpressionId,
    flatten_key: bool,
) -> Result<ExpressionId, Error> {
    let value = edit.tuple_field(pair, 1)?;
    let value = edit.dereference(value)?;
    let fields = flatten_tuple_fields(edit, value)?;
    let row = edit.tuple_expr(value.into(), false, &fields)?;
    let wrapper = edit.custom_order_type(
        comparator.into(),
        edit.expression_type(row)?,
        edit.expression_type(comparator)?,
    )?;
    let value = edit.make_expr(
        comparator.into(),
        ExpressionKind::CustomOrd,
        wrapper,
        [
            ("source", Field::Reference(row.into())),
            ("comparator", Field::Reference(comparator.into())),
        ],
    )?;
    let key_ref = edit.tuple_field(pair, 0)?;
    let mut key = edit.dereference(key_ref)?;
    if flatten_key {
        let fields = flatten_tuple_fields(edit, key)?;
        key = edit.tuple_expr(key.into(), false, &fields)?;
    }
    let output = edit.tuple_expr(pair.into(), true, &[key, value])?;
    edit.closure_from_variables(output, &[pair])
}

fn ordered_row(edit: &CircuitEditor, wrapper: TypeId) -> Result<TypeId, Error> {
    let [row, _] = edit
        .node(wrapper.into())?
        .edges("typeArgs")?
        .try_into()
        .map_err(|_| Error::new("an ordered ASOF input needs a row and a comparator"))?;
    let row = edit.type_id(row)?;
    if edit.type_kind(wrapper)? != TypeKind::WithCustomOrd
        || edit.type_kind(row)? != TypeKind::Tuple
    {
        return Err(Error::new("expected a custom-ordered ASOF tuple"));
    }
    Ok(row)
}

fn unwrap_row(
    edit: &mut CircuitEditor,
    param: ExpressionId,
    row: TypeId,
) -> Result<ExpressionId, Error> {
    let value = edit.dereference(param)?;
    edit.make_expr(
        value.into(),
        ExpressionKind::UnwrapCustomOrd,
        row,
        [("expression", Field::Reference(value.into()))],
    )
}

fn parameter(
    edit: &mut CircuitEditor,
    ty: TypeId,
    optional: bool,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let reference = edit.reference_type(ty)?;
    let reference = edit.with_nullability(reference, optional)?;
    edit.variable(reference, next)
}

pub fn asof_timestamps(
    edit: &mut CircuitEditor,
    wrappers: [TypeId; 2],
    indices: [usize; 2],
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let [left, right] = wrappers;
    let [left_index, right_index] = indices;
    let left_row = ordered_row(edit, left)?;
    let right_row = ordered_row(edit, right)?;
    let left = parameter(edit, left, false, next)?;
    let right = parameter(edit, right, false, next)?;
    let left_value = unwrap_row(edit, left, left_row)?;
    let right_value = unwrap_row(edit, right, right_row)?;
    let left_ts = edit.tuple_field(left_value, left_index)?;
    let right_ts = edit.tuple_field(right_value, right_index)?;
    let left_type = edit.expression_type(left_ts)?;
    let right_type = edit.expression_type(right_ts)?;
    if !edit.same_type_ignoring_nullability(left_type, right_type)? {
        return Err(Error::new("ASOF timestamp columns must have the same type"));
    }
    let common = edit.with_nullability(
        left_type,
        edit.nullable(left_type)? || edit.nullable(right_type)?,
    )?;
    let mut extractor = |param, timestamp: ExpressionId| {
        let value = edit.cast(timestamp.into(), timestamp, common)?;
        let owned = edit.clone_if_needed(value)?;
        edit.closure_from_variables(owned, &[param])
    };
    Ok([extractor(left, left_ts)?, extractor(right, right_ts)?])
}

/// The left row always exists. The optional right row supplies a nullable field
/// for every projected column when the join finds no match.
pub fn asof_projection(
    edit: &mut CircuitEditor,
    function: NodeId,
    key: TypeId,
    left: TypeId,
    right: TypeId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let left_row = ordered_row(edit, left)?;
    let right_row = ordered_row(edit, right)?;
    let key = parameter(edit, key, false, next)?;
    let left = parameter(edit, left, false, next)?;
    let right = parameter(edit, right, true, next)?;
    let mut fields = Vec::new();
    for index in 0..edit.node(left_row.into())?.edges("tupFields")?.len() {
        // The Java visitor constructs a separate read of the wrapper for each field.
        let row = unwrap_row(edit, left, left_row)?;
        fields.push(edit.tuple_field(row, index)?);
    }
    let tuple = edit.tuple_expr(left.into(), false, &fields)?;
    let left_arg = edit.borrow_value(tuple)?;
    fields.clear();
    for (index, ty) in edit
        .node(right_row.into())?
        .edges("tupFields")?
        .into_iter()
        .enumerate()
    {
        let nullable = edit.with_nullability(edit.type_id(ty)?, true)?;
        fields.push(edit.make_expr(
            right.into(),
            ExpressionKind::CustomOrdField,
            nullable,
            [
                ("expression", Field::Reference(right.into())),
                ("fieldNo", Field::Data(index.into())),
            ],
        )?);
    }
    let tuple = edit.tuple_expr(right.into(), false, &fields)?;
    let right_arg = edit.borrow_value(tuple)?;
    let output = apply_closure(edit, function, &[key, left_arg, right_arg], warnings)?;
    edit.closure_from_variables(output, &[key, left, right])
}
