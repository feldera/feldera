//! Expressions for the built-in clock and the table-backed clock used during replay.
use crate::limiters::timestamp_max;
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId, OperatorKind, OutputPort,
    TypeId,
};

/// The clock relation contains one timestamp column.
fn read_timestamp(
    edit: &mut CircuitEditor,
    parameter: ExpressionId,
) -> Result<ExpressionId, Error> {
    let row = edit.dereference(parameter)?;
    edit.tuple_field(row, 0)
}

fn clock_parameter(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let reference = edit.reference_type(row)?;
    edit.variable(reference, next)
}

fn clock_index(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let input = clock_parameter(edit, timestamp, next)?;
    let time = read_timestamp(edit, input)?;
    let key = edit.tuple_expr(input.into(), false, &[])?;
    let value = edit.tuple_expr(input.into(), false, &[time])?;
    let pair = edit.tuple_expr(input.into(), true, &[key, value])?;
    edit.closure_from_variables(pair, &[input])
}

/// Initialize and update the maximum timestamp from changes to the clock table.
fn clock_aggregate(
    edit: &mut CircuitEditor,
    timestamp: TypeId,
    weight: ExpressionId,
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let input = clock_parameter(edit, timestamp, next)?;
    let first = read_timestamp(edit, input)?;
    let initial = edit.tuple_expr(input.into(), false, &[first])?;
    let init = edit.closure_from_variables(initial, &[input, weight])?;

    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let accumulator = edit.variable(row, next)?;
    let current = edit.tuple_field(accumulator, 0)?;
    let incoming = read_timestamp(edit, input)?;
    let maximum = edit.make_expr(
        accumulator.into(),
        ExpressionKind::ConditionalIncrement,
        timestamp,
        [
            ("left", Field::Reference(current.into())),
            ("right", Field::Reference(incoming.into())),
            ("opcode", Field::Data("AGG_MAX".into())),
        ],
    )?;
    let body = edit.tuple_expr(input.into(), false, &[maximum])?;
    let update = edit.closure_from_variables(body, &[accumulator, input, weight])?;
    Ok([init, update])
}

pub struct ClockStreams {
    pub collection: NodeId,
    pub indexed: NodeId,
}

fn port(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn index_clock(
    edit: &mut CircuitEditor,
    body: NodeId,
    input: OutputPort,
    function: ExpressionId,
    ty: TypeId,
) -> Result<NodeId, Error> {
    edit.append_operator(
        body,
        body,
        OperatorKind::MapIndex,
        &[input],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )
}

/// The system clock produces a collection containing the current timestamp.
pub fn builtin_clock(
    edit: &mut CircuitEditor,
    body: NodeId,
    timestamp: TypeId,
    empty_key: TypeId,
    next: &mut u64,
) -> Result<ClockStreams, Error> {
    let path = edit.function_path(timestamp.into(), "now")?;
    let call = edit.make_expr(
        timestamp.into(),
        ExpressionKind::Apply,
        timestamp,
        [
            ("function", Field::Reference(path.into())),
            ("arguments", Field::Array(vec![])),
        ],
    )?;
    let value = edit.tuple_expr(timestamp.into(), false, &[call])?;
    let literal = edit.singleton_zset(value)?;
    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let collection_type = edit.zset_type(row)?;
    let collection = edit.append_operator(
        body,
        body,
        OperatorKind::Now,
        &[],
        [
            ("outputType", Field::Reference(collection_type.into())),
            ("function", Field::Reference(literal.into())),
        ],
    )?;
    let function = clock_index(edit, timestamp, next)?;
    let index_row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let ty = edit.indexed_zset_type(timestamp.into(), empty_key, index_row)?;
    let indexed = index_clock(edit, body, port(collection), function, ty)?;
    edit.annotate(indexed, serde_json::json!({"class":"AlwaysMonotone"}))?;
    Ok(ClockStreams {
        collection,
        indexed,
    })
}

/// The table-backed clock keeps the greatest timestamp received on the empty key.
pub fn table_clock(
    edit: &mut CircuitEditor,
    body: NodeId,
    source: OutputPort,
    timestamp: TypeId,
    weight: ExpressionId,
    next: &mut u64,
) -> Result<ClockStreams, Error> {
    let function = clock_index(edit, timestamp, next)?;
    let signature = edit.expression_type(function)?;
    let pair = edit.node(signature.into())?.edge("resultType")?;
    let [key, row] = edit
        .node(pair)?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("clock index requires a key and a timestamp row"))?;
    let (key, row) = (edit.type_id(key)?, edit.type_id(row)?);
    let ty = edit.indexed_zset_type(pair, key, row)?;
    let index = index_clock(edit, body, source, function, ty)?;
    let [initial, update] = clock_aggregate(edit, timestamp, weight, next)?;
    let indexed = edit.append_operator(
        body,
        body,
        OperatorKind::ChainAggregate,
        &[port(index)],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(update.into())),
            ("init", Field::Reference(initial.into())),
        ],
    )?;
    edit.annotate(indexed, serde_json::json!({"class":"AlwaysMonotone"}))?;
    let deindex = crate::rebuild::deindex_function(edit, update.into(), key, row, next)?;
    let ty = edit.zset_type(row)?;
    let collection = edit.append_operator(
        body,
        body,
        OperatorKind::Deindex,
        &[port(indexed)],
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(deindex.into())),
        ],
    )?;
    Ok(ClockStreams {
        collection,
        indexed,
    })
}

/// The scalar clock starts at the timestamp minimum. Its waterline advances
/// to the maximum of the timestamps seen in successive input batches.
pub fn scalar_clock(
    edit: &mut CircuitEditor,
    row: TypeId,
    empty_key: TypeId,
    minimum: ExpressionId,
    next: &mut u64,
) -> Result<[ExpressionId; 3], Error> {
    let reference = edit.reference_type(row)?;
    let input = edit.variable(reference, next)?;
    let time = read_timestamp(edit, input)?;
    let initial = edit.tuple_expr(row.into(), false, &[minimum])?;
    let value = edit.tuple_expr(row.into(), false, &[time])?;
    let ty = edit.expression_type(initial)?;
    let maximum = timestamp_max(edit, initial.into(), ty, next)?;
    let initial = edit.closure_with_parameters(initial, &[])?;
    let key = edit.reference_type(empty_key)?;
    let key = edit.variable(key, next)?;
    let extract = edit.closure_from_variables(value, &[input, key])?;
    Ok([initial, extract, maximum])
}

/// Turn a clock collection into successive scalar maxima for temporal windows.
/// The differentiator and the waterline inherit the clock producer's origin.
pub fn scalar_clock_operators(
    edit: &mut CircuitEditor,
    body: NodeId,
    source: OutputPort,
    collection: TypeId,
    empty_key: TypeId,
    minimum: ExpressionId,
    next: &mut u64,
) -> Result<[NodeId; 2], Error> {
    let row = edit.type_id(edit.node(collection.into())?.edge("elementType")?)?;
    let [initial, extract, maximum] = scalar_clock(edit, row, empty_key, minimum, next)?;
    let result =
        edit.expression_type(edit.expression(edit.node(maximum.into())?.edge("body")?)?)?;
    let changes = edit.append_operator(
        body,
        source.operator,
        OperatorKind::Differentiate,
        &[source],
        [("outputType", Field::Reference(collection.into()))],
    )?;
    let waterline = edit.append_operator(
        body,
        source.operator,
        OperatorKind::Waterline,
        &[OutputPort {
            operator: changes,
            number: 0,
        }],
        [
            ("outputType", Field::Reference(result.into())),
            ("function", Field::Reference(maximum.into())),
            ("init", Field::Reference(initial.into())),
            ("extractTs", Field::Reference(extract.into())),
        ],
    )?;
    Ok([changes, waterline])
}
