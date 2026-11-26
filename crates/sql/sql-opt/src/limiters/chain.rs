//! A chain aggregate annotated AlwaysMonotone produces its own indexed waterline.
use super::filter::arg;
use super::source::{link, timestamp_max, waterline_value};
use super::Bound;
use crate::rebuild::deindex_function;
use feldera_sql_ir::{
    CircuitEditor, Error, NodeId, OperatorKind as Op, OutputPort, TypeKind as Ty,
};

/// Return the first appended operator and the bound of the original aggregate.
pub(super) fn chain_waterline(
    edit: &mut CircuitEditor,
    body: NodeId,
    aggregate: NodeId,
    next: &mut u64,
) -> Result<(NodeId, Bound), Error> {
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    let node = edit.node(aggregate)?;
    let stream = edit.node(node.edge("outputType")?)?;
    let key = edit.type_id(stream.edge("keyType")?)?;
    let row = edit.type_id(stream.edge("elementType")?)?;
    let projection = deindex_function(edit, node.edge("function")?, key, row, next)?;
    let rows = edit.zset_type(row)?;
    let deindex = edit.append_operator(
        body,
        aggregate,
        Op::Deindex,
        &[port(aggregate)],
        [("outputType", link(rows)), ("function", link(projection))],
    )?;

    let parameter = arg(edit, row, next)?;
    let columns = edit.node(row.into())?.edges("tupFields")?;
    let mut minima = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    for (i, id) in columns.into_iter().enumerate() {
        let nonnull = edit.with_nullability(edit.type_id(id)?, false)?;
        minima.push(edit.minimum_value(nonnull)?);
        let tuple = edit.dereference(parameter)?;
        let field = edit.tuple_field(tuple, i)?;
        let value = edit.clone_if_needed(field)?;
        fields.push(waterline_value(edit, aggregate, value)?);
    }
    let minimum = edit.tuple_expr(row.into(), false, &minima)?;
    let initial_type = edit.expression_type(minimum)?;
    let timestamp = edit.tuple_expr(row.into(), false, &fields)?;
    let maximum = timestamp_max(edit, aggregate, initial_type, next)?;
    let init = edit.closure_with_parameters(minimum, &[])?;
    let empty = edit.standard_type(row.into(), Ty::RawTuple)?;
    let weight = arg(edit, empty, next)?;
    let extract = edit.closure_from_variables(timestamp, &[parameter, weight])?;
    let max_type = edit.expression_type(maximum)?;
    let ty = edit.type_id(edit.node(max_type.into())?.edge("resultType")?)?;
    let clock = edit.append_operator(
        body,
        aggregate,
        Op::Waterline,
        &[port(deindex)],
        [
            ("outputType", link(ty)),
            ("function", link(maximum)),
            ("init", link(init)),
            ("extractTs", link(extract)),
        ],
    )?;

    let var = arg(edit, initial_type, next)?;
    let read = edit.dereference(var)?;
    let value = edit.clone_value(read)?;
    let key = edit.tuple_expr(row.into(), false, &[])?;
    let pair = edit.tuple_expr(row.into(), true, &[key, value])?;
    let extend = edit.closure_from_variables(pair, &[var])?;
    let bound = Bound {
        port: port(clock),
        ty,
    }
    .apply(edit, body, aggregate, extend)?;
    edit.annotate(
        bound.port.operator,
        serde_json::json!({"class":"Waterline"}),
    )?;
    Ok((deindex, bound))
}
