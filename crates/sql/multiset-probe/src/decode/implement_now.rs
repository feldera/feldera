//! Expand direct NOW() projections over integer tuple streams.

use super::{
    array, boolean, encode_rewrite, field, generated_closure,
    generated_closure::clock,
    inner_reference, remove_system_table, text, uint,
    wire_edits::{operator, port, WireEdits},
    wire_port, Definitions,
};
use crate::{provenance::Provenance, Error, Result};
use serde_json::{json, Value};

pub(crate) fn rewrite(request: &Value) -> Result<Value> {
    let circuit = field(request, "circuit")?;
    let definitions = Definitions::read(circuit)?;
    let calls = definitions
        .inner
        .values()
        .filter_map(|node| match is_now(&definitions, node) {
            Ok(true) => Some(Ok(*node)),
            Ok(false) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>>>()?;
    if calls.is_empty() {
        return Ok(json!({"circuit": remove_system_table(circuit, "now")?, "warnings": []}));
    }
    let [call] = calls.as_slice() else {
        return Err(outside_scope());
    };
    if !boolean(request, "now_stream")? {
        return Err(outside_scope());
    }
    let weight_name = text(request, "weight_variable")?;
    let position = field(call, "position")?;
    let result = expand_projection(circuit, &definitions, call, weight_name, position)?;
    Ok(json!({"circuit": result, "warnings": [position]}))
}

fn is_now<'a>(definitions: &Definitions<'a>, node: &'a Value) -> Result<bool> {
    if node["class"] != "DBSPApplyExpression" || !array(node, "arguments")?.is_empty() {
        return Ok(false);
    }
    Ok(definitions
        .call_name(node)?
        .is_some_and(|name| name.eq_ignore_ascii_case("now")))
}

fn expand_projection<'a>(
    circuit: &'a Value,
    definitions: &Definitions<'a>,
    call: &Value,
    weight_name: &str,
    position: &Value,
) -> Result<Value> {
    let original = array(circuit, "allOperators")?;
    let [now, ..] = original else {
        return Err(outside_scope());
    };
    if now["class"] != "DBSPSourceMultisetOperator" || now["tableName"]["name"] != "now" {
        return Err(outside_scope());
    }
    let mut edits = WireEdits::new(circuit)?;
    let mut origins = Provenance::read(original)?;
    let mut ops = Vec::with_capacity(original.len() + 5);
    let now_id = edits.remember_type(now)?;
    ops.push(now.clone());
    let timestamp = json!({
        "class": "DBSPTypeTimestamp", "id": edits.inner_id()?,
        "sourceEmpty": true, "singleton": true, "code": "TIMESTAMP", "mayBeNull": false,
    });
    let clock_origin = origins.empty_origin()?;
    let clock = add_clock_trace(
        &mut edits,
        now_id,
        &timestamp,
        weight_name,
        &clock_origin,
        &mut ops,
    )?;

    let inner_id = uint(call, "id")?;
    let mut rewritten = false;
    for node in &original[1..] {
        let id = edits.remember_type(node)?;
        if node["class"] == "DBSPMapOperator" {
            let function = definitions.resolve(field(node, "function")?)?;
            let body = definitions.resolve(field(function, "body")?)?;
            if body["class"] == "DBSPTupleExpression" {
                let values = array(body, "fields")?
                    .iter()
                    .map(|value| definitions.resolve(value))
                    .collect::<Result<Vec<_>>>()?;
                if values.iter().any(|value| value["id"] == inner_id) {
                    if rewritten {
                        return Err(outside_scope());
                    }
                    let [input] = array(node, "inputs")? else {
                        return Err(outside_scope());
                    };
                    let input = wire_port(input)?;
                    let producer = original
                        .iter()
                        .find(|op| op["id"] == input.operator)
                        .ok_or_else(outside_scope)?;
                    let collection = definitions.resolve(field(producer, "outputType")?)?;
                    let row = definitions.resolve(field(collection, "elementType")?)?;
                    if input.output != 0
                        || collection["class"] != "DBSPTypeZSet"
                        || row["class"] != "DBSPTypeTuple"
                        || array(row, "tupFields")?.iter().any(|ty| {
                            definitions
                                .resolve(ty)
                                .map_or(true, |ty| ty["class"] != "DBSPTypeInteger")
                        })
                    {
                        return Err(outside_scope());
                    }
                    let (parameter, fields) =
                        projection_fields(definitions, function, &values, inner_id, row)?;
                    let origin =
                        origins.with_source_position(field(node, "provenance")?, position)?;
                    let [index, join, mut map] =
                        projection_join(&mut edits, node, input.operator, row, &clock, origin)?;
                    let parameter_type = definitions.resolve(field(parameter, "type")?)?;
                    let parameter_row = definitions.resolve(field(parameter_type, "type")?)?;
                    let function = clock::projection(
                        body,
                        parameter_row,
                        &timestamp,
                        text(parameter, "name")?,
                        &fields,
                        &mut || edits.inner_id(),
                    )?;
                    map["function"] = function;
                    ops.extend([index, join, map]);
                    edits.replaced(id);
                    rewritten = true;
                    continue;
                }
            }
        }
        let mut result = edits.retain(node, id)?;
        edits.retype_consumer(&mut result, id)?;
        ops.push(result);
    }
    if !rewritten {
        return Err(outside_scope());
    }
    let rewritten = edits.finish(circuit, ops);
    encode_rewrite(circuit, &rewritten)
}

struct Clock {
    stream: u64,
    row: Value,
}

fn add_clock_trace(
    edits: &mut WireEdits<'_>,
    now: u64,
    timestamp: &Value,
    weight_name: &str,
    origin: &Value,
    ops: &mut Vec<Value>,
) -> Result<Clock> {
    let name = edits.generated_name()?;
    let index = clock::index(clock::IndexInput::Clock(timestamp), name, &mut || {
        edits.inner_id()
    })?;
    let index_id = edits.id()?;
    let index_type = inner_reference(&index.collection)?;
    let mut indexed = operator(
        "DBSPMapIndexOperator",
        index_id,
        vec![port(now)],
        index.collection,
    );
    indexed["function"] = index.function;
    indexed["provenance"] = origin.clone();
    ops.push(indexed);

    let timestamp_name = edits.generated_name()?;
    let accumulator_name = edits.generated_name()?;
    let aggregate = clock::aggregate(
        timestamp,
        weight_name,
        timestamp_name,
        accumulator_name,
        &mut || edits.inner_id(),
    )?;
    let stream = edits.id()?;
    let mut trace = operator(
        "DBSPChainAggregateOperator",
        stream,
        vec![port(index_id)],
        index_type,
    );
    trace["function"] = aggregate.update;
    trace["init"] = aggregate.init;
    trace["annotations"] = json!([{"class": "AlwaysMonotone"}]);
    trace["provenance"] = origin.clone();
    ops.push(trace);

    let name = edits.generated_name()?;
    let (function, output) =
        generated_closure::initial_deindex(&index.key, &index.row, None, name, &mut || {
            edits.inner_id()
        })?;
    let mut deindex = operator(
        "DBSPDeindexOperator",
        edits.id()?,
        vec![port(stream)],
        output,
    );
    deindex["function"] = function;
    deindex["provenance"] = origin.clone();
    ops.push(deindex);
    Ok(Clock {
        stream,
        row: index.row,
    })
}

fn projection_join(
    edits: &mut WireEdits<'_>,
    map: &Value,
    source: u64,
    row: &Value,
    clock: &Clock,
    join_origin: Value,
) -> Result<[Value; 3]> {
    let origin = field(map, "provenance")?;
    let name = edits.generated_name()?;
    let index = clock::index(clock::IndexInput::Relation(row), name, &mut || {
        edits.inner_id()
    })?;
    let index_id = edits.id()?;
    let mut indexed = operator(
        "DBSPMapIndexOperator",
        index_id,
        vec![port(source)],
        index.collection,
    );
    indexed["function"] = index.function;
    indexed["provenance"] = origin.clone();

    let names = [
        edits.generated_name()?,
        edits.generated_name()?,
        edits.generated_name()?,
    ];
    let (function, output) =
        clock::join(&index.key, row, &clock.row, names, &mut || edits.inner_id())?;
    let joined_id = edits.id()?;
    let mut join = operator(
        "DBSPStreamJoinOperator",
        joined_id,
        vec![port(index_id), port(clock.stream)],
        output,
    );
    join["balanced"] = false.into();
    join["function"] = function;
    join["provenance"] = join_origin;
    let mut map = map.clone();
    map["inputs"] = json!([port(joined_id)]);
    Ok([indexed, join, map])
}

fn projection_fields<'a>(
    definitions: &Definitions<'a>,
    function: &'a Value,
    values: &[&'a Value],
    now: u64,
    row: &Value,
) -> Result<(&'a Value, Vec<usize>)> {
    let [parameter] = array(function, "parameters")? else {
        return Err(outside_scope());
    };
    let parameter = definitions.resolve(parameter)?;
    let variable = text(parameter, "name")?;
    let width = array(row, "tupFields")?.len();
    let fields = values
        .iter()
        .map(|value| {
            if value["id"] == now {
                return Ok(width);
            }
            if value["class"] != "DBSPFieldExpression" {
                return Err(outside_scope());
            }
            let deref = definitions.resolve(field(value, "expression")?)?;
            if deref["class"] != "DBSPDerefExpression" {
                return Err(outside_scope());
            }
            let binding = definitions.resolve(field(deref, "expression")?)?;
            let index = usize::try_from(uint(value, "fieldNo")?).map_err(|_| outside_scope())?;
            if binding["class"] != "DBSPVariablePath"
                || binding["variable"] != variable
                || index >= width
            {
                return Err(outside_scope());
            }
            Ok(index)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((parameter, fields))
}

fn outside_scope() -> Error {
    Error("NOW implementation outside the supported tuple projection slice".into())
}
