//! Lower one timestamp bound through a direct view, with optional DISTINCT.

use crate::decode::{
    array, boolean, encode_rewrite, field,
    generated_closure::lateness as closure,
    inner_reference, text, uint,
    wire_edits::{copy_origin, operator, port, WireEdits},
    wire_port, Definitions,
};
use crate::Result;
use serde_json::{json, Value};

use super::view_needs_bounds;

pub(super) fn rewrite(circuit: &Value) -> Result<Value> {
    let operators = array(circuit, "allOperators")?;
    if !boolean(circuit, "incremental")? {
        return Ok(Value::Null);
    }
    let Some(plan) = Plan::recognize(operators)? else {
        return Ok(Value::Null);
    };
    let mut edits = WireEdits::new(circuit)?;
    let Some((source_type, timestamp)) = plan.timestamp_source(edits.definitions())? else {
        return Ok(Value::Null);
    };
    let first_name = edits.generated_name()?;
    let name_count = if plan.retain.is_some() {
        closure::NAME_COUNT
    } else {
        closure::NAME_COUNT - 2
    };
    for _ in 1..name_count {
        edits.generated_name()?;
    }
    let functions = edits.with_inner_ids(|definitions, mut fresh| {
        closure::build(definitions, timestamp, first_name, &mut fresh)
    })?;
    let mut graph = Vec::with_capacity(operators.len() + 6);
    let mut bounds = None;
    for before in operators {
        let id = edits.remember_type(before)?;
        let mut after = if std::ptr::eq(before, plan.data_consumer) {
            let Some(bounds) = &bounds else {
                return Ok(Value::Null);
            };
            edits.retain_with_inputs(before, id, &[port(*bounds)])?
        } else if std::ptr::eq(before, plan.error_view) {
            let Some(bounds) = bounds else {
                return Ok(Value::Null);
            };
            let error = json!({"operator": {"node": bounds}, "outputNumber": 1});
            edits.retain_with_inputs(before, id, &[error])?
        } else {
            edits.retain(before, id)?
        };
        edits.retype_consumer(&mut after, id)?;
        graph.push(after);
        if std::ptr::eq(before, plan.source) {
            bounds = Some(insert_bounds(
                &mut edits,
                &mut graph,
                &plan,
                &functions,
                source_type,
            )?);
        }
    }
    encode_rewrite(circuit, &edits.finish(circuit, graph))
}

pub(super) fn uses_distinct_bound(circuit: &Value) -> Result<bool> {
    if !boolean(circuit, "incremental")? {
        return Ok(false);
    }
    let operators = array(circuit, "allOperators")?;
    let Some(plan) = Plan::recognize(operators)? else {
        return Ok(false);
    };
    Ok(plan.retain.is_some()
        && operators.len() == 7
        && plan
            .timestamp_source(&Definitions::read(circuit)?)?
            .is_some())
}

struct Plan<'a> {
    source: &'a Value,
    data_consumer: &'a Value,
    retain: Option<&'a Value>,
    error_view: &'a Value,
    index: usize,
    lateness: &'a Value,
}

impl<'a> Plan<'a> {
    fn timestamp_source(
        &self,
        definitions: &Definitions<'a>,
    ) -> Result<Option<(&'a Value, closure::TimestampSource<'a>)>> {
        let collection = definitions.resolve(field(self.source, "outputType")?)?;
        let row = definitions.resolve(field(collection, "elementType")?)?;
        let Some(column) = array(row, "tupFields")?.get(self.index) else {
            return Ok(None);
        };
        let column = definitions.resolve(column)?;
        let interval = definitions.resolve(self.lateness)?;
        if collection["class"] != "DBSPTypeZSet"
            || column["class"] != "DBSPTypeTimestamp"
            || column["mayBeNull"] != true
            || interval["class"] != "DBSPShortIntervalLiteral"
            || interval["value"]
                .as_str()
                .and_then(|value| value.parse::<i64>().ok())
                .is_none_or(|value| value < 0)
        {
            return Ok(None);
        }
        Ok(Some((
            collection,
            closure::TimestampSource {
                table: text(field(self.source, "tableName")?, "name")?,
                row,
                index: self.index,
                column,
                interval,
            },
        )))
    }

    fn recognize(operators: &'a [Value]) -> Result<Option<Self>> {
        let Some(error_index) = operators.iter().position(|node| {
            node["class"] == "DBSPSourceMultisetOperator"
                && node["tableName"]["name"] == "feldera_error_table"
        }) else {
            return Ok(None);
        };
        let (data_path, error_path) = operators.split_at(error_index);
        let [source, middle @ .., sink] = data_path else {
            return Ok(None);
        };
        let (data_consumer, retain) = match middle {
            [] => (sink, None),
            [distinct] if distinct["class"] == "DBSPDistinctOperator" && reads(sink, distinct)? => {
                (distinct, Some(distinct))
            }
            _ => return Ok(None),
        };
        let [error_source, error_view, error_path @ .., error_sink] = error_path else {
            return Ok(None);
        };
        let error_output = match error_path {
            [] => error_view,
            [distinct]
                if distinct["class"] == "DBSPDistinctOperator" && reads(distinct, error_view)? =>
            {
                distinct
            }
            _ => return Ok(None),
        };
        if source["class"] != "DBSPSourceMultisetOperator"
            || source["tableName"]["name"] == "now"
            || source["metadata"]["changes"] != "Unrestricted"
            || source["metadata"]["materialized"] != false
            || sink["class"] != "DBSPSinkOperator"
            || error_source["class"] != "DBSPSourceMultisetOperator"
            || error_source["tableName"]["name"] != "feldera_error_table"
            || error_view["class"] != "DBSPViewOperator"
            || error_view["viewName"]["name"] != "error_view"
            || error_sink["class"] != "DBSPSinkOperator"
        {
            return Ok(None);
        }
        for (input, consumer) in [
            (source, data_consumer),
            (error_source, error_view),
            (error_output, error_sink),
        ] {
            if !reads(consumer, input)? {
                return Ok(None);
            }
        }
        for output in [sink, error_view, error_sink] {
            if view_needs_bounds(field(output, "metadata")?)? {
                return Ok(None);
            }
        }
        let columns = array(&source["metadata"], "columnMetadata")?;
        if columns.iter().any(|column| column["isPrimaryKey"] != false) {
            return Ok(None);
        }
        let mut bounded = columns
            .iter()
            .enumerate()
            .filter(|(_, column)| !column["lateness"].is_null());
        let Some((index, column)) = bounded.next() else {
            return Ok(None);
        };
        if bounded.next().is_some() {
            return Ok(None);
        }
        Ok(Some(Self {
            source,
            data_consumer,
            retain,
            error_view,
            index,
            lateness: &column["lateness"],
        }))
    }
}

fn reads(consumer: &Value, producer: &Value) -> Result<bool> {
    let [input] = array(consumer, "inputs")? else {
        return Ok(false);
    };
    let input = wire_port(input)?;
    Ok(input.output == 0 && input.operator == uint(producer, "id")?)
}

fn insert_bounds(
    edits: &mut WireEdits<'_>,
    graph: &mut Vec<Value>,
    plan: &Plan<'_>,
    functions: &closure::Functions,
    source_type: &Value,
) -> Result<u64> {
    let input = port(uint(plan.source, "id")?);
    let waterline = edits.id()?;
    let delay = edits.id()?;
    let filter = edits.id()?;
    let extend = edits.id()?;
    let bound_type = inner_reference(&functions.waterline_type)?;
    let row_type = inner_reference(source_type)?;
    let mut waterline_op = operator(
        "DBSPWaterlineOperator",
        waterline,
        vec![input.clone()],
        functions.waterline_type.clone(),
    );
    waterline_op["function"] = functions.waterline_max.clone();
    waterline_op["init"] = functions.waterline_init.clone();
    waterline_op["extractTs"] = functions.waterline_extract.clone();

    let mut delay_op = operator(
        "DBSPDelayOperator",
        delay,
        vec![port(waterline)],
        bound_type.clone(),
    );
    delay_op["function"] = functions.minimum.clone();
    let mut extend_op = operator(
        "DBSPApplyOperator",
        extend,
        vec![port(waterline)],
        bound_type.clone(),
    );
    extend_op["function"] = functions.clone_waterline.clone();
    extend_op["annotations"] = json!([{"class": "Waterline"}]);

    let mut filter_op = operator(
        "DBSPControlledKeyFilterOperator",
        filter,
        vec![input, port(delay)],
        row_type.clone(),
    );
    filter_op["errorType"] = functions.error_type.clone();
    filter_op["error"] = functions.error.clone();
    filter_op["function"] = functions.filter.clone();
    edits.remember_generated_output(filter, source_type)?;
    edits.remember_error_output(filter, &functions.error_type)?;

    for mut node in [waterline_op, delay_op, extend_op, filter_op] {
        copy_origin(plan.source, &mut node);
        graph.push(node);
    }
    if let Some(distinct) = plan.retain {
        let delay = edits.id()?;
        let mut delay_op = operator("DBSPDelayOperator", delay, vec![port(extend)], bound_type);
        delay_op["function"] = functions.retain_minimum.clone();
        copy_origin(plan.source, &mut delay_op);
        graph.push(delay_op);

        let mut retain_op = operator(
            "DBSPIntegrateTraceRetainKeysOperator",
            edits.id()?,
            vec![port(filter), port(delay)],
            row_type,
        );
        retain_op["function"] = functions.retain.clone();
        retain_op["accumulate"] = json!(true);
        copy_origin(distinct, &mut retain_op);
        graph.push(retain_op);
    }
    Ok(filter)
}
