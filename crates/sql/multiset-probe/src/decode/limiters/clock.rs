//! Omit analysis-only waterlines for a directly projected system clock.

use crate::decode::{array, boolean, field, text, uint, wire_port, Definitions};
use crate::Result;
use serde_json::Value;

// The Java allocator visits the clock waterline, the copy through the
// integrator boundary, and both join bound projections. Preserve the next
// name when the bounded rewrite chooses not to materialize their streams.
const RESERVED_BOUND_NAMES: usize = 16;

pub(super) fn unused_bound_names(circuit: &Value) -> Result<Option<usize>> {
    if !boolean(circuit, "incremental")? {
        return Ok(None);
    }
    let [clock, clock_index, maximum, source, data_index, noop, join, distinct, sink, error_source, error_view, error_distinct, error_sink] =
        array(circuit, "allOperators")?
    else {
        return Ok(None);
    };
    for (node, class) in [
        (clock, "DBSPSourceMultisetOperator"),
        (clock_index, "DBSPMapIndexOperator"),
        (maximum, "DBSPChainAggregateOperator"),
        (source, "DBSPSourceMultisetOperator"),
        (data_index, "DBSPMapIndexOperator"),
        (noop, "DBSPNoopOperator"),
        (join, "DBSPJoinOperator"),
        (distinct, "DBSPDistinctOperator"),
        (sink, "DBSPSinkOperator"),
        (error_source, "DBSPSourceMultisetOperator"),
        (error_view, "DBSPViewOperator"),
        (error_distinct, "DBSPDistinctOperator"),
        (error_sink, "DBSPSinkOperator"),
    ] {
        if node["class"] != class {
            return Ok(None);
        }
    }
    if clock["tableName"]["name"] != "now"
        || matches!(
            source["tableName"]["name"].as_str(),
            None | Some("now" | "feldera_error_table")
        )
        || error_source["tableName"]["name"] != "feldera_error_table"
        || error_view["viewName"]["name"] != "error_view"
        || !array(maximum, "annotations")?
            .iter()
            .any(|ann| ann["class"] == "AlwaysMonotone")
    {
        return Ok(None);
    }
    for input in [clock, source, error_source] {
        let metadata = field(input, "metadata")?;
        if metadata["changes"] != "Unrestricted"
            || metadata["materialized"] != false
            || array(metadata, "columnMetadata")?
                .iter()
                .any(|c| !c["lateness"].is_null())
        {
            return Ok(None);
        }
    }
    for output in [sink, error_view, error_sink] {
        if super::view_needs_bounds(field(output, "metadata")?)? {
            return Ok(None);
        }
    }
    for (consumer, inputs) in [
        (clock_index, vec![clock]),
        (maximum, vec![clock_index]),
        (data_index, vec![source]),
        (noop, vec![maximum]),
        (join, vec![data_index, noop]),
        (distinct, vec![join]),
        (sink, vec![distinct]),
        (error_view, vec![error_source]),
        (error_distinct, vec![error_view]),
        (error_sink, vec![error_distinct]),
    ] {
        let actual = array(consumer, "inputs")?;
        if actual.len() != inputs.len() {
            return Ok(None);
        }
        for (input, expected) in actual.iter().zip(inputs) {
            let actual = wire_port(input)?;
            if actual.output != 0 || actual.operator != uint(expected, "id")? {
                return Ok(None);
            }
        }
    }

    let definitions = Definitions::read(circuit)?;
    for (index, class) in [
        (clock_index, "DBSPTypeTimestamp"),
        (data_index, "DBSPTypeInteger"),
    ] {
        if !unkeyed_single_column(&definitions, index, class)? {
            return Ok(None);
        }
    }
    let aggregate = definitions.resolve(field(maximum, "function")?)?;
    let body = definitions.resolve(field(aggregate, "body")?)?;
    let [increment] = array(body, "fields")? else {
        return Ok(None);
    };
    let increment = definitions.resolve(increment)?;
    if increment["class"] != "DBSPConditionalIncrementExpression"
        || increment["opcode"] != "AGG_MAX"
    {
        return Ok(None);
    }
    let function = definitions.resolve(field(join, "function")?)?;
    let [_, left, right] = array(function, "parameters")? else {
        return Ok(None);
    };
    let body = definitions.resolve(field(function, "body")?)?;
    let [first, second] = array(body, "fields")? else {
        return Ok(None);
    };
    let left = text(definitions.resolve(left)?, "name")?;
    let right = text(definitions.resolve(right)?, "name")?;
    if body["class"] != "DBSPTupleExpression"
        || !direct_field(&definitions, first, left)?
        || !direct_field(&definitions, second, right)?
    {
        return Ok(None);
    }

    // The Cartesian join has no varying key to retain. Its unrestricted
    // input can change every previously emitted combination, so the bound on
    // the clock's value alone cannot be used to retain the join's result.
    // There is no final-output or error consumer of the clock waterline.
    Ok(Some(RESERVED_BOUND_NAMES))
}

fn unkeyed_single_column<'a>(
    definitions: &Definitions<'a>,
    index: &'a Value,
    class: &str,
) -> Result<bool> {
    let collection = definitions.resolve(field(index, "outputType")?)?;
    let key = definitions.resolve(field(collection, "keyType")?)?;
    let row = definitions.resolve(field(collection, "elementType")?)?;
    let [column] = array(row, "tupFields")? else {
        return Ok(false);
    };
    Ok(collection["class"] == "DBSPTypeIndexedZSet"
        && key["class"] == "DBSPTypeTuple"
        && array(key, "tupFields")?.is_empty()
        && row["class"] == "DBSPTypeTuple"
        && definitions.resolve(column)?["class"] == class)
}

fn direct_field<'a>(
    definitions: &Definitions<'a>,
    node: &'a Value,
    variable: &str,
) -> Result<bool> {
    let node = definitions.resolve(node)?;
    if node["class"] != "DBSPFieldExpression" || node["fieldNo"] != 0 {
        return Ok(false);
    }
    let read = definitions.resolve(field(node, "expression")?)?;
    if read["class"] != "DBSPDerefExpression" {
        return Ok(false);
    }
    let read = definitions.resolve(field(read, "expression")?)?;
    Ok(read["class"] == "DBSPVariablePath" && read["variable"] == variable)
}
