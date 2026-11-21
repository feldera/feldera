//! Expand primary-key integer sources into indexed streams and reconstruct rows.

use super::{
    array, boolean, encode_rewrite, field, generated_closure, inner_reference, text,
    wire_edits::{operator, port, WireEdits},
    wire_port,
};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

pub(crate) fn expand(circuit: &Value, dedup_keys: bool) -> Result<Value> {
    let original = array(circuit, "allOperators")?;
    let mut successors = HashMap::<_, Vec<_>>::new();
    let mut ops = Vec::with_capacity(original.len());
    let mut edits = WireEdits::new(circuit)?;
    for node in original {
        let class = text(node, "class")?;
        for input in array(node, "inputs")? {
            successors
                .entry(wire_port(input)?.operator)
                .or_default()
                .push(class);
        }
    }

    for node in original {
        let class = text(node, "class")?;
        let id = edits.remember_type(node)?;
        let consumers = successors.get(&id).map(Vec::as_slice).unwrap_or_default();
        if consumers.len() > 1
            && !matches!(class, "DBSPConstantOperator" | "DBSPNestedOperator")
            && consumers.iter().all(|class| {
                matches!(
                    *class,
                    "DBSPMapOperator" | "DBSPMapIndexOperator" | "DBSPDeindexOperator"
                )
            })
        {
            return Err(Error(
                "indexed-input rewrite requires common projection handling".into(),
            ));
        }
        if class == "DBSPNestedOperator"
            && array(node, "allOperators")?.iter().any(|child| {
                matches!(
                    child["class"].as_str(),
                    Some("DBSPSourceMultisetOperator" | "DBSPNestedOperator")
                )
            })
        {
            return Err(outside_scope());
        }

        if class == "DBSPSourceMultisetOperator" {
            let columns = array(field(node, "metadata")?, "columnMetadata")?;
            let keys = columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| match boolean(column, "isPrimaryKey") {
                    Ok(true) => Some(Ok(index)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>>>()?;
            if !keys.is_empty() {
                if !matches!(consumers, ["DBSPMapOperator"] | ["DBSPDistinctOperator"]) {
                    return Err(outside_scope());
                }
                let (source, row) =
                    expand_source(&mut edits, node, id, columns, &keys, dedup_keys)?;
                ops.extend([source, row]);
                continue;
            }
        }

        let mut result = edits.retain(node, id)?;
        edits.retype_consumer(&mut result, id)?;
        ops.push(result);
    }
    let rewritten = edits.finish(circuit, ops);
    encode_rewrite(circuit, &rewritten)
}

fn expand_source(
    edits: &mut WireEdits<'_>,
    node: &Value,
    id: u64,
    columns: &[Value],
    keys: &[usize],
    dedup_keys: bool,
) -> Result<(Value, Value)> {
    let definitions = edits.definitions();
    let collection = definitions.resolve(field(node, "outputType")?)?;
    let old_row = definitions.resolve(field(collection, "elementType")?)?;
    if collection["class"] != "DBSPTypeZSet"
        || old_row["class"] != "DBSPTypeTuple"
        || !array(node, "annotations")?.is_empty()
        || node["provenance"]["kind"] != "RelAnd"
    {
        return Err(outside_scope());
    }
    let mut key_fields = Vec::new();
    let mut value_fields = Vec::new();
    let mut layout = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        let ty = definitions.resolve(field(column, "type")?)?;
        if ty["class"] != "DBSPTypeInteger" {
            return Err(outside_scope());
        }
        let (part, fields) = if keys.contains(&index) {
            (0, &mut key_fields)
        } else {
            (1, &mut value_fields)
        };
        layout.push((part, fields.len(), ty));
        fields.push(inner_reference(ty)?);
    }
    let position = field(field(node, "provenance")?, "position")?;
    let key = tuple_type(edits, key_fields)?;
    let row = if dedup_keys {
        tuple_type(edits, value_fields)?
    } else {
        old_row.clone()
    };
    let type_use = |value: &Value| {
        value
            .clone()
            .try_into()
            .map_err(|e: feldera_sql_ir::Error| Error(e.to_string()))
    };
    let collection = feldera_sql_ir::build::CollectionType::IndexedZSet {
        key: type_use(&key)?,
        element: type_use(&row)?,
    }
    .into_wire(edits.inner_id()?, Some(position));
    let source_id = edits.id()?;
    let mut source = node.clone();
    source["id"] = source_id.into();
    source["class"] = "DBSPSourceMapOperator".into();
    source["keyFields"] = json!(keys);
    source["outputType"] = collection;
    edits.remember_generated_output(source_id, &source["outputType"])?;

    let name = edits.generated_name()?;
    let (function, output_type) = if dedup_keys {
        generated_closure::reconstruct_indexed_row(&key, &row, &layout, name, &mut || {
            edits.inner_id()
        })?
    } else {
        generated_closure::initial_deindex(&key, &row, None, name, &mut || edits.inner_id())?
    };
    let class = if dedup_keys {
        "DBSPMapOperator"
    } else {
        "DBSPDeindexOperator"
    };
    edits.remember_generated_output(id, &output_type)?;
    edits.replaced(id);
    let mut reconstruct = operator(class, id, vec![port(source_id)], output_type);
    reconstruct["provenance"] = node["provenance"].clone();
    reconstruct["function"] = function;
    Ok((source, reconstruct))
}

fn tuple_type(edits: &mut WireEdits<'_>, fields: Vec<Value>) -> Result<Value> {
    Ok(json!({
        "class": "DBSPTypeTuple", "id": edits.inner_id()?, "sourceEmpty": true,
        "tupFields": fields, "code": "TUPLE", "mayBeNull": false
    }))
}

fn outside_scope() -> Error {
    Error("indexed input outside the supported key expansion slice".into())
}
