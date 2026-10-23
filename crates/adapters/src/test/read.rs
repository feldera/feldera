//! What output connectors write, read back into rows for the suites to check.
//!
//! The readers take the default JSON flavor and the CSV that the encoders write, and return
//! rows in the published form of the relation, so a suite compares them with [`Updates`] or a
//! [`ZSet`](feldera_adapterlib::test::ZSet).

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use arrow::array::RecordBatch;
use arrow::compute::{cast, concat_batches};
use bytes::Bytes;
use feldera_adapterlib::catalog::{RecordFormat, SerBatchReader};
use feldera_adapterlib::test::Updates;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{ColumnType, Relation, SqlType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value};

use crate::test::published_schema;

/// Returns the rows of `bytes`, CSV that the CSV encoder wrote for `relation`: each record
/// holds the columns of a row, then its weight.
///
/// An empty field of a nullable column is NULL. Fails for a column of a type that CSV does
/// not hold: ARRAY, MAP, STRUCT, and VARIANT.
pub fn csv_updates(relation: &Relation, bytes: &[u8]) -> AnyResult<Updates> {
    let mut rows = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);
    for record in reader.records() {
        let record = record?;
        if record.len() != relation.fields.len() + 1 {
            bail!(
                "a CSV record has {} fields, but relation '{}' has {} columns and a weight",
                record.len(),
                relation.name,
                relation.fields.len()
            );
        }
        let mut row = Map::new();
        for (field, text) in relation.fields.iter().zip(record.iter()) {
            row.insert(field.name.name(), csv_field(&field.columntype, text)?);
        }
        let weight = record[relation.fields.len()]
            .parse::<i64>()
            .with_context(|| format!("weight '{}'", &record[relation.fields.len()]))?;
        rows.push((weight, Value::Object(row).to_string()));
    }
    updates(relation, &rows)
}

/// Returns the rows of `bytes`, JSON that the JSON encoder wrote for `relation` in the
/// insert/delete format: objects, alone or in arrays, each with an `insert` or a `delete`
/// member.
pub fn insert_delete_updates(relation: &Relation, bytes: &[u8]) -> AnyResult<Updates> {
    let mut rows = Vec::new();
    for value in serde_json::Deserializer::from_slice(bytes).into_iter::<Value>() {
        let envelopes = match value? {
            Value::Array(envelopes) => envelopes,
            envelope => vec![envelope],
        };
        for envelope in envelopes {
            let mut object = match envelope {
                Value::Object(object) => object,
                other => bail!("not an insert/delete object: {other}"),
            };
            let (weight, row) = match (object.remove("insert"), object.remove("delete")) {
                (Some(row), None) => (1, row),
                (None, Some(row)) => (-1, row),
                (insert, delete) => bail!(
                    "not an insert/delete object: insert {insert:?}, delete {delete:?}, and {object:?}"
                ),
            };
            rows.push((weight, arrow_form(relation, row)?.to_string()));
        }
    }
    updates(relation, &rows)
}

/// Returns the rows of the Parquet file `bytes`, written for `relation`, each with weight 1.
///
/// Columns whose Parquet type reads back as another Arrow type than the published one are
/// cast to the published type.
pub fn parquet_updates(relation: &Relation, bytes: Vec<u8>) -> AnyResult<Updates> {
    let schema = published_schema(relation);
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?.build()?;
    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let column = batch
                    .column_by_name(field.name())
                    .ok_or_else(|| anyhow!("the Parquet file has no column '{}'", field.name()))?;
                Ok(cast(column, field.data_type())?)
            })
            .collect::<AnyResult<Vec<_>>>()?;
        batches.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    let rows = concat_batches(&schema, &batches)?;
    let weights = vec![1; rows.num_rows()];
    Updates::new(rows, weights)
}

/// Returns the changes in `batch`, an output batch of `relation`, in the order that an encoder
/// writes them: the deletes, then the inserts, each in the order of the batch's cursor and once
/// per unit of weight, with weight -1 or 1.
pub fn encoding_order(batch: &dyn SerBatchReader, relation: &Relation) -> AnyResult<Updates> {
    let mut deletes = Vec::new();
    let mut inserts = Vec::new();
    let mut cursor = batch.cursor(RecordFormat::Json(JsonFlavor::Default))?;
    while cursor.key_valid() {
        while cursor.val_valid() {
            let mut row = Vec::new();
            cursor.serialize_key(&mut row)?;
            let row = arrow_form(relation, serde_json::from_slice(&row)?)?.to_string();
            let weight = cursor.weight();
            let changes = if weight < 0 {
                &mut deletes
            } else {
                &mut inserts
            };
            for _ in 0..weight.unsigned_abs() {
                changes.push((weight.signum(), row.clone()));
            }
            cursor.step_val();
        }
        cursor.step_key()?;
    }
    deletes.append(&mut inserts);
    updates(relation, &deletes)
}

/// Returns `row`, a JSON object in the default JSON flavor, in the form that arrow-json reads
/// into the published form of `relation`.
pub fn arrow_form(relation: &Relation, row: Value) -> AnyResult<Value> {
    let Value::Object(mut row) = row else {
        bail!("a row of '{}' is not a JSON object: {row}", relation.name);
    };
    let mut converted = Map::new();
    for field in &relation.fields {
        let name = field.name.name();
        let value = row.remove(&name).unwrap_or(Value::Null);
        converted.insert(name, arrow_value(&field.columntype, value)?);
    }
    if let Some(name) = row.keys().next() {
        bail!("relation '{}' has no column '{name}'", relation.name);
    }
    Ok(Value::Object(converted))
}

/// Returns `value`, of `column` in the default JSON flavor, as arrow-json reads it into the
/// published form of the column.
fn arrow_value(column: &ColumnType, value: Value) -> AnyResult<Value> {
    // A VARIANT column that is not nullable holds a JSON null as the variant null.
    if value.is_null() && (column.typ != SqlType::Variant || column.nullable) {
        return Ok(value);
    }
    Ok(match column.typ {
        SqlType::Binary | SqlType::Varbinary => {
            let Value::Array(bytes) = value else {
                bail!("a binary value is not an array of bytes: {value}");
            };
            let hex: String = bytes
                .iter()
                .map(|byte| {
                    byte.as_u64()
                        .filter(|byte| *byte <= 255)
                        .map(|byte| format!("{byte:02x}"))
                        .ok_or_else(|| anyhow!("{byte} is not a byte"))
                })
                .collect::<AnyResult<_>>()?;
            Value::from(hex)
        }
        SqlType::Variant => Value::from(value.to_string()),
        SqlType::Array => {
            let component = column
                .component
                .as_ref()
                .expect("an ARRAY has a component type");
            let Value::Array(elements) = value else {
                bail!("an ARRAY value is not an array: {value}");
            };
            Value::Array(
                elements
                    .into_iter()
                    .map(|element| arrow_value(component, element))
                    .collect::<AnyResult<_>>()?,
            )
        }
        SqlType::Map => {
            let value_type = column.value.as_ref().expect("a MAP has a value type");
            let Value::Object(entries) = value else {
                bail!("a MAP value is not an object: {value}");
            };
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| Ok((key, arrow_value(value_type, value)?)))
                    .collect::<AnyResult<_>>()?,
            )
        }
        SqlType::Struct => {
            let fields = column.fields.as_ref().expect("a STRUCT has fields");
            let Value::Object(mut members) = value else {
                bail!("a STRUCT value is not an object: {value}");
            };
            let mut converted = Map::new();
            for field in fields {
                let name = field.name.name();
                let member = members.remove(&name).unwrap_or(Value::Null);
                converted.insert(name, arrow_value(&field.columntype, member)?);
            }
            Value::Object(converted)
        }
        _ => value,
    })
}

/// Returns the CSV field `text` of `column` as arrow-json reads it.
fn csv_field(column: &ColumnType, text: &str) -> AnyResult<Value> {
    if text.is_empty() && column.nullable {
        return Ok(Value::Null);
    }
    Ok(match column.typ {
        SqlType::Boolean => Value::from(text.parse::<bool>().with_context(|| format!("'{text}'"))?),
        SqlType::TinyInt
        | SqlType::SmallInt
        | SqlType::Int
        | SqlType::BigInt
        | SqlType::UTinyInt
        | SqlType::USmallInt
        | SqlType::UInt
        | SqlType::UBigInt
        | SqlType::Real
        | SqlType::Double => {
            serde_json::from_str::<Value>(text).with_context(|| format!("'{text}'"))?
        }
        SqlType::Array | SqlType::Map | SqlType::Struct | SqlType::Variant => {
            bail!("CSV holds no value of type {:?}", column.typ)
        }
        _ => Value::from(text),
    })
}

/// Returns `rows`, JSON objects in the arrow-json form, with their weights.
fn updates(relation: &Relation, rows: &[(i64, String)]) -> AnyResult<Updates> {
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    Updates::from_json(published_schema(relation), &rows)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use feldera_types::program_schema::{ColumnType, Field, Relation};
    use serde_json::json;

    use super::arrow_form;

    /// A JSON null is the variant null in a VARIANT column that is not nullable, and SQL NULL
    /// in one that is.
    #[test]
    fn a_null_variant_is_the_variant_null_unless_the_column_is_nullable() {
        let relation = Relation::new(
            "variants".into(),
            vec![
                Field::new("v".into(), ColumnType::variant(false)),
                Field::new("n".into(), ColumnType::variant(true)),
            ],
            false,
            BTreeMap::new(),
        );
        assert_eq!(
            arrow_form(&relation, json!({ "v": null, "n": null })).unwrap(),
            json!({ "v": "null", "n": null })
        );
    }
}
