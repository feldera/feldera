//! Rows written in the input formats, for the suites to feed connectors with.
//!
//! Each function writes rows in the published form of a relation the way the default JSON
//! flavor or CSV writes them, so that a parser of that format reads back the same rows.

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time64NanosecondType, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use chrono::{DateTime, NaiveTime, TimeDelta};
use feldera_types::program_schema::{ColumnType, Relation, SqlType};
use serde_json::{Map, Value};

/// Returns row `index` of `rows`, which are in the published form of `relation`, as a JSON
/// object in the default JSON flavor.
pub fn json_object(relation: &Relation, rows: &RecordBatch, index: usize) -> Value {
    Value::Object(
        relation
            .fields
            .iter()
            .zip(rows.columns())
            .map(|(field, column)| {
                (
                    field.name.name(),
                    json_value(&field.columntype, column.as_ref(), index),
                )
            })
            .collect(),
    )
}

/// Returns the rows of `rows`, which are in the published form of `relation`, as JSON objects
/// in the default JSON flavor, one per line.
pub fn json_lines(relation: &Relation, rows: &RecordBatch) -> String {
    (0..rows.num_rows())
        .map(|index| format!("{}\n", json_object(relation, rows, index)))
        .collect()
}

/// Returns the rows of `rows`, which are in the published form of `relation`, as CSV records
/// without a header.
///
/// A NULL is an empty field. Panics for a column of a type that CSV does not hold: ARRAY,
/// MAP, STRUCT, and VARIANT.
pub fn csv(relation: &Relation, rows: &RecordBatch) -> Vec<u8> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    for index in 0..rows.num_rows() {
        let record: Vec<String> = relation
            .fields
            .iter()
            .zip(rows.columns())
            .map(
                |(field, column)| match json_value(&field.columntype, column.as_ref(), index) {
                    Value::Null => String::new(),
                    Value::String(string) => string,
                    Value::Bool(_) | Value::Number(_) => {
                        json_value(&field.columntype, column.as_ref(), index).to_string()
                    }
                    Value::Array(_) | Value::Object(_) => {
                        panic!("CSV holds no value of type {:?}", field.columntype.typ)
                    }
                },
            )
            .collect();
        writer.write_record(&record).unwrap();
    }
    writer.into_inner().unwrap()
}

/// Returns value `index` of `array`, a column of type `column` in the published form, in the
/// default JSON flavor.
pub fn json_value(column: &ColumnType, array: &dyn Array, index: usize) -> Value {
    if array.is_null(index) {
        return Value::Null;
    }
    match column.typ {
        SqlType::Boolean => Value::from(array.as_boolean().value(index)),
        SqlType::TinyInt => Value::from(array.as_primitive::<Int8Type>().value(index)),
        SqlType::SmallInt => Value::from(array.as_primitive::<Int16Type>().value(index)),
        SqlType::Int => Value::from(array.as_primitive::<Int32Type>().value(index)),
        SqlType::BigInt => Value::from(array.as_primitive::<Int64Type>().value(index)),
        SqlType::UTinyInt => Value::from(array.as_primitive::<UInt8Type>().value(index)),
        SqlType::USmallInt => Value::from(array.as_primitive::<UInt16Type>().value(index)),
        SqlType::UInt => Value::from(array.as_primitive::<UInt32Type>().value(index)),
        SqlType::UBigInt => Value::from(array.as_primitive::<UInt64Type>().value(index)),
        SqlType::Real => Value::from(f64::from(array.as_primitive::<Float32Type>().value(index))),
        SqlType::Double => Value::from(array.as_primitive::<Float64Type>().value(index)),
        SqlType::Decimal => {
            let decimals = array.as_primitive::<Decimal128Type>();
            Value::from(decimals.value_as_string(index))
        }
        SqlType::Char | SqlType::Varchar | SqlType::Uuid => {
            Value::from(array.as_string::<i32>().value(index))
        }
        SqlType::Variant => serde_json::from_str(array.as_string::<i32>().value(index))
            .expect("a VARIANT holds JSON text in the published form"),
        SqlType::Date => {
            let days = array.as_primitive::<Date32Type>().value(index);
            let date = DateTime::UNIX_EPOCH.date_naive() + TimeDelta::days(days.into());
            Value::from(date.format("%Y-%m-%d").to_string())
        }
        SqlType::Time => {
            let nanos = array.as_primitive::<Time64NanosecondType>().value(index);
            let time = NaiveTime::MIN + TimeDelta::nanoseconds(nanos);
            Value::from(time.format("%H:%M:%S%.f").to_string())
        }
        SqlType::Timestamp => {
            let micros = array
                .as_primitive::<TimestampMicrosecondType>()
                .value(index);
            let timestamp = DateTime::from_timestamp_micros(micros).unwrap();
            Value::from(timestamp.format("%F %T%.f").to_string())
        }
        SqlType::TimestampTz => {
            let micros = array
                .as_primitive::<TimestampMicrosecondType>()
                .value(index);
            let timestamp = DateTime::from_timestamp_micros(micros).unwrap();
            Value::from(timestamp.format("%F %T%.f%:z").to_string())
        }
        SqlType::Binary | SqlType::Varbinary => {
            Value::from(array.as_binary::<i64>().value(index).to_vec())
        }
        SqlType::Array => {
            let component = column
                .component
                .as_ref()
                .expect("an ARRAY has a component type");
            let elements = array.as_list::<i64>().value(index);
            Value::Array(
                (0..elements.len())
                    .map(|element| json_value(component, elements.as_ref(), element))
                    .collect(),
            )
        }
        SqlType::Map => {
            let key = column.key.as_ref().expect("a MAP has a key type");
            let value = column.value.as_ref().expect("a MAP has a value type");
            let entries = array.as_map().value(index);
            let mut object = Map::new();
            for entry in 0..entries.len() {
                let key = match json_value(key, entries.column(0).as_ref(), entry) {
                    Value::String(key) => key,
                    other => other.to_string(),
                };
                object.insert(key, json_value(value, entries.column(1).as_ref(), entry));
            }
            Value::Object(object)
        }
        SqlType::Struct => {
            let fields = column.fields.as_ref().expect("a STRUCT has fields");
            let structure = array.as_struct();
            Value::Object(
                fields
                    .iter()
                    .zip(structure.columns())
                    .map(|(field, child)| {
                        (
                            field.name.name(),
                            json_value(&field.columntype, child.as_ref(), index),
                        )
                    })
                    .collect(),
            )
        }
        _ => panic!("no JSON form of type {:?}", column.typ),
    }
}
