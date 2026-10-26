//! Rows as Avro values, and Avro values back as rows, for the Avro suites.
//!
//! [`record`] writes a row in the published form of a relation as a value of an Avro record
//! schema, column by field name, and [`to_json`] reads a value of a row back as the JSON
//! object that arrow-json reads into the published form. The conversions follow the Avro
//! schema: a TIMESTAMP column becomes a `long` of microseconds, a `timestamp-millis`, or a
//! `timestamp-micros`, whichever the field declares.

use std::collections::HashMap;

use anyhow::{Result as AnyResult, anyhow, bail};
use apache_avro::schema::{Name, RecordSchema, ResolvedSchema};
use apache_avro::types::Value;
use apache_avro::{Decimal, Schema as AvroSchema, to_avro_datum};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time64NanosecondType, TimestampMicrosecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use chrono::{DateTime, NaiveTime, TimeDelta};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlType};
use num_bigint::BigInt;
use serde_json::{Map, Value as JsonValue};

/// The named types of an Avro schema, by name.
pub type Names = HashMap<Name, AvroSchema>;

/// Returns the named types that `schema` defines.
pub fn names(schema: &AvroSchema) -> Names {
    ResolvedSchema::try_from(schema)
        .unwrap()
        .get_names()
        .iter()
        .map(|(name, schema)| (name.clone(), (*schema).clone()))
        .collect()
}

/// Returns `value` of `schema` as a message of the Confluent wire format: a 5-byte header,
/// which a parser with a static schema skips, and the datum.
pub fn message(schema: &AvroSchema, value: Value) -> Vec<u8> {
    let mut message = vec![0; 5];
    message.append(&mut to_avro_datum(schema, value).unwrap());
    message
}

/// Returns row `index` of `rows`, in the published form of `relation`, as a value of the
/// record `schema`, whose named types `names` holds.
///
/// Each field of the record takes the column of the same name. A column that the record does
/// not name is left out. Fails if a field names no column, or if a value does not fit its
/// field.
pub fn record(
    relation: &Relation,
    rows: &RecordBatch,
    index: usize,
    schema: &AvroSchema,
    names: &Names,
) -> AnyResult<Value> {
    let AvroSchema::Record(record_schema) = resolve(schema, names)? else {
        bail!("the Avro schema of a row is not a record: {schema:?}");
    };
    struct_value(
        &relation.fields,
        &columns(rows),
        index,
        record_schema,
        names,
    )
}

/// Returns a Debezium change event of the envelope `schema`, whose named types `names`
/// holds: the operation `op` with the rows `before` and `after`, and default values in the
/// other fields.
pub fn debezium(
    schema: &AvroSchema,
    names: &Names,
    op: &str,
    before: Option<Value>,
    after: Option<Value>,
) -> AnyResult<Value> {
    let AvroSchema::Record(envelope) = resolve(schema, names)? else {
        bail!("the Debezium envelope is not a record: {schema:?}");
    };
    let union = |value: Option<Value>| match value {
        None => Value::Union(0, Box::new(Value::Null)),
        Some(value) => Value::Union(1, Box::new(value)),
    };
    let (mut before, mut after) = (Some(union(before)), Some(union(after)));
    let fields = envelope
        .fields
        .iter()
        .map(|field| {
            let value = match field.name.as_str() {
                "before" => before.take().unwrap(),
                "after" => after.take().unwrap(),
                "op" => Value::String(op.to_string()),
                _ => default_value(&field.schema, names)?,
            };
            Ok((field.name.clone(), value))
        })
        .collect::<AnyResult<_>>()?;
    Ok(Value::Record(fields))
}

/// Returns the Avro `value` of a row of `relation` as a JSON object that arrow-json reads into
/// the published form of `relation`.
///
/// A field that is not a column of `relation` is an error. A column without a field is
/// NULL.
pub fn to_json(relation: &Relation, value: &Value) -> AnyResult<JsonValue> {
    struct_json(&relation.fields, value)
}

/// Returns `schema`, or the schema that it refers to.
fn resolve<'a>(schema: &'a AvroSchema, names: &'a Names) -> AnyResult<&'a AvroSchema> {
    match schema {
        AvroSchema::Ref { name } => names
            .get(name)
            .ok_or_else(|| anyhow!("the Avro schema refers to the unknown type {name}")),
        schema => Ok(schema),
    }
}

/// Returns the columns of `rows`.
fn columns(rows: &RecordBatch) -> Vec<&dyn Array> {
    rows.columns()
        .iter()
        .map(|column| column.as_ref())
        .collect()
}

/// Returns the Avro value of row `index` of the columns `arrays`, described by `fields`, as
/// a value of `schema`.
fn struct_value(
    fields: &[Field],
    arrays: &[&dyn Array],
    index: usize,
    schema: &RecordSchema,
    names: &Names,
) -> AnyResult<Value> {
    let values = schema
        .fields
        .iter()
        .map(|avro_field| {
            let position = column_position(fields, &avro_field.name)
                .ok_or_else(|| anyhow!("the Avro field '{}' names no column", avro_field.name))?;
            let value = avro_value(
                &avro_field.schema,
                names,
                &fields[position].columntype,
                arrays[position],
                index,
            )?;
            Ok((avro_field.name.clone(), value))
        })
        .collect::<AnyResult<_>>()?;
    Ok(Value::Record(values))
}

/// Returns the position of the column of `fields` that the Avro field `name` names.
fn column_position(fields: &[Field], name: &str) -> Option<usize> {
    fields
        .iter()
        .position(|field| field.name.name() == name)
        .or_else(|| {
            fields
                .iter()
                .position(|field| field.name.name().eq_ignore_ascii_case(name))
        })
}

/// Returns value `index` of `array`, of `column`, as a value of `schema`.
fn avro_value(
    schema: &AvroSchema,
    names: &Names,
    column: &ColumnType,
    array: &dyn Array,
    index: usize,
) -> AnyResult<Value> {
    let schema = resolve(schema, names)?;
    if let AvroSchema::Union(union) = schema {
        let variants = union.variants();
        return if array.is_null(index) {
            let position = variants
                .iter()
                .position(|variant| *variant == AvroSchema::Null)
                .ok_or_else(|| anyhow!("NULL for an Avro union without a null: {schema:?}"))?;
            Ok(Value::Union(position as u32, Box::new(Value::Null)))
        } else {
            let position = variants
                .iter()
                .position(|variant| *variant != AvroSchema::Null)
                .ok_or_else(|| anyhow!("a value for an Avro union of null: {schema:?}"))?;
            let value = avro_value(&variants[position], names, column, array, index)?;
            Ok(Value::Union(position as u32, Box::new(value)))
        };
    }
    if array.is_null(index) {
        bail!("NULL for the Avro schema {schema:?}, which has no null");
    }
    let integer = || -> AnyResult<i64> {
        Ok(match column.typ {
            SqlType::TinyInt => array.as_primitive::<Int8Type>().value(index).into(),
            SqlType::SmallInt => array.as_primitive::<Int16Type>().value(index).into(),
            SqlType::Int => array.as_primitive::<Int32Type>().value(index).into(),
            SqlType::BigInt => array.as_primitive::<Int64Type>().value(index),
            SqlType::UTinyInt => array.as_primitive::<UInt8Type>().value(index).into(),
            SqlType::USmallInt => array.as_primitive::<UInt16Type>().value(index).into(),
            SqlType::UInt => array.as_primitive::<UInt32Type>().value(index).into(),
            SqlType::UBigInt => array.as_primitive::<UInt64Type>().value(index).try_into()?,
            SqlType::Date => array.as_primitive::<Date32Type>().value(index).into(),
            SqlType::Timestamp | SqlType::TimestampTz => array
                .as_primitive::<TimestampMicrosecondType>()
                .value(index),
            SqlType::Time => array.as_primitive::<Time64NanosecondType>().value(index) / 1000,
            _ => bail!("a {:?} column has no Avro integer form", column.typ),
        })
    };
    let bytes = || -> AnyResult<Vec<u8>> {
        match column.typ {
            SqlType::Binary | SqlType::Varbinary => {
                Ok(array.as_binary::<i64>().value(index).to_vec())
            }
            _ => bail!("a {:?} column has no Avro bytes form", column.typ),
        }
    };
    Ok(match schema {
        AvroSchema::Null => Value::Null,
        AvroSchema::Boolean => Value::Boolean(array.as_boolean().value(index)),
        AvroSchema::Int => Value::Int(integer()?.try_into()?),
        AvroSchema::Long => Value::Long(integer()?),
        AvroSchema::Float => Value::Float(array.as_primitive::<Float32Type>().value(index)),
        AvroSchema::Double => Value::Double(array.as_primitive::<Float64Type>().value(index)),
        AvroSchema::String => Value::String(match column.typ {
            SqlType::Decimal => array
                .as_primitive::<Decimal128Type>()
                .value_as_string(index),
            _ => array.as_string::<i32>().value(index).to_string(),
        }),
        AvroSchema::Uuid => Value::Uuid(array.as_string::<i32>().value(index).parse()?),
        AvroSchema::Enum(enum_schema) => {
            let symbol = array.as_string::<i32>().value(index);
            let position = enum_schema
                .symbols
                .iter()
                .position(|candidate| candidate == symbol)
                .ok_or_else(|| anyhow!("'{symbol}' is not a symbol of {schema:?}"))?;
            Value::Enum(position as u32, symbol.to_string())
        }
        AvroSchema::Bytes => Value::Bytes(bytes()?),
        AvroSchema::Fixed(fixed) => Value::Fixed(fixed.size, bytes()?),
        AvroSchema::Decimal(decimal) => {
            let decimals = array.as_primitive::<Decimal128Type>();
            let mut mantissa = decimals.value(index);
            let mut scale = i64::from(decimals.scale());
            while scale < decimal.scale as i64 {
                mantissa *= 10;
                scale += 1;
            }
            if scale > decimal.scale as i64 {
                bail!(
                    "the decimal scale {scale} is past the Avro scale {}",
                    decimal.scale
                );
            }
            Value::Decimal(Decimal::from(BigInt::from(mantissa).to_signed_bytes_be()))
        }
        AvroSchema::Date => Value::Date(integer()?.try_into()?),
        AvroSchema::TimeMillis => Value::TimeMillis((integer()? / 1000).try_into()?),
        AvroSchema::TimeMicros => Value::TimeMicros(integer()?),
        AvroSchema::TimestampMillis => Value::TimestampMillis(integer()? / 1000),
        AvroSchema::TimestampMicros => Value::TimestampMicros(integer()?),
        AvroSchema::TimestampNanos => Value::TimestampNanos(integer()? * 1000),
        AvroSchema::LocalTimestampMillis => Value::LocalTimestampMillis(integer()? / 1000),
        AvroSchema::LocalTimestampMicros => Value::LocalTimestampMicros(integer()?),
        AvroSchema::LocalTimestampNanos => Value::LocalTimestampNanos(integer()? * 1000),
        AvroSchema::Record(record_schema) => {
            let fields = column.fields.as_ref().expect("a STRUCT has fields");
            let structure = array.as_struct();
            let arrays: Vec<&dyn Array> = structure
                .columns()
                .iter()
                .map(|column| column.as_ref())
                .collect();
            struct_value(fields, &arrays, index, record_schema, names)?
        }
        AvroSchema::Array(items) => {
            let component = column
                .component
                .as_ref()
                .expect("an ARRAY has a component type");
            let elements = array.as_list::<i64>().value(index);
            Value::Array(
                (0..elements.len())
                    .map(|element| {
                        avro_value(&items.items, names, component, elements.as_ref(), element)
                    })
                    .collect::<AnyResult<_>>()?,
            )
        }
        AvroSchema::Map(values) => {
            let value_type = column.value.as_ref().expect("a MAP has a value type");
            let entries = array.as_map().value(index);
            let keys = entries.column(0).as_string::<i32>();
            Value::Map(
                (0..entries.len())
                    .map(|entry| {
                        let value = avro_value(
                            &values.types,
                            names,
                            value_type,
                            entries.column(1).as_ref(),
                            entry,
                        )?;
                        Ok((keys.value(entry).to_string(), value))
                    })
                    .collect::<AnyResult<_>>()?,
            )
        }
        _ => bail!("no Avro value of {schema:?} for a {:?} column", column.typ),
    })
}

/// Returns a value of `schema` that holds nothing: NULL in a union with null, and an empty
/// or zero value otherwise.
fn default_value(schema: &AvroSchema, names: &Names) -> AnyResult<Value> {
    Ok(match resolve(schema, names)? {
        AvroSchema::Null => Value::Null,
        AvroSchema::Union(union) => {
            let variants = union.variants();
            match variants
                .iter()
                .position(|variant| *variant == AvroSchema::Null)
            {
                Some(position) => Value::Union(position as u32, Box::new(Value::Null)),
                None => Value::Union(0, Box::new(default_value(&variants[0], names)?)),
            }
        }
        AvroSchema::Boolean => Value::Boolean(false),
        AvroSchema::Int => Value::Int(0),
        AvroSchema::Long => Value::Long(0),
        AvroSchema::String => Value::String(String::new()),
        AvroSchema::Record(record_schema) => Value::Record(
            record_schema
                .fields
                .iter()
                .map(|field| Ok((field.name.clone(), default_value(&field.schema, names)?)))
                .collect::<AnyResult<_>>()?,
        ),
        schema => bail!("no default value of {schema:?}"),
    })
}

/// Returns the Avro record `value` of `fields` as a JSON object in the arrow-json form.
fn struct_json(fields: &[Field], value: &Value) -> AnyResult<JsonValue> {
    let Value::Record(values) = value else {
        bail!("not an Avro record: {value:?}");
    };
    let mut object = Map::new();
    for field in fields {
        object.insert(field.name.name(), JsonValue::Null);
    }
    for (name, value) in values {
        let position = column_position(fields, name)
            .ok_or_else(|| anyhow!("the Avro field '{name}' names no column"))?;
        let field = &fields[position];
        object.insert(field.name.name(), json_value(&field.columntype, value)?);
    }
    Ok(JsonValue::Object(object))
}

/// Returns the Avro `value` of `column` as arrow-json reads it into the published form.
fn json_value(column: &ColumnType, value: &Value) -> AnyResult<JsonValue> {
    let hex = |bytes: &[u8]| -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() };
    let time = |micros: i64| {
        let time = NaiveTime::MIN + TimeDelta::microseconds(micros);
        JsonValue::from(time.format("%H:%M:%S%.6f").to_string())
    };
    let timestamp = |micros: i64| -> AnyResult<JsonValue> {
        let timestamp = DateTime::from_timestamp_micros(micros)
            .ok_or_else(|| anyhow!("timestamp {micros} is out of range"))?;
        Ok(JsonValue::from(match column.typ {
            SqlType::TimestampTz => timestamp.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
            _ => timestamp.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
        }))
    };
    Ok(match value {
        Value::Null => JsonValue::Null,
        Value::Union(_, value) => json_value(column, value)?,
        Value::Boolean(value) => JsonValue::from(*value),
        Value::Int(value) => match column.typ {
            SqlType::Date => {
                let date = DateTime::UNIX_EPOCH.date_naive() + TimeDelta::days((*value).into());
                JsonValue::from(date.format("%Y-%m-%d").to_string())
            }
            _ => JsonValue::from(*value),
        },
        Value::Long(value) => match column.typ {
            SqlType::Timestamp | SqlType::TimestampTz => timestamp(*value)?,
            SqlType::Time => time(*value),
            _ => JsonValue::from(*value),
        },
        Value::Float(value) => JsonValue::from(f64::from(*value)),
        Value::Double(value) => JsonValue::from(*value),
        Value::String(value) | Value::Enum(_, value) => JsonValue::from(value.clone()),
        Value::Uuid(value) => JsonValue::from(value.to_string()),
        Value::Bytes(value) | Value::Fixed(_, value) => JsonValue::from(hex(value)),
        Value::Decimal(decimal) => {
            let mantissa = BigInt::from(decimal.clone());
            let scale = column.scale.unwrap_or(0) as usize;
            let digits = mantissa.magnitude().to_string();
            let sign = if mantissa.sign() == num_bigint::Sign::Minus {
                "-"
            } else {
                ""
            };
            let digits = format!("{digits:0>width$}", width = scale + 1);
            let (whole, fraction) = digits.split_at(digits.len() - scale);
            JsonValue::from(if scale == 0 {
                format!("{sign}{whole}")
            } else {
                format!("{sign}{whole}.{fraction}")
            })
        }
        Value::Date(days) => {
            let date = DateTime::UNIX_EPOCH.date_naive() + TimeDelta::days((*days).into());
            JsonValue::from(date.format("%Y-%m-%d").to_string())
        }
        Value::TimeMillis(millis) => time(i64::from(*millis) * 1000),
        Value::TimeMicros(micros) => time(*micros),
        Value::TimestampMillis(millis) | Value::LocalTimestampMillis(millis) => {
            timestamp(millis * 1000)?
        }
        Value::TimestampMicros(micros) | Value::LocalTimestampMicros(micros) => timestamp(*micros)?,
        Value::TimestampNanos(nanos) | Value::LocalTimestampNanos(nanos) => {
            timestamp(nanos / 1000)?
        }
        Value::Record(_) => {
            struct_json(column.fields.as_ref().expect("a STRUCT has fields"), value)?
        }
        Value::Array(elements) => {
            let component = column
                .component
                .as_ref()
                .expect("an ARRAY has a component type");
            JsonValue::Array(
                elements
                    .iter()
                    .map(|element| json_value(component, element))
                    .collect::<AnyResult<_>>()?,
            )
        }
        Value::Map(entries) => {
            let value_type = column.value.as_ref().expect("a MAP has a value type");
            JsonValue::Object(
                entries
                    .iter()
                    .map(|(key, value)| Ok((key.clone(), json_value(value_type, value)?)))
                    .collect::<AnyResult<_>>()?,
            )
        }
        value => bail!("no JSON form of the Avro value {value:?}"),
    })
}
