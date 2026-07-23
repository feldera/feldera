use std::io::Cursor;

use anyhow::{anyhow, bail, Context, Result as AnyResult};
use arrow::{
    array::{
        Array, ArrayRef, Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StructArray, Time64NanosecondArray, TimestampMicrosecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use feldera_types::transport::snowflake::SnowflakeNumberMode;

/// Decode an inline Snowflake `rowsetBase64` payload into Arrow record batches.
pub(crate) fn decode_base64_ipc_stream(
    rowset_base64: &str,
    number_mode: SnowflakeNumberMode,
) -> AnyResult<Vec<RecordBatch>> {
    let bytes = BASE64_STANDARD
        .decode(rowset_base64)
        .context("invalid base64 in Snowflake Arrow rowset")?;
    decode_ipc_stream(&bytes, number_mode)
}

/// Decode a raw Snowflake Arrow chunk into Arrow record batches.
pub(crate) fn decode_ipc_stream(
    bytes: &[u8],
    number_mode: SnowflakeNumberMode,
) -> AnyResult<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .context("error opening Snowflake Arrow IPC stream")?;

    reader
        .map(|batch| batch.context("error reading Snowflake Arrow IPC record batch"))
        .map(|batch| batch.and_then(|batch| normalize_snowflake_batch(batch, number_mode)))
        .collect()
}

/// Convert Snowflake-specific Arrow encodings into standard Arrow types understood by Feldera.
pub(crate) fn normalize_snowflake_batch(
    batch: RecordBatch,
    number_mode: SnowflakeNumberMode,
) -> AnyResult<RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let logical_type = field.metadata().get("logicalType").map(String::as_str);

        // Snowflake encodes scaled FIXED values as unscaled signed integers. Its C/C++ client
        // applies the scale during conversion as well:
        // https://github.com/snowflakedb/libsnowflakeclient/blob/master/cpp/lib/ArrowChunkIterator.cpp
        if logical_type == Some("FIXED") && field_scale(field)? != 0 {
            let normalized: ArrayRef = match number_mode {
                SnowflakeNumberMode::Decimal => {
                    std::sync::Arc::new(normalize_fixed(field.as_ref(), column.as_ref())?)
                }
                SnowflakeNumberMode::Double => {
                    std::sync::Arc::new(normalize_fixed_to_double(field.as_ref(), column.as_ref())?)
                }
            };
            fields.push(
                Field::new(
                    field.name(),
                    normalized.data_type().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            );
            columns.push(normalized);
        } else if matches!(
            logical_type,
            Some("TIMESTAMP_NTZ" | "TIMESTAMP_LTZ" | "TIMESTAMP_TZ")
        ) {
            let timezone = match logical_type {
                Some("TIMESTAMP_LTZ" | "TIMESTAMP_TZ") => Some("UTC".into()),
                _ => None,
            };
            let timestamps = normalize_timestamp(field.as_ref(), column.as_ref(), logical_type)?
                .with_timezone_opt(timezone.clone());
            fields.push(
                Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Microsecond, timezone),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            );
            columns.push(std::sync::Arc::new(timestamps) as _);
        } else if logical_type == Some("TIME") {
            let times = normalize_time(field.as_ref(), column.as_ref())?;
            fields.push(
                Field::new(
                    field.name(),
                    DataType::Time64(TimeUnit::Nanosecond),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            );
            columns.push(std::sync::Arc::new(times) as _);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }

    let schema = Schema::new(fields).with_metadata(schema.metadata().clone());
    RecordBatch::try_new(std::sync::Arc::new(schema), columns)
        .context("error constructing normalized Snowflake Arrow record batch")
}

fn normalize_fixed(field: &Field, column: &dyn Array) -> AnyResult<Decimal128Array> {
    let precision = field_metadata::<u8>(field, "precision")?;
    let scale = field_metadata::<i8>(field, "scale")?;
    if let Some(values) = column.as_any().downcast_ref::<Decimal128Array>() {
        return values
            .clone()
            .with_precision_and_scale(precision, scale)
            .with_context(|| {
                format!(
                    "invalid precision or scale metadata for Snowflake FIXED column '{}'",
                    field.name()
                )
            });
    }

    let values = (0..column.len())
        .map(|row| fixed_integer_at(column, row, field.name()))
        .collect::<AnyResult<Vec<_>>>()?;

    Decimal128Array::from(values)
        .with_precision_and_scale(precision, scale)
        .with_context(|| {
            format!(
                "invalid precision or scale metadata for Snowflake FIXED column '{}'",
                field.name()
            )
        })
}

fn normalize_fixed_to_double(field: &Field, column: &dyn Array) -> AnyResult<Float64Array> {
    let scale = field_metadata::<i32>(field, "scale")?;
    let divisor = 10_f64.powi(scale);

    (0..column.len())
        .map(|row| {
            fixed_integer_at(column, row, field.name())
                .map(|value| value.map(|value| value as f64 / divisor))
        })
        .collect()
}

fn fixed_integer_at(column: &dyn Array, row: usize, field_name: &str) -> AnyResult<Option<i128>> {
    if let Some(values) = column.as_any().downcast_ref::<Decimal128Array>() {
        return Ok((!values.is_null(row)).then(|| values.value(row)));
    }

    signed_integer_at(column, row, field_name).map(|value| value.map(i128::from))
}

fn normalize_timestamp(
    field: &Field,
    column: &dyn Array,
    logical_type: Option<&str>,
) -> AnyResult<TimestampMicrosecondArray> {
    // Snowflake's reference implementation documents both the current structured timestamp
    // encoding and the legacy TIMESTAMP_TZ layout handled below:
    // https://github.com/snowflakedb/libsnowflakeclient/blob/master/cpp/lib/ArrowChunkIterator.cpp
    if let Some(values) = column.as_any().downcast_ref::<StructArray>() {
        let epoch = struct_child::<Int64Array>(values, field.name(), "epoch")?;
        let fraction = struct_child::<Int32Array>(values, field.name(), "fraction")?;
        let legacy_timestamp_tz =
            logical_type == Some("TIMESTAMP_TZ") && values.column_by_name("timezone").is_none();

        return (0..values.len())
            .map(|row| {
                if values.is_null(row) || epoch.is_null(row) || fraction.is_null(row) {
                    return Ok(None);
                }

                // Modern structured timestamps store seconds since the Unix epoch plus a
                // nanosecond fraction. In Snowflake's legacy TIMESTAMP_TZ struct, `epoch`
                // instead contains the complete scaled timestamp and `fraction` contains
                // the display timezone.
                let micros = if legacy_timestamp_tz {
                    scaled_integer_to_micros(
                        epoch.value(row),
                        field_scale(field)?,
                        field.name(),
                        row,
                    )?
                } else {
                    epoch
                        .value(row)
                        .checked_mul(1_000_000)
                        .and_then(|value| value.checked_add(i64::from(fraction.value(row)) / 1_000))
                        .ok_or_else(|| timestamp_out_of_range(field.name(), row))?
                };
                Ok(Some(micros))
            })
            .collect::<AnyResult<TimestampMicrosecondArray>>();
    }

    let scale = field_scale(field)?;
    (0..column.len())
        .map(|row| {
            signed_integer_at(column, row, field.name())?
                .map(|value| scaled_integer_to_micros(value, scale, field.name(), row))
                .transpose()
        })
        .collect::<AnyResult<TimestampMicrosecondArray>>()
}

fn normalize_time(field: &Field, column: &dyn Array) -> AnyResult<Time64NanosecondArray> {
    let scale = field_scale(field)?;
    if scale > 9 {
        bail!(
            "Snowflake TIME column '{}' has unsupported scale {scale}",
            field.name()
        );
    }
    let multiplier = 10_i64.pow(9 - scale);

    (0..column.len())
        .map(|row| {
            signed_integer_at(column, row, field.name())?
                .map(|value| {
                    value.checked_mul(multiplier).ok_or_else(|| {
                        anyhow!(
                            "Snowflake TIME value in column '{}' at row {row} is out of range",
                            field.name()
                        )
                    })
                })
                .transpose()
        })
        .collect::<AnyResult<Time64NanosecondArray>>()
}

fn signed_integer_at(column: &dyn Array, row: usize, field_name: &str) -> AnyResult<Option<i64>> {
    macro_rules! value {
        ($array:ty) => {
            if let Some(values) = column.as_any().downcast_ref::<$array>() {
                return Ok((!values.is_null(row)).then(|| i64::from(values.value(row))));
            }
        };
    }

    value!(Int8Array);
    value!(Int16Array);
    value!(Int32Array);
    value!(Int64Array);
    bail!(
        "Snowflake column '{field_name}' has unsupported physical type {}",
        column.data_type()
    )
}

fn field_scale(field: &Field) -> AnyResult<u32> {
    field_metadata(field, "scale")
}

fn field_metadata<T>(field: &Field, key: &str) -> AnyResult<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    field
        .metadata()
        .get(key)
        .ok_or_else(|| anyhow!("Snowflake column '{}' has no {key} metadata", field.name()))?
        .parse()
        .with_context(|| {
            format!(
                "invalid {key} metadata for Snowflake column '{}'",
                field.name()
            )
        })
}

fn scaled_integer_to_micros(
    value: i64,
    scale: u32,
    field_name: &str,
    row: usize,
) -> AnyResult<i64> {
    if scale > 9 {
        bail!("Snowflake timestamp column '{field_name}' has unsupported scale {scale}");
    }
    let micros = if scale <= 6 {
        value.checked_mul(10_i64.pow(6 - scale))
    } else {
        Some(value / 10_i64.pow(scale - 6))
    };
    micros.ok_or_else(|| timestamp_out_of_range(field_name, row))
}

fn timestamp_out_of_range(field_name: &str, row: usize) -> anyhow::Error {
    anyhow!("Snowflake timestamp value in column '{field_name}' at row {row} is out of range")
}

fn struct_child<'a, T: Array + 'static>(
    values: &'a StructArray,
    field_name: &str,
    child_name: &str,
) -> AnyResult<&'a T> {
    let child = values.column_by_name(child_name).ok_or_else(|| {
        anyhow!("Snowflake timestamp column '{field_name}' has no '{child_name}' child")
    })?;
    child.as_any().downcast_ref::<T>().ok_or_else(|| {
        anyhow!(
            "Snowflake timestamp column '{field_name}' has an invalid '{child_name}' child type"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, Int64Array, StringArray, StructArray},
        datatypes::{DataType, Field, Schema},
        ipc::writer::StreamWriter,
    };
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn decodes_ipc_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ID", DataType::Int64, false),
            Field::new("NAME", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .unwrap();

        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let batches = decode_ipc_stream(&bytes, SnowflakeNumberMode::Decimal).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "ID");
    }

    #[test]
    fn normalizes_snowflake_timestamp_structs() {
        let timestamp_fields = vec![
            Arc::new(Field::new("epoch", DataType::Int64, true)),
            Arc::new(Field::new("fraction", DataType::Int32, true)),
            Arc::new(Field::new("timezone", DataType::Int32, true)),
        ];
        let timestamps = StructArray::from(vec![
            (
                timestamp_fields[0].clone(),
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            ),
            (
                timestamp_fields[1].clone(),
                Arc::new(Int32Array::from(vec![Some(234_567_890), None])) as ArrayRef,
            ),
            (
                timestamp_fields[2].clone(),
                Arc::new(Int32Array::from(vec![Some(1_440), None])) as ArrayRef,
            ),
        ]);
        let field = Field::new(
            "CREATED_AT",
            DataType::Struct(timestamp_fields.into()),
            true,
        )
        .with_metadata(HashMap::from([(
            "logicalType".to_string(),
            "TIMESTAMP_TZ".to_string(),
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(timestamps)],
        )
        .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Decimal).unwrap();
        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(values.value(0), 1_234_567);
        assert!(values.is_null(1));
    }

    #[test]
    fn normalizes_scaled_timestamp_and_time_integers() {
        let timestamp = Field::new("TS", DataType::Int64, false).with_metadata(HashMap::from([
            ("logicalType".to_string(), "TIMESTAMP_NTZ".to_string()),
            ("scale".to_string(), "3".to_string()),
        ]));
        let time = Field::new("T", DataType::Int64, false).with_metadata(HashMap::from([
            ("logicalType".to_string(), "TIME".to_string()),
            ("scale".to_string(), "3".to_string()),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![timestamp, time])),
            vec![
                Arc::new(Int64Array::from(vec![1_234])),
                Arc::new(Int64Array::from(vec![3_723_004])),
            ],
        )
        .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Decimal).unwrap();
        let timestamp = normalized
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let time = normalized
            .column(1)
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap();
        assert_eq!(timestamp.value(0), 1_234_000);
        assert_eq!(time.value(0), 3_723_004_000_000);
    }

    #[test]
    fn normalizes_scaled_fixed_integers() {
        let field = Field::new("AMOUNT", DataType::Int8, true).with_metadata(HashMap::from([
            ("logicalType".to_string(), "FIXED".to_string()),
            ("precision".to_string(), "10".to_string()),
            ("scale".to_string(), "2".to_string()),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int8Array::from(vec![Some(123), None]))],
        )
        .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Decimal).unwrap();
        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
        let values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.value(0), 123);
        assert!(values.is_null(1));
    }

    #[test]
    fn converts_scaled_fixed_integers_to_double() {
        let field = Field::new("AMOUNT", DataType::Int8, true).with_metadata(HashMap::from([
            ("logicalType".to_string(), "FIXED".to_string()),
            ("precision".to_string(), "10".to_string()),
            ("scale".to_string(), "2".to_string()),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int8Array::from(vec![Some(123), None]))],
        )
        .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Double).unwrap();
        assert_eq!(normalized.schema().field(0).data_type(), &DataType::Float64);
        let values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1.23);
        assert!(values.is_null(1));
    }

    #[test]
    fn converts_scaled_decimal128_to_double() {
        let field =
            Field::new("AMOUNT", DataType::Decimal128(38, 2), true).with_metadata(HashMap::from([
                ("logicalType".to_string(), "FIXED".to_string()),
                ("precision".to_string(), "38".to_string()),
                ("scale".to_string(), "2".to_string()),
            ]));
        let values = Decimal128Array::from(vec![Some(-123), None])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(values)])
                .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Double).unwrap();
        let values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(values.value(0), -1.23);
        assert!(values.is_null(1));
    }

    #[test]
    fn double_mode_leaves_scale_zero_numbers_integral() {
        let field = Field::new("ID", DataType::Int8, false).with_metadata(HashMap::from([
            ("logicalType".to_string(), "FIXED".to_string()),
            ("precision".to_string(), "10".to_string()),
            ("scale".to_string(), "0".to_string()),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int8Array::from(vec![123]))],
        )
        .unwrap();

        let normalized = normalize_snowflake_batch(batch, SnowflakeNumberMode::Double).unwrap();
        assert_eq!(normalized.schema().field(0).data_type(), &DataType::Int8);
    }
}
