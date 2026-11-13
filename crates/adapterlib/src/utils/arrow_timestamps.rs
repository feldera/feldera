//! Arrow timestamps read as microseconds since the epoch.
//!
//! `serde_arrow` gives a timestamp as its raw integer, without its unit. An
//! input connector that reads with `TimestampFormat::MicrosSinceEpochOrString`
//! converts the timestamps that its SQL `TIMESTAMP` columns read to
//! microseconds with [`timestamps_to_micros`], and checks with [`check_timestamp_columns`] that
//! no SQL `TIMESTAMP` column reads an Arrow integer, which the format would
//! take for microseconds. A string column passes the check: the format parses
//! each value. The connector checks at startup, and again for each batch,
//! because the schema of the source can change while the connector follows it.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    PrimitiveArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    ArrowTimestampType, DataType, Field as ArrowField, FieldRef, Fields, Schema, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::error::ArrowError;
use feldera_types::program_schema::{ColumnType, Field, SqlType};

/// Converts to microseconds each timestamp in `batch`, at any depth, that a
/// SQL `TIMESTAMP` in `fields` reads.
///
/// A timestamp that another SQL type reads keeps its unit: `BIGINT` reads its
/// integer, and `VARCHAR` reads all its digits. A batch without timestamps in
/// other units is returned as it is.
pub fn timestamps_to_micros(
    batch: RecordBatch,
    fields: &[Field],
) -> Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| needs_conversion(f.data_type()))
    {
        return Ok(batch);
    }

    let columns = struct_to_micros(schema.fields(), batch.columns(), fields)?;
    let arrow_fields = schema
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, column)| with_data_type(field, column.data_type()))
        .collect::<Fields>();
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(
            arrow_fields,
            schema.metadata().clone(),
        )),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
}

fn needs_conversion(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp(unit, _) => *unit != TimeUnit::Microsecond,
        DataType::Struct(fields) => fields.iter().any(|f| needs_conversion(f.data_type())),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => needs_conversion(f.data_type()),
        DataType::Dictionary(_, value) => needs_conversion(value),
        DataType::RunEndEncoded(_, values) => needs_conversion(values.data_type()),
        DataType::Union(fields, _) => fields.iter().any(|(_, f)| needs_conversion(f.data_type())),
        _ => false,
    }
}

/// `data_type` with each timestamp in microseconds.
fn micros_type(data_type: &DataType) -> DataType {
    let field = |f: &FieldRef| with_data_type(f, &micros_type(f.data_type()));
    match data_type {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::ListView(f) => DataType::ListView(field(f)),
        DataType::LargeListView(f) => DataType::LargeListView(field(f)),
        DataType::FixedSizeList(f, size) => DataType::FixedSizeList(field(f), *size),
        DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(micros_type(value)))
        }
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(ends.clone(), field(values))
        }
        DataType::Union(fields, mode) => {
            DataType::Union(fields.iter().map(|(id, f)| (id, field(f))).collect(), *mode)
        }
        other => other.clone(),
    }
}

fn with_data_type(field: &FieldRef, data_type: &DataType) -> FieldRef {
    if field.data_type() == data_type {
        field.clone()
    } else {
        Arc::new(ArrowField::clone(field).with_data_type(data_type.clone()))
    }
}

/// Converts the columns of a batch or of a struct that `fields` read.
fn struct_to_micros(
    arrow_fields: &Fields,
    columns: &[ArrayRef],
    fields: &[Field],
) -> Result<Vec<ArrayRef>, ArrowError> {
    arrow_fields
        .iter()
        .zip(columns)
        .map(|(arrow_field, column)| {
            if !needs_conversion(column.data_type()) {
                return Ok(column.clone());
            }
            match sql_field(fields, arrow_field.name()) {
                Some(field) => array_to_micros(column, &field.columntype),
                None => Ok(column.clone()),
            }
        })
        .collect()
}

/// The SQL field that reads the Arrow field `name`: the field with this
/// spelling, or else with this name when case is ignored.
fn sql_field<'a>(fields: &'a [Field], name: &str) -> Option<&'a Field> {
    fields.iter().find(|f| f.name.name() == name).or_else(|| {
        let lowercase = name.to_lowercase();
        fields
            .iter()
            .find(|f| f.name.name().to_lowercase() == lowercase)
    })
}

fn array_to_micros(array: &ArrayRef, column_type: &ColumnType) -> Result<ArrayRef, ArrowError> {
    if !needs_conversion(array.data_type()) || !has_timestamp(column_type) {
        return Ok(array.clone());
    }

    match (&column_type.typ, array.data_type()) {
        (SqlType::Timestamp, DataType::Timestamp(unit, tz)) => {
            let micros = match unit {
                TimeUnit::Second => {
                    convert::<TimestampSecondType>(array, |s| s.checked_mul(1_000_000))
                }
                TimeUnit::Millisecond => {
                    convert::<TimestampMillisecondType>(array, |ms| ms.checked_mul(1_000))
                }
                // Rounds down, as formatting and parsing back did.
                TimeUnit::Nanosecond => {
                    convert::<TimestampNanosecondType>(array, |ns| Some(ns.div_euclid(1_000)))
                }
                TimeUnit::Microsecond => unreachable!(),
            }?;
            Ok(Arc::new(micros.with_timezone_opt(tz.clone())))
        }
        (SqlType::Struct, DataType::Struct(_)) => {
            let array = array.as_struct();
            let sql_fields = column_type.fields.as_deref().unwrap_or_default();
            let columns = struct_to_micros(array.fields(), array.columns(), sql_fields)?;
            let fields = array
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| with_data_type(field, column.data_type()))
                .collect();
            Ok(Arc::new(StructArray::try_new(
                fields,
                columns,
                array.nulls().cloned(),
            )?))
        }
        (SqlType::Array, DataType::List(field)) => {
            let array = array.as_list::<i32>();
            let values = component_to_micros(array.values(), column_type)?;
            Ok(Arc::new(ListArray::try_new(
                with_data_type(field, values.data_type()),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?))
        }
        (SqlType::Array, DataType::LargeList(field)) => {
            let array = array.as_list::<i64>();
            let values = component_to_micros(array.values(), column_type)?;
            Ok(Arc::new(LargeListArray::try_new(
                with_data_type(field, values.data_type()),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )?))
        }
        (SqlType::Array, DataType::FixedSizeList(field, size)) => {
            let array = array.as_fixed_size_list();
            let values = component_to_micros(array.values(), column_type)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                with_data_type(field, values.data_type()),
                *size,
                values,
                array.nulls().cloned(),
            )?))
        }
        (SqlType::Map, DataType::Map(field, sorted)) => {
            let array = array.as_map();
            let entries = array.entries();
            let columns = [&column_type.key, &column_type.value]
                .into_iter()
                .zip(entries.columns())
                .map(|(sql_type, column)| match sql_type {
                    Some(sql_type) => array_to_micros(column, sql_type),
                    None => Ok(column.clone()),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let entry_fields = entries
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| with_data_type(field, column.data_type()))
                .collect();
            let entries = StructArray::try_new(entry_fields, columns, entries.nulls().cloned())?;
            Ok(Arc::new(MapArray::try_new(
                with_data_type(field, entries.data_type()),
                array.offsets().clone(),
                entries,
                array.nulls().cloned(),
                *sorted,
            )?))
        }
        // The SQL type does not read these as timestamps.
        (
            _,
            DataType::Timestamp(_, _)
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _),
        ) => Ok(array.clone()),
        // Rare containers go through the Arrow cast kernel, which rounds
        // nanoseconds toward zero and converts all the timestamps they hold.
        (_, data_type) => cast_with_options(
            array,
            &micros_type(data_type),
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        ),
    }
}

fn component_to_micros(
    values: &ArrayRef,
    column_type: &ColumnType,
) -> Result<ArrayRef, ArrowError> {
    match &column_type.component {
        Some(component) => array_to_micros(values, component),
        None => Ok(values.clone()),
    }
}

fn convert<T: ArrowTimestampType>(
    array: &ArrayRef,
    to_micros: impl Fn(i64) -> Option<i64>,
) -> Result<PrimitiveArray<TimestampMicrosecondType>, ArrowError> {
    array.as_primitive::<T>().try_unary(|value| {
        to_micros(value).ok_or_else(|| {
            ArrowError::ComputeError(format!(
                "timestamp {value} does not fit in microseconds since 1970-01-01"
            ))
        })
    })
}

/// Checks that each SQL `TIMESTAMP` column in `fields`, at any depth, reads an
/// Arrow timestamp or string from `schema`.
///
/// Columns that `schema` does not have are not checked.
pub fn check_timestamp_columns(fields: &[Field], schema: &Schema) -> Result<(), String> {
    check_fields(fields, schema.fields(), "")
}

fn check_fields(fields: &[Field], arrow_fields: &Fields, prefix: &str) -> Result<(), String> {
    // The check runs for each batch, so skip the columns it does not apply to.
    for field in fields.iter().filter(|f| has_timestamp(&f.columntype)) {
        let lowercase = field.name.name().to_lowercase();
        // The source can have more than one such column, e.g., `t` and `T`:
        // a read by name can take its value from any of them.
        for arrow_field in arrow_fields
            .iter()
            .filter(|f| f.name().to_lowercase() == lowercase)
        {
            check_type(
                &field.columntype,
                arrow_field.data_type(),
                &format!("{prefix}{}", field.name.sql_name()),
            )?;
        }
    }
    Ok(())
}

/// Tells whether a value of this type holds a `TIMESTAMP` at any depth.
fn has_timestamp(column_type: &ColumnType) -> bool {
    match column_type.typ {
        SqlType::Timestamp => true,
        SqlType::Struct => column_type
            .fields
            .as_ref()
            .is_some_and(|fields| fields.iter().any(|f| has_timestamp(&f.columntype))),
        SqlType::Array => column_type.component.as_deref().is_some_and(has_timestamp),
        SqlType::Map => {
            column_type.key.as_deref().is_some_and(has_timestamp)
                || column_type.value.as_deref().is_some_and(has_timestamp)
        }
        _ => false,
    }
}

fn check_type(column_type: &ColumnType, data_type: &DataType, path: &str) -> Result<(), String> {
    let data_type = match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        DataType::RunEndEncoded(_, values) => values.data_type(),
        data_type => data_type,
    };
    match (&column_type.typ, data_type) {
        (
            SqlType::Timestamp,
            DataType::Timestamp(_, _) | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => Ok(()),
        (SqlType::Timestamp, data_type) => Err(format!(
            "column {path} has SQL type TIMESTAMP, but the source stores it as Arrow type {data_type}; \
             a TIMESTAMP column must be an Arrow timestamp or string"
        )),
        (SqlType::Struct, DataType::Struct(arrow_fields)) => match &column_type.fields {
            Some(fields) => check_fields(fields, arrow_fields, &format!("{path}.")),
            None => Ok(()),
        },
        (
            SqlType::Array,
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _),
        ) => match &column_type.component {
            Some(component) => check_type(component, item.data_type(), &format!("{path}[]")),
            None => Ok(()),
        },
        (SqlType::Map, DataType::Map(entries, _)) => {
            let DataType::Struct(kv) = entries.data_type() else {
                return Ok(());
            };
            if let (Some(key), Some(arrow_key)) = (&column_type.key, kv.first()) {
                check_type(key, arrow_key.data_type(), &format!("{path}.key"))?;
            }
            if let (Some(value), Some(arrow_value)) = (&column_type.value, kv.get(1)) {
                check_type(value, arrow_value.data_type(), &format!("{path}.value"))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, AsArray, ListArray, RecordBatch, StructArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{
        DataType, Field as ArrowField, Fields, Schema, TimeUnit, TimestampMicrosecondType,
    };
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};

    use super::{check_timestamp_columns, timestamps_to_micros};

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn micros(array: &ArrayRef) -> Vec<Option<i64>> {
        array
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .collect()
    }

    fn micros_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }

    fn field(name: &str, column_type: ColumnType) -> Field {
        Field::new(SqlIdentifier::new(name, false), column_type)
    }

    fn timestamp(name: &str) -> Field {
        field(name, ColumnType::timestamp(true))
    }

    #[test]
    fn units() {
        let utc = Some("UTC".into());
        let fields = [timestamp("s"), timestamp("MS"), timestamp("ns")];
        let converted = timestamps_to_micros(
            batch(vec![
                (
                    "s",
                    Arc::new(TimestampSecondArray::from(vec![Some(-2), None, Some(3)])),
                ),
                (
                    "ms",
                    Arc::new(
                        TimestampMillisecondArray::from(vec![Some(-2), Some(3), None])
                            .with_timezone_opt(utc.clone()),
                    ),
                ),
                (
                    "ns",
                    Arc::new(TimestampNanosecondArray::from(vec![
                        Some(-1_500),
                        Some(1_500),
                        None,
                    ])),
                ),
            ]),
            &fields,
        )
        .unwrap();

        assert_eq!(
            micros(converted.column(0)),
            [Some(-2_000_000), None, Some(3_000_000)]
        );
        assert_eq!(
            micros(converted.column(1)),
            [Some(-2_000), Some(3_000), None]
        );
        assert_eq!(
            converted.schema().field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, utc)
        );
        // Rounds down, as parsing the formatted value did.
        assert_eq!(micros(converted.column(2)), [Some(-2), Some(1), None]);
    }

    #[test]
    fn nested() {
        let inner = StructArray::from(vec![(
            Arc::new(ArrowField::new(
                "t",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            Arc::new(TimestampMillisecondArray::from(vec![1, 2])) as ArrayRef,
        )]);
        let list = ListArray::new(
            Arc::new(ArrowField::new(
                "item",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            )),
            OffsetBuffer::from_lengths([2, 1]),
            Arc::new(TimestampSecondArray::from(vec![1, 2, 3])),
            None,
        );
        let fields = [
            field("row", ColumnType::structure(true, &[timestamp("t")])),
            field("list", ColumnType::array(true, ColumnType::timestamp(true))),
        ];
        let converted = timestamps_to_micros(
            batch(vec![("row", Arc::new(inner)), ("list", Arc::new(list))]),
            &fields,
        )
        .unwrap();

        let row = converted.column(0).as_struct();
        assert_eq!(micros(row.column(0)), [Some(1_000), Some(2_000)]);
        assert_eq!(
            converted.schema().field(0).data_type(),
            &DataType::Struct(Fields::from(vec![ArrowField::new(
                "t",
                micros_type(),
                true
            )]))
        );
        let list = converted.column(1).as_list::<i32>();
        assert_eq!(
            micros(list.values()),
            [Some(1_000_000), Some(2_000_000), Some(3_000_000)]
        );
        assert_eq!(list.value_offsets(), [0, 2, 3]);
    }

    #[test]
    fn micros_unchanged() {
        let column: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1, 2]));
        let converted =
            timestamps_to_micros(batch(vec![("t", column.clone())]), &[timestamp("t")]).unwrap();
        assert!(Arc::ptr_eq(converted.column(0), &column));
    }

    #[test]
    fn overflow() {
        let error = timestamps_to_micros(
            batch(vec![(
                "s",
                Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
            )]),
            &[timestamp("s")],
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not fit"), "{error}");
    }

    /// A timestamp that a SQL type other than TIMESTAMP reads keeps its unit.
    #[test]
    fn only_sql_timestamps() {
        let nanos = || Arc::new(TimestampNanosecondArray::from(vec![1_500])) as ArrayRef;
        let nanos_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let row = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("t", nanos_type.clone(), true)),
                nanos(),
            ),
            (
                Arc::new(ArrowField::new("n", nanos_type.clone(), true)),
                nanos(),
            ),
        ]);
        let fields = [
            timestamp("t"),
            field("n", ColumnType::bigint(true)),
            field("s", ColumnType::varchar(true)),
            field(
                "row",
                ColumnType::structure(
                    true,
                    &[timestamp("t"), field("n", ColumnType::bigint(true))],
                ),
            ),
        ];
        let converted = timestamps_to_micros(
            batch(vec![
                ("T", nanos()),
                ("n", nanos()),
                ("s", nanos()),
                ("row", Arc::new(row)),
                ("other", nanos()),
            ]),
            &fields,
        )
        .unwrap();

        let schema = converted.schema();
        assert_eq!(micros(converted.column(0)), [Some(1)]);
        for unchanged in [1, 2, 4] {
            assert_eq!(schema.field(unchanged).data_type(), &nanos_type);
        }
        let row = converted.column(3).as_struct();
        assert_eq!(micros(row.column(0)), [Some(1)]);
        assert_eq!(row.column(1).data_type(), &nanos_type);
    }

    #[test]
    fn check() {
        let after = ColumnType::structure(
            true,
            &[Field::new(
                SqlIdentifier::new("time", false),
                ColumnType::timestamp(true),
            )],
        );
        let fields = [
            Field::new(SqlIdentifier::new("ts", false), ColumnType::timestamp(true)),
            Field::new(SqlIdentifier::new("after", false), after),
            Field::new(
                SqlIdentifier::new("missing", false),
                ColumnType::timestamp(true),
            ),
        ];
        let schema = |time: DataType| {
            Schema::new(vec![
                ArrowField::new("TS", micros_type(), true),
                ArrowField::new(
                    "after",
                    DataType::Struct(Fields::from(vec![ArrowField::new("time", time, true)])),
                    true,
                ),
            ])
        };

        check_timestamp_columns(&fields, &schema(micros_type())).unwrap();
        check_timestamp_columns(&fields, &schema(DataType::Utf8)).unwrap();
        let error = check_timestamp_columns(&fields, &schema(DataType::Int64)).unwrap_err();
        assert!(
            error.starts_with("column after.time has SQL type TIMESTAMP"),
            "{error}"
        );

        let ints = Schema::new(vec![ArrowField::new("ts", DataType::Int64, true)]);
        assert!(check_timestamp_columns(&fields, &ints).is_err());

        // A read by name can take the value of any column with this name.
        let both = Schema::new(vec![
            ArrowField::new("ts", micros_type(), true),
            ArrowField::new("TS", DataType::Int64, true),
        ]);
        assert!(check_timestamp_columns(&fields, &both).is_err());
    }
}
