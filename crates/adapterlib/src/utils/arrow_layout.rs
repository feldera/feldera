//! Puts the columns of a record batch in the order of the SQL declaration, at
//! every depth, so that the deserializer can read each record by position
//! instead of by field name.
//!
//! The same pass gives each `DECIMAL` column as its 128-bit significands, so
//! that the deserializer does not read each value through a formatted string.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeBinaryArray, FixedSizeListArray, LargeListArray, ListArray,
    MapArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow::datatypes::{DataType, Decimal128Type, Field as ArrowField, FieldRef, Fields, Schema};
use feldera_types::program_schema::{ColumnType, Field, SqlType};

/// Schema metadata key of a batch whose columns `order_columns` put in order.
pub const POSITIONAL_FIELDS: &str = "feldera.positional_fields";

/// The value of [`POSITIONAL_FIELDS`]: a random number of this process.
///
/// Schema metadata also comes from outside: from a file, or from a client that
/// sends Arrow data. A batch that only claims to be in order would be read into
/// the wrong columns, so the mark is a value that only `order_columns` knows.
static POSITIONAL_FIELDS_TOKEN: LazyLock<String> =
    LazyLock::new(|| format!("{:032x}", rand::random::<u128>()));

/// Tells whether `order_columns` of this process put the columns of a batch
/// in order.
pub fn has_positional_fields(schema: &Schema) -> bool {
    schema
        .metadata()
        .get(POSITIONAL_FIELDS)
        .is_some_and(|value| *value == *POSITIONAL_FIELDS_TOKEN)
}

/// Puts the columns of `batch`, and of each nested `ROW`, in the order of
/// `fields`, drops the columns that `fields` does not have, and marks the
/// schema with [`POSITIONAL_FIELDS`].
///
/// A `Decimal128` column with the scale of its SQL column becomes a
/// `FixedSizeBinary(16)` column on the same buffer, which
/// `DecimalFormat::I128` reads as significands.
///
/// When the batch cannot be put in order, because a column is absent or
/// misspelled, returns the batch unchanged with the reason. The deserializer
/// then reads the records by name, which fills in an absent column from its
/// default and reports the other problems.
pub fn order_columns(batch: RecordBatch, fields: &[Field]) -> (RecordBatch, Option<String>) {
    let len = batch.num_rows();
    let ordered = order_struct(batch.schema_ref().fields(), batch.columns(), fields).and_then(
        |(arrow_fields, columns)| {
            let mut metadata = batch.schema_ref().metadata().clone();
            metadata.insert(
                POSITIONAL_FIELDS.to_string(),
                POSITIONAL_FIELDS_TOKEN.clone(),
            );
            let schema = Arc::new(Schema::new_with_metadata(arrow_fields, metadata));
            let options = RecordBatchOptions::new().with_row_count(Some(len));
            RecordBatch::try_new_with_options(schema, columns, &options).map_err(|e| e.to_string())
        },
    );
    match ordered {
        Ok(ordered) => (ordered, None),
        Err(reason) => (batch, Some(reason)),
    }
}

fn order_struct(
    arrow_fields: &Fields,
    columns: &[ArrayRef],
    fields: &[Field],
) -> Result<(Fields, Vec<ArrayRef>), String> {
    let mut out_fields = Vec::with_capacity(fields.len());
    let mut out_columns = Vec::with_capacity(fields.len());
    // Two SQL fields that differ only in case, such as `a` and `"A"`, can
    // both match one source column. The by-name path resolves that.
    let mut lowercase_names = HashSet::with_capacity(fields.len());
    for field in fields {
        let name = field.name.name();
        if !lowercase_names.insert(name.to_lowercase()) {
            return Err(format!(
                "more than one column is named {} when case is ignored",
                field.name.sql_name()
            ));
        }
        match find_column(arrow_fields, columns, &name, field.name.case_sensitive)? {
            Some((arrow_field, column)) => {
                let column = order_array(column, &field.columntype)?;
                out_fields.push(with_data_type(arrow_field, column.data_type()));
                out_columns.push(column);
            }
            // Only the by-name path knows the value of an absent column: NULL,
            // its default, or an error.
            None => return Err(format!("column {} is absent", field.name.sql_name())),
        }
    }
    Ok((out_fields.into(), out_columns))
}

/// Finds the one column named `name` when case is ignored. Its spelling must
/// be `name` if the SQL name is case-sensitive.
///
/// The by-name deserializer matches the spelling first and then the lowercase
/// name. With more than one candidate, such as `x` and `X`, its result depends
/// on the order of the columns, so it must make that choice.
fn find_column<'a>(
    arrow_fields: &'a Fields,
    columns: &'a [ArrayRef],
    name: &str,
    case_sensitive: bool,
) -> Result<Option<(&'a FieldRef, &'a ArrayRef)>, String> {
    let lowercase = name.to_lowercase();
    let mut found = None;
    for (arrow_field, column) in arrow_fields.iter().zip(columns) {
        if arrow_field.name().to_lowercase() != lowercase {
            continue;
        }
        if found.is_some() {
            return Err(format!(
                "the source has more than one column named {name} when case is ignored"
            ));
        }
        found = Some((arrow_field, column));
    }
    match found {
        Some((arrow_field, _)) if case_sensitive && arrow_field.name() != name => Err(format!(
            "case-sensitive column {name} is spelled {} in the source",
            arrow_field.name()
        )),
        found => Ok(found),
    }
}

fn order_array(array: &ArrayRef, column_type: &ColumnType) -> Result<ArrayRef, String> {
    if !has_struct(column_type) && !has_decimal(column_type) {
        return Ok(array.clone());
    }
    match (&column_type.typ, array.data_type()) {
        (SqlType::Decimal, DataType::Decimal128(_, scale))
            if column_type.scale == Some(i64::from(*scale)) =>
        {
            let array = array.as_primitive::<Decimal128Type>();
            // The buffer of 128-bit values is also a buffer of 16-byte values.
            FixedSizeBinaryArray::try_new(
                16,
                array.values().inner().clone(),
                array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        (SqlType::Struct, DataType::Struct(_)) => {
            let Some(fields) = &column_type.fields else {
                return Err("a ROW column has no field list".to_string());
            };
            let array = array.as_struct();
            let (arrow_fields, columns) = order_struct(array.fields(), array.columns(), fields)?;
            StructArray::try_new_with_length(
                arrow_fields,
                columns,
                array.nulls().cloned(),
                array.len(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        (SqlType::Array, DataType::List(field)) => {
            let array = array.as_list::<i32>();
            let values = order_array(array.values(), component(column_type)?)?;
            ListArray::try_new(
                with_data_type(field, values.data_type()),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        (SqlType::Array, DataType::LargeList(field)) => {
            let array = array.as_list::<i64>();
            let values = order_array(array.values(), component(column_type)?)?;
            LargeListArray::try_new(
                with_data_type(field, values.data_type()),
                array.offsets().clone(),
                values,
                array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        (SqlType::Array, DataType::FixedSizeList(field, size)) => {
            let array = array.as_fixed_size_list();
            let values = order_array(array.values(), component(column_type)?)?;
            FixedSizeListArray::try_new(
                with_data_type(field, values.data_type()),
                *size,
                values,
                array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        (SqlType::Map, DataType::Map(field, sorted)) => {
            let array = array.as_map();
            let entries = array.entries();
            let (Some(key), Some(value)) = (&column_type.key, &column_type.value) else {
                return Err("a MAP column has no key or value type".to_string());
            };
            let columns = vec![
                order_array(entries.column(0), key)?,
                order_array(entries.column(1), value)?,
            ];
            let entry_fields = entries
                .fields()
                .iter()
                .zip(&columns)
                .map(|(field, column)| with_data_type(field, column.data_type()))
                .collect();
            let entries = StructArray::try_new(entry_fields, columns, entries.nulls().cloned())
                .map_err(|e| e.to_string())?;
            MapArray::try_new(
                with_data_type(field, entries.data_type()),
                array.offsets().clone(),
                entries,
                array.nulls().cloned(),
                *sorted,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|e| e.to_string())
        }
        // A decimal in any other form is read through its string.
        _ if !has_struct(column_type) => Ok(array.clone()),
        (_, data_type) => Err(format!(
            "cannot put the fields of a ROW stored as Arrow type {data_type} in order"
        )),
    }
}

fn component(column_type: &ColumnType) -> Result<&ColumnType, String> {
    column_type
        .component
        .as_deref()
        .ok_or_else(|| "an ARRAY column has no element type".to_string())
}

/// Tells whether a value of this type holds a `ROW` at any depth.
fn has_struct(column_type: &ColumnType) -> bool {
    match column_type.typ {
        SqlType::Struct => true,
        SqlType::Array => column_type.component.as_deref().is_some_and(has_struct),
        SqlType::Map => {
            column_type.key.as_deref().is_some_and(has_struct)
                || column_type.value.as_deref().is_some_and(has_struct)
        }
        _ => false,
    }
}

/// Tells whether a value of this type holds a `DECIMAL` at any depth outside a `ROW`.
fn has_decimal(column_type: &ColumnType) -> bool {
    match column_type.typ {
        SqlType::Decimal => true,
        SqlType::Array => column_type.component.as_deref().is_some_and(has_decimal),
        SqlType::Map => {
            column_type.key.as_deref().is_some_and(has_decimal)
                || column_type.value.as_deref().is_some_and(has_decimal)
        }
        _ => false,
    }
}

fn with_data_type(field: &FieldRef, data_type: &DataType) -> FieldRef {
    if field.data_type() == data_type {
        field.clone()
    } else {
        Arc::new(ArrowField::clone(field).with_data_type(data_type.clone()))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, Decimal128Array, Int64Array, ListArray, RecordBatch, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field as ArrowField, Fields, Int64Type};
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};

    use super::{POSITIONAL_FIELDS, has_positional_fields, order_columns};

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn field(name: &str, column_type: ColumnType) -> Field {
        Field::new(SqlIdentifier::new(name, false), column_type)
    }

    fn values(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<Int64Type>().iter().collect()
    }

    #[test]
    fn reorders_and_drops_extra_columns() {
        let batch = batch(vec![
            ("extra", ints(&[9])),
            ("B", ints(&[2])),
            ("a", ints(&[1])),
        ]);
        let fields = [
            field("a", ColumnType::bigint(false)),
            field("b", ColumnType::bigint(false)),
        ];

        let (ordered, reason) = order_columns(batch, &fields);

        assert_eq!(reason, None);
        assert!(has_positional_fields(ordered.schema_ref()));
        assert_eq!(ordered.num_columns(), 2);
        assert_eq!(values(ordered.column(0)), vec![Some(1)]);
        assert_eq!(values(ordered.column(1)), vec![Some(2)]);
    }

    /// An absent column needs the by-name path, which knows its default.
    #[test]
    fn absent_column() {
        let batch = batch(vec![("other", ints(&[1, 2]))]);
        let fields = [field("a", ColumnType::bigint(true))];

        let (unchanged, reason) = order_columns(batch, &fields);

        assert!(reason.unwrap().contains("absent"));
        assert!(!has_positional_fields(unchanged.schema_ref()));
        assert_eq!(unchanged.column(0).len(), 2);
    }

    /// A batch from outside that claims to be in order is not read by position.
    #[test]
    fn mark_from_outside() {
        let marked = |columns| {
            let batch = batch(columns);
            let metadata = [(POSITIONAL_FIELDS.to_string(), "true".to_string())].into();
            let schema = batch.schema().as_ref().clone().with_metadata(metadata);
            batch.with_schema(Arc::new(schema)).unwrap()
        };
        let fields = [field("a", ColumnType::bigint(true))];

        let in_order = marked(vec![("a", ints(&[1]))]);
        assert!(!has_positional_fields(in_order.schema_ref()));

        // The batch that cannot be put in order keeps its metadata.
        let (unchanged, reason) = order_columns(marked(vec![("other", ints(&[1]))]), &fields);
        assert!(reason.is_some());
        assert!(!has_positional_fields(unchanged.schema_ref()));
    }

    /// A sliced batch, as DataFusion produces, keeps its offset.
    #[test]
    fn sliced_decimal_significands() {
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(2), None, Some(4)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let batch = batch(vec![("d", decimals)]).slice(1, 3);
        let fields = [field("d", ColumnType::decimal(10, 2, true))];

        let (ordered, reason) = order_columns(batch, &fields);

        assert_eq!(reason, None);
        let significands = ordered.column(0).as_fixed_size_binary();
        assert_eq!(significands.len(), 3);
        assert_eq!(significands.value(0), 2_i128.to_ne_bytes());
        assert!(significands.is_null(1));
        assert_eq!(significands.value(2), 4_i128.to_ne_bytes());
    }

    /// Names that differ only in case have more than one possible match, in
    /// the SQL fields or in the source. The by-name path resolves them.
    #[test]
    fn names_that_differ_only_in_case() {
        let sensitive =
            |name: &str| Field::new(SqlIdentifier::new(name, true), ColumnType::bigint(true));
        let both = [field("a", ColumnType::bigint(true)), sensitive("A")];

        // `a` and `"A"`: one source column must not fill both.
        let (_, reason) = order_columns(batch(vec![("A", ints(&[5]))]), &both);
        assert!(reason.unwrap().contains("when case is ignored"));
        let source = vec![("a", ints(&[10])), ("A", ints(&[20]))];
        let (_, reason) = order_columns(batch(source.clone()), &both);
        assert!(reason.unwrap().contains("when case is ignored"));

        // One SQL column, two source columns `a` and `A`.
        let (_, reason) = order_columns(batch(source), &[field("a", ColumnType::bigint(true))]);
        assert!(reason.unwrap().contains("more than one column named a"));

        // The same in a nested ROW, also inside a MAP value.
        let row = ColumnType::structure(true, &both);
        let nested = StructArray::from(vec![(
            Arc::new(ArrowField::new("A", DataType::Int64, true)),
            ints(&[5]),
        )]);
        let (_, reason) = order_columns(
            batch(vec![("r", Arc::new(nested) as ArrayRef)]),
            &[field("r", row)],
        );
        assert!(reason.unwrap().contains("when case is ignored"));
    }

    /// A ROW that the source does not store as a struct cannot be put in
    /// order. A column without a ROW or a DECIMAL passes in any layout.
    #[test]
    fn unsupported_layout() {
        let row = ColumnType::structure(true, &[field("x", ColumnType::bigint(true))]);
        let (unchanged, reason) =
            order_columns(batch(vec![("r", ints(&[1]))]), &[field("r", row.clone())]);
        assert!(reason.unwrap().contains("stored as Arrow type Int64"));
        assert!(!has_positional_fields(unchanged.schema_ref()));

        let (_, reason) = order_columns(
            batch(vec![("r", ints(&[1]))]),
            &[field("r", ColumnType::array(true, row))],
        );
        assert!(reason.unwrap().contains("stored as Arrow type Int64"));

        // The deserializer reports the type mismatch of a plain column.
        let (ordered, reason) = order_columns(
            batch(vec![("s", ints(&[1]))]),
            &[field("s", ColumnType::varchar(true))],
        );
        assert_eq!(reason, None);
        assert!(has_positional_fields(ordered.schema_ref()));
    }

    #[test]
    fn case_sensitive_name() {
        let batch = || batch(vec![("A", ints(&[1]))]);
        let case_sensitive = [Field::new(
            SqlIdentifier::new("a", true),
            ColumnType::bigint(false),
        )];
        let (_, reason) = order_columns(batch(), &case_sensitive);
        assert!(reason.unwrap().contains("spelled A"));

        let (_, reason) = order_columns(batch(), &[field("a", ColumnType::bigint(false))]);
        assert_eq!(reason, None);
    }

    #[test]
    fn nested_row_in_array() {
        // An array of ROW(x, y) stored with the fields as (y, x).
        let row = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("y", DataType::Int64, false)),
                ints(&[20, 21]),
            ),
            (
                Arc::new(ArrowField::new("x", DataType::Int64, false)),
                ints(&[10, 11]),
            ),
        ]);
        let item = Arc::new(ArrowField::new("item", row.data_type().clone(), true));
        let list = ListArray::new(item, OffsetBuffer::from_lengths([2]), Arc::new(row), None);
        let batch = batch(vec![("rows", Arc::new(list) as ArrayRef)]);
        let row_type = ColumnType::structure(
            true,
            &[
                field("x", ColumnType::bigint(false)),
                field("y", ColumnType::bigint(false)),
            ],
        );
        let fields = [field("rows", ColumnType::array(true, row_type))];

        let (ordered, reason) = order_columns(batch, &fields);

        assert_eq!(reason, None);
        let rows = ordered
            .column(0)
            .as_list::<i32>()
            .values()
            .as_struct()
            .clone();
        assert_eq!(rows.column_names(), vec!["x", "y"]);
        assert_eq!(values(rows.column(0)), vec![Some(10), Some(11)]);
        let DataType::List(item) = ordered.column(0).data_type() else {
            panic!("not a list");
        };
        let DataType::Struct(item_fields) = item.data_type() else {
            panic!("not a struct");
        };
        assert_eq!(item_fields, &Fields::from(rows.fields().to_vec()));
    }

    /// A decimal with the scale of its SQL column is given as significands;
    /// with another scale it is left for the string path.
    #[test]
    fn decimal_significands() {
        let decimals = |scale| {
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None, Some(-1)])
                    .with_precision_and_scale(10, scale)
                    .unwrap(),
            ) as ArrayRef
        };
        let batch = batch(vec![("same", decimals(2)), ("other", decimals(3))]);
        let fields = [
            field("same", ColumnType::decimal(10, 2, true)),
            field("other", ColumnType::decimal(10, 2, true)),
        ];

        let (ordered, reason) = order_columns(batch, &fields);

        assert_eq!(reason, None);
        let same = ordered.column(0).as_fixed_size_binary();
        assert_eq!(same.value(0), 12345_i128.to_ne_bytes());
        assert!(same.is_null(1));
        assert_eq!(same.value(2), (-1_i128).to_ne_bytes());
        assert_eq!(ordered.schema().field(0).data_type(), same.data_type());
        assert_eq!(ordered.column(1).data_type(), &DataType::Decimal128(10, 3));
    }
}
