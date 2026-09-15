//! Pairing a data file's columns with the Delta table's schema.
//!
//! Under `delta.columnMapping.mode = 'name'` or `'id'` a column's name on disk
//! is not the name the table's schema gives it, so the two sides pair by field
//! id: Delta stamps `delta.columnMapping.id` on the schema and a writer stamps
//! `PARQUET:field_id` on the file. Nested fields need the same treatment, which
//! Arrow's `cast` cannot give them, so a struct, list or map is rebuilt here
//! instead.
//!
//! [`super::field_id_adapter`] applies this to a listing's scan;
//! [`project_to_logical`] applies it batch by batch to the reader that a
//! deletion vector forces.

use arrow::array::{
    Array, ArrayRef, LargeListArray, ListArray, MapArray, StructArray, new_null_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

/// A field's Parquet field id. The data file stamps `PARQUET:field_id`; the Delta
/// read schema carries `delta.columnMapping.id`. Either identifies the same column.
pub(super) fn field_id(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .or_else(|| field.metadata().get("delta.columnMapping.id"))
        .map(String::as_str)
}

/// Index a field list by field id, skipping fields without one.
pub(super) fn field_index_by_id(fields: &Fields) -> HashMap<&str, usize> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| field_id(f).map(|id| (id, i)))
        .collect()
}

/// Where `want` lives in `file`: by field id, else by name. A name match must
/// be on an id-less column, since one carrying another id is another column.
pub(super) fn source_index(
    file: &Schema,
    by_id: &HashMap<&str, usize>,
    want: &Field,
) -> Option<usize> {
    match field_id(want) {
        Some(id) => by_id.get(id).copied().or_else(|| {
            file.fields()
                .iter()
                .position(|f| field_id(f).is_none() && f.name() == want.name())
        }),
        None => file.index_of(want.name()).ok(),
    }
}

/// `file` names the file when the caller knows it; a listing's reader does not.
fn conversion_error(
    file: Option<&str>,
    column: &str,
    from: &DataType,
    to: &DataType,
    cause: impl fmt::Display,
) -> DataFusionError {
    let of_file = file.map_or(String::new(), |f| format!(" of file '{f}'"));
    DataFusionError::External(
        format!(
            "Delta file reader: cannot read column '{column}'{of_file}: the file stores \
             it as {from:?}, which is not convertible to the {to:?} that the Delta table's \
             schema declares: {cause}"
        )
        .into(),
    )
}

/// The element field of a container type: a list's element, a map's entries.
/// Delta has no fixed-size list, so that Arrow type never appears here.
fn container_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element) | DataType::LargeList(element) | DataType::Map(element, _) => {
            Some(element)
        }
        _ => None,
    }
}

/// Rebuild container `array` around elements realigned to `target`'s element
/// type, keeping the array's own container kind. `None` when either side is not
/// a container, leaving the caller to handle it.
fn realign_container(
    array: &ArrayRef,
    target: &DataType,
    file: Option<&str>,
    column: &str,
) -> Result<Option<ArrayRef>, DataFusionError> {
    let Some(element) = container_element(target) else {
        return Ok(None);
    };
    let realigned = |values: &ArrayRef| realign_array(values, element.data_type(), file, column);
    let rebuild_error =
        |e: arrow::error::ArrowError| conversion_error(file, column, array.data_type(), target, e);

    let rebuilt: ArrayRef = if let Some(source) = array.as_any().downcast_ref::<ListArray>() {
        Arc::new(
            ListArray::try_new(
                Arc::clone(element),
                source.offsets().clone(),
                realigned(source.values())?,
                source.nulls().cloned(),
            )
            .map_err(rebuild_error)?,
        )
    } else if let Some(source) = array.as_any().downcast_ref::<LargeListArray>() {
        Arc::new(
            LargeListArray::try_new(
                Arc::clone(element),
                source.offsets().clone(),
                realigned(source.values())?,
                source.nulls().cloned(),
            )
            .map_err(rebuild_error)?,
        )
    } else if let Some(source) = array.as_any().downcast_ref::<MapArray>() {
        // A map's element is its entries struct, which holds the key and value.
        let entries: ArrayRef = Arc::new(source.entries().clone());
        let entries = realign_array(&entries, element.data_type(), file, column)?;
        let Some(entries) = entries.as_any().downcast_ref::<StructArray>() else {
            return Ok(None);
        };
        Arc::new(
            MapArray::try_new(
                Arc::clone(element),
                source.offsets().clone(),
                entries.clone(),
                source.nulls().cloned(),
                // Delta never marks a map sorted, so this matches the target.
                matches!(target, DataType::Map(_, true)),
            )
            .map_err(rebuild_error)?,
        )
    } else {
        return Ok(None);
    };
    Ok(Some(rebuilt))
}

/// Convert a file column `array` to the `target` type the read schema expects.
/// `file` and `column` (dotted for a nested child) locate it in errors.
///
/// Under column mapping a struct's field names differ between the file and the
/// schema (a file may use logical names, the schema uses `col-<id>`), so a
/// struct is rebuilt: each target child takes the source child with the same
/// field id, or an id-less one of the same name. Position serves an unmapped
/// struct, where no side carries ids. A target child the file lacks is
/// null-filled.
///
/// A list or map is rebuilt around its realigned elements, because a struct inside
/// one needs the same id matching: `cast` pairs children by name, falling back
/// to position when no name matches, which is every column-mapped struct.
/// `cast` handles scalars and the container kind itself (`List` vs `LargeList`).
pub(super) fn realign_array(
    array: &ArrayRef,
    target: &DataType,
    file: Option<&str>,
    column: &str,
) -> Result<ArrayRef, DataFusionError> {
    // Errors name the file's own type, not an intermediate one.
    let cast_to_target = |from: &ArrayRef| {
        cast(from, target).map_err(|e| conversion_error(file, column, array.data_type(), target, e))
    };
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    if let Some(rebuilt) = realign_container(array, target, file, column)? {
        // The rebuild keeps the file's container kind, so a target that spells
        // it differently needs one more cast, of the offsets alone.
        return if rebuilt.data_type() == target {
            Ok(rebuilt)
        } else {
            cast_to_target(&rebuilt)
        };
    }
    let DataType::Struct(target_fields) = target else {
        return cast_to_target(array);
    };
    let Some(source) = array.as_any().downcast_ref::<StructArray>() else {
        return cast_to_target(array);
    };
    let src_idx_by_id = field_index_by_id(source.fields());
    // Position is for an unmapped struct alone: once any target field carries an
    // id, the source child at the same position may belong to another field.
    let target_has_ids = target_fields.iter().any(|f| field_id(f).is_some());
    let children = target_fields
        .iter()
        .enumerate()
        .map(|(pos, tf)| {
            // Match by id, then by name, but only against a child that carries
            // no id of its own: a child naming a different id means it, and
            // taking it would read one column's values under another's name.
            let idx = field_id(tf)
                .and_then(|id| src_idx_by_id.get(id).copied())
                .or_else(|| {
                    source
                        .fields()
                        .iter()
                        .position(|sf| field_id(sf).is_none() && sf.name() == tf.name())
                })
                .or_else(|| (!target_has_ids).then_some(pos));
            match idx.and_then(|i| source.columns().get(i)) {
                Some(child) => realign_array(
                    child,
                    tf.data_type(),
                    file,
                    &format!("{column}.{}", tf.name()),
                ),
                None => Ok(new_null_array(tf.data_type(), source.len())),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The children match `target_fields` by construction, so this rejects only a
    // null-filled child of a NOT NULL target field, i.e. a column the file lacks
    // that the table requires.
    StructArray::try_new(target_fields.clone(), children, source.nulls().cloned())
        .map(|s| Arc::new(s) as ArrayRef)
        .map_err(|e| conversion_error(file, column, array.data_type(), target, e))
}

/// Project `batch` onto `logical_schema`, matching columns by field id (falling
/// back to name), rebuilding nested shapes and casting leaves, and null-filling
/// columns the file lacks. Field-id matching handles `columnMapping.mode=id`
/// tables, whose files name columns logically rather than by physical `col-<id>`.
///
/// Partition columns are absent here (Delta stores them in `partitionValues`,
/// not in the file); the caller adds them back as constants.
pub(super) fn project_to_logical(
    batch: &RecordBatch,
    logical_schema: &SchemaRef,
    file: &str,
) -> Result<RecordBatch, DataFusionError> {
    let num_rows = batch.num_rows();
    let file_schema = batch.schema();
    let file_idx_by_id = field_index_by_id(file_schema.fields());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical_schema.fields().len());
    for field in logical_schema.fields().iter() {
        let col = match source_index(&file_schema, &file_idx_by_id, field) {
            Some(idx) => realign_array(
                batch.column(idx),
                field.data_type(),
                Some(file),
                field.name(),
            )?,
            None => new_null_array(field.data_type(), num_rows),
        };
        columns.push(col);
    }
    RecordBatch::try_new(Arc::clone(logical_schema), columns).map_err(|e| {
        DataFusionError::External(
            format!(
                "Delta file reader: file '{file}' does not satisfy the Delta table's schema: {e}. \
                 A column the file lacks is read as NULL, which the table rejects when it \
                 declares the column NOT NULL."
            )
            .into(),
        )
    })
}

/// Build the [`ProjectionMask`] selecting the root file columns `logical_schema`
/// wants, matched by field id (falling back to name); the rest are never decoded.
pub(super) fn logical_projection_mask(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    logical_schema: &SchemaRef,
) -> ProjectionMask {
    let want_ids: HashSet<&str> = logical_schema
        .fields()
        .iter()
        .filter_map(|f| field_id(f))
        .collect();
    let roots = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field_id(field).is_some_and(|id| want_ids.contains(id))
                || logical_schema.column_with_name(field.name()).is_some()
        })
        .map(|(idx, _)| idx);
    ProjectionMask::roots(builder.parquet_schema(), roots)
}

#[cfg(test)]
pub(in crate::integrated::delta_table) mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };

    /// Stands in for the data file the reader is decoding; it appears in errors.
    const TEST_FILE: &str = "part-00000.parquet";

    pub(in crate::integrated::delta_table) fn with_id(
        field: ArrowField,
        key: &str,
        id: &str,
    ) -> ArrowField {
        field.with_metadata(HashMap::from([(key.to_string(), id.to_string())]))
    }

    /// Source `struct<alpha (id 5), beta (id 6)>` for a list element or map value,
    /// paired with the target that lists the same two children in the opposite
    /// order, so pairing by position swaps the values.
    fn reordered_struct_pair() -> (StructArray, ArrowFields) {
        let source = StructArray::from(vec![
            (
                Arc::new(with_id(
                    ArrowField::new("alpha", ArrowDataType::Utf8, true),
                    "PARQUET:field_id",
                    "5",
                )),
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            ),
            (
                Arc::new(with_id(
                    ArrowField::new("beta", ArrowDataType::Utf8, true),
                    "PARQUET:field_id",
                    "6",
                )),
                Arc::new(StringArray::from(vec!["B"])) as ArrayRef,
            ),
        ]);
        let target = ArrowFields::from(vec![
            with_id(
                ArrowField::new("col-6", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "6",
            ),
            with_id(
                ArrowField::new("col-5", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "5",
            ),
        ]);
        (source, target)
    }

    /// The value of `array`'s first child, as a string.
    fn first_child_value(array: &ArrayRef) -> String {
        array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    // A struct inside a container is column-mapped like any other, so realign
    // must recurse by field id. The two sides share no child name here, so
    // `cast` would pair them by position and swap the values. `LargeList` is
    // Arrow's own spelling; `delta_table_follow_uniform_field_id_test` covers
    // Delta's `List` and `Map` end to end.
    #[test]
    fn realign_array_matches_nested_struct_by_field_id() {
        use arrow::buffer::OffsetBuffer;

        let (value, target_fields) = reordered_struct_pair();
        let element =
            |element_type: ArrowDataType| Arc::new(ArrowField::new("element", element_type, true));
        let list: ArrayRef = Arc::new(
            LargeListArray::try_new(
                element(value.data_type().clone()),
                OffsetBuffer::new(vec![0i64, 1].into()),
                Arc::new(value) as ArrayRef,
                None,
            )
            .unwrap(),
        );

        // A `List` target over a `LargeList` file also exercises the coercion
        // the rebuild leaves to `cast`.
        let target = ArrowDataType::List(element(ArrowDataType::Struct(target_fields)));
        let out = realign_array(&list, &target, Some(TEST_FILE), "history").unwrap();
        assert_eq!(out.data_type(), &target);
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            first_child_value(out.values()),
            "B",
            "the target's first child is id 6, so it must carry beta's value"
        );
    }

    // The rebuild reports a rejected element with the column and file, which
    // Arrow's own error lacks.
    #[test]
    fn realign_array_rejects_null_element_of_not_null_target() {
        use arrow::array::{ListBuilder, StringBuilder};
        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("a");
        b.values().append_null();
        b.append(true);
        let source: ArrayRef = Arc::new(b.finish());

        let target = ArrowDataType::List(Arc::new(ArrowField::new(
            "element",
            ArrowDataType::Utf8,
            false,
        )));
        let error = realign_array(&source, &target, Some(TEST_FILE), "items")
            .expect_err("a null element must not pass into a NOT NULL element type")
            .to_string();
        assert!(error.contains("cannot read column 'items'"), "{error}");
        assert!(error.contains(TEST_FILE), "{error}");
        assert!(error.contains("cannot contain nulls"), "{error}");
    }

    // The read schema's list kind may differ from the file's (Delta `List` vs a
    // file's `LargeList`); realign must coerce the container instead of failing.
    #[test]
    fn realign_array_coerces_list_containers() {
        use arrow::array::{LargeListBuilder, StringBuilder};
        let mut b = LargeListBuilder::new(StringBuilder::new());
        b.values().append_value("a");
        b.values().append_value("b");
        b.append(true);
        b.values().append_value("c");
        b.append(true);
        let source: ArrayRef = Arc::new(b.finish());

        let target =
            ArrowDataType::List(Arc::new(ArrowField::new("item", ArrowDataType::Utf8, true)));
        let out = realign_array(&source, &target, Some(TEST_FILE), "items").unwrap();
        assert_eq!(out.data_type(), &target);
        assert_eq!(out.len(), 2);
    }

    // A columnMapping.mode=id file names columns logically (`op`, `after`) and
    // carries `PARQUET:field_id`; the Delta read schema uses physical `col-<id>`
    // names and `delta.columnMapping.id`. project_to_logical must pair them by
    // field id, not name, else it null-fills and drops the data.
    #[test]
    fn project_to_logical_matches_by_field_id() {
        let file_after: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("transaction__id", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            Arc::new(StringArray::from(vec!["t1"])) as ArrayRef,
        )]));
        let file_op: ArrayRef = Arc::new(StringArray::from(vec!["INSERT"]));
        let file_schema = Arc::new(ArrowSchema::new(vec![
            with_id(
                ArrowField::new("after", file_after.data_type().clone(), true),
                "PARQUET:field_id",
                "1",
            ),
            with_id(
                ArrowField::new("op", ArrowDataType::Utf8, false),
                "PARQUET:field_id",
                "8",
            ),
        ]));
        let batch = RecordBatch::try_new(file_schema, vec![file_after, file_op]).unwrap();

        let read_schema = Arc::new(ArrowSchema::new(vec![
            with_id(
                ArrowField::new(
                    "col-1",
                    ArrowDataType::Struct(ArrowFields::from(vec![with_id(
                        ArrowField::new("col-2", ArrowDataType::Utf8, true),
                        "delta.columnMapping.id",
                        "2",
                    )])),
                    true,
                ),
                "delta.columnMapping.id",
                "1",
            ),
            with_id(
                ArrowField::new("col-8", ArrowDataType::Utf8, false),
                "delta.columnMapping.id",
                "8",
            ),
        ]));

        let out = project_to_logical(&batch, &read_schema, TEST_FILE).unwrap();
        assert_eq!(out.schema().field(1).name(), "col-8");
        let op = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            op.value(0),
            "INSERT",
            "op resolved by field id, not null-filled"
        );
        let after = out
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            after
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "t1"
        );
    }

    // A struct child the file lacks (e.g. a field added to the struct after the
    // file was written) must null-fill, not error or grab a wrong-id sibling.
    #[test]
    fn realign_array_null_fills_missing_struct_child() {
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("id", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            Arc::new(StringArray::from(vec!["t1", "t2"])) as ArrayRef,
        )]));

        // Target wants both id 2 (present) and id 3 (absent from the file).
        let target = ArrowDataType::Struct(ArrowFields::from(vec![
            with_id(
                ArrowField::new("col-2", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "2",
            ),
            with_id(
                ArrowField::new("col-3", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "3",
            ),
        ]));

        let out = realign_array(&source, &target, Some(TEST_FILE), "after").unwrap();
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        let present = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(present.value(0), "t1");
        let missing = out
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(missing.len(), 2);
        assert!(missing.is_null(0) && missing.is_null(1));
    }

    // A file may name a struct's children physically and omit their Parquet
    // field ids. `project_to_logical` falls back to the name at the top
    // level, so the struct branch must too, or those children read as NULL.
    #[test]
    fn realign_array_matches_struct_child_by_name_without_an_id() {
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("col-2", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["t1"])) as ArrayRef,
        )]));
        let target = ArrowDataType::Struct(ArrowFields::from(vec![with_id(
            ArrowField::new("col-2", ArrowDataType::Utf8, true),
            "delta.columnMapping.id",
            "2",
        )]));

        let out = realign_array(&source, &target, Some(TEST_FILE), "after").unwrap();
        assert_eq!(
            first_child_value(&out),
            "t1",
            "an id-less child sharing the target's name must not be null-filled"
        );
    }

    // A source child that names a different id is that other column, whatever
    // it is called, so the name must not claim it.
    #[test]
    fn realign_array_ignores_a_name_match_carrying_another_id() {
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("col-5", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "10",
            )),
            Arc::new(StringArray::from(vec!["ten"])) as ArrayRef,
        )]));
        let target = ArrowDataType::Struct(ArrowFields::from(vec![with_id(
            ArrowField::new("col-5", ArrowDataType::Utf8, true),
            "delta.columnMapping.id",
            "5",
        )]));

        let out = realign_array(&source, &target, Some(TEST_FILE), "after").unwrap();
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(
            out.column(0).is_null(0),
            "id 10's values must not be read as id 5"
        );
    }

    // Once a struct's fields carry ids, position says nothing: the id-carrying
    // children match out of position, so a child left unmatched must read NULL
    // rather than whatever sits at its index.
    #[test]
    fn realign_array_null_fills_an_unmatched_id_less_child() {
        let source: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(ArrowField::new("gamma", ArrowDataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["G"])) as ArrayRef,
            ),
            (
                Arc::new(with_id(
                    ArrowField::new("alpha", ArrowDataType::Utf8, true),
                    "PARQUET:field_id",
                    "5",
                )),
                Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            ),
        ]));
        let target = ArrowDataType::Struct(ArrowFields::from(vec![
            with_id(
                ArrowField::new("col-5", ArrowDataType::Utf8, true),
                "delta.columnMapping.id",
                "5",
            ),
            ArrowField::new("beta", ArrowDataType::Utf8, true),
        ]));

        let out = realign_array(&source, &target, Some(TEST_FILE), "after").unwrap();
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "A",
            "id 5 must come from the child naming it"
        );
        assert!(
            out.column(1).is_null(0),
            "an unmatched child must not take id 5's neighbor by position"
        );
    }

    // The top-level match follows the same rule as the struct children: a file
    // column naming another id is that column, whatever it is called.
    #[test]
    fn project_to_logical_ignores_a_name_match_carrying_another_id() {
        let file_schema = Arc::new(ArrowSchema::new(vec![with_id(
            ArrowField::new("col-5", ArrowDataType::Utf8, true),
            "PARQUET:field_id",
            "10",
        )]));
        let batch = RecordBatch::try_new(
            file_schema,
            vec![Arc::new(StringArray::from(vec!["ten"])) as ArrayRef],
        )
        .unwrap();
        let read_schema = Arc::new(ArrowSchema::new(vec![with_id(
            ArrowField::new("col-5", ArrowDataType::Utf8, true),
            "delta.columnMapping.id",
            "5",
        )]));

        let out = project_to_logical(&batch, &read_schema, TEST_FILE).unwrap();
        assert!(
            out.column(0).is_null(0),
            "id 10's values must not be read as id 5"
        );
    }

    // A user hitting a type mismatch sees only the physical `col-<uuid>` name on
    // disk, so the error has to name the column, the file, and both types. Arrow's
    // bare cast error carries none of that.
    #[test]
    fn conversion_error_names_column_file_and_types() {
        let file_child: ArrayRef = Arc::new(StringArray::from(vec!["not-a-timestamp"]));
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(with_id(
                ArrowField::new("amount", ArrowDataType::Utf8, true),
                "PARQUET:field_id",
                "2",
            )),
            file_child,
        )]));
        // Utf8 to a fixed-size binary is not a cast Arrow supports.
        let target = ArrowDataType::Struct(ArrowFields::from(vec![with_id(
            ArrowField::new("col-2", ArrowDataType::FixedSizeBinary(16), true),
            "delta.columnMapping.id",
            "2",
        )]));

        let err = realign_array(&source, &target, Some(TEST_FILE), "after")
            .expect_err("Utf8 does not cast to FixedSizeBinary")
            .to_string();

        // The nested child, not just the top-level column.
        assert!(err.contains("'after.col-2'"), "{err}");
        assert!(err.contains(TEST_FILE), "{err}");
        assert!(err.contains("Utf8"), "{err}");
        assert!(err.contains("FixedSizeBinary(16)"), "{err}");
    }
}
