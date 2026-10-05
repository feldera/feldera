//! Pairing a data file's columns with the Delta table's schema.
//!
//! Under `delta.columnMapping.mode = 'name'` or `'id'` a column's name on disk
//! is not the name the table's schema gives it. There are two ways across, and
//! this module holds both.
//!
//! By name: Delta stamps each field's physical name into its metadata, so
//! [`field_to_physical`] builds the schema a file is read with, and
//! [`relabel_nested_columns`] puts the logical names back on the batch. Only
//! nested names need putting back; the top level is already logical.
//!
//! By field id: Delta stamps `delta.columnMapping.id` on the schema and a writer
//! stamps `PARQUET:field_id` on the file. Nested fields need the same treatment,
//! which Arrow's `cast` cannot give them, so a struct, list or map is rebuilt
//! here instead. [`super::field_id_adapter`] applies this to a listing's scan;
//! [`project_to_logical`] applies it batch by batch to the direct reader that a
//! deletion vector or an unlistable location forces.

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{
    Array, ArrayData, ArrayRef, LargeListArray, ListArray, MapArray, StructArray, make_array,
    new_null_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use delta_kernel::schema::ColumnMetadataKey;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

/// A field's Parquet field id. The data file stamps `PARQUET:field_id`; the Delta
/// read schema carries `delta.columnMapping.id`. Either identifies the same column.
fn field_id(field: &Field) -> Option<&str> {
    field
        .metadata()
        .get("PARQUET:field_id")
        .or_else(|| field.metadata().get("delta.columnMapping.id"))
        .map(String::as_str)
}

/// Index a field list by field id, skipping fields without one. Two fields
/// sharing an id is malformed; the later one wins.
pub(super) fn field_index_by_id(fields: &Fields) -> HashMap<&str, usize> {
    fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| field_id(f).map(|id| (id, i)))
        .collect()
}

/// Finds the field `want` among the fields of the schema `file`.
///
/// Matches by field id, then by name. A name match is accepted only against a
/// file column carrying no id of its own, since one carrying a different id is
/// a different column.
///
/// # Arguments
///
/// * `file` - the data file's schema, to search.
/// * `by_id` - `file`'s fields indexed by field id, from [`field_index_by_id`].
/// * `want` - the field to find.
///
/// # Returns
///
/// Its index into `file`'s fields, or `None` when the file has no such column.
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

/// Builds the error for a file column that cannot be read as the type the Delta
/// table's schema declares for it.
///
/// # Arguments
///
/// * `file` - the data file's path, when the caller knows it; a listing's
///   reader does not.
/// * `column` - the column's name, dotted for a nested child.
/// * `from` - the type the file stores.
/// * `to` - the type the table's schema declares.
/// * `cause` - the failure underneath.
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

/// Returns the element field of a container type: a list's element, or a map's
/// entries struct. `None` for any other type.
///
/// Delta has no fixed-size list, so that Arrow type never appears here.
fn container_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(element) | DataType::LargeList(element) | DataType::Map(element, _) => {
            Some(element)
        }
        _ => None,
    }
}

/// Rebuilds the list or map `array` around elements realigned to `target`'s
/// element type, keeping the array's own container kind.
///
/// # Arguments
///
/// * `array` - the file's column.
/// * `target` - the type the read schema declares for it.
/// * `file` - the data file's path, for errors.
/// * `column` - the column's name, for errors.
///
/// # Returns
///
/// The rebuilt array, or `None` when either side is not a container, which
/// leaves the caller to handle it.
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

/// Converts the file column `array` to the `target` type the read schema
/// declares.
///
/// Under column mapping the file and the schema name a struct's fields
/// differently, so a struct is rebuilt child by child: each target child takes
/// the source child of the same field id, or an id-less one of the same name,
/// or, in an unmapped struct where no side carries ids, the child at the same
/// position. A target child the file lacks is null-filled. A list or map is
/// rebuilt around its realigned elements, since a struct inside one needs that
/// same matching; `cast` handles the rest.
///
/// # Arguments
///
/// * `array` - the file's column.
/// * `target` - the type the read schema declares for it.
/// * `file` - the data file's path, for errors.
/// * `column` - the column's name, dotted for a nested child, for errors.
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
///
/// The name fallback is unconditional here, where [`source_index`] refuses one on
/// a column carrying another id. A mask only decides what to decode, so the looser
/// rule can over-select but cannot mispair: [`project_to_logical`] still pairs.
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

/// A field's physical (on-disk) name under column mapping, or its logical name
/// when unmapped. Delta stamps it into the Arrow field metadata at every level.
pub(super) fn physical_name(field: &Field) -> String {
    field
        .metadata()
        .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
        .cloned()
        .unwrap_or_else(|| field.name().clone())
}

/// Returns a copy of `field` whose own name, and every nested field name (struct
/// children and list/map element fields, at any depth), is `rename`d. Nullability
/// and metadata carry over unchanged.
fn rename_fields(field: &FieldRef, rename: &dyn Fn(&Field) -> String) -> FieldRef {
    Arc::new(
        Field::new(
            rename(field.as_ref()),
            rename_nested_fields(field.data_type(), rename),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone()),
    )
}

/// Recurse [`rename_fields`] into every field a container type holds. Scalar
/// types are returned unchanged.
fn rename_nested_fields(data_type: &DataType, rename: &dyn Fn(&Field) -> String) -> DataType {
    let renamed = |field: &FieldRef| rename_fields(field, rename);
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(renamed).collect()),
        DataType::List(field) => DataType::List(renamed(field)),
        DataType::LargeList(field) => DataType::LargeList(renamed(field)),
        DataType::FixedSizeList(field, len) => DataType::FixedSizeList(renamed(field), *len),
        DataType::Map(field, sorted) => DataType::Map(renamed(field), *sorted),
        other => other.clone(),
    }
}

/// Returns a copy of `field` named as it appears on disk at every level: each
/// column-mapped name becomes its physical name, unmapped names carry over.
pub(super) fn field_to_physical(field: &FieldRef) -> FieldRef {
    rename_fields(field, &physical_name)
}

/// Maps each nested column-mapped field's physical name to its logical name,
/// descending through struct children and list/map element fields at any depth.
/// Top-level fields are excluded. Empty unless the table nests column-mapped
/// fields.
pub(super) fn nested_physical_to_logical(schema: &Schema) -> HashMap<String, String> {
    fn collect(data_type: &DataType, map: &mut HashMap<String, String>) {
        for field in child_fields(data_type) {
            let physical = physical_name(field);
            if physical != *field.name() {
                map.insert(physical, field.name().clone());
            }
            collect(field.data_type(), map);
        }
    }
    let mut map = HashMap::new();
    for field in schema.fields() {
        collect(field.data_type(), &mut map);
    }
    map
}

/// The fields a container type holds directly: a struct's children, or the sole
/// element field of a list/map. Scalar types hold none.
fn child_fields(data_type: &DataType) -> Vec<&FieldRef> {
    match data_type {
        DataType::Struct(fields) => fields.iter().collect(),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => vec![field],
        _ => vec![],
    }
}

/// Rebuild `array` with every struct/list/map field name substituted through
/// `map`, at any nesting depth, reusing the underlying buffers. Only field-name
/// metadata changes; the physical layout is untouched. Names absent from `map`
/// carry over unchanged.
fn relabel_array(array: &ArrayRef, map: &HashMap<String, String>) -> AnyResult<ArrayRef> {
    Ok(make_array(relabel_array_data(array.to_data(), map)?))
}

/// Recursive core of [`relabel_array`], operating on the raw [`ArrayData`] tree.
fn relabel_array_data(data: ArrayData, map: &HashMap<String, String>) -> AnyResult<ArrayData> {
    let relabeled_type = relabel_data_type(data.data_type(), map);
    let children: Vec<ArrayData> = data
        .child_data()
        .iter()
        .map(|child| relabel_array_data(child.clone(), map))
        .collect::<AnyResult<_>>()?;
    // Relabeling only renames fields; buffers, offsets, lengths, and null bitmaps
    // carry over untouched, so the built data is structurally identical. `build`
    // only errors on a real layout mismatch, which would be a bug here.
    data.into_builder()
        .data_type(relabeled_type)
        .child_data(children)
        .build()
        .map_err(|e| anyhow!("relabeling column-mapped field names failed: {e}"))
}

/// Substitute nested field names in `data_type` through `map`. Names absent from
/// `map`, and scalar types, are left unchanged.
fn relabel_data_type(data_type: &DataType, map: &HashMap<String, String>) -> DataType {
    rename_nested_fields(data_type, &|field| {
        map.get(field.name())
            .cloned()
            .unwrap_or_else(|| field.name().clone())
    })
}

/// Translate a batch's nested field names physical-to-logical. Top-level names
/// are left as-is (already logical); only names nested inside a struct, list, or
/// map are rewritten.
pub(super) fn relabel_nested_columns(
    batch: &RecordBatch,
    map: &HashMap<String, String>,
) -> AnyResult<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| relabel_array(c, map))
        .collect::<AnyResult<_>>()?;
    let fields: Vec<FieldRef> = batch
        .schema()
        .fields()
        .iter()
        .zip(&columns)
        .map(|(f, c)| {
            Arc::new(
                Field::new(f.name(), c.data_type().clone(), f.is_nullable())
                    .with_metadata(f.metadata().clone()),
            )
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields).with_metadata(batch.schema().metadata().clone())),
        columns,
    )
    .map_err(|e| anyhow!("relabeling column-mapped field names failed: {e}"))
}

/// Copies `field` with field id `id` under metadata `key`, for a test that
/// builds a column-mapped schema by hand. `deletion_vector`'s reader tests build
/// the same shape, so it lives here rather than in either `mod tests`.
#[cfg(test)]
pub(super) fn with_id(field: Field, key: &str, id: &str) -> Field {
    field.with_metadata(HashMap::from([(key.to_string(), id.to_string())]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, StringViewArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields,
        Schema as ArrowSchema,
    };

    /// Stands in for the data file the reader is decoding; it appears in errors.
    const TEST_FILE: &str = "part-00000.parquet";

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

    // The map branch of the rebuild: a map whose value is a column-mapped struct
    // must pair the value's children by field id, as a list's element does.
    #[test]
    fn realign_array_matches_map_value_struct_by_field_id() {
        let (value, target_fields) = reordered_struct_pair();
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["k"]));
        let source_entries = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
                keys,
            ),
            (
                Arc::new(ArrowField::new("value", value.data_type().clone(), true)),
                Arc::new(value) as ArrayRef,
            ),
        ]);
        let entries_field =
            |entries_type: ArrowDataType| Arc::new(ArrowField::new("entries", entries_type, false));
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                entries_field(source_entries.data_type().clone()),
                OffsetBuffer::new(vec![0i32, 1].into()),
                source_entries,
                None,
                false,
            )
            .unwrap(),
        );

        let target = ArrowDataType::Map(
            entries_field(ArrowDataType::Struct(ArrowFields::from(vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", ArrowDataType::Struct(target_fields), true),
            ]))),
            false,
        );

        let out = realign_array(&map, &target, Some(TEST_FILE), "attrs").unwrap();
        assert_eq!(out.data_type(), &target);
        let out = out.as_any().downcast_ref::<MapArray>().unwrap();
        let value: ArrayRef = Arc::clone(out.entries().column(1));
        assert_eq!(
            first_child_value(&value),
            "B",
            "the target value's first child is id 6, so it must carry beta's value"
        );
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

    /// A column-mapped field: logical `name`, physical name in its metadata.
    fn mapped(name: &str, data_type: DataType, physical: &str) -> Field {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            "delta.columnMapping.physicalName".to_string(),
            physical.to_string(),
        )]))
    }

    /// Relabeling rebuilds each column's `ArrayData` with renamed fields, and a
    /// view array's layout is several data buffers behind a buffer of views
    /// rather than one values buffer behind offsets. Since reads ask for view
    /// types, a column-mapped nested string column arrives here as one, so the
    /// rebuild has to carry that layout through untouched.
    #[test]
    fn relabel_carries_view_arrays_through() {
        let ids: ArrayRef = Arc::new(StringViewArray::from(vec!["t1", "t2"]));
        let after: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("col-id", DataType::Utf8View, true)),
            ids.clone(),
        )]));
        let batch = RecordBatch::try_from_iter(vec![("after", after)]).unwrap();

        let map = nested_physical_to_logical(&Schema::new(vec![mapped(
            "after",
            struct_of(vec![mapped("id", DataType::Utf8View, "col-id")]),
            "col-after",
        )]));
        let relabeled = relabel_nested_columns(&batch, &map).unwrap();

        let DataType::Struct(children) = relabeled.schema().field(0).data_type().clone() else {
            panic!("`after` must stay a struct");
        };
        assert_eq!(children[0].name(), "id");
        assert_eq!(
            children[0].data_type(),
            &DataType::Utf8View,
            "relabeling renames fields, it must not change their types"
        );

        let after = relabeled
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(after.column(0).as_ref(), ids.as_ref());
    }

    /// A `struct<..>` data type from mapped fields.
    fn struct_of(fields: Vec<Field>) -> DataType {
        DataType::Struct(Fields::from(fields))
    }

    /// A `list<element: ..>` data type. The `element` field itself is not column
    /// mapped, matching how Delta stores list elements.
    fn list_of(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("element", element, true)))
    }

    /// The children of a struct-typed field, or panic.
    fn struct_children(field: &Field) -> &Fields {
        match field.data_type() {
            DataType::Struct(children) => children,
            other => panic!("expected struct, got {other:?}"),
        }
    }

    /// The element field of a list-typed field, or panic.
    fn list_element(field: &Field) -> &Field {
        match field.data_type() {
            DataType::List(element) => element,
            other => panic!("expected list, got {other:?}"),
        }
    }

    // The read side: nested struct children must be renamed to physical names,
    // else the Parquet read fails the struct cast.
    #[test]
    fn read_schema_renames_nested_fields() {
        let after = DataType::Struct(Fields::from(vec![
            mapped("id", DataType::Utf8, "col-id"),
            mapped("amount", DataType::Utf8, "col-amount"),
        ]));
        let physical = field_to_physical(&Arc::new(mapped("after", after, "col-after")));

        assert_eq!(physical.name(), "col-after");
        let DataType::Struct(children) = physical.data_type() else {
            panic!("`after` must stay a struct");
        };
        assert_eq!(children[0].name(), "col-id");
        assert_eq!(children[1].name(), "col-amount");
    }

    // The write side: the read batch arrives with logical top-level names but
    // physical nested names; relabeling must restore logical nested names while
    // preserving the data, else nested fields silently read as NULL.
    #[test]
    fn relabel_restores_nested_names_and_preserves_data() {
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["t1", "t2"]));
        let amounts: ArrayRef = Arc::new(StringArray::from(vec!["10", "20"]));
        let after: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("col-id", DataType::Utf8, true)),
                ids.clone(),
            ),
            (
                Arc::new(Field::new("col-amount", DataType::Utf8, true)),
                amounts.clone(),
            ),
        ]));
        let batch = RecordBatch::try_from_iter(vec![("after", after)]).unwrap();

        let map = nested_physical_to_logical(&Schema::new(vec![mapped(
            "after",
            DataType::Struct(Fields::from(vec![
                mapped("id", DataType::Utf8, "col-id"),
                mapped("amount", DataType::Utf8, "col-amount"),
            ])),
            "col-after",
        )]));
        let relabeled = relabel_nested_columns(&batch, &map).unwrap();

        let DataType::Struct(children) = relabeled.schema().field(0).data_type().clone() else {
            panic!("`after` must stay a struct");
        };
        assert_eq!(children[0].name(), "id");
        assert_eq!(children[1].name(), "amount");

        let after = relabeled
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(after.column(0).as_ref(), ids.as_ref());
        assert_eq!(after.column(1).as_ref(), amounts.as_ref());
    }

    // Structs in structs: the rename must reach every level.
    #[test]
    fn read_schema_renames_struct_in_struct() {
        let inner = struct_of(vec![mapped("leaf", DataType::Utf8, "col-leaf")]);
        let outer = struct_of(vec![mapped("inner", inner, "col-inner")]);
        let physical = field_to_physical(&Arc::new(mapped("outer", outer, "col-outer")));

        assert_eq!(physical.name(), "col-outer");
        let inner = &struct_children(&physical)[0];
        assert_eq!(inner.name(), "col-inner");
        assert_eq!(struct_children(inner)[0].name(), "col-leaf");
    }

    // Arrays in structs: the rename descends through the list element.
    #[test]
    fn read_schema_renames_array_in_struct() {
        let outer = struct_of(vec![mapped("items", list_of(DataType::Utf8), "col-items")]);
        let physical = field_to_physical(&Arc::new(mapped("outer", outer, "col-outer")));

        // The list element itself carries no mapping, so it keeps its name; only
        // the struct field wrapping the list is renamed.
        let items = &struct_children(&physical)[0];
        assert_eq!(items.name(), "col-items");
        assert!(matches!(items.data_type(), DataType::List(_)));
    }

    // Structs in arrays: the rename descends into the list element's struct.
    #[test]
    fn read_schema_renames_struct_in_array() {
        let element = struct_of(vec![mapped("id", DataType::Utf8, "col-id")]);
        let physical = field_to_physical(&Arc::new(mapped("items", list_of(element), "col-items")));

        assert_eq!(physical.name(), "col-items");
        let element = list_element(&physical);
        assert_eq!(struct_children(element)[0].name(), "col-id");
    }

    // Structs of structs of arrays of structs: the deepest leaf must be renamed.
    #[test]
    fn read_schema_renames_struct_of_struct_of_array_of_struct() {
        let leaf = struct_of(vec![mapped("amount", DataType::Utf8, "col-amount")]);
        let mid = struct_of(vec![mapped("rows", list_of(leaf), "col-rows")]);
        let outer = struct_of(vec![mapped("mid", mid, "col-mid")]);
        let physical = field_to_physical(&Arc::new(mapped("outer", outer, "col-outer")));

        let mid = &struct_children(&physical)[0];
        assert_eq!(mid.name(), "col-mid");
        let rows = &struct_children(mid)[0];
        assert_eq!(rows.name(), "col-rows");
        let leaf = list_element(rows);
        assert_eq!(struct_children(leaf)[0].name(), "col-amount");
    }

    // Structs in arrays of structs, end to end: relabeling must restore logical
    // names inside the list element and preserve the leaf data.
    #[test]
    fn relabel_restores_names_inside_array_of_structs() {
        // A list<struct<col-id: utf8>> with two lists over three elements.
        let ids: ArrayRef = Arc::new(StringArray::from(vec!["t1", "t2", "t3"]));
        let element_values: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("col-id", DataType::Utf8, true)),
            ids.clone(),
        )]));
        let element_field = Arc::new(Field::new(
            "element",
            element_values.data_type().clone(),
            true,
        ));
        let items: ArrayRef = Arc::new(ListArray::new(
            element_field,
            OffsetBuffer::new(vec![0, 2, 3].into()),
            element_values,
            None,
        ));
        let batch = RecordBatch::try_from_iter(vec![("items", items)]).unwrap();

        let map = nested_physical_to_logical(&Schema::new(vec![mapped(
            "items",
            list_of(struct_of(vec![mapped("id", DataType::Utf8, "col-id")])),
            "col-items",
        )]));
        let relabeled = relabel_nested_columns(&batch, &map).unwrap();

        let schema = relabeled.schema();
        let element = list_element(schema.field(0));
        assert_eq!(struct_children(element)[0].name(), "id");

        // The leaf data survives the relabel unchanged.
        let list = relabeled
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let element = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(element.column(0).as_ref(), ids.as_ref());
    }
}
