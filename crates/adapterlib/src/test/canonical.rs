//! One form for rows that SQL holds equal: the oracle compares rows in this form.

use std::sync::Arc;

use anyhow::Result as AnyResult;
use arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, Float64Array, LargeListArray, ListArray, MapArray,
    RecordBatch, StructArray, UInt32Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{sort_to_indices, take};
use arrow::datatypes::{DataType, Float32Type, Float64Type};

/// Returns `batch` with each column in its canonical form, as [`canonicalize`] makes it.
pub(crate) fn canonicalize_batch(batch: &RecordBatch) -> AnyResult<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(canonicalize)
        .collect::<AnyResult<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Returns `column` in its canonical form, at any depth of structs, lists, and maps: every NaN
/// is the canonical NaN, every negative zero is zero, and the entries of each map are in key
/// order, since a SQL map has no order.
pub(crate) fn canonicalize(column: &ArrayRef) -> AnyResult<ArrayRef> {
    Ok(match column.data_type() {
        DataType::Float64 => {
            let floats: Float64Array = column
                .as_primitive::<Float64Type>()
                .iter()
                .map(|value| value.map(canonical_f64))
                .collect();
            Arc::new(floats)
        }
        DataType::Float32 => {
            let floats: Float32Array = column
                .as_primitive::<Float32Type>()
                .iter()
                .map(|value| value.map(canonical_f32))
                .collect();
            Arc::new(floats)
        }
        DataType::Struct(_) => {
            let rows = column.as_struct();
            let children = rows
                .columns()
                .iter()
                .map(canonicalize)
                .collect::<AnyResult<_>>()?;
            Arc::new(StructArray::new(
                rows.fields().clone(),
                children,
                rows.nulls().cloned(),
            ))
        }
        DataType::List(field) => {
            let lists = column.as_list::<i32>();
            Arc::new(ListArray::new(
                field.clone(),
                lists.offsets().clone(),
                canonicalize(lists.values())?,
                lists.nulls().cloned(),
            ))
        }
        DataType::LargeList(field) => {
            let lists = column.as_list::<i64>();
            Arc::new(LargeListArray::new(
                field.clone(),
                lists.offsets().clone(),
                canonicalize(lists.values())?,
                lists.nulls().cloned(),
            ))
        }
        DataType::Map(field, sorted) => {
            let maps = column.as_map();
            let keys = canonicalize(maps.keys())?;
            let values = canonicalize(maps.values())?;
            let offsets = maps.offsets();
            let mut order = Vec::with_capacity(keys.len());
            for map in 0..maps.len() {
                let start = offsets[map] as usize;
                let end = offsets[map + 1] as usize;
                let indices = sort_to_indices(&keys.slice(start, end - start), None, None)?;
                order.extend(indices.values().iter().map(|index| start as u32 + index));
            }
            let order = UInt32Array::from(order);
            let entries = StructArray::new(
                maps.entries().fields().clone(),
                vec![take(&keys, &order, None)?, take(&values, &order, None)?],
                None,
            );
            // The entries hold only the maps' own entries, which a slice of a larger array
            // finds past the start of its child, so the offsets count from the first one.
            let first = offsets[0];
            let offsets = OffsetBuffer::new(offsets.iter().map(|offset| offset - first).collect());
            Arc::new(MapArray::new(
                field.clone(),
                offsets,
                entries,
                maps.nulls().cloned(),
                *sorted,
            ))
        }
        _ => column.clone(),
    })
}

fn canonical_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}

fn canonical_f32(value: f32) -> f32 {
    if value.is_nan() {
        f32::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, AsArray, Int64Builder, MapBuilder, StringBuilder};
    use arrow::datatypes::Int64Type;

    use super::canonicalize;

    /// The entries of each map come out in key order, and the maps keep their boundaries and
    /// their NULLs.
    #[test]
    fn map_entries_are_in_key_order() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        for (key, value) in [("foo", 1), ("bar", 2), ("baz", 3)] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_value("b");
        builder.values().append_value(4);
        builder.keys().append_value("a");
        builder.values().append_value(5);
        builder.append(true).unwrap();
        let maps: ArrayRef = Arc::new(builder.finish());

        let canonical = canonicalize(&maps).unwrap();
        let canonical = canonical.as_map();
        assert_eq!(canonical.len(), 3);
        assert!(canonical.is_null(1));
        let entries = |map: usize| -> Vec<(String, i64)> {
            let entries = canonical.value(map);
            let keys = entries.column(0).as_string::<i32>();
            let values = entries.column(1).as_primitive::<Int64Type>();
            (0..entries.len())
                .map(|entry| (keys.value(entry).to_string(), values.value(entry)))
                .collect()
        };
        assert_eq!(
            entries(0),
            [
                ("bar".to_string(), 2),
                ("baz".to_string(), 3),
                ("foo".to_string(), 1)
            ]
        );
        assert_eq!(entries(2), [("a".to_string(), 5), ("b".to_string(), 4)]);

        // A slice whose maps start past the first entry of the child.
        let canonical = canonicalize(&maps.slice(2, 1)).unwrap();
        let canonical = canonical.as_map();
        assert_eq!(canonical.len(), 1);
        let entries = canonical.value(0);
        let keys = entries.column(0).as_string::<i32>();
        assert_eq!(keys.iter().flatten().collect::<Vec<_>>(), ["a", "b"]);
    }
}
