//! Updates in arrival order: the rows that a connector flushed, before any consolidation.

use std::fmt::{self, Debug, Formatter};

use anyhow::{Result as AnyResult, bail};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{Int64Type, SchemaRef};
use arrow::json::ReaderBuilder;

use super::canonical::canonicalize_batch;
use super::zset::{ZSet, write_weighted_rows};

/// The name of the column that holds the weight of each row in a weighted batch.
pub const WEIGHT_COLUMN: &str = "weight";

/// Rows with weights, in the order a connector flushed them: inserts and deletes that the
/// table has not consolidated yet. The rows are in the canonical form of the Z-set oracle, so
/// map entries are in key order.
#[derive(Clone)]
pub struct Updates {
    rows: RecordBatch,
    weights: Vec<i64>,
}

impl Updates {
    /// Returns `rows`, each with the weight at its index in `weights`.
    ///
    /// Fails if there is not one weight per row.
    pub fn new(rows: RecordBatch, weights: Vec<i64>) -> AnyResult<Self> {
        if rows.num_rows() != weights.len() {
            bail!("{} rows, but {} weights", rows.num_rows(), weights.len());
        }
        Ok(Self {
            rows: canonicalize_batch(&rows)?,
            weights,
        })
    }

    /// Returns the rows of `batches`, in order, in `schema`.
    ///
    /// Fails if a batch has other columns than `schema`.
    pub fn concat(schema: SchemaRef, batches: &[Updates]) -> AnyResult<Self> {
        let rows: Vec<RecordBatch> = batches.iter().map(|batch| batch.rows.clone()).collect();
        let weights = batches
            .iter()
            .flat_map(|batch| batch.weights.iter().copied())
            .collect();
        Self::new(concat_batches(&schema, &rows)?, weights)
    }

    /// Returns the rows at `indices`, in that order, each with the weight at its position in
    /// `weights`.
    ///
    /// Fails if an index is past the last row, or if there is not one weight per index.
    pub fn take(&self, indices: &[u32], weights: Vec<i64>) -> AnyResult<Self> {
        let indices = UInt32Array::from(indices.to_vec());
        let rows = take_record_batch(&self.rows, &indices)?;
        Self::new(rows, weights)
    }

    /// Returns rows of `schema`, each written as one JSON object, with its weight.
    ///
    /// A column that an object omits is NULL. Fails if an object names a column that `schema`
    /// does not have, or holds a value that does not fit its column.
    pub fn from_json(schema: SchemaRef, rows: &[(i64, &str)]) -> AnyResult<Self> {
        let mut batches = Vec::with_capacity(rows.len());
        for (_, row) in rows {
            let reader = ReaderBuilder::new(schema.clone())
                .with_strict_mode(true)
                .build(row.as_bytes())?;
            for batch in reader {
                batches.push(batch?);
            }
        }
        let batch = concat_batches(&schema, &batches)?;
        if batch.num_rows() != rows.len() {
            bail!(
                "{} JSON strings hold {} rows; write one object per row",
                rows.len(),
                batch.num_rows()
            );
        }
        Self::new(batch, rows.iter().map(|(weight, _)| *weight).collect())
    }

    /// Returns the rows of `batches`, in order, without their [`WEIGHT_COLUMN`], each with
    /// the weight that the column holds for it.
    ///
    /// Fails if a batch has no weight column of non-null `Int64` values, or if its other
    /// columns differ in name or type from those of `schema`.
    pub fn from_weighted_batches(schema: SchemaRef, batches: &[RecordBatch]) -> AnyResult<Self> {
        let mut rows = Vec::with_capacity(batches.len());
        let mut weights = Vec::new();
        for batch in batches {
            let index = batch.schema_ref().index_of(WEIGHT_COLUMN)?;
            let Some(column) = batch.column(index).as_primitive_opt::<Int64Type>() else {
                bail!("the weight column '{WEIGHT_COLUMN}' is not an Int64 column");
            };
            if column.null_count() > 0 {
                bail!("the weight column '{WEIGHT_COLUMN}' holds a null");
            }
            weights.extend_from_slice(column.values());

            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
            columns.remove(index);
            let mut fields = batch.schema_ref().fields().to_vec();
            fields.remove(index);
            if fields.len() != schema.fields().len()
                || fields
                    .iter()
                    .zip(schema.fields().iter())
                    .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
            {
                bail!(
                    "the batch has the columns {fields:?}, but the rows have the columns {:?}",
                    schema.fields()
                );
            }
            rows.push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        Self::new(concat_batches(&schema, &rows)?, weights)
    }

    /// Returns the rows, in order.
    pub fn rows(&self) -> &RecordBatch {
        &self.rows
    }

    /// Returns the weight of each row, in order.
    pub fn weights(&self) -> &[i64] {
        &self.weights
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.weights.len()
    }

    /// Returns `true` if there is no row.
    pub fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }

    /// Returns the rows consolidated into a Z-set.
    pub fn zset(&self) -> AnyResult<ZSet> {
        ZSet::from_batch(&self.rows, &self.weights)
    }
}

/// Equal when the rows and their weights are equal, in the same order. Column names and
/// types must match; nullability and field metadata do not matter.
impl PartialEq for Updates {
    fn eq(&self, other: &Self) -> bool {
        let ours = self.rows.schema_ref().fields();
        let theirs = other.rows.schema_ref().fields();
        self.weights == other.weights
            && ours.len() == theirs.len()
            && ours
                .iter()
                .zip(theirs.iter())
                .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
            && self
                .rows
                .columns()
                .iter()
                .zip(other.rows.columns())
                .all(|(a, b)| a.to_data() == b.to_data())
    }
}

/// One line per row, in order: the weight, then the values.
impl Debug for Updates {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_weighted_rows(f, "Updates", &self.rows, &self.weights)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::{Updates, WEIGHT_COLUMN};
    use crate::test::ZSet;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// `ids` and `names` with a weight column of `weights`.
    fn weighted(ids: &[i64], names: &[Option<&str>], weights: &[i64]) -> RecordBatch {
        let mut fields = schema().fields().to_vec();
        fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(weights.to_vec())),
            ],
        )
        .unwrap()
    }

    /// JSON objects read as the rows of the flushed batches, in order and with their
    /// weights; an omitted nullable column is NULL.
    #[test]
    fn json_rows_match_flushed_batches() {
        let flushed = Updates::from_weighted_batches(
            schema(),
            &[
                weighted(&[1], &[Some("a")], &[1]),
                weighted(&[2, 1], &[None, Some("a")], &[1, -1]),
            ],
        )
        .unwrap();
        let expected = Updates::from_json(
            schema(),
            &[
                (1, r#"{"id": 1, "name": "a"}"#),
                (1, r#"{"id": 2}"#),
                (-1, r#"{"id": 1, "name": "a"}"#),
            ],
        )
        .unwrap();
        assert_eq!(flushed, expected);
        assert_eq!(
            flushed.zset().unwrap(),
            ZSet::from_batch(&expected.rows().slice(1, 1), &[1]).unwrap()
        );
    }

    /// Order and weights matter, unlike in a Z-set.
    #[test]
    fn order_and_weights_matter() {
        let rows = |weights: [i64; 2], ids: [&str; 2]| {
            Updates::from_json(schema(), &[(weights[0], ids[0]), (weights[1], ids[1])]).unwrap()
        };
        let one = r#"{"id": 1}"#;
        let two = r#"{"id": 2}"#;
        assert_ne!(rows([1, 1], [one, two]), rows([1, 1], [two, one]));
        assert_ne!(rows([1, 1], [one, two]), rows([1, 2], [one, two]));
        assert_eq!(
            format!("{:?}", rows([1, -1], [one, two])),
            "Updates [weight] id, name\n  +1 1 NULL\n  -1 2 NULL\n"
        );
    }

    /// A misspelled column, a value of the wrong type, or two objects in one string is
    /// refused.
    #[test]
    fn malformed_json_is_refused() {
        assert!(Updates::from_json(schema(), &[(1, r#"{"id": 1, "nmae": "a"}"#)]).is_err());
        assert!(Updates::from_json(schema(), &[(1, r#"{"id": "one"}"#)]).is_err());
        assert!(Updates::from_json(schema(), &[(1, r#"{"id": 1} {"id": 2}"#)]).is_err());
    }

    /// A weighted batch without a weight column, or with other columns, is refused.
    #[test]
    fn mismatched_batches_are_refused() {
        let rows = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .unwrap();
        assert!(Updates::from_weighted_batches(schema(), &[rows]).is_err());
        let narrower = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert!(
            Updates::from_weighted_batches(narrower, &[weighted(&[1], &[None], &[1])]).is_err()
        );
    }
}
