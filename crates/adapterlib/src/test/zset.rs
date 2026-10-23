//! An Arrow-only Z-set: the oracle that the shared connector suites check results against.
//!
//! A [`ZSet`] is a multiset of rows with `i64` weights over one Arrow schema. A row is
//! identified by its [`arrow::row`] encoding in the canonical form (one NaN, one zero, map
//! entries in key order), so two Z-sets are equal when they hold the same rows with the same
//! weights, whatever the order and the batching of the records that built them.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use anyhow::{Result as AnyResult, bail};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow::util::display::{ArrayFormatter, FormatOptions};

use super::canonical::canonicalize;

/// A consolidated multiset of rows: each distinct row with the sum of its weights. A row
/// whose weights sum to zero is not in the set.
#[derive(Clone)]
pub struct ZSet {
    schema: SchemaRef,
    converter: Arc<RowConverter>,
    rows: BTreeMap<OwnedRow, i64>,
}

impl ZSet {
    /// Returns the empty Z-set over `schema`.
    ///
    /// Fails if Arrow cannot encode a column type of `schema` as a row.
    pub fn empty(schema: SchemaRef) -> AnyResult<Self> {
        let fields = schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect();
        Ok(Self {
            converter: Arc::new(RowConverter::new(fields)?),
            schema,
            rows: BTreeMap::new(),
        })
    }

    /// Returns the rows of `batch`, each with the weight at its index in `weights`.
    pub fn from_batch(batch: &RecordBatch, weights: &[i64]) -> AnyResult<Self> {
        let mut zset = Self::empty(batch.schema())?;
        zset.insert(batch, weights)?;
        Ok(zset)
    }

    /// Returns the rows of `batch` without its column `weight`, each with the weight that
    /// the column holds for it.
    ///
    /// Fails if `batch` has no such column, or if the column is not a non-null `Int64`.
    pub fn from_weighted_batch(batch: &RecordBatch, weight: &str) -> AnyResult<Self> {
        let index = batch.schema().index_of(weight)?;
        let Some(weights) = batch
            .column(index)
            .as_primitive_opt::<arrow::datatypes::Int64Type>()
        else {
            bail!("the weight column '{weight}' is not an Int64 column");
        };
        if weights.null_count() > 0 {
            bail!("the weight column '{weight}' holds a null");
        }
        let weights = weights.values().to_vec();
        let mut rows = batch.clone();
        rows.remove_column(index);
        Self::from_batch(&rows, &weights)
    }

    /// Adds the rows of `batch`, each with the weight at its index in `weights`.
    ///
    /// Fails if the columns of `batch` differ in name or type from the schema of `self`, or
    /// if `weights` has another length than `batch`.
    pub fn insert(&mut self, batch: &RecordBatch, weights: &[i64]) -> AnyResult<()> {
        let ours = self.schema.fields();
        let theirs = batch.schema_ref().fields();
        if ours.len() != theirs.len()
            || ours
                .iter()
                .zip(theirs.iter())
                .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
        {
            bail!("the batch has the columns {theirs:?}, but the Z-set has the columns {ours:?}");
        }
        if batch.num_rows() != weights.len() {
            bail!(
                "the batch has {} rows, but {} weights",
                batch.num_rows(),
                weights.len()
            );
        }
        let columns = batch
            .columns()
            .iter()
            .map(canonicalize)
            .collect::<AnyResult<Vec<ArrayRef>>>()?;
        let rows = self.converter.convert_columns(&columns)?;
        for (row, weight) in rows.iter().zip(weights) {
            self.add_row(row.owned(), *weight);
        }
        Ok(())
    }

    /// Adds every row of `other`, with its weight.
    ///
    /// Fails if the columns of `other` differ in name or type from the schema of `self`.
    pub fn add(&mut self, other: &ZSet) -> AnyResult<()> {
        if Arc::ptr_eq(&self.converter, &other.converter) {
            for (row, weight) in &other.rows {
                self.add_row(row.clone(), *weight);
            }
            return Ok(());
        }
        // A converter decodes only the rows that it encoded, so the rows are encoded again.
        let (batch, weights) = other.to_batch()?;
        self.insert(&batch, &weights)
    }

    /// Returns `self` with every weight negated.
    pub fn negate(&self) -> ZSet {
        ZSet {
            schema: self.schema.clone(),
            converter: self.converter.clone(),
            rows: self
                .rows
                .iter()
                .map(|(row, weight)| (row.clone(), -weight))
                .collect(),
        }
    }

    /// Returns the schema of the rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns the number of distinct rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if the Z-set holds no row.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the distinct rows, in the order of their encoding, and their weights.
    pub fn to_batch(&self) -> AnyResult<(RecordBatch, Vec<i64>)> {
        let columns = self
            .converter
            .convert_rows(self.rows.keys().map(|row| row.row()))?;
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok((batch, self.rows.values().copied().collect()))
    }

    fn add_row(&mut self, row: OwnedRow, weight: i64) {
        match self.rows.entry(row) {
            Entry::Vacant(entry) => {
                if weight != 0 {
                    entry.insert(weight);
                }
            }
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += weight;
                if *entry.get() == 0 {
                    entry.remove();
                }
            }
        }
    }
}

impl PartialEq for ZSet {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

impl Eq for ZSet {}

/// One line per row: the weight, then the values, as SQL tests read them.
impl Debug for ZSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let (batch, weights) = self.to_batch().map_err(|_| fmt::Error)?;
        write_weighted_rows(f, "ZSet", &batch, &weights)
    }
}

/// Writes `title`, the column names of `rows`, and then one line per row: its weight, then
/// its values.
pub(super) fn write_weighted_rows(
    f: &mut Formatter<'_>,
    title: &str,
    rows: &RecordBatch,
    weights: &[i64],
) -> fmt::Result {
    let options = FormatOptions::default().with_null("NULL");
    let formatters = rows
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| fmt::Error)?;
    let names: Vec<&str> = rows
        .schema_ref()
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    writeln!(f, "{title} [weight] {}", names.join(", "))?;
    for (index, weight) in weights.iter().enumerate() {
        write!(f, "  {weight:+}")?;
        for formatter in &formatters {
            write!(f, " {}", formatter.value(index))?;
        }
        writeln!(f)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
        StructArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};

    use super::ZSet;

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    /// `rows` with a nullable `weight` column holding `weights`.
    fn weighted(rows: &RecordBatch, weights: Int64Array) -> RecordBatch {
        let mut fields = rows.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("weight", DataType::Int64, true)));
        let mut columns = rows.columns().to_vec();
        columns.push(Arc::new(weights));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    /// Equal multisets are equal whatever the order and batching of their rows; weights
    /// that cancel remove a row, and a row of weight zero is never in the set.
    #[test]
    fn rows_consolidate_by_value() {
        let mut first = ZSet::from_batch(&batch(&[1, 2], &["a", "b"]), &[1, 1]).unwrap();
        first
            .insert(&batch(&[2, 3], &["b", "c"]), &[1, -1])
            .unwrap();
        let rows = batch(&[3, 2, 1], &["c", "b", "a"]);
        let second = ZSet::from_batch(&rows, &[-1, 2, 1]).unwrap();
        assert_eq!(first, second);
        assert_ne!(first, ZSet::from_batch(&rows, &[-1, 1, 1]).unwrap());

        first.add(&second.negate()).unwrap();
        assert!(first.is_empty(), "{first:?}");
        assert!(
            ZSet::from_batch(&batch(&[1], &["a"]), &[0])
                .unwrap()
                .is_empty()
        );
    }

    /// The weight column of a weighted batch becomes the weights, and a null weight is
    /// refused.
    #[test]
    fn a_weight_column_holds_the_weights() {
        let rows = batch(&[1, 2], &["a", "b"]);
        assert_eq!(
            ZSet::from_weighted_batch(&weighted(&rows, Int64Array::from(vec![3, -1])), "weight")
                .unwrap(),
            ZSet::from_batch(&rows, &[3, -1]).unwrap()
        );
        let null_weight = weighted(&rows, Int64Array::from(vec![Some(1), None]));
        assert!(ZSet::from_weighted_batch(&null_weight, "weight").is_err());
    }

    /// A NaN with any payload and both zeros are each one value, in either float width and
    /// inside a struct.
    #[test]
    fn floats_compare_as_sql_values() {
        let floats = |wide: Vec<f64>, narrow: Vec<f32>| {
            let column: ArrayRef = Arc::new(Float64Array::from(wide));
            let narrow: ArrayRef = Arc::new(Float32Array::from(narrow));
            let nested = StructArray::from(vec![(
                Arc::new(Field::new("x", DataType::Float64, false)),
                column.clone(),
            )]);
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Float64, false),
                    Field::new("y", DataType::Float32, false),
                    Field::new("s", nested.data_type().clone(), false),
                ])),
                vec![column, narrow, Arc::new(nested)],
            )
            .unwrap()
        };
        let wide_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let narrow_nan = f32::from_bits(f32::NAN.to_bits() | 1);
        assert_eq!(
            ZSet::from_batch(
                &floats(vec![-0.0, wide_nan], vec![-0.0, narrow_nan]),
                &[1, 1]
            )
            .unwrap(),
            ZSet::from_batch(&floats(vec![0.0, f64::NAN], vec![0.0, f32::NAN]), &[1, 1]).unwrap()
        );
    }

    /// A batch whose columns differ from the Z-set's in name or type, or whose weights do
    /// not match its rows, is refused.
    #[test]
    fn mismatched_input_is_refused() {
        let mut zset = ZSet::from_batch(&batch(&[1], &["a"]), &[1]).unwrap();
        let renamed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("title", DataType::Utf8, false),
            ])),
            batch(&[1], &["a"]).columns().to_vec(),
        )
        .unwrap();
        assert!(zset.insert(&renamed, &[1]).is_err());
        let ids = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(zset.insert(&ids, &[1]).is_err());
        assert!(zset.insert(&batch(&[1], &["a"]), &[1, 1]).is_err());
    }

    /// The rows that one Z-set adds to another read back, and columns that differ are
    /// refused.
    #[test]
    fn added_rows_read_back() {
        let mut sum = ZSet::from_batch(&batch(&[1], &["a"]), &[1]).unwrap();
        sum.add(&ZSet::from_batch(&batch(&[1, 2], &["a", "b"]), &[1, -1]).unwrap())
            .unwrap();
        assert_eq!(
            format!("{sum:?}"),
            "ZSet [weight] id, name\n  +2 1 a\n  -1 2 b\n"
        );

        let ids = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(sum.add(&ZSet::from_batch(&ids, &[1]).unwrap()).is_err());
    }

    /// A row reads back as a line with its weight and values.
    #[test]
    fn debug_lists_rows_with_weights() {
        let zset = ZSet::from_batch(&batch(&[1, 2], &["a", "b"]), &[2, -1]).unwrap();
        assert_eq!(
            format!("{zset:?}"),
            "ZSet [weight] id, name\n  +2 1 a\n  -1 2 b\n"
        );
    }
}
