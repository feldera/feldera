//! Deciding which files and row groups the lookup has to read.
//!
//! The lookup's cost is the key columns of everything it opens, so pruning is what keeps a
//! flush proportional to the change rather than to the table. Two facts make it cheap:
//!
//! - The keys being sought are known exactly, as a sorted [`LookupChunk`], so a unit can be
//!   tested against the key set itself rather than against a summary of it.
//! - The row encoding orders bytes the way it orders values, so a unit's per-column
//!   `[min, max]` box maps to one lexicographic interval `[min_tuple, max_tuple]` that
//!   contains the box. Testing "does the chunk meet this interval" is then one binary
//!   search, whatever the key's arity.
//!
//! Partition columns get an exact test of their own, [`PartitionFilter`], because the box
//! test wastes them: a wide range on a leading column swallows what a trailing partition
//! column says. Since the partition columns are key columns, the flush knows every partition
//! its keys belong to, so a file outside that set holds nothing it wants. On a table
//! partitioned by date, a flush touching one day skips every other day unread.
//!
//! # Pruning is sound in one direction only
//!
//! Failing to prune costs a read. Pruning a unit that holds a key we must supersede skips
//! the tombstone and leaves two live rows for one key -- silent corruption. So every
//! uncertainty resolves to "keep":
//!
//! | Situation | Behavior |
//! |-----------|----------|
//! | A key column has no statistic on this unit | Keep. A missing statistic is not an empty range |
//! | A statistic is null, or a `ROW` statistic has a null leaf | Keep. Delta records no statistic for a `BINARY` leaf, and `delta.dataSkippingNumIndexedCols` can cut through a struct |
//! | The encoder rejects a statistic's type | Keep |
//! | A key column is `FLOAT` or `DOUBLE` | Statistics pruning is off for the whole key: Parquet and Delta conventionally leave NaN out of min/max, so a box test could exclude a NaN key that is present |
//! | A key being sought is null | Pruning is off for that lookup pass: nulls are left out of min/max too, so a null key falls below every range |
//!
//! String statistics are truncated by the writer -- minimum down, maximum up -- so the box
//! stays a superset of the values, which is what soundness needs.

use std::collections::{HashMap, HashSet};

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::row::{RowConverter, SortField};
use delta_kernel::expressions::Scalar;

use super::chunk::LookupChunk;
use super::key::{KeyEncoder, contains_null, normalize_floats};

/// Per-key-column `[min, max]` statistics for one file or row group.
///
/// One-element arrays, in key declaration order. `None` for a column whose statistic is
/// missing, which makes the whole unit unprunable.
pub struct KeyStats {
    pub mins: Vec<Option<ArrayRef>>,
    pub maxes: Vec<Option<ArrayRef>>,
}

impl KeyStats {
    pub fn with_capacity(columns: usize) -> Self {
        Self {
            mins: Vec::with_capacity(columns),
            maxes: Vec::with_capacity(columns),
        }
    }

    pub fn push(&mut self, min: Option<ArrayRef>, max: Option<ArrayRef>) {
        self.mins.push(min);
        self.maxes.push(max);
    }

    /// Both bounds present and non-null for every key column.
    ///
    /// Uses the same recursion as `contains_null`, because a `ROW` bound with a null leaf is
    /// as unusable as a null bound: encoded, it sorts outside the box it is meant to bound.
    fn is_complete(&self) -> bool {
        self.mins
            .iter()
            .chain(self.maxes.iter())
            .all(|bound| match bound {
                Some(array) => array.len() == 1 && !contains_null(std::slice::from_ref(array)),
                None => false,
            })
    }
}

/// The partitions a chunk's keys belong to.
///
/// Exact, not an approximation: when partition columns are key columns the partition is a
/// function of the key, so a file whose partition is absent holds no key in the chunk. Sized
/// by the number of distinct partitions a flush touches, not by its key count.
pub struct PartitionFilter {
    /// Encoded partition tuples, in key declaration order of the partition columns.
    seen: HashSet<Vec<u8>>,
    converter: RowConverter,
    /// Indices of the partition columns within the key's columns.
    columns: Vec<usize>,
}

impl PartitionFilter {
    /// Build a filter over the key columns named in `partition_key_columns`. Returns `None`
    /// when no key column is a partition column, since there is then nothing to filter on.
    pub fn new(
        key_columns: &[String],
        key_fields: &[ArrowField],
        partition_key_columns: &[String],
    ) -> AnyResult<Option<Self>> {
        if partition_key_columns.is_empty() {
            return Ok(None);
        }

        let mut sort_fields = Vec::new();
        let mut columns = Vec::new();
        for name in partition_key_columns {
            let index = key_columns
                .iter()
                .position(|c| c == name)
                .ok_or_else(|| anyhow!("partition key column '{name}' is not a key column"))?;
            sort_fields.push(SortField::new(key_fields[index].data_type().clone()));
            columns.push(index);
        }

        Ok(Some(Self {
            seen: HashSet::new(),
            converter: RowConverter::new(sort_fields)
                .map_err(|e| anyhow!("unable to build the partition filter encoder: {e}"))?,
            columns,
        }))
    }

    /// Record the partitions a batch of keys belongs to, given its columns in declaration order.
    pub fn record(&mut self, key_columns: &[ArrayRef]) -> AnyResult<()> {
        // Normalized like the key encoder, so a float partition matches either sign of zero.
        let projected: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|i| normalize_floats(&key_columns[*i]))
            .collect();
        let rows = self
            .converter
            .convert_columns(&projected)
            .map_err(|e| anyhow!("unable to encode partition values of a key: {e}"))?;
        for i in 0..rows.num_rows() {
            self.seen.insert(rows.row(i).as_ref().to_vec());
        }
        Ok(())
    }

    /// Whether a file in `partition` can hold any key recorded so far. An unknown partition
    /// value yields `true`: unknown is not empty.
    pub fn may_contain(&self, partition: &HashMap<String, Scalar>, names: &[String]) -> bool {
        let mut columns = Vec::with_capacity(self.columns.len());
        for index in &self.columns {
            let Some(value) = partition.get(&names[*index]) else {
                return true;
            };
            match value.to_array(1) {
                Ok(array) if array.null_count() == 0 => columns.push(normalize_floats(&array)),
                // Null is a legitimate partition, but not one this encoder is trusted to
                // render identically, so keep the file.
                _ => return true,
            }
        }
        match self.converter.convert_columns(&columns) {
            Ok(rows) => self.seen.contains(rows.row(0).as_ref()),
            Err(_) => true,
        }
    }

    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

/// Whether min/max statistics can be trusted to bound this key (see the NaN row in the module
/// docs). A property of the key's types, so it is checked at startup, not per flush.
pub fn stats_pruning_sound(key_fields: &[ArrowField]) -> bool {
    !key_fields
        .iter()
        .any(|field| contains_float(field.data_type()))
}

fn contains_float(data_type: &ArrowDataType) -> bool {
    match data_type {
        ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => true,
        ArrowDataType::Struct(fields) => fields.iter().any(|f| contains_float(f.data_type())),
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::ListView(f)
        | ArrowDataType::LargeListView(f)
        | ArrowDataType::FixedSizeList(f, _) => contains_float(f.data_type()),
        _ => false,
    }
}

/// Whether `chunk` can hold a key inside `stats`.
///
/// `false` only when the chunk provably meets none of the unit's key range; anything
/// uncertain returns `true`. The chunk must be sorted.
pub fn may_contain(chunk: &LookupChunk, encoder: &KeyEncoder, stats: &KeyStats) -> bool {
    if chunk.is_empty() {
        return false;
    }
    if chunk.has_null_key() {
        // Statistics leave nulls out of min/max, so no range rules a null key out.
        return true;
    }
    if !stats.is_complete() {
        return true;
    }

    let columns: Vec<ArrayRef> = stats.mins.iter().flatten().cloned().collect();
    let Ok(min) = encoder.encode_columns(&columns) else {
        return true;
    };
    let columns: Vec<ArrayRef> = stats.maxes.iter().flatten().cloned().collect();
    let Ok(max) = encoder.encode_columns(&columns) else {
        return true;
    };

    // The box is contained in `[min_tuple, max_tuple]` under lexicographic order, which is
    // the order the encoded bytes carry, so one interval test covers every key column.
    chunk.intersects(min.row(0).as_ref(), max.row(0).as_ref())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::integrated::delta_table::merge::test::{arrow_schema, key_relation};
    use arrow::array::{BinaryArray, Float64Array, Int64Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
    use arrow::row::{RowConverter, Rows, SortField};
    use feldera_types::program_schema::{
        ColumnType, Field as SqlField, Relation, SqlIdentifier, SqlType,
    };
    use std::sync::Arc;

    fn encoder() -> KeyEncoder {
        KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap()
    }

    fn chunk_of(values: &[i64]) -> LookupChunk {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        let rows: Rows = converter.convert_columns(&[column]).unwrap();
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&rows).unwrap();
        chunk.sort();
        chunk
    }

    /// Encoder for a single `ROW` key column named `r` with the given leaves.
    fn row_key_encoder(leaves: &Fields) -> KeyEncoder {
        let schema = ArrowSchema::new(vec![Field::new(
            "r",
            DataType::Struct(leaves.clone()),
            true,
        )]);
        let column = |typ| ColumnType {
            typ,
            nullable: true,
            precision: None,
            scale: None,
            component: None,
            fields: None,
            key: None,
            value: None,
        };
        let mut row = column(SqlType::Struct);
        row.fields = Some(vec![
            SqlField::new("a".into(), column(SqlType::BigInt)),
            SqlField::new("b".into(), column(SqlType::Binary)),
        ]);
        let relation = Relation {
            name: SqlIdentifier::new("k", false),
            fields: vec![SqlField::new("r".into(), row)],
            materialized: false,
            properties: Default::default(),
            primary_key: None,
        };
        KeyEncoder::new(&relation, &schema).unwrap()
    }

    /// One `ROW(a, b)` value. `b` is `None` where the statistic is absent.
    fn row_key(leaves: &Fields, a: i64, b: Option<&[u8]>) -> ArrayRef {
        Arc::new(StructArray::new(
            leaves.clone(),
            vec![
                Arc::new(Int64Array::from(vec![a])),
                Arc::new(BinaryArray::from(vec![b])),
            ],
            None,
        ))
    }

    fn bounds(min: i64, max: i64) -> KeyStats {
        let mut stats = KeyStats::with_capacity(1);
        stats.push(
            Some(Arc::new(Int64Array::from(vec![min]))),
            Some(Arc::new(Int64Array::from(vec![max]))),
        );
        stats
    }

    #[test]
    fn prunes_a_disjoint_range_and_keeps_an_overlapping_one() {
        let chunk = chunk_of(&[10, 20, 30]);
        let encoder = encoder();

        assert!(!may_contain(&chunk, &encoder, &bounds(40, 50)));
        assert!(!may_contain(&chunk, &encoder, &bounds(0, 9)));
        assert!(may_contain(&chunk, &encoder, &bounds(15, 25)));
        assert!(may_contain(&chunk, &encoder, &bounds(30, 30)));
        // Keys clustered at the ends: the exact test prunes the gap; an interval
        // approximation of the chunk would not.
        let clustered = chunk_of(&[1, 2, 100, 101]);
        assert!(!may_contain(&clustered, &encoder, &bounds(40, 60)));
    }

    /// A null key falls below every recorded range, so its file must be kept.
    #[test]
    fn a_null_key_disables_pruning() {
        let mut chunk = chunk_of(&[10]);
        let encoder = encoder();
        assert!(!may_contain(&chunk, &encoder, &bounds(40, 50)));

        chunk.note_null_key();
        assert!(
            may_contain(&chunk, &encoder, &bounds(40, 50)),
            "a disjoint range must not prune a chunk holding a null key"
        );
    }

    #[test]
    fn a_missing_or_null_statistic_keeps_the_unit() {
        let chunk = chunk_of(&[10]);
        let encoder = encoder();

        let mut missing = KeyStats::with_capacity(1);
        missing.push(None, Some(Arc::new(Int64Array::from(vec![5]))));
        assert!(may_contain(&chunk, &encoder, &missing));

        let mut null = KeyStats::with_capacity(1);
        null.push(
            Some(Arc::new(Int64Array::from(vec![None::<i64>]))),
            Some(Arc::new(Int64Array::from(vec![5]))),
        );
        assert!(may_contain(&chunk, &encoder, &null));
    }

    /// A `ROW` key whose leaf has no statistic: the log gives a struct bound with a null
    /// child, which encodes below every real key, so a box test would prune the file that
    /// holds the key. Delta records nothing for a `BINARY` leaf, so this is reachable.
    #[test]
    fn a_struct_bound_with_a_null_leaf_keeps_the_unit() {
        let leaves = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Binary, true),
        ]);
        let encoder = row_key_encoder(&leaves);

        let mut chunk = LookupChunk::new(usize::MAX);
        chunk
            .extend(
                &encoder
                    .encode_columns(&[row_key(&leaves, 100, Some(b"\xab"))])
                    .unwrap(),
            )
            .unwrap();
        chunk.sort();

        // Complete bounds still prune: the key is outside [1, 50].
        let mut complete = KeyStats::with_capacity(1);
        complete.push(
            Some(row_key(&leaves, 1, Some(b"\x00"))),
            Some(row_key(&leaves, 50, Some(b"\xff"))),
        );
        assert!(!may_contain(&chunk, &encoder, &complete));

        // The same range with no statistic for `b` must keep the file, even though the key
        // is inside the surviving `a` bound.
        let mut null_leaf = KeyStats::with_capacity(1);
        null_leaf.push(
            Some(row_key(&leaves, 1, None)),
            Some(row_key(&leaves, 100, None)),
        );
        assert!(
            may_contain(&chunk, &encoder, &null_leaf),
            "a struct bound with a null leaf must not prune the file holding the key"
        );
    }

    #[test]
    fn a_statistic_the_encoder_rejects_keeps_the_unit() {
        let chunk = chunk_of(&[10]);
        let encoder = encoder();

        // A string bound for an integer key: keep, rather than prune on a comparison
        // that cannot be made.
        let mut mistyped = KeyStats::with_capacity(1);
        mistyped.push(
            Some(Arc::new(StringArray::from(vec!["a"]))),
            Some(Arc::new(StringArray::from(vec!["b"]))),
        );
        assert!(may_contain(&chunk, &encoder, &mistyped));
    }

    #[test]
    fn an_empty_chunk_prunes_everything() {
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.sort();
        assert!(!may_contain(&chunk, &encoder(), &bounds(0, 100)));
    }

    #[test]
    fn float_keys_disable_stats_pruning() {
        assert!(stats_pruning_sound(&[Field::new(
            "id",
            DataType::Int64,
            false
        )]));
        assert!(!stats_pruning_sound(&[Field::new(
            "x",
            DataType::Float64,
            false
        )]));

        let nested = Field::new(
            "k",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float32, false),
            ])),
            false,
        );
        assert!(
            !stats_pruning_sound(&[nested]),
            "a float nested in a ROW key is still a float"
        );
    }

    /// A float partition column must match either sign of zero, since the key encoder holds
    /// them equal. A filter that did not would prune the file holding the row, leaving two
    /// live rows for one key.
    #[test]
    fn a_float_partition_value_matches_either_zero() {
        let names = vec!["d".to_string()];
        let fields = vec![Field::new("d", DataType::Float64, false)];
        let mut filter = PartitionFilter::new(&names, &fields, &names)
            .unwrap()
            .unwrap();

        let key: ArrayRef = Arc::new(Float64Array::from(vec![0.0f64]));
        filter.record(&[key]).unwrap();

        let stored = HashMap::from([("d".to_string(), Scalar::Double(-0.0))]);
        assert!(
            filter.may_contain(&stored, &names),
            "a row stored under -0.0 holds the key 0.0"
        );

        let elsewhere = HashMap::from([("d".to_string(), Scalar::Double(1.0))]);
        assert!(
            !filter.may_contain(&elsewhere, &names),
            "a different value must still prune"
        );

        // The other direction, since either side may hold either sign.
        let key: ArrayRef = Arc::new(Float64Array::from(vec![-0.0f64]));
        filter.clear();
        filter.record(&[key]).unwrap();
        let stored = HashMap::from([("d".to_string(), Scalar::Double(0.0))]);
        assert!(
            filter.may_contain(&stored, &names),
            "a row stored under 0.0 holds the key -0.0"
        );
    }
}
