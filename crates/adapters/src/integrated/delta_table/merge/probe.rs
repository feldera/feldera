//! Locating the rows a flush must supersede.
//!
//! A deletion vector addresses rows by file path and physical row ordinal, not by key, so the
//! connector must turn a set of keys into a set of (path, ordinal) pairs. This lookup is the
//! only expensive step in a flush, and it reads the key columns of the row groups that survive
//! pruning and nothing else.
//!
//! The keys sought live in a sorted [`LookupChunk`], so each decoded key is resolved by binary
//! search with no auxiliary index. Peak memory is one decoded batch per concurrent task.
//!
//! # Ordinals are physical
//!
//! Ordinals count rows as if no deletion vector were applied, since that is the space a vector
//! addresses. Two things perturb the count, and either one, got wrong, shifts every ordinal
//! after it and tombstones rows nobody asked about:
//!
//! - **Skipped row groups.** A pruned group's rows still occupy their positions, so each
//!   group's base ordinal comes from the footer's row counts rather than from a running
//!   count of rows actually read.
//! - **Rows an existing vector already covers.** They must not shift the numbering, so the
//!   read applies no row selection.
//!
//! The invariant is checked rather than assumed: a batch may not cross a row group boundary,
//! and every group read must yield exactly the rows its footer declares.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{ArrayRef, RecordBatch};
use delta_kernel::expressions::Scalar;
use deltalake::kernel::LogicalFileView;
use deltalake::{DeltaTable, ObjectStore, Path};
use futures::StreamExt;
use futures::stream::{self, TryStreamExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::chunk::LookupChunk;
use super::key::KeyEncoder;
use super::prune::{KeyStats, PartitionFilter, may_contain};
use super::tombstone::Tombstones;

/// A data file the lookup may have to read, with everything the log says about it.
///
/// Built once per flush. The statistics live here rather than being looked up per lookup
/// pass, so a key set split across several passes reads the log once.
///
/// Deliberately not `Clone`: pruning borrows, so nothing copies this per file.
#[derive(Debug)]
pub struct Candidate {
    /// Path relative to the table root, as it appears in the log.
    pub path: String,
    /// Values of the key columns that are partition columns of the table.
    ///
    /// Delta keeps partition values in the log, not in the data file, so a key column that is
    /// also a partition column is reconstructed from here. Its value is constant across the
    /// file, which also makes it an exact statistic for pruning.
    pub partition_keys: HashMap<String, Scalar>,
    /// Per-column minima from the log, as a struct scalar. `None` when the log carries none
    /// for this file, which makes the file unprunable.
    pub min_values: Option<Scalar>,
    /// Per-column maxima, as [`Self::min_values`].
    pub max_values: Option<Scalar>,
}

impl Candidate {
    /// Build from a log entry. `with_stats` is false when the key's types make min/max
    /// untrustworthy, in which case nothing reads the bounds and parsing them is wasted.
    pub fn from_log(
        file: &LogicalFileView,
        partition_keys: HashMap<String, Scalar>,
        with_stats: bool,
    ) -> Self {
        let (min_values, max_values) = if with_stats {
            (as_struct(file.min_values()), as_struct(file.max_values()))
        } else {
            (None, None)
        };
        Self {
            path: file.path().to_string(),
            partition_keys,
            min_values,
            max_values,
        }
    }
}

/// What one lookup pass did, for metrics and for the efficiency tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProbeMetrics {
    /// Files whose footer was opened.
    pub files_scanned: usize,
    /// Files pruned before being opened, from the statistics in the Delta log.
    pub files_pruned: usize,
    /// Row groups whose key columns were decoded.
    pub row_groups_scanned: usize,
    /// Row groups skipped on their footer statistics.
    pub row_groups_pruned: usize,
    /// Rows to tombstone. Exceeds the number of keys found when the table holds one key in
    /// several files, which resolves once every copy is tombstoned.
    pub rows_located: u64,
    /// Keys no candidate file contained. Not an error, but a sustained nonzero rate means the
    /// table has diverged from the view.
    pub keys_not_found: u64,
}

impl ProbeMetrics {
    /// Accumulate another pass's counts. A flush runs one pass per lookup chunk.
    pub fn merge(&mut self, other: &ProbeMetrics) {
        self.files_scanned += other.files_scanned;
        self.files_pruned += other.files_pruned;
        self.row_groups_scanned += other.row_groups_scanned;
        self.row_groups_pruned += other.row_groups_pruned;
        self.rows_located += other.rows_located;
        self.keys_not_found += other.keys_not_found;
    }
}

/// What the lookup may skip without reading it.
///
/// Both tests are sound in one direction only: they may keep a unit that holds nothing, and
/// must never skip one that holds a wanted key. See [`super::prune`].
#[derive(Default, Clone, Copy)]
pub struct Pruning<'a> {
    /// Test each file and row group's key range against the chunk.
    ///
    /// Off when the key contains a float, where NaN's absence from min/max statistics would
    /// make the test able to skip a unit that holds the key.
    pub on_stats: bool,
    /// Test each file's partition against the partitions the chunk's keys belong to.
    ///
    /// Present only when some key columns are partition columns.
    pub partitions: Option<&'a PartitionFilter>,
}

impl<'a> Pruning<'a> {
    /// Read everything: the baseline the pruning tests compare against.
    #[cfg(test)]
    pub fn none() -> Self {
        Self::default()
    }

    pub fn new(on_stats: bool, partitions: Option<&'a PartitionFilter>) -> Self {
        Self {
            on_stats,
            partitions,
        }
    }
}

/// Locate every key in `chunk`, recording the rows to tombstone in `tombstones`.
///
/// `candidates` is every data file in the snapshot; the pruning happens here. `max_concurrent`
/// bounds both request concurrency and the number of decoded batches held at once. The chunk
/// must already be sorted.
pub async fn locate(
    chunk: &LookupChunk,
    candidates: &[Candidate],
    table: &DeltaTable,
    encoder: &KeyEncoder,
    max_concurrent: usize,
    pruning: Pruning<'_>,
    tombstones: &mut Tombstones,
) -> AnyResult<ProbeMetrics> {
    let mut metrics = ProbeMetrics::default();
    if chunk.is_empty() || candidates.is_empty() {
        metrics.keys_not_found = chunk.len() as u64;
        return Ok(metrics);
    }

    let to_read = prune_files(chunk, candidates, encoder, pruning, &mut metrics);
    metrics.files_scanned = to_read.len();

    let store = table.object_store();
    let results: Vec<FileHits> = stream::iter(to_read.iter().map(|candidate| {
        let store = store.clone();
        async move { probe_file(chunk, candidate, store, encoder, pruning.on_stats).await }
    }))
    .buffer_unordered(max_concurrent.max(1))
    .try_collect()
    .await?;

    // A key found in two files yields two rows to tombstone but is one key found, so
    // distinct positions are tracked separately from row count.
    let mut found = vec![false; chunk.len()];
    for file in results {
        metrics.row_groups_scanned += file.row_groups_scanned;
        metrics.row_groups_pruned += file.row_groups_pruned;
        for (ordinal, position) in file.hits {
            tombstones.insert(&file.path, ordinal);
            metrics.rows_located += 1;
            found[position] = true;
        }
    }
    metrics.keys_not_found = found.iter().filter(|f| !**f).count() as u64;

    Ok(metrics)
}

/// Drop the files whose key range cannot meet the chunk, using only the Delta log.
///
/// The pruning that pays best: a file dropped here costs no request at all, while row group
/// pruning still has to fetch the footer.
fn prune_files<'a>(
    chunk: &LookupChunk,
    candidates: &'a [Candidate],
    encoder: &KeyEncoder,
    pruning: Pruning<'_>,
    metrics: &mut ProbeMetrics,
) -> Vec<&'a Candidate> {
    let Pruning {
        on_stats,
        partitions,
    } = pruning;
    if !on_stats && partitions.is_none() {
        return candidates.iter().collect();
    }

    let names = encoder.column_names();
    let mut keep = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        // The partition test first: it is exact and cheaper than assembling statistics.
        if let Some(filter) = partitions
            && !filter.may_contain(&candidate.partition_keys, names)
        {
            metrics.files_pruned += 1;
            continue;
        }

        if !on_stats {
            keep.push(candidate);
            continue;
        }

        // The log carries min/max only for the first `delta.dataSkippingNumIndexedCols`
        // columns. A key outside that prefix has no statistic, so its file is kept.
        let mut stats = KeyStats::with_capacity(names.len());
        for name in names {
            if let Some(value) = candidate.partition_keys.get(name) {
                // Constant within the file, so an exact bound on both sides.
                let array = value.to_array(1).ok();
                stats.push(array.clone(), array);
            } else {
                stats.push(
                    field_array(candidate.min_values.as_ref(), name),
                    field_array(candidate.max_values.as_ref(), name),
                );
            }
        }

        if may_contain(chunk, encoder, &stats) {
            keep.push(candidate);
        } else {
            metrics.files_pruned += 1;
        }
    }
    keep
}

fn as_struct(scalar: Option<Scalar>) -> Option<Scalar> {
    matches!(scalar, Some(Scalar::Struct(_)))
        .then_some(scalar)
        .flatten()
}

/// One-element array holding `name`'s value inside a struct scalar of statistics.
fn field_array(stats: Option<&Scalar>, name: &str) -> Option<ArrayRef> {
    let Some(Scalar::Struct(data)) = stats else {
        return None;
    };
    let index = data.fields().iter().position(|f| f.name() == name)?;
    let value = data.values().get(index)?;
    if value.is_null() {
        return None;
    }
    value.to_array(1).ok()
}

/// Rows one file contributed, plus what it cost to find them.
struct FileHits {
    path: String,
    /// `(physical ordinal, position of the key in the sorted chunk)`.
    hits: Vec<(u64, usize)>,
    row_groups_scanned: usize,
    row_groups_pruned: usize,
}

/// One row group the probe intends to read.
struct RowGroup {
    index: usize,
    /// Ordinal of this group's first row within the file, counting pruned groups.
    base: u64,
    rows: u64,
}

/// Read one file's key columns and return the rows whose key is in `chunk`.
async fn probe_file(
    chunk: &LookupChunk,
    candidate: &Candidate,
    store: Arc<dyn ObjectStore>,
    encoder: &KeyEncoder,
    prune_on_stats: bool,
) -> AnyResult<FileHits> {
    let path = Path::from(candidate.path.as_str());
    let reader = ParquetObjectReader::new(store, path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| anyhow!("failed to open Delta data file '{}': {e}", candidate.path))?;

    let selected = select_row_groups(chunk, candidate, &builder, encoder, prune_on_stats);
    let total_groups = builder.metadata().row_groups().len();
    let pruned = total_groups - selected.len();

    if selected.is_empty() {
        return Ok(FileHits {
            path: candidate.path.clone(),
            hits: Vec::new(),
            row_groups_scanned: 0,
            row_groups_pruned: pruned,
        });
    }

    let mask = key_projection_mask(&builder, encoder);
    let mut stream = builder
        .with_projection(mask)
        .with_row_groups(selected.iter().map(|g| g.index).collect())
        .build()
        .map_err(|e| anyhow!("failed to read Delta data file '{}': {e}", candidate.path))?;

    let mut hits = Vec::new();
    let mut group = selected.iter();
    let mut current = group.next().expect("checked non-empty");
    let mut consumed = 0u64;

    while let Some(batch) = stream.next().await {
        let batch: RecordBatch = batch.map_err(|e| {
            anyhow!(
                "error reading Delta data file '{}' while locating rows: {e}",
                candidate.path
            )
        })?;

        if consumed == current.rows {
            current = group.next().ok_or_else(|| {
                anyhow!(
                    "internal error: '{}' yielded more rows than its selected row groups \
                     declare; physical row ordinals would be wrong",
                    candidate.path
                )
            })?;
            consumed = 0;
        }
        let rows_in_batch = batch.num_rows() as u64;
        if consumed + rows_in_batch > current.rows {
            // The ordinal arithmetic assumes the reader never merges row groups into one
            // batch. If that changes, fail loudly rather than tombstone the wrong rows.
            return Err(anyhow!(
                "internal error: a batch from '{}' spans a row group boundary; \
                 physical row ordinals would be wrong",
                candidate.path
            ));
        }

        let keys = encode_keys(&batch, candidate, encoder)?;
        for i in 0..keys.num_rows() {
            if let Some(position) = chunk.position(keys.row(i).as_ref()) {
                hits.push((current.base + consumed + i as u64, position));
            }
        }
        consumed += rows_in_batch;
    }

    if consumed != current.rows || group.next().is_some() {
        return Err(anyhow!(
            "internal error: '{}' yielded fewer rows than its selected row groups declare; \
             physical row ordinals would be wrong",
            candidate.path
        ));
    }

    Ok(FileHits {
        path: candidate.path.clone(),
        hits,
        row_groups_scanned: selected.len(),
        row_groups_pruned: pruned,
    })
}

/// Encode the key of every row in `batch`. A key column that is also a partition column is
/// absent from the data file, so it comes as a constant from the file's partition values.
fn encode_keys(
    batch: &RecordBatch,
    candidate: &Candidate,
    encoder: &KeyEncoder,
) -> AnyResult<arrow::row::Rows> {
    let mut columns = Vec::with_capacity(encoder.column_names().len());
    for name in encoder.column_names() {
        if let Some(column) = batch.column_by_name(name) {
            columns.push(column.clone());
        } else if let Some(value) = candidate.partition_keys.get(name) {
            columns.push(value.to_array(batch.num_rows()).map_err(|e| {
                anyhow!("unable to expand the partition value of key column '{name}': {e}")
            })?);
        } else {
            return Err(anyhow!(
                "key column '{name}' is neither in data file '{}' nor among its partition \
                 values, so the row to supersede cannot be identified",
                candidate.path
            ));
        }
    }
    encoder.encode_columns(&columns)
}

/// Row groups whose key range can meet the chunk, with their base ordinals.
///
/// Bases are computed over *all* row groups, so a pruned group still advances the count.
fn select_row_groups(
    chunk: &LookupChunk,
    candidate: &Candidate,
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    encoder: &KeyEncoder,
    prune_on_stats: bool,
) -> Vec<RowGroup> {
    let metadata = builder.metadata();
    let groups = metadata.row_groups();

    let mut bases = Vec::with_capacity(groups.len());
    let mut base = 0u64;
    for group in groups {
        bases.push(base);
        base += group.num_rows().max(0) as u64;
    }

    // An empty row group yields no batch, so it would strand the walk over the
    // selected groups. Bases are computed over all groups, so ordinals are unaffected.
    if !prune_on_stats {
        return groups
            .iter()
            .enumerate()
            .filter(|(_, group)| group.num_rows() > 0)
            .map(|(index, group)| RowGroup {
                index,
                base: bases[index],
                rows: group.num_rows() as u64,
            })
            .collect();
    }

    // Statistics arrive per column with one entry per row group, so they are gathered
    // column by column and then read across.
    let arrow_schema = builder.schema();
    let mut mins: Vec<Option<ArrayRef>> = Vec::new();
    let mut maxes: Vec<Option<ArrayRef>> = Vec::new();
    for name in encoder.column_names() {
        if candidate.partition_keys.contains_key(name) {
            // Constant across the file: handled per row group below.
            mins.push(None);
            maxes.push(None);
            continue;
        }
        let converter = StatisticsConverter::try_new(
            name,
            arrow_schema,
            metadata.file_metadata().schema_descr(),
        );
        match converter {
            Ok(converter) => {
                mins.push(converter.row_group_mins(groups.iter()).ok());
                maxes.push(converter.row_group_maxes(groups.iter()).ok());
            }
            Err(_) => {
                mins.push(None);
                maxes.push(None);
            }
        }
    }

    (0..groups.len())
        .filter(|index| groups[*index].num_rows() > 0)
        .filter_map(|index| {
            let mut stats = KeyStats::with_capacity(encoder.column_names().len());
            for (column, name) in encoder.column_names().iter().enumerate() {
                if let Some(value) = candidate.partition_keys.get(name) {
                    let array = value.to_array(1).ok();
                    stats.push(array.clone(), array);
                } else {
                    stats.push(
                        slice_one(&mins[column], index),
                        slice_one(&maxes[column], index),
                    );
                }
            }
            may_contain(chunk, encoder, &stats).then(|| RowGroup {
                index,
                base: bases[index],
                rows: groups[index].num_rows() as u64,
            })
        })
        .collect()
}

/// One-element slice of a per-row-group statistics array.
fn slice_one(array: &Option<ArrayRef>, index: usize) -> Option<ArrayRef> {
    let array = array.as_ref()?;
    (index < array.len()).then(|| array.slice(index, 1))
}

/// Select only the key columns the file actually stores, by name.
///
/// By root, so a `ROW` key pulls its own leaves and nothing else. By name rather than
/// position, so a file whose column order differs from the table schema still works, and a
/// key column the file does not store (a partition column) is skipped.
fn key_projection_mask(
    builder: &ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
    encoder: &KeyEncoder,
) -> ProjectionMask {
    let names = encoder.column_names();
    let roots = builder
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| names.iter().any(|n| n == field.name()))
        .map(|(idx, _)| idx);
    ProjectionMask::roots(builder.parquet_schema(), roots)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::integrated::delta_table::merge::key::{KeyEncoder, contains_null};
    use crate::integrated::delta_table::merge::test::{
        arrow_schema, fixture_columns, key_relation,
    };
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};
    use deltalake::operations::create::CreateBuilder;
    use tempfile::TempDir;

    fn encode_keys(values: &[i64]) -> arrow::row::Rows {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        converter.convert_columns(&[column]).unwrap()
    }

    /// Ordinals per file path, for comparing two lookups.
    fn tombstone_summary(
        tombstones: &Tombstones,
        candidates: &[Candidate],
    ) -> Vec<(String, Vec<u64>)> {
        candidates
            .iter()
            .filter_map(|c| {
                tombstones
                    .ordinals_for(&c.path)
                    .map(|bitmap| (c.path.clone(), bitmap.iter().collect()))
            })
            .collect()
    }

    fn chunk_of(values: &[i64]) -> LookupChunk {
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&encode_keys(values)).unwrap();
        chunk.sort();
        chunk
    }

    fn batch_of(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(arrow_schema()),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("v{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    /// The same, with `None` for a null key, marked the way a flush marks one.
    fn chunk_of_opt(values: &[Option<i64>]) -> LookupChunk {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        let rows = converter
            .convert_columns(std::slice::from_ref(&column))
            .unwrap();

        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&rows).unwrap();
        if contains_null(&[column]) {
            chunk.note_null_key();
        }
        chunk.sort();
        chunk
    }

    fn batch_of_opt(ids: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(arrow_schema()),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("v{i:?}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn candidates_of(table: &DeltaTable) -> Vec<Candidate> {
        let mut candidates: Vec<Candidate> = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| Candidate::from_log(&f, HashMap::new(), true))
            .collect();
        candidates.sort_by(|a, b| a.path.cmp(&b.path));
        candidates
    }

    /// Build a table holding `ids` as one data file.
    async fn table_with(dir: &TempDir, ids: &[i64]) -> (DeltaTable, Vec<Candidate>) {
        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns(fixture_columns())
            .await
            .unwrap();
        let table = table.write(vec![batch_of(ids)]).await.unwrap();
        let candidates = candidates_of(&table);
        (table, candidates)
    }

    /// Build a table where each element of `files` becomes its own data file.
    async fn table_with_files(dir: &TempDir, files: &[&[i64]]) -> (DeltaTable, Vec<Candidate>) {
        let mut table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns(fixture_columns())
            .await
            .unwrap();
        for ids in files {
            table = table.write(vec![batch_of(ids)]).await.unwrap();
        }
        let candidates = candidates_of(&table);
        (table, candidates)
    }

    /// Build a table holding `ids` in one file split into row groups of `row_group_rows`.
    ///
    /// delta-rs defaults to a row group large enough that any fixture this test would
    /// tolerate writing lands in one group, which would leave row group pruning untested.
    async fn table_with_row_groups(
        dir: &TempDir,
        ids: &[i64],
        row_group_rows: usize,
    ) -> (DeltaTable, Vec<Candidate>) {
        use parquet::file::properties::WriterProperties;

        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns(fixture_columns())
            .await
            .unwrap();
        let table = table
            .write(vec![batch_of(ids)])
            .with_writer_properties(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(row_group_rows))
                    .build(),
            )
            .await
            .unwrap();
        let candidates = candidates_of(&table);
        (table, candidates)
    }

    #[tokio::test]
    async fn locates_present_keys_and_reports_absent_ones() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[10, 20, 30, 40]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of(&[20, 40, 99]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(metrics.rows_located, 2);
        assert_eq!(metrics.keys_not_found, 1);
        assert_eq!(tombstones.total_rows(), 2);
        assert_eq!(tombstones.touched_files(), 1);
    }

    /// Ordinals must be physical positions in the file, not positions among the rows read.
    ///
    /// The file spans several decoded batches, otherwise a running counter and a per-batch
    /// index agree and the test cannot tell them apart.
    #[tokio::test]
    async fn ordinals_are_physical_positions() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..3000).collect();
        let (table, candidates) = table_with(&dir, &ids).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of(&[1500, 2999]);

        let mut tombstones = Tombstones::new();
        locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            1,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        // Ids were written in order, so a key's ordinal equals its value.
        let found = tombstones.ordinals_for(&candidates[0].path).unwrap();
        assert_eq!(found.iter().collect::<Vec<_>>(), vec![1500, 2999]);
    }

    /// A key living in two files must yield a tombstone for each, counted as one key found.
    ///
    /// The connector maintains one live row per key, but Delta enforces no such constraint,
    /// so a table the connector adopted can already hold two rows for one key. Tombstoning
    /// every match is what makes that self-healing: after one update the key has a single
    /// live row again. Recording only the first match would leave the duplicate live forever.
    #[tokio::test]
    async fn a_key_in_two_files_is_tombstoned_in_both() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with_files(&dir, &[&[10, 20], &[20, 30]]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of(&[20]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(metrics.rows_located, 2, "both copies must be located");
        assert_eq!(metrics.keys_not_found, 0);
        assert_eq!(tombstones.total_rows(), 2);
        assert_eq!(
            tombstones.touched_files(),
            2,
            "the copies live in different files, so both files need a vector"
        );
    }

    /// Build a table holding `ids` as one data file, where `None` is a null key.
    async fn table_with_opt(dir: &TempDir, ids: &[Option<i64>]) -> (DeltaTable, Vec<Candidate>) {
        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns(fixture_columns())
            .await
            .unwrap();
        let table = table.write(vec![batch_of_opt(ids)]).await.unwrap();
        let candidates = candidates_of(&table);
        (table, candidates)
    }

    /// A null key must survive pruning.
    ///
    /// Min/max statistics leave nulls out, so this file reports the range [100, 100] while
    /// also holding a null-keyed row. A range test puts the null below that range and prunes
    /// the file, which would skip the tombstone and leave two live rows for one key.
    #[tokio::test]
    async fn a_null_key_is_not_pruned_away() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with_opt(&dir, &[None, Some(100)]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of_opt(&[None]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            1,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(
            metrics.keys_not_found, 0,
            "the null-keyed row was pruned away"
        );
        assert_eq!(metrics.rows_located, 1);
        assert_eq!(tombstones.total_rows(), 1);
    }

    /// File-level pruning must skip files whose key range cannot hold any wanted key,
    /// without changing which rows are located.
    ///
    /// Four files with disjoint ranges; the chunk touches one. The other three must not be
    /// opened at all, which is the pruning that costs no request rather than a cheaper one.
    #[tokio::test]
    async fn file_pruning_skips_disjoint_files() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with_files(
            &dir,
            &[&[0, 1, 2], &[10, 11, 12], &[20, 21, 22], &[30, 31, 32]],
        )
        .await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of(&[11, 12]);

        let mut pruned_tombstones = Tombstones::new();
        let pruned = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            Pruning::new(true, None),
            &mut pruned_tombstones,
        )
        .await
        .unwrap();

        let mut full_tombstones = Tombstones::new();
        locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            Pruning::none(),
            &mut full_tombstones,
        )
        .await
        .unwrap();

        assert_eq!(pruned.files_pruned, 3, "{pruned:?}");
        assert_eq!(pruned.files_scanned, 1, "{pruned:?}");
        assert_eq!(pruned.rows_located, 2);
        assert_eq!(
            tombstone_summary(&pruned_tombstones, &candidates),
            tombstone_summary(&full_tombstones, &candidates),
            "pruning changed which rows were located"
        );
    }

    /// Row group pruning must skip groups *and* keep ordinals physical.
    ///
    /// This is the pairing that a naive implementation gets wrong: skipping a row group
    /// while counting ordinals over the rows actually read shifts every later ordinal, so
    /// the tombstones land on rows nobody asked about. The ids here equal their own
    /// ordinals, so a shift is visible.
    #[tokio::test]
    async fn row_group_pruning_keeps_ordinals_physical() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let (table, candidates) = table_with_row_groups(&dir, &ids, 100).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        // Keys in the last two groups only, so eight of ten groups must be skipped.
        let chunk = chunk_of(&[850, 999]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            1,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(metrics.row_groups_scanned, 2, "{metrics:?}");
        assert_eq!(metrics.row_groups_pruned, 8, "{metrics:?}");
        assert_eq!(
            tombstones
                .ordinals_for(&candidates[0].path)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![850, 999],
            "an ordinal shifted by the skipped row groups"
        );
    }

    /// Reading with pruning off must give the same ordinals, on the same fixture.
    ///
    /// The negative control for the test above: it fixes the expected ordinals
    /// independently of the pruning path.
    #[tokio::test]
    async fn row_group_pruning_agrees_with_reading_everything() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let (table, candidates) = table_with_row_groups(&dir, &ids, 100).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let chunk = chunk_of(&[3, 250, 850, 999]);

        let mut pruned = Tombstones::new();
        locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            1,
            Pruning::new(true, None),
            &mut pruned,
        )
        .await
        .unwrap();
        let mut full = Tombstones::new();
        let full_metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            1,
            Pruning::none(),
            &mut full,
        )
        .await
        .unwrap();

        assert_eq!(full_metrics.row_groups_scanned, 10);
        assert_eq!(
            tombstone_summary(&pruned, &candidates),
            tombstone_summary(&full, &candidates)
        );
        assert_eq!(
            pruned
                .ordinals_for(&candidates[0].path)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![3, 250, 850, 999]
        );
    }

    #[tokio::test]
    async fn empty_chunk_reads_nothing() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[1, 2]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();

        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.sort();
        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(metrics.files_scanned, 0);
        assert_eq!(metrics.rows_located, 0);
        assert!(tombstones.is_empty());
    }
}
