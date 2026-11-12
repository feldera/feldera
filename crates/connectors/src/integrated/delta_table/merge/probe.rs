//! Locating the rows a flush must supersede.
//!
//! A deletion vector addresses rows by file path and physical row ordinal, not by key, so the
//! lookup turns a set of keys into a set of (path, ordinal) pairs. It is the only expensive
//! step in a flush, reading the key columns of the row groups that survive pruning.
//!
//! The keys sought live in a sorted [`LookupChunk`]: its Bloom filter rejects most decoded
//! keys, and a binary search confirms the rest. Peak memory is one decoded batch per task.
//!
//! # Ordinals are physical
//!
//! Ordinals count rows as if no deletion vector were applied, since that is the space a
//! vector addresses. Get that wrong and every ordinal after the mistake shifts, tombstoning
//! rows nobody asked about. Two things perturb the count:
//!
//! - A pruned row group's rows still occupy their positions, so a group's base ordinal comes
//!   from the footer's row counts, not from a count of rows actually read.
//! - Rows an existing vector covers must not shift the numbering, so the read applies no row
//!   selection.
//!
//! Both are checked rather than assumed: a batch may not cross a row group boundary, and
//! every group read must yield exactly the rows its footer declares.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::{
    Array, ArrayRef, AsArray, RecordBatch, TimestampMicrosecondArray, new_null_array,
};
use arrow::compute::concat;
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use delta_kernel::expressions::Scalar;
use deltalake::datafusion::common::Column;
use deltalake::datafusion::physical_optimizer::pruning::PruningStatistics;
use deltalake::kernel::{LogDataHandler, LogicalFileView};
use deltalake::logstore::ObjectStoreRef;
use deltalake::table::state::DeltaTableState;
use deltalake::{ObjectStore, Path};
use futures::StreamExt;
use futures::stream::{self, TryStreamExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use tokio_util::task::AbortOnDropHandle;

use super::super::WriteError;
use super::super::deletion_vector::ObjectStoreReader;
use super::chunk::LookupChunk;
use super::key::KeyEncoder;
use super::prune::{KeyStats, PartitionFilter, may_contain};
use super::tombstone::Tombstones;
use super::{retry_io, transient};

/// Bytes of a data file's tail to fetch when opening it.
///
/// Without a hint the reader spends one request learning the footer's length and another
/// reading it. One request covers both whenever the footer fits, and a footer that does not
/// fit simply costs the second request it would have cost anyway.
const FOOTER_SIZE_HINT: usize = 32 * 1024;

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
    /// Size in bytes from the log, so the reader addresses the footer directly rather than
    /// asking for a suffix of the object, which not every store serves.
    size: Option<u64>,
    /// Values of the key columns that are partition columns of the table.
    ///
    /// Delta keeps partition values in the log, not in the data file, so a key column that is
    /// also a partition column is reconstructed from here. Its value is constant across the
    /// file, which also makes it an exact statistic for pruning.
    pub partition_keys: HashMap<String, Scalar>,
    /// The key bounds of all files in the snapshot. All candidates share them.
    bounds: Arc<LogBounds>,
    /// The position of this file in `bounds`.
    row: usize,
}

impl Candidate {
    /// One-element array of key column `index`'s minimum, or `None` without one.
    fn min_of(&self, index: usize) -> Option<ArrayRef> {
        bound_at(self.bounds.mins.get(index), self.row)
    }

    fn max_of(&self, index: usize) -> Option<ArrayRef> {
        bound_at(self.bounds.maxes.get(index), self.row)
    }
}

/// Make one candidate for each data file in the snapshot. If `with_stats` is false, the
/// candidates have no key bounds.
pub fn candidates_from_log(
    snapshot: &DeltaTableState,
    encoder: &KeyEncoder,
    with_stats: bool,
    mut partition_keys: impl FnMut(&LogicalFileView) -> HashMap<String, Scalar>,
) -> Vec<Candidate> {
    let log = snapshot.log_data();
    let bounds = Arc::new(if with_stats {
        LogBounds::read(&log, encoder, snapshot.metadata().partition_columns())
    } else {
        LogBounds::default()
    });
    log.iter()
        .enumerate()
        .map(|(row, file)| Candidate {
            path: file.path().to_string(),
            size: u64::try_from(file.size()).ok().filter(|size| *size > 0),
            partition_keys: partition_keys(&file),
            bounds: bounds.clone(),
            row,
        })
        .collect()
}

/// The min and max of each key column for all files, in `log_data()` order. One read for
/// each column is fast. One read for each file took 1.6 s on 200k files.
#[derive(Debug, Default)]
struct LogBounds {
    /// In key column order. `None` when the column has no usable bounds.
    mins: Vec<Option<ArrayRef>>,
    maxes: Vec<Option<ArrayRef>>,
}

impl LogBounds {
    fn read(log: &LogDataHandler<'_>, encoder: &KeyEncoder, partition_columns: &[String]) -> Self {
        let mut bounds = Self::default();
        for (name, data_type) in encoder.column_names().iter().zip(encoder.column_types()) {
            let (min, max) = if partition_columns.contains(name) {
                // The log has no stats for a partition column. Pruning uses its value.
                (None, None)
            } else if let DataType::Struct(_) = data_type {
                // For a struct column, delta-rs returns the first field, not the struct.
                (
                    struct_bounds(log, name, false),
                    struct_bounds(log, name, true),
                )
            } else {
                let column = Column::from_name(name);
                let files = log.num_files();
                (
                    usable(log.min_values(&column), files),
                    usable(log.max_values(&column), files).map(|max| round_up_ms(&max)),
                )
            };
            bounds.mins.push(min);
            bounds.maxes.push(max);
        }
        bounds
    }
}

/// Return the bounds only when they are safe for pruning.
fn usable(array: Option<ArrayRef>, files: usize) -> Option<ArrayRef> {
    let array = array?;
    // delta-rs writes decimal stats through f64, so a decimal bound can be wrong.
    let decimal = matches!(
        array.data_type(),
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    );
    // A different length can give a file the bound of a different file.
    (array.len() == files && !decimal).then_some(array)
}

/// Raise each whole-millisecond timestamp max by 1 ms, as `LogicalFileView::max_values`
/// does. The log can cut a timestamp to ms, so the true max can be higher.
fn round_up_ms(array: &ArrayRef) -> ArrayRef {
    if !matches!(
        array.data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, _)
    ) {
        return array.clone();
    }
    let raised: TimestampMicrosecondArray =
        array.as_primitive::<TimestampMicrosecondType>().unary(|v| {
            if v % 1_000 == 0 {
                v.saturating_add(1_000)
            } else {
                v
            }
        });
    Arc::new(raised.with_data_type(array.data_type().clone()))
}

/// The min or max of a ROW column for all files, read file by file.
fn struct_bounds(log: &LogDataHandler<'_>, name: &str, max: bool) -> Option<ArrayRef> {
    // Join often: one small array for each file uses 2.6 times the memory of the old code.
    const CHUNK: usize = 1024;
    let (mut data_type, mut leading_nulls) = (None, 0);
    let (mut parts, mut chunk) = (Vec::new(), Vec::with_capacity(CHUNK));
    for file in log.iter() {
        let stats = if max {
            file.max_values()
        } else {
            file.min_values()
        };
        let bound = struct_bound(stats, name);
        match (&data_type, &bound) {
            (None, None) => leading_nulls += 1,
            (None, Some(first)) => data_type = Some(first.data_type().clone()),
            _ => {}
        }
        if data_type.is_some() {
            chunk.push(bound);
        }
        if chunk.len() == CHUNK {
            parts.push(join(&chunk, data_type.as_ref()?)?);
            chunk.clear();
        }
    }
    let data_type = data_type?;
    // An empty chunk cannot be joined.
    if !chunk.is_empty() {
        parts.push(join(&chunk, &data_type)?);
    }
    parts.insert(0, new_null_array(&data_type, leading_nulls));
    concat(&parts.iter().map(|a| a.as_ref()).collect::<Vec<_>>()).ok()
}

/// Join one-row bounds into one array. A file without a bound gets a null.
fn join(bounds: &[Option<ArrayRef>], data_type: &DataType) -> Option<ArrayRef> {
    let null = new_null_array(data_type, 1);
    let arrays: Vec<&dyn Array> = bounds
        .iter()
        .map(|bound| bound.as_deref().unwrap_or(null.as_ref()))
        .collect();
    concat(&arrays).ok()
}

/// The bound of column `name` in one file, or `None` if it is missing, null or a decimal.
fn struct_bound(stats: Option<Scalar>, name: &str) -> Option<ArrayRef> {
    let Some(Scalar::Struct(data)) = stats else {
        return None;
    };
    let index = data.fields().iter().position(|f| f.name() == name)?;
    let value = data.values().get(index)?;
    if value.is_null() || contains_decimal(value) {
        return None;
    }
    value.to_array(1).ok()
}

fn contains_decimal(value: &Scalar) -> bool {
    match value {
        Scalar::Decimal(_) => true,
        Scalar::Struct(data) => data.values().iter().any(contains_decimal),
        _ => false,
    }
}

/// The bound of one file as a one-row array, or `None` if the file has no bound.
fn bound_at(bounds: Option<&Option<ArrayRef>>, row: usize) -> Option<ArrayRef> {
    let array = bounds?.as_ref()?;
    (row < array.len() && !array.is_null(row)).then(|| array.slice(row, 1))
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
    /// Compressed bytes of the key columns the probe read. A lookup that cannot prune reads
    /// the key column of the whole table, so this is the flush's read amplification.
    pub key_bytes_read: u64,
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
        self.key_bytes_read += other.key_bytes_read;
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
/// Probe one file, retrying it alone: one flaky request costs a re-read of that file rather
/// than a re-run of the whole lookup.
async fn probe_with_retries<'a>(
    chunk: &'a LookupChunk,
    candidate: &'a Candidate,
    store: ObjectStoreRef,
    encoder: &'a KeyEncoder,
    on_stats: bool,
) -> Result<FileHits, WriteError> {
    retry_io(
        &format!("probing Delta data file '{}'", candidate.path),
        || probe_file(chunk, candidate, store.clone(), encoder, on_stats),
    )
    .await
}

pub async fn locate(
    chunk: Arc<LookupChunk>,
    candidates: Arc<Vec<Candidate>>,
    store: ObjectStoreRef,
    encoder: Arc<KeyEncoder>,
    max_concurrent: usize,
    pruning: Pruning<'_>,
    tombstones: &mut Tombstones,
) -> Result<ProbeMetrics, WriteError> {
    let mut metrics = ProbeMetrics::default();
    if chunk.is_empty() || candidates.is_empty() {
        metrics.keys_not_found = chunk.len() as u64;
        return Ok(metrics);
    }

    let to_read = prune_files(&chunk, &candidates, &encoder, pruning, &mut metrics);
    metrics.files_scanned = to_read.len();
    let on_stats = pruning.on_stats;

    // Spawned, not awaited in place: decoding a file's key columns is CPU-bound, and polling
    // every probe on the one task driving the stream runs it all on a single core.
    // `buffer_unordered` still caps how many are in flight, so memory is unchanged.
    //
    // Aborted on drop, because the first error drops the probes still in flight: a plain
    // `JoinHandle` would only detach them, leaving them decoding against the store the flush
    // just failed against and holding their share of the chunk alive across the retry.
    let results: Vec<FileHits> = stream::iter(to_read)
        .map(|file| {
            let (chunk, candidates, encoder, store) = (
                chunk.clone(),
                candidates.clone(),
                encoder.clone(),
                store.clone(),
            );
            AbortOnDropHandle::new(tokio::spawn(async move {
                probe_with_retries(&chunk, &candidates[file], store, &encoder, on_stats).await
            }))
        })
        .buffer_unordered(max_concurrent.max(1))
        .map(|joined| match joined {
            Ok(hits) => hits,
            // Deterministic, not transient: the probe panicked on data that has not changed,
            // so retrying the flush would panic again. A cancelled probe cannot appear here:
            // the abort only fires once this stream is dropped, which is after the collect.
            Err(e) => Err(WriteError::Deterministic(anyhow!(
                "a merge probe panicked: {e}"
            ))),
        })
        .try_collect()
        .await?;

    // A key found in two files yields two rows to tombstone but is one key found, so
    // distinct positions are tracked separately from row count.
    let mut found = vec![false; chunk.len()];
    for file in results {
        metrics.row_groups_scanned += file.row_groups_scanned;
        metrics.row_groups_pruned += file.row_groups_pruned;
        metrics.key_bytes_read += file.key_bytes_read;
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
fn prune_files(
    chunk: &LookupChunk,
    candidates: &[Candidate],
    encoder: &KeyEncoder,
    pruning: Pruning<'_>,
    metrics: &mut ProbeMetrics,
) -> Vec<usize> {
    let Pruning {
        on_stats,
        partitions,
    } = pruning;
    if !on_stats && partitions.is_none() {
        return (0..candidates.len()).collect();
    }

    let names = encoder.column_names();
    let mut keep = Vec::with_capacity(candidates.len());
    for (file, candidate) in candidates.iter().enumerate() {
        // The partition test first: it is exact and cheaper than assembling statistics.
        if let Some(filter) = partitions
            && !filter.may_contain(&candidate.partition_keys, names)
        {
            metrics.files_pruned += 1;
            continue;
        }

        if !on_stats {
            keep.push(file);
            continue;
        }

        // The log carries min/max only for the first `delta.dataSkippingNumIndexedCols`
        // columns. A key outside that prefix has no statistic, so its file is kept.
        let mut stats = KeyStats::with_capacity(names.len());
        for (index, name) in names.iter().enumerate() {
            if let Some(value) = candidate.partition_keys.get(name) {
                // Constant within the file, so an exact bound on both sides.
                let array = value.to_array(1).ok();
                stats.push(array.clone(), array);
            } else {
                stats.push(candidate.min_of(index), candidate.max_of(index));
            }
        }

        if may_contain(chunk, encoder, &stats) {
            keep.push(file);
        } else {
            metrics.files_pruned += 1;
        }
    }
    keep
}

/// Rows one file contributed, plus what it cost to find them.
struct FileHits {
    path: String,
    /// `(physical ordinal, position of the key in the sorted chunk)`.
    hits: Vec<(u64, usize)>,
    row_groups_scanned: usize,
    row_groups_pruned: usize,
    key_bytes_read: u64,
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
) -> Result<FileHits, WriteError> {
    // delta-rs already decoded this path, so `Path::from` would encode it twice and name an
    // object that does not exist. Same choice as delta-rs's own `object_store_path`.
    let path = Path::parse(candidate.path.as_str())
        .unwrap_or_else(|_| Path::from(candidate.path.as_str()));
    let mut reader = ObjectStoreReader::new(store, path.clone());
    if let Some(size) = candidate.size {
        reader = reader.with_file_size(size);
    }
    reader = reader.with_footer_size_hint(match candidate.size {
        // Never ask for more of the object than there is.
        Some(size) => FOOTER_SIZE_HINT.min(size as usize),
        None => FOOTER_SIZE_HINT,
    });
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            transient(format!(
                "failed to open Delta data file '{}': {e}",
                candidate.path
            ))
        })?;

    let selected = select_row_groups(chunk, candidate, &builder, encoder, prune_on_stats);
    let total_groups = builder.metadata().row_groups().len();
    let pruned = total_groups - selected.len();

    if selected.is_empty() {
        return Ok(FileHits {
            path: candidate.path.clone(),
            hits: Vec::new(),
            row_groups_scanned: 0,
            row_groups_pruned: pruned,
            key_bytes_read: 0,
        });
    }

    let mask = key_projection_mask(&builder, encoder);
    let key_bytes_read = selected_key_bytes(builder.metadata(), &selected, &mask);
    let mut stream = builder
        .with_projection(mask)
        .with_row_groups(selected.iter().map(|g| g.index).collect())
        .build()
        .map_err(|e| {
            transient(format!(
                "failed to read Delta data file '{}': {e}",
                candidate.path
            ))
        })?;

    let mut hits = Vec::new();
    let mut group = selected.iter();
    let mut current = group.next().expect("checked non-empty");
    let mut consumed = 0u64;

    while let Some(batch) = stream.next().await {
        let batch: RecordBatch = batch.map_err(|e| {
            transient(format!(
                "error reading Delta data file '{}' while locating rows: {e}",
                candidate.path
            ))
        })?;

        if consumed == current.rows {
            current = group.next().ok_or_else(|| {
                WriteError::Deterministic(anyhow!(
                    "internal error: '{}' yielded more rows than its selected row groups \
                     declare; physical row ordinals would be wrong",
                    candidate.path
                ))
            })?;
            consumed = 0;
        }
        let rows_in_batch = batch.num_rows() as u64;
        if consumed + rows_in_batch > current.rows {
            // The ordinal arithmetic assumes the reader never merges row groups into one
            // batch. If that changes, fail loudly rather than tombstone the wrong rows.
            return Err(WriteError::Deterministic(anyhow!(
                "internal error: a batch from '{}' spans a row group boundary; \
                 physical row ordinals would be wrong",
                candidate.path
            )));
        }

        let keys = encode_keys(&batch, candidate, encoder).map_err(WriteError::Deterministic)?;
        for i in 0..keys.num_rows() {
            if let Some(position) = chunk.position(keys.row(i).as_ref()) {
                hits.push((current.base + consumed + i as u64, position));
            }
        }
        consumed += rows_in_batch;
    }

    if consumed != current.rows || group.next().is_some() {
        return Err(WriteError::Deterministic(anyhow!(
            "internal error: '{}' yielded fewer rows than its selected row groups declare; \
             physical row ordinals would be wrong",
            candidate.path
        )));
    }

    Ok(FileHits {
        path: candidate.path.clone(),
        hits,
        row_groups_scanned: selected.len(),
        row_groups_pruned: pruned,
        key_bytes_read,
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

/// Compressed bytes of the key columns in the row groups the probe will read.
///
/// A row group's column chunks are in leaf-column order, which is the order the projection
/// mask indexes.
fn selected_key_bytes(
    metadata: &ParquetMetaData,
    selected: &[RowGroup],
    mask: &ProjectionMask,
) -> u64 {
    selected
        .iter()
        .flat_map(|group| metadata.row_group(group.index).columns().iter().enumerate())
        .filter(|(leaf, _)| mask.leaf_included(*leaf))
        .map(|(_, column)| column.compressed_size().max(0) as u64)
        .sum()
}

/// Row groups whose key range can meet the chunk, with their base ordinals.
///
/// Bases are computed over *all* row groups, so a pruned group still advances the count.
fn select_row_groups(
    chunk: &LookupChunk,
    candidate: &Candidate,
    builder: &ParquetRecordBatchStreamBuilder<ObjectStoreReader>,
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
    builder: &ParquetRecordBatchStreamBuilder<ObjectStoreReader>,
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
    use crate::integrated::delta_table::merge::IO_ATTEMPTS;
    use crate::integrated::delta_table::merge::key::{KeyEncoder, contains_null};
    use crate::integrated::delta_table::merge::test::{
        arrow_schema, fixture_columns, key_relation,
    };
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};
    use deltalake::DeltaTable;
    use deltalake::kernel::StructField;
    use deltalake::operations::create::CreateBuilder;
    use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
    use tempfile::TempDir;

    /// The old bounds, read file by file. The tests compare the new bounds with these.
    fn per_file_bound(stats: Option<Scalar>, name: &str) -> Option<ArrayRef> {
        fn has_decimal(value: &Scalar) -> bool {
            match value {
                Scalar::Decimal(_) => true,
                Scalar::Struct(data) => data.values().iter().any(has_decimal),
                _ => false,
            }
        }
        let Some(Scalar::Struct(data)) = stats else {
            return None;
        };
        let index = data.fields().iter().position(|f| f.name() == name)?;
        let value = data.values().get(index)?;
        (!value.is_null() && !has_decimal(value)).then(|| value.to_array(1).ok())?
    }

    /// A partition key column gets no bound from the stats. Pruning uses its value.
    #[tokio::test]
    async fn a_partition_key_column_takes_no_log_bound() {
        use crate::integrated::delta_table::merge::test::{
            partitioned_fixture_table, partitioned_key_relation,
        };

        let dir = TempDir::new().unwrap();
        let table = partitioned_fixture_table(&dir, &[(1, "a"), (2, "b")]).await;
        let encoder = KeyEncoder::new(&partitioned_key_relation(), &arrow_schema()).unwrap();
        let candidates = candidates_from_log(table.snapshot().unwrap(), &encoder, true, |_| {
            HashMap::new()
        });
        assert_eq!(candidates.len(), 2);
        let payload = encoder
            .column_names()
            .iter()
            .position(|n| n == "payload")
            .unwrap();
        for candidate in &candidates {
            assert!(candidate.min_of(payload).is_none() && candidate.max_of(payload).is_none());
        }
    }

    /// ROW bounds stay with the correct file across many chunks. The first files have no stats.
    #[tokio::test]
    async fn row_bounds_stay_aligned_across_chunks() {
        // 2048 files with stats fill the last chunk. 2497 files leave it part full.
        for files in [2051, 2500] {
            row_bounds_case(files).await;
        }
    }

    async fn row_bounds_case(files: i64) {
        use deltalake::kernel::{Action, Add, DataType as D, StructType};

        let row = StructType::try_new([StructField::new("a", D::INTEGER, true)]).unwrap();
        let dir = TempDir::new().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns([
                StructField::new("id", D::LONG, true),
                StructField::new("row", D::Struct(Box::new(row)), true),
            ])
            .await
            .unwrap();
        // Only the log is read, so the data files do not have to exist.
        let adds = (0..files)
            .map(|f| {
                let stats = (f >= 3).then(|| {
                    serde_json::json!({
                        "numRecords": 1,
                        "minValues": {"id": f, "row": {"a": f}},
                        "maxValues": {"id": f, "row": {"a": f}},
                    })
                    .to_string()
                });
                Action::Add(Add {
                    path: format!("f{f}.parquet"),
                    size: 1,
                    data_change: true,
                    stats,
                    ..Default::default()
                })
            })
            .collect();
        crate::integrated::delta_table::merge::test::commit(&mut table, adds).await;

        let names = ["id".to_string(), "row".to_string()];
        let mut usable = std::collections::BTreeSet::new();
        assert_bounds_match(&table, &names, &mut usable, &format!("{files} files"));
        assert_eq!(usable, std::collections::BTreeSet::from(names));
    }

    /// One file with all key types. Even files have whole-ms timestamps. File 2 has a null string.
    fn typed_batch(schema: &arrow::datatypes::SchemaRef, f: i64) -> RecordBatch {
        use arrow::array::{
            BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array,
            Int32Array, ListArray, StructArray, TimestampMicrosecondArray,
        };
        use arrow::buffer::OffsetBuffer;

        let v: Vec<i64> = (0..3).map(|r| f * 10 + r).collect();
        // Some times are before 1970 to test the round-up of negative values.
        let micros: Vec<i64> = v
            .iter()
            .map(|v| (v - 50) * 1_000_000 + if f % 2 == 0 { 0 } else { 123 })
            .collect();
        let int32 = || Arc::new(Int32Array::from_iter_values(v.iter().map(|v| *v as i32)));
        let binary = || {
            Arc::new(BinaryArray::from_iter_values(
                v.iter().map(|v| vec![*v as u8]),
            ))
        };
        let child_fields = |name: &str| match schema.field_with_name(name).unwrap().data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => panic!("{name} is {other}"),
        };
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(v.clone())),
            int32(),
            Arc::new(Int16Array::from_iter_values(v.iter().map(|v| *v as i16))),
            Arc::new(Int8Array::from_iter_values(
                v.iter().map(|v| (*v % 120) as i8),
            )),
            Arc::new(StringArray::from_iter(
                v.iter().map(|v| (f != 2).then(|| format!("s{v:05}"))),
            )),
            Arc::new(Date32Array::from_iter_values(v.iter().map(|v| *v as i32))),
            Arc::new(TimestampMicrosecondArray::from(micros.clone()).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(micros.clone())),
            Arc::new(BooleanArray::from_iter(v.iter().map(|v| Some(v % 2 == 0)))),
            binary(),
            Arc::new(
                Decimal128Array::from_iter_values(v.iter().map(|v| *v as i128 * 1000 + 5))
                    .with_precision_and_scale(10, 3)
                    .unwrap(),
            ),
            Arc::new(StructArray::new(
                child_fields("row"),
                vec![
                    int32(),
                    Arc::new(StringArray::from_iter_values(
                        v.iter().map(|v| format!("r{v}")),
                    )),
                    Arc::new(TimestampMicrosecondArray::from(micros.clone())),
                ],
                None,
            )),
            Arc::new(StructArray::new(
                child_fields("rowbin"),
                vec![int32(), binary()],
                None,
            )),
            Arc::new(StructArray::new(
                child_fields("rowdec"),
                vec![
                    int32(),
                    Arc::new(
                        Decimal128Array::from_iter_values(v.iter().map(|v| *v as i128))
                            .with_precision_and_scale(10, 3)
                            .unwrap(),
                    ),
                ],
                None,
            )),
            {
                let DataType::List(item) = schema.field_with_name("arr").unwrap().data_type()
                else {
                    panic!("arr is not a list")
                };
                Arc::new(ListArray::new(
                    item.clone(),
                    OffsetBuffer::from_lengths([1, 1, 1]),
                    int32(),
                    None,
                ))
            },
            Arc::new(Int64Array::from(v.clone())),
            Arc::new(Int64Array::from(v.clone())),
        ];
        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    /// The new bounds equal the old bounds for each key type and file, after each table change.
    #[tokio::test]
    async fn log_bounds_match_the_per_file_view() {
        use deltalake::kernel::{ArrayType, DataType as D, PrimitiveType as P, StructType};

        let leaf = |name: &str, t: D| StructField::new(name, t, true);
        let columns = vec![
            leaf("id", D::LONG),
            leaf("i", D::INTEGER),
            leaf("sm", D::SHORT),
            leaf("ti", D::BYTE),
            leaf("str", D::STRING),
            leaf("d", D::DATE),
            leaf("ts", D::TIMESTAMP),
            leaf("tsn", D::TIMESTAMP_NTZ),
            leaf("b", D::BOOLEAN),
            leaf("bin", D::BINARY),
            leaf("dec", D::Primitive(P::decimal(10, 3).unwrap())),
            leaf(
                "row",
                D::Struct(Box::new(
                    StructType::try_new([
                        leaf("a", D::INTEGER),
                        leaf("s", D::STRING),
                        leaf("t", D::TIMESTAMP_NTZ),
                    ])
                    .unwrap(),
                )),
            ),
            leaf(
                "rowbin",
                D::Struct(Box::new(
                    StructType::try_new([leaf("a", D::INTEGER), leaf("bin", D::BINARY)]).unwrap(),
                )),
            ),
            leaf(
                "rowdec",
                D::Struct(Box::new(
                    StructType::try_new([
                        leaf("a", D::INTEGER),
                        leaf("x", D::Primitive(P::decimal(10, 3).unwrap())),
                    ])
                    .unwrap(),
                )),
            ),
            leaf("arr", D::Array(Box::new(ArrayType::new(D::INTEGER, true)))),
            leaf("dot.ted", D::LONG),
            leaf("far", D::LONG),
        ];
        let names: Vec<String> = columns.iter().map(|c| c.name().to_string()).collect();

        let dir = TempDir::new().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns(columns)
            // The kernel counts `arr` as a column. So `far` gets no stats.
            .with_configuration_property(
                deltalake::TableProperty::DataSkippingNumIndexedCols,
                Some("20"),
            )
            .await
            .unwrap();
        let schema = table.snapshot().unwrap().snapshot().arrow_schema();
        for f in 0..8 {
            if f == 5 {
                deltalake::checkpoints::create_checkpoint(&table)
                    .await
                    .unwrap();
            }
            table = table.write(vec![typed_batch(&schema, f)]).await.unwrap();
        }
        let mut usable = std::collections::BTreeSet::<String>::new();
        assert_bounds_match(&table, &names, &mut usable, "after the writes");
        // Make sure that the test compares real bounds, not only `None`.
        let expected = [
            "b", "d", "dot.ted", "i", "id", "row", "rowbin", "sm", "str", "ti", "ts", "tsn",
        ];
        assert_eq!(
            usable,
            std::collections::BTreeSet::from(expected.map(String::from))
        );

        let reopen = |table: &DeltaTable| table.table_url().clone();
        let table = deltalake::open_table(reopen(&table)).await.unwrap();
        assert_bounds_match(&table, &names, &mut usable, "after a reopen");

        // DELETE removes the file with the row and adds a new file.
        let (table, _) = table.delete().with_predicate("id = 13").await.unwrap();
        assert_bounds_match(&table, &names, &mut usable, "after a delete");

        // Some writers add a file without stats.
        let mut table = table;
        let first = table.snapshot().unwrap().log_data().iter().next().unwrap();
        std::fs::copy(
            dir.path().join(first.path().as_ref()),
            dir.path().join("no-stats.parquet"),
        )
        .unwrap();
        let add = deltalake::kernel::Add {
            path: "no-stats.parquet".into(),
            size: first.size(),
            modification_time: first.modification_time(),
            data_change: true,
            stats: None,
            ..Default::default()
        };
        crate::integrated::delta_table::merge::test::commit(
            &mut table,
            vec![deltalake::kernel::Action::Add(add)],
        )
        .await;
        assert_bounds_match(&table, &names, &mut usable, "after an add without stats");

        let (mut table, metrics) = table.optimize().await.unwrap();
        assert!(metrics.num_files_removed > 0, "OPTIMIZE rewrote nothing");
        assert_bounds_match(&table, &names, &mut usable, "after OPTIMIZE");

        for f in 8..10 {
            table = table.write(vec![typed_batch(&schema, f)]).await.unwrap();
        }
        assert_bounds_match(&table, &names, &mut usable, "after writes past OPTIMIZE");

        let (table, _) = crate::integrated::delta_table::merge::test::vacuum_everything(
            table,
            deltalake::operations::vacuum::VacuumMode::Full,
        )
        .await;
        assert_bounds_match(&table, &names, &mut usable, "after VACUUM");
        let table = deltalake::open_table(reopen(&table)).await.unwrap();
        assert_bounds_match(&table, &names, &mut usable, "after the final reopen");
    }

    /// Make sure that the new and old bounds are equal. Record the columns that have bounds.
    fn assert_bounds_match(
        table: &DeltaTable,
        names: &[String],
        usable: &mut std::collections::BTreeSet<String>,
        step: &str,
    ) {
        let snapshot = table.snapshot().unwrap();
        let schema = snapshot.snapshot().arrow_schema();
        let mut keys: Vec<Vec<String>> = names.iter().map(|n| vec![n.clone()]).collect();
        keys.push(vec!["id".into(), "str".into(), "ts".into(), "row".into()]);
        let files: Vec<LogicalFileView> = snapshot.log_data().iter().collect();
        for key in keys {
            let relation = Relation {
                name: SqlIdentifier::new("k", false),
                fields: key
                    .iter()
                    .map(|n| Field::new(n.as_str().into(), ColumnType::bigint(true)))
                    .collect(),
                materialized: false,
                properties: Default::default(),
                primary_key: None,
            };
            let Ok(encoder) = KeyEncoder::new(&relation, &schema) else {
                continue;
            };
            let candidates = candidates_from_log(snapshot, &encoder, true, |_| HashMap::new());
            assert_eq!(candidates.len(), files.len(), "{step}");
            for (candidate, file) in candidates.iter().zip(&files) {
                assert_eq!(candidate.path, file.path(), "{step}");
                for (index, name) in key.iter().enumerate() {
                    let old_min = per_file_bound(file.min_values(), name);
                    let old_max = per_file_bound(file.max_values(), name);
                    let new_min = candidate.min_of(index);
                    let new_max = candidate.max_of(index);
                    assert_eq!(
                        new_min.as_deref().map(|a| a.to_data()),
                        old_min.as_deref().map(|a| a.to_data()),
                        "{step}: min of {name} in {}",
                        file.path()
                    );
                    assert_eq!(
                        new_max.as_deref().map(|a| a.to_data()),
                        old_max.as_deref().map(|a| a.to_data()),
                        "{step}: max of {name} in {}",
                        file.path()
                    );
                    if new_min.is_some() && new_max.is_some() {
                        usable.insert(name.clone());
                    }
                    if name == "id" {
                        // Make sure that each file with stats gets a bound.
                        let has_stats = file.num_records().is_some();
                        assert_eq!(new_min.is_some(), has_stats, "{step}: id");
                    }
                }
            }
        }
    }

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

    fn chunk_of(values: &[i64]) -> Arc<LookupChunk> {
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&encode_keys(values)).unwrap();
        chunk.sort();
        Arc::new(chunk)
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
    fn chunk_of_opt(values: &[Option<i64>]) -> Arc<LookupChunk> {
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
        Arc::new(chunk)
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

    fn candidates_of(table: &DeltaTable) -> Arc<Vec<Candidate>> {
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();
        let mut candidates = candidates_from_log(table.snapshot().unwrap(), &encoder, true, |_| {
            HashMap::new()
        });
        candidates.sort_by(|a, b| a.path.cmp(&b.path));
        Arc::new(candidates)
    }

    /// Build a table holding `ids` as one data file.
    async fn table_with(dir: &TempDir, ids: &[i64]) -> (DeltaTable, Arc<Vec<Candidate>>) {
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
    async fn table_with_files(
        dir: &TempDir,
        files: &[&[i64]],
    ) -> (DeltaTable, Arc<Vec<Candidate>>) {
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
    ) -> (DeltaTable, Arc<Vec<Candidate>>) {
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
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        let chunk = chunk_of(&[20, 40, 99]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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

    /// A file the object store cannot serve is transient, and is retried before giving up.
    ///
    /// Classification decides the connector's behaviour: transient lets the caller retry the
    /// flush, deterministic stops it. A missing object is the shape an S3 outage takes.
    #[tokio::test]
    async fn an_unreadable_file_is_transient_and_retried() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[10, 20, 30, 40]).await;
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        std::fs::remove_file(dir.path().join(&candidates[0].path)).unwrap();

        let error = locate(
            chunk_of(&[20]),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
            4,
            Pruning::new(true, None),
            &mut Tombstones::new(),
        )
        .await
        .unwrap_err();

        assert!(matches!(error, WriteError::Transient(_)), "{error:?}");
        // Names the budget rather than timing the backoff, which would be flaky.
        assert!(
            error
                .to_string()
                .contains(&format!("after {IO_ATTEMPTS} attempts")),
            "{error}"
        );
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
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        let chunk = chunk_of(&[1500, 2999]);

        let mut tombstones = Tombstones::new();
        locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        let chunk = chunk_of(&[20]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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
    async fn table_with_opt(
        dir: &TempDir,
        ids: &[Option<i64>],
    ) -> (DeltaTable, Arc<Vec<Candidate>>) {
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
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        let chunk = chunk_of_opt(&[None]);

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        let chunk = chunk_of(&[11, 12]);

        let mut pruned_tombstones = Tombstones::new();
        let pruned = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
            4,
            Pruning::new(true, None),
            &mut pruned_tombstones,
        )
        .await
        .unwrap();

        let mut full_tombstones = Tombstones::new();
        locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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

    /// The probe reports the key bytes it read, and pruning is what moves that number.
    ///
    /// It is the flush's read amplification: a lookup that cannot prune reads the key column
    /// of the whole table on every flush, which is the cost that dominates a large table. A
    /// count that ignored the row group selection, or that included the value columns, would
    /// misreport exactly the quantity an operator would act on.
    #[tokio::test]
    async fn the_probe_reports_the_key_bytes_it_read() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let (table, candidates) = table_with_row_groups(&dir, &ids, 100).await;
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        // Keys in two of the ten groups, so pruning leaves a fifth of the file to read.
        let chunk = chunk_of(&[850, 999]);

        let read = |on_stats| {
            let (table, candidates, encoder, chunk) =
                (&table, candidates.clone(), encoder.clone(), chunk.clone());
            async move {
                let mut tombstones = Tombstones::new();
                locate(
                    chunk,
                    candidates,
                    table.object_store(),
                    encoder,
                    1,
                    Pruning::new(on_stats, None),
                    &mut tombstones,
                )
                .await
                .unwrap()
            }
        };

        let whole = read(false).await;
        let pruned = read(true).await;

        assert_eq!(whole.row_groups_scanned, 10);
        assert_eq!(pruned.row_groups_scanned, 2);
        assert!(pruned.key_bytes_read > 0, "{pruned:?}");
        assert!(
            pruned.key_bytes_read * 3 < whole.key_bytes_read,
            "reading two of ten row groups must cost well under a third of the whole              key column: {} against {}",
            pruned.key_bytes_read,
            whole.key_bytes_read
        );

        // Only the key columns: the file also stores `payload`, which the probe never reads.
        let file_bytes: u64 = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
            .map(|e| e.metadata().unwrap().len())
            .sum();
        assert!(
            whole.key_bytes_read * 2 < file_bytes,
            "the whole-file read counted more than the key column: {} of {file_bytes} bytes",
            whole.key_bytes_read
        );
    }

    /// Row group pruning must skip groups and still count ordinals over every row.
    ///
    /// Counting only the rows read shifts every later ordinal, so the tombstones land on
    /// rows nobody asked about. Reading everything is the control: it fixes the expected
    /// ordinals without the pruning path, so both paths cannot be wrong the same way.
    #[tokio::test]
    async fn row_group_pruning_keeps_ordinals_physical() {
        let dir = TempDir::new().unwrap();
        let ids: Vec<i64> = (0..1000).collect();
        let (table, candidates) = table_with_row_groups(&dir, &ids, 100).await;
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());
        // Keys in the last two groups only, so eight of ten groups must be skipped.
        let chunk = chunk_of(&[850, 999]);

        let mut pruned = Tombstones::new();
        let metrics = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
            1,
            Pruning::new(true, None),
            &mut pruned,
        )
        .await
        .unwrap();

        let mut full = Tombstones::new();
        let full_metrics = locate(
            chunk.clone(),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
            1,
            Pruning::none(),
            &mut full,
        )
        .await
        .unwrap();

        assert_eq!(metrics.row_groups_scanned, 2, "{metrics:?}");
        assert_eq!(metrics.row_groups_pruned, 8, "{metrics:?}");
        assert_eq!(full_metrics.row_groups_scanned, 10, "{full_metrics:?}");
        assert_eq!(
            tombstone_summary(&pruned, &candidates),
            tombstone_summary(&full, &candidates),
            "pruning changed which rows were located"
        );
        assert_eq!(
            pruned
                .ordinals_for(&candidates[0].path)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![850, 999],
            "an ordinal shifted by the skipped row groups"
        );
    }

    #[tokio::test]
    async fn empty_chunk_reads_nothing() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[1, 2]).await;
        let encoder = Arc::new(KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap());

        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.sort();
        let mut tombstones = Tombstones::new();
        let metrics = locate(
            Arc::new(chunk),
            candidates.clone(),
            table.object_store(),
            encoder.clone(),
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

    /// A decimal key beyond `f64` precision must still be found. delta-rs rounds its log
    /// bound, so pruning on it would skip the file and leave the old row live.
    #[tokio::test]
    async fn a_wide_decimal_key_is_not_pruned_away() {
        use arrow::array::Decimal128Array;
        use deltalake::kernel::{DataType as DeltaDataType, DecimalType, StructField};
        use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};

        let key = 100_000_000_000_000_000_000i128;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Decimal128(38, 0), false),
        ]));
        let dir = TempDir::new().unwrap();
        let table = CreateBuilder::new()
            .with_location(dir.path().to_str().unwrap())
            .with_columns([StructField::new(
                "id",
                DeltaDataType::Primitive(deltalake::kernel::PrimitiveType::Decimal(
                    DecimalType::try_new(38, 0).unwrap(),
                )),
                false,
            )])
            .await
            .unwrap();
        let ids: ArrayRef = Arc::new(
            Decimal128Array::from(vec![key])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(schema.clone(), vec![ids.clone()]).unwrap();
        let table = table.write(vec![batch]).await.unwrap();

        let relation = Relation {
            name: SqlIdentifier::new("k", false),
            fields: vec![Field::new("id".into(), ColumnType::decimal(38, 0, false))],
            materialized: false,
            properties: Default::default(),
            primary_key: None,
        };
        let encoder = Arc::new(KeyEncoder::new(&relation, &schema).unwrap());
        let candidates: Arc<Vec<Candidate>> =
            candidates_from_log(table.snapshot().unwrap(), &encoder, true, |_| {
                Default::default()
            })
            .into();
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk
            .extend(&encoder.encode_columns(&[ids]).unwrap())
            .unwrap();
        chunk.sort();

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            Arc::new(chunk),
            candidates,
            table.object_store(),
            encoder,
            4,
            Pruning::new(true, None),
            &mut tombstones,
        )
        .await
        .unwrap();
        assert_eq!(metrics.rows_located, 1, "{metrics:?}");
    }
}
