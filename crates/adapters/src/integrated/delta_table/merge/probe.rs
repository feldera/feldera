//! Locating the rows a flush must supersede.
//!
//! A deletion vector addresses rows by file path and physical row ordinal, not by key, so
//! before anything can be tombstoned the connector has to turn a set of keys into a set
//! of (path, ordinal) pairs. That lookup is the only expensive step in a flush, and it is
//! deliberately narrow: it reads the key columns of candidate row groups and nothing
//! else.
//!
//! The keys being sought live in a sorted [`LookupChunk`], so each decoded key is
//! resolved with a binary search and neither side needs an auxiliary hash index. Peak
//! memory is therefore one decoded batch per concurrent task.
//!
//! # Ordinals are physical
//!
//! The ordinals produced here count rows as if no deletion vector were applied, because
//! that is the space a deletion vector addresses. Two things perturb the count and are
//! compensated for: row groups skipped before the one being read, whose row counts come
//! from the footer, and rows already tombstoned by an existing vector, which must not
//! shift the numbering. Nothing here applies a row selection for that reason.

use std::sync::Arc;

use anyhow::{Result as AnyResult, anyhow};
use arrow::array::RecordBatch;
use deltalake::{DeltaTable, ObjectStore, Path};
use futures::StreamExt;
use futures::stream::{self, TryStreamExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::chunk::LookupChunk;
use super::key::KeyEncoder;
use super::tombstone::Tombstones;

/// A data file the lookup may have to read.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Path relative to the table root, as it appears in the log.
    pub path: String,
}

/// What one lookup pass did, for metrics and for the efficiency tests.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProbeMetrics {
    /// Files whose key columns were read.
    pub files_scanned: usize,
    /// Row groups whose key columns were decoded.
    pub row_groups_scanned: usize,
    /// Rows to tombstone. Exceeds the number of keys found when the table holds the same
    /// key in more than one file, which converges once every copy is tombstoned.
    pub rows_located: u64,
    /// Keys no candidate file contained.
    ///
    /// Not an error: a delete of an absent row is a no-op and an update of one becomes a
    /// plain insert. A sustained nonzero rate means the table has diverged from the view.
    pub keys_not_found: u64,
}

/// Locate every key in `chunk`, recording the rows to tombstone in `tombstones`.
///
/// `candidates` is the set of files that survived pruning. Row groups are read
/// concurrently up to `max_concurrent`, which bounds both request concurrency and the
/// number of decoded batches held at once.
///
/// The chunk must already be sorted.
pub async fn locate(
    chunk: &LookupChunk,
    candidates: &[Candidate],
    table: &DeltaTable,
    encoder: &KeyEncoder,
    max_concurrent: usize,
    tombstones: &mut Tombstones,
) -> AnyResult<ProbeMetrics> {
    let mut metrics = ProbeMetrics::default();
    if chunk.is_empty() || candidates.is_empty() {
        metrics.keys_not_found = chunk.len() as u64;
        return Ok(metrics);
    }
    metrics.files_scanned = candidates.len();

    let store = table.object_store();
    let results: Vec<FileHits> = stream::iter(candidates.iter().map(|candidate| {
        let store = store.clone();
        async move { probe_file(chunk, candidate, store, encoder).await }
    }))
    .buffer_unordered(max_concurrent.max(1))
    .try_collect()
    .await?;

    // A key found in two files yields two rows to tombstone but is one key found, so
    // distinct positions are tracked separately from row count.
    let mut found = vec![false; chunk.len()];
    for file in results {
        metrics.row_groups_scanned += file.row_groups;
        for (ordinal, position) in file.hits {
            tombstones.insert(&file.path, ordinal);
            metrics.rows_located += 1;
            found[position] = true;
        }
    }
    metrics.keys_not_found = found.iter().filter(|f| !**f).count() as u64;

    Ok(metrics)
}

/// Rows one file contributed, plus what it cost to find them.
struct FileHits {
    path: String,
    /// `(physical ordinal, position of the key in the sorted chunk)`.
    hits: Vec<(u64, usize)>,
    row_groups: usize,
}

/// Read one file's key columns and return the rows whose key is in `chunk`.
async fn probe_file(
    chunk: &LookupChunk,
    candidate: &Candidate,
    store: Arc<dyn ObjectStore>,
    encoder: &KeyEncoder,
) -> AnyResult<FileHits> {
    let path = Path::from(candidate.path.as_str());
    let reader = ParquetObjectReader::new(store, path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| anyhow!("failed to open Delta data file '{}': {e}", candidate.path))?;

    let mask = key_projection_mask(&builder, encoder);

    // Row counts per row group let the ordinal of a batch's first row be computed without
    // assuming a uniform group size.
    let row_group_rows: Vec<i64> = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .collect();

    let mut stream = builder
        .with_projection(mask)
        .build()
        .map_err(|e| anyhow!("failed to read Delta data file '{}': {e}", candidate.path))?;

    let mut hits = Vec::new();
    let mut ordinal: u64 = 0;
    // Batches arrive in file order and cover every row, since no row selection is
    // applied, so a running counter is the physical ordinal.
    while let Some(batch) = stream.next().await {
        let batch: RecordBatch = batch.map_err(|e| {
            anyhow!(
                "error reading Delta data file '{}' while locating rows: {e}",
                candidate.path
            )
        })?;
        let rows = encoder.encode_batch(&batch)?;
        for i in 0..rows.num_rows() {
            if let Some(position) = chunk.position(rows.row(i).as_ref()) {
                hits.push((ordinal + i as u64, position));
            }
        }
        ordinal += batch.num_rows() as u64;
    }

    let total: i64 = row_group_rows.iter().sum();
    if ordinal != total as u64 {
        // A mismatch means the reader skipped rows, which would silently shift every
        // ordinal after the gap and tombstone the wrong rows.
        return Err(anyhow!(
            "internal error: read {ordinal} rows from '{}' but its footer declares {total}; \
             physical row ordinals would be wrong",
            candidate.path
        ));
    }

    Ok(FileHits {
        path: candidate.path.clone(),
        hits,
        row_groups: row_group_rows.len(),
    })
}

/// Select only the key columns, by name.
///
/// Projecting by leaf rather than by root keeps a `ROW` key from pulling fields it does
/// not need, and matching by name rather than position tolerates a file whose column
/// order differs from the table schema.
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
    use crate::integrated::delta_table::merge::key::KeyEncoder;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow::row::{RowConverter, SortField};
    use deltalake::DeltaOps;
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
    use deltalake::operations::create::CreateBuilder;
    use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier, SqlType};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn key_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("k", false),
            fields: vec![Field::new(
                "id".into(),
                ColumnType {
                    typ: SqlType::BigInt,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                    key: None,
                    value: None,
                },
            )],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    fn arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, true),
            ArrowField::new("payload", DataType::Utf8, true),
        ])
    }

    fn encode_keys(values: &[i64]) -> arrow::row::Rows {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        converter.convert_columns(&[column]).unwrap()
    }

    /// Build a table holding `ids` and return it plus the paths of its data files.
    async fn table_with(dir: &TempDir, ids: &[i64]) -> (DeltaTable, Vec<Candidate>) {
        let uri = dir.path().to_str().unwrap();
        let table = CreateBuilder::new()
            .with_location(uri)
            .with_columns(vec![
                StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Long), true),
                StructField::new(
                    "payload",
                    DeltaDataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema()),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("v{i}")).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let table = DeltaOps(table).write(vec![batch]).await.unwrap();
        let candidates = table
            .snapshot()
            .unwrap()
            .log_data()
            .into_iter()
            .map(|f| Candidate {
                path: f.path().to_string(),
            })
            .collect();
        (table, candidates)
    }

    #[tokio::test]
    async fn locates_present_keys_and_reports_absent_ones() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[10, 20, 30, 40]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();

        // Two present, one absent.
        let rows = encode_keys(&[20, 40, 99]);
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&rows);
        chunk.sort();

        let mut tombstones = Tombstones::new();
        let metrics = locate(
            &chunk,
            &candidates,
            &table,
            &encoder,
            4,
            &mut tombstones,
        )
        .await
        .unwrap();

        assert_eq!(metrics.rows_located, 2);
        assert_eq!(metrics.keys_not_found, 1);
        assert_eq!(tombstones.total_rows(), 2);
        assert_eq!(tombstones.touched_files(), 1);
    }

    #[tokio::test]
    async fn ordinals_are_physical_positions() {
        let dir = TempDir::new().unwrap();
        // The file must span several decoded batches, otherwise a running counter and a
        // per-batch index agree and the test cannot tell them apart. Parquet's default
        // batch size is 1024, so 3000 rows guarantees at least three batches and the
        // targets below land in the second and third.
        let ids: Vec<i64> = (0..3000).collect();
        let (table, candidates) = table_with(&dir, &ids).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();

        let rows = encode_keys(&[1500, 2999]);
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&rows);
        chunk.sort();

        let mut tombstones = Tombstones::new();
        locate(&chunk, &candidates, &table, &encoder, 1, &mut tombstones)
            .await
            .unwrap();

        // Ids were written in order, so a key's ordinal equals its value.
        let found = tombstones.ordinals_for(&candidates[0].path).unwrap();
        assert_eq!(found.iter().collect::<Vec<_>>(), vec![1500, 2999]);
    }

    #[tokio::test]
    async fn empty_chunk_reads_nothing() {
        let dir = TempDir::new().unwrap();
        let (table, candidates) = table_with(&dir, &[1, 2]).await;
        let encoder = KeyEncoder::new(&key_relation(), &arrow_schema()).unwrap();

        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.sort();
        let mut tombstones = Tombstones::new();
        let metrics = locate(&chunk, &candidates, &table, &encoder, 4, &mut tombstones)
            .await
            .unwrap();

        assert_eq!(metrics.files_scanned, 0);
        assert_eq!(metrics.rows_located, 0);
        assert!(tombstones.is_empty());
    }
}
