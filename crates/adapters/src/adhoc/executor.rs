use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::IpcSchemaEncoder;
use arrow::ipc::writer::StreamWriter;
use arrow::util::pretty::pretty_format_batches;
use arrow_json::WriterBuilder;
use arrow_json::writer::LineDelimited;
use async_stream::{stream, try_stream};
use bytes::{BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SendableRecordBatchStream;
use feldera_types::query::MAX_WS_FRAME_SIZE;
use futures::stream::Stream;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::{StreamExt, select};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};

use super::format;
use crate::PipelineError;

/// We execute the dataframe in our dbsp tokio runtime. The reason is that this runtime will
/// have a multi-threaded scheduler that can run things on many cores with work-stealing, whereas
/// the actix-web runtime is single-threaded. This is important for datafusion because it can
/// parallelize query execution.
fn execute_stream(df: DataFrame) -> Receiver<DFResult<SendableRecordBatchStream>> {
    let (tx, rx) = oneshot::channel();
    dbsp::circuit::tokio::TOKIO.spawn(async move {
        let _r = tx.send(df.execute_stream().await);
    });

    rx
}

/// Drives physical planning and execution setup for `df`, returning the
/// record-batch stream together with its schema.
///
/// Planning and execute-time-setup errors — e.g. selecting from a
/// non-materialized table — surface here, before any result data is produced.
/// Hoisting this step out of the per-format encoders lets the HTTP handler
/// translate such an error into a proper 4xx response
/// instead of a `200 OK` whose body is silently truncated.
pub(crate) async fn execute_adhoc_stream(
    df: DataFrame,
) -> Result<(SendableRecordBatchStream, SchemaRef), PipelineError> {
    let schema = df.schema().inner().clone();
    let stream = execute_stream(df)
        .await
        .map_err(|e| PipelineError::AdHocQueryError {
            error: e.to_string(),
            df: None,
        })?
        .map_err(|e| PipelineError::AdHocQueryError {
            error: e.to_string(),
            df: Some(Box::new(e)),
        })?;
    Ok((stream, schema))
}

pub(crate) fn infallible_from_bytestring(
    fallible_stream: impl Stream<Item = Result<ByteString, PipelineError>>,
    map_err: impl Fn(PipelineError) -> Bytes + 'static,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    fallible_stream.map(move |r| {
        Ok(match r {
            Ok(bytes) => bytes.into_bytes(),
            Err(e) => map_err(e),
        })
    })
}

pub(crate) fn stream_text_query(
    mut stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> impl Stream<Item = Result<ByteString, PipelineError>> {
    try_stream! {
        let mut headers_sent = false;
        let mut last_line: Option<String> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(PipelineError::from)?;
            let txt_table = format::create_table(&[batch])
                .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None })?;
            let txt_table = txt_table.to_string();
            let pretty_results_lines: Vec<&str> = txt_table.lines().skip(if headers_sent { 3 } else { 0 }).collect::<Vec<&str>>();
            if let Some((last_str, other_lines)) = pretty_results_lines.split_last() {
                last_line = Some(last_str.to_string());
                let mut pretty_results_adjusted = other_lines.join("\n");
                pretty_results_adjusted.push('\n');
                yield pretty_results_adjusted.into();
            }
            headers_sent = true;
        }
        if let Some(ll) = last_line {
            yield ll.into();
        }

        // For some queries df.execute_stream() won't yield any batches
        // in case there aren't any results. When this happens we never sent the headers.
        // We correct it here and send an empty batch.
        // This isn't a problem in JSON. And in parquet the file writer will
        // produce an empty file with a schema by default.
        if !headers_sent {
            let batch = RecordBatch::new_empty(schema);
            let txt_table = pretty_format_batches(&[batch])
                .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None })?;
            let txt_table = txt_table.to_string();
            yield txt_table.into();
        }
    }
}

/// Incremental, order-independent hasher for record batches.
///
/// Uses DataFusion's `create_hashes` (ahash) per batch with two independent
/// seeds for ~128-bit collision resistance. Row hashes are combined via
/// wrapping u64 addition (commutative), so no sorting is required. Memory
/// usage is O(batch_size).
struct BatchHasher {
    rs1: ahash::RandomState,
    rs2: ahash::RandomState,
    buf1: Vec<u64>,
    buf2: Vec<u64>,
    acc1: u64,
    acc2: u64,
    row_count: u64,
}

impl BatchHasher {
    fn new() -> Self {
        Self {
            rs1: ahash::RandomState::with_seeds('M' as u64, 'U' as u64, 'A' as u64, 'Y' as u64),
            rs2: ahash::RandomState::with_seeds('T' as u64, 'H' as u64, 'A' as u64, 'I' as u64),
            buf1: Vec::new(),
            buf2: Vec::new(),
            acc1: 0,
            acc2: 0,
            row_count: 0,
        }
    }

    /// Feeds one batch into the running hash. Can be called repeatedly.
    fn update(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let n = batch.num_rows();
        self.buf1.clear();
        self.buf1.resize(n, 0);
        self.buf2.clear();
        self.buf2.resize(n, 0);
        create_hashes(batch.columns(), &self.rs1, &mut self.buf1)?;
        create_hashes(batch.columns(), &self.rs2, &mut self.buf2)?;
        for i in 0..n {
            self.acc1 = self.acc1.wrapping_add(self.buf1[i]);
            self.acc2 = self.acc2.wrapping_add(self.buf2[i]);
        }
        self.row_count += n as u64;
        Ok(())
    }

    /// Produces the final hex-encoded SHA-256 digest over schema, row count,
    /// and the two accumulated hashes.
    fn finalize(self, schema: &arrow::datatypes::Schema) -> String {
        let mut hasher = Sha256::new();
        let schema_bytes = IpcSchemaEncoder::new()
            .schema_to_fb(schema)
            .finished_data()
            .to_vec();
        hasher.update(&schema_bytes);
        hasher.update(self.row_count.to_le_bytes());
        hasher.update(self.acc1.to_le_bytes());
        hasher.update(self.acc2.to_le_bytes());
        format!("{:X}", hasher.finalize())
    }
}

/// Computes an order-independent hash of a DataFrame's result set.
pub(crate) async fn hash_query_result(
    mut stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> Result<String, PipelineError> {
    let mut hasher = BatchHasher::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(PipelineError::from)?;
        hasher.update(&batch).map_err(PipelineError::from)?;
    }
    Ok(hasher.finalize(&schema))
}

pub(crate) fn stream_json_query(
    mut stream: SendableRecordBatchStream,
) -> impl Stream<Item = Result<ByteString, PipelineError>> {
    try_stream! {
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(PipelineError::from)?;
            let mut buf = Vec::with_capacity(4096);
            let builder = WriterBuilder::new().with_explicit_nulls(true);
            let mut writer = builder.build::<_, LineDelimited>(&mut buf);
            writer.write(&batch).map_err(DataFusionError::from).map_err(PipelineError::from)?;
            writer.finish().map_err(DataFusionError::from).map_err(PipelineError::from)?;
            yield buf.try_into().map_err(|_| PipelineError::AdHocQueryError {
                error: "Failed to encode query result buffer as UTF-8".to_string(),
                df: None,
            })?;
        }
    }
}

/// Async byte sink used by the parquet writer.
///
/// `parquet::arrow::AsyncArrowWriter` drives this through the
/// `AsyncFileWriter` trait, awaiting each `write` future before issuing the
/// next, so byte ordering is preserved.
struct ChannelWriter {
    tx: mpsc::Sender<Bytes>,
}

impl ChannelWriter {
    fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { tx }
    }
}

impl AsyncFileWriter for ChannelWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        let tx = self.tx.clone();
        async move {
            tx.send(bs)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))?;
            Ok(())
        }
        .boxed()
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        async move { Ok(()) }.boxed()
    }
}

/// Streams an ad-hoc query as Arrow IPC stream-format bytes.
///
/// Encodes one record batch at a time into an in-memory `Vec<u8>` via the
/// synchronous `arrow::ipc::writer::StreamWriter`, then yields the buffer
/// as a single `Bytes` chunk. Memory is bounded by one record batch.
///
/// arrow-rs does not yet ship an async IPC stream writer; see
/// <https://github.com/apache/arrow-rs/issues/7812>,
/// <https://github.com/apache/arrow-rs/issues/9212>, and
/// <https://github.com/apache/arrow-rs/pull/9241>. Once that lands the
/// per-batch buffering here can be replaced with a direct async sink.
pub(crate) fn stream_arrow_query(
    mut stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> impl Stream<Item = Result<Bytes, DataFusionError>> {
    try_stream! {
        // `try_new` writes the schema message to the inner buffer. The
        // `BytesMut` behind `BufMut::writer()` is a single allocation
        // that we slice off in O(1) chunks via `split()` after each
        // synchronous write: encoded batches share that backing storage
        // until it fills, and each `freeze()` hands the receiver a
        // tight `Bytes` view without an extra allocation. The
        // `.get_mut().get_mut()` chain reaches through the `bytes::buf::Writer`
        // wrapper into the underlying `BytesMut`.
        const CHUNK_CAPACITY: usize = 64 * 1024;
        let mut writer = StreamWriter::try_new(
            BytesMut::with_capacity(CHUNK_CAPACITY).writer(),
            &schema,
        )
        .map_err(DataFusionError::from)?;
        let header = writer.get_mut().get_mut().split();
        if !header.is_empty() {
            yield header.freeze();
        }

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            writer.write(&batch).map_err(DataFusionError::from)?;
            let chunk = writer.get_mut().get_mut().split();
            if !chunk.is_empty() {
                yield chunk.freeze();
            }
        }

        writer.finish().map_err(DataFusionError::from)?;
        let tail = writer.into_inner().map_err(DataFusionError::from)?.into_inner();
        if !tail.is_empty() {
            yield tail.freeze();
        }
    }
}

pub(crate) fn stream_parquet_query(
    mut stream: SendableRecordBatchStream,
    schema: SchemaRef,
) -> impl Stream<Item = Result<Bytes, DataFusionError>> {
    // Should probably be smaller than `MAX_WS_FRAME_SIZE`.
    const PARQUET_CHUNK_SIZE: usize = MAX_WS_FRAME_SIZE / 2;

    // Create a channel to communicate between the parquet writer and the HTTP response
    let (tx, mut rx) = mpsc::channel(1024);

    let mut stream_job = Box::pin(
        async move {
            let mut writer = AsyncArrowWriter::try_new(
                ChannelWriter::new(tx),
                schema,
                Some(
                    WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .build(),
                ),
            )?;
            while let Some(batch) = stream.next().await.transpose()? {
                writer.write(&batch).await?;
                if writer.in_progress_size() > PARQUET_CHUNK_SIZE {
                    writer.flush().await?;
                }
            }
            writer.flush().await?;
            writer.close().await?;
            <datafusion::common::Result<_>>::Ok(())
        }
        .fuse(),
    );

    stream! {
        loop {
            select! {
                stream_res = stream_job.as_mut() => {
                    match stream_res {
                        Ok(()) => {}
                        Err(err) => {
                            yield Err(err);
                        }
                    }
                },
                maybe_bytes = rx.recv().fuse() => {
                    if let Some(bytes) = maybe_bytes {
                        yield Ok(bytes);
                    } else {
                        // Channel closed, we're done
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn hash_batches(schema: &Schema, batches: &[RecordBatch]) -> String {
        let mut hasher = BatchHasher::new();
        for batch in batches {
            hasher.update(batch).unwrap();
        }
        hasher.finalize(schema)
    }

    #[test]
    fn hash_order_independent() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
            Field::new("num", DataType::Int32, false),
        ]);
        let asc = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let desc = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
                Arc::new(Int32Array::from(vec![30, 10, 20])),
            ],
        )
        .unwrap();
        assert_eq!(
            hash_batches(&schema, &[asc]),
            hash_batches(&schema, &[desc])
        );
    }

    #[test]
    fn hash_batch_boundary_independent() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]);
        let full = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let p1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let p2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["b", "c"])),
            ],
        )
        .unwrap();
        assert_eq!(
            hash_batches(&schema, &[full]),
            hash_batches(&schema, &[p1, p2])
        );
    }

    #[test]
    fn hash_distinguishes_duplicates() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let one = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let two = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 1]))],
        )
        .unwrap();
        assert_ne!(hash_batches(&schema, &[one]), hash_batches(&schema, &[two]));
    }

    #[test]
    fn hash_different_data() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let b1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        assert_ne!(hash_batches(&schema, &[b1]), hash_batches(&schema, &[b2]));
    }

    #[test]
    fn hash_empty_result() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
        let h_empty = hash_batches(&schema, &[empty]);
        assert!(!h_empty.is_empty());

        let one = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        assert_ne!(h_empty, hash_batches(&schema, &[one]));
    }

    /// Drives a synthetic DataFusion query through `stream_arrow_query`,
    /// collects the streamed Arrow IPC bytes, and decodes them back. Each
    /// iteration creates fresh state so we don't accidentally test caching.
    ///
    /// The synthetic query selects from a five-batch in-memory table; this
    /// shape (multiple small batches) maximises the number of `write_all`
    /// calls the IPC encoder makes, which is what previously surfaced the
    /// per-call ordering race.
    async fn round_trip_via_stream_arrow_query()
    -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
        use arrow::ipc::reader::StreamReader;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from(vec![i, i + 1, i + 2, i + 3, i + 4])),
                        Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "eeeee"])),
                    ],
                )
                .unwrap()
            })
            .collect();
        let mem = MemTable::try_new(schema, vec![batches]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        let df = ctx.sql("SELECT * FROM t ORDER BY id").await.unwrap();

        let (record_stream, schema) = execute_adhoc_stream(df)
            .await
            .expect("execute_adhoc_stream failed to set up the query");
        let mut buf = Vec::<u8>::new();
        let mut stream = Box::pin(stream_arrow_query(record_stream, schema));
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk.expect("stream_arrow_query yielded an error"));
        }

        let reader = StreamReader::try_new(buf.as_slice(), None)?;
        reader.collect::<Result<Vec<_>, _>>()
    }

    /// Round-trips one query through `stream_arrow_query` + `StreamReader`.
    #[test]
    fn stream_arrow_query_decodes_cleanly() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let decoded = rt
            .block_on(round_trip_via_stream_arrow_query())
            .expect("stream_arrow_query output failed to parse");
        let rows: usize = decoded.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 25, "expected exactly 25 rows in the result");
        for batch in &decoded {
            assert_eq!(batch.num_columns(), 2);
        }
    }

    /// Regression check for #4287: 200 round trips on a 4-worker runtime,
    /// every decoded stream must parse cleanly.
    #[test]
    fn stream_arrow_query_is_stable_under_contention() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for i in 0..200 {
                round_trip_via_stream_arrow_query()
                    .await
                    .unwrap_or_else(|e| panic!("iteration {i} failed: {e}"));
            }
        });
    }

    /// Empty result must still produce a parseable Arrow IPC stream: the
    /// schema header is emitted, the batch loop is skipped, and
    /// `writer.finish()` writes the end-of-stream marker.
    #[test]
    fn stream_arrow_query_handles_empty_result() {
        use arrow::ipc::reader::StreamReader;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let mem = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(mem)).unwrap();
            let df = ctx.sql("SELECT * FROM t").await.unwrap();

            let (record_stream, schema) = execute_adhoc_stream(df)
                .await
                .expect("execute_adhoc_stream failed to set up the query");
            let mut buf = Vec::<u8>::new();
            let mut stream = Box::pin(stream_arrow_query(record_stream, schema));
            while let Some(chunk) = stream.next().await {
                buf.extend_from_slice(&chunk.expect("stream_arrow_query yielded an error"));
            }

            let reader = StreamReader::try_new(buf.as_slice(), None)
                .expect("empty-result stream must carry a valid schema header");
            let batches: Vec<RecordBatch> = reader
                .collect::<Result<Vec<_>, _>>()
                .expect("empty-result stream must decode without framing errors");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 0);
            for batch in &batches {
                assert_eq!(batch.num_columns(), 1);
            }
        });
    }
}
