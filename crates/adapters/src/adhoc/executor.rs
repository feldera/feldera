use arrow::array::RecordBatch;
use arrow::ipc::convert::IpcSchemaEncoder;
use arrow::ipc::writer::StreamWriter;
use arrow::util::pretty::pretty_format_batches;
use arrow_json::WriterBuilder;
use arrow_json::writer::LineDelimited;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use bytestring::ByteString;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SendableRecordBatchStream;
use feldera_storage::tokio::TOKIO;
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
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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
    df: DataFrame,
) -> impl Stream<Item = Result<ByteString, PipelineError>> {
    let schema = df.schema().inner().clone();
    try_stream! {
        let stream_executor = execute_stream(df).await.map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None })?;
        let mut stream = stream_executor
            .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: Some(Box::new(e)) })?;

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
pub(crate) async fn hash_query_result(df: DataFrame) -> Result<String, PipelineError> {
    let schema = df.schema().inner().clone();

    let stream_executor = execute_stream(df)
        .await
        .map_err(|e| PipelineError::AdHocQueryError {
            error: e.to_string(),
            df: None,
        })?;
    let mut stream = stream_executor.map_err(|e| PipelineError::AdHocQueryError {
        error: e.to_string(),
        df: Some(Box::new(e)),
    })?;

    let mut hasher = BatchHasher::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(PipelineError::from)?;
        hasher.update(&batch).map_err(PipelineError::from)?;
    }
    Ok(hasher.finalize(&schema))
}

pub(crate) fn stream_json_query(
    df: DataFrame,
) -> impl Stream<Item = Result<ByteString, PipelineError>> {
    try_stream! {
        let stream_executor = execute_stream(df).await.map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None })?;
        let mut stream = stream_executor
            .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: Some(Box::new(e)) })?;
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

struct ChannelWriter {
    tx: mpsc::Sender<Bytes>,
    handles: Vec<JoinHandle<Result<(), SendError<Bytes>>>>,
}

impl ChannelWriter {
    fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            tx,
            handles: vec![],
        }
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

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Clone the buffer and send it
        let bytes = Bytes::copy_from_slice(buf);
        let len = bytes.len();
        let tx = self.tx.clone();
        let handle = TOKIO.spawn(async move { tx.send(bytes).await });
        self.handles.push(handle);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // It's ok for this to be a no-op, we don't require a flush anywhere.
        //
        // The proper way to implement this is to block until everything in `handles`
        // completed but we can't do that in a sync interface.
        Ok(())
    }
}

pub(crate) fn stream_arrow_query(
    df: DataFrame,
) -> impl Stream<Item = Result<Bytes, DataFusionError>> {
    let (tx, mut rx) = mpsc::channel(1024);

    let mut stream_job = Box::pin(
        async move {
            let mut channel_writer = ChannelWriter::new(tx);
            let schema = df.schema().inner().clone();
            let mut stream = execute_stream(df)
                .await
                .expect("unable to receive stream")?;
            let mut writer = StreamWriter::try_new(&mut channel_writer, &schema).unwrap();

            while let Some(batch) = stream.next().await {
                let batch = batch?;
                writer.write(&batch).map_err(DataFusionError::from)?;
            }
            writer.flush().map_err(DataFusionError::from)?;
            writer.finish().map_err(DataFusionError::from)?;
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

pub(crate) fn stream_parquet_query(
    df: DataFrame,
) -> impl Stream<Item = Result<Bytes, DataFusionError>> {
    // Should probably be smaller than `MAX_WS_FRAME_SIZE`.
    const PARQUET_CHUNK_SIZE: usize = MAX_WS_FRAME_SIZE / 2;

    // Create a channel to communicate between the parquet writer and the HTTP response
    let (tx, mut rx) = mpsc::channel(1024);

    let mut stream_job = Box::pin(
        async move {
            let schema = df.schema().inner().clone();
            let mut stream = execute_stream(df)
                .await
                .expect("unable to receive stream")?;

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
}
