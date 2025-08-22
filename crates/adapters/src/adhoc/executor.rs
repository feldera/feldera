use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use arrow::util::pretty::pretty_format_batches;
use arrow_digest::{RecordDigest, RecordDigestV0};
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use bytestring::ByteString;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::SortExpr;
use datafusion::prelude::col;
use feldera_storage::tokio::TOKIO;
use feldera_types::query::MAX_WS_FRAME_SIZE;
use futures::stream::Stream;
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::{select, StreamExt};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use sha2::Sha256;
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

/// Hashes the result set of a DataFrame query using SHA-256.
pub(crate) async fn hash_query_result(mut df: DataFrame) -> Result<String, PipelineError> {
    let schema = df.schema().inner().clone();

    let fields = df.schema().fields();
    let sort_exprs: Vec<_> = fields
        .iter()
        .map(|f| col(f.name()).sort(true, true))
        .collect();
    df = df.sort(sort_exprs).map_err(PipelineError::from)?;

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

    let mut digest = RecordDigestV0::<Sha256>::new(&schema);
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(PipelineError::from)?;
        digest.update(&batch);
    }

    let hash = digest.finalize();
    Ok(format!("{:X}", hash))
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
