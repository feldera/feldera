use crate::PipelineError;
use actix_web::http::header;
use actix_web::HttpResponse;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_json::writer::LineDelimited;
use arrow_json::WriterBuilder;
use async_stream::{stream, try_stream};
use bytes::Bytes;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::dataframe::DataFrame;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use feldera_types::query::{AdHocResultFormat, AdhocQueryArgs};
use futures_util::future::{BoxFuture, FutureExt};
use futures_util::{select, StreamExt};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::convert::Infallible;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{mpsc, oneshot};

mod format;
pub(crate) mod table;

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

/// Stream the result of an ad-hoc query.
pub async fn stream_adhoc_result(
    args: AdhocQueryArgs,
    session: SessionContext,
) -> Result<HttpResponse, PipelineError> {
    let df = session.sql(&args.sql).await?;
    let schema = df.schema().inner().clone();

    // Note that once we are in the stream!{} macros any error that occurs will lead to the connection
    // in the manager being terminated and a 500 error being returned to the client.
    // We can't return an error in a stream that is already Response::Ok.
    //
    // Sometimes things do tend to fail inside the stream!{} macro, e.g., "select 1/0;" will cause a
    // division by zero error during query execution. So we return errors according to the chosen
    // format for text and json, and for parquet we return the 500 error.
    match args.format {
        AdHocResultFormat::Text => {
            Ok(HttpResponse::Ok()
                .content_type(mime::TEXT_PLAIN)
                .streaming::<_, Infallible>(try_stream! {
                    let stream_exec = match execute_stream(df).await {
                        Ok(res) => res.map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None }),
                        Err(e) => {
                            yield format!("ERROR: {}", e).into();
                            return;
                        }
                    };

                    let mut headers_sent = false;
                    let mut last_line: Option<String> = None;
                    match stream_exec {
                        Ok(mut stream) => {
                            while let Some(batch) = stream.next().await {
                                let batch_result = batch.map_err(PipelineError::from);
                                match batch_result {
                                    Ok(batch) => {
                                        let txt_table_format = format::create_table(&[batch])
                                            .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None });
                                        match txt_table_format {
                                            Ok(txt_table) => {
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
                                            Err(e) => {
                                                yield format!("ERROR: {}", e).into();
                                                return;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        yield format!("ERROR: {}", e).into();
                                        return;
                                    }
                                }
                            }
                            if let Some(last_line) = last_line {
                                yield last_line.into();
                            }
                        }
                        Err(e) => {
                            yield format!("ERROR: {}", e).into();
                            return;
                        }
                    };

                    // For some queries df.execute_stream() won't yield any batches
                    // in case there aren't any results. When this happens we never sent the headers.
                    // We correct it here and send an empty batch.
                    // This isn't a problem in JSON. And in parquet the file writer will
                    // produce an empty file with a schema by default.
                    if !headers_sent {
                        let batch = RecordBatch::new_empty(schema);
                        let txt_table_format = pretty_format_batches(&[batch])
                            .map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None });
                        match txt_table_format {
                            Ok(txt_table) => {
                                let txt_table = txt_table.to_string();
                                yield txt_table.into();
                            }
                            Err(e) => {
                                yield format!("ERROR: {}", e).into();
                                return;
                            }
                        }
                    }
                }))
        }
        AdHocResultFormat::Json => Ok(HttpResponse::Ok()
            .content_type(mime::APPLICATION_JSON)
            .streaming::<_, Infallible>(try_stream! {
                let stream_exec = match execute_stream(df).await {
                    Ok(res) => res.map_err(|e| PipelineError::AdHocQueryError { error: e.to_string(), df: None }),
                    Err(e) => {
                        yield format!("ERROR: {}", e).into();
                        return;
                    }
                };

                match stream_exec {
                    Ok(mut stream) => {
                        while let Some(batch) = stream.next().await {
                            let batch_result = batch.map_err(PipelineError::from);
                            match batch_result {
                                Ok(batch) => {
                                    let mut buf = Vec::with_capacity(4096);
                                    let builder = WriterBuilder::new().with_explicit_nulls(true);
                                    let mut writer = builder.build::<_, LineDelimited>(&mut buf);
                                    if let Err(e) = writer.write(&batch).map_err(DataFusionError::from).map_err(PipelineError::from) {
                                        yield serde_json::to_string(&e).unwrap().into();
                                        return;
                                    }
                                    if let Err(e) = writer.finish().map_err(DataFusionError::from).map_err(PipelineError::from) {
                                        yield serde_json::to_string(&e).unwrap().into();
                                        return;
                                    }
                                    yield buf.into();
                                }
                                Err(e) => {
                                    yield serde_json::to_string(&e).unwrap().into();
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        yield serde_json::to_string(&e).unwrap().into();
                        return;
                    }
                }
            })),
        AdHocResultFormat::Parquet => {
            let file_name = format!(
                "results_{}.parquet",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            );
            // Create a channel to communicate between the parquet writer and the HTTP response
            let (tx, mut rx) = mpsc::channel(1024);

            let mut stream_job = Box::pin(async move {
                let schema = df.schema().inner().clone();
                let mut stream = execute_stream(df).await.expect("unable to receive stream")?;

                let mut writer = AsyncArrowWriter::try_new(
                    ChannelWriter::new(tx),
                    schema,
                    Some(WriterProperties::builder().set_compression(Compression::SNAPPY).build()),
                )?;
                while let Some(batch) = stream.next().await {
                    let batch = batch.map_err(DataFusionError::from)?;
                    writer.write(&batch).await?;
                }
                writer.flush().await?;
                writer.close().await?;
                <datafusion::common::Result<_>>::Ok(())
            }.fuse());

            Ok(HttpResponse::Ok()
                .insert_header(header::ContentDisposition::attachment(file_name))
                .content_type(mime::APPLICATION_OCTET_STREAM)
                .streaming(stream! {
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
                }))
        }
    }
}
