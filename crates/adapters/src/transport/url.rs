use super::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, TransportInputEndpoint,
};
use crate::{InputBuffer, Parser, ensure_default_crypto_provider, format::StreamSplitter};
use actix::System;
use actix_web::http::StatusCode;
use actix_web::{
    dev::{Decompress, Payload},
    http::header::{ByteRangeSpec, CONTENT_RANGE, ContentRangeSpec, Range as ActixRange},
};
use anyhow::{Result as AnyResult, anyhow, bail};
use awc::error::HeaderValue;
use awc::{Client, ClientResponse, Connector, http::header::HeaderMap};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use feldera_types::transport::url::UrlInputConfig;
use futures::{StreamExt, future::OptionFuture};
use serde::{Deserialize, Serialize};
use std::thread;
use std::{
    cmp::{Ordering, min},
    collections::VecDeque,
    hash::Hasher,
    ops::Range,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    time::{Instant, sleep_until},
};
use tracing::{info_span, warn};
use xxhash_rust::xxh3::Xxh3Default;

pub(crate) struct UrlInputEndpoint {
    config: Arc<UrlInputConfig>,
}

impl UrlInputEndpoint {
    pub(crate) fn new(config: UrlInputConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl InputEndpoint for UrlInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for UrlInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        seek: Option<serde_json::Value>,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(UrlInputReader::new(
            &self.config,
            consumer,
            parser,
            seek,
        )?))
    }
}

struct UrlStream<'a> {
    client: Client,
    path: &'a str,

    response: Option<ClientResponse<Decompress<Payload>>>,
    next_read_ofs: u64,

    buffer: Bytes,
    ofs: u64,
}

/// Starting offset of the response sent by the server, determined based on
/// the status code and the `Content-Range` HTTP header that could be present.
/// The offset is the first byte position.
///
/// - If its status code is 2xx, it will return zero offset.
///   An error will be returned if it still has a `Content-Range` header
///   because the header only has a meaning for status code 206 and 416:
///   https://datatracker.ietf.org/doc/html/rfc9110#name-content-range
///
/// - If its status code is 206 PARTIAL CONTENT, the first byte position of
///   the single `Content-Range` will be returned. An error will be returned
///   if no range is returned, the range is not a byte range, the range is
///   unsatisfiable, or more than one range is returned.
fn get_response_starting_offset(status: StatusCode, headers: &HeaderMap) -> AnyResult<u64> {
    if status != StatusCode::PARTIAL_CONTENT {
        if headers.get(CONTENT_RANGE).is_some() {
            bail!(
                "HTTP response contains the Content-Range header but its status code ({}) is not 206 Partial Content",
                status
            );
        }
        Ok(0)
    } else {
        let content_range_values: Vec<&HeaderValue> = headers.get_all(CONTENT_RANGE).collect();
        match content_range_values[..] {
            [] => {
                bail!("HTTP response is 206 Partial Content but has no Content-Range header");
            }
            [range] => match ContentRangeSpec::from_str(range.to_str()?)? {
                ContentRangeSpec::Bytes {
                    range: Some((start, _)),
                    ..
                } => Ok(start),
                ContentRangeSpec::Bytes { range: None, .. } => {
                    bail!(
                        "HTTP response is 206 Partial Content but has a Content-Range which indicates it is unsatisfiable"
                    );
                }
                other => {
                    bail!(
                        "expected byte range in HTTP response Content-Range header, instead received: {other}"
                    );
                }
            },
            _ => {
                bail!(
                    "HTTP response should have only a single Content-Range header, but {} are present",
                    content_range_values.len()
                );
            }
        }
    }
}

impl<'a> UrlStream<'a> {
    /// HTTP request timeout to receive the initial response (e.g., status code).
    /// This does not include the time to receive the content itself following it.
    const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    pub fn new(path: &'a str) -> Self {
        Self {
            client: Client::builder().connector(Connector::new()).finish(),
            path,
            response: None,
            next_read_ofs: 0,
            buffer: Bytes::new(),
            ofs: 0,
        }
    }

    pub async fn read(&mut self, limit: usize) -> AnyResult<Option<Bytes>> {
        loop {
            if !self.buffer.is_empty() {
                let n = min(limit, self.buffer.len());
                self.ofs += n as u64;
                return Ok(Some(self.buffer.split_to(n)));
            }

            if self.response.is_none() {
                let mut request = self.client.get(self.path);

                // Attempt to request data only from the offset we left off previously by inserting
                // a `Range` header. This is only an attempt, as the server can decide to ignore it.
                if self.ofs > 0 {
                    request = request
                        .insert_header(ActixRange::Bytes(vec![ByteRangeSpec::From(self.ofs)]));
                }

                let response = request
                    .timeout(Self::HTTP_REQUEST_TIMEOUT)
                    .send()
                    .await
                    .map_err(
                        // `awc` intentionally uses errors that aren't `Sync`, but
                        // `anyhow::Error` requires `Sync`.  Transform the error so we can
                        // return it.
                        |error| anyhow!("{error}"),
                    )?;
                let status = response.status();
                // info!("HTTP response status code: {}", status);

                if status.as_u16() == StatusCode::RANGE_NOT_SATISFIABLE {
                    warn!(
                        "Received HTTP status code 416 (RANGE_NOT_SATISFIABLE)--connector has reached the end of input."
                    );
                    return Ok(None);
                }

                // All 2xx status codes are considered valid, including ones that don't provide content.
                // Note that redirection (3xx) is not followed and will also result in an error.
                if !status.is_success() {
                    bail!(
                        "HTTP status code of response ({}) is not success (2xx)",
                        status
                    );
                }

                // It is possible that the server returns an earlier offset (generally, 0),
                // especially when it does not support a partial response. However, it is
                // not acceptable if it returns a later offset.
                self.next_read_ofs = get_response_starting_offset(status, response.headers())?;
                if self.next_read_ofs > self.ofs {
                    Err(anyhow!(
                        "HTTP response skipped past data we need, by starting at {} instead of {}",
                        self.next_read_ofs,
                        self.ofs
                    ))?
                }

                self.response = Some(response);
            };

            let Some(result) = self.response.as_mut().unwrap().next().await else {
                // End of stream.
                return Ok(None);
            };

            // Get the data and its starting offset from the beginning of
            // the URL.
            let data = result?;
            let data_ofs = self.next_read_ofs;
            self.next_read_ofs += data.len() as u64;

            // Extract a chunk of data that can be returned to our
            // caller. If this is nonempty, then `self.ofs` is its starting
            // offset from the beginning of the URL.
            self.buffer = match data_ofs.cmp(&self.ofs) {
                Ordering::Equal => data,
                Ordering::Less => {
                    let skip = (self.ofs - data_ofs) as usize;
                    if skip < data.len() {
                        data.slice(skip..)
                    } else {
                        Bytes::new()
                    }
                }
                Ordering::Greater => unreachable!(),
            };
        }
    }

    fn seek(&mut self, ofs: u64) {
        if ofs != self.ofs {
            self.ofs = ofs;
            self.disconnect();
        }
    }

    fn disconnect(&mut self) {
        self.response = None;
        self.buffer = Bytes::new();
    }

    fn is_connected(&self) -> bool {
        self.response.is_some()
    }
}

struct UrlInputReader {
    sender: UnboundedSender<InputReaderCommand>,
}

impl UrlInputReader {
    fn new(
        config: &Arc<UrlInputConfig>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        seek: Option<serde_json::Value>,
    ) -> AnyResult<Self> {
        let (sender, receiver) = unbounded_channel();
        thread::Builder::new()
            .name("url-connector".to_string())
            .spawn({
                let config = config.clone();
                move || {
                    let _guard = info_span!("url_input", path = config.path.clone()).entered();
                    System::new().block_on(async move {
                        if let Err(error) = Self::worker_thread(
                            config,
                            &mut parser,
                            receiver,
                            consumer.as_ref(),
                            seek,
                        )
                        .await
                        {
                            consumer.error(true, error, None);
                        };
                    });
                }
            })
            .expect("failed to spawn URL connector thread");

        Ok(Self { sender })
    }

    async fn worker_thread(
        config: Arc<UrlInputConfig>,
        parser: &mut Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
        consumer: &dyn InputConsumer,
        seek: Option<serde_json::Value>,
    ) -> AnyResult<()> {
        ensure_default_crypto_provider();

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);
        let offset = if let Some(metadata) = seek {
            let metadata = serde_json_path_to_error::from_value::<Metadata>(metadata)
                .map_err(|e| anyhow!("error deserializing checkpointed connector metadata: {e}"))?;
            metadata.offsets.end
        } else {
            0
        };

        let mut splitter = StreamSplitter::new(parser.splitter());
        let mut stream = UrlStream::new(&config.path);
        stream.seek(offset);
        splitter.seek(offset);

        while let Some((Metadata { offsets }, ())) = command_receiver.recv_replay().await? {
            stream.seek(offsets.start);
            splitter.seek(offsets.start);
            let mut remainder = (offsets.end - offsets.start) as usize;
            let mut total = BufferSize::empty();
            let mut hasher = Xxh3Default::new();
            while remainder > 0 {
                let bytes = stream.read(remainder).await?;
                let Some(bytes) = bytes else {
                    return Err(anyhow!("unexpected end of data replaying read from URL"));
                };
                splitter.append(&bytes);
                remainder -= bytes.len();
                while let Some(chunk) = splitter.next(remainder == 0) {
                    let (mut buffer, errors) = parser.parse(chunk, None);
                    consumer.parse_errors(errors);
                    consumer.buffered(buffer.len());
                    total += buffer.len();
                    buffer.hash(&mut hasher);
                    consumer.check_flush(buffer.flush());
                }
            }
            consumer.replayed(total, hasher.finish());
        }

        let mut queue = VecDeque::<(Range<u64>, Box<dyn InputBuffer>, DateTime<Utc>)>::new();

        // The time at which we will disconnect from the server, if we are
        // paused when this time arrives.
        let mut deadline = None;

        let mut extending = false;
        let mut eof = false;
        loop {
            let running = extending && !eof;
            if running || !stream.is_connected() {
                deadline = None;
            } else if deadline.is_none() {
                // If the pause timeout is so big that it overflows `Instant`,
                // leave it as `None` and we'll just never timeout.
                deadline = Instant::now().checked_add(config.pause_linger().into());
            }
            let disconnect: OptionFuture<_> = deadline.map(sleep_until).into();

            // Use timestamp before we start reading data from the stream as ingestion timestamp
            // for all records derived from the next input chunk.
            let timestamp = Utc::now();

            select! {
                _ = disconnect, if deadline.is_some() => {
                    stream.disconnect()
                },
                command = command_receiver.recv() => {
                    match command? {
                        command @ InputReaderCommand::Replay{..} => {
                            unreachable!("{command:?} must be at the beginning of the command stream")
                        }
                        InputReaderCommand::Extend => {
                            extending = true;
                        }
                        InputReaderCommand::Pause => {
                            extending = false;
                        }
                        InputReaderCommand::Queue{..} => {
                            let mut total = BufferSize::empty();
                            let mut hasher = consumer.hasher();
                            let limit = consumer.max_batch_size();
                            let mut range: Option<Range<u64>> = None;
                            let mut watermarks = Vec::new();

                            while let Some((offsets, mut buffer, timestamp)) = queue.pop_front() {
                                range = match range {
                                    Some(range) => Some(range.start..offsets.end),
                                    None => Some(offsets),
                                };
                                total += buffer.len();
                                if let Some(hasher) = hasher.as_mut() {
                                    buffer.hash(hasher);
                                }
                                consumer.check_flush(buffer.flush());
                                watermarks.push(Watermark::new(timestamp, None));
                                if total.records >= limit {
                                    break;
                                }
                            }
                            let seek = serde_json::to_value(Metadata {
                                    offsets: range.unwrap_or_else(|| {
                                        let ofs = splitter.position();
                                        ofs..ofs
                                    }),
                            }).unwrap();
                            consumer.extended(total, Some(Resume::new_metadata_only(seek, hasher.map(|h| h.finish()))), watermarks);
                        }
                        InputReaderCommand::Disconnect => return Ok(()),
                    }
                },
                bytes = stream.read(usize::MAX), if running => {
                    match bytes? {
                        None => eof = true,
                        Some(bytes) => splitter.append(&bytes),
                    };
                    loop {
                        let start = splitter.position();
                        let Some(chunk) = splitter.next(eof) else {
                            break;
                        };
                        let (buffer, errors) = parser.parse(chunk, None);
                        consumer.buffered(buffer.len());
                        consumer.parse_errors(errors);

                        if let Some(buffer) = buffer {
                            let end = splitter.position();
                            queue.push_back((start..end, buffer, timestamp));
                        }
                    }
                    if eof {
                        consumer.eoi();
                        stream.disconnect();
                    }
                },
            };
        }
    }
}

impl InputReader for UrlInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for UrlInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    offsets: Range<u64>,
}

#[cfg(any(test, feature = "testing"))]
pub mod tests;
