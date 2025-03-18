use super::{
    InputConsumer, InputEndpoint, InputReader, InputReaderCommand, TransportInputEndpoint,
};
use crate::{ensure_default_crypto_provider, format::StreamSplitter, InputBuffer, Parser};
use actix::System;
use actix_web::http::StatusCode;
use actix_web::{
    dev::{Decompress, Payload},
    http::header::{ByteRangeSpec, ContentRangeSpec, Range as ActixRange, CONTENT_RANGE},
};
use anyhow::{anyhow, bail, Result as AnyResult};
use awc::error::HeaderValue;
use awc::{http::header::HeaderMap, Client, ClientResponse, Connector};
use bytes::Bytes;
use feldera_adapterlib::transport::InputCommandReceiver;
use feldera_types::program_schema::Relation;
use feldera_types::transport::url::UrlInputConfig;
use futures::{future::OptionFuture, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    cmp::{min, Ordering},
    collections::VecDeque,
    hash::Hasher,
    ops::Range,
    str::FromStr,
    sync::Arc,
    thread::spawn,
    time::Duration,
};
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::{sleep_until, Instant},
};
use tracing::{info, info_span};
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
    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl TransportInputEndpoint for UrlInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(UrlInputReader::new(
            &self.config,
            consumer,
            parser,
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
            bail!("HTTP response contains the Content-Range header but its status code ({}) is not 206 Partial Content", status);
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
                    bail!("HTTP response is 206 Partial Content but has a Content-Range which indicates it is unsatisfiable");
                }
                other => {
                    bail!("expected byte range in HTTP response Content-Range header, instead received: {other}");
                }
            },
            _ => {
                bail!("HTTP response should have only a single Content-Range header, but {} are present", content_range_values.len());
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
                info!("HTTP response status code: {}", status);

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
    ) -> AnyResult<Self> {
        let (sender, receiver) = unbounded_channel();
        spawn({
            let config = config.clone();
            move || {
                let _guard = info_span!("url_input", path = config.path.clone()).entered();
                System::new().block_on(async move {
                    if let Err(error) =
                        Self::worker_thread(config, &mut parser, receiver, consumer.as_ref()).await
                    {
                        consumer.error(true, error);
                    };
                });
            }
        });

        Ok(Self { sender })
    }

    async fn worker_thread(
        config: Arc<UrlInputConfig>,
        parser: &mut Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
        consumer: &dyn InputConsumer,
    ) -> AnyResult<()> {
        ensure_default_crypto_provider();

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);
        let offset = if let Some(metadata) = command_receiver.recv_seek().await? {
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
            let mut num_records = 0;
            let mut hasher = Xxh3Default::new();
            while remainder > 0 {
                let bytes = stream.read(remainder).await?;
                let Some(bytes) = bytes else {
                    return Err(anyhow!("unexpected end of data replaying read from URL"));
                };
                splitter.append(&bytes);
                remainder -= bytes.len();
                while let Some(chunk) = splitter.next(remainder == 0) {
                    let (mut buffer, errors) = parser.parse(chunk);
                    consumer.parse_errors(errors);
                    consumer.buffered(buffer.len(), chunk.len());
                    num_records += buffer.len();
                    buffer.hash(&mut hasher);
                    buffer.flush();
                }
            }
            consumer.replayed(num_records, hasher.finish());
        }

        let mut queue = VecDeque::<(Range<u64>, Box<dyn InputBuffer>)>::new();

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
                deadline =
                    Instant::now().checked_add(Duration::from_secs(config.pause_timeout as u64));
            }
            let disconnect: OptionFuture<_> = deadline.map(sleep_until).into();

            select! {
                _ = disconnect, if deadline.is_some() => {
                    stream.disconnect()
                },
                command = command_receiver.recv() => {
                    match command? {
                        command @ InputReaderCommand::Seek(_) | command @ InputReaderCommand::Replay{..} => {
                            unreachable!("{command:?} must be at the beginning of the command stream")
                        }
                        InputReaderCommand::Extend => {
                            extending = true;
                        }
                        InputReaderCommand::Pause => {
                            extending = false;
                        }
                        InputReaderCommand::Queue => {
                            let mut total = 0;
                            let mut hasher = consumer.is_pipeline_fault_tolerant().then(Xxh3Default::new);
                            let limit = consumer.max_batch_size();
                            let mut range: Option<Range<u64>> = None;
                            while let Some((offsets, mut buffer)) = queue.pop_front() {
                                range = match range {
                                    Some(range) => Some(range.start..offsets.end),
                                    None => Some(offsets),
                                };
                                total += buffer.len();
                                if let Some(hasher) = hasher.as_mut() {
                                    buffer.hash(hasher);
                                }
                                buffer.flush();
                                if total >= limit {
                                    break;
                                }
                            }
                            consumer.extended(
                                total,
                                hasher.map_or(0, |hasher| hasher.finish()),
                                serde_json::to_value(Metadata {
                                    offsets: range.unwrap_or_else(|| {
                                        let ofs = splitter.position();
                                        ofs..ofs
                                    }),
                                }).unwrap(),
                                rmpv::Value::Nil,
                            );
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
                        let (buffer, errors) = parser.parse(chunk);
                        consumer.buffered(buffer.len(), chunk.len());
                        consumer.parse_errors(errors);

                        if let Some(buffer) = buffer {
                            let end = splitter.position();
                            queue.push_back((start..end, buffer));
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

#[cfg(test)]
mod test {
    use crate::{
        test::{
            mock_input_pipeline, wait, MockDeZSet, MockInputConsumer, MockInputParser,
            DEFAULT_TIMEOUT_MS,
        },
        transport::InputReader,
    };
    use actix::System;
    use actix_web::{
        middleware,
        web::{self, Bytes},
        App, FromRequest, Handler, HttpResponse, HttpServer, Responder, Result,
    };
    use async_stream::stream;
    use feldera_types::deserialize_without_context;
    use feldera_types::program_schema::Relation;
    use futures_timer::Delay;
    use serde::{Deserialize, Serialize};
    use std::{
        io::Error as IoError,
        sync::mpsc::channel,
        thread::{sleep, spawn},
        time::Duration,
    };

    #[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
    struct TestStruct {
        s: String,
        b: bool,
        i: i64,
    }

    impl TestStruct {
        fn new(s: String, b: bool, i: i64) -> Self {
            Self { s, b, i }
        }
    }

    deserialize_without_context!(TestStruct);

    fn n_recs(zset: &MockDeZSet<TestStruct, TestStruct>) -> usize {
        zset.state().flushed.len()
    }

    async fn setup_test<F, Args>(
        response: F,
        path: &str,
        pause_timeout: u32,
    ) -> (
        Box<dyn InputReader>,
        MockInputConsumer,
        MockInputParser,
        MockDeZSet<TestStruct, TestStruct>,
    )
    where
        F: Handler<Args> + Send + Copy,
        Args: FromRequest + 'static,
        F::Output: Responder + 'static,
    {
        // Start HTTP server listening on an arbitrary local port, and obtain
        // its socket address as `addr`.
        let (sender, receiver) = channel();
        spawn(move || {
            System::new().block_on(async {
                let server = HttpServer::new(move || {
                    App::new()
                        // enable logger
                        .wrap(middleware::Logger::default())
                        .service(web::resource("/test.csv").to(response))
                })
                .workers(1)
                .bind(("127.0.0.1", 0))
                .unwrap();
                sender.send(server.addrs()[0]).unwrap();
                server.run().await.unwrap();
            });
        });
        let addr = receiver.recv().unwrap();
        // Create a transport endpoint attached to the file.
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: url_input
    config:
        path: http://{addr}/{path}
        pause_timeout: {pause_timeout}
format:
    name: csv
"#
        );

        mock_input_pipeline::<TestStruct, TestStruct>(
            serde_yaml::from_str(&config_str).unwrap(),
            Relation::empty(),
            true,
        )
        .unwrap()
    }

    /// Test normal successful data retrieval.
    #[actix_web::test]
    async fn test_success() -> Result<()> {
        let test_data = [
            TestStruct::new("foo".to_string(), true, 10),
            TestStruct::new("bar".to_string(), false, -10),
        ];

        let (endpoint, consumer, parser, zset) = setup_test(
            || async {
                "\
foo,true,10
bar,false,-10
"
            },
            "test.csv",
            60,
        )
        .await;

        consumer.on_error(Some(Box::new(|_, _| ())));

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.extend();
        wait(
            || {
                endpoint.queue();
                n_recs(&zset) == test_data.len()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
        Ok(())
    }

    /// Test connection failure.
    #[actix_web::test]
    async fn test_failure() -> Result<()> {
        let (endpoint, consumer, parser, _zset) =
            setup_test(|| async { "" }, "nonexistent", 60).await;

        // Disable panic on error so we can detect it gracefully below.
        consumer.on_error(Some(Box::new(|_, _| ())));

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(consumer.state().endpoint_error.is_none());
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the error.
        endpoint.extend();
        wait(
            || {
                endpoint.queue();
                consumer.state().endpoint_error.is_some()
            },
            DEFAULT_TIMEOUT_MS,
        )
        .unwrap();
        Ok(())
    }

    /// Test pause and resume of connections.
    async fn test_pause(with_reconnect: bool) -> Result<()> {
        let test_data: Vec<_> = (0..100)
            .map(|i| TestStruct {
                s: "foo".into(),
                b: true,
                i,
            })
            .collect();

        let (endpoint, consumer, parser, zset) = setup_test(
            || async {
                let stream = stream! {
                    for i in 0..100 {
                        let s = format!("foo,true,{i}\n");
                        yield Ok(Bytes::from(s));
                        Delay::new(Duration::from_millis(10)).await;
                    }
                };
                HttpResponse::Ok().streaming::<_, IoError>(stream)
            },
            "test.csv",
            if with_reconnect { 0 } else { 60 },
        )
        .await;

        consumer.on_error(Some(Box::new(|_, _| ())));

        sleep(Duration::from_millis(10));

        // No outputs should be produced at this point.
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint.  Outputs should start arriving, one record
        // every 10 ms.
        endpoint.extend();

        // The first 10 records should take about 100 ms to arrive.  In practice
        // on busy CI systems it often seems to take longer, so be generous.
        let timeout_ms = 2000;
        let n1_time = wait(
            || {
                endpoint.queue();
                n_recs(&zset) >= 10
            },
            timeout_ms,
        )
        .unwrap_or_else(|()| panic!("only {} records after {timeout_ms} ms", n_recs(&zset)));
        let n1 = n_recs(&zset);
        println!("{n1} records took {n1_time} ms to arrive");

        // After another 100 ms, there should be more records.
        sleep(Duration::from_millis(100));
        endpoint.queue(); // XXX need to wait for it to be processed.
        let n2 = n_recs(&zset);
        println!("100 ms later, {n2} records arrived");
        assert!(n2 > n1, "After 100 ms longer, no more records arrived");

        // After another 100 ms, there should be more records again.  This time,
        // check that we've got at least 10 more than `n1` (really it should be
        // 20).
        sleep(Duration::from_millis(100));
        endpoint.queue();
        sleep(Duration::from_millis(10));
        let n3 = n_recs(&zset);
        println!("100 ms later, {n3} records arrived");
        assert!(
            n3 >= n1 + 10,
            "At least {} records should have arrived but only {n3} did",
            n1 + 10
        );

        // Wait for the first 50 records to arrive.
        let n4_time = wait(
            || {
                endpoint.queue();
                n_recs(&zset) >= 50
            },
            350,
        )
        .unwrap_or_else(|()| panic!("only {} records after 350 ms", n_recs(&zset)));
        let n4 = n_recs(&zset);
        println!(
            "{} more records ({n4} total) took {n4_time} ms longer to arrive",
            n4 - n3
        );

        // Pause the endpoint.  No more records should arrive but who knows,
        // there could be a race, so don't be precise about it.
        println!("pausing...");
        endpoint.pause();
        sleep(Duration::from_millis(100));
        endpoint.queue(); // XXX need to wait for it to be processed.
        let n5 = n_recs(&zset);
        println!("100 ms later, {n5} records arrived");

        // But now that we've waited a bit, no more should definitely arrive.
        for _ in 0..2 {
            sleep(Duration::from_millis(100));
            let n = n_recs(&zset);
            endpoint.queue(); // XXX need to wait for it to be processed.
            println!("100 ms later, {n} records arrived");
            assert_eq!(n5, n);
        }

        // Now restart the endpoint.
        endpoint.extend();
        println!("restarting...");
        if with_reconnect {
            // The adapter will reopen the connection to the server.  The server
            // is dumb and it can still only generate records one per second.
            // Our code will discard the records until they get up to the
            // previous position.  That means that if we wait up to 500 ms,
            // there should be no new data.  Since real life is full of races,
            // let's only wait 400 ms.
            for _ in 0..4 {
                endpoint.queue();
                sleep(Duration::from_millis(100));
                let n = n_recs(&zset);
                println!("100 ms later, {n} records arrived");
                assert_eq!(n5, n);
            }
        } else {
            // The records should start arriving again immediately.
            sleep(Duration::from_millis(200));
            endpoint.queue();
            sleep(Duration::from_millis(100));
            let n = n_recs(&zset);
            println!("200 ms later, {n} records arrived");
            assert!(n > n5);
        }

        // Within 600 ms more, we should get all 100 records, but fudge it to
        // 1000 ms.
        let n6_time = wait(
            || {
                endpoint.queue();
                n_recs(&zset) >= 100
            },
            1000,
        )
        .unwrap_or_else(|()| panic!("only {} records after 1000 ms", n_recs(&zset)));
        let n6 = n_recs(&zset);
        println!("{} more records took {n6_time} ms to arrive", n6 - n5);

        for (i, upd) in zset.state().flushed.iter().enumerate() {
            assert_eq!(upd.unwrap_insert(), &test_data[i]);
        }
        Ok(())
    }

    /// Test pause and resume of connections, with a zero `pause_timeout` so
    /// that the server connection gets dropped and reconnects.
    #[actix_web::test]
    #[ignore]
    async fn test_pause_with_reconnect() -> Result<()> {
        test_pause(true).await
    }

    /// Test pause and resume of connections, with a long `pause_timeout` so
    /// that the server connection stays up.
    #[actix_web::test]
    #[ignore]
    async fn test_pause_without_reconnect() -> Result<()> {
        test_pause(false).await
    }
}
