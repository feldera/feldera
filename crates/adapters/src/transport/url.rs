use super::{InputConsumer, InputEndpoint, InputQueue, InputReader, Step, TransportInputEndpoint};
use crate::{ensure_default_crypto_provider, format::AppendSplitter, Parser, PipelineState};
use actix::System;
use actix_web::http::header::{ByteRangeSpec, ContentRangeSpec, Range, CONTENT_RANGE};
use anyhow::{anyhow, Result as AnyResult};
use awc::{Client, Connector};
use feldera_types::program_schema::Relation;
use feldera_types::transport::url::UrlInputConfig;
use futures::StreamExt;
use std::{cmp::Ordering, str::FromStr, sync::Arc, thread::spawn, time::Duration};
use tokio::{
    select,
    sync::watch::{channel, Receiver, Sender},
    time::{sleep_until, Instant},
};

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
        false
    }
}

impl TransportInputEndpoint for UrlInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(UrlInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

struct UrlInputReader {
    sender: Sender<PipelineState>,
    queue: Arc<InputQueue>,
}

impl UrlInputReader {
    fn new(
        config: &Arc<UrlInputConfig>,
        consumer: Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let (sender, receiver) = channel(PipelineState::Paused);
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        spawn({
            let config = config.clone();
            let receiver = receiver.clone();
            let queue = queue.clone();
            move || {
                System::new().block_on(async move {
                    let mut splitter = AppendSplitter::new(parser.splitter());
                    if let Err(error) =
                        Self::worker_thread(config, &mut parser, &mut splitter, receiver, &queue)
                            .await
                    {
                        consumer.error(true, error);
                    } else {
                        if let Some(record) = splitter.next(true) {
                            queue.push(record.len(), parser.parse(record));
                        }
                        consumer.eoi();
                    };
                });
            }
        });

        Ok(Self { sender, queue })
    }

    async fn worker_thread(
        config: Arc<UrlInputConfig>,
        parser: &mut Box<dyn Parser>,
        splitter: &mut AppendSplitter,
        mut receiver: Receiver<PipelineState>,
        queue: &InputQueue,
    ) -> AnyResult<()> {
        ensure_default_crypto_provider();

        let client = Client::builder().connector(Connector::new()).finish();

        // Number of bytes of URL content that we've delivered to `consumer`.
        let mut consumed_bytes = 0;

        // The URL content offset of the next byte that we'll receive.
        let mut offset = 0;

        // The `ClientResponse`, if there is one.
        let mut response = None;

        // The time at which we will disconnect from the server, if we are
        // paused when this time arrives.
        let mut deadline = None;

        loop {
            let state = *receiver.borrow();
            match state {
                PipelineState::Terminated => return Ok(()),
                PipelineState::Paused => {
                    if deadline.is_none() {
                        // If the pause timeout is so big that it overflows
                        // `Instant`, leave it as `None` and we'll just never
                        // timeout.
                        deadline = Instant::now()
                            .checked_add(Duration::from_secs(config.pause_timeout as u64));
                    }
                    loop {
                        let sleep = deadline.map(sleep_until);
                        tokio::pin!(sleep);
                        select! {
                            _ = sleep.as_pin_mut().unwrap(), if sleep.is_some() && response.is_some() => {
                                // On pause, drop the connection after a delay.
                                // We will reconnect when we start running
                                // again.
                                //
                                // Dropping the connection immediately is a bad
                                // idea because the pause might be just long
                                // enough to let a data ingress backlog clear
                                // up, probably at most a few seconds.  It's not
                                // good to churn the connection to the server
                                // for that, especially if it's one that can't
                                // restart mid-file.
                                //
                                // On the other hand, if we never drop the
                                // connection while paused, we risk getting an
                                // error from the server disconnecting when we
                                // go idle for a long time.  Then we'd have to
                                // be able to distinguish idle disconnects from
                                // other server errors, which could be
                                // challenging.  It seems easier to just
                                // disconnect and reconnect.
                                let _ = response.take();
                            },

                            // Wait for a state change.
                            result = receiver.changed() => { result?; break; }
                        }
                    }
                }
                PipelineState::Running => {
                    deadline = None;

                    // If we haven't connected yet, or if we're resuming
                    // following pause, connect to the server.
                    if response.is_none() {
                        let mut request = client.get(&config.path);
                        if consumed_bytes > 0 {
                            // Try to resume at the point where we left off.
                            request =
                                request.insert_header(Range::Bytes(vec![ByteRangeSpec::From(
                                    consumed_bytes,
                                )]));
                        }
                        let r = request.send().await.map_err(
                            // `awc` intentionally uses errors that aren't `Sync`, but
                            // `anyhow::Error` requires `Sync`.  Transform the error so we can
                            // return it.
                            |error| anyhow!("{error}"),
                        )?;
                        if !r.status().is_success() {
                            Err(anyhow!(
                                "received unexpected HTTP status code ({})",
                                r.status()
                            ))?
                        }

                        // The server tells us the range of the URL content it's
                        // sending us.  If it doesn't say anything (which is
                        // valid even if we asked for a range), then it is
                        // starting at the beginning.
                        offset = if let Some(range) = r.headers().get(CONTENT_RANGE) {
                            match ContentRangeSpec::from_str(range.to_str()?)? {
                                ContentRangeSpec::Bytes {
                                    range: Some((start, _)),
                                    ..
                                } => start,
                                ContentRangeSpec::Bytes { range: None, .. } => {
                                    // Weird flex, bro.
                                    0
                                },
                                other => Err(anyhow!("expected byte range in HTTP response, instead received {other}"))?,
                            }
                        } else {
                            0
                        };
                        if offset > consumed_bytes {
                            Err(anyhow!("HTTP server skipped past data we need, by starting at {offset} instead of {consumed_bytes}"))?
                        }
                        response = Some(r);
                    };
                    let response = response.as_mut().unwrap();

                    select! {
                        _ = receiver.changed() => (),
                        result = response.next() => {
                            match result {
                                None => return Ok(()),
                                Some(Ok(data)) => {
                                    let data_len = data.len() as u64;

                                    // Figure out what part of the data we
                                    // received should be fed to `consumer`.  In
                                    // the common case, that's all of it.  But
                                    // if we paused and restarted, and the HTTP
                                    // server didn't honor our range request, we
                                    // have to discard data up to offset
                                    // `consumed_bytes`.
                                    let chunk = match offset.cmp(&consumed_bytes) {
                                        Ordering::Equal => &data[..],
                                        Ordering::Less => {
                                            let skip = consumed_bytes - offset;
                                            if skip >= data_len {
                                                &[]
                                            } else {
                                                &data[skip as usize..]
                                            }
                                        },
                                        Ordering::Greater => unreachable!()
                                    };
                                    if !chunk.is_empty() {
                                        consumed_bytes += chunk.len() as u64;
                                        splitter.append(chunk);
                                        while let Some(record) = splitter.next(false) {
                                            queue.push(record.len(), parser.parse(record));
                                        }
                                    }
                                    offset += data_len;
                                },
                                Some(Err(error)) => Err(error)?,
                            }
                        }
                    }
                }
            }
        }
    }
}

impl InputReader for UrlInputReader {
    fn pause(&self) -> AnyResult<()> {
        // Use `send_replace`, instead of `send`, to make it a no-op if the
        // worker thread has died.  We want that behavior because pausing a
        // download that is already complete should be a no-op.
        //
        // Same for `start` and `disconnect`, below.
        self.sender.send_replace(PipelineState::Paused);
        Ok(())
    }

    fn start(&self, _step: Step) -> AnyResult<()> {
        self.sender.send_replace(PipelineState::Running);
        Ok(())
    }

    fn disconnect(&self) {
        self.sender.send_replace(PipelineState::Terminated);
    }

    fn flush(&self, n: usize) -> usize {
        self.queue.flush(n)
    }
}

impl Drop for UrlInputReader {
    fn drop(&mut self) {
        self.disconnect();
    }
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

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
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
        endpoint.flush_all();

        // No outputs should be produced at this point.
        assert!(parser.state().data.is_empty());
        assert!(!consumer.state().eoi);

        // Unpause the endpoint, wait for the data to appear at the output.
        endpoint.start(0).unwrap();
        wait(
            || {
                endpoint.flush_all();
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
        endpoint.start(0).unwrap();
        wait(
            || consumer.state().endpoint_error.is_some(),
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
        endpoint.start(0).unwrap();

        // The first 10 records should take about 100 ms to arrive.  In practice
        // on busy CI systems it often seems to take longer, so be generous.
        let timeout_ms = 2000;
        let n1_time = wait(
            || {
                endpoint.flush_all();
                n_recs(&zset) >= 10
            },
            timeout_ms,
        )
        .unwrap_or_else(|()| panic!("only {} records after {timeout_ms} ms", n_recs(&zset)));
        let n1 = n_recs(&zset);
        println!("{n1} records took {n1_time} ms to arrive");

        // After another 100 ms, there should be more records.
        sleep(Duration::from_millis(100));
        endpoint.flush_all();
        let n2 = n_recs(&zset);
        println!("100 ms later, {n2} records arrived");
        assert!(n2 > n1, "After 100 ms longer, no more records arrived");

        // After another 100 ms, there should be more records again.  This time,
        // check that we've got at least 10 more than `n1` (really it should be
        // 20).
        sleep(Duration::from_millis(100));
        endpoint.flush_all();
        let n3 = n_recs(&zset);
        println!("100 ms later, {n3} records arrived");
        assert!(
            n3 > n1 + 10,
            "At least {} records should have arrived but only {n3} did",
            n1 + 10
        );

        // Wait for the first 50 records to arrive.
        let n4_time = wait(
            || {
                endpoint.flush_all();
                n_recs(&zset) >= 50
            },
            350,
        )
        .unwrap_or_else(|()| panic!("only {} records after 350 ms", n_recs(&zset)));
        let n4 = n_recs(&zset);
        println!("{} records took {n4_time} ms longer to arrive", n4 - n3);

        // Pause the endpoint.  No more records should arrive but who knows,
        // there could be a race, so don't be precise about it.
        println!("pausing...");
        endpoint.pause().unwrap();
        sleep(Duration::from_millis(100));
        endpoint.flush_all();
        let n5 = n_recs(&zset);
        println!("100 ms later, {n5} records arrived");

        // But now that we've waited a bit, no more should definitely arrive.
        for _ in 0..2 {
            sleep(Duration::from_millis(100));
            endpoint.flush_all();
            let n = n_recs(&zset);
            println!("100 ms later, {n} records arrived");
            assert_eq!(n5, n);
        }

        // Now restart the endpoint.
        endpoint.start(0).unwrap();
        println!("restarting...");
        if with_reconnect {
            // The adapter will reopen the connection to the server.  The server
            // is dumb and it can still only generate records one per second.
            // Our code will discard the records until they get up to the
            // previous position.  That means that if we wait up to 500 ms,
            // there should be no new data.  Since real life is full of races,
            // let's only wait 400 ms.
            for _ in 0..4 {
                sleep(Duration::from_millis(100));
                endpoint.flush_all();
                let n = n_recs(&zset);
                println!("100 ms later, {n} records arrived");
                assert_eq!(n5, n);
            }
        } else {
            // The records should start arriving again immediately.
            sleep(Duration::from_millis(200));
            endpoint.flush_all();
            let n = n_recs(&zset);
            println!("200 ms later, {n} records arrived");
            assert!(n > n5);
        }

        // Within 600 ms more, we should get all 100 records, but fudge it to
        // 1000 ms.
        let n6_time = wait(
            || {
                endpoint.flush_all();
                n_recs(&zset) >= 100
            },
            1000,
        )
        .unwrap_or_else(|()| panic!("only {} records after 1000 ms", n_recs(&zset)));
        endpoint.flush_all();
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
    async fn test_pause_with_reconnect() -> Result<()> {
        test_pause(true).await
    }

    /// Test pause and resume of connections, with a long `pause_timeout` so
    /// that the server connection stays up.
    #[actix_web::test]
    async fn test_pause_without_reconnect() -> Result<()> {
        test_pause(false).await
    }
}
