//! The URL input connector: a document read over HTTP, a failed request, and a stream that
//! pauses and resumes, with and without reconnecting.

use crate::test::engine_test;
use std::collections::BTreeMap;
use std::io::Error as IoError;
use std::sync::mpsc::channel;
use std::thread::{self, sleep};
use std::time::Duration;

use actix::System;
use actix_web::web::{self, Bytes};
use actix_web::{App, FromRequest, Handler, HttpResponse, HttpServer, Responder, middleware};
use async_stream::stream;
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, Updates, default_timeout_ms, wait, wait_for_rows,
};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::json;

use crate::test::{InputPipeline, input_pipeline, published_schema};

/// The table that the cases read into.
pub fn relation() -> Relation {
    Relation::new(
        "url_input".into(),
        vec![
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Starts an HTTP server whose route `/test.csv` answers with `response`, and opens a URL
/// endpoint with `pause_timeout` that reads the CSV at `path` on it into a fresh table.
fn setup<F, Args>(
    engine: &dyn ConnectorTestEngine,
    response: F,
    path: &str,
    pause_timeout: u32,
) -> InputPipeline
where
    F: Handler<Args> + Send + Copy,
    Args: FromRequest + 'static,
    F::Output: Responder + 'static,
{
    let (sender, receiver) = channel();
    thread::Builder::new()
        .name("url-connector-test".to_string())
        .spawn(move || {
            System::new().block_on(async {
                let server = HttpServer::new(move || {
                    App::new()
                        .wrap(middleware::Logger::default())
                        .service(web::resource("/test.csv").to(response))
                })
                .workers(1)
                .bind(("127.0.0.1", 0))
                .unwrap();
                sender.send(server.addrs()[0]).unwrap();
                server.run().await.unwrap();
            });
        })
        .expect("failed to spawn test thread");
    let address = receiver.recv().unwrap();
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "url_input",
            "config": { "path": format!("http://{address}/{path}"), "pause_timeout": pause_timeout }
        },
        "format": { "name": "csv" }
    }))
    .unwrap();
    input_pipeline(engine, &relation(), &InputOptions::default(), config).unwrap()
}

/// A document arrives in order once the endpoint starts.
#[engine_test]
pub fn success(engine: &dyn ConnectorTestEngine) {
    let pipeline = setup(
        engine,
        || async { "foo,true,10\nbar,false,-10\n" },
        "test.csv",
        60,
    );
    pipeline.consumer.on_error(Some(Box::new(|_, _| ())));
    sleep(Duration::from_millis(10));
    assert!(pipeline.parser.state().data.is_empty());
    assert!(!pipeline.consumer.state().eoi);

    pipeline.reader.extend();
    let updates = wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false)
    });
    assert_eq!(
        updates,
        Updates::from_json(
            published_schema(&relation()),
            &[
                (1, r#"{"s": "foo", "b": true, "i": 10}"#),
                (1, r#"{"s": "bar", "b": false, "i": -10}"#),
            ],
        )
        .unwrap()
    );
}

/// A request for a path that the server does not have is an endpoint error, once the endpoint
/// starts.
#[engine_test]
pub fn failure(engine: &dyn ConnectorTestEngine) {
    let pipeline = setup(engine, || async { "" }, "nonexistent", 60);
    pipeline.consumer.on_error(Some(Box::new(|_, _| ())));
    sleep(Duration::from_millis(10));
    assert!(pipeline.consumer.state().endpoint_error.is_none());
    assert!(pipeline.parser.state().data.is_empty());
    assert!(!pipeline.consumer.state().eoi);

    pipeline.reader.extend();
    wait(
        || {
            pipeline.reader.queue(false);
            pipeline.consumer.state().endpoint_error.is_some()
        },
        default_timeout_ms(),
    )
    .unwrap();
}

/// Reads a stream of 100 records, one every 10 ms, pausing and resuming the endpoint in the
/// middle, with a `pause_timeout` of 0, which drops the connection, or of 60 seconds.
fn check_pause(engine: &dyn ConnectorTestEngine, with_reconnect: bool) {
    let pipeline = setup(
        engine,
        || async {
            let stream = stream! {
                for i in 0..100 {
                    let s = format!("foo,true,{i}\n");
                    yield Ok::<_, IoError>(Bytes::from(s));
                    actix_web::rt::time::sleep(Duration::from_millis(10)).await;
                }
            };
            HttpResponse::Ok().streaming::<_, IoError>(stream)
        },
        "test.csv",
        if with_reconnect { 0 } else { 60 },
    );
    pipeline.consumer.on_error(Some(Box::new(|_, _| ())));
    let rows = || pipeline.collection.len();
    sleep(Duration::from_millis(10));
    assert!(pipeline.parser.state().data.is_empty());
    assert!(!pipeline.consumer.state().eoi);

    // Records arrive one every 10 ms. Busy machines are slow, so allow two seconds for the
    // first ten.
    pipeline.reader.extend();
    let timeout_ms = 2000;
    wait(
        || {
            pipeline.reader.queue(false);
            rows() >= 10
        },
        timeout_ms,
    )
    .unwrap_or_else(|()| panic!("only {} records after {timeout_ms} ms", rows()));
    let n1 = rows();

    // More arrive 100 ms later, and at least ten more 100 ms after that.
    sleep(Duration::from_millis(100));
    pipeline.reader.queue(false);
    let n2 = rows();
    assert!(n2 > n1, "after 100 ms longer, no more records arrived");
    sleep(Duration::from_millis(100));
    pipeline.reader.queue(false);
    sleep(Duration::from_millis(10));
    let n3 = rows();
    assert!(
        n3 >= n1 + 10,
        "at least {} records should have arrived, but {n3} did",
        n1 + 10
    );

    wait(
        || {
            pipeline.reader.queue(false);
            rows() >= 50
        },
        350,
    )
    .unwrap_or_else(|()| panic!("only {} records after 350 ms", rows()));

    // A paused endpoint delivers no more records, after records already in flight.
    pipeline.reader.pause();
    sleep(Duration::from_millis(100));
    pipeline.reader.queue(false);
    let n5 = rows();
    for _ in 0..2 {
        sleep(Duration::from_millis(100));
        let n = rows();
        pipeline.reader.queue(false);
        assert_eq!(n5, n);
    }

    pipeline.reader.extend();
    if with_reconnect {
        // The endpoint reconnects, and the server starts the stream over, one record every
        // 10 ms; the endpoint skips the records it already has, so none arrive for 400 ms.
        for _ in 0..4 {
            pipeline.reader.queue(false);
            sleep(Duration::from_millis(100));
            assert_eq!(n5, rows());
        }
    } else {
        // Records arrive again at once.
        sleep(Duration::from_millis(200));
        pipeline.reader.queue(false);
        sleep(Duration::from_millis(100));
        assert!(rows() > n5);
    }

    wait(
        || {
            pipeline.reader.queue(false);
            rows() >= 100
        },
        1000,
    )
    .unwrap_or_else(|()| panic!("only {} records after 1000 ms", rows()));
    let expected: Vec<String> = (0..100)
        .map(|i| format!(r#"{{"s": "foo", "b": true, "i": {i}}}"#))
        .collect();
    let expected: Vec<(i64, &str)> = expected.iter().map(|row| (1, row.as_str())).collect();
    assert_eq!(
        pipeline.collection.updates().unwrap(),
        Updates::from_json(published_schema(&relation()), &expected).unwrap()
    );
}

/// With a `pause_timeout` of 0, a pause drops the connection, and a resume reconnects.
#[engine_test(ignore = "depends on timing")]
pub fn pause_with_reconnect(engine: &dyn ConnectorTestEngine) {
    check_pause(engine, true);
}

/// With a long `pause_timeout`, the connection stays up across a pause.
#[engine_test(ignore = "depends on timing")]
pub fn pause_without_reconnect(engine: &dyn ConnectorTestEngine) {
    check_pause(engine, false);
}
