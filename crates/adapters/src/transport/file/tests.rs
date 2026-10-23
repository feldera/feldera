//! The file input connector: a file read once, and a file followed as it grows.

use std::collections::BTreeMap;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;

use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates, wait_for_rows};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::json;
use tempfile::NamedTempFile;
use url::Url;

use crate::test::{input_pipeline, published_schema};
use feldera_adapterlib::test::{default_timeout_ms, wait};

/// The table that the cases read into.
pub fn relation() -> Relation {
    Relation::new(
        "file_input".into(),
        vec![
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The rows that each case writes.
fn rows() -> Updates {
    Updates::from_json(
        published_schema(&relation()),
        &[
            (1, r#"{"s": "foo", "b": true, "i": 10}"#),
            (1, r#"{"s": "bar", "b": false, "i": -10}"#),
        ],
    )
    .unwrap()
}

/// A CSV file with a header row and `|` delimiters, read through a 5-byte buffer, arrives in
/// order once the endpoint starts.
pub fn csv_file_nofollow(engine: &dyn ConnectorTestEngine) {
    let file = NamedTempFile::new().unwrap();
    file.as_file()
        .write_all(b"s|b|i\nfoo|true|10\nbar|false|-10\n")
        .unwrap();
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "file_input",
            "config": { "path": file.path(), "buffer_size_bytes": 5 }
        },
        "format": { "name": "csv", "config": { "delimiter": "|", "headers": true } }
    }))
    .unwrap();
    let pipeline = input_pipeline(engine, &relation(), &InputOptions::default(), config).unwrap();

    sleep(Duration::from_millis(10));
    assert!(!pipeline.consumer.state().eoi);

    pipeline.reader.extend();
    let updates = wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false)
    });
    assert_eq!(updates, rows());
}

/// A followed file, named by a `file://` URL, delivers each append in order, and a record
/// that does not parse is an error that inserts nothing.
pub fn csv_file_follow(engine: &dyn ConnectorTestEngine) {
    let file = NamedTempFile::new().unwrap();
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "file_input",
            "config": {
                "path": Url::from_file_path(file.path()).unwrap(),
                "buffer_size_bytes": 5,
                "follow": true
            }
        },
        "format": { "name": "csv" }
    }))
    .unwrap();
    let pipeline = input_pipeline(engine, &relation(), &InputOptions::default(), config).unwrap();
    pipeline.reader.extend();

    for _ in 0..10 {
        file.as_file()
            .write_all(b"foo,true,10\nbar,false,-10\n")
            .unwrap();
        file.as_file().flush().unwrap();
        sleep(Duration::from_millis(10));
        assert!(!pipeline.consumer.state().eoi);

        let updates = wait_for_rows(pipeline.collection.as_ref(), 2, || {
            pipeline.reader.queue(false)
        });
        assert_eq!(updates, rows());
        pipeline.consumer.reset();
        pipeline.collection.reset();
    }

    pipeline.consumer.on_error(Some(Box::new(|_, _| {})));
    pipeline.parser.on_error(Some(Box::new(|_, _| {})));
    file.as_file().write_all(b"xxx\n").unwrap();
    file.as_file().flush().unwrap();
    wait(
        || {
            pipeline.reader.queue(false);
            let state = pipeline.parser.state();
            state
                .parser_result
                .as_ref()
                .is_some_and(|errors| !errors.is_empty())
        },
        default_timeout_ms(),
    )
    .unwrap();
    assert!(pipeline.collection.is_empty());
    pipeline.reader.disconnect();
}
