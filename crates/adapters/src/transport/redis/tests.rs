//! The Redis output connector in a pipeline: the rows of a view, written to Redis as JSON values
//! under keys made of their key fields and deleted with them, and the formats that the connector
//! refuses.
//!
//! The cases need a Redis server at `REDIS_URL`, by default `redis://localhost:6379/0`.

use std::collections::HashSet;
use std::env;
use std::io::Write;

use feldera_adapterlib::test::wait;
use feldera_types::program_schema::Relation;
use redis::Commands;
use serde_json::{Value, json};
use tempfile::NamedTempFile;

use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::relations;

/// The view of [`redis_output`]: a column of each type that the JSON format writes.
pub fn relation() -> Relation {
    relations::every_type("redis")
}

/// The view of [`redis_output_fail`], with the columns of [`relations::basic`].
pub fn basic_relation() -> Relation {
    relations::basic("redis_basic")
}

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or("redis://localhost:6379/0".to_string())
}

/// Returns a row of [`relation`] as the JSON encoder writes it in the default flavor.
fn record() -> Value {
    let basic = |id: i64, b: bool, i: Option<i64>| json!({ "id": id, "b": b, "i": i, "s": "test" });
    json!({
        "bigint": 1,
        "binary": [0, 1, 2],
        "boolean": false,
        "date": "1970-01-02",
        "decimal_10_3": 1.23,
        "double": 1.123,
        "float": 1.123,
        "int": 1,
        "smallint": 2,
        "string": "test",
        "unused": null,
        "timestamp_ntz": "1970-01-01 00:00:00.001",
        "timestamp_tz": "1970-01-01 00:00:00.001+00:00",
        "tinyint": 1,
        "string_array": ["a", "b"],
        "struct1": basic(1, true, None),
        "struct_array": [basic(1, true, None), basic(2, false, Some(1))],
        "string_string_map": { "a": "b", "c": "d" },
        "string_struct_map": { "a": basic(1, true, None), "b": basic(2, false, Some(1)) },
        "variant": { "foo": "bar" },
        "uuid": "01010101-0101-0101-0101-010101010101",
    })
}

pub fn redis_output(engine: &dyn PipelineTestEngine) {
    let temp_input_file1 = NamedTempFile::new().unwrap();
    let temp_input_file2 = NamedTempFile::new().unwrap();

    let serialized = record();

    let insert_records = json!([{
      "insert": serialized
    }]);
    let delete_records = json!([{
      "delete": serialized
    }]);

    temp_input_file1
        .as_file()
        .write_all(&serde_json::to_vec(&insert_records).unwrap())
        .unwrap();

    temp_input_file2
        .as_file()
        .write_all(&serde_json::to_vec(&delete_records).unwrap())
        .unwrap();

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "file1": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": temp_input_file1.path(),
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete",
              "array": true
            }
          }
        },
        "file2": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": temp_input_file2.path(),
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete",
              "array": true
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "redis_output",
            "config": {
              "connection_string": redis_url(),
              "key_separator": ":"
            }
          },
          "format": {
            "name": "json",
            "config": {
              "key_fields": [
                "struct1",
                "decimal_10_3"
              ]
            }
          }
        }
      }
    }))
    .unwrap();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |e, _| {
                let msg = format!("redis_output_test: error: {e}");
                println!("{msg}");
                err_sender.send(msg).unwrap()
            }),
        )
        .unwrap();

    controller.start();
    controller.start_input_endpoint("file1").unwrap();

    wait(
        || {
            controller.status().num_total_processed_records()
                == insert_records.as_array().unwrap().len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");

    let client = redis::Client::open(redis_url()).unwrap();
    let mut conn = client.get_connection().unwrap();

    wait(
        || {
            let keys = conn.keys::<_, HashSet<String>>("*").unwrap();
            insert_records.as_array().unwrap().iter().all(|v| {
                let key = format!("{}:{}", v["insert"]["struct1"], v["insert"]["decimal_10_3"]);
                keys.contains(&key)
            })
        },
        5_000,
    )
    .expect("timeout while waiting for redis entry to update");

    for record in insert_records.as_array().unwrap() {
        let key = format!(
            "{}:{}",
            record["insert"]["struct1"], record["insert"]["decimal_10_3"]
        );
        let s: String = conn.get(&key).expect("error reading from redis");
        assert_eq!(
            record["insert"],
            serde_json::from_str::<serde_json::Value>(&s).unwrap()
        );
    }

    controller.start_input_endpoint("file2").unwrap();

    wait(
        || {
            let keys = conn.keys::<_, HashSet<String>>("*").unwrap();
            delete_records.as_array().unwrap().iter().all(|v| {
                let key = format!("{}:{}", v["delete"]["struct1"], v["delete"]["decimal_10_3"]);
                !keys.contains(&key)
            })
        },
        5_000,
    )
    .expect("timeout while waiting for redis entry to update");

    controller.stop().unwrap();
}

pub fn redis_output_fail(engine: &dyn PipelineTestEngine) {
    let temp_input_file1 = NamedTempFile::new().unwrap();

    let val = json!({
        "id": 1,
        "b": false,
        "i": null,
        "s": "test"
    });

    temp_input_file1
        .as_file()
        .write_all(&serde_json::to_vec(&val).unwrap())
        .unwrap();

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "file1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file1.path()
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                        "array": false
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "redis_output",
                    "config": {
                        "connection_string": redis_url(),
                        "key_separator": ":"
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {
                        "key_fields": [
                            "s",
                            "id"
                        ]
                    }
                }
            }
        }
    }))
    .unwrap();

    let Err(err) = engine.controller(
        &TestCircuit::identity(basic_relation()),
        &config,
        Box::new(move |e, _| {
            let msg = format!("redis_output_test: error: {e}");
            println!("{msg}");
        }),
    ) else {
        panic!("redis connector with csv format did not panic as expected");
    };

    assert!(err.to_string().contains("not yet supported"));
}
