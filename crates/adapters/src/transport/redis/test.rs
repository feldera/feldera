use feldera_types::config::PipelineConfig;
use redis::Commands;
use serde_json::json;
use std::{collections::HashSet, io::Write};
use tempfile::NamedTempFile;

use crate::{
    test::{data::TestStruct, test_circuit, wait},
    Controller,
};

#[test]
fn test_redis_output() {
    let temp_input_file1 = NamedTempFile::new().unwrap();
    let temp_input_file2 = NamedTempFile::new().unwrap();

    let insert_records = json!([
        {
            "insert": {
                "id": 1,
                "b": true,
                "i": null,
                "s": "foo"
            }
        },
        {
            "insert": {
                "id": 2,
                "b": true,
                "i": null,
                "s": "bar"
            }
        }
    ]);

    let delete_records = json!([
        {
            "delete": {
                "id": 1,
                "b": true,
                "i": null,
                "s": "foo"
            }
        },
        {
            "delete": {
                "id": 2,
                "b": true,
                "i": null,
                "s": "bar"
            }
        }
    ]);

    temp_input_file1
        .as_file()
        .write_all(&serde_json::to_vec(&insert_records).unwrap())
        .unwrap();

    temp_input_file2
        .as_file()
        .write_all(&serde_json::to_vec(&delete_records).unwrap())
        .unwrap();

    let schema = TestStruct::schema();
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
  file1:
    paused: true
    stream: test_input1
    transport:
      name: file_input
      config:
        path: {}
    format:
      name: json
      config:
        update_format: insert_delete
        array: true
  file2:
    paused: true
    stream: test_input1
    transport:
      name: file_input
      config:
        path: {}
    format:
      name: json
      config:
        update_format: insert_delete
        array: true
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: redis_output
      config:
        connection_string: redis://localhost:6379/0
    format:
      name: json
      config:
        key_fields:
        - id
        - s
        key_separator: ':'
"#,
        temp_input_file1.path().display(),
        temp_input_file2.path().display(),
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let schema = schema.to_vec();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    let controller = Controller::with_config(
        move |workers| Ok(test_circuit::<TestStruct>(workers, &schema)),
        &config,
        Box::new(move |e| {
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

    let client = redis::Client::open("redis://localhost:6379/0").unwrap();
    let mut conn = client.get_connection().unwrap();
    for record in insert_records.as_array().unwrap() {
        let key = format!(
            "{}:{}",
            record["insert"]["id"],
            record["insert"]["s"].as_str().unwrap()
        );
        let s: String = conn.get(&key).unwrap();
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
                let key = format!(
                    "{}:{}",
                    v["delete"]["id"],
                    v["delete"]["s"].as_str().unwrap()
                );
                !keys.contains(&key)
            })
        },
        1_000,
    )
    .expect("timeout while waiting for redis entry to update");

    controller.stop().unwrap();
}
