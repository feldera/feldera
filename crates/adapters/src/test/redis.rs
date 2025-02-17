use feldera_adapterlib::catalog::RecordFormat;
use feldera_types::{
    config::PipelineConfig, format::json::JsonFlavor, program_schema::SqlIdentifier,
};
use redis::Commands;
use std::io::Write;
use tempfile::NamedTempFile;

use crate::{test::init_test_logger, Controller};

use super::{data::TestStruct, test_circuit, wait};

#[test]
fn test_redis_output() {
    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();

    let records = br#"[{"id": 1, "b": true, "i": null, "s": "foo"}, {"id": 2, "b": true, "i": null, "s": "foo"}]"#;
    let parsed: serde_json::Value = serde_json::from_slice(&records[..]).unwrap();

    temp_input_file.as_file().write_all(records).unwrap();

    let schema = TestStruct::schema();
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
  test_input1:
    stream: test_input1
    transport:
      name: file_input
      config:
        path: {}
    format:
      name: json
      config:
        update_format: raw
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
        temp_input_file.path().display()
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

    wait(
        || controller.status().pipeline_complete() || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout");

    let client = redis::Client::open("redis://localhost:6379/0").unwrap();
    let mut conn = client.get_connection().unwrap();
    let s: String = conn.get("1:foo").unwrap();
    assert_eq!(
        parsed[0],
        serde_json::from_str::<serde_json::Value>(&s).unwrap()
    );
    let s: String = conn.get("2:foo").unwrap();
    assert_eq!(
        parsed[1],
        serde_json::from_str::<serde_json::Value>(&s).unwrap()
    );

    let mut in_handle = controller
        .catalog()
        .input_collection_handle(&SqlIdentifier::new("test_input1", false))
        .unwrap()
        .handle
        .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
        .unwrap();

    let new_input = r#"{"id": 1, "b": false, "i": 10, "s": "foo"}"#;
    in_handle.insert(new_input.as_bytes()).unwrap();
    in_handle.flush();

    wait(
        || {
            let s: String = conn.get("1:foo").unwrap();
            serde_json::from_str::<serde_json::Value>(new_input).unwrap()
                == serde_json::from_str::<serde_json::Value>(&s).unwrap()
        },
        2_000,
    )
    .expect("timeout while waiting for redis entry to update");

    controller.stop().unwrap();
}
