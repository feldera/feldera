use feldera_sqllib::{ByteArray, Date, SqlString, Timestamp, Uuid, Variant, F32, F64};
use feldera_types::{
    config::PipelineConfig,
    format::json::JsonFlavor,
    serde_with_context::{SerializeWithContext, SqlSerdeConfig},
};
use redis::Commands;
use rust_decimal::Decimal;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashSet},
    env,
    io::Write,
    str::FromStr,
};
use tempfile::NamedTempFile;

use crate::{
    test::{data::TestStruct, test_circuit, wait, DeltaTestStruct},
    Controller,
};

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or("redis://localhost:6379/0".to_string())
}

#[test]
fn test_redis_output() {
    let temp_input_file1 = NamedTempFile::new().unwrap();
    let temp_input_file2 = NamedTempFile::new().unwrap();

    let data = DeltaTestStruct {
        bigint: 1,
        binary: ByteArray::new(&[0, 1, 2]),
        boolean: false,
        date: Date::new(1),
        decimal_10_3: Decimal::new(123, 2),
        double: F64::from_str("1.123").unwrap(),
        float: F32::from_str("1.123").unwrap(),
        int: 1,
        smallint: 2,
        string: "test".to_owned(),
        unused: None,
        timestamp_ntz: Timestamp::new(1),
        tinyint: 1,
        string_array: vec!["a".to_owned(), "b".to_owned()],
        struct1: TestStruct {
            id: 1,
            b: true,
            i: None,
            s: "test".to_owned(),
        },
        struct_array: vec![
            TestStruct {
                id: 1,
                b: true,
                i: None,
                s: "test".to_owned(),
            },
            TestStruct {
                id: 2,
                b: false,
                i: Some(1),
                s: "test".to_owned(),
            },
        ],
        string_string_map: BTreeMap::from_iter([
            ("a".to_owned(), "b".to_owned()),
            ("c".to_owned(), "d".to_owned()),
        ]),
        string_struct_map: BTreeMap::from_iter([
            (
                "a".to_owned(),
                TestStruct {
                    id: 1,
                    b: true,
                    i: None,
                    s: "test".to_owned(),
                },
            ),
            (
                "b".to_owned(),
                TestStruct {
                    id: 2,
                    b: false,
                    i: Some(1),
                    s: "test".to_owned(),
                },
            ),
        ]),
        variant: Variant::Map(
            std::iter::once((
                (Variant::String(SqlString::from_ref("foo"))),
                Variant::String(SqlString::from_ref("bar")),
            ))
            .collect::<BTreeMap<Variant, Variant>>()
            .into(),
        ),
        uuid: Uuid::from_bytes([1; 16]),
    };

    let serialized = data
        .serialize_with_context(
            serde_json::value::Serializer,
            &SqlSerdeConfig::from(JsonFlavor::default()),
        )
        .unwrap();

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

    let schema = DeltaTestStruct::schema();
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
        connection_string: {}
        key_separator: ':'
    format:
      name: json
      config:
        key_fields:
        - struct1
        - decimal_10_3
"#,
        temp_input_file1.path().display(),
        temp_input_file2.path().display(),
        redis_url()
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let schema = schema.to_vec();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    let controller = Controller::with_config(
        move |workers| Ok(test_circuit::<DeltaTestStruct>(workers, &schema, None)),
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

#[test]
fn test_redis_output_fail() {
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

    let schema = TestStruct::schema();
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
  file1:
    stream: test_input1
    transport:
      name: file_input
      config:
        path: {}
    format:
      name: json
      config:
        update_format: raw
        array: false
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: redis_output
      config:
        connection_string: {}
        key_separator: ':'
    format:
      name: csv
      config:
        key_fields:
        - s
        - id
"#,
        temp_input_file1.path().display(),
        redis_url()
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let schema = schema.to_vec();

    let Err(err) = Controller::with_config(
        move |workers| Ok(test_circuit::<TestStruct>(workers, &schema, None)),
        &config,
        Box::new(move |e| {
            let msg = format!("redis_output_test: error: {e}");
            println!("{msg}");
        }),
    ) else {
        panic!("redis connector with csv format did not panic as expected");
    };

    assert!(err.to_string().contains("not yet supported"));
}
