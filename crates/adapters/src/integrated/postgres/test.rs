use std::{collections::BTreeMap, io::Write};

use dbsp::{
    utils::{Tup1, Tup4},
    Runtime,
};
use feldera_types::{
    config::PipelineConfig,
    deserialize_table_record,
    program_schema::{Relation, SqlIdentifier},
    serialize_table_record,
};
use postgres::NoTls;
use serde_json::json;
use tempfile::NamedTempFile;

use crate::{
    test::{test_circuit, wait, TestStruct},
    Catalog, Controller,
};

fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
}

#[test]
fn test_postgres_simple() {
    let url = postgres_url();
    let table_name = "simple_test";

    let mut client = postgres::Client::connect(&url, NoTls).expect("failed to connect to postgres");
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS {table_name} (
    id int primary key,
    b bool not null,
    i int8,
    s varchar not null
)"#
            ),
            &[],
        )
        .expect("failed to create test table in postgres");

    client
        .execute(&format!("TRUNCATE {table_name}"), &[])
        .unwrap();

    let input_file = NamedTempFile::new().unwrap();
    let data = vec![
        TestStruct {
            id: 1,
            b: true,
            i: Some(2),
            s: "test".to_owned(),
        },
        TestStruct {
            id: 2,
            b: false,
            i: Some(1),
            s: "test".to_owned(),
        },
    ];

    input_file
        .as_file()
        .write_all(&serde_json::to_vec(&data).unwrap())
        .unwrap();

    let upsert_file = NamedTempFile::new().unwrap();
    let upsert_data: Vec<serde_json::Value> = data
        .clone()
        .into_iter()
        .flat_map(|d| {
            let updated = {
                let mut n = d.clone();
                n.s = "updated".to_owned();
                n
            };
            [
                json!({
                    "delete": d,
                }),
                json!({
                    "insert": updated,
                }),
            ]
        })
        .collect();

    upsert_file
        .as_file()
        .write_all(&serde_json::to_vec(&upsert_data).unwrap())
        .unwrap();

    let delete_file = NamedTempFile::new().unwrap();
    delete_file
        .as_file()
        .write_all(
            &serde_json::to_vec(
                &data
                    .clone()
                    .into_iter()
                    .map(|d| json!({"delete": d}))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap();

    let idx = "v1_idx";

    let schema = TestStruct::schema();
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
  ins:
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
  ups:
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
  del:
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
      name: postgres_output
      config:
        uri: {url}
        table: {table_name}
    index: {idx}
"#,
        input_file.path().display(),
        upsert_file.path().display(),
        delete_file.path().display(),
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = Controller::with_config(
        move |workers| {
            Ok({
                let (circuit, catalog) = Runtime::init_circuit(workers, move |circuit| {
                    let mut catalog = Catalog::new();
                    let (input, hinput) = circuit.add_input_zset::<TestStruct>();

                    let input_schema = serde_json::to_string(&Relation::new(
                        "test_input1".into(),
                        schema.clone(),
                        false,
                        BTreeMap::new(),
                    ))
                    .unwrap();
                    catalog.register_materialized_input_zset(input.clone(), hinput, &input_schema);

                    let indexed_input =
                        input.map_index(|r| (Tup1(r.id), Tup4(r.id, r.s.clone(), r.b, r.i)));

                    let output_schema = serde_json::to_string(&Relation::new(
                        "test_output1".into(),
                        schema,
                        false,
                        BTreeMap::new(),
                    ))
                    .unwrap();

                    catalog.register_materialized_output_zset(input, &output_schema);

                    #[derive(Clone, Debug, Eq, PartialEq, Default)]
                    pub struct KeyStruct {
                        field0: u32,
                    }
                    impl From<KeyStruct> for Tup1<u32> {
                        fn from(t: KeyStruct) -> Self {
                            Tup1::new(t.field0)
                        }
                    }
                    impl From<Tup1<u32>> for KeyStruct {
                        fn from(t: Tup1<u32>) -> Self {
                            Self { field0: t.0 }
                        }
                    }
                    deserialize_table_record!(KeyStruct["v1_idx", 1] {
                        (field0, "id", false, u32, None)
                    });
                    serialize_table_record!(KeyStruct[1]{
                        field0["id"]: u32
                    });
                    #[derive(Clone, Debug, Eq, PartialEq, Default)]
                    pub struct ValueStruct {
                        field0: u32,
                        field1: String,
                        field2: bool,
                        field3: Option<i64>,
                    }
                    impl From<ValueStruct> for Tup4<u32, String, bool, Option<i64>> {
                        fn from(t: ValueStruct) -> Self {
                            Tup4::new(t.field0, t.field1, t.field2, t.field3)
                        }
                    }
                    impl From<Tup4<u32, String, bool, Option<i64>>> for ValueStruct {
                        fn from(t: Tup4<u32, String, bool, Option<i64>>) -> Self {
                            Self {
                                field0: t.0,
                                field1: t.1,
                                field2: t.2,
                                field3: t.3,
                            }
                        }
                    }
                    deserialize_table_record!(ValueStruct["test_output1", 4] {
                        (field0, "id", false, u32, None),
                        (field1, "s", false, String, None),
                        (field2, "b", false, bool, None),
                        (field3, "i", false, Option<i64>, Some(None))
                    });
                    serialize_table_record!(ValueStruct[4]{
                        field0["id"]: u32,
                        field1["s"]: String,
                        field2["b"]: bool,
                        field3["i"]: Option<i64>
                    });
                    catalog.register_index::<
                        Tup1<u32>,
                        KeyStruct,
                        Tup4<u32, String, bool, Option<i64>>,
                        ValueStruct>(
                    indexed_input.clone(), &SqlIdentifier::from(idx), &SqlIdentifier::from("test_output1"),
                    &[&SqlIdentifier::from("id")]).expect("failed to register index");

                    Ok(catalog)
                })
                .unwrap();
                (circuit, Box::new(catalog))
            })
        },
        &config,
        Box::new(move |e| {
            let msg = format!("postgres_output_test: error: {e}");
            println!("{msg}");
            err_sender.send(msg).unwrap()
        }),
    )
    .unwrap();

    controller.start();

    wait(
        || {
            controller.status().num_total_processed_records() == data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");
    let total_processed = controller.status().num_total_processed_records();

    let rows = client
        .query(&format!("SELECT * FROM {table_name}"), &[])
        .unwrap();
    let got: Vec<TestStruct> = rows
        .into_iter()
        .map(|row| {
            let id: i32 = row.get(0);
            let b: bool = row.get(1);
            let i: Option<i64> = row.get(2);
            let s: String = row.get(3);

            TestStruct {
                id: id as u32,
                b,
                i,
                s,
            }
        })
        .collect();

    assert_eq!(got, data);

    controller.start_input_endpoint("ups").unwrap();

    wait(
        || {
            controller.status().num_total_processed_records() - total_processed
                == upsert_data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");
    let total_processed = controller.status().num_total_processed_records();

    let total_processed = controller.status().num_total_processed_records();

    let rows = client
        .query(&format!("SELECT * FROM {table_name}"), &[])
        .unwrap();

    assert!(rows.iter().all(|r| {
        let s: String = r.get("s");
        s.as_str() == "updated"
    }));

    controller.start_input_endpoint("del").unwrap();

    wait(
        || {
            controller.status().num_total_processed_records() - total_processed == data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");

    let rows = client
        .query(&format!("SELECT * FROM {table_name}"), &[])
        .unwrap();

    assert!(rows.is_empty());

    client
        .execute(&format!("DROP TABLE {table_name}"), &[])
        .unwrap();
}
