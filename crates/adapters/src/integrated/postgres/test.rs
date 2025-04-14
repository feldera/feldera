use chrono::{NaiveDate, NaiveTime};
use dbsp::{
    circuit::CircuitConfig,
    utils::{Tup1, Tup4},
    DBSPHandle, Runtime,
};
use feldera_adapterlib::catalog::CircuitCatalog;
use feldera_sqllib::{ByteArray, SqlString, F32, F64};
use feldera_types::{
    config::PipelineConfig,
    deserialize_table_record,
    format::json::JsonFlavor,
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
    serialize_table_record,
};
use pg::PostgresTestStruct;
use postgres::NoTls;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use serial_test::serial;
use std::{collections::BTreeMap, io::Write, str::FromStr};
use tempfile::NamedTempFile;

use crate::{
    test::{test_circuit, wait, TestStruct},
    Catalog, Controller,
};

fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
}

mod pg {
    use std::collections::BTreeMap;

    use dbsp::{circuit::CircuitConfig, typed_batch::TypedBatch, utils::Tup1, DBSPHandle, Runtime};
    use feldera_sqllib::{SqlString, F32, F64};
    use feldera_types::{
        config::PipelineConfig,
        deserialize_table_record, deserialize_without_context,
        program_schema::{ColumnType, Field, Relation, SqlIdentifier},
        serialize_struct, serialize_table_record,
    };
    use num_traits::FromPrimitive;
    use postgres::{NoTls, Row};
    use rand::{distributions::Standard, prelude::Distribution, Rng};
    use rust_decimal::Decimal;

    use crate::{test::TestStruct, Catalog, Controller};

    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
    #[postgres(name = "test_struct")]
    struct TempTestStruct {
        id: Option<i32>,
        i: Option<i64>,
        b: Option<bool>,
        s: Option<String>,
    }

    impl From<TempTestStruct> for TestStruct {
        fn from(val: TempTestStruct) -> Self {
            TestStruct {
                id: val.id.unwrap() as u32,
                i: val.i,
                b: val.b.unwrap(),
                s: val.s.unwrap(),
            }
        }
    }

    #[derive(
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Clone,
        Hash,
        size_of::SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    pub(super) struct PostgresTestStruct {
        pub boolean_: bool,
        pub tinyint_: i8,
        pub smallint_: i16,
        pub int_: i32,
        pub bigint_: i64,
        pub decimal_: Decimal,
        pub float_: F32,
        pub double_: F64,
        pub varchar_: SqlString,
        pub time_: feldera_sqllib::Time,
        pub date_: feldera_sqllib::Date,
        pub timestamp_: feldera_sqllib::Timestamp,
        pub variant_: feldera_sqllib::Variant,
        pub uuid_: feldera_sqllib::Uuid,
        pub varbinary_: feldera_sqllib::ByteArray,
        pub struct_: TestStruct,
        pub string_array_: Vec<feldera_sqllib::SqlString>,
        pub struct_array_: Vec<TestStruct>,
        pub map_: BTreeMap<feldera_sqllib::SqlString, TestStruct>,
    }

    serialize_table_record!(PostgresTestStruct[19]{
        boolean_["boolean_"]: bool,
        tinyint_["tinyint_"]: i8,
        smallint_["smallint_"]: i16,
        int_["int_"]: i32,
        bigint_["bigint_"]: i64,
        decimal_["decimal_"]: Decimal,
        float_["float_"]: F32,
        double_["double_"]: F64,
        varchar_["varchar_"]: SqlString,
        time_["time_"]: feldera_sqllib::Time,
        date_["date_"]: feldera_sqllib::Date,
        timestamp_["timestamp_"]: feldera_sqllib::Timestamp,
        variant_["variant_"]: feldera_sqllib::Variant,
        uuid_["uuid_"]: feldera_sqllib::Uuid,
        varbinary_["varbinary_"]: feldera_sqllib::ByteArray,
        struct_["struct_"]: TestStruct,
        string_array_["string_array_"]: Vec<feldera_sqllib::SqlString>,
        struct_array_["struct_array_"]: Vec<TestStruct>,
        map_["map_"]: BTreeMap<feldera_sqllib::SqlString, TestStruct>
    });

    deserialize_table_record!(PostgresTestStruct["PostgresTestStruct", 19] {
        (boolean_, "boolean_", false, bool, None),
        (tinyint_, "tinyint_", false, i8, None),
        (smallint_, "smallint_", false, i16, None),
        (int_, "int_", false, i32, None),
        (bigint_, "bigint_", false, i64, None),
        (decimal_, "decimal_", false, Decimal, None),
        (float_, "float_", false, F32, None),
        (double_, "double_", false, F64, None),
        (varchar_, "varchar_", false, SqlString, None),
        (time_, "time_", false, feldera_sqllib::Time, None),
        (date_, "date_", false, feldera_sqllib::Date, None),
        (timestamp_, "timestamp_", false, feldera_sqllib::Timestamp, None),
        (variant_, "variant_", false, feldera_sqllib::Variant, None),
        (uuid_, "uuid_", false, feldera_sqllib::Uuid, None),
        (varbinary_, "varbinary_", false, feldera_sqllib::ByteArray, None),
        (struct_, "struct_", false, TestStruct, None),
        (string_array_, "string_array_", false, Vec<feldera_sqllib::SqlString>, None),
        (struct_array_, "struct_array_", false, Vec<TestStruct>, None),
        (map_, "map_", false, BTreeMap<feldera_sqllib::SqlString, TestStruct>, None)
    });

    pub(super) struct TempPgTable {
        client: postgres::Client,
        name: String,
    }

    impl TempPgTable {
        fn new(name: &str, uri: String) -> Self {
            let mut client = postgres::Client::connect(&uri, NoTls).unwrap();

            client
                .execute(
                    r#"CREATE TYPE test_struct AS (
    id INTEGER,
    b BOOL,
    i BIGINT,
    s VARCHAR
)"#,
                    &[],
                )
                .unwrap();

            client
                .execute(
                    &format!(
                        r#"
CREATE TABLE {name} (
    boolean_      BOOLEAN,
    tinyint_      SMALLINT,
    smallint_     SMALLINT,
    int_          INTEGER,
    bigint_       BIGINT,
    decimal_      DECIMAL,
    float_        REAL,
    double_       DOUBLE PRECISION,
    varchar_      TEXT,
    time_         TIME,
    date_         DATE,
    timestamp_    TIMESTAMP,
    variant_      JSONB,
    uuid_         UUID,
    varbinary_    BYTEA,
    string_array_ VARCHAR ARRAY,
    struct_       test_struct,
    struct_array_ test_struct ARRAY,
    map_          JSONB
)"#
                    ),
                    &[],
                )
                .unwrap_or_else(|_| panic!("failed to create table {name}"));

            TempPgTable {
                client,
                name: name.to_owned(),
            }
        }

        pub fn query(&mut self) -> Vec<Row> {
            self.client
                .query(&format!("SELECT * FROM {}", self.name), &[])
                .expect("failed to query table")
        }
    }

    impl Drop for TempPgTable {
        fn drop(&mut self) {
            self.client
                .execute(&format!("DROP TABLE {}", self.name), &[])
                .unwrap_or_else(|_| panic!("failed to drop table {}", self.name));

            self.client
                .execute("DROP TYPE test_struct", &[])
                .unwrap_or_else(|_| panic!("failed to drop type test_struct"));
        }
    }

    impl From<Row> for PostgresTestStruct {
        fn from(r: Row) -> Self {
            PostgresTestStruct {
                boolean_: r.get("boolean_"),
                tinyint_: r.get::<_, i16>("tinyint_") as i8,
                smallint_: r.get("smallint_"),
                int_: r.get("int_"),
                bigint_: r.get("bigint_"),
                decimal_: r.get("decimal_"),
                float_: F32::new(r.get("float_")),
                double_: F64::new(r.get("double_")),
                varchar_: SqlString::from_ref(r.get("varchar_")),
                time_: feldera_sqllib::Time::from_time(r.get("time_")),
                date_: feldera_sqllib::Date::from_date(r.get("date_")),
                timestamp_: feldera_sqllib::Timestamp::from_naiveDateTime(r.get("timestamp_")),
                variant_: feldera_sqllib::from_json_string(
                    &r.get::<_, serde_json::Value>("variant_").to_string(),
                )
                .unwrap(),
                uuid_: r.get::<_, uuid::Uuid>("uuid_").into(),
                varbinary_: feldera_sqllib::ByteArray::from_vec(r.get::<_, Vec<u8>>("varbinary_")),
                struct_: { r.get::<_, TempTestStruct>("struct_").into() },
                string_array_: r
                    .get::<_, Vec<String>>("string_array_")
                    .into_iter()
                    .map(|s| s.into())
                    .collect(),
                struct_array_: r
                    .get::<_, Vec<TempTestStruct>>("struct_array_")
                    .into_iter()
                    .map(TestStruct::from)
                    .collect(),
                map_: serde_json::from_value(r.get::<_, serde_json::Value>("map_")).unwrap(),
            }
        }
    }

    impl PostgresTestStruct {
        pub fn schema() -> Vec<Field> {
            vec![
                Field::new("boolean_".into(), ColumnType::boolean(false)),
                Field::new("tinyint_".into(), ColumnType::tinyint(false)),
                Field::new("smallint_".into(), ColumnType::smallint(false)),
                Field::new("int_".into(), ColumnType::int(false)),
                Field::new("bigint_".into(), ColumnType::bigint(false)),
                Field::new("decimal_".into(), ColumnType::decimal(10, 3, false)),
                Field::new("float_".into(), ColumnType::real(false)),
                Field::new("double_".into(), ColumnType::double(false)),
                Field::new("varchar_".into(), ColumnType::varchar(false)),
                Field::new("time_".into(), ColumnType::time(false)),
                Field::new("date_".into(), ColumnType::date(false)),
                Field::new("timestamp_".into(), ColumnType::timestamp(false)),
                Field::new("variant_".into(), ColumnType::variant(false)),
                Field::new("uuid_".into(), ColumnType::uuid(false)),
                Field::new("varbinary_".into(), ColumnType::varbinary(false)),
                Field::new(
                    "struct_".into(),
                    ColumnType::structure(false, &TestStruct::schema()),
                ),
                Field::new(
                    "string_array_".into(),
                    ColumnType::array(false, ColumnType::varchar(false)),
                ),
                Field::new(
                    "struct_array_".into(),
                    ColumnType::array(false, ColumnType::structure(false, &TestStruct::schema())),
                ),
                Field::new(
                    "map_".into(),
                    ColumnType::map(
                        false,
                        ColumnType::varchar(false),
                        ColumnType::structure(false, &TestStruct::schema()),
                    ),
                ),
            ]
        }

        pub fn create_table(name: &str, uri: String) -> TempPgTable {
            TempPgTable::new(name, uri)
        }

        pub fn test_circuit(
            config: PipelineConfig,
        ) -> (Controller, crossbeam::channel::Receiver<String>) {
            let schema = PostgresTestStruct::schema();
            let (err_sender, err_receiver) = crossbeam::channel::unbounded();
            let controller = Controller::with_config(
                move |workers| {
                    Ok({
                        let (circuit, catalog) = Runtime::init_circuit(workers, move |circuit| {
                            let mut catalog = Catalog::new();
                            let (input, hinput) = circuit.add_input_zset::<PostgresTestStruct>();

                            let input_schema = serde_json::to_string(&Relation::new(
                                "test_input1".into(),
                                schema.clone(),
                                false,
                                BTreeMap::new(),
                            ))
                            .unwrap();

                            let output_schema = serde_json::to_string(&Relation::new(
                                "test_output1".into(),
                                schema,
                                false,
                                BTreeMap::new(),
                            ))
                            .unwrap();

                            catalog.register_materialized_input_zset::<_, PostgresTestStruct>
                            (
                                input.clone(),
                                hinput,
                                &input_schema,
                            );

                            #[derive(Clone, Debug, Eq, PartialEq, Default)]
                            pub struct KeyStruct {
                                field0: i64,
                            }

                            impl From<KeyStruct> for Tup1<i64> {
                                fn from(t: KeyStruct) -> Self {
                                    Tup1::new(t.field0)
                                }
                            }
                            impl From<Tup1<i64>> for KeyStruct {
                                fn from(t: Tup1<i64>) -> Self {
                                    Self { field0: t.0 }
                                }
                            }

                            deserialize_table_record!(KeyStruct["idx", 1] {
                                (field0, "bigint_", false, i64, None)
                            });
                            serialize_table_record!(KeyStruct[1]{
                                field0["bigint_"]: i64
                            });

                            let indexed_input = input
                                .map_index(|r| (Tup1(r.bigint_), r.to_owned()));
                            catalog.register_materialized_output_zset::<_, PostgresTestStruct>(input, &output_schema);

                            catalog
                                .register_index::<Tup1<i64>, KeyStruct, PostgresTestStruct, PostgresTestStruct>(
                                    indexed_input.clone(),
                                    &SqlIdentifier::from("idx"),
                                    &SqlIdentifier::from("test_output1"),
                                    &[&SqlIdentifier::from("bigint_")],
                                )
                                .expect("failed to register index");

                            Ok(catalog)
                        })
                        .unwrap();
                        (circuit, Box::new(catalog))
                    })
                },
                &config,
                Box::new(move |e| {
                    let msg = format!("redis_output_test: error: {e}");
                    println!("{msg}");
                    err_sender.send(msg).unwrap()
                }),
            )
            .unwrap();

            (controller, err_receiver)
        }
    }

    impl Distribution<PostgresTestStruct> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> PostgresTestStruct {
            PostgresTestStruct {
                boolean_: rng.gen(),
                tinyint_: rng.gen(),
                smallint_: rng.gen(),
                int_: rng.gen(),
                bigint_: rng.gen(),
                decimal_: Decimal::from_f32(rng.gen_range::<f32, _>(-100.0..100.0))
                    .unwrap()
                    .trunc_with_scale(3),
                float_: F32::new((rng.gen::<f32>() * 1000.0).trunc() / 1000.0),
                double_: F64::new((rng.gen::<f64>() * 1000.0).trunc() / 1000.0),
                varchar_: rng.gen::<u32>().to_string().into(),
                time_: feldera_sqllib::Time::from_time(chrono::Utc::now().naive_utc().time()),
                date_: feldera_sqllib::Date::from_date(chrono::Utc::now().date_naive()),
                timestamp_: feldera_sqllib::Timestamp::from_naiveDateTime(
                    chrono::Utc::now().naive_utc(),
                ),
                variant_: feldera_sqllib::Variant::String(rng.gen::<u32>().to_string().into()),
                uuid_: uuid::Uuid::new_v4().into(),
                varbinary_: feldera_sqllib::ByteArray::from_vec(vec![rng.gen_range(0..127)]),
                struct_: rng.gen(),
                string_array_: vec![],
                struct_array_: vec![rng.gen()],
                map_: {
                    let mut map = BTreeMap::new();
                    map.insert(rng.gen::<u32>().to_string().into(), rng.gen());
                    map
                },
            }
        }
    }
}

#[test]
#[serial]
fn test_pg_insert() {
    let table_name = "test_pg_insert";
    let url = postgres_url();

    let mut data = vec![
        rand::random::<PostgresTestStruct>(),
        rand::random(),
        rand::random(),
    ];

    let mut temp_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        datum
            .serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        temp_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        temp_file.write_all(b"\n").unwrap();
    }

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
        array: false
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: postgres_output
      config:
        uri: {url}
        table: {table_name}
    index: idx
"#,
        temp_file.path().display(),
    );

    let mut table = PostgresTestStruct::create_table(table_name, url);

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let (controller, err_receiver) = PostgresTestStruct::test_circuit(config);

    controller.start();

    wait(
        || controller.pipeline_complete() || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout");

    let rows = table.query();

    let mut got = rows
        .into_iter()
        .map(PostgresTestStruct::from)
        .collect::<Vec<_>>();

    got.sort();
    data.sort();

    assert_eq!(got, data, "failed to insert data into postgres");
}

#[test]
#[serial]
fn test_pg_upsert() {
    let table_name = "test_pg_upsert";
    let url = postgres_url();

    let mut data = vec![
        rand::random::<PostgresTestStruct>(),
        rand::random(),
        rand::random(),
    ];

    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        datum
            .serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        insert_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let mut upsert_data = data
        .clone()
        .into_iter()
        .map(|mut d| {
            d.varchar_ = "updated".into();
            d
        })
        .collect::<Vec<_>>();

    let mut upsert_file = NamedTempFile::new().unwrap();

    for old in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        old.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"delete": "#)
            .unwrap();
        upsert_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

    for new in upsert_data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        new.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"insert": "#)
            .unwrap();
        upsert_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

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
        array: false
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
        array: false
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: postgres_output
      config:
        uri: {url}
        table: {table_name}
    index: idx
"#,
        insert_file.path().display(),
        upsert_file.path().display(),
    );

    let mut table = PostgresTestStruct::create_table(table_name, url);

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let (controller, err_receiver) = PostgresTestStruct::test_circuit(config);

    controller.start();

    wait(
        || {
            controller.status().num_total_processed_records() == data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");

    let rows = table.query();

    let mut got = rows
        .into_iter()
        .map(PostgresTestStruct::from)
        .collect::<Vec<_>>();

    got.sort();
    data.sort();

    assert_eq!(got, data, "failed to insert data into postgres");

    controller
        .start_input_endpoint("ups")
        .expect("failed to start upsert input file connector");

    wait(
        || controller.pipeline_complete() || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout");

    let rows = table.query();

    let mut got = rows
        .into_iter()
        .map(PostgresTestStruct::from)
        .collect::<Vec<_>>();

    got.sort();
    upsert_data.sort();

    assert_eq!(got, upsert_data, "failed to update data into postgres");
}

#[test]
#[serial]
fn test_pg_delete() {
    let table_name = "test_pg_delete";
    let url = postgres_url();

    let mut data = vec![
        rand::random::<PostgresTestStruct>(),
        rand::random(),
        rand::random(),
    ];

    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        datum
            .serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        insert_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let mut delete_file = NamedTempFile::new().unwrap();

    for old in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        old.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        delete_file
            .as_file_mut()
            .write_all(br#"{"delete": "#)
            .unwrap();
        delete_file
            .as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        delete_file.write_all(b"}\n").unwrap();
    }

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
        array: false
  dels:
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
        array: false
outputs:
  test_output1:
    stream: test_output1
    transport:
      name: postgres_output
      config:
        uri: {url}
        table: {table_name}
    index: idx
"#,
        insert_file.path().display(),
        delete_file.path().display(),
    );

    let mut table = PostgresTestStruct::create_table(table_name, url);

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let (controller, err_receiver) = PostgresTestStruct::test_circuit(config);

    controller.start();

    wait(
        || {
            controller.status().num_total_processed_records() == data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");

    let rows = table.query();

    let mut got = rows
        .into_iter()
        .map(PostgresTestStruct::from)
        .collect::<Vec<_>>();

    got.sort();
    data.sort();

    assert_eq!(got, data, "failed to insert data into postgres");

    controller
        .start_input_endpoint("dels")
        .expect("failed to start delete input file connector");

    wait(
        || controller.pipeline_complete() || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout");

    let rows = table.query();

    let got = rows
        .into_iter()
        .map(PostgresTestStruct::from)
        .collect::<Vec<_>>();

    assert!(got.is_empty(), "failed to delete data from postgres");
}

#[test]
fn test_pg_simple() {
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
            i: None,
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

                    let output_schema = serde_json::to_string(&Relation::new(
                        "test_output1".into(),
                        schema,
                        false,
                        BTreeMap::new(),
                    ))
                    .unwrap();

                    catalog.register_materialized_input_zset::<_, TestStruct>(
                        input.clone(),
                        hinput,
                        &input_schema,
                    );

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

                    let indexed_input = input.map_index(|r| (Tup1(r.id), r.to_owned()));

                    catalog
                        .register_materialized_output_zset::<_, TestStruct>(input, &output_schema);

                    catalog
                        .register_index::<Tup1<u32>, KeyStruct, TestStruct, TestStruct>(
                            indexed_input.clone(),
                            &SqlIdentifier::from(idx),
                            &SqlIdentifier::from("test_output1"),
                            &[&SqlIdentifier::from("id")],
                        )
                        .expect("failed to register index");

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

    assert_eq!(got, data, "inserting records into postgres failed");

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

    assert!(
        rows.iter().all(|r| {
            let s: String = r.get("s");
            s.as_str() == "updated"
        }),
        "updating records in postgres failed"
    );

    controller.start_input_endpoint("del").unwrap();

    println!("before: {total_processed}");
    wait(
        || {
            controller.status().num_total_processed_records() - total_processed == data.len() as u64
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timeout");
    println!(
        "before: {}",
        controller.status().num_total_processed_records()
    );

    let rows = client
        .query(&format!("SELECT * FROM {table_name}"), &[])
        .unwrap();

    assert!(rows.is_empty(), "deleting records from postgres failed");

    client
        .execute(&format!("DROP TABLE {table_name}"), &[])
        .unwrap();
}
