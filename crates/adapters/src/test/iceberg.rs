//! See crates/iceberg/srd/tests/README.md for a description of the Iceberg test harness.

use crate::{
    test::{file_to_zset, wait},
    Controller,
};
use crossbeam::channel::Receiver;
use dbsp::DBData;
use feldera_sqllib::{ByteArray, F32, F64};
use feldera_types::{
    program_schema::Field,
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
};
use serde_json::json;

use std::{collections::HashMap, time::Instant};
use tempfile::NamedTempFile;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(feature = "iceberg-tests-fs")]
use std::io::Write;

use super::{test_circuit, IcebergTestStruct};

fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .try_init();
}

#[cfg(feature = "iceberg-tests-fs")]
/// Store test dataset in an ndjson file
fn data_to_ndjson(data: Vec<IcebergTestStruct>) -> NamedTempFile {
    println!("delta_table_output_test: preparing input file");
    let mut file = NamedTempFile::new().unwrap();
    for v in data.iter() {
        let buffer: Vec<u8> = Vec::new();
        let mut serializer = serde_json::Serializer::new(buffer);
        v.serialize_with_context(&mut serializer, &SqlSerdeConfig::default())
            .unwrap();
        file.as_file_mut()
            .write_all(&serializer.into_inner())
            .unwrap();
        file.write_all(b"\n").unwrap();
    }

    file
}

/// Read a snapshot of an Iceberg table with records of type `T` to a temporary JSON file.
fn iceberg_snapshot_to_json<T>(schema: &[Field], config: &HashMap<String, String>) -> NamedTempFile
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    let start = Instant::now();
    let json_file = NamedTempFile::new().unwrap();
    println!(
        "iceberg_snapshot_to_json: writing output to {}",
        json_file.path().display()
    );

    let mut config = config.clone();
    config.insert("mode".to_string(), "snapshot".to_string());

    let (input_pipeline, err_receiver) =
        iceberg_input_pipeline::<T>(schema, &config, &json_file.path().display().to_string());
    input_pipeline.start();
    wait(
        || input_pipeline.status().pipeline_complete() || err_receiver.len() > 0,
        400_000,
    )
    .expect("timeout");

    assert!(err_receiver.is_empty());

    input_pipeline.stop().unwrap();

    info!("Read Iceberg snapshot in {:?}", start.elapsed());

    json_file
}

/// Build a pipeline that reads from an Iceberg table and writes to a JSON file.
fn iceberg_input_pipeline<T>(
    schema: &[Field],
    config: &HashMap<String, String>,
    output_file_path: &str,
) -> (Controller, Receiver<String>)
where
    T: DBData
        + SerializeWithContext<SqlSerdeConfig>
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Sync,
{
    init_logging();

    // Create controller.
    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "file_output",
            "config": {
              "path": output_file_path
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete"
            }
          }
        }
      },
      "inputs": {
        "test_input1": {
          "stream": "test_input1",
          "transport": {
              "name": "iceberg_input",
              "config": config
          }
        }
      }
    }))
    .unwrap();

    let schema = schema.to_vec();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();

    let controller = Controller::with_config(
        move |workers| Ok(test_circuit::<T>(workers, &schema, &[None])),
        &config,
        Box::new(move |e, _| {
            let msg = format!("iceberg_input_test: error: {e}");
            println!("{}", msg);
            err_sender.send(msg).unwrap()
        }),
    )
    .unwrap();

    (controller, err_receiver)
}

/// Generate up to `max_records` _unique_ records.
#[cfg(feature = "iceberg-tests-fs")]
fn data(n_records: usize) -> Vec<IcebergTestStruct> {
    let mut result = Vec::with_capacity(n_records);

    let mut time =
        chrono::NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap();

    for i in 0..n_records {
        result.push(IcebergTestStruct {
            b: i % 2 != 0,
            i: i as i32,
            l: i as i64,
            r: F32::from(i as f32),
            d: F64::from(i as f64),
            dec: feldera_sqllib::SqlDecimal::<10, 3>::new(i as i128, 2).unwrap(),
            dt: feldera_sqllib::Date::from_date(time.date()),
            tm: feldera_sqllib::Time::from_time(time.time()),
            ts: feldera_sqllib::Timestamp::from_naiveDateTime(time),
            s: format!("s{i}"),
            // uuid: ByteArray::new([0u8; 16].as_slice()),
            fixed: ByteArray::new([0u8; 5].as_slice()),
            varbin: ByteArray::new([0u8; 5].as_slice()),
        });

        time += std::time::Duration::from_secs(1);
    }

    result
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_unordered() {
    iceberg_localfs_input_test(&[], &|_| true);
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_ordered() {
    iceberg_localfs_input_test(
        &[("timestamp_column".to_string(), "ts".to_string())],
        &|_| true,
    );
}

#[test]
#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test_ordered_with_filter() {
    iceberg_localfs_input_test(
        &[
            ("timestamp_column".to_string(), "ts".to_string()),
            ("snapshot_filter".to_string(), "i >= 10000".to_string()),
        ],
        &|x| x.i >= 10000,
    );
}

#[cfg(feature = "iceberg-tests-fs")]
fn iceberg_localfs_input_test(
    extra_config: &[(String, String)],
    filter: &dyn Fn(&IcebergTestStruct) -> bool,
) {
    let data = data(1_000_000);

    let table_dir = tempfile::TempDir::new().unwrap();
    let table_path = table_dir.path().display().to_string();

    let ndjson_file = data_to_ndjson(data.clone());
    println!("wrote test data to {}", ndjson_file.path().display());

    // Uncomment to inspect output parquet files produced by the test.
    std::mem::forget(table_dir);

    let script_path = "../iceberg/src/test/create_test_table_s3.py";

    // Run the Python script using the Python interpreter
    let output = std::process::Command::new("python3")
        .arg(script_path)
        .arg("--catalog=sql")
        .arg(format!("--warehouse-path={table_path}"))
        .arg(format!("--json-file={}", ndjson_file.path().display()))
        .output()
        .map_err(|e| {
            format!("Error running '{script_path}' script to generate an Iceberg table: {e}")
        })
        .unwrap();

    if !output.status.success() {
        panic!(
            "'{script_path}' failed (status: {}), stdout:{}\nstderr:{}",
            output.status,
            &String::from_utf8(output.stdout).unwrap(),
            &String::from_utf8(output.stderr).unwrap()
        );
    }

    // The script should print table metadata location on the last line.
    let metadata_path = String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .last()
        .unwrap()
        .to_string();

    let mut json_file = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[("metadata_location".to_string(), metadata_path.to_string())]
            .into_iter()
            .chain(extra_config.into_iter().cloned())
            .collect::<HashMap<_, _>>(),
    );

    let expected_zset = dbsp::OrdZSet::from_tuples(
        (),
        data.clone()
            .into_iter()
            .filter(filter)
            .map(|x| dbsp::utils::Tup2(dbsp::utils::Tup2(x, ()), 1))
            .collect(),
    );
    let zset = file_to_zset::<IcebergTestStruct>(
        json_file.as_file_mut(),
        "json",
        r#"update_format: "insert_delete""#,
    );

    assert_eq!(zset, expected_zset);
}

#[test]
#[cfg(feature = "iceberg-tests-glue")]
fn iceberg_glue_s3_input_test() {
    use dbsp::trace::BatchReader;
    // Read delta table unordered.
    let mut json_file = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[
            ("catalog_type".to_string(), "glue".to_string()),
            (
                "glue.warehouse".to_string(),
                "s3://feldera-iceberg-test/".to_string(),
            ),
            (
                "table_name".to_string(),
                "iceberg_test.test_table".to_string(),
            ),
            (
                "glue.access-key-id".to_string(),
                std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            ),
            (
                "glue.secret-access-key".to_string(),
                std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            ),
            ("glue.region".to_string(), "us-east-1".to_string()),
            (
                "s3.access-key-id".to_string(),
                std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            ),
            (
                "s3.secret-access-key".to_string(),
                std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    );

    let zset = file_to_zset::<IcebergTestStruct>(
        json_file.as_file_mut(),
        "json",
        r#"update_format: "insert_delete""#,
    );

    // The data for this test is generated by the Python script, we don't know the
    // exact set of records in the dataset.
    assert_eq!(zset.len(), 2000000);
}

#[test]
#[cfg(feature = "iceberg-tests-rest")]
fn iceberg_rest_s3_input_test() {
    use dbsp::trace::BatchReader;

    // Read delta table unordered.
    let mut json_file = iceberg_snapshot_to_json::<IcebergTestStruct>(
        &IcebergTestStruct::schema_with_lateness(),
        &[
            ("catalog_type".to_string(), "rest".to_string()),
            ("rest.uri".to_string(), "http://localhost:8181".to_string()),
            (
                "rest.warehouse".to_string(),
                "s3://feldera-iceberg-test/".to_string(),
            ),
            (
                "table_name".to_string(),
                "iceberg_test.test_table".to_string(),
            ),
            (
                "s3.access-key-id".to_string(),
                std::env::var("ICEBERG_TEST_AWS_ACCESS_KEY_ID").unwrap(),
            ),
            (
                "s3.secret-access-key".to_string(),
                std::env::var("ICEBERG_TEST_AWS_SECRET_ACCESS_KEY").unwrap(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    );

    let zset = file_to_zset::<IcebergTestStruct>(
        json_file.as_file_mut(),
        "json",
        r#"update_format: "insert_delete""#,
    );

    assert_eq!(zset.len(), 2000000);
    //assert_eq!(zset, expected_zset);
}
