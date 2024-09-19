use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use arrow::array::RecordBatch;
use dbsp::utils::Tup2;
use dbsp::OrdZSet;
use feldera_types::format::json::JsonFlavor;
use feldera_types::format::parquet::ParquetEncoderConfig;
use feldera_types::program_schema::Relation;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::serialized_reader::SerializedFileReader;
use pretty_assertions::assert_eq;
use tempfile::NamedTempFile;

use crate::{
    catalog::SerBatchReader,
    format::{parquet::ParquetEncoder, Encoder},
    static_compile::seroutput::SerBatchImpl,
    test::{mock_input_pipeline, wait, MockOutputConsumer, TestStruct2, DEFAULT_TIMEOUT_MS},
};

/// Parse Parquet file into an array of `T`.
pub fn load_parquet_file<T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>>(
    path: &Path,
) -> Vec<T> {
    let file = File::open(path).unwrap();

    SerializedFileReader::new(file)
        .unwrap_or_else(|_| panic!("error opening parquet file {path:?}"))
        .into_iter()
        .map(|row| {
            let row = row.unwrap().to_json_value();
            //println!("row: {}", &row);

            T::deserialize_with_context(row, &SqlSerdeConfig::from(JsonFlavor::ParquetConverter))
                .unwrap()
        })
        .collect::<Vec<_>>()
}

#[test]
fn rel_to_schema() {
    use super::relation_to_parquet_schema;
    relation_to_parquet_schema(&TestStruct2::schema(), false).expect("Can convert");
}

#[test]
fn parquet_input() {
    // Prepare input data & pipeline
    let test_data = TestStruct2::data();
    let temp_file = NamedTempFile::new().unwrap();
    let config_str = format!(
        r#"
stream: test_input
transport:
    name: file_input
    config:
        path: {:?}
        buffer_size_bytes: 5
format:
    name: parquet
"#,
        temp_file.path().to_str().unwrap()
    );

    let batch = RecordBatch::try_new(
        TestStruct2::arrow_schema(),
        TestStruct2::make_arrow_array(&test_data),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&temp_file, TestStruct2::arrow_schema(), Some(props))
        .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");

    // Send the data through the mock pipeline
    let (endpoint, consumer, parser, zset) = mock_input_pipeline::<TestStruct2, TestStruct2>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::new("test".into(), TestStruct2::schema(), false, BTreeMap::new()),
    )
    .unwrap();
    sleep(Duration::from_millis(10));
    assert!(parser.state().data.is_empty());
    assert!(!consumer.state().eoi);
    endpoint.extend();
    wait(
        || {
            endpoint.queue();
            zset.state().flushed.len() == test_data.len()
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    // Make sure the input data matches original test structs
    for (i, upd) in zset.state().flushed.iter().enumerate() {
        assert_eq!(upd.unwrap_insert(), &test_data[i]);
    }
}

#[test]
fn parquet_output() {
    let buffer = Arc::new(Mutex::new(Vec::with_capacity(4096)));
    let consumer = MockOutputConsumer::with_buffer(buffer.clone());
    let _consumer_data = consumer.data.clone();

    let config = ParquetEncoderConfig {
        buffer_size_records: usize::MAX,
    };

    let test_data = TestStruct2::data();
    let mut encoder = ParquetEncoder::new(
        Box::new(consumer),
        config,
        Relation::new(
            "TestStruct2".into(),
            TestStruct2::schema(),
            false,
            BTreeMap::new(),
        ),
    )
    .expect("Can't create encoder");
    let zset = OrdZSet::from_keys(
        (),
        vec![Tup2(test_data[0].clone(), 2), Tup2(test_data[1].clone(), 1)],
    );

    let zset = &SerBatchImpl::<_, TestStruct2, ()>::new(zset) as &dyn SerBatchReader;
    encoder.consumer().batch_start(0);
    encoder.encode(zset).unwrap();
    encoder.consumer().batch_end();

    // Verify output buffer...
    // Construct the expected file manually:
    let test_denorm = vec![
        test_data[0].clone(),
        test_data[0].clone(),
        test_data[1].clone(),
    ];
    let batch = RecordBatch::try_new(
        TestStruct2::arrow_schema(),
        TestStruct2::make_arrow_array(&test_denorm),
    )
    .expect("RecordBatch creation should succeed");
    let props = WriterProperties::builder().build();

    let mut expected_buffer: Vec<u8> = vec![];
    let mut expected_buffer_cursor = Cursor::new(&mut expected_buffer);
    let mut writer = ArrowWriter::try_new(
        &mut expected_buffer_cursor,
        TestStruct2::arrow_schema(),
        Some(props),
    )
    .expect("Writer creation should succeed");
    writer
        .write(&batch)
        .expect("Writing to parquet should succeed");
    writer.close().expect("Closing the writer should succeed");
    debug_parquet_buffer(
        buffer
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_k, v)| v.clone())
            .flatten()
            .collect(),
    );

    let buffer_copy = buffer
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_k, v)| v.clone())
        .flatten()
        .collect::<Vec<_>>();

    assert_eq!(expected_buffer, buffer_copy);
}

fn debug_parquet_buffer(buffer: Vec<u8>) {
    use bytes::Bytes;
    use parquet::file::reader::FileReader;

    let _r = env_logger::try_init();
    let buffer_copy = Bytes::from(buffer);
    let reader = SerializedFileReader::new(buffer_copy).expect("Reader creation should succeed");
    let row_iter = reader
        .get_row_iter(None)
        .expect("Row iterator creation should succeed");
    for maybe_record in row_iter {
        let record = maybe_record.expect("Record should be read successfully");
        log::info!("record = {:?}", record.to_string());
    }
}
