//! Compile beside the unmodified classic generated Unicode circuit.

use super::circuit0;
use dbsp::{
    OrdZSet, Runtime,
    circuit::{CircuitConfig, StepSize},
};
use dbsp_connectors::format::{InputBuffer, OutputConsumer, get_input_format, get_output_format};
use feldera_adapterlib::{
    catalog::InputCollectionHandle,
    transport::{OutputBatchType, Step},
};
use feldera_native_dbsp_probe::{
    ProjectedStringRow, StringProjectionSpec, StringRow, build_string_projection_streams,
};
use feldera_sqllib::read_output_spine;
use feldera_sqllib::{SqlString, SqlVariant};
use feldera_types::{
    config::ConnectorConfig, deserialize_table_record, program_schema::Relation,
    serialize_table_record,
};
use static_connectors::{DeZSetHandle, seroutput::SerBatchImpl};
use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::Hasher,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct TextInput {
    s: Option<SqlString>,
}

deserialize_table_record!(TextInput["t", SqlVariant, 1] {
    (s, "s", false, Option<SqlString>, |_| Some(None))
});

impl From<TextInput> for StringRow {
    fn from(value: TextInput) -> Self {
        Self::new(value.s)
    }
}

#[derive(Debug)]
struct TextOutput {
    prefix: Option<SqlString>,
    literal: SqlString,
    padded_length: Option<i32>,
    literal_length: i32,
    trimmed: Option<SqlString>,
}

impl From<ProjectedStringRow> for TextOutput {
    fn from(row: ProjectedStringRow) -> Self {
        Self {
            prefix: row.0,
            literal: row.1,
            padded_length: row.2,
            literal_length: row.3,
            trimmed: row.4,
        }
    }
}

serialize_table_record!(TextOutput[5] {
    prefix["EXPR$0"]: Option<SqlString>,
    literal["EXPR$1"]: SqlString,
    padded_length["EXPR$2"]: Option<i32>,
    literal_length["EXPR$3"]: i32,
    trimmed["EXPR$4"]: Option<SqlString>
});

#[derive(Clone, Default)]
struct CollectedBytes(Arc<Mutex<Vec<(Vec<u8>, usize)>>>);

impl OutputConsumer for CollectedBytes {
    fn max_buffer_size_bytes(&self) -> usize {
        1 << 20
    }

    fn batch_start(&mut self, _: Step, _: OutputBatchType) {}

    fn push_buffer(&mut self, bytes: &[u8], records: usize) {
        self.0.lock().unwrap().push((bytes.to_vec(), records));
    }

    fn push_key(
        &mut self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
        _: &[(&str, Option<&[u8]>)],
        _: usize,
    ) {
        panic!("expected unkeyed JSON")
    }

    fn batch_end(&mut self) {}
}

fn ingest(
    input: &InputCollectionHandle,
    connector_config: &serde_json::Value,
    payload: &[u8],
) -> u64 {
    let mut parser = get_input_format("json")
        .unwrap()
        .new_parser("frozen-json", input, connector_config)
        .unwrap();
    let (mut buffer, errors) = parser.parse(payload, None);
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(buffer.len().records, 1);
    let mut hash = DefaultHasher::new();
    buffer.hash(&mut hash);
    buffer.flush().unwrap();
    hash.finish()
}

fn encode(
    output: OrdZSet<ProjectedStringRow>,
    config: &ConnectorConfig,
    schema: &Relation,
) -> Vec<(Vec<u8>, usize)> {
    let consumer = CollectedBytes::default();
    let mut encoder = get_output_format("json")
        .unwrap()
        .new_encoder(
            "frozen-json-output",
            config,
            &None,
            schema,
            Box::new(consumer.clone()),
            false,
        )
        .unwrap();
    encoder
        .encode(Arc::new(SerBatchImpl::<_, TextOutput, ()>::new(output)))
        .unwrap();
    let result = consumer.0.lock().unwrap().clone();
    result
}

#[test]
fn frozen_json_connector_reaches_both_circuits() {
    let fixture_dir = std::env::var_os("P0_CONNECTOR_FIXTURE").unwrap();
    let fixture_dir = Path::new(&fixture_dir);
    let inventory: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture_dir.join("inventory.json")).unwrap()).unwrap();
    let fixture = &inventory["connector"];
    let schema: Relation = serde_json::from_value(fixture["input_schema"].clone()).unwrap();
    let input_config = &fixture["format"]["config"];
    let output_schema = inventory["programs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|program| program["id"] == fixture["case_id"])
        .unwrap()["schema"]["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|relation| relation["name"] == "v")
        .unwrap();
    let output_schema: Relation = serde_json::from_value(output_schema.clone()).unwrap();
    let output_config: ConnectorConfig = serde_json::from_value(serde_json::json!({
        "transport": {"name":"http_output","config":{}},
        "format": fixture["format"]
    }))
    .unwrap();

    let payload = fs::read(fixture_dir.join("connector-input.jsonl")).unwrap();
    let (mut classic, (classic_input, classic_out, classic_errors)) =
        circuit0(CircuitConfig::with_workers(1)).unwrap();
    let (mut native, (input, output, errors)) = Runtime::init_circuit(
        CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
        |circuit| {
            let streams = build_string_projection_streams(
                circuit,
                &StringProjectionSpec {
                    varchar_literal: "🐍".into(),
                    padded_width: 2,
                    padded_literal: "🐍 ".into(),
                    trim_pattern: "🐍".into(),
                },
            )?;
            Ok((
                streams.input,
                streams.output.accumulate_output(),
                streams.errors.accumulate_output(),
            ))
        },
    )
    .unwrap();
    let classic_input = InputCollectionHandle::new(
        schema.clone(),
        DeZSetHandle::<StringRow, TextInput>::new(classic_input),
        None,
    );
    let native_input = InputCollectionHandle::new(
        schema,
        DeZSetHandle::<StringRow, TextInput>::new(input),
        None,
    );
    let classic_hash = ingest(&classic_input, input_config, &payload);
    let native_hash = ingest(&native_input, input_config, &payload);
    assert_eq!(classic_hash, native_hash);
    classic.transaction().unwrap();
    native.transaction().unwrap();
    let classic_out = read_output_spine(&classic_out);
    let native_out = read_output_spine(&output);
    assert_eq!(classic_out, native_out);
    let empty = OrdZSet::from_keys((), vec![]);
    assert_eq!(read_output_spine(&classic_errors), empty);
    assert_eq!(read_output_spine(&errors), empty);
    let classic = encode(classic_out, &output_config, &output_schema);
    let native = encode(native_out, &output_config, &output_schema);
    assert_eq!(classic, native);
    let [(bytes, count)] = classic.as_slice() else {
        panic!("expected one encoded buffer");
    };
    assert_eq!(*count, 1);
    let record: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(
        record,
        serde_json::json!({
            "insert": {"EXPR$0":"🐍", "EXPR$1":"🐍", "EXPR$2":2, "EXPR$3":2, "EXPR$4":"x"}
        })
    );
    println!("input_records=1 output_records={count} decoded_hash={classic_hash:016x}");
    println!("encoded={}", String::from_utf8_lossy(bytes).trim_end());
    fs::write(fixture_dir.join("classic-output.jsonl"), bytes).unwrap();
    fs::write(fixture_dir.join("native-output.jsonl"), &native[0].0).unwrap();
    fs::write(
        fixture_dir.join("output-schema.json"),
        serde_json::to_vec_pretty(&output_schema).unwrap(),
    )
    .unwrap();
}
