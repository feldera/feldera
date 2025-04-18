//! Tests for datagen input adapter that generates random data based on a schema and config.
use crate::test::{mock_input_pipeline, MockDeZSet, MockInputConsumer, TestStruct2};
use crate::InputReader;
use anyhow::Result as AnyResult;
use dbsp::algebra::F64;
use feldera_sqllib::binary::ByteArray;
use feldera_sqllib::{Date, Time, Timestamp};
use feldera_types::config::{InputEndpointConfig, TransportConfig};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use feldera_types::transport::datagen::GenerationPlan;
use feldera_types::{deserialize_table_record, serialize_table_record};
use size_of::SizeOf;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::time::Duration;
use std::{env, fmt::Debug, thread};

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct ByteStruct {
    #[serde(rename = "bs")]
    field: ByteArray,
}

impl ByteStruct {
    pub fn schema() -> Vec<Field> {
        vec![Field::new("bs".into(), ColumnType::varbinary(false))]
    }
}
serialize_table_record!(ByteStruct[1]{
    r#field["bs"]: ByteArray
});

deserialize_table_record!(ByteStruct["ByteStruct", 1] {
    (r#field, "bs", false, ByteArray, None)
});

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct RealStruct {
    #[serde(rename = "double")]
    field: F64,
}

impl RealStruct {
    pub fn schema() -> Vec<Field> {
        vec![Field::new("double".into(), ColumnType::double(false))]
    }
}
serialize_table_record!(RealStruct[1]{
    r#field["double"]: F64
});

deserialize_table_record!(RealStruct["RealStruct", 1] {
    (r#field, "double", false, F64, None)
});

fn mk_pipeline<T, U>(
    config_str: &str,
    fields: Vec<Field>,
) -> AnyResult<(Box<dyn InputReader>, MockInputConsumer, MockDeZSet<T, U>)>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    let relation = Relation::new("test_input".into(), fields, true, BTreeMap::new());
    let (endpoint, consumer, _parser, zset) =
        mock_input_pipeline::<T, U>(serde_yaml::from_str(config_str)?, relation)?;
    endpoint.extend();
    Ok((endpoint, consumer, zset))
}

fn wait_for_data(endpoint: &dyn InputReader, consumer: &MockInputConsumer) {
    while !consumer.state().eoi {
        thread::sleep(Duration::from_millis(20));
    }
    endpoint.queue();
    while consumer.state().n_extended == 0 {
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn test_limit_increment() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: {} } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    let mut idx = 0;

    for upd in iter {
        let record = upd.unwrap_insert();
        assert_eq!(record.field, idx);
        assert_eq!(record.field_1, idx % 2 == 1);
        assert_eq!(record.field_5.as_ref().unwrap().field, idx % 2 == 1);
        idx += 1;
    }
    assert_eq!(idx, 10);
}

#[test]
fn test_scaled_range_increment() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: { "id": { "strategy": "increment", "range": [10, 20], scale: 3 } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    let mut idx = 10;
    for upd in iter {
        let record = upd.unwrap_insert();
        assert_eq!(record.field, 10 + (idx % 10));
        idx += 3;
    }
    assert!(idx > 10);
}

#[test]
fn test_scaled_range_increment_reals() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 5, fields: { "double": { "strategy": "increment", "range": [1.1, 10.1], scale: 1 } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config_str, RealStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    let mut idx = 0f64;
    let eps = F64::new(1e-5);
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!((record.field - (1.1 + idx)).abs() < eps);
        idx += 1.0;
    }
    assert!(idx > 0.0);
}

#[test]
fn test_array_increment() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: { "bs": { "range": [10, 11] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config_str, ByteStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    let mut idx = 0;

    for upd in iter {
        let record = upd.unwrap_insert();
        assert_eq!(record.field.as_slice(), &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        idx += 1;
    }
    assert_eq!(idx, 2);
}

#[test]
fn test_uniform_range() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 1, fields: { "id": { "strategy": "uniform", "range": [10, 20] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(record.field >= 10 && record.field < 20);
    }
}

#[test]
fn test_values() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 4, fields: { "id": { values: [99, 100, 101] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let mut next = 99;
    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert_eq!(record.field, next);
        next += 1;
        if next == 102 {
            next = 99;
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_integer_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: 4, fields: {{ "id": {{ values: ["a", "b"], "strategy": "{strategy}" }} }} }} ]
"#,
        );

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(&config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_string_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: 4, fields: {{ "name": {{ values: [1, 2], "strategy": "{strategy}" }} }} }} ]
"#,
        );

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(&config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_timestamp_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: 4, fields: {{ "ts": {{ values: [true], "strategy": "{strategy}" }} }} }} ]
"#,
        );

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(&config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_time_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: 4, fields: {{ "t": {{ values: [1], "strategy": "{strategy}" }} }} }} ]
"#,
        );

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(&config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_date_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config_str = format!(
            r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: 4, fields: {{ "dt": {{ values: [1.0], "strategy": "{strategy}" }} }} }} ]
"#,
        );

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(&config_str, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn missing_config_does_something_sane() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
      workers: 3
"#;
    let cfg: InputEndpointConfig = serde_yaml::from_str(config_str).unwrap();

    if let TransportConfig::Datagen(dtg) = cfg.connector_config.transport {
        assert_eq!(dtg.plan.len(), 1);
        assert_eq!(dtg.plan[0], GenerationPlan::default());
    }
}

#[test]
fn test_null() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 10, fields: { "name": { null_percentage: 100 } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(record.field_0.is_none());
    }
}

#[test]
fn test_null_percentage() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 100, fields: { "name": { null_percentage: 50 } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        // This assert is not asserting anything useful, but it's just running this test
        // checks that datagen doesn't panic when null_percentage is set
        // as we always need a proper Value::String type in the &mut Value field and
        // with null percentage it sometimes can get set to Value::Null
        assert!(record.field_0.is_none() || record.field_0.is_some());
    }
}

#[test]
fn test_string_generators() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: { "name": { "strategy": "word" } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config_str, TestStruct2::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(record.field_0.is_some());
    }
}

#[test]
fn test_byte_array_with_values() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 3, fields: { "bs": { "range": [ 1, 5 ], values: [[1,2], [1,2,3]] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config_str, ByteStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert_eq!(record.field.as_slice(), &[1, 2]);

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field.as_slice(), &[1, 2, 3]);

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field.as_slice(), &[1, 2]);
}

#[test]
fn test_byte_array_with_increment() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 5, fields: { "bs": { "range": [0, 3], "value": { "range": [ 0, 2 ] } } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config_str, ByteStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();

    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert!(record.field.as_slice().is_empty());

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field.as_slice(), &[0]);

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field.as_slice(), &[0, 1]);

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert!(record.field.as_slice().is_empty());
}

#[test]
fn test_byte_array_with_value() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: { "bs": { "range": [ 1, 2 ], value: { "range": [128, 255], "strategy": "uniform" } } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config_str, ByteStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert!(*record.field.as_slice().first().unwrap() >= 128u8);

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert!(*record.field.as_slice().first().unwrap() >= 128u8);
}

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TimeStuff {
    #[serde(rename = "ts")]
    pub field: Timestamp,
    #[serde(rename = "dt")]
    pub field_1: Date,
    #[serde(rename = "t")]
    pub field_2: Time,
}

impl TimeStuff {
    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("ts".into(), ColumnType::timestamp(false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new("\"t\"".into(), ColumnType::time(false)),
        ]
    }
}
serialize_table_record!(TimeStuff[3]{
    r#field["ts"]: Timestamp,
    r#field_1["dt"]: Date,
    r#field_2["t"]: Time
});

deserialize_table_record!(TimeStuff["TimeStuff", 3] {
    (r#field, "ts", false, Timestamp, None),
    (r#field_1, "dt", false, Date, None),
    (r#field_2, "t", false, Time, None)
});

#[test]
fn test_time_types() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: {} } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(0));
    assert_eq!(record.field_1, Date::new(0));
    assert_eq!(record.field_2, Time::new(0));

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1));
    assert_eq!(record.field_1, Date::new(1));
    assert_eq!(record.field_2, Time::new(1000000));
}

#[test]
fn test_time_types_with_integer_range() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 3, fields: { "ts": { "range": [1724803200000, 1724803200002] }, "dt": { "range": [19963, 19965] }, "t": { "range": [5, 7] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000));
    assert_eq!(record.field_1, Date::new(19963));
    assert_eq!(record.field_2, Time::new(5000000));

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000 + 1));
    assert_eq!(record.field_1, Date::new(19963 + 1));
    assert_eq!(record.field_2, Time::new(5000000 + 1000000));

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000));
    assert_eq!(record.field_1, Date::new(19963));
    assert_eq!(record.field_2, Time::new(5000000));
}

#[test]
fn test_uniform_dates_times_timestamps() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 100, fields: { "ts": { "strategy": "uniform", "range": ["2024-10-11T11:04:00Z", "2024-10-11T11:05:02Z"] }, "dt": { "strategy": "uniform", "range": [19963, 19965] }, "t": { "strategy": "uniform", "range": [5, 7] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    for record in zst.flushed.iter() {
        let record = record.unwrap_insert();
        assert!(record.field >= Timestamp::from_dateTime("2024-10-11T11:04:00Z".parse().unwrap()));
        assert!(record.field < Timestamp::from_dateTime("2024-10-11T11:05:02Z".parse().unwrap()));
        assert!(record.field_1 >= Date::new(19963));
        assert!(record.field_1 < Date::new(19965));
        assert!(record.field_2 >= Time::new(5000000));
        assert!(record.field_2 < Time::new(7000000));
    }
}

#[test]
fn test_invalid_configs() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        unknown: 1
        plan: [ { limit: 3, fields: { "ts": { "range": [1724803200000, 1724803200002] }, "dt": { "range": [19963, 19965] }, "t": { "range": [5, 7] } } } ]
"#;
    let r = mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema());
    assert!(r.is_err());

    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limitx: 1 } ]
"#;
    let r = mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema());
    assert!(r.is_err());

    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { fields: { "ts": { "unknown": "abc", "range": [1724803200000, 1724803200002] } } } ]
"#;
    let r = mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema());
    assert!(r.is_err());

    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { fields: { "ts": { "strategy": "unknown" } } } ]
"#;
    let r = mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema());
    assert!(r.is_err());
}

#[test]
fn test_time_types_with_string_range() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 3, fields: {  "ts": { "range": ["2024-08-28T00:00:00Z", "2024-08-28T00:00:02Z"], "scale": 1000 }, "dt": { "range": ["2024-08-28", "2024-08-30"] }, "t": { "range": ["00:00:05", "00:00:07"], "scale": 1000 } } } ]

"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000));
    assert_eq!(record.field_1, Date::new(19963));
    assert_eq!(record.field_2, Time::new(5000000000));

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000 + 1000));
    assert_eq!(record.field_1, Date::new(19963 + 1));
    assert_eq!(record.field_2, Time::new(6000000000));

    let second = iter.next().unwrap();
    let record = second.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(1724803200000));
    assert_eq!(record.field_1, Date::new(19963));
    assert_eq!(record.field_2, Time::new(5000000000));
}

/// Field T not found, "t" is case sensitive.
#[test]
#[should_panic]
fn test_case_sensitivity() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 2, fields: { "T": {} } } ]
"#;
    let (_endpoint, _consumer, _zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();
}

#[test]
fn test_case_insensitivity() {
    let config_str = r#"
stream: test_input
transport:
    name: datagen
    config:
        plan: [ { limit: 1, fields: { "TS": { "values": ["1970-01-01T00:00:00Z"] }, "dT": { "values": ["1970-01-02"] }, "t": { "values": ["00:00:01"] } } } ]
"#;
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str, TimeStuff::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let mut iter = zst.flushed.iter();
    let first = iter.next().unwrap();
    let record = first.unwrap_insert();
    assert_eq!(record.field, Timestamp::new(0));
    assert_eq!(record.field_1, Date::new(1));
    assert_eq!(record.field_2, Time::new(1_000_000_000));
}

#[test]
#[ignore]
fn test_tput() {
    static DEFAULT_SIZE: usize = 5_000_000;
    let size = env::var("RECORDS")
        .map(|r| r.parse().unwrap_or(DEFAULT_SIZE))
        .unwrap_or(DEFAULT_SIZE);

    static DEFAULT_WORKERS: usize = 1;
    let workers = env::var("WORKERS")
        .map(|r| r.parse().unwrap_or(DEFAULT_WORKERS))
        .unwrap_or(DEFAULT_WORKERS);

    let config_str = format!(
            "
stream: test_input
transport:
    name: datagen
    config:
        plan: [ {{ limit: {size}, fields: {{ ts: {{ range: ['1970-01-01T00:00:00Z', '1980-01-01T00:00:00Z'] }} }} }} ]
        workers: {workers}
"
        );
    let (endpoint, consumer, _zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config_str.as_str(), TimeStuff::schema()).unwrap();

    let start = std::time::Instant::now();
    wait_for_data(endpoint.as_ref(), &consumer);
    let elapsed = start.elapsed();
    let tput = size as f64 / elapsed.as_secs_f64();
    println!("Tput({workers}, {size}): {tput:.2} records/sec");
}
