//! Tests for datagen input adapter that generates random data based on a schema and config.
use crate::test::{mock_input_pipeline, MockDeZSet, MockInputConsumer, TestStruct2};
use crate::InputReader;
use anyhow::Result as AnyResult;
use dbsp::algebra::F64;
use feldera_sqllib::binary::ByteArray;
use feldera_sqllib::{Date, SqlDecimal, Time, Timestamp};
use feldera_types::config::{InputEndpointConfig, TransportConfig};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use feldera_types::transport::datagen::GenerationPlan;
use feldera_types::{deserialize_table_record, serialize_table_record};
use serde_json::json;
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
    #[serde(rename = "dec")]
    field_1: SqlDecimal<28, 0>,
    #[serde(rename = "dec_scale")]
    field_2: SqlDecimal<10, 10>,
    #[serde(rename = "dec_w_scale")]
    field_3: SqlDecimal<2, 1>,
}

impl RealStruct {
    pub fn schema() -> Vec<Field> {
        vec![
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("dec".into(), ColumnType::decimal(28, 0, false)),
            Field::new("dec_scale".into(), ColumnType::decimal(10, 10, false)),
            Field::new("dec_w_scale".into(), ColumnType::decimal(2, 1, false)),
        ]
    }
}
serialize_table_record!(RealStruct[4]{
    r#field["double"]: F64,
    r#field_1["dec"]: SqlDecimal<28, 0>,
    r#field_2["dec_scale"]: SqlDecimal<10, 10>,
    r#field_3["dec_w_scale"]: SqlDecimal<2, 1>
});

deserialize_table_record!(RealStruct["RealStruct", 4] {
    (r#field, "double", false, F64, None),
    (r#field_1, "dec", false, SqlDecimal<28, 0>, None),
    (r#field_2, "dec_scale", false, SqlDecimal<10, 10>, None),
    (r#field_3, "dec_w_scale", false, SqlDecimal<2, 1>, None)
});

fn mk_pipeline<T, U>(
    config: InputEndpointConfig,
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
    let (endpoint, consumer, _parser, zset) = mock_input_pipeline::<T, U>(config, relation)?;
    endpoint.extend();
    Ok((endpoint, consumer, zset))
}

fn wait_for_data(endpoint: &dyn InputReader, consumer: &MockInputConsumer) {
    while !consumer.state().eoi {
        thread::sleep(Duration::from_millis(20));
    }
    endpoint.queue(false);
    while consumer.state().n_extended == 0 {
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn test_limit_increment() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 10,
                        "fields": {}
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 10,
                        "fields": {
                            "id": {
                                "strategy": "increment",
                                "range": [
                                    10,
                                    20
                                ],
                                "scale": 3
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 5,
                        "fields": {
                            "double": {
                                "strategy": "increment",
                                "range": [
                                    1.1,
                                    10.1
                                ],
                                "scale": 1
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config, RealStruct::schema()).unwrap();

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
fn test_decimal_increment() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 20,
                        "fields": {
                            "dec": {
                                "strategy": "increment"
                            },
                            "dec_w_scale": {
                                "strategy": "increment"
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config, RealStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(record.field_1 >= SqlDecimal::<28, 0>::new(0, 0).unwrap());
        assert!(record.field_1 < SqlDecimal::<27, 0>::new(20, 0).unwrap());

        assert!(record.field_2 >= SqlDecimal::<10, 10>::new(0, 0).unwrap());
        assert!(record.field_2 < SqlDecimal::<10, 10>::new(9999, 4).unwrap());

        assert!(record.field_3 >= SqlDecimal::<2, 1>::new(0, 1).unwrap());
        assert!(record.field_3 < SqlDecimal::<3, 1>::new(10, 0).unwrap());
    }

    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 20,
                        "fields": {
                            "dec": {
                                "strategy": "increment",
                                "range": [
                                    1,
                                    5
                                ]
                            },
                            "dec_w_scale": {
                                "values": [
                                    0.1,
                                    1,
                                    9.9
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config, RealStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(record.field_1 >= SqlDecimal::<28, 0>::new(1, 0).unwrap());
        assert!(record.field_1 < SqlDecimal::<28, 0>::new(5, 0).unwrap());
        assert!(
            record.field_3 == SqlDecimal::<2, 1>::new(1, 1).unwrap()
                || record.field_3 == SqlDecimal::<2, 1>::new(1, 0).unwrap()
                || record.field_3 == SqlDecimal::<2, 1>::new(99, 1).unwrap()
        );
    }
}

#[test]
fn test_decimal_uniform() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 1000,
                        "fields": {
                            "dec_scale": {
                                "strategy": "uniform",
                                "range": [
                                    0.5,
                                    0.6
                                ]
                            },
                            "dec_w_scale": {
                                "strategy": "uniform"
                            },
                            "dec": {
                                "strategy": "uniform",
                                "values": [
                                    0.1,
                                    1,
                                    2
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config, RealStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(
            record.field_1 == SqlDecimal::<28, 0>::new(1, 1).unwrap()
                || record.field_1 == SqlDecimal::<28, 0>::ONE
                || record.field_1 == SqlDecimal::<28, 0>::new(2, 0).unwrap()
        );

        assert!(record.field_2 >= SqlDecimal::<10, 10>::new(5, 1).unwrap());
        assert!(record.field_2 < SqlDecimal::<10, 10>::new(6, 1).unwrap());

        assert!(record.field_3 >= SqlDecimal::<2, 1>::new(-99, 1).unwrap());
        assert!(record.field_3 < SqlDecimal::<2, 1>::new(99, 1).unwrap());
    }
}

#[test]
fn test_decimal_zipf() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 1000,
                        "fields": {
                            "dec_scale": {
                                "strategy": "zipf",
                                "range": [
                                    0,
                                    1
                                ]
                            },
                            "dec_w_scale": {
                                "strategy": "zipf"
                            },
                            "dec": {
                                "strategy": "zipf",
                                "values": [
                                    0.1,
                                    1,
                                    2
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<RealStruct, RealStruct>(config, RealStruct::schema()).unwrap();

    wait_for_data(endpoint.as_ref(), &consumer);

    let zst = zset.state();
    let iter = zst.flushed.iter();
    for upd in iter {
        let record = upd.unwrap_insert();
        assert!(
            record.field_1 == SqlDecimal::<28, 0>::new(1, 1).unwrap()
                || record.field_1 == SqlDecimal::<28, 0>::ONE
                || record.field_1 == SqlDecimal::<28, 0>::new(2, 0).unwrap()
        );

        assert!(record.field_2 >= SqlDecimal::<10, 10>::new(0, 0).unwrap());
        // Decimal::<10, 10> cannot represent 1
        assert!(record.field_2 < SqlDecimal::<11, 10>::new(1, 0).unwrap());

        assert!(record.field_3 >= SqlDecimal::<2, 1>::new(0, 0).unwrap());
        assert!(record.field_3 < SqlDecimal::<2, 1>::new(99, 1).unwrap());
    }
}

#[test]
fn test_array_increment() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 2,
                        "fields": {
                            "bs": {
                                "range": [
                                    10,
                                    11
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config, ByteStruct::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 1,
                        "fields": {
                            "id": {
                                "strategy": "uniform",
                                "range": [
                                    10,
                                    20
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 4,
                        "fields": {
                            "id": {
                                "values": [
                                    99,
                                    100,
                                    101
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
        let config = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [
                        {
                            "limit": 4,
                            "fields": {
                                "id": {
                                    "values": [
                                        "a",
                                        "b"
                                    ],
                                    "strategy": strategy,
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap();

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_string_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [
                        {
                            "limit": 4,
                            "fields": {
                                "name": {
                                    "values": [
                                        1,
                                        2
                                    ],
                                    "strategy": strategy,
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap();

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_timestamp_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [
                        {
                            "limit": 4,
                            "fields": {
                                "ts": {
                                    "values": [
                                        true
                                    ],
                                    "strategy": strategy,
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap();

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_time_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [
                        {
                            "limit": 4,
                            "fields": {
                                "t": {
                                    "values": [
                                        1
                                    ],
                                    "strategy": strategy,
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap();

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
#[should_panic]
fn test_invalid_date_values() {
    for strategy in &["increment", "uniform", "zipf"] {
        let config = serde_json::from_value(json!({
            "stream": "test_input",
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [
                        {
                            "limit": 4,
                            "fields": {
                                "dt": {
                                    "values": [
                                        1
                                    ],
                                    "strategy": strategy,
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .unwrap();

        let (_endpoint, consumer, _zset) =
            mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

        while !consumer.state().eoi {
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn missing_config_does_something_sane() {
    let config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "workers": 3
            }
        }
    }))
    .unwrap();

    if let TransportConfig::Datagen(dtg) = config.connector_config.transport {
        assert_eq!(dtg.plan.len(), 1);
        assert_eq!(dtg.plan[0], GenerationPlan::default());
    }
}

#[test]
fn test_null() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 10,
                        "fields": {
                            "nAmE": {
                                "null_percentage": 100
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 100,
                        "fields": {
                            "nAmE": {
                                "null_percentage": 50
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 2,
                        "fields": {
                            "nAmE": {
                                "strategy": "word"
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TestStruct2, TestStruct2>(config, TestStruct2::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
                "stream": "test_input",
                "transport": {
                    "name": "datagen",
                    "config": {
                        "plan": [
                            {
                                "limit": 3,
                                "fields": {
                                    "bs": {
                                        "range": [
                                            1,
                                            5
                                        ],
                                        "values": [
    [
                                            1,
                                            2
                                        ],
                                        [
                                            1,
                                            2,
                                            3
                                        ]
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config, ByteStruct::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 5,
                        "fields": {
                            "bs": {
                                "range": [
                                    0,
                                    3
                                ],
                                "value": {
                                    "range": [
                                        0,
                                        2
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config, ByteStruct::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 2,
                        "fields": {
                            "bs": {
                                "range": [
                                    1,
                                    2
                                ],
                                "value": {
                                    "range": [
                                        128,
                                        255
                                    ],
                                    "strategy": "uniform"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<ByteStruct, ByteStruct>(config, ByteStruct::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 2,
                        "fields": {}
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 3,
                        "fields": {
                            "ts": {
                                "range": [
                                    1724803200000u64,
                                    1724803200002u64
                                ]
                            },
                            "dt": {
                                "range": [
                                    19963,
                                    19965
                                ]
                            },
                            "t": {
                                "range": [
                                    5,
                                    7
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 100,
                        "fields": {
                            "ts": {
                                "strategy": "uniform",
                                "range": [
                                    "2024-10-11T11:04:00Z",
                                    "2024-10-11T11:05:02Z"
                                ]
                            },
                            "dt": {
                                "strategy": "uniform",
                                "range": [
                                    19963,
                                    19965
                                ]
                            },
                            "t": {
                                "strategy": "uniform",
                                "range": [
                                    5,
                                    7
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

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
    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "unknown": 1,
                "plan": [
                    {
                        "limit": 3,
                        "fields": {
                            "ts": {
                                "range": [
                                    1724803200000u64,
                                    1724803200002u64
                                ]
                            },
                            "dt": {
                                "range": [
                                    19963,
                                    19965
                                ]
                            },
                            "t": {
                                "range": [
                                    5,
                                    7
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limitx": 1
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "fields": {
                            "ts": {
                                "unknown": "abc",
                                "range": [
                                    1724803200000u64,
                                    1724803200002u64
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());

    let config: Result<InputEndpointConfig, _> = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "fields": {
                            "ts": {
                                "strategy": "unknown"
                            }
                        }
                    }
                ]
            }
        }
    }));
    assert!(config.is_err());
}

#[test]
fn test_time_types_with_string_range() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 3,
                        "fields": {
                            "ts": {
                                "range": [
                                    "2024-08-28T00:00:00Z",
                                    "2024-08-28T00:00:02Z"
                                ],
                                "scale": 1000
                            },
                            "dt": {
                                "range": [
                                    "2024-08-28",
                                    "2024-08-30"
                                ]
                            },
                            "t": {
                                "range": [
                                    "00:00:05",
                                    "00:00:07"
                                ],
                                "scale": 1000
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

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
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 2,
                        "fields": {
                            "T": {}
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (_endpoint, _consumer, _zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();
}

#[test]
fn test_case_insensitivity() {
    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": 1,
                        "fields": {
                            "TS": {
                                "values": [
                                    "1970-01-01T00:00:00Z"
                                ]
                            },
                            "dT": {
                                "values": [
                                    "1970-01-02"
                                ]
                            },
                            "t": {
                                "values": [
                                    "00:00:01"
                                ]
                            }
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

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

    let config = serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "datagen",
            "config": {
                "plan": [
                    {
                        "limit": size,
                        "fields": {
                            "ts": {
                                "range": [
                                    "1970-01-01T00:00:00Z",
                                    "1980-01-01T00:00:00Z"
                                ]
                            }
                        }
                    }
                ],
                "workers": workers
            }
        }
    }))
    .unwrap();
    let (endpoint, consumer, _zset) =
        mk_pipeline::<TimeStuff, TimeStuff>(config, TimeStuff::schema()).unwrap();

    let start = std::time::Instant::now();
    wait_for_data(endpoint.as_ref(), &consumer);
    let elapsed = start.elapsed();
    let tput = size as f64 / elapsed.as_secs_f64();
    println!("Tput({workers}, {size}): {tput:.2} records/sec");
}
