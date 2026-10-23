//! JSON update records on the static engine's record layer: an update in the insert/delete
//! format may skip non-key columns, and `null` sets a column to NULL. The shared suites cover
//! the other records; updates need a keyed table, which the suites' fixture does not have yet.

use std::{borrow::Cow, fmt::Debug, hash::Hash, panic::Location};

use dbsp_adapters::{
    FormatConfig, ParseError,
    format::{InputBuffer, Parser},
};
use feldera_sqllib::SqlVariant;
use feldera_types::{
    deserialize_table_record,
    format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat},
    program_schema::Relation,
    serde_with_context::{DeserializeWithContext, SerializeWithContext, SqlSerdeConfig},
    serialize_table_record,
};
use tracing::trace;

use crate::test::{MockDeZSet, MockUpdate, TypedCollection, init_test_logger, parser};

#[derive(PartialEq, Debug, Eq, Hash, Clone)]
struct TestStruct {
    b: bool,
    i: i32,
    s: Option<String>,
}

deserialize_table_record!(TestStruct["TestStruct", SqlVariant, 3] {
    (b, "b", false, bool, |_| None),
    (i, "i", false, i32, |_| None),
    (s, "s", false, Option<String>, |_| Some(None))
});

serialize_table_record!(TestStruct[3]{
    b["b"]: bool,
    i["i"]: i32,
    s["s"]: Option<String>
});

// TODO: tests for RecordFormat::Raw.

// Used to test RecordFormat::Raw.
/*#[derive(Deserialize, PartialEq, Debug, Eq)]
struct OneColumn {
    json: JsonValue,
}*/

#[derive(PartialEq, Debug, Eq, Hash, Clone)]
struct TestStructUpd {
    b: Option<bool>,
    i: i32,
    s: Option<Option<String>>,
}

deserialize_table_record!(TestStructUpd["TestStructUpd", SqlVariant, 3] {
    (b, "b", false, Option<bool>, |_| Some(None)),
    (i, "i", false, i32, |_| None),
    (s, "s", false, Option<Option<String>>, |_| Some(None), |x| if x.is_none() { Some(None) } else {x})
});

impl TestStructUpd {
    fn new(b: Option<bool>, i: i32, s: Option<Option<&str>>) -> Self {
        Self {
            b,
            i,
            s: s.map(|s| s.map(str::to_string)),
        }
    }
}

#[derive(Debug)]
struct TestCase<T, U> {
    /// Putting the location in here makes it easier to find failing test
    /// cases because they get logged.
    #[allow(dead_code)]
    location: &'static Location<'static>,

    config: JsonParserConfig,
    /// Input data, expected result.
    input_batches: Vec<(String, Vec<ParseError>)>,
    /// Expected contents at the end of the test.
    expected_output: Vec<MockUpdate<T, U>>,
}

fn run_test_cases<T, U>(test_cases: Vec<TestCase<T, U>>)
where
    T: Debug
        + Eq
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + SerializeWithContext<SqlSerdeConfig>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: Debug
        + Eq
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    for test in test_cases {
        trace!("test: {test:?}");
        let format_config = FormatConfig {
            name: Cow::from("json"),
            config: serde_json::to_value(test.config).unwrap(),
        };

        let outputs = MockDeZSet::<T, U>::new();
        let collection = TypedCollection::new(Relation::empty(), outputs.clone());
        let mut parser = parser(&collection, &format_config);
        parser.on_error(Some(Box::new(|_, _| {})));
        for (json, expected_errors) in test.input_batches {
            let (mut buffer, errors) = parser.parse(json.as_bytes(), None);
            assert_eq!(&errors, &expected_errors);
            buffer.flush().unwrap();
        }
        assert_eq!(&test.expected_output, &outputs.state().flushed);
    }
}

impl<T, U> TestCase<T, U> {
    #[track_caller]
    fn new(
        config: JsonParserConfig,
        input_batches: Vec<(String, Vec<ParseError>)>,
        expected_output: Vec<MockUpdate<T, U>>,
    ) -> Self {
        Self {
            location: Location::caller(),
            config,
            input_batches,
            expected_output,
        }
    }
}

/// An update record in the insert/delete format may skip non-key columns, and `null`
/// sets a column to NULL. The connector suites cover the other records; updates need a
/// keyed table, which the suites' fixture does not have yet.
#[test]
fn test_json_update_records() {
    init_test_logger();
    let test_cases: Vec<TestCase<TestStruct, TestStructUpd>> = vec![TestCase::new(
        JsonParserConfig {
            update_format: JsonUpdateFormat::InsertDelete,
            json_flavor: JsonFlavor::Default,
            array: false,
            lines: JsonLines::Single,
        },
        vec![
            (r#"{"update": {"b": true, "i": 0}}"#.to_string(), Vec::new()),
            (
                r#"{"update": {"b": true, "i": 0, "s": "foo"}}"#.to_string(),
                Vec::new(),
            ),
            (r#"{"update": {"i": 0, "s": null}}"#.to_string(), Vec::new()),
        ],
        vec![
            MockUpdate::Update(TestStructUpd::new(Some(true), 0, None)),
            MockUpdate::Update(TestStructUpd::new(Some(true), 0, Some(Some("foo")))),
            MockUpdate::Update(TestStructUpd::new(None, 0, Some(None))),
        ],
    )];

    run_test_cases(test_cases);
}
