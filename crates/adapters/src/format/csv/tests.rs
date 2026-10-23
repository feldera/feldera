//! The CSV parser's options: quoting, escaping, trimming, comments, field counts, and a
//! header row.

use std::borrow::Cow;
use std::collections::BTreeMap;

use feldera_adapterlib::format::{InputBuffer, ParseError, Parser};
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates};
use feldera_types::config::FormatConfig;
use feldera_types::format::csv::{CsvFormatConfig, CsvTrim};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use proptest::prelude::*;

use crate::test::{parser, relations, render, rows};

/// The table that the cases parse into.
pub fn relation() -> Relation {
    Relation::new(
        "csv_input".into(),
        vec![
            Field::new("first".into(), ColumnType::varchar(false)),
            Field::new("second".into(), ColumnType::varchar(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Parses `data` with `config` into a fresh table of `relation`, and returns the errors that the parser
/// reports and the updates that the table receives.
fn parse(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    config: CsvFormatConfig,
    data: &[u8],
) -> (Vec<ParseError>, Updates) {
    let collection = engine
        .input_collection(relation, &InputOptions::default())
        .unwrap();
    let format = FormatConfig {
        name: Cow::from("csv"),
        config: serde_json::to_value(config).unwrap(),
    };
    let mut parser = parser(collection.as_ref(), &format);
    parser.on_error(Some(Box::new(|_, _| {})));
    let (mut buffer, errors) = parser.parse(data, None);
    buffer.flush().unwrap();
    (errors, collection.updates().unwrap())
}

/// Checks that parsing `data` with `config` reports no error and inserts `rows`, in order.
#[track_caller]
fn check(engine: &dyn ConnectorTestEngine, config: CsvFormatConfig, data: &[u8], rows: &[&str]) {
    let (errors, updates) = parse(engine, &relation(), config, data);
    assert_eq!(errors, []);
    assert_eq!(updates, inserts(&updates, rows));
}

/// Returns `rows`, each inserted once, in the schema of `like`.
fn inserts(like: &Updates, rows: &[&str]) -> Updates {
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, *row)).collect();
    Updates::from_json(like.rows().schema(), &rows).unwrap()
}

/// A quote character other than `"` wraps a field that holds the delimiter.
pub fn quote(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            quote: '\'',
            ..Default::default()
        },
        b"foo,'bar,baz'\nqux,'quux,corge'\n",
        &[
            r#"{"first": "foo", "second": "bar,baz"}"#,
            r#"{"first": "qux", "second": "quux,corge"}"#,
        ],
    );
}

/// With double quoting off, the escape character escapes a quote.
pub fn escape(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            escape: Some('\\'),
            double_quote: false,
            ..Default::default()
        },
        b"\"foo\\\"bar\",baz\n",
        &[r#"{"first": "foo\"bar", "second": "baz"}"#],
    );
}

/// By default, `""` inside a quoted field is one quote.
pub fn double_quote(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig::default(),
        b"\"foo\"\"bar\",baz\n",
        &[r#"{"first": "foo\"bar", "second": "baz"}"#],
    );
}

/// With double quoting off, the first quote of `""` ends the quoted field, and the rest of
/// the field is kept as it is.
pub fn double_quote_disabled(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            double_quote: false,
            ..Default::default()
        },
        b"\"foo\"\"bar\",baz\n",
        &[r#"{"first": "foo\"bar\"", "second": "baz"}"#],
    );
}

/// By default, whitespace around a field is part of it.
pub fn trim_none(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig::default(),
        b"  foo  ,  bar  \n",
        &[r#"{"first": "  foo  ", "second": "  bar  "}"#],
    );
}

/// With `trim = fields`, whitespace around a field is dropped.
pub fn trim_fields(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            trim: CsvTrim::Fields,
            ..Default::default()
        },
        b"  foo  ,  bar  \n",
        &[r#"{"first": "foo", "second": "bar"}"#],
    );
}

/// With quoting off, a quote is an ordinary character.
pub fn quoting_disabled(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            quoting: false,
            ..Default::default()
        },
        b"\"hello\",world\n",
        &[r#"{"first": "\"hello\"", "second": "world"}"#],
    );
}

/// A line that starts with the comment character holds no record.
pub fn comment(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            comment: Some('#'),
            ..Default::default()
        },
        b"# opening comment\nfoo,bar\n# mid comment\nbaz,qux\n",
        &[
            r#"{"first": "foo", "second": "bar"}"#,
            r#"{"first": "baz", "second": "qux"}"#,
        ],
    );
}

/// By default, a record may have more fields than the table; the parser ignores the rest.
pub fn flexible(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig::default(),
        b"foo,bar\nqux,quux,extra\n",
        &[
            r#"{"first": "foo", "second": "bar"}"#,
            r#"{"first": "qux", "second": "quux"}"#,
        ],
    );
}

/// When the field count must not change, a record with another count is an error.
pub fn inflexible(engine: &dyn ConnectorTestEngine) {
    let (errors, updates) = parse(
        engine,
        &relation(),
        CsvFormatConfig {
            flexible: false,
            ..Default::default()
        },
        b"foo,bar\nqux,quux,extra\n",
    );
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        updates,
        inserts(&updates, &[r#"{"first": "foo", "second": "bar"}"#])
    );
}

/// A header row names the columns and holds no record.
pub fn headers(engine: &dyn ConnectorTestEngine) {
    check(
        engine,
        CsvFormatConfig {
            headers: true,
            ..Default::default()
        },
        b"first,second\nfoo,bar\n",
        &[r#"{"first": "foo", "second": "bar"}"#],
    );
}

/// The table of [`random_rows`].
pub fn basic_relation() -> Relation {
    relations::basic("csv_basic")
}

/// Random rows, written as CSV records, read back unchanged and in order.
pub fn random_rows(engine: &dyn ConnectorTestEngine) {
    let relation = basic_relation();
    proptest!(ProptestConfig::with_cases(32), |(batches in rows::batches(&relation, "id", 1..=1, 20))| {
        let rows = &batches[0];
        let (errors, updates) = parse(
            engine,
            &relation,
            CsvFormatConfig::default(),
            &render::csv(&relation, rows.rows()),
        );
        prop_assert_eq!(errors, vec![]);
        prop_assert_eq!(&updates, rows);
    });
}
