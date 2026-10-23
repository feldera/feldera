//! Random rows of a relation, for the property tests of the suites.
//!
//! The strategies draw each value as arrow-json reads it into the published form of its
//! column, and build the rows with [`Updates::from_json`]. The values stay in ranges that
//! every format and connector of the suites can hold: floats with a short decimal form,
//! dates and timestamps from 1970 to 2100 with microsecond precision, and strings of up to 32
//! characters without control characters. A nullable column is NULL in one row of two.

use std::ops::RangeInclusive;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeDelta};
use feldera_adapterlib::test::Updates;
use feldera_types::program_schema::{ColumnType, Relation, SqlType};
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;
use serde_json::{Map, Value};

use crate::test::published_schema;

/// Days from 1970-01-01 to 2100-01-01.
const MAX_DAYS: i64 = 47_482;

/// Microseconds in a day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Returns a strategy for `batches` batches of up to `max_rows` rows of `relation` each, every
/// row inserted once.
///
/// The integer column `numbered` counts the rows up from 0 across the batches, so each row is
/// distinct and the rows are in the order of that column.
pub fn batches(
    relation: &Relation,
    numbered: &str,
    batches: RangeInclusive<usize>,
    max_rows: usize,
) -> BoxedStrategy<Vec<Updates>> {
    weighted_batches(relation, numbered, batches, max_rows, 1..=1)
}

/// Like [`batches`], but each row has a random weight in `weights`.
pub fn weighted_batches(
    relation: &Relation,
    numbered: &str,
    batches: RangeInclusive<usize>,
    max_rows: usize,
    weights: RangeInclusive<i64>,
) -> BoxedStrategy<Vec<Updates>> {
    assert!(
        relation
            .fields
            .iter()
            .any(|field| field.name.name() == numbered),
        "relation '{}' has no column '{numbered}'",
        relation.name
    );
    let relation = relation.clone();
    let numbered = numbered.to_string();
    vec(vec((row(&relation), weights), 0..=max_rows), batches)
        .prop_map(move |batches| {
            let mut next = 0;
            batches
                .into_iter()
                .map(|batch| {
                    let rows: Vec<(i64, String)> = batch
                        .into_iter()
                        .map(|(mut row, weight)| {
                            row.insert(numbered.clone(), Value::from(next));
                            next += 1;
                            (weight, Value::Object(row).to_string())
                        })
                        .collect();
                    let rows: Vec<(i64, &str)> = rows
                        .iter()
                        .map(|(weight, row)| (*weight, row.as_str()))
                        .collect();
                    Updates::from_json(published_schema(&relation), &rows).unwrap()
                })
                .collect()
        })
        .boxed()
}

/// Returns a strategy for one row of `relation`, as a JSON object that arrow-json reads into
/// its published form.
pub fn row(relation: &Relation) -> BoxedStrategy<Map<String, Value>> {
    let columns: Vec<(String, BoxedStrategy<Value>)> = relation
        .fields
        .iter()
        .map(|field| (field.name.name(), value(&field.columntype)))
        .collect();
    let (names, values): (Vec<String>, Vec<BoxedStrategy<Value>>) = columns.into_iter().unzip();
    values
        .prop_map(move |values| names.iter().cloned().zip(values).collect())
        .boxed()
}

/// Returns a strategy for one value of `column`, as arrow-json reads it into the published
/// form of the column.
///
/// Panics for a type that the strategies do not cover: INTERVAL and NULL.
pub fn value(column: &ColumnType) -> BoxedStrategy<Value> {
    let value = non_null_value(column);
    if column.nullable {
        prop_oneof![Just(Value::Null), value].boxed()
    } else {
        value
    }
}

fn non_null_value(column: &ColumnType) -> BoxedStrategy<Value> {
    match column.typ {
        SqlType::Boolean => any::<bool>().prop_map(Value::from).boxed(),
        SqlType::TinyInt => any::<i8>().prop_map(Value::from).boxed(),
        SqlType::SmallInt => any::<i16>().prop_map(Value::from).boxed(),
        SqlType::Int => any::<i32>().prop_map(Value::from).boxed(),
        SqlType::BigInt => any::<i64>().prop_map(Value::from).boxed(),
        SqlType::UTinyInt => any::<u8>().prop_map(Value::from).boxed(),
        SqlType::USmallInt => any::<u16>().prop_map(Value::from).boxed(),
        SqlType::UInt => any::<u32>().prop_map(Value::from).boxed(),
        SqlType::UBigInt => any::<u64>().prop_map(Value::from).boxed(),
        // Quarters and thousandths: a float that a short decimal names exactly, which every
        // JSON reader rounds the same way.
        SqlType::Real => any::<i16>()
            .prop_map(|quarters| Value::from(f64::from(f32::from(quarters) / 4.0)))
            .boxed(),
        SqlType::Double => any::<i32>()
            .prop_map(|thousandths| Value::from(f64::from(thousandths) / 1000.0))
            .boxed(),
        SqlType::Decimal => {
            let precision = column.precision.unwrap_or(0) as u32;
            let scale = column.scale.unwrap_or(0) as u32;
            let max = 10i128.pow(precision) - 1;
            (-max..=max)
                .prop_map(move |mantissa| Value::from(decimal(mantissa, scale)))
                .boxed()
        }
        SqlType::Char | SqlType::Varchar => "\\PC{0,32}".prop_map(Value::from).boxed(),
        SqlType::Date => (0..MAX_DAYS)
            .prop_map(|days| Value::from(date(days).to_string()))
            .boxed(),
        SqlType::Time => (0..MICROS_PER_DAY)
            .prop_map(|micros| {
                let time = NaiveTime::MIN + TimeDelta::microseconds(micros);
                Value::from(time.format("%H:%M:%S%.6f").to_string())
            })
            .boxed(),
        SqlType::Timestamp => (0..MAX_DAYS * MICROS_PER_DAY)
            .prop_map(|micros| {
                Value::from(
                    timestamp(micros)
                        .format("%Y-%m-%dT%H:%M:%S%.6f")
                        .to_string(),
                )
            })
            .boxed(),
        SqlType::TimestampTz => (0..MAX_DAYS * MICROS_PER_DAY)
            .prop_map(|micros| {
                Value::from(
                    timestamp(micros)
                        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                        .to_string(),
                )
            })
            .boxed(),
        SqlType::Uuid => any::<u128>()
            .prop_map(|bits| Value::from(uuid::Uuid::from_u128(bits).to_string()))
            .boxed(),
        SqlType::Binary | SqlType::Varbinary => vec(any::<u8>(), 0..8)
            .prop_map(|bytes| Value::from(hex(&bytes)))
            .boxed(),
        SqlType::Variant => variant()
            .prop_map(|variant| Value::from(variant.to_string()))
            .boxed(),
        SqlType::Array => {
            let component = column
                .component
                .as_ref()
                .expect("an ARRAY has a component type");
            vec(value(component), 0..4).prop_map(Value::Array).boxed()
        }
        SqlType::Map => {
            let key = column.key.as_ref().expect("a MAP has a key type");
            let val = column.value.as_ref().expect("a MAP has a value type");
            assert!(
                matches!(key.typ, SqlType::Char | SqlType::Varchar),
                "random maps have string keys"
            );
            btree_map("\\PC{0,6}", value(val), 0..4)
                .prop_map(|map| Value::Object(map.into_iter().collect()))
                .boxed()
        }
        SqlType::Struct => {
            let fields = column.fields.as_ref().expect("a STRUCT has fields");
            let columns: Vec<(String, BoxedStrategy<Value>)> = fields
                .iter()
                .map(|field| (field.name.name(), value(&field.columntype)))
                .collect();
            let (names, values): (Vec<String>, Vec<BoxedStrategy<Value>>) =
                columns.into_iter().unzip();
            values
                .prop_map(move |values| Value::Object(names.iter().cloned().zip(values).collect()))
                .boxed()
        }
        _ => panic!("no random values of type {:?}", column.typ),
    }
}

/// Returns a strategy for a VARIANT value: a scalar, or a short array or object of scalars,
/// with no floats, so that its JSON text has one form.
fn variant() -> impl Strategy<Value = Value> {
    let scalar = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i32>().prop_map(Value::from),
        "[a-z]{0,6}".prop_map(Value::from),
    ];
    prop_oneof![
        scalar.clone(),
        vec(scalar.clone(), 0..3).prop_map(Value::Array),
        btree_map("[a-z]{1,4}", scalar, 0..3)
            .prop_map(|map| Value::Object(map.into_iter().collect())),
    ]
}

/// Returns `mantissa` scaled down by `scale` decimal digits, as decimal text.
fn decimal(mantissa: i128, scale: u32) -> String {
    let sign = if mantissa < 0 { "-" } else { "" };
    let digits = mantissa.unsigned_abs().to_string();
    let scale = scale as usize;
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = digits.split_at(digits.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

/// Returns the date `days` days after 1970-01-01.
fn date(days: i64) -> NaiveDate {
    DateTime::UNIX_EPOCH.date_naive() + TimeDelta::days(days)
}

/// Returns the time `micros` microseconds after 1970-01-01 00:00:00.
fn timestamp(micros: i64) -> chrono::NaiveDateTime {
    DateTime::UNIX_EPOCH.naive_utc() + TimeDelta::microseconds(micros)
}

/// Returns `bytes` as lowercase hex digits.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use feldera_types::program_schema::{ColumnType, Field, Relation};
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    use super::{batches, decimal};

    /// A relation with a column of each type that the strategies cover.
    fn every_type() -> Relation {
        let scalar = |name: &str, column: ColumnType| Field::new(name.into(), column);
        Relation::new(
            "every_type".into(),
            vec![
                scalar("id", ColumnType::bigint(false)),
                scalar("b", ColumnType::boolean(true)),
                scalar("ti", ColumnType::tinyint(false)),
                scalar("si", ColumnType::smallint(false)),
                scalar("i", ColumnType::int(false)),
                scalar("r", ColumnType::real(false)),
                scalar("d", ColumnType::double(false)),
                scalar("dec", ColumnType::decimal(10, 3, false)),
                scalar("s", ColumnType::varchar(true)),
                scalar("dt", ColumnType::date(false)),
                scalar("t", ColumnType::time(false)),
                scalar("ts", ColumnType::timestamp(false)),
                scalar("tz", ColumnType::timestamp_tz(false)),
                scalar("u", ColumnType::uuid(false)),
                scalar("bin", ColumnType::varbinary(false)),
                scalar("v", ColumnType::variant(false)),
                scalar("a", ColumnType::array(false, ColumnType::varchar(false))),
                scalar(
                    "m",
                    ColumnType::map(true, ColumnType::varchar(false), ColumnType::bigint(true)),
                ),
                scalar(
                    "st",
                    ColumnType::structure(false, &[Field::new("x".into(), ColumnType::int(true))]),
                ),
            ],
            false,
            BTreeMap::new(),
        )
    }

    /// The strategies build rows of every covered type, numbered in order across batches.
    #[test]
    fn batches_of_every_type_are_numbered_in_order() {
        let mut runner = TestRunner::default();
        for _ in 0..32 {
            let batches = batches(&every_type(), "id", 1..=3, 5)
                .new_tree(&mut runner)
                .unwrap()
                .current();
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|batch| {
                    let column = batch.rows().column_by_name("id").unwrap().clone();
                    let column = column
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .clone();
                    column.values().to_vec()
                })
                .collect();
            let expected: Vec<i64> = (0..ids.len() as i64).collect();
            assert_eq!(ids, expected);
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.weights().iter().all(|w| *w == 1))
            );
        }
    }

    #[test]
    fn decimals_keep_their_scale() {
        assert_eq!(decimal(0, 3), "0.000");
        assert_eq!(decimal(-5, 3), "-0.005");
        assert_eq!(decimal(123_456, 3), "123.456");
        assert_eq!(decimal(42, 0), "42");
    }
}
