//! Arrow to row deserialization as the Delta Lake input connector does it, on
//! a wide CDC record: two nested structs of 41 columns each.
//!
//! With records this wide, the parser tasks can limit the rate of a backfill.
//! Strings are views, as the connector reads them.

// The row fields are only written, as the connector writes them, and keep
// their SQL column names.
#![allow(dead_code, non_snake_case)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, ListBuilder,
    RecordBatch, StringViewBuilder, StructArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dbsp_connectors::integrated::delta_table::delta_input_serde_config;
use feldera_adapterlib::utils::arrow_layout::order_columns;
use feldera_adapterlib::utils::arrow_timestamps::timestamps_to_micros;
use feldera_sqllib::{Array, F64, SqlDecimal, SqlString, SqlVariant, Timestamp};
use feldera_types::deserialize_table_record;
use feldera_types::program_schema::{ColumnType, Field as SqlField, SqlIdentifier};
use feldera_types::serde_with_context::DeserializeWithContext;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use static_connectors::deinput::{ArrowSerdeConfig, arrow_deserializer};

// Pipelines allocate through jemalloc.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BATCH_ROWS: usize = 8192;

#[derive(Debug, Clone)]
pub struct Meta {
    time_01: Option<Timestamp>,
    time_02: Option<Timestamp>,
}

deserialize_table_record!(Meta["Meta", SqlVariant, 2] {
    (time_01, "time_01", false, Option<Timestamp>, |_| Some(None)),
    (time_02, "time_02", false, Option<Timestamp>, |_| Some(None))
});

#[derive(Debug, Clone)]
pub struct Source {
    count_01: Option<i64>,
    tags_01: Option<Array<Option<SqlString>>>,
    text_01: Option<SqlString>,
    text_02: Option<SqlString>,
    count_02: Option<i64>,
    time_03: Option<Timestamp>,
    text_03: Option<SqlString>,
    text_04: Option<SqlString>,
    text_05: Option<SqlString>,
    text_06: Option<SqlString>,
    text_07: Option<SqlString>,
    count_03: Option<i64>,
}

deserialize_table_record!(Source["Source", SqlVariant, 12] {
    (count_01, "count_01", false, Option<i64>, |_| Some(None)),
    (tags_01, "tags_01", false, Option<Array<Option<SqlString>>>, |_| Some(None)),
    (text_01, "text_01", false, Option<SqlString>, |_| Some(None)),
    (text_02, "text_02", false, Option<SqlString>, |_| Some(None)),
    (count_02, "count_02", false, Option<i64>, |_| Some(None)),
    (time_03, "time_03", false, Option<Timestamp>, |_| Some(None)),
    (text_03, "text_03", false, Option<SqlString>, |_| Some(None)),
    (text_04, "text_04", false, Option<SqlString>, |_| Some(None)),
    (text_05, "text_05", false, Option<SqlString>, |_| Some(None)),
    (text_06, "text_06", false, Option<SqlString>, |_| Some(None)),
    (text_07, "text_07", false, Option<SqlString>, |_| Some(None)),
    (count_03, "count_03", false, Option<i64>, |_| Some(None))
});

/// Declares the 41 column change struct, its deserializer and its Arrow
/// fields from one list.
macro_rules! change_struct {
    ($(($name:ident, $col:literal, $ty:ty, $arrow:expr)),* $(,)?) => {
        #[derive(Debug, Clone)]
        pub struct Change {
            $($name: $ty,)*
        }

        deserialize_table_record!(Change["Change", SqlVariant, 41] {
            $(($name, $col, false, $ty, |_| Some(None))),*
        });

        fn change_fields() -> Fields {
            Fields::from(vec![$(Field::new($col, $arrow, true)),*])
        }
    };
}

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

fn dec() -> DataType {
    DataType::Decimal128(38, 18)
}

fn str_list() -> DataType {
    DataType::List(Arc::new(Field::new("element", DataType::Utf8View, true)))
}

change_struct!(
    (time_04, "time_04", Option<Timestamp>, ts()),
    (text_08, "text_08", Option<SqlString>, DataType::Utf8View),
    (amount_01, "amount_01", Option<SqlDecimal<38, 18>>, dec()),
    (text_09, "text_09", Option<SqlString>, DataType::Utf8View),
    (text_10, "text_10", Option<SqlString>, DataType::Utf8View),
    (time_05, "time_05", Option<Timestamp>, ts()),
    (flag_01, "flag_01", Option<bool>, DataType::Boolean),
    (text_11, "text_11", Option<SqlString>, DataType::Utf8View),
    (flag_02, "flag_02", Option<bool>, DataType::Boolean),
    (text_12, "text_12", Option<SqlString>, DataType::Utf8View),
    (text_13, "text_13", Option<SqlString>, DataType::Utf8View),
    (text_14, "text_14", Option<SqlString>, DataType::Utf8View),
    (tags_02, "tags_02", Option<Array<Option<SqlString>>>, str_list()),
    (text_15, "text_15", Option<SqlString>, DataType::Utf8View),
    (flag_03, "flag_03", Option<bool>, DataType::Boolean),
    (text_16, "text_16", Option<SqlString>, DataType::Utf8View),
    (amount_02, "amount_02", Option<SqlDecimal<38, 18>>, dec()),
    (tags_03, "tags_03", Option<Array<Option<SqlString>>>, str_list()),
    (count_04, "count_04", Option<i64>, DataType::Int64),
    (text_17, "text_17", Option<SqlString>, DataType::Utf8View),
    (text_18, "text_18", Option<SqlString>, DataType::Utf8View),
    (text_19, "text_19", Option<SqlString>, DataType::Utf8View),
    (time_06, "time_06", Option<Timestamp>, ts()),
    (amount_03, "amount_03", Option<SqlDecimal<38, 18>>, dec()),
    (text_20, "text_20", Option<SqlString>, DataType::Utf8View),
    (text_21, "text_21", Option<SqlString>, DataType::Utf8View),
    (text_22, "text_22", Option<SqlString>, DataType::Utf8View),
    (tags_04, "tags_04", Option<Array<Option<SqlString>>>, str_list()),
    (amount_04, "amount_04", Option<SqlDecimal<38, 18>>, dec()),
    (text_23, "text_23", Option<SqlString>, DataType::Utf8View),
    (text_24, "text_24", Option<SqlString>, DataType::Utf8View),
    (text_25, "text_25", Option<SqlString>, DataType::Utf8View),
    (text_26, "text_26", Option<SqlString>, DataType::Utf8View),
    (ratio_01, "ratio_01", Option<F64>, DataType::Float64),
    (time_07, "time_07", Option<Timestamp>, ts()),
    (flag_04, "flag_04", Option<bool>, DataType::Boolean),
    (text_27, "text_27", Option<SqlString>, DataType::Utf8View),
    (flag_05, "flag_05", Option<bool>, DataType::Boolean),
    (ratio_02, "ratio_02", Option<F64>, DataType::Float64),
    (text_28, "text_28", Option<SqlString>, DataType::Utf8View),
    (amount_05, "amount_05", Option<SqlDecimal<38, 18>>, dec()),
);

#[derive(Debug, Clone)]
pub struct Record {
    meta: Option<Meta>,
    source: Option<Source>,
    after: Option<Change>,
    before: Option<Change>,
    ts_ms: Option<Timestamp>,
    op: Option<SqlString>,
}

deserialize_table_record!(Record["wide_cdc_record", SqlVariant, 6] {
    (meta, "meta", false, Option<Meta>, |_| Some(None)),
    (source, "source", false, Option<Source>, |_| Some(None)),
    (after, "after", false, Option<Change>, |_| Some(None)),
    (before, "before", false, Option<Change>, |_| Some(None)),
    (ts_ms, "ts_ms", false, Option<Timestamp>, |_| Some(None)),
    (op, "op", false, Option<SqlString>, |_| Some(None))
});

fn schema() -> Schema {
    let meta = Fields::from(vec![
        Field::new("time_01", ts(), true),
        Field::new("time_02", ts(), true),
    ]);
    let source = Fields::from(vec![
        Field::new("count_01", DataType::Int64, true),
        Field::new("tags_01", str_list(), true),
        Field::new("text_01", DataType::Utf8View, true),
        Field::new("text_02", DataType::Utf8View, true),
        Field::new("count_02", DataType::Int64, true),
        Field::new("time_03", ts(), true),
        Field::new("text_03", DataType::Utf8View, true),
        Field::new("text_04", DataType::Utf8View, true),
        Field::new("text_05", DataType::Utf8View, true),
        Field::new("text_06", DataType::Utf8View, true),
        Field::new("text_07", DataType::Utf8View, true),
        Field::new("count_03", DataType::Int64, true),
    ]);
    Schema::new(vec![
        Field::new("meta", DataType::Struct(meta), true),
        Field::new("source", DataType::Struct(source), true),
        Field::new("after", DataType::Struct(change_fields()), true),
        Field::new("before", DataType::Struct(change_fields()), true),
        Field::new("ts_ms", ts(), true),
        Field::new("op", DataType::Utf8View, true),
    ])
}

/// A random array of `rows` values of `data_type`, about 5 % of them NULL.
fn random_array(data_type: &DataType, rows: usize, rng: &mut SmallRng) -> ArrayRef {
    fn word(rng: &mut SmallRng) -> String {
        let len = rng.gen_range(8..40);
        (0..len)
            .map(|_| rng.gen_range(b'a'..=b'z') as char)
            .collect()
    }
    let null = |rng: &mut SmallRng| rng.gen_bool(0.05);

    match data_type {
        DataType::Utf8View => {
            let mut b = StringViewBuilder::new();
            for _ in 0..rows {
                if null(rng) {
                    b.append_null()
                } else {
                    b.append_value(word(rng))
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for _ in 0..rows {
                b.append_option((!null(rng)).then(|| rng.r#gen()));
            }
            Arc::new(b.finish())
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for _ in 0..rows {
                b.append_option((!null(rng)).then(|| rng.gen_range(-180.0..180.0)));
            }
            Arc::new(b.finish())
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for _ in 0..rows {
                b.append_option((!null(rng)).then(|| rng.r#gen()));
            }
            Arc::new(b.finish())
        }
        DataType::Decimal128(p, s) => {
            let mut b = Decimal128Builder::new();
            for _ in 0..rows {
                // Up to 1e9 with 18 fractional digits.
                b.append_option(
                    (!null(rng)).then(|| rng.gen_range(0..1_000_000_000_000_000_000_000_000i128)),
                );
            }
            Arc::new(b.finish().with_precision_and_scale(*p, *s).unwrap())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::new();
            for _ in 0..rows {
                // 2025 and 2026.
                b.append_option(
                    (!null(rng))
                        .then(|| rng.gen_range(1_735_689_600_000_000..1_798_761_600_000_000)),
                );
            }
            Arc::new(b.finish().with_timezone_opt(tz.clone()))
        }
        DataType::List(item) => {
            let mut b = ListBuilder::new(StringViewBuilder::new()).with_field(item.clone());
            for _ in 0..rows {
                if null(rng) {
                    b.append_null();
                } else {
                    for _ in 0..rng.gen_range(0..3) {
                        b.values().append_value(word(rng));
                    }
                    b.append(true);
                }
            }
            Arc::new(b.finish())
        }
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|f| random_array(f.data_type(), rows, rng))
                .collect();
            Arc::new(StructArray::new(fields.clone(), children, None))
        }
        other => unimplemented!("{other}"),
    }
}

fn random_batch(rows: usize) -> RecordBatch {
    let mut rng = SmallRng::seed_from_u64(0);
    let schema = Arc::new(schema());
    let columns = schema
        .fields()
        .iter()
        .map(|f| random_array(f.data_type(), rows, &mut rng))
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// What the connector's parser task does with a batch, without the Z-set.
fn parse(batch: &RecordBatch, fields: &[SqlField]) -> Vec<Record> {
    let batch = timestamps_to_micros(batch.clone(), fields).unwrap();
    let (batch, by_name) = order_columns(batch, fields);
    assert_eq!(by_name, None);
    // As `ArrowZSetStream` does: positional when the columns are in order.
    let config = ArrowSerdeConfig::new(delta_input_serde_config());
    let deserializer = arrow_deserializer(&batch).unwrap();
    Vec::<Record>::deserialize_with_context_aux(deserializer, &config.for_batch(&batch), &None)
        .unwrap()
}

/// The SQL columns that match `fields`. Only the ROW structure and the decimals
/// matter to `order_columns`, so every other type is a placeholder.
fn sql_fields(fields: &Fields) -> Vec<SqlField> {
    fields
        .iter()
        .map(|field| {
            let column_type = match field.data_type() {
                DataType::Struct(fields) => ColumnType::structure(true, &sql_fields(fields)),
                DataType::List(item) => ColumnType::array(
                    true,
                    sql_fields(&Fields::from(vec![item.clone()]))
                        .remove(0)
                        .columntype,
                ),
                DataType::Decimal128(precision, scale) => {
                    ColumnType::decimal(i64::from(*precision), i64::from(*scale), true)
                }
                _ => ColumnType::bigint(true),
            };
            SqlField::new(SqlIdentifier::new(field.name(), false), column_type)
        })
        .collect()
}

fn bench_delta_parse(c: &mut Criterion) {
    let batch = random_batch(BATCH_ROWS);
    let fields = sql_fields(batch.schema_ref().fields());
    assert_eq!(parse(&batch, &fields).len(), BATCH_ROWS);

    let mut group = c.benchmark_group("delta_parse");
    group.throughput(Throughput::Elements(BATCH_ROWS as u64));
    group.bench_function("wide_cdc_record", |b| b.iter(|| parse(&batch, &fields)));
    group.finish();
}

criterion_group!(benches, bench_delta_parse);
criterion_main!(benches);
