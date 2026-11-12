//! The rate at which a pipeline reads a Delta table of wide CDC records: the
//! Delta Lake input connector in `snapshot` mode feeds an input table of a
//! circuit that counts the records.
//!
//! The first run writes the table. Arguments: the table directory, the number
//! of batches of 8192 rows in a new table, and `num_parsers` (0 for the
//! default). The record is the one of the `delta_parse` benchmark.

#![allow(non_snake_case)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayRef, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, ListBuilder,
    RecordBatch, StringBuilder, StructArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use dbsp::{BatchReader, NumEntries, OrdZSet, Runtime};
use dbsp_connectors::Controller;
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTableBuilder, ensure_table_uri};
use feldera_macros::{IsNone, OrdRepr};
use feldera_sqllib::{Array, F64, SqlDecimal, SqlString, SqlVariant, Timestamp};
use feldera_types::config::PipelineConfig;
use feldera_types::program_schema::{ColumnType, Field as SqlField, Relation, SqlIdentifier};
use feldera_types::{deserialize_table_record, serialize_table_record};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use size_of::SizeOf;
use static_connectors::Catalog;
use static_connectors::engine::{static_circuit_factory, static_open_storage};

// Pipelines allocate through jemalloc.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BATCH_ROWS: usize = 8192;

/// Declares a struct, its serializer, its deserializer and its Arrow fields
/// from one list.
macro_rules! record_struct {
    ($name:ident, $sql_name:literal, $n:literal, $fields_fn:ident,
     $(($field:ident, $col:literal, $ty:ty, $arrow:expr)),* $(,)?) => {
        #[derive(
            Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Hash, SizeOf,
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, IsNone, OrdRepr,
        )]
        #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
        pub struct $name {
            $($field: $ty,)*
        }

        serialize_table_record!($name[$n]{
            $($field[$col]: $ty),*
        });

        deserialize_table_record!($name[$sql_name, SqlVariant, $n] {
            $(($field, $col, false, $ty, |_| Some(None))),*
        });

        fn $fields_fn() -> Fields {
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
    DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)))
}

record_struct!(
    Meta, "Meta", 2, meta_fields,
    (time_01, "time_01", Option<Timestamp>, ts()),
    (time_02, "time_02", Option<Timestamp>, ts()),
);

record_struct!(
    Source, "Source", 12, source_fields,
    (count_01, "count_01", Option<i64>, DataType::Int64),
    (tags_01, "tags_01", Option<Array<Option<SqlString>>>, str_list()),
    (text_01, "text_01", Option<SqlString>, DataType::Utf8),
    (text_02, "text_02", Option<SqlString>, DataType::Utf8),
    (count_02, "count_02", Option<i64>, DataType::Int64),
    (time_03, "time_03", Option<Timestamp>, ts()),
    (text_03, "text_03", Option<SqlString>, DataType::Utf8),
    (text_04, "text_04", Option<SqlString>, DataType::Utf8),
    (text_05, "text_05", Option<SqlString>, DataType::Utf8),
    (text_06, "text_06", Option<SqlString>, DataType::Utf8),
    (text_07, "text_07", Option<SqlString>, DataType::Utf8),
    (count_03, "count_03", Option<i64>, DataType::Int64),
);

record_struct!(
    Change, "Change", 41, change_fields,
    (time_04, "time_04", Option<Timestamp>, ts()),
    (text_08, "text_08", Option<SqlString>, DataType::Utf8),
    (amount_01, "amount_01", Option<SqlDecimal<38, 18>>, dec()),
    (text_09, "text_09", Option<SqlString>, DataType::Utf8),
    (text_10, "text_10", Option<SqlString>, DataType::Utf8),
    (time_05, "time_05", Option<Timestamp>, ts()),
    (flag_01, "flag_01", Option<bool>, DataType::Boolean),
    (text_11, "text_11", Option<SqlString>, DataType::Utf8),
    (flag_02, "flag_02", Option<bool>, DataType::Boolean),
    (text_12, "text_12", Option<SqlString>, DataType::Utf8),
    (text_13, "text_13", Option<SqlString>, DataType::Utf8),
    (text_14, "text_14", Option<SqlString>, DataType::Utf8),
    (tags_02, "tags_02", Option<Array<Option<SqlString>>>, str_list()),
    (text_15, "text_15", Option<SqlString>, DataType::Utf8),
    (flag_03, "flag_03", Option<bool>, DataType::Boolean),
    (text_16, "text_16", Option<SqlString>, DataType::Utf8),
    (amount_02, "amount_02", Option<SqlDecimal<38, 18>>, dec()),
    (tags_03, "tags_03", Option<Array<Option<SqlString>>>, str_list()),
    (count_04, "count_04", Option<i64>, DataType::Int64),
    (text_17, "text_17", Option<SqlString>, DataType::Utf8),
    (text_18, "text_18", Option<SqlString>, DataType::Utf8),
    (text_19, "text_19", Option<SqlString>, DataType::Utf8),
    (time_06, "time_06", Option<Timestamp>, ts()),
    (amount_03, "amount_03", Option<SqlDecimal<38, 18>>, dec()),
    (text_20, "text_20", Option<SqlString>, DataType::Utf8),
    (text_21, "text_21", Option<SqlString>, DataType::Utf8),
    (text_22, "text_22", Option<SqlString>, DataType::Utf8),
    (tags_04, "tags_04", Option<Array<Option<SqlString>>>, str_list()),
    (amount_04, "amount_04", Option<SqlDecimal<38, 18>>, dec()),
    (text_23, "text_23", Option<SqlString>, DataType::Utf8),
    (text_24, "text_24", Option<SqlString>, DataType::Utf8),
    (text_25, "text_25", Option<SqlString>, DataType::Utf8),
    (text_26, "text_26", Option<SqlString>, DataType::Utf8),
    (ratio_01, "ratio_01", Option<F64>, DataType::Float64),
    (time_07, "time_07", Option<Timestamp>, ts()),
    (flag_04, "flag_04", Option<bool>, DataType::Boolean),
    (text_27, "text_27", Option<SqlString>, DataType::Utf8),
    (flag_05, "flag_05", Option<bool>, DataType::Boolean),
    (ratio_02, "ratio_02", Option<F64>, DataType::Float64),
    (text_28, "text_28", Option<SqlString>, DataType::Utf8),
    (amount_05, "amount_05", Option<SqlDecimal<38, 18>>, dec()),
);

record_struct!(
    Record, "wide_cdc_record", 6, record_fields,
    (meta, "meta", Option<Meta>, DataType::Struct(meta_fields())),
    (source, "source", Option<Source>, DataType::Struct(source_fields())),
    (after, "after", Option<Change>, DataType::Struct(change_fields())),
    (before, "before", Option<Change>, DataType::Struct(change_fields())),
    (ts_ms, "ts_ms", Option<Timestamp>, ts()),
    (op, "op", Option<SqlString>, DataType::Utf8),
);

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
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
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
            let mut b = ListBuilder::new(StringBuilder::new()).with_field(item.clone());
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

fn random_batch(rows: usize, seed: u64) -> RecordBatch {
    let mut rng = SmallRng::seed_from_u64(seed);
    let schema = Arc::new(Schema::new(record_fields()));
    let columns = schema
        .fields()
        .iter()
        .map(|f| random_array(f.data_type(), rows, &mut rng))
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}

/// The SQL columns that match `fields`.
fn sql_fields(fields: &Fields) -> Vec<SqlField> {
    fn column_type(data_type: &DataType) -> ColumnType {
        match data_type {
            DataType::Struct(fields) => ColumnType::structure(true, &sql_fields(fields)),
            DataType::List(item) => ColumnType::array(true, column_type(item.data_type())),
            DataType::Decimal128(precision, scale) => {
                ColumnType::decimal(i64::from(*precision), i64::from(*scale), true)
            }
            DataType::Timestamp(_, _) => ColumnType::timestamp(true),
            DataType::Utf8 => ColumnType::varchar(true),
            DataType::Int64 => ColumnType::bigint(true),
            DataType::Float64 => ColumnType::double(true),
            DataType::Boolean => ColumnType::boolean(true),
            other => unimplemented!("{other}"),
        }
    }
    fields
        .iter()
        .map(|field| {
            SqlField::new(
                SqlIdentifier::new(field.name(), false),
                column_type(field.data_type()),
            )
        })
        .collect()
}

/// Writes `batches` batches, one data file each, to a new table at `path`.
fn write_table(path: &str, batches: usize) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut table = DeltaTableBuilder::from_url(ensure_table_uri(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for seed in 0..batches {
            table = table
                .write(vec![random_batch(BATCH_ROWS, seed as u64)])
                .with_save_mode(SaveMode::Append)
                .await
                .unwrap();
        }
    });
}

fn main() {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| a != "--bench")
        .collect();
    let [path, batches, num_parsers] = args.as_slice() else {
        panic!("arguments: <table directory> <batches> <num_parsers, 0 for the default>");
    };
    let batches: usize = batches.parse().unwrap();
    let num_parsers: u32 = num_parsers.parse().unwrap();

    if !std::path::Path::new(path).join("_delta_log").exists() {
        std::fs::create_dir_all(path).unwrap();
        let start = Instant::now();
        write_table(path, batches);
        println!("wrote {batches} batches in {:?}", start.elapsed());
    }

    let mut connector = serde_json::json!({"uri": path, "mode": "snapshot"});
    if num_parsers != 0 {
        connector["num_parsers"] = num_parsers.into();
    }
    let config: PipelineConfig = serde_json::from_value(serde_json::json!({
        "name": "delta_pipeline",
        "workers": 8,
        "inputs": {
            "wide": {
                "stream": "wide",
                "transport": {"name": "delta_table_input", "config": connector}
            }
        }
    }))
    .unwrap();

    let records = Arc::new(AtomicUsize::new(0));
    let counter = records.clone();
    let factory = static_circuit_factory(Box::new(move |config| {
        let (circuit, catalog) = Runtime::init_circuit(config, move |circuit| {
            let mut catalog = Catalog::new();
            let (input, handle) = circuit.add_input_zset::<Record>();
            let schema = serde_json::to_string(&Relation::new(
                "wide".into(),
                sql_fields(&record_fields()),
                false,
                Default::default(),
            ))
            .unwrap();
            catalog.register_input_zset::<OrdZSet<Record>, Record>(input.clone(), handle, &schema);
            input.inspect(move |batch| {
                counter.fetch_add(batch.inner().num_entries_shallow(), Ordering::Relaxed);
            });
            Ok(catalog)
        })
        .unwrap();
        Ok((circuit, Box::new(catalog)))
    }));

    let controller = Controller::with_test_config(
        factory,
        &static_open_storage(),
        &config,
        Box::new(|error, _| panic!("pipeline error: {error}")),
    )
    .unwrap();

    let start = Instant::now();
    controller.start();
    while !controller.pipeline_complete() {
        std::thread::sleep(Duration::from_millis(20));
    }
    let elapsed = start.elapsed();
    let records = records.load(Ordering::Relaxed);
    println!(
        "{records} records in {:.2} s: {:.0} records/s",
        elapsed.as_secs_f64(),
        records as f64 / elapsed.as_secs_f64()
    );
    controller.stop().unwrap();
}
