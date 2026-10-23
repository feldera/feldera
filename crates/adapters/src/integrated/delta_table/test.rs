//! Tests of the Delta Lake connectors that reach no engine. The `delta_table` connector suite
//! holds the pipeline tests.

use crate::format::parquet::relation_to_arrow_fields;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use deltalake::datafusion::prelude::{SessionContext, col, lit};
use deltalake::kernel::{DataType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::table::config::TableProperty;
use deltalake::{DeltaTable, DeltaTableBuilder, ensure_table_uri};
use feldera_types::program_schema::Field;
use roaring::RoaringTreemap;
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;
use std::mem::forget;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use super::tests::{create_table, delta_schema_options, init_logging, z85_encode};

/// Assert the logical-plan shape of the CDC `DataFrame`: filter on
/// both sides of an `EXCEPT ALL`, then a sort by `cdc_order_by`. Also
/// checks that an `order_by` referencing an unknown column fails at
/// build time.
#[tokio::test]
async fn build_cdc_dataframe_plan_structure() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
    use datafusion::datasource::MemTable;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("__feldera_op", ArrowDataType::Utf8, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    // Two independently-registered tables so the filter expression has
    // to resolve against each side of the set difference.
    let ctx = SessionContext::new();
    let empty = || Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap());
    ctx.register_table("cdc_adds", empty()).unwrap();
    ctx.register_table("cdc_removes", empty()).unwrap();

    // Full path: filter + removes + order_by.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        Some("id % 2 = 0"),
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());

    assert!(
        plan.contains("Sort: "),
        "expected a Sort node; plan was:\n{plan}"
    );
    assert!(
        plan.contains("__feldera_ts"),
        "expected sort key '__feldera_ts'; plan was:\n{plan}"
    );
    assert!(
        plan.contains("LeftAnti"),
        "expected a LeftAnti join (DataFusion encoding of EXCEPT); plan was:\n{plan}"
    );
    // EXCEPT ALL uses a LeftAnti join without a Distinct wrapper;
    // EXCEPT DISTINCT inserts a `Distinct:` node above the join.
    assert!(
        !plan.contains("Distinct:"),
        "expected EXCEPT ALL (no Distinct node); plan was:\n{plan}"
    );
    let filter_lines = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("Filter:"))
        .count();
    assert_eq!(
        filter_lines, 2,
        "expected Filter applied to both adds and removes; plan was:\n{plan}"
    );

    // No removes: no set-difference in the plan.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        Some("id % 2 = 0"),
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());
    assert!(
        plan.contains("Sort: "),
        "expected Sort even without removes; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("LeftAnti"),
        "no removes -> no LeftAnti join; plan was:\n{plan}"
    );
    let filter_lines = plan
        .lines()
        .filter(|line| line.trim_start().starts_with("Filter:"))
        .count();
    assert_eq!(
        filter_lines, 1,
        "expected one Filter (adds only); plan was:\n{plan}"
    );

    // No filter: set difference and sort, no Filter node.
    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        None,
        "__feldera_ts",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());
    assert!(plan.contains("Sort: "), "expected Sort; plan was:\n{plan}");
    assert!(
        plan.contains("LeftAnti"),
        "expected LeftAnti join; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Filter:"),
        "no filter configured -> no Filter node; plan was:\n{plan}"
    );

    // Invalid `order_by`: error surfaces at build time.
    let err = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        None,
        "no_such_column",
        "unit-test",
    )
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("no_such_column"),
        "expected error to name the missing column; got: {err}"
    );
    assert!(
        err.contains("cdc_order_by"),
        "expected error to mention 'cdc_order_by'; got: {err}"
    );
}

/// A `cdc_order_by` with several keys and explicit ASC/DESC directions
/// (a real ORDER BY clause body) must build a multi-key Sort. Guards a
/// regression where the clause was parsed as a single scalar expression.
#[tokio::test]
async fn build_cdc_dataframe_multi_key_order_by() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, TimeUnit};
    use datafusion::datasource::MemTable;
    use feldera_adapterlib::utils::datafusion::validate_sql_order_by;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new(
            "__feldera_ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    let ctx = SessionContext::new();
    ctx.register_table(
        "cdc_adds",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();

    let df = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        None,
        None,
        "__feldera_ts asc, id desc",
        "unit-test",
    )
    .unwrap();
    let plan = format!("{}", df.logical_plan().display_indent());

    // One Sort node carrying both keys with their directions.
    assert!(
        plan.contains("__feldera_ts ASC") && plan.contains("id DESC"),
        "expected both sort keys with directions; plan was:\n{plan}"
    );

    // The validator accepts the same multi-key clause and rejects garbage.
    assert!(validate_sql_order_by("__feldera_ts asc, id desc").is_ok());
    assert!(validate_sql_order_by("ts asc !! lsn").is_err());
}

/// `parse_cdc_order_by` applies DataFusion's SQL planner defaults: a missing
/// direction is `ASC`, a missing `NULLS` placement is nulls-last for `ASC` and
/// nulls-first for `DESC` (the `nulls_max` convention), and explicit `ASC` /
/// `DESC` / `NULLS FIRST` / `NULLS LAST` override those defaults.
#[tokio::test]
async fn parse_cdc_order_by_defaults() {
    use crate::integrated::delta_table::input::parse_cdc_order_by;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Int64, false),
    ]));
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();
    let df = ctx.table("t").await.unwrap();

    // (clause, expected [(asc, nulls_first), ...] per key).
    let cases: &[(&str, &[(bool, bool)])] = &[
        // No direction => ASC; ASC default nulls placement is last.
        ("a", &[(true, false)]),
        ("a asc", &[(true, false)]),
        // DESC default nulls placement is first.
        ("a desc", &[(false, true)]),
        // Explicit NULLS overrides the default for either direction.
        ("a nulls first", &[(true, true)]),
        ("a asc nulls first", &[(true, true)]),
        ("a desc nulls last", &[(false, false)]),
        // Each key carries its own direction and default.
        ("a asc, b desc", &[(true, false), (false, true)]),
    ];

    for (clause, expected) in cases {
        let sort_exprs = parse_cdc_order_by(&df, clause).unwrap();
        let got: Vec<(bool, bool)> = sort_exprs.iter().map(|s| (s.asc, s.nulls_first)).collect();
        assert_eq!(
            got.as_slice(),
            *expected,
            "clause '{clause}': expected {expected:?}, got {got:?}"
        );
    }
}

/// Runs the real CDC `EXCEPT ALL` over a `Map` column; the rewritten row must cancel.
#[tokio::test]
async fn except_all_cancels_a_rewrite_on_a_map_column() {
    use crate::integrated::delta_table::input::build_cdc_dataframe;
    use arrow::array::{Array, ArrayRef, Int64Array, MapBuilder, RecordBatch, StringBuilder};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;

    // The schema must repeat the builder's entry field names exactly.
    let map_column = |keys: &[&str]| -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for key in keys {
            builder.keys().append_value(*key);
            builder.values().append_value(format!("v-{key}"));
            builder.append(true).unwrap();
        }
        Arc::new(builder.finish())
    };

    let probe = map_column(&["a"]);
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("m", probe.data_type().clone(), false),
    ]));

    let batch = |ids: &[i64], keys: &[&str]| {
        vec![vec![
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
                    map_column(keys),
                ],
            )
            .unwrap(),
        ]]
    };

    let ctx = SessionContext::new();
    // Row 1 is rewritten unchanged, so its Add and Remove must cancel.
    ctx.register_table(
        "cdc_adds",
        Arc::new(MemTable::try_new(arrow_schema.clone(), batch(&[1, 2], &["a", "b"])).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "cdc_removes",
        Arc::new(MemTable::try_new(arrow_schema.clone(), batch(&[1], &["a"])).unwrap()),
    )
    .unwrap();

    let rows = build_cdc_dataframe(
        ctx.table("cdc_adds").await.unwrap(),
        Some(ctx.table("cdc_removes").await.unwrap()),
        None,
        "id",
        "unit-test",
    )
    .unwrap()
    .collect()
    .await
    .unwrap();

    let ids: Vec<i64> = rows
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(
        ids,
        vec![2],
        "the unchanged row must cancel against its Remove"
    );
}

/// A change feed read prunes from the scan a column that neither the filter nor
/// the projection names, even though the declared read schema lists it.
///
/// `skip_unused_columns` on the change feed path rests on DataFusion's
/// projection pushdown rather than on the declared read schema:
/// `change_data_read_schema` declares every column, and the projection to
/// `used_columns` comes several nodes later, after the physical-to-logical
/// rename, the partition literals, and a union across partition groups.
/// [`follow_filter_before_projection_prunes_scan`] asserts the same for a follow
/// read, which stacks fewer nodes between the scan and the projection.
#[tokio::test]
async fn change_feed_projection_prunes_scan() {
    use crate::integrated::delta_table::input::apply_filter;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    // A change data file's columns: the table's own, minus the partition column
    // that lives in the log, plus `_change_type`. `region` is filtered on but
    // not projected; `junk` is neither.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("junk", ArrowDataType::Utf8, false),
        ArrowField::new("_change_type", ArrowDataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    let mut groups = Vec::new();
    for (i, grp) in ["0", "1"].iter().enumerate() {
        let name = format!("cdc_{i}");
        ctx.register_table(
            name.as_str(),
            Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
        )
        .unwrap();
        // What `add_partition_columns` produces: the table's columns in schema
        // order, the partition column supplied as a literal from the log, and
        // the extra column carried through.
        groups.push(
            ctx.table(name.as_str())
                .await
                .unwrap()
                .select(vec![
                    col("id"),
                    col("s"),
                    col("region"),
                    col("junk"),
                    lit(*grp).alias("grp"),
                    col("_change_type"),
                ])
                .unwrap(),
        );
    }

    let mut groups = groups.into_iter();
    let df = groups.next().unwrap();
    let df = groups.try_fold(df, |acc, g| acc.union(g)).unwrap();
    let df = apply_filter(df, Some("region = 'us'"), "unit-test", None).unwrap();
    let df = df
        .select_columns(&["id", "s", "grp", "_change_type"])
        .unwrap();

    let plan = format!("{}", df.into_optimized_plan().unwrap().display_indent());
    assert!(
        plan.contains("projection=") && !plan.contains("junk"),
        "the scan must read only the projected columns plus the filter's, so \
         'junk' must be pruned through the partition literals and the union; \
         plan was:\n{plan}"
    );
}

#[tokio::test]
async fn follow_filter_before_projection_prunes_scan() {
    use crate::integrated::delta_table::input::apply_filter;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    // `region` is filtered on but not projected; `junk` is neither.
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("b", ArrowDataType::Boolean, false),
        ArrowField::new("s", ArrowDataType::Utf8, false),
        ArrowField::new("region", ArrowDataType::Utf8, false),
        ArrowField::new("junk", ArrowDataType::Utf8, false),
    ]));

    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(arrow_schema.clone(), vec![vec![]]).unwrap()),
    )
    .unwrap();

    let df = apply_filter(
        ctx.table("t").await.unwrap(),
        Some("region = 'us'"),
        "unit-test",
        None,
    )
    .unwrap();
    let df = df.select_columns(&["id", "b", "s"]).unwrap();

    let plan = format!("{}", df.into_optimized_plan().unwrap().display_indent());
    assert!(
        plan.contains("projection=") && !plan.contains("junk"),
        "the scan must read only the projected columns plus the filter's, so \
         'junk' must be pruned; plan was:\n{plan}"
    );
}

/// A `VARIANT` nested in a `ROW`, an `ARRAY` and a `MAP` must be declared as a
/// Delta `variant`, not as the struct of two binary buffers it is encoded as.
///
/// The data is written as a variant either way, so a Feldera-to-Feldera round
/// trip cannot see the difference: the reader recognizes the buffers by name.
/// Only the declared type tells another engine what the column holds.
#[test]
fn delta_output_nested_variant_type_test() {
    use crate::integrated::delta_table::output::delta_variant_types;
    use delta_kernel::engine::arrow_conversion::TryFromArrow;
    use deltalake::kernel::DataType;
    use feldera_types::program_schema::ColumnType;

    let variant = || ColumnType::variant(true);
    let cases = [
        ("plain", variant()),
        (
            "row",
            ColumnType::structure(true, &[Field::new("v".into(), variant())]),
        ),
        ("array", ColumnType::array(true, variant())),
        (
            "map",
            ColumnType::map(true, ColumnType::varchar(false), variant()),
        ),
        (
            "row_of_array",
            ColumnType::structure(
                true,
                &[Field::new("a".into(), ColumnType::array(true, variant()))],
            ),
        ),
    ];

    for (name, sql_type) in cases {
        let arrow = relation_to_arrow_fields(
            &[Field::new(name.into(), sql_type.clone())],
            delta_schema_options(),
        );
        let from_arrow = DataType::try_from_arrow(arrow[0].data_type()).unwrap();

        // What the Arrow type alone says: the storage struct, at every depth.
        let raw = serde_json::to_string(&from_arrow).unwrap();
        assert!(
            raw.contains("\"name\":\"metadata\""),
            "{name} was expected to arrive as the storage struct: {raw}"
        );

        let json =
            serde_json::to_string(&delta_variant_types(&sql_type, from_arrow).unwrap()).unwrap();
        assert!(
            json.contains("\"variant\""),
            "{name} declares no variant: {json}"
        );
        assert!(
            !json.contains("\"name\":\"metadata\""),
            "{name} still declares the storage struct: {json}"
        );
    }
}

/// Appending to a table an earlier Feldera created is the likeliest upgrade
/// failure of the default flip, and nothing downstream catches it: `append`
/// opens the existing table with `SaveMode::Ignore`, which keeps its schema,
/// the writer validates each batch against the schema the connector computed
/// itself, and the commit checks no schema at all. Without this guard the
/// connector writes variant buffers into a column the table declares a
/// `string`, with no error.
#[tokio::test]
async fn delta_output_variant_encoding_mismatch_test() {
    use crate::integrated::delta_table::output::check_variant_encoding;
    use feldera_types::transport::delta_table::DeltaVariantEncoding;

    init_logging();

    let string_column = StructField::new("v", DataType::STRING, true);
    let variant_column = StructField::new("v", DataType::unshredded_variant(), true);

    for (existing, configured, encoding, expected) in [
        (
            &string_column,
            &variant_column,
            DeltaVariantEncoding::Variant,
            "'json_string'",
        ),
        (
            &variant_column,
            &string_column,
            DeltaVariantEncoding::JsonString,
            "'variant'",
        ),
    ] {
        let table_dir = TempDir::new().unwrap();
        let table_uri = table_dir.path().display().to_string();
        let table = create_table(&table_uri, &HashMap::new(), std::slice::from_ref(existing)).await;

        let error = check_variant_encoding(&table, std::slice::from_ref(configured), encoding)
            .unwrap_err()
            .to_string();
        assert!(error.contains("column 'v'"), "{error}");
        assert!(error.contains(expected), "{error}");

        // The matching encoding is accepted.
        check_variant_encoding(&table, std::slice::from_ref(existing), encoding).unwrap();
    }
}

/// CRC-32/ISO-HDLC, the trailer of a deletion-vector file.
///
/// Written out here because the workspace's `crc32c` uses a different
/// polynomial.
fn crc32_iso_hdlc(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

/// Build one deletion vector file in the layout the Delta protocol specifies,
/// and return it with the `sizeInBytes` its descriptor must carry.
///
/// ```text
/// 1 byte    version (1)
/// 4 bytes   dv_size, big endian      <- the descriptor's offset points here
/// 4 bytes   magic, little endian
/// n bytes   portable Roaring bitmap
/// 4 bytes   CRC-32/ISO-HDLC over magic..=bitmap, big endian
/// ```
fn deletion_vector_file(deleted_rows: &[u64]) -> (Vec<u8>, i32) {
    /// Delta marks a deletion vector's bitmap with this, in little-endian bytes.
    const PORTABLE_ROARING_MAGIC: u32 = 1681511377;

    let bitmap: RoaringTreemap = deleted_rows.iter().copied().collect();
    let mut bitmap_bytes = Vec::new();
    bitmap.serialize_into(&mut bitmap_bytes).unwrap();

    // `dv_size` covers the magic and the bitmap, but not the CRC.
    let mut checksummed = PORTABLE_ROARING_MAGIC.to_le_bytes().to_vec();
    checksummed.extend_from_slice(&bitmap_bytes);
    let dv_size = checksummed.len() as u32;

    let mut bytes = vec![1u8];
    bytes.extend_from_slice(&dv_size.to_be_bytes());
    bytes.extend_from_slice(&checksummed);
    bytes.extend_from_slice(&crc32_iso_hdlc(&checksummed).to_be_bytes());

    (bytes, dv_size as i32)
}

/// Scan planning must not need a blocking thread per deletion vector.
///
/// Loading a vector held a blocking thread while it waited on the kernel's
/// sync-over-async bridge, which needs a second one. Once the loads reached
/// `max_blocking_threads` the pool deadlocked, with no error. The fork's
/// `943cf48b` fixes it; eight vectors against four threads reproduce it.
/// The scan reports through a channel, so a regression fails here rather than
/// hanging the suite.
#[test]
fn delta_table_deletion_vector_scan_without_blocking_pool_test() {
    use deltalake::kernel::{Action, Add, DeletionVectorDescriptor, PrimitiveType, StorageType};

    const FILES: usize = 8;
    const ROWS_PER_FILE: i64 = 4;

    let dir = TempDir::new().unwrap();
    let root = dir.path().to_path_buf();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        arrow::datatypes::Field::new("id", ArrowDataType::Int64, false),
        arrow::datatypes::Field::new("val", ArrowDataType::Utf8, true),
    ]));

    // Written by hand: delta-rs reads deletion vectors but cannot write them.
    // Each file hides its second row.
    let mut adds = Vec::with_capacity(FILES);
    for index in 0..FILES {
        let file_name = format!("part-{index:05}.parquet");
        let first_id = index as i64 * ROWS_PER_FILE;
        let ids: Vec<i64> = (first_id..first_id + ROWS_PER_FILE).collect();
        let vals: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(ids)) as Arc<dyn arrow::array::Array>,
                Arc::new(arrow::array::StringArray::from(vals)),
            ],
        )
        .unwrap();
        let path = root.join(&file_name);
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            File::create(&path).unwrap(),
            arrow_schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let uuid = uuid::Uuid::new_v4();
        let (dv_bytes, size_in_bytes) = deletion_vector_file(&[1]);
        std::fs::write(root.join(format!("deletion_vector_{uuid}.bin")), dv_bytes).unwrap();

        adds.push(Add {
            path: file_name,
            size: std::fs::metadata(&path).unwrap().len() as i64,
            modification_time: 1_700_000_000_000,
            data_change: true,
            stats: Some(json!({ "numRecords": ROWS_PER_FILE }).to_string()),
            deletion_vector: Some(DeletionVectorDescriptor {
                storage_type: StorageType::UuidRelativePath,
                path_or_inline_dv: z85_encode(uuid.as_bytes()),
                offset: Some(1),
                size_in_bytes,
                cardinality: 1,
            }),
            ..Default::default()
        });
    }

    let table_uri = root.to_str().unwrap().to_string();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        CreateBuilder::new()
            .with_location(&table_uri)
            .with_columns([
                StructField::new("id", DataType::Primitive(PrimitiveType::Long), false),
                StructField::new("val", DataType::Primitive(PrimitiveType::String), true),
            ])
            .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
            .with_actions(adds.into_iter().map(Action::Add))
            .await
            .unwrap();
    });

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // Snapshot loading blocks a pooled thread and then waits on the same
        // pool, so four leaves room for that but not for eight vectors.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let rows = runtime.block_on(async {
            let table: DeltaTable =
                DeltaTableBuilder::from_url(ensure_table_uri(&table_uri).unwrap())
                    .unwrap()
                    .load()
                    .await
                    .unwrap();
            let ctx = SessionContext::new();
            let log_store = table.log_store();
            ctx.runtime_env()
                .register_object_store(log_store.root_url(), log_store.root_object_store());
            ctx.register_table("snapshot", table.table_provider().await.unwrap())
                .unwrap();
            ctx.sql("select \"id\" from snapshot")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>()
        });
        let _ = tx.send(rows);
        // Dropping a runtime joins its blocking threads. A regression leaves
        // one wedged, which would hang the suite's exit instead of failing.
        forget(runtime);
    });

    let rows = rx
        .recv_timeout(Duration::from_secs(120))
        .expect("scan planning wedged: the deletion vectors exhausted the blocking pool");
    assert_eq!(
        rows,
        FILES * (ROWS_PER_FILE as usize - 1),
        "every file's second row is deleted by its vector"
    );
}
