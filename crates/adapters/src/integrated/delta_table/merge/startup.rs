//! What merge mode checks before the first row moves.
//!
//! Anything that could stop the lookup from finding a row it should have found leaves two
//! live rows for one key, which nothing downstream would notice. So each such reason becomes
//! a startup error here rather than a silent divergence later.
//!
//! | Group | Checks | Failure mode they prevent |
//! |-------|--------|---------------------------|
//! | Can we tombstone at all | deletion vectors enabled, protocol writable, not append-only | Every commit rejected |
//! | Can we compare keys | unique key present, supported key types, key columns identical in view and table | A key that does not equal itself after a round trip through parquet |
//! | Can we address rows | at least one key column lives in the data files, change data feed off | Key values absent from the data file, or a change feed we do not maintain |

use std::collections::HashMap;

use anyhow::{Result as AnyResult, anyhow, bail};
use arrow::datatypes::Field as ArrowField;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use deltalake::DeltaTable;
use deltalake::kernel::transaction::PROTOCOL;
use deltalake::kernel::{DataType, StructField, StructType};
use deltalake::operations::get_num_idx_cols_and_stats_columns;
use feldera_types::program_schema::Relation;
use tracing::{info, warn};

use super::key::{KeyEncoder, key_leaf_paths, validate_key_types};
use super::prune::stats_pruning_sound;

/// Whether everything in the target table was written by this run.
///
/// Read from the table, not configured: a table with no data files holds nothing to look up,
/// which is what lets an insert skip the lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// The table was empty when the connector opened it, so a key new to the view is
    /// absent from the table and an insert can skip the lookup.
    Owned,
    /// The table may hold a row for a key this run has never seen, so every changed key is
    /// looked up, inserts included.
    Default,
}

impl Regime {
    /// Whether an insert -- a key the view did not hold before this step -- needs a lookup.
    pub fn insert_needs_lookup(&self) -> bool {
        *self == Regime::Default
    }
}

/// Everything merge mode needs from the target table, resolved once at startup.
#[derive(Debug)]
pub struct MergeSetup {
    pub key_encoder: KeyEncoder,
    /// Arrow fields of the key columns, taken from the *table's* schema.
    ///
    /// Both sides of the key comparison are built from these. They come from the table rather
    /// than the view because Delta has no `LargeBinary` or `LargeList` and normalizes them on
    /// the way in, so only the table's types are what the probe will read back.
    pub key_arrow_fields: Vec<ArrowField>,
    pub regime: Regime,
    /// Partition columns of the target table, in table order, for the append writer.
    pub partition_columns: Vec<String>,
    /// Key columns that are also partition columns. Their values live in the log rather than
    /// the data files, so the probe reconstructs them per file.
    pub partition_key_columns: Vec<String>,
    /// How the target table says data-skipping statistics are collected. Read from the table,
    /// so this connector's files stay skippable on the same columns as every other writer's.
    pub stats_config: StatsConfig,
    /// Whether min/max statistics can be trusted to bound this key. False for a key holding a
    /// float, since NaN is left out of statistics. See [`super::prune`].
    pub prune_on_stats: bool,
}

/// The target table's data-skipping statistics configuration.
#[derive(Debug, Clone)]
pub struct StatsConfig {
    pub num_indexed_cols: DataSkippingNumIndexedCols,
    pub stats_columns: Option<Vec<String>>,
}

/// Check the target table against merge mode's requirements and resolve its setup.
///
/// `columns` is the schema the connector writes, derived from the view. Checking it here turns
/// a mismatch into one error naming the column, instead of a cast failure on the first flush.
pub fn prepare(
    table: &DeltaTable,
    key_schema: &Option<Relation>,
    columns: &[StructField],
    threads: usize,
) -> AnyResult<MergeSetup> {
    let key_schema = key_schema.as_ref().ok_or_else(|| {
        anyhow!(
            "'update_mode: merge' requires the view to have a unique key, so the connector \
             knows which row to supersede. Specify the 'index' property in the connector \
             configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys"
        )
    })?;

    if threads > 1 {
        // Each thread walks a disjoint key range and so would need its own pass over the
        // candidate files: the dominant cost of a flush, multiplied by the thread count.
        bail!(
            "'update_mode: merge' does not support 'threads' > 1 (configured: {threads}). \
             Merge mode already reads the target table concurrently while locating rows; \
             splitting the batch as well would repeat that work per thread."
        );
    }

    validate_key_types(key_schema).map_err(|e| anyhow!("{e}"))?;

    let snapshot = table
        .snapshot()
        .map_err(|e| anyhow!("unable to read the Delta table snapshot: {e}"))?;
    let properties = snapshot.snapshot().table_properties();

    if !properties.enable_deletion_vectors.unwrap_or(false) {
        bail!(
            "'update_mode: merge' requires the target Delta table to have deletion vectors \
             enabled, which is how the connector supersedes a row without rewriting the file \
             that holds it. Enable it with:\n    \
             ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')\n\
             The connector does not set the property itself, because doing so upgrades the \
             table's reader protocol and any reader that cannot read deletion vectors would \
             lose access to the table."
        );
    }

    if properties.enable_change_data_feed.unwrap_or(false) {
        bail!(
            "'update_mode: merge' cannot write to a table with 'delta.enableChangeDataFeed' \
             set, because a change data feed requires '_change_data' files describing every \
             update and delete, which this connector does not produce."
        );
    }

    if properties.append_only.unwrap_or(false) {
        bail!(
            "'update_mode: merge' cannot write to a table with 'delta.appendOnly' set: \
             superseding a row requires removing the old one."
        );
    }

    PROTOCOL.can_write_to(snapshot).map_err(|e| {
        anyhow!(
            "delta-rs cannot write to the target Delta table's protocol, so merge mode \
             cannot commit to it: {e}"
        )
    })?;

    check_schema_matches(snapshot.schema().as_ref(), columns)?;

    let partition_columns = snapshot.metadata().partition_columns().to_vec();
    let partition_key_columns = partition_key_columns(key_schema, &partition_columns)?;

    let table_arrow_schema = snapshot.snapshot().arrow_schema();
    let key_encoder = KeyEncoder::new(key_schema, table_arrow_schema.as_ref())?;
    let key_arrow_fields: Vec<ArrowField> = key_encoder
        .column_indices()
        .iter()
        .map(|i| table_arrow_schema.field(*i).clone())
        .collect();
    let prune_on_stats = stats_pruning_sound(&key_arrow_fields);

    let (num_indexed_cols, stats_columns) =
        get_num_idx_cols_and_stats_columns(Some(properties), HashMap::new());
    let stats_config = StatsConfig {
        num_indexed_cols,
        stats_columns,
    };
    report_unindexed_key_columns(
        snapshot.schema().as_ref(),
        &partition_columns,
        &stats_config,
        key_schema,
    );

    let regime = if snapshot.log_data().into_iter().next().is_none() {
        Regime::Owned
    } else {
        Regime::Default
    };

    info!(
        "delta merge mode: key ({}), {} partition column(s), {regime:?} regime{}",
        key_encoder.column_names().join(", "),
        partition_columns.len(),
        match regime {
            Regime::Owned => ": the table started empty, so inserts skip the row lookup",
            Regime::Default => ": the table already holds data, so every changed key is looked up",
        }
    );

    Ok(MergeSetup {
        key_encoder,
        key_arrow_fields,
        regime,
        partition_columns,
        partition_key_columns,
        stats_config,
        prune_on_stats,
    })
}

/// Warn when the table collects no statistics for a key column.
///
/// Not a correctness problem, since a missing statistic keeps the file, but it turns file
/// pruning off, and it would do so silently.
fn report_unindexed_key_columns(
    schema: &StructType,
    partition_columns: &[String],
    stats: &StatsConfig,
    key_schema: &Relation,
) {
    let leaves = data_file_leaves(schema, partition_columns);
    let uncovered: Vec<String> = key_leaf_paths(key_schema)
        .into_iter()
        // A partition column's value comes from the log, so it never needs a statistic.
        .filter(|path| !partition_columns.iter().any(|c| c == leading_segment(path)))
        .filter(|path| !covered_by_statistics(path, &leaves, stats))
        .collect();

    if uncovered.is_empty() {
        return;
    }

    warn!(
        "delta merge mode: the target table collects no statistics for key column(s) {}, so \
         the row lookup cannot skip files on their values and will open every file it has \
         not otherwise ruled out. Add them to the table's statistics columns:\n    \
         ALTER TABLE <table> SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '{}')",
        uncovered.join(", "),
        uncovered.join(",")
    );
}

/// Whether the Delta log will carry min/max for this leaf, following what the writer does: an
/// explicit column list wins and matches on the leaf's own name, else the first `n` leaves.
fn covered_by_statistics(path: &str, leaves: &[String], stats: &StatsConfig) -> bool {
    if let Some(columns) = &stats.stats_columns {
        let leaf = path.rsplit('.').next().unwrap_or(path);
        return columns.iter().any(|c| c == leaf);
    }
    match stats.num_indexed_cols {
        DataSkippingNumIndexedCols::AllColumns => true,
        DataSkippingNumIndexedCols::NumColumns(n) => leaves
            .iter()
            .position(|leaf| leaf == path)
            .is_some_and(|index| (index as u64) < n),
    }
}

/// Leaf paths of the table's data files, depth first.
///
/// `delta.dataSkippingNumIndexedCols` counts parquet leaves, not top-level columns, so an
/// early nested column pushes everything after it down the count. Partition columns are not
/// in the data files, so they take no position.
fn data_file_leaves(schema: &StructType, partition_columns: &[String]) -> Vec<String> {
    let mut leaves = Vec::new();
    for field in schema.fields() {
        if partition_columns.iter().any(|c| c == field.name()) {
            continue;
        }
        push_leaves(field.name(), field.data_type(), &mut leaves);
    }
    leaves
}

fn push_leaves(path: &str, data_type: &DataType, leaves: &mut Vec<String>) {
    match data_type {
        DataType::Struct(fields) => {
            for field in fields.fields() {
                push_leaves(
                    &format!("{path}.{}", field.name()),
                    field.data_type(),
                    leaves,
                );
            }
        }
        // As parquet flattens them: a list contributes its element's leaves, a map its key's
        // and its value's.
        DataType::Array(array) => push_leaves(path, array.element_type(), leaves),
        DataType::Map(map) => {
            push_leaves(path, map.key_type(), leaves);
            push_leaves(path, map.value_type(), leaves);
        }
        _ => leaves.push(path.to_string()),
    }
}

fn leading_segment(path: &str) -> &str {
    path.split('.').next().unwrap_or(path)
}

/// Require the table to hold exactly the columns the connector writes, with the same types.
///
/// A key column at another decimal scale or timestamp unit would not compare equal to itself
/// after the round trip through parquet, and a column the connector does not write would fill
/// with nulls on every append.
///
/// Compared as Delta types, not arrow types: arrow's `Binary` and `LargeBinary` are one Delta
/// type, so comparing arrow would report a mismatch that does not exist.
fn check_schema_matches(table: &StructType, expected: &[StructField]) -> AnyResult<()> {
    let mut mismatches = Vec::new();

    for field in expected {
        match table.field(field.name()) {
            Some(actual) if actual.data_type() == field.data_type() => {}
            Some(actual) => mismatches.push(format!(
                "column '{}': view has {:?}, table has {:?}",
                field.name(),
                field.data_type(),
                actual.data_type()
            )),
            None => mismatches.push(format!(
                "column '{}' is missing from the table",
                field.name()
            )),
        }
    }

    for field in table.fields() {
        if !expected.iter().any(|f| f.name() == field.name()) {
            mismatches.push(format!(
                "column '{}' exists in the table but not in the view",
                field.name()
            ));
        }
    }

    if !mismatches.is_empty() {
        bail!(
            "the target Delta table's schema does not match the view's, which merge mode \
             requires because it keeps the table in sync with the view rather than appending \
             to it: {}",
            mismatches.join("; ")
        );
    }
    Ok(())
}

/// Key columns that are also partition columns of the table.
///
/// The probe reconstructs these from each file's partition values, which works as long as at
/// least one key column is in the data files: it reads those to get one row per physical row
/// and fills the rest in as constants. A key made entirely of partition columns leaves nothing
/// to read.
fn partition_key_columns(
    key_schema: &Relation,
    partition_columns: &[String],
) -> AnyResult<Vec<String>> {
    let overlap: Vec<String> = key_schema
        .fields
        .iter()
        .map(|f| f.name.name())
        .filter(|name| partition_columns.contains(name))
        .collect();

    if overlap.len() == key_schema.fields.len() && !overlap.is_empty() {
        bail!(
            "'update_mode: merge' cannot use a key whose every column is a partition column \
             of the target table ({}). Delta keeps partition values in the log rather than in \
             the data files, so there would be nothing in the file to read the key from -- and \
             a table partitioned on its whole unique key has one directory per row. Partition \
             on a subset of the key, or on a column outside it.",
            overlap.join(", ")
        );
    }
    Ok(overlap)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::integrated::delta_table::merge::test::{
        fixture_columns, fixture_table, key_relation,
    };
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType};
    use tempfile::TempDir;

    #[tokio::test]
    async fn accepts_a_matching_deletion_vector_table() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[], true).await;
        let setup = prepare(&table, &Some(key_relation()), &fixture_columns(), 1).unwrap();

        assert_eq!(setup.regime, Regime::Owned);
        assert!(setup.partition_columns.is_empty());
        assert_eq!(setup.key_encoder.column_names(), ["id"]);
    }

    /// A table that already holds data cannot assume a key is new to it.
    #[tokio::test]
    async fn populated_table_uses_the_default_regime() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1, 2], true).await;
        let setup = prepare(&table, &Some(key_relation()), &fixture_columns(), 1).unwrap();

        assert_eq!(setup.regime, Regime::Default);
        assert!(setup.regime.insert_needs_lookup());
    }

    /// Without deletion vectors there is no way to supersede a row, so the error must name
    /// the property to set.
    #[tokio::test]
    async fn rejects_a_table_without_deletion_vectors() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[1], false).await;
        let err = prepare(&table, &Some(key_relation()), &fixture_columns(), 1)
            .unwrap_err()
            .to_string();

        assert!(err.contains("delta.enableDeletionVectors"), "{err}");
        assert!(err.contains("ALTER TABLE"), "{err}");
    }

    /// Merge mode has no row to supersede without a unique key.
    #[tokio::test]
    async fn rejects_a_view_without_a_unique_key() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[], true).await;
        let err = prepare(&table, &None, &fixture_columns(), 1)
            .unwrap_err()
            .to_string();

        assert!(err.contains("unique key"), "{err}");
        assert!(err.contains("index"), "{err}");
    }

    /// Splitting the batch across threads would repeat the lookup per thread.
    #[tokio::test]
    async fn rejects_multiple_threads() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[], true).await;
        let err = prepare(&table, &Some(key_relation()), &fixture_columns(), 4)
            .unwrap_err()
            .to_string();

        assert!(err.contains("threads"), "{err}");
    }

    /// A key column that is also a partition column is supported; the probe reconstructs its
    /// value from the log.
    #[tokio::test]
    async fn accepts_a_key_column_that_is_a_partition_column() {
        use crate::integrated::delta_table::merge::test::{
            partitioned_fixture_table, partitioned_key_relation,
        };

        let dir = TempDir::new().unwrap();
        let table = partitioned_fixture_table(&dir, &[(1, "a")]).await;
        let setup = prepare(
            &table,
            &Some(partitioned_key_relation()),
            &fixture_columns(),
            1,
        )
        .unwrap();

        assert_eq!(setup.partition_columns, ["payload"]);
        assert_eq!(setup.partition_key_columns, ["payload"]);
        assert!(setup.prune_on_stats);
    }

    /// A key made entirely of partition columns leaves nothing in the data file to read the
    /// key from, and would mean one partition directory per row.
    #[tokio::test]
    async fn rejects_a_key_that_is_entirely_partition_columns() {
        use crate::integrated::delta_table::merge::test::partitioned_fixture_table;
        use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier, SqlType};

        let dir = TempDir::new().unwrap();
        let table = partitioned_fixture_table(&dir, &[(1, "a")]).await;
        let key = Relation {
            name: SqlIdentifier::new("k", false),
            fields: vec![Field::new(
                "payload".into(),
                ColumnType {
                    typ: SqlType::Varchar,
                    nullable: false,
                    precision: None,
                    scale: None,
                    component: None,
                    fields: None,
                    key: None,
                    value: None,
                },
            )],
            materialized: false,
            properties: Default::default(),
            primary_key: None,
        };

        let err = prepare(&table, &Some(key), &fixture_columns(), 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("every column is a partition column"), "{err}");
    }

    /// A column only the view has cannot be written; a column only the table has would fill
    /// with nulls on every append.
    #[tokio::test]
    async fn rejects_a_schema_that_does_not_match_the_view() {
        let dir = TempDir::new().unwrap();
        let table = fixture_table(&dir, &[], true).await;

        let mut extra = fixture_columns();
        extra.push(StructField::new(
            "extra",
            DeltaDataType::Primitive(PrimitiveType::Long),
            true,
        ));
        let err = prepare(&table, &Some(key_relation()), &extra, 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("'extra' is missing from the table"), "{err}");

        let mut retyped = fixture_columns();
        retyped[0] = StructField::new("id", DeltaDataType::Primitive(PrimitiveType::Integer), true);
        let err = prepare(&table, &Some(key_relation()), &retyped, 1)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column 'id'"), "{err}");

        let fewer = fixture_columns()[..1].to_vec();
        let err = prepare(&table, &Some(key_relation()), &fewer, 1)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("'payload' exists in the table but not in the view"),
            "{err}"
        );
    }
}

#[cfg(test)]
mod stats_config_test {
    use super::*;
    use deltalake::kernel::{ArrayType, DataType as DeltaDataType, PrimitiveType, StructType};

    fn primitive(name: &str) -> StructField {
        StructField::nullable(name, DeltaDataType::Primitive(PrimitiveType::Integer))
    }

    fn nested(name: &str, leaves: &[&str]) -> StructField {
        StructField::nullable(
            name,
            DeltaDataType::Struct(Box::new(
                StructType::try_new(leaves.iter().map(|leaf| primitive(leaf))).unwrap(),
            )),
        )
    }

    fn config(n: u64) -> StatsConfig {
        StatsConfig {
            num_indexed_cols: DataSkippingNumIndexedCols::NumColumns(n),
            stats_columns: None,
        }
    }

    /// The count is over parquet leaves, so a nested column takes as many positions as it has
    /// leaves. Counting top-level columns would put `key` at position 1 and call it indexed.
    #[test]
    fn a_nested_column_pushes_later_columns_down_the_count() {
        let schema =
            StructType::try_new(vec![nested("s", &["a", "b", "c"]), primitive("key")]).unwrap();
        let leaves = data_file_leaves(&schema, &[]);

        assert_eq!(leaves, ["s.a", "s.b", "s.c", "key"]);
        assert!(!covered_by_statistics("key", &leaves, &config(3)));
        assert!(covered_by_statistics("key", &leaves, &config(4)));
    }

    /// Partition columns are not written to the data files, so they occupy no leaf position.
    #[test]
    fn partition_columns_take_up_no_position() {
        let schema = StructType::try_new(vec![primitive("day"), primitive("key")]).unwrap();
        let leaves = data_file_leaves(&schema, &["day".to_string()]);

        assert_eq!(leaves, ["key"]);
        assert!(covered_by_statistics("key", &leaves, &config(1)));
    }

    /// A list contributes its element's leaves, a map its key's and its value's.
    #[test]
    fn collections_contribute_their_element_leaves() {
        let schema = StructType::try_new(vec![
            StructField::nullable(
                "tags",
                DeltaDataType::Array(Box::new(ArrayType::new(
                    DeltaDataType::Primitive(PrimitiveType::String),
                    true,
                ))),
            ),
            primitive("key"),
        ])
        .unwrap();
        let leaves = data_file_leaves(&schema, &[]);

        assert_eq!(leaves, ["tags", "key"]);
    }

    /// An explicit column list takes precedence over the count, and is matched on the leaf's
    /// own name -- which is what the writer compares against.
    #[test]
    fn an_explicit_column_list_overrides_the_count() {
        let schema =
            StructType::try_new(vec![nested("s", &["a", "b", "c"]), primitive("key")]).unwrap();
        let leaves = data_file_leaves(&schema, &[]);
        let stats = StatsConfig {
            num_indexed_cols: DataSkippingNumIndexedCols::NumColumns(1),
            stats_columns: Some(vec!["key".to_string()]),
        };

        assert!(covered_by_statistics("key", &leaves, &stats));
        assert!(!covered_by_statistics("s.a", &leaves, &stats));
    }

    #[test]
    fn all_columns_covers_everything() {
        let schema = StructType::try_new(vec![nested("s", &["a"]), primitive("key")]).unwrap();
        let leaves = data_file_leaves(&schema, &[]);
        let stats = StatsConfig {
            num_indexed_cols: DataSkippingNumIndexedCols::AllColumns,
            stats_columns: None,
        };

        assert!(covered_by_statistics("key", &leaves, &stats));
    }
}
