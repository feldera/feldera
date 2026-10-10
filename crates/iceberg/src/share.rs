//! One host's share of an Iceberg table snapshot, for a distributed connector.
//!
//! The hosts of a distributed connector divide the snapshot's file scan tasks
//! among themselves.  [IcebergShareTableProvider] reads the same snapshot as
//! [IcebergStaticTableProvider](iceberg_datafusion::IcebergStaticTableProvider),
//! but plans the scan's tasks, sorts them, and reads only the ones that this
//! host's [InputShard] owns.  A task carries the delete files that apply to its
//! data file, so dividing tasks divides the snapshot's rows exactly.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use feldera_types::coordination::InputShard;
use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::Runtime;
use iceberg_datafusion::physical_plan::convert_filters_to_predicate;
use log::info;

/// Returns the items that `shard` owns, out of `items`, which every host of
/// the connector lists the same way but maybe in a different order.
///
/// Sorting by `key` puts the items in the same order on every host, and the
/// `i`th item goes to the host that owns unit `i`, so each item goes to
/// exactly one host.
pub(crate) fn select_share<T, K: Ord>(
    mut items: Vec<T>,
    key: impl Fn(&T) -> K,
    shard: &InputShard,
) -> Vec<T> {
    items.sort_by_key(|item| key(item));
    items
        .into_iter()
        .enumerate()
        .filter(|(index, _item)| shard.contains(*index as u64))
        .map(|(_index, item)| item)
        .collect()
}

/// A table of one host's share of an Iceberg table snapshot.
#[derive(Debug)]
pub(crate) struct IcebergShareTableProvider {
    endpoint_name: String,
    table: Table,
    /// `None` means that the table had no snapshot when host 0 chose one, so
    /// the share is empty, even if this host's copy of the table now has a
    /// snapshot.
    snapshot_id: Option<i64>,
    schema: ArrowSchemaRef,
    shard: InputShard,
}

impl IcebergShareTableProvider {
    /// Returns `shard`'s share of snapshot `snapshot_id` of `table`, whose
    /// schema is `schema` (the schema of the whole snapshot's provider).
    pub(crate) fn new(
        endpoint_name: &str,
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        shard: InputShard,
    ) -> Self {
        Self {
            endpoint_name: endpoint_name.to_string(),
            table,
            snapshot_id,
            schema,
            shard,
        }
    }
}

#[async_trait]
impl TableProvider for IcebergShareTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let output_schema = match projection {
            None => self.schema.clone(),
            Some(projection) => Arc::new(self.schema.project(projection)?),
        };
        let column_names = projection.map(|projection| {
            projection
                .iter()
                .map(|index| self.schema.field(*index).name().clone())
                .collect()
        });
        Ok(Arc::new(IcebergShareScan {
            endpoint_name: self.endpoint_name.clone(),
            table: self.table.clone(),
            snapshot_id: self.snapshot_id,
            shard: self.shard,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(output_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            column_names,
            predicate: convert_filters_to_predicate(filters),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // The scan prunes files and row groups with the filters, but it may
        // still return rows that they reject, so DataFusion applies them too.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Reads one host's share of an Iceberg table snapshot.
#[derive(Debug)]
struct IcebergShareScan {
    endpoint_name: String,
    table: Table,
    snapshot_id: Option<i64>,
    shard: InputShard,
    properties: Arc<PlanProperties>,
    /// `None` reads every column.
    column_names: Option<Vec<String>>,
    predicate: Option<Predicate>,
}

impl IcebergShareScan {
    /// Plans the scan, and reads the tasks that this host owns.
    ///
    /// Without a snapshot, there is nothing to read.  Scanning the table's
    /// current snapshot instead could read a snapshot that another host's
    /// copy of the table does not have yet, so that the hosts divide
    /// different snapshots.
    async fn read(
        endpoint_name: String,
        table: Table,
        snapshot_id: Option<i64>,
        column_names: Option<Vec<String>>,
        predicate: Option<Predicate>,
        shard: InputShard,
    ) -> DFResult<BoxStream<'static, DFResult<RecordBatch>>> {
        let Some(snapshot_id) = snapshot_id else {
            info!(
                "iceberg {endpoint_name}: host {} reads nothing, because the table had no snapshot when host 0 opened it",
                shard.host()
            );
            return Ok(stream::empty().boxed());
        };
        let builder = table.scan().snapshot_id(snapshot_id);
        let mut builder = match column_names {
            Some(column_names) => builder.select(column_names),
            None => builder.select_all(),
        };
        if let Some(predicate) = predicate {
            builder = builder.with_filter(predicate);
        }
        let scan = builder.build().map_err(external)?;
        let tasks: Vec<FileScanTask> = scan
            .plan_files()
            .await
            .map_err(external)?
            .try_collect()
            .await
            .map_err(external)?;
        let n_tasks = tasks.len();
        let tasks = select_share(
            tasks,
            |task| (task.data_file_path.clone(), task.start),
            &shard,
        );
        info!(
            "iceberg {endpoint_name}: host {} reads {} of the snapshot's {n_tasks} files",
            shard.host(),
            tasks.len(),
        );

        let runtime = Runtime::try_current().map_err(external)?;
        let reader = ArrowReaderBuilder::new(table.file_io().clone(), runtime)
            .with_row_group_filtering_enabled(true)
            .build();
        let stream = reader
            .read(stream::iter(tasks.into_iter().map(Ok)).boxed())
            .map_err(external)?
            .stream()
            .map_err(external)
            .boxed();
        Ok(stream)
    }
}

fn external(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

impl DisplayAs for IcebergShareScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "IcebergShareScan host:[{}] projection:[{}] predicate:[{}]",
            self.shard.host(),
            self.column_names
                .as_ref()
                .map_or(String::new(), |names| names.join(",")),
            self.predicate
                .as_ref()
                .map_or(String::new(), |predicate| predicate.to_string()),
        )
    }
}

impl ExecutionPlan for IcebergShareScan {
    fn name(&self) -> &str {
        "IcebergShareScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let read = Self::read(
            self.endpoint_name.clone(),
            self.table.clone(),
            self.snapshot_id,
            self.column_names.clone(),
            self.predicate.clone(),
            self.shard,
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.properties.eq_properties.schema().clone(),
            stream::once(read).try_flatten(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{select_share, IcebergShareScan};
    use dbsp::circuit::tokio::TOKIO;
    use feldera_types::coordination::{InputDistribution, InputShard};
    use futures_util::TryStreamExt;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference,
        SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type, UnboundPartitionSpec,
        MAIN_BRANCH,
    };
    use iceberg::table::Table;
    use iceberg::{Runtime, TableIdent};
    use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

    /// A table whose current snapshot cannot be read, because its manifest
    /// list does not exist.
    fn table_with_unreadable_snapshot() -> Table {
        let schema = Schema::builder()
            .with_fields(vec![NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )
            .into()])
            .build()
            .unwrap();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(now_ms)
            .with_manifest_list("memory:///t/metadata/missing.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(0)
            .build();
        let metadata = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "memory:///t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .add_snapshot(snapshot)
        .unwrap()
        .set_ref(
            MAIN_BRANCH,
            SnapshotReference {
                snapshot_id: 1,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let _guard = TOKIO.enter();
        Table::builder()
            .runtime(Runtime::try_current().unwrap())
            .metadata(metadata)
            .identifier(TableIdent::from_strs(["ns", "t"]).unwrap())
            .file_io(FileIOBuilder::new(Arc::new(OpenDalResolvingStorageFactory::new())).build())
            .build()
            .unwrap()
    }

    /// If host 0 chose no snapshot, a host reads nothing, even if its own
    /// copy of the table has a snapshot by now.
    #[test]
    fn no_chosen_snapshot_reads_nothing() {
        let table = table_with_unreadable_snapshot();
        let read = |snapshot_id| {
            TOKIO.block_on(async {
                IcebergShareScan::read(
                    "t".to_string(),
                    table.clone(),
                    snapshot_id,
                    None,
                    None,
                    InputShard::ALL,
                )
                .await?
                .try_collect::<Vec<_>>()
                .await
            })
        };
        assert_eq!(read(None).unwrap().len(), 0);
        // Reading the snapshot itself fails, so the share above did not read it.
        assert!(read(Some(1)).is_err());
    }

    fn shard(host: usize) -> InputShard {
        InputShard::new(host, 3, InputDistribution { home: 1 }).unwrap()
    }

    /// The hosts divide the items exactly, whatever order each host lists
    /// them in.
    #[test]
    fn hosts_divide_items_exactly() {
        let items: Vec<(String, u64)> = (0..10)
            .flat_map(|i| [(format!("file-{i}"), 0), (format!("file-{i}"), 100)])
            .collect();
        let mut reversed = items.clone();
        reversed.reverse();
        let key = |item: &(String, u64)| item.clone();

        let mut all = Vec::new();
        for host in 0..3 {
            let share = select_share(items.clone(), key, &shard(host));
            // Another listing order gives the same share.
            assert_eq!(share, select_share(reversed.clone(), key, &shard(host)));
            all.extend(share);
        }
        all.sort();
        let mut expected = items;
        expected.sort();
        assert_eq!(all, expected);
    }
}
