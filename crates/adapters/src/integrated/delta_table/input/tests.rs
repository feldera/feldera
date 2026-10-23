//! The Delta Lake input connector's snapshot reader, pushing into an input stream of the
//! engine.
//!
//! This module also holds the fixtures that the input module's own distributed snapshot tests
//! share with it: a partitioned table and the reader over it.

use crate::test::engine_test;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use chrono::Utc;
use datafusion::datasource::MemTable;
use deltalake::DeltaTable;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, MockInputConsumer};
use feldera_adapterlib::transport::InputQueueEntry;
use feldera_types::coordination::{InputDistribution, InputShard};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::serde_with_context::SqlSerdeConfig;
use feldera_types::transport::delta_table::DeltaTableReaderConfig;
use serde_json::json;
use tokio::sync::watch::channel;

use super::{DeltaResumeInfo, DeltaTableInputEndpointInner, Polarity, QueueEntry};
use crate::PipelineState;
use crate::integrated::DistributedInput;
use crate::test::{relations, run_async};

/// The table whose input stream the case pushes into, which receives no row.
pub fn stream_relation() -> Relation {
    relations::basic("delta_input")
}

/// Writes a table partitioned by `part`, whose rows have IDs `0..n_rows`
/// and `part` equal to `p{id % 3}`, in `n_writes` appends, so that the
/// table has several files.  Returns the table and its URI.
pub(super) async fn write_table(
    dir: &std::path::Path,
    n_rows: i64,
    n_writes: i64,
) -> (DeltaTable, String) {
    let uri = format!("file://{}", dir.display());
    let mut table = CreateBuilder::new()
        .with_location(&uri)
        .with_save_mode(SaveMode::Ignore)
        .with_columns([
            StructField::new("id", DataType::Primitive(PrimitiveType::Long), false),
            StructField::new("part", DataType::Primitive(PrimitiveType::String), true),
        ])
        .with_partition_columns(["part"])
        .await
        .unwrap();
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("part", ArrowDataType::Utf8, true),
    ]));
    for write in 0..n_writes {
        let ids = (0..n_rows)
            .filter(|id| id % n_writes == write)
            .collect::<Vec<_>>();
        let parts = ids
            .iter()
            .map(|id| format!("p{}", id % 3))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(parts)),
            ],
        )
        .unwrap();
        table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }
    (table, uri)
}

pub(super) fn endpoint_with_consumer(
    config: serde_json::Value,
    shard: Option<InputShard>,
    choice: Option<serde_json::Value>,
    consumer: MockInputConsumer,
) -> DeltaTableInputEndpointInner {
    resumed_endpoint(config, shard, choice, consumer, None)
}

pub(super) fn resumed_endpoint(
    config: serde_json::Value,
    shard: Option<InputShard>,
    choice: Option<serde_json::Value>,
    consumer: MockInputConsumer,
    resume_info: Option<DeltaResumeInfo>,
) -> DeltaTableInputEndpointInner {
    let config: DeltaTableReaderConfig = serde_json::from_value(config).unwrap();
    let relation = Relation::new(
        SqlIdentifier::new("t", false),
        vec![
            Field::new(SqlIdentifier::new("id", false), ColumnType::bigint(false)),
            Field::new(SqlIdentifier::new("part", false), ColumnType::varchar(true)),
        ],
        false,
        BTreeMap::new(),
    );
    DeltaTableInputEndpointInner::new(
        "t.delta",
        config,
        SessionContext::new(),
        Box::new(consumer),
        relation,
        resume_info,
        shard.map(|shard| DistributedInput { shard, choice }),
    )
}

pub(super) fn shard(host: usize) -> InputShard {
    InputShard::new(host, 3, InputDistribution { home: 1 }).unwrap()
}

/// A read error in a distributed snapshot keeps the snapshot's
/// transaction open, so that the retry reads into the same transaction.
/// Without distribution, the error commits the transaction.
#[engine_test]
pub fn distributed_snapshot_error_keeps_transaction_open(engine: &dyn ConnectorTestEngine) {
    run_async(async {
        let dir = tempfile::TempDir::new().unwrap();
        let (table, uri) = write_table(dir.path(), 3, 1).await;
        let table = Arc::new(table);

        // Dividing by zero fails while the query runs, after planning.
        let ctx = SessionContext::new();
        let zero = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                ArrowDataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![0]))],
        )
        .unwrap();
        ctx.register_table(
            "zero",
            Arc::new(MemTable::try_new(zero.schema(), vec![vec![zero]]).unwrap()),
        )
        .unwrap();

        for (shard, stays_open) in [(Some(shard(2)), true), (None, false)] {
            let consumer = MockInputConsumer::new();
            let endpoint = endpoint_with_consumer(
                json!({"uri": uri, "mode": "snapshot", "transaction_mode": "snapshot"}),
                shard,
                None,
                consumer.clone(),
            );
            *endpoint.schema_table.lock().unwrap() = Some(table.clone());

            // The snapshot's first records start its transaction.
            let transaction = Some(Some("snapshot-0".to_string()));
            endpoint.queue.push_entry(
                InputQueueEntry::new_with_aux(Utc::now(), QueueEntry::ResumeInfo(None))
                    .with_start_transaction(transaction.clone()),
                Vec::new(),
            );
            endpoint.queue.flush_with_aux();
            assert!(consumer.state().transaction_in_progress);

            let dataframe = ctx.sql("select 10 / id as id from zero").await.unwrap();
            // The query fails before it yields a row, so the stream only has to exist.
            let collection = engine
                .input_collection(&stream_relation(), &InputOptions::default())
                .unwrap();
            let mut input_stream = collection
                .input_handle()
                .handle
                .configure_arrow_deserializer(SqlSerdeConfig::default())
                .unwrap();
            let (_sender, mut receiver) = channel(PipelineState::Running);
            assert!(
                endpoint
                    .execute_df_inner(
                        dataframe,
                        Polarity::Fixed(true),
                        input_stream.as_mut(),
                        &mut receiver,
                        &transaction,
                        None,
                    )
                    .await
                    .is_err()
            );
            endpoint.queue.flush_with_aux();
            assert_eq!(
                consumer.state().transaction_in_progress,
                stays_open,
                "{shard:?}"
            );
        }
    })
}
