# Delta Lake input connector

:::note
This page describes configuration options specific to the Delta Lake connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

[Delta Lake](https://delta.io/) is a popular open table format based on Parquet files.
It is typically used with the [Apache Spark](https://spark.apache.org/) runtime.
Data in a Delta Lake is organized in tables, stored in
a file system or an object stores like [AWS S3](https://aws.amazon.com/s3/),
[Google GCS](https://cloud.google.com/storage), or
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

The Delta Lake input connector does not yet support [fault
tolerance](/pipelines/fault-tolerance).

## Delta Lake input connector configuration

| Property                    | Type   | Default    | Description   |
|-----------------------------|--------|------------|---------------|
| `uri`*                      | string |            | Table URI, e.g., "s3://feldera-fraud-detection-data/demographics_train"|
| `mode`*                     | enum   |            | Table read mode. Three options are available: <ul> <li>`snapshot` - read a snapshot of the table and stop.</li> <li>`follow` - follow the changelog of the table, only ingesting changes (new and deleted rows)</li> <li>`snapshot_and_follow` - Read a snapshot of the table before switching to the `follow` mode.  This mode implements the backfill pattern where we load historical data for the table before ingesting the stream of real-time updates.</li> </ul>|
| `timestamp_column`          | string |            | Table column that serves as an event timestamp. When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`, table rows are ingested in the timestamp order, respecting the [`LATENESS`](/sql/streaming#lateness-expressions) property of the column: each ingested row has a timestamp no more than `LATENESS` time units earlier than the most recent timestamp of any previously ingested row.  See details [below](#ingesting-time-series-data-from-a-delta-lake). |
| `filter`                    | string |            | <p>Optional row filter.</p> <p>When specified, only rows that satisfy the filter condition are read from the delta table. The condition must be a valid SQL Boolean expression that can be used in the `where` clause of the `select * from my_table where ...` query.</p> |
| `snapshot_filter`           | string |            | <p>Optional snapshot filter.</p><p>This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`. When specified, only rows that satisfy the filter condition are included in the snapshot.</p> <p>The condition must be a valid SQL Boolean expression that can be used in  the `where` clause of the `select * from snapshot where ...` query.</p><p>Unlike the `filter` option, which applies to all records retrieved from the table, this filter only applies to rows in the initial snapshot of the table. For instance, it can be used to specify the range of event times to include in the snapshot, e.g.: `ts BETWEEN TIMESTAMP '2005-01-01 00:00:00' AND TIMESTAMP '2010-12-31 23:59:59'`. This option can be used together with the `filter` option. During the initial snapshot, only rows that satisfy both `filter` and `snapshot_filter` are retrieved from the Delta table. When subsequently following changes in the the transaction log (`mode = snapshot_and_follow`), all rows that meet the `filter` condition are ingested, regardless of `snapshot_filter`. </p> |
| `version`                   | integer|            | <p>Optional table version.  When this option is set, the connector finds and opens the specified version of the table. In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of the table.  In `follow` and `snapshot_and_follow` modes, it follows transaction log records **after** this version.</p><p>Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p>
| `datetime`                  | string |            | <p>Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g., "2024-12-09T16:09:53+00:00". When this option is set, the connector finds and opens the version of the table as of the specified point in time (based on the server time recorded in the transaction log, not the event time encoded in the data).  In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of the table.  In `follow` and `snapshot_and_follow` modes, it follows transaction log records **after** this version.</p><p> Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p>|
| `num_parsers`               | string |            | The number of parallel parsing tasks the connector uses to process data read from the table. Increasing this value can enhance performance by allowing more concurrent processing. Recommended range: 1–10. The default is 4.|
| `skip_unused_columns`       | bool   | false      | <p>Don't read unused columns from the Delta table.  When set to `true`, this option instructs the connector to avoid reading columns from the Delta table that are not used in any view definitions. To be skipped, the columns must be either nullable or have default values. This can improve ingestion performance, especially for wide tables.</p><p>Note: The simplest way to exclude unused columns is to omit them from the Feldera SQL table declaration. The connector never reads columns that aren't declared in the SQL schema. Additionally, the SQL compiler emits warnings for declared but unused columns—use these as a guide to optimize your schema.</p>|

[*]: Required fields

### Storage parameters

Along with the parameters listed above, there are additional configuration options for
specific storage backends.  Refer to backend-specific documentation for details:

* [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
* [Azure Blob Storage options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
* [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)

## Data type mapping

The following table lists supported Delta Lake data types and corresponding Feldera types.

| Delta Lake type             | Feldera SQL type | Comment       |
|-----------------------------|------------------|---------------|
| `BIGINT`                    | `BIGINT`         |               |
| `BINARY`                    | `VARBINARY`      |               |
| `BOOLEAN`                   | `BOOLEAN`        |               |
| `DATE`                      | `DATE`           |               |
| `DOUBLE`                    | `DOUBLE`         |               |
| `FLOAT`                     | `REAL`           |               |
| `INT`                       | `INT`            |               |
| `SMALLINT`                  | `SMALLINT`       |               |
| `STRING`                    | `STRING`         |               |
| `DECIMAL(P,S)`              | `DECIMAL(P,S)`   | The largest supported precision `P` is 28.|
| `TIMESTAMP`, `TIMESTAMP_NTZ`| `TIMESTAMP`      | Timestamp values are rounded to the nearest millisecond.  Feldera currently does not support timestamps with time zones.  When using the `TIMESTAMP` DeltaLake type, time zone information gets discarded. |
| `TINYINT`                   | `TINYINT`        |               |
| `MAP<K,V>`                  | `MAP<K,V>`       |               |
| `ARRAY<T>`                  | `T ARRAY`        |               |
| `STRUCT`                    | `ROW` or [user-defined type](/sql/types#user-defined-types)| structs can be encoded as either anonymous `ROW` types or as named user-defined structs |
| `VARIANT`                   | `VARIANT`        |               |

## Ingesting time series data from a Delta Lake

Feldera is optimized to efficiently process time series data by taking advantage
of the fact that such data often arrives ordered by timestamp, i.e., every event
has the same or larger timestamp than the previous event. In some cases, events
can get reordered and delayed, but this delay is bounded, e.g., it may not
exceed 1 hour. We refer to this bound as **lateness** and specify it by
attaching the [`LATENESS`](/sql/streaming#lateness-expressions) attribute to the
timestamp column of the table declaraion.  See our [Time Series Analysis
Guide](/tutorials/time-series) for more details.

When reading from a Delta Table that contains time series data, the user must
ensure that the initial snapshot of the table is ingested respecting the
`LATENESS` annotation, e.g., if the table contains one year worth of data, and
its lateness is equal to 1 month, then the connector must ingest all data for the
first month before moving to the second month, and so on.  If this requirement
is violated, the pipeline will drop records that arrive more that `LATENESS` out
of order.

This can be achieved using the `timestamp_column` property, which specifies the table column
that serves as an event timestamp. When this property is set, and `mode` is
one of `snapshot` or `snapshot_and_follow`, table rows are ingested in the timestamp
order, respecting the `LATENESS` annotation on the column: each ingested row has a
timestamp no more than `LATENESS` time units earlier than the most recent timestamp
of any previously ingested row.  The ingestion is performed by partitioning the table
into timestamp ranges of width `LATENESS` and ingesting ranges one by one in increasing timestamp order.

Requirements:
* The timestamp column must be of a supported type: integer, `DATE`, or `TIMESTAMP`.
* The timestamp column must be declared with non-zero `LATENESS`.
* `LATENESS` must be a valid constant expression in the [DataFusion
  SQL dialect](https://datafusion.apache.org/). The reason for this is that Feldera
  uses the Apache Datafusion engine to query Delta Lake.  In practice, most
  valid Feldera SQL expressions are accepted by DataFusion.
* For efficient ingest, the Delta table must be optimized for timestamp-based queries
  using partitioning, Z-ordering, or liquid clustering.

Note that the `timestamp_column` property only controls the initial table snapshot.
When `mode` is set to `follow` or `snapshot_and_follow` and the connector is following
the transaction log of the table, it ingests changes in the order they appear in the
log.  It is the responsibility of the application that writes to the table
to ensure that changes it applies to the table respect the `LATENESS` annotations.

### Example

The following table contains a timestamp column of type `TIMESTAMP` with `LATENESS` equal
to `INTERVAL 30 days`. Assuming that the oldest timestamp in the table is `2024-01-01T00:00:00`,
the connector will fetch all records with timestamps from `2024-01-01`, then all records for
`2024-01-02`, `2024-01-03`, etc., until all rcords in the table have been ingested.

```sql
CREATE TABLE transaction(
    trans_date_trans_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 day,
    cc_num BIGINT,
    merchant STRING,
    category STRING,
    amt DOUBLE,
    trans_num STRING,
    unix_time BIGINT,
    merch_lat DOUBLE,
    merch_long DOUBLE,
    is_fraud BIGINT
) WITH (
  'connectors' = '[{
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://feldera-fraud-detection-data/transaction_train",
        "mode": "snapshot",
        "aws_skip_signature": "true",
        "timestamp_column": "trans_date_trans_time"
      }
    }
  }
]');
```

## Additional examples

Create a Delta Lake input connector to read a snapshot of a table from a public S3 bucket, using
`unix_time` as the timestamp column.  The column stores event time in seconds since UNIX epoch
and has lateness equal to 30 days (3600 seconds/hour * 24 hours/day * 30 days).
Note the `aws_skip_signature` flag, required to read from the bucket without authentcation,


```sql
CREATE TABLE transaction(
    trans_date_trans_time TIMESTAMP,
    cc_num BIGINT,
    merchant STRING,
    category STRING,
    amt DOUBLE,
    trans_num STRING,
    unix_time BIGINT LATENESS 3600 * 24 * 30,
    merch_lat DOUBLE,
    merch_long DOUBLE,
    is_fraud BIGINT
) WITH (
  'connectors' = '[{
    "transport": {
      "name": "delta_table_input",
      "config": {
        "uri": "s3://feldera-fraud-detection-data/transaction_train",
        "mode": "snapshot",
        "aws_skip_signature": "true",
        "timestamp_column": "unix_time"
      }
    }
  }
]');
```

Read a full snapshot of version 10 of the table before ingesting the stream of
changes for versions 11 onward.  The initial snapshot will be sorted by the
`unix_time` column.  Here and below we only show the contents of the
`transport.config` field of the connector.

```json
{
  "uri": "s3://feldera-fraud-detection-data/transaction_infer",
  "mode": "snapshot_and_follow",
  "version": 10,
  "timestamp_column": "unix_time",
  "aws_skip_signature": "true"
}
```

Read a full snapshot of a Delta table using the specified AWS access key. Note that
the `aws_region` parameter is required in this case, because the Delta Lake Rust
library we use does not currently auto-detect the AWS region.

```json
{
  "uri": "s3://feldera-fraud-detection-demo/transaction_train",
  "mode": "snapshot",
  "aws_access_key_id": <AWS_ACCESS_KEY_ID>,
  "aws_secret_access_key": <AWS_SECRET_ACCESS_KEY>,
  "aws_region": "us-east-1"
}
```
