# Apache Iceberg input connector

:::note
This page describes configuration options specific to the Apache Iceberg connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

:::warning

Iceberg support is still experimental, and it may be substantially modified in the future.

:::

The Iceberg input connector enables data ingestion from an Apache Iceberg table into
a Feldera pipeline. Currently, the connector supports batch reads, allowing users to
load a static snapshot of the table. However, it does not yet support ingesting
incremental changes. Incremental ingestion capabilities are planned for future releases.

The connector is compatible with REST, AWS Glue, and Amazon S3 Tables catalogs and also
supports direct table reads without a catalog, provided the location of the metadata file.
Supported storage systems include S3, GCS, and local file systems.

The Iceberg input connector supports [fault tolerance](/pipelines/fault-tolerance) at the
[at-least-once](/pipelines/fault-tolerance#fault-tolerance-guarantees) level: see
[Fault tolerance](#fault-tolerance) below.


## Configuration

| Property                    | Type   | Description   |
|-----------------------------|--------|---------------|
| `mode`*                     | enum   | <p>Table read mode. Supported values:</p><ul><li>`snapshot` - read a snapshot of the table and stop.</li><li>`follow` - skip the initial snapshot and only ingest subsequent changes to the table (new and deleted rows) by following its transaction log.</li><li>`snapshot_and_follow` - read a snapshot of the table, then switch to `follow` mode.</li></ul><p>`follow` and `snapshot_and_follow` require an Iceberg catalog (set `catalog_type`); they cannot be used with `metadata_location`, which points at a fixed snapshot. See [Follow mode](#follow-mode) below.</p>|
| `transaction_mode`          | enum   | Determines how the connector breaks up its input into transactions. Supported values are `none` (default), `snapshot`, `catchup`, and `always`. See [below](#transactions) for details. |
| `timestamp_column`          | string | Table column that serves as an event timestamp. When this option is specified, table rows are ingested in the timestamp order, respecting the [`LATENESS`](/sql/streaming#lateness-expressions) property of the column: each ingested row has a timestamp no more than `LATENESS` time units earlier than the most recent timestamp of any previously ingested row. See details [below](#ingesting-time-series-data-from-iceberg). |
| `snapshot_filter`           | string | <p>Optional row filter.  When specified, only rows that satisfy the filter condition are included in the snapshot.  The condition must be a valid SQL Boolean expression that can be used in the `where` clause of the `select * from snapshot where ..` query.</p><p> This option can be used to specify the range of event times to include in the snapshot, e.g.: `ts BETWEEN TIMESTAMP '2005-01-01 00:00:00' AND TIMESTAMP '2010-12-31 23:59:59'`.</p>
| `snapshot_id`               | integer| <p>Optional table snapshot id.  When this option is set, the connector reads the specified snapshot of the table.</p><p>Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest snapshot of the table is used.</p>
| `datetime`                  | string | <p>Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g., "2024-12-09T16:09:53+00:00". When this option is set, the connector reads the version of the table as of the specified point in time (based on the server time recorded in the transaction log, not the event time encoded in the data). </p><p> Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p>|
| `end_snapshot_id`           | integer| <p>Optional final snapshot id. Valid only in `follow` and `snapshot_and_follow` modes. When set, the connector stops after fully ingesting the snapshot with this id, then signals end-of-input.</p><p>Iceberg snapshot ids are not ordered, so this bound is an exact match: the id must name a snapshot committed after the starting snapshot and already present in the table's current history. The connector rejects any other value at startup (including a not-yet-committed id) rather than follow forever.</p>|
| `metadata_location`         | string | Location of the table metadata JSON file. This property is used to access an Iceberg table directly, without a catalog. It is mutually exclusive with the `catalog_type` property.|
| `table_name`                | string | Specifies the Iceberg table name within the catalog in the `namespace.table` format. This option is applicable when an Iceberg catalog is configured using the `catalog_type` property.|
| `catalog_type`              | enum   | Type of the Iceberg catalog used to access the table. Supported options include `rest`, `glue`, and `s3tables`. This property is mutually exclusive with `metadata_location`.|
| `num_parsers`               | integer| Number of parallel parsing tasks used to process data read from the table. Increasing this value can improve throughput by parsing record batches concurrently. Recommended range: 1-10. Default: `4`.|
| `max_retries`               | integer| <p>Maximum number of retries for reading the table snapshot. When reading the snapshot fails partway through, for example because an object store read times out or is throttled, the connector retries the entire read with exponential backoff. This is in addition to the lower-level retries performed by the object store client.</p><p>Defaults to unlimited retries. Set to `0` to disable retries.</p>|

[*]: Required fields

### Rest catalog configuration

The following properties are used when `catalog_type` is set to `rest` to configure access to an Iceberg REST catalog.

| Property                    | Type                | Description   |
|-----------------------------|---------------------|---------------|
| `rest.uri`*                 | string              | URI identifying the REST catalog server|
| `rest.warehouse`            | string              | The default location for managed tables created by the catalog.|
| `rest.oauth2-server-uri`    | string              | Authentication URL to use for client credentials authentication (default: `uri` + `v1/oauth/tokens`)|
| `rest.credential`           | string              | Credential to use for OAuth2 credential flow when initializing the catalog. A key and secret pair separated by ":" (key is optional).|
| `rest.token`                | string              | Bearer token value to use for `Authorization` header.|
| `rest.scope`                | string              | Desired scope of the requested security token (default: catalog).|
| `rest.prefix`               | string              | Customize table storage paths. When combined with the `warehouse` property, the prefix determines how table data is organized within the storage.|
| `rest.audience`             | string              | Logical name of target resource or service.|
| `rest.resource`             | string              | URI for the target resource or service.|
| `rest.headers`              | [(string, string)]  | Additional HTTP request headers added to each catalog REST API call.|

[*]: These fields are required when the `catalog_type` property is set to `rest`.

### Glue catalog configuration

The following properties are used when `catalog_type` is set to `glue` to configure access to the AWS Glue catalog.

| Property                    | Type   | Description   |
|-----------------------------|--------|---------------|
| `glue.warehouse`*           | string | Location for table metadata. Example: `s3://my-data-warehouse/tables/`|
| `glue.endpoint`             | string | Configure an alternative endpoint of the Glue service for Glue catalog to access. Example: `https://glue.us-east-1.amazonaws.com`|
| `glue.access-key-id`        | string | Access key id used to access the Glue catalog.|
| `glue.secret-access-key`    | string | Secret access key used to access the Glue catalog.|
| `glue.profile-name`         | string | Profile used to access the Glue catalog.|
| `glue.region`               | string | Region of the Glue catalog.|
| `glue.session-token`        | string | Static session token used to access the Glue catalog.|
| `glue.id`                   | string | The 12-digit ID of the Glue catalog.|

[*]: These fields are required when the `catalog_type` property is set to `glue`.

### S3 Tables catalog configuration

The following properties are used when `catalog_type` is set to `s3tables` to configure
access to an [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html)
table bucket.

| Property                        | Type   | Description   |
|---------------------------------|--------|---------------|
| `s3tables.table-bucket-arn`*    | string | ARN of the S3 table bucket that contains the table. Note that this is the ARN of the table _bucket_, not of an individual table, e.g., `arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket`.|
| `s3tables.endpoint`             | string | Custom endpoint URL for the S3 Tables service. Primarily used to target a local or mock S3 Tables implementation for testing.|
| `s3tables.access-key-id`        | string | Access key id used to access the S3 Tables catalog.|
| `s3tables.secret-access-key`    | string | Secret access key used to access the S3 Tables catalog.|
| `s3tables.session-token`        | string | Static session token used to access the S3 Tables catalog. Required when using temporary credentials.|
| `s3tables.profile-name`         | string | Profile used to access the S3 Tables catalog.|
| `s3tables.region`               | string | Region of the S3 Tables catalog.|

[*]: These fields are required when the `catalog_type` property is set to `s3tables`.

### FileIO configuration

Iceberg works with the concept of a FileIO which is a pluggable module for reading, writing, and deleting files.
Feldera currently supports S3, GCS, and file system-based FileIO implementations. The Iceberg connector detects
the correct type of FileIO from the prefix of the Iceberg table location:

* `s3://`, `s3a://` - S3.
* `gs://` - Google Cloud Storage.
* `file://` or no prefix - local file system.

S3 and GCP FileIO implementations require additional configuration options documented below.

#### S3 FileIO configuration

| Property                    | Type   | Description   |
|-----------------------------|--------|---------------|
| `glue.warehouse`*           | string | Location for table metadata. Example: `s3://my-data-warehouse/tables/`|
| `s3.access-key-id`          | string | S3 access key id.|
| `s3.secret-access-key`      | string | S3 secret access key.|
| `s3.endpoint`               | string | Configure an alternative endpoint of the S3 service for the FileIO to access. This could be used to use S3 FileIO with any S3-compatible object storage service that has a different endpoint, or access a private S3 endpoint in a virtual private cloud.|
| `s3.region`                 | string | S3 region.|
| `s3.session-token`          | string | S3 session token. This is required when using temporary credentials.|
| `s3.allow-anonymous`        | string | Set to `"true"` to skip signing requests (e.g., for public buckets).|
| `s3.disable-ec2-metadata`   | string | Set to `"true"` to skip loading the credential from EC2 metadata (typically used in conjunction with `s3.allow-anonymous`).|

#### GCS FileIO configuration

| Property                    | Type   | Description   |
|-----------------------------|--------|---------------|
| `gcs.project-id`            | string | Google Cloud Project ID.|
| `gcs.service.path`          | string | Google Cloud Storage endpoint.|
| `gcs.no-auth`               | string | Set to `"true"` to allow unauthenticated requests.|
| `gcs.credentials-json`      | string | Google Cloud Storage credentials JSON string, base64 encoded.|
| `gcs.oauth2.token`          | string | String representation of the access token used for temporary access.|

## Data type mapping

The following table lists supported Iceberg data types and corresponding Feldera types.

| Iceberg type                | Feldera SQL type | Comment       |
|-----------------------------|------------------|---------------|
| `boolean`                   | `BOOLEAN`        |               |
| `int`                       | `INT`            |               |
| `long`                      | `BIGINT`         |               |
| `float`                     | `REAL`           |               |
| `double`                    | `DOUBLE`         |               |
| `decimal(P,S)`              | `DECIMAL(P, S)`  | The largest supported precision `P` is 28.|
| `date`                      | `DATE`           |               |
| `time`                      | `TIME`           |               |
| `timestamp`                 | `TIMESTAMP`      | Timestamp values are rounded to the nearest millisecond.|
| `timestamp_ns`              | `TIMESTAMP`      | Timestamp values are rounded to the nearest millisecond.|
| `timestamptz`               | `TIMESTAMP WITH TIME ZONE` | Timestamp values are rounded to the nearest millisecond.|
| `timestamptz_ns`            | `TIMESTAMP WITH TIME ZONE` | Timestamp values are rounded to the nearest millisecond.|
| `string`                    | `STRING`         |               |
| `fixed(L)`                  | `BINARY(L)`      |               |
| `binary`                    | `VARBINARY`      |               |
| `uuid`                      | `UUID`           |               |
| `struct`                    | `ROW(...)`       | Read as a whole column; nested fields map by name.|
| `list`                      | `<element> ARRAY`| |
| `map`                       | `MAP<<key>, <value>>` | |

All Iceberg data types are supported, including the nested types (`struct`, `list`,
and `map`), which the connector reads as whole columns.

## Column selection

The connector reads only the columns that appear in the Feldera SQL table
declaration. Other columns of the Iceberg table are never read. In addition,
when the table declaration sets the [`skip_unused_columns` property](/sql/grammar#ignoring-unused-columns), the connector skips declared columns that no view uses, provided they are
nullable or have default values.

## Follow mode

In `follow` and `snapshot_and_follow` modes the connector continuously ingests
changes committed to the table after its starting snapshot. It polls the catalog
for new snapshots and, for each one, ingests added rows as inserts and
removed rows as deletes.

The starting snapshot is chosen the same way as in `snapshot` mode: by
`snapshot_id`, by `datetime`, or, when neither is set, the latest snapshot at the
time the connector starts. In `follow` mode the connector ingests only changes
committed after the starting snapshot; in `snapshot_and_follow` mode it first
reads the starting snapshot in full, then follows.

Requirements and limitations:

* **A catalog is required.** Set `catalog_type`; follow mode cannot be used with
  `metadata_location`, which points at a fixed snapshot and cannot observe new
  commits.
* **Copy-on-write only.** Follow mode reads copy-on-write changes. If a followed
  snapshot adds a merge-on-read delete file (position or equality deletes), the
  connector stops with an error. Configure the writer to use copy-on-write.

## Transactions

The Iceberg connector can be configured to automatically initiate [transactions](/pipelines/transactions)
when ingesting the table. The `transaction_mode` property configures this feature:

* `none` - the connector does not group inputs into transactions. This is the default.
* `snapshot` - ingest the initial snapshot of the table in one or several transactions. Changes
  ingested afterward, in the follow phase, are not grouped into transactions.
* `catchup` - ingest the initial snapshot like `snapshot`. In the follow phase, the connector
  groups all table commits that are already available into a single transaction: while catching up
  on a backlog it ingests many commits per transaction, and once caught up it ingests about one
  commit per transaction. This is the most efficient mode for backfill and steady-state following.
* `always` - ingest the initial snapshot like `snapshot`. In the follow phase, each table commit is
  ingested in its own transaction.

### Ingesting the table snapshot using transactions

When `transaction_mode` is set to `snapshot`, the connector ingests the snapshot of the table
in one or several transactions. The exact behavior depends on the value of the `timestamp_column`
option. If `timestamp_column` is not set, the connector ingests the whole snapshot in one big
transaction.

If `timestamp_column` is set, the connector ingests the snapshot in a series of batches, one for
each timestamp range of width equal to the `LATENESS` attribute of the `timestamp_column`. Each
range is ingested in a separate transaction. The number of transactions therefore depends on the
range of values in the timestamp column and the width of `LATENESS`, not on the physical layout
(partitioning) of the table. See `timestamp_column` documentation
[below](#ingesting-time-series-data-from-iceberg) for more details.

## Fault tolerance

The connector supports [fault tolerance](/pipelines/fault-tolerance) at the
**at-least-once** level. On a pipeline restart it resumes the snapshot read from the last
checkpoint instead of re-ingesting the whole table, which matters for large tables where a
full re-read is expensive. Records are not deduplicated, so a resumed read may re-emit some
of the rows ingested just before the checkpoint.

The connector pins the snapshot it reads at the first read (resolving the latest snapshot to
a concrete snapshot id) and records that id in every checkpoint, so a resumed read sees the
same immutable data even if the table has advanced in the meantime.

How much a restart re-reads depends on `timestamp_column`:

| Configuration | Checkpoint granularity | Re-read on restart |
|---|---|---|
| `timestamp_column` set (ordered read) | one [lateness](#ingesting-time-series-data-from-iceberg) range | at most the range in flight |
| `timestamp_column` unset (unordered read) | the whole snapshot | the whole snapshot |

An unordered read has no seekable interior boundary, so a checkpoint taken while it is in
progress resumes by re-reading the whole snapshot. Set `timestamp_column` to get incremental,
bounded-re-read checkpointing on large tables. Once the snapshot has been fully ingested, a
restart resumes directly into the completed state and reads nothing further, regardless of
`timestamp_column`.

## Ingesting time series data from Iceberg

Feldera is optimized to efficiently process time series data by taking advantage
of the fact that such data often arrives ordered by timestamp, i.e., every event
has the same or larger timestamp than the previous event. In some cases, events
can get reordered and delayed, but this delay is bounded, e.g., it may not
exceed 1 hour. We refer to this bound as **lateness** and specify it by
attaching the [`LATENESS`](/sql/streaming#lateness-expressions) attribute to the
timestamp column of the table declaration.  See our [Time Series Analysis
Guide](/tutorials/time-series) for more details.

When reading from an Iceberg table that contains time series data, the user must
ensure that the initial snapshot of the table is ingested respecting the
`LATENESS` annotation, e.g., if the table contains one year worth of data, and
its lateness is equal to 1 month, then the connector must ingest all data for the
first month before moving to the second month, and so on.  If this requirement
is violated, the pipeline will drop records that arrive more than `LATENESS` out
of order.

This can be achieved using the `timestamp_column` property, which specifies the table column
that serves as an event timestamp. When this property is set, table rows are ingested in the timestamp
order, respecting the `LATENESS` annotation on the column: each ingested row has a
timestamp no more than `LATENESS` time units earlier than the most recent timestamp
of any previously ingested row.  The ingestion is performed by partitioning the table
into timestamp ranges of width `LATENESS` and ingesting ranges one by one in increasing
timestamp order.

Requirements:
* The timestamp column must be of a supported type: integer, `DATE`, or `TIMESTAMP`.
* The timestamp column must be declared with non-zero `LATENESS`.
* `LATENESS` must be a valid constant expression in the [DataFusion
  SQL dialect](https://datafusion.apache.org/). The reason for this is that Feldera
  uses the Apache DataFusion engine to query the Iceberg table.  In practice, most
  valid Feldera SQL expressions are accepted by DataFusion.
* For efficient ingest, the Iceberg table must be optimized for timestamp-based queries
  using partitioning and sorting.

### Example

The following table contains a timestamp column of type `TIMESTAMP` with `LATENESS` equal
to `INTERVAL 1 day`. Assuming that the oldest timestamp in the table is `2023-01-01T00:00:00`,
the connector fetches all records with timestamps from `2023-01-01`, then all records for
`2023-01-02`, `2023-01-03`, etc., until all records in the table have been ingested. With
`transaction_mode` set to `snapshot`, each daily range is ingested in a separate transaction.

```sql
CREATE TABLE iceberg_table(
  id BIGINT,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 DAY,
  dt DATE
) WITH (
  'materialized' = 'true',
  'connectors' = '[{
    "transport": {
      "name": "iceberg_input",
      "config": {
        "mode": "snapshot",
        "transaction_mode": "snapshot",
        "timestamp_column": "ts",
        "metadata_location": "file:///tmp/warehouse/test_table/metadata/00001-26093ae9-b816-40ca-8ca4-05bd445a8a1d.metadata.json"
      }
    }
  }]'
);
```

## Examples

### Read an Iceberg table from S3 through the AWS Glue catalog

Create an Iceberg input connector to read a snapshot of a table stored in an S3 bucket
through the [AWS Glue Catalog](https://docs.aws.amazon.com/glue/). Note that the connector
configuration specifies separate AWS credentials — including the access key ID, secret
access key, and region — for the AWS Glue Catalog and the S3 bucket containing the table
data. These credentials can either be the same, when using a single IAM identity for both
services, or different, when using separate IAM identities.

```sql
CREATE TABLE iceberg_table(
  id BIGINT,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "iceberg_input",
            "config": {
                "mode": "snapshot",
                "glue.warehouse": "s3://feldera-iceberg-test/",
                "catalog_type": "glue",
                "table_name": "iceberg_test.test_table",
                "glue.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "glue.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "glue.region": "us-east-1",
                "s3.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "s3.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "s3.region": "us-east-1"
            }
        }
    }]'
);
```

### Read an Iceberg table from S3 through the S3 Tables catalog

Create an Iceberg input connector to read a snapshot of a table stored in an
[Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html)
table bucket. As with the AWS Glue catalog, the configuration specifies AWS credentials
separately for the S3 Tables catalog API (`s3tables.*`) and for reading the table's data
files from S3 (`s3.*`). These credentials can either be the same, when using a single IAM
identity for both, or different, when using separate IAM identities.

```sql
CREATE TABLE iceberg_table(
  id BIGINT,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "iceberg_input",
            "config": {
                "mode": "snapshot",
                "catalog_type": "s3tables",
                "table_name": "iceberg_test.test_table",
                "s3tables.table-bucket-arn": "arn:aws:s3tables:us-east-2:123456789012:bucket/my-bucket",
                "s3tables.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "s3tables.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "s3tables.region": "us-east-2",
                "s3.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "s3.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "s3.region": "us-east-2"
            }
        }
    }]'
);
```

### Read an Iceberg table from S3 through a REST catalog

Create an Iceberg input connector to read a snapshot of a table stored in an S3 bucket
through a REST catalog running on `http://127.0.0.1:8181`.

```sql
CREATE TABLE iceberg_table(
  id BIGINT,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
)
WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "iceberg_input",
            "config": {
                "mode": "snapshot",
                "catalog_type": "rest",
                "table_name": "iceberg_test.test_table",
                "rest.uri": "http://127.0.0.1:8181",
                "rest.warehouse": "s3://feldera-iceberg-test/",
                "s3.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "s3.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "s3.region": "us-east-1"
            }
        }
    }]'
);
```

### Read an Iceberg table from local file system

Read an Iceberg table from the local file system. Use the specified snapshot id.
Only select records with timestamp `2023-01-01 00:00:00` or later.

```sql
CREATE TABLE iceberg_table(
  id BIGINT,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "iceberg_input",
            "config": {
                "mode": "snapshot",
                "metadata_location": "file:///tmp/warehouse/test_table/metadata/00001-26093ae9-b816-40ca-8ca4-05bd445a8a1d.metadata.json",
                "snapshot_id": 3325185130458326470,
                "snapshot_filter": "ts >= ''2023-01-01 00:00:00''",
            }
        }
    }]'
);
```
