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

The connector is compatible with REST and AWS Glue catalogs and also supports direct
table reads without a catalog, provided the location of the metadata file. Supported
storage systems include S3, GCS, and local file systems.

The Iceberg input connector does not yet support [fault tolerance](/pipelines/fault-tolerance).


## Configuration

| Property                    | Type   | Description   |
|-----------------------------|--------|---------------|
| `mode`*                     | enum   | Table read mode. Currently, the only supported mode is `snapshot`, in which the connector reads a snapshot of the table and stops.|
| `snapshot_filter`           | string | <p>Optional row filter.  When specified, only rows that satisfy the filter condition are included in the snapshot.  The condition must be a valid SQL Boolean expression that can be used in the `where` clause of the `select * from snapshot where ..` query.</p><p> This option can be used to specify the range of event times to include in the snapshot, e.g.: `ts BETWEEN TIMESTAMP '2005-01-01 00:00:00' AND TIMESTAMP '2010-12-31 23:59:59'`.</p>
| `snapshot_id`               | integer| <p>Optional table snapshot id.  When this option is set, the connector reads the specified snapshot of the table.</p><p>Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest snapshot of the table is used.</p>
| `datetime`                  | string | <p>Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g., "2024-12-09T16:09:53+00:00". When this option is set, the connector reads the version of the table as of the specified point in time (based on the server time recorded in the transaction log, not the event time encoded in the data). </p><p> Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p>|
| `metadata_location`         | string | Location of the table metadata JSON file. This property is used to access an Iceberg table directly, without a catalog. It is mutually exclusive with the `catalog_type` property.|
| `table_name`                | string | Specifies the Iceberg table name within the catalog in the `namespace.table` format. This option is applicable when an Iceberg catalog is configured using the `catalog_type` property.|
| `catalog_type`              | enum   | Type of the Iceberg catalog used to access the table. Supported options include `rest` and `glue`. This property is mutually exclusive with `metadata_location`.|

<!-- | `timestamp_column`          | string | Table column that serves as an event timestamp. When this option is specified, table rows are ingested in the timestamp order, respecting the [`LATENESS`](/sql/streaming#lateness-expressions) property of the column: each ingested row has a timestamp no more than `LATENESS` time units earlier than the most recent timestamp of any previously ingested row.  See details [below](#ingesting-time-series-data-from-iceberg). | -->


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
| `string`                    | `STRING`         |               |
| `fixed(L)`                  | `BINARY(L)`      |               |
| `binary`                    | `VARBINARY`      |               |

<!-- | `uuid`                      | `BINARY(16)`     |               | -->


Types that are currently not supported include Iceberg's nested data types (`struct`s,
`list`s and `map`s), `uuid`, and timestamps with time zone.

<!--  Uncomment when https://github.com/apache/iceberg-rust/issues/811 is resolved>
<!-- ## Ingesting time series data from Iceberg

Feldera is optimized to efficiently process time series data by taking advantage
of the fact that such data often arrives ordered by timestamp, i.e., every event
has the same or larger timestamp than the previous event. In some cases, events
can get reordered and delayed, but this delay is bounded, e.g., it may not
exceed 1 hour. We refer to this bound as **lateness** and specify it by
attaching the [`LATENESS`](/sql/streaming#lateness-expressions) attribute to the
timestamp column of the table declaration.  See our [Time Series Analysis
Guide](/tutorials/time-series) for more details.

When reading from an Iceberg that contains time series data, the user must
ensure that the initial snapshot of the table is ingested respecting the
`LATENESS` annotation, e.g., if the table contains one year worth of data, and
its lateness is equal to 1 month, then the connector must ingest all data for the
first month before moving to the second month, and so on.  If this requirement
is violated, the pipeline will drop records that arrive more that `LATENESS` out
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
  uses the Apache Datafusion engine to query Delta Lake.  In practice, most
  valid Feldera SQL expressions are accepted by DataFusion.
* For efficient ingest, the Iceberg table must be optimized for timestamp-based queries
  using partitioning and sorting.
 -->
<!-- Note that the `timestamp_column` property only controls the initial table snapshot.
When `mode` is set to `follow` or `snapshot_and_follow` and the connector is following
the transaction log of the table, it ingests changes in the order they appear in the
log.  It is the responsibility of the application that writes to the table
to ensure that changes it applies to the table respect the `LATENESS` annotations. -->

<!-- ### Example

The following table contains a timestamp column of type `TIMESTAMP` with `LATENESS` equal
to `INTERVAL 30 days`. Assuming that the oldest timestamp in the table is `2024-01-01T00:00:00`,
the connector will fetch all records with timestamps from `2024-01-01`, then all records for
`2024-01-02`, `2024-01-03`, etc., until all records in the table have been ingested.

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
``` -->

## Examples

### Read an Iceberg table from S3 through the AWS Glue catalog

Create an Iceberg input connector to read a snapshot of a table stored in an S3 bucket
through the [AWS Glue Catalog](https://docs.aws.amazon.com/glue/). Note that the connector
configuration specifies separate AWS credentials — including the access key ID, secret
access key, and region — for the AWS Glue Catalog and the S3 bucket containing the table
data. These credentials can either be the same, when using a single IAM identity for both
services, or different, when using separate IAM identities.

```sql
create table iceberg_table(
  id bigint,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
) with (
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

### Read an Iceberg table from S3 through a REST catalog

Create an Iceberg input connector to read a snapshot of a table stored in an S3 bucket
through a REST catalog running on `http://127.0.0.1:8181`.

```sql
create table iceberg_table(
  id bigint,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
)
with (
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
create table iceberg_table(
  id bigint,
  name STRING,
  b BOOLEAN,
  ts TIMESTAMP,
  dt DATE
) with (
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