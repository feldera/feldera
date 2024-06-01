# Delta Lake input connector

[Delta Lake](https://delta.io/) is an open-source storage framework for the
[Lakehouse architecture](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf).
It is typically used with the [Apache Spark](https://spark.apache.org/) runtime.
Data in a Delta Lake is organized in tables (called Delta Tables), stored in
a file system or an object stores like [AWS S3](https://aws.amazon.com/s3/),
[Google GCS](https://cloud.google.com/storage), or
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
Like other Lakehouse-native storage formats, Delta Lake is optimized for both
batch and stream processing, offering a bridge between the two worlds.

## Delta Lake input connector configuration

### Required parameters

* `uri` - Table URI, e.g., "s3://feldera-fraud-detection-data/demographics_train"
* `mode` - Table read mode.  Three options are available:
  * `snapshot` - read a snapshot of the table and stop.
  * `follow` - follow the changelog of the table, only ingesting changes (new and deleted rows)
  * `snapshot_and_follow` - Read a snapshot of the table before switching to the `follow` mode.
    This mode implements the backfill pattern where we load historical data for the table
    before ingesting the stream of real-time updates.

### Optional parameters

* `timestamp_column` - Table column that serves as an event timestamp.  When this option is
   specified, and `mode` is one of `snapshot` or `snapshot_and_follow`,
   the snapshot of the table is ingested in the timestamp order.  This setting is required
   for tables declared with the
   [`LATENESS`](https://www.feldera.com/docs/sql/streaming#lateness-expressions) attribute
   in Feldera SQL.  It impacts the performance of the connector, since data must be sorted
   before pushing it to the pipeline; therefore it is not recommended to use this
   settings for tables without `LATENESS`.

* `snapshot_filter` - Optional row filter.  This option is only valid when `mode` is set to
  `snapshot` or `snapshot_and_follow`.  When specified, only rows that satisfy the filter
   condition are included in the snapshot.  The condition must be a valid SQL Boolean
   expression that can be used in the `where` clause of the `select * from snapshot where ..`
   query.

   This option can be used for example to specify the range of event times to
   include in the snapshot, e.g.: `ts BETWEEN '2005-01-01 00:00:00' AND
   '2010-12-31 23:59:59'`.

* `version` - Optional table version.  When this option is set, the connector finds and
   opens the specified version of the table. In `snapshot` and `snapshot_and_follow` modes,
   it retrieves the snapshot of this version of the table.  In `follow` and
   `snapshot_and_follow` modes, it follows transaction log records **after** this version.

   Note: at most one of `version` and `datetime` options can be specified.
   When neither of the two options is specified, the latest committed version of the table
   is used.

* `datetime` - Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g.,
   "2024-12-09T16:09:53+00:00". When this option is set, the connector finds and opens the
   version of the table as of the specified point in time (based on the server time recorded
   in the transaction log, not the event time encoded in the data).  In `snapshot` and
   `snapshot_and_follow` modes, it retrieves the snapshot of this version of the table.
   In `follow` and `snapshot_and_follow` modes, it follows transaction log records
   **after** this version.

   Note: at most one of `version` and `datetime` options can be specified.
   When neither of the two options is specified, the latest committed version of the table
   is used.

### Storage parameters

Along with the parameters mentioned above, there are additional configuration options for
specific storage backends.  Refer to backend-specific documentation for details:

* [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
* [Azure Blob Storage options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
* [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)


## Example usage

### curl

Create a Delta Lake input connector that reads a snapshot of a table from a public S3 bucket.
Note the `aws_skip_signature` flag, required to read from the bucket without authentcation.  The
snapshot will be sorted by the `unix_time` column.

```bash
curl -i -X PUT http://localhost:8080/v0/connectors/delta-input-test \
-H "Authorization: Bearer <API-KEY>" \
-H 'Content-Type: application/json' \
-d '{
  "description": "Delta table input connector that reads from a public S3 bucket",
  "config": {
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
}'
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

### Python SDK

See [Delta Lake input connector documentation](https://www.feldera.com/python/feldera.html#feldera.sql_context.SQLContext.connect_source_delta_table)
in the Python SDK.
