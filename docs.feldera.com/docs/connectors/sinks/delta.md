# Delta Lake output connector

[Delta Lake](https://delta.io/) is a popular open table format based on Parquet files.
It is typically used with the [Apache Spark](https://spark.apache.org/) runtime.
Data in a Delta Lake is organized in tables, stored in
a file system or an object stores like [AWS S3](https://aws.amazon.com/s3/),
[Google GCS](https://cloud.google.com/storage), or
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

The Delta Lake output connector does not yet support [fault
tolerance](/pipelines/fault-tolerance).

## Support for delete operations

The Delta Lake format does not support efficient real-time deletes and updates.
To delete a record from a Delta table, one must first locate the record, which
often requires an expensive table scan. This limitation makes it inefficient to
directly write the output of a Feldera pipeline, which consists of both inserts
and deletes, to a Delta table.

To address this issue, the Delta Lake connector transforms both inserts and deletes
into table records with additional metadata columns that describe the type and order
of operations. Specifically, the connector adds the following columns to the output
Delta table:

| Column         | Type      | Description                                                                   |
|----------------|-----------|-------------------------------------------------------------------------------|
| `__feldera_op` | `VARCHAR` | Operation that this record represents: `i` for "insert", `d` for "delete", or `u` for "update".  |
| `__feldera_ts` | `BIGINT`  | Timestamp of the update, used to establish the order of updates. Updates with smaller timestamps are applied before those with larger timestamps. |

Effectively, we treat the table as a change log, where every record corresponds to
either an insert or delete operation. The user can run a periodic Spark job to
incorporate these change log into another Delta table, using the SQL `MERGE INTO` operation.

## Delta Lake output connector configuration

| Parameter  | Description |
|------------|------------|
| `uri`*     | Table URI, e.g., `"s3://feldera-fraud-detection-data/feature_train"`. |
| `mode`*    | Determines how the Delta table connector handles an existing table at the target location. Options: |
|            | - `append`: New updates will be appended to the existing table at the target location. |
|            | - `truncate`: Existing table at the specified location will be truncated. The connector achieves this by outputting delete actions for all files in the latest snapshot of the table. |
|            | - `error_if_exists`: If a table exists at the specified location, the operation will fail. |

[*]: Required fields

### Storage parameters

Additional configuration options are defined for specific storage backends.  Refer to
backend-specific documentation for details:

* [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
* [Azure Blob Storage options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
* [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)

### Views with unique keys

If the SQL view contains a **unique key**—a set of columns that uniquely identify each record—the Delta Lake connector can optimize updates by combining a delete and insert with the same key into a single **atomic update**. In such cases, the connector emits a record with the `__feldera_op` field set to `'u'` (for **update**).

To enable this optimization:

* Use the `CREATE INDEX` statement to define the unique key on the view.
* Set the connector's `index` property to reference this index.

For more information, see the [documentation on views with unique keys](/connectors/unique_keys#views-with-unique-keys).

## Data type mapping

See [source connector documentation](/connectors/sources/delta/#data-type-mapping) for DeltaLake to Feldera SQL
type mapping.

## The small file problem and output buffer configuration

By default a Feldera pipeline sends a batch of changes to the output transport
for each batch of input updates it processes.  This can result in a stream of
small updates, which is normal and even preferable for output transports like
Kafka; however it can cause problems for the Delta Lake format by creating a large
number of small files.

The output buffer mechanism is designed to solve this problem by decoupling the
rate at which the pipeline pushes changes to the output transport from the rate
of input changes.  It works by accumulating updates inside the pipeline
for up to a user-defined period of time or until accumulating a user-defined number
of updates and writing them to the Delta Table as a small number of large files.

See [output buffer](/connectors#configuring-the-output-buffer) for details on configuring the output buffer mechanism.

## Example usage

Create a Delta Lake output connector that writes a stream of updates to a table
stored in an S3 bucket, truncating any existing contents of the table.

```sql
CREATE VIEW V
WITH (
 'connectors' = '[{
    "transport": {
      "name": "delta_table_output",
      "config": {
        "uri": "s3://feldera-fraud-detection-demo/feature_train",
        "mode": "truncate",
        "aws_access_key_id": <AWS_ACCESS_KEY_ID>,
        "aws_secret_access_key": <AWS_SECRET_ACCESS_KEY>,
        "aws_region": "us-east-1"
      }
    },
    "enable_output_buffer": true,
    "max_output_buffer_time_millis": 10000
 }]'
)
AS SELECT * FROM my_table;
```
