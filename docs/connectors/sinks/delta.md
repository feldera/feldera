# Delta Lake output connector

[Delta Lake](https://delta.io/) is an open-source storage framework for the
[Lakehouse architecture](https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf).
It is typically used with the [Apache Spark](https://spark.apache.org/) runtime.
Data in a Delta Lake is organized in tables (called Delta Tables), stored in
a file system or an object stores like [AWS S3](https://aws.amazon.com/s3/),
[Google GCS](https://cloud.google.com/storage), or
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
Like other Lakehouse-native storage formats, Delta Lake is optimized for both
batch and stream processing, offering a bridge between the two worlds.

The Delta Lake output connector does not yet support [fault
tolerance](..#fault-tolerance).

## Delta Lake output connector configuration

### Required parameters

* `uri` - Table URI, e.g., "s3://feldera-fraud-detection-data/feature_train"
* `mode` - Determines how the Delta table connector handles an existing table at the target
   location.  Three options are available:
  * `append` - New updates will be appended to the existing table at the target location
  * `truncate` - Existing table at the specified location will get truncated. The connector
     truncates the table by outputing delete actions for all files in the latest snapshot
     of the table.
  * `error_if_exists` - If a table exists at the specified location, the operation will fail.

### Storage parameters

Additional configuration options are defined for specific storage backends.  Refer to
backend-specific documentation for details:

* [Amazon S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html)
* [Azure Blob Storage options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html)
* [Google Cloud Storage options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html)

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

## Limitations

This connector currently only appends new records to the Delta Table.  Deletions
output by the pipeline are discarded.  The reason for this limitation is that the
Delta Lake format only supports appending new data to a table in streaming mode.

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
