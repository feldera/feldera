# Apache Iceberg output connector

:::note
This page describes configuration options specific to the Apache Iceberg connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

<p className="preview-note">Iceberg output is in <a href="/feature-maturity">preview</a>.</p>

The Iceberg output connector writes the changes of a view to an
[Apache Iceberg](https://iceberg.apache.org/) table.
It works with REST, AWS Glue, and Amazon S3 Tables catalogs.
It needs a catalog, because the catalog makes each commit atomic.

The connector does not yet support [fault tolerance](/pipelines/fault-tolerance).
See [Delivery guarantees](#delivery-guarantees).

:::tip
Configure the [output buffer](/connectors#configuring-the-output-buffer). Without it, each
pipeline step that has output adds one snapshot: 86,400 per day at one step per second.
See [Table maintenance](#table-maintenance).
:::

## What the connector writes

Iceberg has no cheap way to update or delete single rows at streaming rates, so the
connector writes a change log: each insert, delete, and update of the view becomes one new
row. Each row holds the view columns and two more columns:

| Column         | Type      | Description                                                                   |
|----------------|-----------|-------------------------------------------------------------------------------|
| `__feldera_op` | `VARCHAR` | Operation of the row: `i` for insert, `d` for delete, or `u` for update.  |
| `__feldera_ts` | `BIGINT`  | Time of the batch that holds the row, in microseconds since the UNIX epoch. All rows of a batch have the same value. Apply rows with a smaller value first. |

The connector writes `u` only when it has an [index](/connectors/unique_keys#views-with-unique-keys)
on the view. A `u` row holds the new values of the row. Without an index, an update is a `d`
row and an `i` row with the same `__feldera_ts`: apply the `d` row first.

Each batch of changes becomes one Iceberg snapshot. A reader sees all rows of a batch or none.
The connector writes to v2 and v3 tables.

`SELECT *` returns every change. To get the current rows of a view with a unique key, keep the
last row of each key and drop the keys whose last row is a `d` row:

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY <key columns>
               ORDER BY __feldera_ts DESC,
                        CASE WHEN __feldera_op = 'd' THEN 0 ELSE 1 END DESC
           ) AS rn
    FROM <table>
) AS changes
WHERE rn = 1 AND __feldera_op <> 'd';
```

This query reads the whole log. For a large table, apply only the new changes to a second
table with a periodic `MERGE INTO` job. The job must pick the last row of each key in the
same order.

## Configuration

| Property        | Type    | Description   |
|-----------------|---------|---------------|
| `table_name`*   | string  | Table name in the `namespace.table` format. The namespace must exist. |
| `catalog_type`* | enum    | Catalog that holds the table: `rest`, `glue`, or `s3tables`. |
| `mode`          | enum    | <p>What to do when the table exists as the pipeline starts:</p><ul><li>`append` (default): write to the table. Create it if it does not exist.</li><li>`error_if_exists`: fail if the table exists when the connector starts for the first time. A pipeline that resumes from a checkpoint, with the same connector configuration, opens the table that it created.</li></ul> |
| `max_retries`   | integer | Maximum number of retries of a failed write or commit. Writing a batch again after the table changed also counts as a retry. The connector waits up to 10 seconds between retries and reports itself unhealthy until a retry succeeds. The default is no limit, except when the pipeline starts: then the connector tries 10 more times to open the table, for at most 5 minutes. `0` disables retries. |
| `threads`       | integer | Number of threads that encode and upload one batch at the same time. A value above 1 needs an [index](/connectors/unique_keys#views-with-unique-keys) on the view. The default is 1. |

[*]: Required fields

The catalog and storage properties are the same as those of the Iceberg input connector:

| Properties | Described in |
|------------|--------------|
| `rest.*` | [Rest catalog configuration](/connectors/sources/iceberg#rest-catalog-configuration) |
| `glue.*` | [Glue catalog configuration](/connectors/sources/iceberg#glue-catalog-configuration) |
| `s3tables.*` | [S3 Tables catalog configuration](/connectors/sources/iceberg#s3-tables-catalog-configuration) |
| `s3.*`, `gcs.*`, and other storage properties | [FileIO configuration](/connectors/sources/iceberg#fileio-configuration) |

### Vended credentials

To get short-lived storage credentials from a REST catalog, add this header to `rest.headers`:

```json
"rest.headers": [["X-Iceberg-Access-Delegation", "vended-credentials"]]
```

The connector uses the credentials in the `config` field of the catalog response. It does not
use the `storage-credentials` field. Do not set storage credentials such as
`s3.access-key-id`, because they take precedence over the ones from the catalog.

## The table

### A table that the connector creates

If the table does not exist, the connector creates it: a table that is not partitioned,
with one column for each column of the view and the two change log columns.
A REST catalog gives the table its default format version. With AWS Glue and S3 Tables,
the table is v2.
A `NOT NULL` column of the view becomes a `required` column of the table.
The name of a column is in lower case, unless the SQL program has it in double quotes.
The table property `feldera.create-id` marks the table as the connector's own, so that a
retried start recognizes it in `error_if_exists` mode. The connector also sets
`write.metadata.delete-after-commit.enabled` to `true`. See [Table maintenance](#table-maintenance).

| SQL type | Iceberg type |
|----------|--------------|
| `BOOLEAN` | `boolean` |
| `TINYINT`, `SMALLINT`, `INT` | `int` |
| `BIGINT` | `long` |
| `TINYINT UNSIGNED`, `SMALLINT UNSIGNED` | `int` |
| `INT UNSIGNED` | `long` |
| `REAL` | `float` |
| `DOUBLE` | `double` |
| `DECIMAL(p, s)` | `decimal(p, s)` |
| `CHAR`, `VARCHAR` | `string` |
| `BINARY`, `VARBINARY` | `binary` |
| `DATE` | `date` |
| `TIME` | `time`. Iceberg keeps microseconds, so the connector drops the nanoseconds of the value. |
| `TIMESTAMP` | `timestamp` |
| `TIMESTAMP WITH TIME ZONE` | `timestamptz` |
| `UUID` | `uuid` |
| `VARIANT` | `string` that holds JSON text |
| `ARRAY` | `list` |
| `MAP` | `map` |
| `ROW` | `struct` |

The connector cannot write a view that has a `BIGINT UNSIGNED`, `INTERVAL`, or `NULL`
column. Cast a `BIGINT UNSIGNED` column to `DECIMAL(20, 0)` in the view, and a `NULL`
column to a type, for example `CAST(NULL AS INT)`.

### A table that exists

The connector can write to a table that you created with another tool, for example to set
table properties or a location. The connector checks the table when the pipeline starts,
and again before each batch:

| The table | Result |
|-----------|--------|
| Has every view column and both change log columns, with the same names (case-sensitive) and the types above | The connector writes to it, in any column order. |
| Has more columns, and they are optional or have a write default | The connector writes the write default, or NULL. |
| Has more fields in a struct column, and they are optional with no write default | The connector writes NULL to them. |
| Lacks a column of the view, has a column of another type, or has a `required` column where the view column can be NULL | Error |
| Has a `required` column with no write default, or a struct field that is `required` or has a write default, that the view lacks | Error |
| Is partitioned or encrypted | Error. The connector does not support these tables yet. |

The connector honors these table properties: `write.parquet.compression-codec`,
`write.parquet.compression-level`, `write.parquet.page-size-bytes`,
`write.parquet.page-row-limit`, `write.parquet.dict-size-bytes`,
`write.target-file-size-bytes`, `write.data.path`, `write.object-storage.enabled`, and the
`commit.retry.*` properties. It caps `write.parquet.row-group-size-bytes` at 16 MiB. Each
thread buffers at most one row group and 100,000 encoded rows.

You can add an optional column, or an optional field of a struct column, while the
pipeline runs. If the schema of the table changes while the connector writes a batch, the
connector writes that batch again, which counts as a retry of `max_retries`.

## Delivery guarantees

| Event | What the table holds |
|-------|----------------------|
| A write or a commit fails and the connector tries it again | Each batch one time. The connector records the id of each batch in the snapshot summary (`feldera.commit-id`), and looks for it before it commits a batch again. |
| The pipeline stops and resumes from a checkpoint | The changes since the checkpoint a second time. Remove the repeated rows in the job that applies the change log. |
| `max_retries` is set and all retries of a batch fail | Possibly not the rows of that batch. The connector reports an error. With the default, the connector retries until the batch is in the table. |
| A row of a batch cannot be encoded | Not the rows of that batch. The connector reports an error. |

While the connector retries, the pipeline waits for it. If the table no longer fits the view,
for example because a column was removed, the connector reports the mismatch and
continues when the table fits again. If no change can make the table fit, for example
because it is encrypted, each batch fails with an error.

## Table maintenance

The connector only adds data. Each batch adds one snapshot and these files: one manifest
list, one manifest, one table metadata file (`metadata.json`), and one or more data files.
The connector does not merge or remove any of them, and a table with many of them becomes
slow to read and slow to commit to.
In a test with a REST catalog, the time to commit one batch grew by about 30 milliseconds
for each 1,000 snapshots of the table.

1. Configure the [output buffer](/connectors#configuring-the-output-buffer), so that the
   connector commits at a fixed cadence and not after every step of the pipeline. A cadence of
   one minute gives 1,440 snapshots per day.
2. Run table maintenance on a schedule, with an engine that supports it, such as
   [Spark](https://iceberg.apache.org/docs/latest/spark-procedures/), or use a catalog that
   maintains its tables, such as Amazon S3 Tables:

| Procedure | Purpose |
|-----------|---------|
| `rewrite_manifests` | Merges the manifest files. |
| `rewrite_data_files` | Merges small data files. |
| `expire_snapshots` | Removes old snapshots and the files that only they use, but not old `metadata.json` files. |
| `remove_orphan_files` | Removes the files that the table does not reference. These include the data files of batches that were never committed. They also include the `metadata.json` files older than the current file and, by default, the 100 before it. |

Maintenance can run while the pipeline runs. Each commit builds on the latest table state.

A REST catalog can remove old `metadata.json` files itself when the table property
`write.metadata.delete-after-commit.enabled` is `true`. It then keeps the current file and,
by default, the 100 before it (`write.metadata.previous-versions-max`). The connector sets
this property on the tables that it creates. With AWS Glue and S3 Tables, the connector writes these files
and does not remove them.

## Limitations

| Limitation | Detail |
|------------|--------|
| No `truncate` mode | The connector cannot empty a table. |
| No partitioned tables | The connector refuses to write to a partitioned table. |
| AWS Glue: no `timestamptz` | With `catalog_type` set to `glue`, the connector cannot write to a table that has, or had in an earlier schema, a `timestamptz` column, and so not a view that has a `TIMESTAMP WITH TIME ZONE` column. |
| Storage credentials from the catalog | The connector takes new credentials from the catalog for each batch. One batch must not take longer to write than the credentials are valid. |
| `rest.token` | The connector never renews this token, even when you set `rest.credential`, and stops writing when it expires. Use `rest.credential` without `rest.token` instead. |
| No OAuth2 token exchange | The connector gets a token only with `rest.credential` (the client credentials grant), or uses `rest.token`. |
| PyIceberg cannot read the table | PyIceberg up to version 0.12 fails with `Unsupported codec: gzip` on the manifest files that the connector writes. Spark, DuckDB, and the Feldera Iceberg input connector read them. PyIceberg can read the table after Spark `rewrite_manifests`, until the connector commits again. |
| Spark cannot read a `time` column | Spark has no type for Iceberg `time`. It cannot read a table that has such a column. |
| Table changes while the pipeline runs | Do not drop the table, replace it, or change its partition spec while the connector writes to it. Pause the connector first. |

## Example

The following view writes its changes to the table `analytics.price_changes` in a REST
catalog, with the data in S3. The connector commits once per minute or once per one million
changes, whichever comes first.

```sql
CREATE VIEW price_changes
WITH (
    'connectors' = '[{
        "transport": {
            "name": "iceberg_output",
            "config": {
                "catalog_type": "rest",
                "rest.uri": "http://localhost:8181",
                "table_name": "analytics.price_changes",
                "s3.access-key-id": "<AWS_ACCESS_KEY_ID>",
                "s3.secret-access-key": "<AWS_SECRET_ACCESS_KEY>",
                "s3.region": "us-east-1"
            }
        },
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 60000,
        "max_output_buffer_size_records": 1000000
    }]'
)
AS SELECT * FROM price;
```

## Writing to Iceberg via Kafka Connect

To write the current rows of a view to an Iceberg table, and not a change log, use Kafka
Connect. With the configuration below, Kafka Connect writes deletes and updates as equality
delete files.

Feldera enables integration with Apache Iceberg by first writing
change data capture (CDC) records to Kafka, and then using the
[Iceberg Sink Connector for Kafka Connect](https://github.com/databricks/iceberg-kafka-connect) to persist these changes
to Iceberg tables.

#### Workflow

1. Feldera writes Avro-encoded CDC data to a Kafka topic.
2. Kafka Connect consumes this topic using the Iceberg Sink Connector.
3. The connector writes updates to an Iceberg table.

##### Avro format configuration

The Avro format must be configured with:
- An index on the identifying columns of the view, consistent with `iceberg.tables.default-id-columns`.
- `update_format` set to `raw`
- `cdc_field` defined (e.g. set to `op`)
  - This `cdc_field` should match the `iceberg.tables.cdc-field` config in Iceberg sink connector for Kafka Connect.
  - If `cdc_field` is not set, it will be append-only log of output records without any CDC metadata.
  - Each output record will contain the field `op` which will have one of the following values:
    - `I` for **Insert**
    - `D` for **Delete**
    - `U` for **Upsert**

Example:

```sql
CREATE MATERIALIZED VIEW pizzas WITH (
   'connectors' = '[
    {
      "index": "idx1",
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "localhost:29092",
              "topic": "pizzas"
          }
      },
      "format": {
          "name": "avro",
          "config": {
              "registry_urls": ["http://localhost:18081"],
              "update_format": "raw",
              "cdc_field": "op",
              "subject_name_strategy": "topic_name"
          }
      }
    }
   ]'
) AS SELECT * FROM tbl ORDER BY order_number DESC LIMIT 10;
CREATE INDEX idx1 ON pizzas(order_number);
```

:::important
The index attribute is required and ensures proper materialization of the Iceberg table.
For more information, see [uniqueness constraints](/connectors/unique_keys#views-with-unique-keys).
:::

#### Iceberg sink connector configuration for Kafka Connect

Sample configuration for setting up Iceberg Sink Connector using Kafka
Connect.

```json
{
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "6",
    "topics": "pizzas",
    "iceberg.tables": "rpc.pizzas",
    "iceberg.catalog": "demo",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.catalog.client.region": "us-east-1",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.control.commit.interval-ms": 30000,
    "iceberg.tables.cdc-field": "op",
    "iceberg.tables.default-id-columns": "order_number",
    "iceberg.tables.upsert-mode-enabled": "true",
    "key.converter.schema.registry.url": "http://registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schema.registry.url": "http://registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schemas.enable": "true"
}
```
- Ensure that `iceberg.tables.default-id-columns` is consistent with the index
  definition in Feldera.
- Ensure that the Iceberg table is accessible from Kafka Connect.
- Ensure that `iceberg.tables.auto-create-enabled` is set to `true` if the
  table doesn't already exist.
- Ensure that `iceberg.tables.evolve-schema-enabled` is set to `true` if you
  want to dynamically update the Iceberg table schema to match the SQL view
  declaration.
- Ensure that `iceberg.tables.cdc-field` is set to the same value as `cdc_field`
  in the output connector configuration in Feldera.

