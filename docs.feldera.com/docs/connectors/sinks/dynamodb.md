# DynamoDB output connector

:::caution Experimental feature
DynamoDB support is an experimental feature of Feldera.
:::

Feldera allows you to output data from a SQL view to an Amazon DynamoDB table.

:::important
Only SQL views with [Uniqueness Constraints](/connectors/unique_keys) can output
data to a DynamoDB table.

The columns of the Feldera index must match the primary key of the DynamoDB
table. If the table uses a composite primary key (partition key + sort key),
the index must include both columns.
:::

## DynamoDB output configuration

| Property                         | Type    | Default   | Description                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------------------------- | ------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `table`\*                        | string  |           | Name of the DynamoDB table to write to. Must be between 3 and 255 characters and contain only letters, numbers, hyphens, underscores, and dots.                                                                                                                                                                                                                                                         |
| `region`\*                       | string  |           | AWS region where the DynamoDB table resides, e.g., `"us-east-1"`.                                                                                                                                                                                                                                                                                                                                       |
| `endpoint_url`                   | string  |           | Optional endpoint URL override, for example when using a local DynamoDB-compatible service such as [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html).                                                                                                                                                                                               |
| `aws_access_key_id`              | string  |           | AWS access key ID for static credentials. Must be specified together with `aws_secret_access_key`. If both are omitted, the connector uses the default AWS credential provider chain (environment variables, `~/.aws/credentials`, IAM roles for Amazon EKS, and so forth).                                                                                                                             |
| `aws_secret_access_key`          | string  |           | AWS secret access key for static credentials. Must be specified together with `aws_access_key_id`.                                                                                                                                                                                                                                                                                                      |
| `write_mode`                     | string  | `batch`   | [Write mode](#write-modes) to use when flushing records (`"batch"` or `"transactional"`).                                                                                                                                                                                                                                                                                                               |
| `batch_size`                     | integer |           | Maximum number of write requests per DynamoDB API call. Defaults to `25` for `batch` mode and `100` for `transactional` mode, which are the respective DynamoDB limits.                                                                                                                                                                                                                                 |
| `max_buffer_size_bytes`          | integer | `1048576` | Maximum number of bytes buffered by each worker thread before flushing to DynamoDB. Default is 1 MiB (`1048576` bytes).                                                                                                                                                                                                                                                                                 |
| `max_concurrent_requests`        | integer | `64`      | Maximum number of DynamoDB write requests in flight per worker thread at any one time. The connector blocks the encoding thread when this limit is reached, applying backpressure to the pipeline.                                                                                                                                                                                                      |
| `threads`                        | integer | `1`       | Number of parallel worker threads used to encode and write disjoint key ranges. Each thread makes its own DynamoDB API calls. Increasing this value can improve throughput for large batches.                                                                                                                                                                                                           |
| `allow_cross_step_write_overlap` | boolean | `false`   | Allow writes from later DBSP steps to be submitted before all writes from earlier steps have completed. This can improve throughput for small steps by keeping DynamoDB requests in flight across step boundaries. Use for mostly disjoint-key workloads where cross-step write ordering to the same DynamoDB item is not required.                                                                     |
| `max_retries`                    | integer | `10`      | Maximum number of retries for a failed or partially-applied DynamoDB write. For `batch` mode, retries apply to items returned as unprocessed in a successful response. For `transactional` mode, retries apply to failed `TransactWriteItems` calls. Set to `null` to retry indefinitely. Transient errors such as throttling are handled separately by the AWS SDK and do not count toward this limit. |

[*]: Required fields

## Write modes

The DynamoDB connector supports two write modes:

### Batch mode (default)

In batch mode (`"write_mode": "batch"`), the connector uses the
[`BatchWriteItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
API. This mode:

- Provides high throughput by writing up to 25 items per API call
- Does not make writes atomic across items in the same batch

### Transactional mode

In transactional mode (`"write_mode": "transactional"`), the connector uses the
[`TransactWriteItems`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)
API. This mode:

- Guarantees atomicity for each transaction chunk of up to 100 items
- Is substantially slower than batch mode due to the overhead of ACID transactions

## Item-size limit

DynamoDB rejects any item larger than its 400 KB
[item-size limit](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-items).
Because such an item can never be written, the connector drops it while encoding
rather than sending it. This keeps an oversized record from failing the write
chunk or transaction it would otherwise share, so the remaining records in the
same batch are still written. Dropped records are reported through the
`dynamodb_output_oversized_items_dropped_total` metric.

## Performance

The following samples were measured using a Feldera pipeline writing
well-distributed records smaller than 1 KiB to a DynamoDB table provisioned with
40k WCU.

| `threads` | `max_concurrent_requests` | WCU/s without overlap | WCU/s with overlap |
| --------- | ------------------------- | --------------------- | ------------------ |
| 1         | 64                        | ~7.7k                 | ~16.9k             |
| 1         | 128                       | ~27.2k                | ~37.1k             |

Suggestions:

- Enable `allow_cross_step_write_overlap` to keep DynamoDB requests in flight
  across step boundaries instead of draining the entire write pipeline at every
  `batch_end`; in these measurements, it more than doubled WCU/s at `threads=1`,
  `max_concurrent_requests=64`.
- Increase `max_concurrent_requests` when DynamoDB still has unused capacity.
- If increasing `threads` or `max_concurrent_requests` does not improve
  throughput, check whether the pipeline is stalling because output buffers are
  full. In that case, increase the output connector's `max_queued_records` so
  the pipeline can queue more completed records while DynamoDB writes are still
  in flight.

## AWS credentials

If `aws_access_key_id` and `aws_secret_access_key` are both specified, the
connector uses those static credentials. Otherwise it falls back to the default
AWS credential provider chain, which checks (in order):

1. The `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables
2. The `~/.aws/credentials` file
3. IAM Roles for Service Accounts (IRSA) when running in Amazon EKS

## Data type mapping

:::info
The following table lists supported DynamoDB attribute types.
Please [let us know](https://github.com/feldera/feldera/issues) if you need support for a specific type.
:::

| Feldera Type      | DynamoDB Attribute Type               | Comments                                          |
| ----------------- | ------------------------------------- | ------------------------------------------------- |
| BOOL              | Boolean (`BOOL`)                      |                                                   |
| TINYINT           | Number (`N`)                          | Encoded as a numeric string.                      |
| SMALLINT          | Number (`N`)                          | Encoded as a numeric string.                      |
| INT               | Number (`N`)                          | Encoded as a numeric string.                      |
| BIGINT            | Number (`N`)                          | Encoded as a numeric string.                      |
| DECIMAL           | Number (`N`)                          | Encoded as a numeric string.                      |
| REAL              | Number (`N`)                          | Encoded as a numeric string.                      |
| DOUBLE            | Number (`N`)                          | Encoded as a numeric string.                      |
| VARCHAR           | String (`S`)                          |                                                   |
| TIME              | String (`S`)                          |                                                   |
| DATE              | String (`S`)                          |                                                   |
| TIMESTAMP         | String (`S`)                          |                                                   |
| UUID              | String (`S`)                          |                                                   |
| VARIANT           | `S`, `N`, `BOOL`, `NULL`, `M`, or `L` | Attribute type follows the underlying JSON value. |
| VARBINARY         | Binary (`B`)                          |                                                   |
| ARRAY             | List (`L`)                            |                                                   |
| User Defined Type | Map (`M`)                             |                                                   |
| MAP               | Map (`M`)                             |                                                   |
| NULL values       | Null (`NULL`)                         |                                                   |

## Example

First, create a DynamoDB table with a composite key. The partition key must
correspond to one of the index columns and the sort key to another.

```sh
aws dynamodb create-table \
  --table-name feldera_out \
  --attribute-definitions \
      AttributeName=id,AttributeType=N \
      AttributeName=sort,AttributeType=S \
  --key-schema \
      AttributeName=id,KeyType=HASH \
      AttributeName=sort,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

Now output data to this table from a Feldera view.

```sql
-- Feldera SQL

-- Create a table and fill it with 5 randomly generated records.
CREATE TABLE t0 (id INT NOT NULL, sort VARCHAR NOT NULL, s VARCHAR) WITH (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{ "rate": 1, "limit": 5 }]
      }
    }
  }]'
);

-- Create a view and attach a DynamoDB output connector to it.
CREATE MATERIALIZED VIEW v1 WITH (
    'connectors' = '[{
        "index": "v1_idx",
        "transport": {
            "name": "dynamodb_output",
            "config": {
                "table": "feldera_out",
                "region": "us-east-1"
            }
        }
    }]'
) AS SELECT * FROM t0;

-- Index v1 using (id, sort) as a composite key.
-- Both columns must correspond to the DynamoDB table's key schema.
CREATE INDEX v1_idx ON v1(id, sort);
```

:::important
Column names in the Feldera SQL index and in the DynamoDB table key schema must
match exactly.
:::
