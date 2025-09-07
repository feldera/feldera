import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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

The Delta Lake input connector supports checkpoint and resume and
at-least-once [fault tolerance](/pipelines/fault-tolerance), but not
exactly once fault tolerance.

## Delta Lake input connector configuration

| Property                    | Type   | Default    | Description   |
|-----------------------------|--------|------------|---------------|
| `uri`*                      | string |            | Table URI, e.g., `s3://feldera-fraud-detection-data/demographics_train`. Supported URI schemes include: <ul><li>AWS S3: `s3://`, `s3a://`</li><li>Azure Blob Storage: `az://`, `adl://`, `azure://`, `abfs://`, `abfss://`</li><li>Google Cloud Storage: `gs://`</li><li>`uc://` - Unity catalog</li></ul> |
| `mode`*                     | enum   |            | Table read mode. Three options are available: <ul> <li>`snapshot` - read a snapshot of the table and stop.</li> <li>`follow` - follow the changelog of the table, only ingesting changes (new and deleted rows)</li> <li>`snapshot_and_follow` - Read a snapshot of the table before switching to the `follow` mode.  This mode implements the backfill pattern where we load historical data for the table before ingesting the stream of real-time updates.</li><li>`cdc` - Change-Data-Capture (CDC) mode. The table behaves as an append-only log where every row represents an insert or delete action.  The order of actions is determined by the `cdc_order_by` property, and the type of each action is determined by the `cdc_delete_filter` property. In this mode, the connector does not read the initial snapshot of the table and follows the transaction log starting from the version of the table specified by the `version` or `datetime` property.</li> </ul>|
| `timestamp_column`          | string |            | Table column that serves as an event timestamp. When this option is specified, and `mode` is one of `snapshot` or `snapshot_and_follow`, table rows are ingested in the timestamp order, respecting the [`LATENESS`](/sql/streaming#lateness-expressions) property of the column: each ingested row has a timestamp no more than `LATENESS` time units earlier than the most recent timestamp of any previously ingested row.  See details [below](#ingesting-time-series-data-from-a-delta-lake). |
| `filter`                    | string |            | <p>Optional row filter.</p> <p>When specified, only rows that satisfy the filter condition are read from the delta table. The condition must be a valid SQL Boolean expression that can be used in the `where` clause of the `select * from my_table where ...` query.</p> |
| `snapshot_filter`           | string |            | <p>Optional snapshot filter.</p><p>This option is only valid when `mode` is set to `snapshot` or `snapshot_and_follow`. When specified, only rows that satisfy the filter condition are included in the snapshot.</p> <p>The condition must be a valid SQL Boolean expression that can be used in  the `where` clause of the `select * from snapshot where ...` query.</p><p>Unlike the `filter` option, which applies to all records retrieved from the table, this filter only applies to rows in the initial snapshot of the table. For instance, it can be used to specify the range of event times to include in the snapshot, e.g.: `ts BETWEEN TIMESTAMP '2005-01-01 00:00:00' AND TIMESTAMP '2010-12-31 23:59:59'`. This option can be used together with the `filter` option. During the initial snapshot, only rows that satisfy both `filter` and `snapshot_filter` are retrieved from the Delta table. When subsequently following changes in the the transaction log (`mode = snapshot_and_follow`), all rows that meet the `filter` condition are ingested, regardless of `snapshot_filter`. </p> |
| `version`, `start_version`  | integer|            | <p>Optional table version.  When this option is set, the connector finds and opens the specified version of the table. In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of the table.  In `follow`, `snapshot_and_follow`, and `cdc` modes, it follows transaction log records **after** this version.</p><p>Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p> |
| `datetime`                  | string |            | <p>Optional timestamp for the snapshot in the ISO-8601/RFC-3339 format, e.g., "2024-12-09T16:09:53+00:00". When this option is set, the connector finds and opens the version of the table as of the specified point in time (based on the server time recorded in the transaction log, not the event time encoded in the data).  In `snapshot` and `snapshot_and_follow` modes, it retrieves the snapshot of this version of the table.  In `follow`, `snapshot_and_follow`, and `cdc` modes, it follows transaction log records **after** this version.</p><p> Note: at most one of `version` and `datetime` options can be specified.  When neither of the two options is specified, the latest committed version of the table is used.</p>|
| `end_version`               | integer|            | <p>Optional final table version.</p><p>Valid only when the connector is configured in `follow`, `snapshot_and_follow`, or `cdc` mode.</p><p>When set, the connector will stop scanning the table’s transaction log after reaching this version or any greater version.</p><p>This bound is inclusive: if the specified version appears in the log, it will be processed before signaling end-of-input.</p>|
| `cdc_delete_filer`          | string |            | <p>A predicate that determines whether the record represents a deletion.</p><p>This setting is only valid in the `cdc` mode. It specifies a predicate applied to each row in the Delta table to determine whether the row represents a deletion event. Its value must be a valid Boolean SQL expression that can be used in a query of the form `SELECT * from <table> WHERE <cdc_delete_filter>`.</p>|
| `cdc_order_by`              | string |            | <p>An expression that determines the ordering of updates in the Delta table.</p><p>This setting is only valid in the `cdc` mode. It specifies a predicate applied to each row in the Delta table to determine the order in which updates in the table should be applied. Its value must be a valid SQL expression that can be used in a query of the form `SELECT * from <table> ORDER BY <cdc_order_by>`.</p>|
| `num_parsers`               | string |            | The number of parallel parsing tasks the connector uses to process data read from the table. Increasing this value can enhance performance by allowing more concurrent processing. Recommended range: 1–10. The default is 4.|
| `skip_unused_columns`       | bool   | false      | <p>Don't read unused columns from the Delta table.  When set to `true`, this option instructs the connector to avoid reading columns from the Delta table that are not used in any view definitions. To be skipped, the columns must be either nullable or have default values. This can improve ingestion performance, especially for wide tables.</p><p>Note: The simplest way to exclude unused columns is to omit them from the Feldera SQL table declaration. The connector never reads columns that aren't declared in the SQL schema. Additionally, the SQL compiler emits warnings for declared but unused columns—use these as a guide to optimize your schema.</p>|
| `max_concurrent_readers`    | integer| 6          | <p>Maximum number of concurrent object store reads performed by all Delta Lake connectors.</p><p>This setting is used to limit the number of concurrent reads of the object store in a pipeline with a large number of Delta Lake connectors. When multiple connectors are simultaneously reading from the object store, this can lead to transport timeouts.</p><p>When enabled, this setting limits the number of concurrent reads across all connectors. This is a global setting that affects all Delta Lake connectors, and not just the connector where it is specified. It should therefore be used at most once in a pipeline.  If multiple connectors specify this setting, they must all use the same value.</p><p>The default value is 6.</p>|

[*]: Required fields

### Storage parameters

Along with the parameters listed above, there are additional configuration options for
specific storage backends. These can be configured either as connector properties
or as environment variables set inside the pipeline pod. If the same option is specified
in both places, the connector property takes precedence.

<Tabs>
    <TabItem value="s3" label="Amazon S3">
The following configuration options are supported for Amazon S3 object store. See [`object_store` documentation](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html) for additional details.

| Configuration Option                                                      | Environment Variable                     | Description                                                                       |
| ------------------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------- |
| `aws_access_key_id`, `access_key_id`                                      | `AWS_ACCESS_KEY_ID`                      | AWS access key ID for authentication.                                             |
| `aws_secret_access_key`, `secret_access_key`                              | `AWS_SECRET_ACCESS_KEY`                  | AWS secret access key for authentication.                                         |
| `aws_region`, `region`                                                    | `AWS_REGION`                             | AWS default region for operations.                                                |
| `aws_endpoint_url`, `aws_endpoint`, `endpoint_url`, `endpoint`            | `AWS_ENDPOINT`                           | Custom S3 endpoint (e.g., for local or alternative services).                     |
| `aws_session_token`, `aws_token`, `session_token`, `token`                | `AWS_SESSION_TOKEN`                      | Session token for temporary credentials.                                          |
| `aws_skip_signature`, `skip_signature`                                    | `AWS_SKIP_SIGNATURE`                     | If enabled, AmazonS3 will not fetch credentials and will not sign requests. This can be useful when interacting with public S3 buckets that deny authorized requests |
| `aws_imdsv1_fallback`, `imdsv1_fallback`                                  | `AWS_IMDSV1_FALLBACK`                    | Enable automatic fallback to using IMDSv1 if the token endpoint returns a 403 error indicating that IMDSv2 is not supported. |
| `aws_virtual_hosted_style_request`, `virtual_hosted_style_request`        | `AWS_VIRTUAL_HOSTED_STYLE_REQUEST`       | Configured whether virtual hosted style request has to be used. If `"false"` (default), path style request is used, if `"true"`, virtual hosted style request is used. If the endpoint is provided then it should be consistent with virtual_hosted_style_request. i.e. if virtual_hosted_style_request is set to true then endpoint should have bucket name included.|
| `aws_unsigned_payload`, `unsigned_payload`                                | `AWS_UNSIGNED_PAYLOAD`                   | Avoid computing payload checksum when calculating signature.                      |
| `aws_metadata_endpoint`, `metadata_endpoint`                              | `AWS_METADATA_ENDPOINT`                  | Instance metadata endpoint                                                        |
| `aws_container_credentials_relative_uri`, `container_credentials_relative_uri` | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` | Container credentials relative URI when used in ECS                          |
| `aws_container_credentials_full_uri`, `container_credentials_full_uri`    | `AWS_CONTAINER_CREDENTIALS_FULL_URI`     | Container credentials full URI when used in EKS. |
| `aws_container_authorization_token_file`, `container_authorization_token_file` | `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` | Authorization token in plain text when used in EKS to authenticate with `aws_container_credentials_full_uri`|
| `aws_s3_express`, `s3_express`                                            | `AWS_S3_EXPRESS`                         | Enable support for S3 Express One Zone. |
| `aws_request_payer`, `request_payer`                                      | `AWS_REQUEST_PAYER`                      | Enable support for S3 Requester Pays. |
| `aws_allow_http`, `allow_http`                                            | `AWS_ALLOW_HTTP`                         | Set to `"true"` to permit HTTP (insecure) connections to S3.      |
| `aws_server_side_encryption`                                              | `AWS_SERVER_SIDE_ENCRYPTION`             | Type of encryption to use. If set, must be one of `"AES256"` (SSE-S3), `"aws:kms"` (SSE-KMS), `"aws:kms:dsse"` (DSSE-KMS) or `"sse-c"`. |
| `aws_sse_kms_key_id`                                                      | `AWS_SSE_KMS_KEY_ID`                     | The KMS key ID to use for server-side encryption. If set, `aws_server_side_encryption` must be "aws:kms" or "aws:kms:dsse".|
| `aws_sse_bucket_key_enabled`                                              | `AWS_SSE_BUCKET_KEY_ENABLED`             | If set to `"true"`, will use the bucket's default KMS key for server-side encryption. If set to `"false"`, will disable the use of the bucket's default KMS key for server-side encryption.|
| `aws_sse_customer_key_base64`                                             | `AWS_SSE_CUSTOMER_KEY_BASE64`            | The base64 encoded, 256-bit customer encryption key to use for server-side encryption. If set, ServerSideEncryption must be "sse-c".|
    </TabItem>

    <TabItem value="abs" label="Azure Blob Storage">
The following configuration options are supported for Azure Blob Storage object store. See [`object_store` documentation](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html) for additional details.

| Configuration Option                                                      | Environment Variable                     | Description                                                                       |
| ------------------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------- |
| `azure_storage_account_name`, `account_name`                              | `AZURE_STORAGE_ACCOUNT_NAME`             | The name of the azure storage account.                                            |
| `azure_storage_account_key`, `azure_storage_access_key`, `azure_storage_master_key`, `access_key`, `account_key`, `master_key` | `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_ACCESS_KEY` | Master key for accessing storage account.|
| `azure_storage_client_id`, `azure_client_id`, `client_id`                 | `AZURE_STORAGE_CLIENT_ID`                | Service principal client id for authorizing requests.                             |
| `azure_storage_client_secret`, `azure_client_secret`, `client_secret`     | `AZURE_STORAGE_CLIENT_SECRET`            | Service principal client secret for authorizing requests.                         |
| `azure_storage_tenant_id`, `azure_storage_authority_id`, `azure_tenant_id`, `azure_authority_id`, `tenant_id`, `authority_id`| `AZURE_STORAGE_TENANT_ID`| Tenant id used in oauth flows.                 |
| `azure_storage_authority_host`, `azure_authority_host`, `authority_host`  | `AZURE_STORAGE_AUTHORITY_HOST`, `AZURE_AUTHORITY_HOST`| Authority host used in oauth flows.                                  |
| `azure_storage_sas_key`, `azure_storage_sas_token`, `sas_key`, `sas_token`| `AZURE_STORAGE_SAS_KEY`, `AZURE_STORAGE_SAS_TOKEN`| Shared access signature. The signature is expected to be percent-encoded, much like it is provided in the Azure storage explorer or Azure portal.|
| `azure_storage_token`, `bearer_token`, `token`                            | `AZURE_STORAGE_TOKEN`                    | Bearer token.                                                                     |
| `azure_storage_use_emulator`, `object_store_use_emulator`, `use_emulator` | `AZURE_STORAGE_USE_EMULATOR`             | Use object store with azurite storage emulator.                                   |
| `azure_storage_endpoint`, `azure_endpoint`, `endpoint`                    | `AZURE_STORAGE_ENDPOINT`, `AZURE_ENDPOINT` | Override the endpoint used to communicate with blob storage.                    |
| `azure_use_fabric_endpoint`, `use_fabric_endpoint`                        | `AZURE_USE_FABRIC_ENDPOINT`              | Use object store with url scheme `account.dfs.fabric.microsoft.com`               |
| `azure_msi_endpoint`, `azure_identity_endpoint`, `identity_endpoint`, `msi_endpoint`| `AZURE_MSI_ENDPOINT`, `AZURE_IDENTITY_ENDPOINT`| Endpoint to request a IMDS managed identity token.                |
| `azure_object_id`, `object_id`                                            | `AZURE_OBJECT_ID`                        | Object id for use with managed identity authentication.                           |
| `azure_msi_resource_id`, `msi_resource_id`                                | `AZURE_MSI_RESOURCE_ID`                  | Msi resource id for use with managed identity authentication.                     |
| `azure_federated_token_file`, `federated_token_file`                      | `AZURE_FEDERATED_TOKEN_FILE`             | File containing token for Azure AD workload identity federation.                  |
| `azure_skip_signature`, `skip_signature`                                  | `AZURE_SKIP_SIGNATURE`                   | Skip signing requests.                                                            |
| `azure_fabric_token_service_url`, `fabric_token_service_url`              | `AZURE_FABRIC_TOKEN_SERVICE_URL`         | Fabric token service url.                                                         |
| `azure_fabric_workload_host`, `fabric_workload_host`                      | `AZURE_FABRIC_WORKLOAD_HOST`             | Fabric workload host.                                                             |
| `azure_fabric_session_token`, `fabric_session_token`                      | `AZURE_FABRIC_SESSION_TOKEN`             | Fabric session token.                                                             |
| `azure_fabric_cluster_identifier`, `fabric_cluster_identifier`            | `AZURE_FABRIC_CLUSTER_IDENTIFIER`        | Fabric cluster identifier.                                                        |
    </TabItem>

    <TabItem value="gcs" label="Google Cloud Storage">
The following configuration options are supported for Google Cloud Storage object store. See [`object_store` documentation](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html) for additional details.

| Configuration Option                                                      | Environment Variable                     | Description                                                                       |
| ------------------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------- |
| `google_service_account`, `service_account`, `google_service_account_path`, `service_account_path`| `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`| Path to the service account file.           |
| `google_service_account_key`, `service_account_key`                       | `GOOGLE_SERVICE_ACCOUNT_KEY`             | The serialized service account key.                                               |
| `google_application_credentials`                                          | `GOOGLE_APPLICATION_CREDENTIALS`         | Application credentials path.                                                     |
| `google_skip_signature`, `skip_signature`                                 | `GOOGLE_SKIP_SIGNATURE`                  | Skip signing request.                                                             |
    </TabItem>

    <TabItem value="uc" label="Unity Catalog">
Follow Databricks documentation to create and configure a service principal with external access to the target Delta table via the Unity catalog. This usually involves the following steps:
* Enable external data access for the Metastore the Delta table belongs to.
* Create a Databricks service principal.
* Add the service principal to the relevant Databricks workspace.
* [Grant the principal Unity catalog privileges](https://docs.databricks.com/aws/en/external-access/admin#grant-a-principal-unity-catalog-privileges).

To configure Delta Lake connector to use Unity catalog, specify table URI in the following format: `uc://<workspace>.<schema>.<table>`,
along with the following configuration options
(see [`delta-rs` documentation](https://docs.rs/deltalake-catalog-unity/0.12.0/deltalake_catalog_unity/enum.UnityCatalogConfigKey.html) for additional details):

| Configuration Option                                                      | Environment Variable                     | Description                                                                       |
| ------------------------------------------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------- |
| `databricks_host`                                                         | `DATABRICKS_HOST`                        | Host of the Databricks workspace (e.g., `https://dbc-XXX-XXX.cloud.databricks.com`).|
| `unity_token`, `databricks_token`                                         | `UNITY_TOKEN`, `DATABRICKS_TOKEN`        | Access token to authorize API requests.                                           |
| `unity_client_id`, `databricks_client_id`                                 | `UNITY_CLIENT_ID`, `DATABRICKS_CLIENT_ID`| Service principal client id for authorizing requests.                             |
| `unity_client_secret`, `databricks_client_secret`                         | `UNITY_CLIENT_SECRET`, `DATABRICKS_CLIENT_SECRET`| Service principal client secret for authorizing requests.                 |
| `unity_tenant_id`, `databricks_tenant_id`                                 | `UNITY_TENANT_ID`, `DATABRICKS_TENANT_ID`| Authority (tenant) id used in oauth flows.                                        |
| `unity_authority_host`, `databricks_authority_host`                       | `UNITY_AUTHORITY_HOST`, `DATABRICKS_AUTHORITY_HOST`| Authority host used in oauth flows.                                     |
| `unity_msi_endpoint`, `databricks_msi_endpoint`                           | `UNITY_MSI_ENDPOINT`, `DATABRICKS_MSI_ENDPOINT` | Endpoint to request a IMDS managed identity token.                         |
| `unity_object_id`, `databricks_object_id`                                 | `UNITY_OBJECT_ID`, `DATABRICKS_OBJECT_ID`| Object id for use with managed identity authentication.                           |
| `unity_msi_resource_id`, `databricks_msi_resource_id`                     | `UNITY_MSI_RESOURCE_ID`, `DATABRICKS_MSI_RESOURCE_ID`| MSI resource id for use with managed identity authentication.         |
| `unity_federated_token_file`, `databricks_federated_token_file`           | `UNITY_FEDERATED_TOKEN_FILE`, `DATABRICKS_FEDERATED_TOKEN_FILE`| File containing token for Azure AD workload identity federation.|
| `unity_allow_http_url`, `databricks_allow_http_url`                       | `UNITY_ALLOW_HTTP_URL`, `DATABRICKS_ALLOW_HTTP_URL`| Allow http url (e.g. http://localhost:8080/api/2.1/...)                 |
    </TabItem>
</Tabs>

A typical connector config includes `unity_client_id`, `unity_client_secret`,  and `unity_host` options.
You may need to configure additional object store-specific properties,
e.g., you may need to configure `aws_region` when opening a Delta table in S3.

### HTTP client configuration

Additional configuration options to configure HTTP client for remote object stores:

| Configuration Option          | Description                                                                                                                                                    |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `allow_http`                  | Allow non-TLS, i.e. non-HTTPS connections.                                                                                                                     |
| `allow_invalid_certificates`  | Skip certificate validation on https connections. <b>This introduces significant vulnerabilities, and should only be used as a last resort or for testing.</b> |
| `connect_timeout`             | Timeout for only the connect phase of a client. Format: `<number><units>`, e.g., `30s`, `1.5m`.                                                                |
| `http1_only`                  | Only use http1 connections.                                                                                                                                    |
| `http2_only`                  | Only use http2 connections.                                                                                                                                    |
| `http2_keep_alive_interval`   | Interval for HTTP2 Ping frames should be sent to keep a connection alive. Format: `<number><units>`, e.g., `30s`, `1.5m`.                                      |
| `http2_keep_alive_timeout`    | Timeout for receiving an acknowledgement of the keep-alive ping  Format: `<number><units>`, e.g., `30s`, `1.5m`.                                               |
| `http2_keep_alive_while_idle` | Enable HTTP2 keep alive pings for idle connections.                                                                                                            |
| `http2_max_frame_size`        | Maximum frame size to use for HTTP2.                                                                                                                  |
| `pool_idle_timeout`           | The pool max idle timeout. This is the length of time an idle connection will be kept alive. Format: `<number><units>`, e.g., `30s`, `1.5m`.                   |
| `pool_max_idle_per_host`      | Maximum number of idle connections per host.                                                                                                                   |
| `proxy_url`                   | HTTP proxy to use for requests.                                                                                                                                |
| `proxy_ca_certificate`        | PEM-formatted CA certificate for proxy connections.                                                                                                            |
| `proxy_excludes`              | List of hosts that bypass proxy.                                                                                                                               |
| `randomize_addresses`         | Randomize order addresses that the DNS resolution yields. This will spread the connections across more servers.                                                |
| `timeout`                     | Request timeout. The timeout is applied from when the request starts connecting until the response body has finished. Format: `<number><units>`, e.g., `30s`, `1.5m`.|
| `user_agent`                  | User-Agent header to be used by this client.                                                                                                                   |

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
timestamp column of the table declaration.  See our [Time Series Analysis
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
`2024-01-02`, `2024-01-03`, etc., until all records in the table have been ingested.

```sql
CREATE TABLE transaction(
    trans_date_trans_time TIMESTAMP NOT NULL LATENESS INTERVAL 1 day,
    cc_num BIGINT,
    merchant STRING,
    category STRING,
    amt DECIMAL(38, 2),
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

### Example: Setting `timestamp_column`

Create a Delta Lake input connector to read a snapshot of a table from a public S3 bucket, using
`unix_time` as the timestamp column.  The column stores event time in seconds since UNIX epoch
and has lateness equal to 30 days (3600 seconds/hour * 24 hours/day * 30 days).
Note the `aws_skip_signature` flag, required to read from the bucket without authentication,

```sql
CREATE TABLE transaction(
    trans_date_trans_time TIMESTAMP,
    cc_num BIGINT,
    merchant STRING,
    category STRING,
    amt DECIMAL(38, 2),
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

### Example: Using `snapshot_and_follow` mode

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

### Example: Setting AWS credentials

Read a full snapshot of a Delta table using the specified AWS access key. Note that
the `aws_region` parameter is required in this case, because the Delta Lake Rust
library we use does not currently auto-detect the AWS region.

```json
{
  "uri": "s3://feldera-fraud-detection-demo/transaction_train",
  "mode": "snapshot",
  "aws_access_key_id": "<AWS_ACCESS_KEY_ID>",
  "aws_secret_access_key": "<AWS_SECRET_ACCESS_KEY>",
  "aws_region": "us-east-1"
}
```

### Example: Unity catalog

Read table snapshot via Unity catalog.

```json
{
  "uri": "uc://feldera_experimental.default.people_2m",
  "mode": "snapshot",
  "unity_client_id": "<CLIENT_ID>",
  "unity_client_secret": "<CLIENT_SECRET>",
  "unity_host": "https://dbc-XXX-XXX.cloud.databricks.com",
  "aws_region": "us-west-1"
}
```