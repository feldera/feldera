# Delta Lake output connector

[Delta Lake](https://delta.io/) is a popular open table format based on Parquet files.
It is typically used with the [Apache Spark](https://spark.apache.org/) runtime.
Data in a Delta Lake is organized in tables, stored in
a file system or an object stores like [AWS S3](https://aws.amazon.com/s3/),
[Google GCS](https://cloud.google.com/storage), or
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

The Delta Lake output connector does not yet support [fault
tolerance](/pipelines/fault-tolerance).

## Update modes

The connector has two ways of applying a view's changes to a Delta table, chosen with
`update_mode`.

| Mode | What the table holds | Who folds the changes in |
|------|----------------------|--------------------------|
| `cdc` (default) | A change log: every insert and delete, tagged with `__feldera_op` and `__feldera_ts` | You, with a periodic Spark `MERGE INTO` job |
| `merge` | Exactly the view's rows, one per key, no extra columns | The connector |

`cdc` is described in the next section, `merge` in [Merge mode](#merge-mode) below.

## Support for delete operations

This section describes `cdc` mode, the default.

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
incorporate this change log into another Delta table, using the SQL `MERGE INTO` operation. An example of the code is below:

```sql
MERGE INTO {target_table} AS target
        USING (
          SELECT *
          FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                     PARTITION BY {merge_key}
                     ORDER BY __feldera_ts DESC
                   ) AS rn
            FROM {source_table}
            -- Only consider new updates since the last merge.
            WHERE __feldera_ts >= (
              SELECT COALESCE(MAX(__feldera_ts), 0)
              FROM {target_table}
            )
          )
          -- Only apply the last update for each key.
          WHERE rn = 1
        ) AS source
        ON target.{merge_key} = source.{merge_key}

        WHEN MATCHED AND source.__feldera_op = 'd' THEN
          DELETE

        WHEN MATCHED AND source.__feldera_op = 'u' THEN
          UPDATE SET *

        WHEN NOT MATCHED AND source.__feldera_op = 'i' THEN
          INSERT *

        WHEN NOT MATCHED AND source.__feldera_op = 'u' THEN
          INSERT *
```

## Delta Lake output connector configuration

| Parameter  | Description |
|------------|------------|
| `uri`*     | Table URI, e.g., `"s3://feldera-fraud-detection-data/feature_train"`. |
| `mode`*    | Determines how the Delta table connector handles an existing table at the target location. Options: |
|            | - `append`: New updates will be appended to the existing table at the target location. If the table doesn't exist, it will be created. |
|            | - `truncate`: Existing table at the specified location will be truncated on the first pipeline start. When the pipeline resumes from a checkpoint the table is kept as-is so that data written before the restart is preserved. |
|            | - `error_if_exists`: If a table exists at the specified location, the operation will fail. When the pipeline resumes from a checkpoint the existing table is opened without error. |
| `checkpoint_interval` | <p>Checkpoint interval (i.e., the number of commits after which a new checkpoint should be created) for newly created Delta tables.</p><p>The option is only available when creating the Delta table (`mode = append` and there is no existing table at the target location or `mode = truncate`). It configures the `checkpointInterval` table property, which determines the number of commits after which a new checkpoint should be created.</p><p>0 means no checkpoints are created.</p><p>Default: 10.</p>|
| `log_retention_duration` | <p>Log retention duration for newly created Delta tables.</p><p>Configures the `delta.logRetentionDuration` table property, which controls how long the table's transaction-log history is kept.  Each time a checkpoint is written, Delta Lake automatically cleans up log entries older than this interval (subject to `enable_expired_log_cleanup`).</p><p>The option is only available when creating the Delta table (`mode = append` and there is no existing table at the target location, or `mode = truncate`).</p><p>The value follows the Delta Lake interval syntax `"interval <N> <unit>"`, where `<unit>` is one of `nanosecond[s]`, `microsecond[s]`, `millisecond[s]`, `second[s]`, `minute[s]`, `hour[s]`, `day[s]`, or `week[s]`.  Examples: `"interval 30 days"`, `"interval 6 hours"`.</p><p>Default: `"interval 30 days"` (Delta Lake default).</p>|
| `enable_expired_log_cleanup` | <p>Whether to clean up expired log entries when a checkpoint is written.</p><p>Configures the `delta.enableExpiredLogCleanup` table property.  When set to `false`, transaction-log entries are retained indefinitely regardless of `log_retention_duration`.</p><p>The option is only available when creating the Delta table (`mode = append` and there is no existing table at the target location, or `mode = truncate`).</p><p>Default: `true` (Delta Lake default).</p>|
| `max_retries`|<p>Maximum number of retries for failed Delta Lake operations like writing Parquet files and committing transactions.</p><p>The connector performs retries on several levels: individual S3 operations, Delta Lake transaction commits, and overall operation retries. This setting controls the overall operation retries. When a write to the table fails, because of an S3 timeout or any other reason that was not resolved by lower-level retries, the connector will retry the entire operation.</p><p>When not specified, the connector performs infinite retries. When set to 0, the connector doesn't retry failed operations.</p>|
| `threads` | <p>Number of parallel threads used by the connector. Increasing this value can improve Delta Lake write throughput by enabling concurrent writes.</p><p>Values above 1 require the view to have a unique key, so that the connector can order inserts and deletes correctly. Define the key with `CREATE INDEX` and set the connector's `index` property to that index; see [views with unique keys](#views-with-unique-keys) and [writing in parallel](#writing-in-parallel).</p><p>Must be `1` when `update_mode` is `merge`.</p><p>Default: `1`.</p>|
| `variant_encoding` | <p>Encoding of `VARIANT` columns. Options:</p><p>- `variant`: the Delta `variant` type, holding the Parquet variant binary encoding.</p><p>- `json_string`: JSON text in a `string` column.</p><p>See [VARIANT](#variant).</p><p>Default: `variant`.</p>|
| `update_mode` | <p>How the connector applies the view's changes to the table. Orthogonal to `mode`, which governs what happens to an existing table when the pipeline starts.</p><p>- `cdc`: append a change log with `__feldera_op` and `__feldera_ts` metadata columns, which a job of yours folds into a state table.</p><p>- `merge`: keep the table in sync with the view. See [Merge mode](#merge-mode).</p><p>Default: `cdc`.</p>|
| `lookup_chunk_bytes` | <p>Ceiling, in bytes, on the encoded keys the connector holds while locating rows to supersede. `merge` mode only.</p><p>A flush whose key set exceeds this budget is split into successive lookup passes, which bounds memory at the cost of re-scanning candidate files. Default: 256 MiB.</p>|
| `max_concurrent_probes` | <p>Number of data files read concurrently while locating rows to supersede. `merge` mode only.</p><p>Each concurrent read holds one decoded batch, so this bounds memory as well as request concurrency. Default: `4`.</p>|
| `optimize_interval_secs` | <p>Compact the target table from the connector, at most once every this many seconds. `merge` mode only.</p><p>Off by default, because compacting is normally the table administrator's job and an existing `OPTIMIZE` schedule already does the right thing. Set it for tables where Feldera is the only writer and nothing else will compact them. The compaction runs in the background and does not hold up a flush.</p>|

[*]: Required fields

## Merge mode

With `"update_mode": "merge"` the connector keeps the target table equal to the current
contents of the view: one row per key, no metadata columns, no change history. There is no
Spark merge job to run.

It works by never rewriting a data file. A change to a row appends the new version and marks
the old one deleted in a [deletion
vector](https://docs.delta.io/latest/delta-deletion-vectors.html) attached to the file that
holds it, so the cost of a flush is proportional to the change, not to the size of the files
being changed. One flush produces one commit, so a reader never sees half of one.

### Requirements

The connector checks all of these when the pipeline starts, so a misconfiguration fails
before any data moves.

| Requirement | Why |
|-------------|-----|
| The view has a unique key: the `index` property | The key identifies the row to supersede |
| `delta.enableDeletionVectors` is `true` on the table | Without it the connector cannot mark a row deleted |
| The table's schema matches the view's | The table holds the view's rows, so its columns are the view's columns |
| Key columns are scalars, or `ROW` of scalars | See [Key types](#key-types) |
| At least one key column is stored in the data files | A key made entirely of partition columns leaves nothing in the file to read the key from, and would mean one partition directory per row |
| `delta.enableChangeDataFeed` is off | A change data feed needs `_change_data` files that this connector does not write |
| `delta.appendOnly` is off | Superseding a row means removing the old one |
| `threads` is `1` | The connector already reads the table concurrently while locating rows |

Merge mode also assumes the pipeline is the only process writing rows to the table.
Maintenance is fine -- `OPTIMIZE` and `VACUUM` conflict with a flush and the connector
retries -- but a second writer inserting rows is not: the connector locates the row to
supersede in the snapshot it read, and an insert of the same key committed in between would
leave two live rows for one key.

On a table the connector creates, it enables deletion vectors itself. On a table that
already exists it does not, and reports the statement to run:

```sql
ALTER TABLE <table> SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
```

That is deliberate. Enabling deletion vectors raises the table to reader version 3, and any
consumer that cannot read deletion vectors then loses access to the table entirely. Whether
to accept that is the table owner's decision, not the pipeline's.

### Key types

A type may form part of the key only when a value survives the round trip into Parquet and
back still comparing equal to itself, because that comparison is how the connector finds the
row to supersede.

| Type | Usable as a key |
|------|-----------------|
| Scalars | Yes |
| `ROW` of scalars | Yes |
| `ARRAY` | No |
| `MAP`, `VARIANT` | No: two values Feldera considers equal can be stored differently, so the connector could fail to find the row |

For an unsupported type, project a scalar surrogate into the view and index on that.

### What it reads, and what it skips

The connector finds the row to supersede by reading the key columns of the target table. It
narrows that read two ways, both of which it will keep a file for whenever it cannot be
certain:

| Test | Effect |
|------|--------|
| Key range, from the statistics in the Delta log | A file whose key range cannot hold any changed key is skipped without being opened |
| Key range, from each Parquet file's footer | Within a file that is opened, only the row groups whose key range can hold a changed key are read |
| Partition, when partition columns are key columns | A file in a partition none of the changed keys belongs to is skipped without being opened |

A few things make the read wider than it needs to be, all worth knowing when a flush costs
more than expected:

- **A key that is not part of the table's clustering.** If changed keys are spread across the
  whole table, every file's range contains some of them and nothing is skipped. Partitioning
  or `ZORDER`ing the table on the key is what makes pruning effective.
- **The files the connector itself appends.** A flush writes a file holding the new versions
  of whatever keys changed, so if those keys are spread across the key space, that one file's
  key range spans the key space too, and no later flush can prune it. Clustering the table
  therefore only helps for as long as it lasts: every file appended since the last `OPTIMIZE`
  is read by every flush. Measured on a table grown one flush at a time with scattered
  updates, pruning skipped no files at all and each flush read every file in the table. How
  often `OPTIMIZE` runs is what bounds that, which is a second reason to run it on a
  schedule.
- **A `FLOAT` or `DOUBLE` key column.** Parquet and Delta leave `NaN` out of min/max
  statistics, so a range test could skip a file that holds the key. Range pruning is
  therefore switched off for the whole key, and every file is read. Floating point makes a
  poor key in any case; use an exact type.
- **A null key value.** Nulls are left out of min/max statistics too, so a null key sits below
  the recorded range of every file. Range pruning is therefore switched off for any flush that
  changes a null-keyed row, and every file is read. Only that flush is affected; a nullable key
  column that holds no nulls costs nothing.

Statistics only exist for the columns the table asks to index, and the connector collects
exactly the set the table declares: `delta.dataSkippingStatsColumns` when it names one, and
otherwise the first `delta.dataSkippingNumIndexedCols` columns, 32 by default. The count is
over leaf columns, so a nested column early in the schema uses up several positions.

A key column outside that set has no statistics, so its files are always read. The connector
says so at startup, naming the property to set:

```sql
ALTER TABLE <table> SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '<key columns>')
```

It does not set the property itself. What a table collects statistics for is the table
owner's decision, and a connector that widened it silently would write files that disagree
with the table's own declaration.

### Metrics

The connector exports these alongside the standard connector metrics.

| Metric | Meaning |
|--------|---------|
| `delta_merge_tombstone_ratio_permille` | Superseded rows per thousand rows in the table. The signal for when to compact |
| `delta_merge_rows_appended_total`, `delta_merge_rows_superseded_total` | Change volume, split by side |
| `delta_merge_keys_probed_total` | Changed keys whose row had to be located. Below the changed-key count by whatever the insert shortcut saved |
| `delta_merge_keys_not_found_total` | Keys the lookup did not find. Not an error, but a sustained rate means the table has diverged from the view |
| `delta_merge_probe_files_scanned_total`, `delta_merge_probe_files_pruned_total` | Whether file pruning is doing anything |
| `delta_merge_probe_row_groups_scanned_total`, `delta_merge_probe_row_groups_pruned_total` | The same, within the files that are opened |
| `delta_merge_files_appended_total`, `delta_merge_files_dropped_total` | Small-file growth, and the files reclaimed because every row in them was superseded |
| `delta_merge_lookup_passes_total` | Above one per flush only when a key set exceeded `lookup_chunk_bytes` |
| `delta_merge_bytes_written_total` | Bytes written: new data files plus deletion vectors |

### Compaction is required

Every update adds a row and marks one deleted. Without compaction the table grows without
bound, and read cost grows with the number of updates rather than the number of live rows.

Run `OPTIMIZE` on a schedule. Rewriting a file materializes its deletion vector, which every
Delta engine implements, so an existing `OPTIMIZE` schedule already does the right thing. The
connector warns, at most once an hour, when more than 20% of the table's rows are superseded
versions.

If Feldera is the table's only writer and nothing else will compact it, set
`optimize_interval_secs` and the connector runs the compaction itself, in the background, no
more often than that interval. The first one happens one interval after the connector starts,
not at startup, so adopting a large table does not rewrite it immediately. A compaction that
fails leaves the table exactly as it was and is retried at the next interval. Each one starts
after a flush, so a pipeline that stops writing stops compacting, and it replaces files
rather than deleting them -- `VACUUM` still reclaims the space, as described below.

A file whose every row has been superseded is dropped outright rather than kept behind a full
deletion vector, so a delete-heavy workload reclaims some space without `OPTIMIZE`, but an
update-heavy one does not. Writing to the table never rewrites a data file, changes the
schema, changes the partitioning, alters a table property, or upgrades the protocol, so it
does not conflict with that maintenance: if a compaction replaces a file mid-flush, the
connector notices and redoes the flush against the new files.

One file the connector writes needs mentioning. Deletion vectors go at the table root as
`deletion_vector_<uuid>.bin`, which is where Delta Spark writes them and what the protocol
describes. Every flush that tombstones rows writes one, and the vector each touched file
named before is left unreferenced -- so `VACUUM` is required maintenance here, alongside
`OPTIMIZE`, and for the same reason: without it the objects accumulate, one per flush.

`VACUUM` is also what reclaims a data file the connector drops whole, so a merge-mode table
needs it whether or not deletion vectors are in play. A `VACUUM` spares a vector that a live
file still references, because it reads the path out of that file's `add` action, so running
it is safe at any retention the table allows.

One exception worth knowing if you maintain the table with delta-rs, through either the Rust
crate or the `deltalake` Python package: its full-mode `VACUUM` deletes live deletion vectors,
which brings back every row they marked deleted. Full mode lists the table directory and keeps
only paths a live `add` names, and a vector is named inside a descriptor rather than as a path
of its own. Do not pass `full=True`. The default mode is unaffected, and so is Delta Spark's
`VACUUM`, which handles vectors correctly in either mode.

### Flush cost, and how to size it

A flush costs one pass over the table's file list, plus one row-group read for every distinct
file its keys land in. Two things follow, and both matter more than the pipeline's row rate:

| Lever | Why it moves the cost |
|-------|-----------------------|
| Output buffering | Merge mode writes at least one data file per flush, and the pass over the file list is linear in the file count. Buffering cuts the number of flushes, which cuts both terms at once |
| `OPTIMIZE` | Fewer, larger files. The lookup reads one row group of one column, so a file 100x larger costs about 3x more to search -- far less than searching 100 files |

Output buffering is a requirement of merge mode rather than a tuning knob. Measured on a local
table, an unbuffered flush of 16 keys cost 4.5 ms per record where a buffered flush of 1000
keys cost 0.13 ms per record, and the buffered pipeline also grew the file count 60x more
slowly. See [output buffer](/connectors#configuring-the-output-buffer).

On object storage, raise `max_concurrent_probes` above its default of `4`. Each file the
lookup opens is a separate request, and the default is sized for local disk, where the cost is
CPU rather than round trips.

### Limits

Merge mode is not exactly-once, and the connector is still not fault tolerant. After a
restart the connector replays whatever the pipeline replays, and because a Feldera change
states the final value or a deletion per key, re-applying one converges: it marks the row it
previously wrote as deleted and appends an identical one. The cost is one extra superseded
row per replayed row, which the next compaction reclaims.

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

## VARIANT

By default, the connector writes a `VARIANT` column as the Delta `variant` type,
holding the Parquet variant binary encoding. Alternatively, the user can configure
the connector to write `VARIANT` columns as JSON-encoded `string` columns by
setting `"variant_encoding": "json_string"`:

```json
{
  "transport": {
    "name": "delta_table_output",
    "config": {
      "uri": "s3://feldera-fraud-detection-demo/feature_train",
      "variant_encoding": "json_string"
    }
  }
}
```

The `variant` encoding enables the `variantType` table feature on tables the
connector creates. Under the `variant` encoding, three SQL types have no counterpart
and are rejected: long and short `INTERVAL`, and `GEOMETRY`. Unsigned
integers widen to the smallest signed type that holds them, and a `BIGINT
UNSIGNED` too large for `BIGINT` becomes a decimal. SQL `NULL` and a `VARIANT`
null share one encoding.

Appending to a table whose `VARIANT` column is already a `string` requires
`variant_encoding: json_string`. The connector compares the two when it opens
the table and refuses to start on a mismatch, naming the column and the setting
to change.

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

### Streaming incremental updates

Create a Delta Lake output connector that writes a stream of updates to a table
stored in an S3 bucket, truncating any existing contents of the table.

```sql
CREATE VIEW v
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

### Sending a snapshot at startup

Set `send_snapshot: true` to have the connector emit a full snapshot of a
materialized view to the Delta table before streaming incremental updates. This
is useful when downstream consumers need the complete current state of the
view, not just changes since the connector was created.

The snapshot is sent exactly once per connector lifetime. Resuming the
pipeline from a checkpoint does not re-send it. Modifying the connector
triggers a fresh snapshot on the next start, and is delivered even if the
pipeline is started or resumed in `Paused` state.

```sql
CREATE MATERIALIZED VIEW v
WITH (
 'connectors' = '[{
    "name": "delta_sink",
    "send_snapshot": true,
    "transport": {
      "name": "delta_table_output",
      "config": {
        "uri": "s3://my-bucket/my-table",
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

### Writing in parallel

Set `threads` above 1 to have the connector write Parquet files concurrently. Parallel writes require the view to have a
unique key, so that the connector can order inserts and deletes correctly; without one the pipeline fails to start.
Define the key with `CREATE INDEX` and set the connector's `index` property to that index. See
[views with unique keys](#views-with-unique-keys) for additional details.

```sql
CREATE VIEW v
WITH (
 'connectors' = '[{
    "index": "v_idx",
    "transport": {
      "name": "delta_table_output",
      "config": {
        "uri": "s3://feldera-fraud-detection-demo/feature_train",
        "threads": 4,
        "mode": "append",
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

-- Unique key required by "threads": 4.
CREATE INDEX v_idx ON v(id);
```

Note where each option goes: `threads` is a transport option, while `index` is a connector option.
