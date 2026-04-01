# Checkpoint Sync to Object Store

:::caution Experimental feature
Synchronizing checkpoints to object store is a highly experimental feature.
:::

Feldera can synchronize pipeline checkpoints to an S3-compatible object store
and restore them at startup. This enables disaster recovery scenarios where
local storage may be lost, and supports running standby pipelines that can
take over from a failed primary within seconds.

Key capabilities:

- **Automatic sync** — Periodically push local checkpoints to object store.
- **Standby pipelines** — A secondary pipeline continuously pulls checkpoints
  and activates on demand.
- **Pipeline seeding** — Bootstrap a new pipeline from another pipeline's
  checkpoints via a read-only bucket.

## Getting started

Checkpoint sync uses the `file` storage backend with
[`rclone`](https://rclone.org/) under the hood to interact with S3-compatible
object stores.

Here is a minimal configuration to sync checkpoints to S3:

```json
"storage": {
  "backend": {
    "name": "file",
    "config": {
      "sync": {
        "bucket": "BUCKET_NAME/DIRECTORY_NAME",
        "provider": "AWS",
        "access_key": "ACCESS_KEY",
        "secret_key": "SECRET_KEY",
        "start_from_checkpoint": "latest",
        "push_interval": 120
      }
    }
  }
}
```

This configuration restores from the latest checkpoint on startup (downloading
from S3 if not available locally) and pushes new checkpoints to S3 every 120
seconds.

## Standby mode

Pipelines can start in **standby** mode by setting `standby` to `true`. A
standby pipeline does not process data. Instead, it continuously pulls the
latest checkpoint from object store at the interval specified by
`pull_interval`, staying ready for immediate activation.

To activate a standby pipeline:

```sh
curl -X POST https://{FELDERA_HOST}/v0/pipelines/{PIPELINE_NAME}/activate
```

:::important
A pipeline that was previously activated and still has its storage intact will
auto-activate from the latest local checkpoint. This prevents unintentional
standby behavior if the pipeline is rescheduled or restarted.
:::

Standby mode is useful for keeping a backup pipeline ready to take over if the
primary fails. The following table illustrates a typical failover scenario:

| Time    | Pipeline A (Primary) | Pipeline B (Standby)          |
| ------- | -------------------- | ----------------------------- |
| Step 1  | **Start**            | **Standby Start**             |
| Step 2  | _Processing_         | _Standby_                     |
| Step 3  | _Checkpoint 1_       | _Standby_                     |
| Step 4  | _Sync 1 to S3_       | _Standby_                     |
| Step 5  | _Processing_         | _Pulls Checkpoint 1_          |
| Step 6  | _Checkpoint 2_       | _Standby_                     |
| Step 7  | _Sync 2 to S3_       | _Standby_                     |
| Step 8  | _Processing_         | _Pulls Checkpoint 2_          |
| Step 9  | _Failed_             | _Standby_                     |
| Step 10 |                      | **Activate**                  |
| Step 11 |                      | **Running From Checkpoint 2** |

## Seeding from an existing pipeline

`read_bucket` lets you seed a new pipeline from a read-only checkpoint source
without giving it write access to the source.

For example, suppose pipeline **A** pushes checkpoints to `bucket-a/pipeline-a`
and you want a new pipeline **B** to start from **A**'s latest state without
writing back to **A**'s bucket:

```json
"sync": {
  "bucket": "bucket-b/pipeline-b",
  "read_bucket": "bucket-a/pipeline-a",
  "provider": "AWS",
  "access_key": "ACCESS_KEY",
  "secret_key": "SECRET_KEY",
  "start_from_checkpoint": "latest"
}
```

On its first start **B** pulls from `bucket-a/pipeline-a` (because
`bucket-b/pipeline-b` is empty). All subsequent checkpoints are pushed to
`bucket-b/pipeline-b`. **A**'s bucket is never modified.

`read_bucket` requires only read permissions on the source bucket
(`ListBucket` and `GetObject`). See [S3 permissions](#s3-permissions).

## Configuration reference

### `sync` configuration fields

| Field                   | Type            | Default     | Description                                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------- | --------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`              | `string`        |             | The S3-compatible object store endpoint (e.g., `http://localhost:9000` for MinIO).                                                                                                                                                                                                                                                                                                  |
| `bucket` \*             | `string`        |             | The bucket name and optional prefix to store checkpoints (e.g., `mybucket/checkpoints`).                                                                                                                                                                                                                                                                                            |
| `read_bucket`           | `string`        |             | A read-only fallback bucket used to seed the pipeline when `bucket` has no checkpoint. Uses the same connection settings as `bucket` (`provider`, `access_key`, `secret_key`, `endpoint`, `region`). The pipeline **never writes** to `read_bucket`. Must point to a different location than `bucket`. See [Seeding from an existing pipeline](#seeding-from-an-existing-pipeline). |
| `region`                | `string`        | `us-east-1` | The region of the bucket. Leave empty for MinIO. If `provider` is AWS, and no region is specified, `us-east-1` is used.                                                                                                                                                                                                                                                             |
| `provider` \*           | `string`        |             | The S3 provider identifier. Must match [rclone's list](https://rclone.org/s3/#providers). Case-sensitive. Use `"Other"` if unsure.                                                                                                                                                                                                                                                  |
| `access_key`            | `string`        |             | S3 access key. Not required if using environment-based auth (e.g., IRSA).                                                                                                                                                                                                                                                                                                           |
| `secret_key`            | `string`        |             | S3 secret key. Not required if using environment-based auth.                                                                                                                                                                                                                                                                                                                        |
| `start_from_checkpoint` | `string`        |             | Checkpoint UUID to resume from, or `latest` to restore from the latest checkpoint.                                                                                                                                                                                                                                                                                                  |
| `fail_if_no_checkpoint` | `boolean`       | `false`     | When `true`, the pipeline fails to start if no checkpoint is found in any source (local storage, `bucket`, or `read_bucket`). When `false`, the pipeline starts from scratch instead.                                                                                                                                                                                               |
| `standby`               | `boolean`       | `false`     | When `true`, the pipeline starts in **standby** mode. See [Standby mode](#standby-mode). `start_from_checkpoint` must be set to use standby mode.                                                                                                                                                                                                                                  |
| `pull_interval`         | `integer(u64)`  | `10`        | Interval (in seconds) between fetch attempts for the latest checkpoint while in standby.                                                                                                                                                                                                                                                                                            |
| `push_interval`         | `integer(u64)`  |             | Interval (in seconds) between automatic syncs of a local checkpoint to object store, measured from the completion of the previous sync. Disabled by default. See [Automatic checkpoint synchronization](#automatic-checkpoint-synchronization).                                                                                                                                      |
| `transfers`             | `integer (u8)`  | `20`        | Number of concurrent file transfers.                                                                                                                                                                                                                                                                                                                                                |
| `checkers`              | `integer (u8)`  | `20`        | Number of parallel checkers for verification.                                                                                                                                                                                                                                                                                                                                       |
| `ignore_checksum`       | `boolean`       | `false`     | Skip checksum verification after transfer and only check the file size. May improve throughput.                                                                                                                                                                                                                                                                                     |
| `multi_thread_streams`  | `integer (u8)`  | `10`        | Number of streams for multi-threaded downloads.                                                                                                                                                                                                                                                                                                                                     |
| `multi_thread_cutoff`   | `string`        | `100M`      | File size threshold to enable multi-threaded downloads (e.g., `100M`, `1G`). Supported suffixes: `k`, `M`, `G`, `T`.                                                                                                                                                                                                                                                                |
| `upload_concurrency`    | `integer (u8)`  | `10`        | Number of concurrent chunks to upload during multipart uploads.                                                                                                                                                                                                                                                                                                                     |
| `flags`                 | `array[string]` |             | Extra flags to pass to `rclone`. Incorrect or conflicting flags may break behavior. See [rclone flags](https://rclone.org/flags/) and [S3 flags](https://rclone.org/s3/).                                                                                                                                                                                                           |
| `retention_min_count`   | `integer (u32)` | `10`        | The minimum number of checkpoints to retain in object store. No checkpoints will be deleted if the total count is below this threshold.                                                                                                                                                                                                                                             |
| `retention_min_age`     | `integer (u32)` | `30`        | The minimum age (in days) a checkpoint must reach before it becomes eligible for deletion. All younger checkpoints will be preserved.                                                                                                                                                                                                                                               |

\*Fields marked with an asterisk are required.

## Checkpoint resolution priority

When `start_from_checkpoint` is set, Feldera resolves the checkpoint to restore
from using the following priority order:

1. **Local storage** — if a matching checkpoint (or, for `latest`, any local
   checkpoint) exists on the local file system, it is used immediately; no
   download is performed. This makes restarts fast when the pipeline's local
   state is still intact.
2. **`bucket`** — if no matching checkpoint is found locally, Feldera downloads
   it from the primary read/write bucket.
3. **`read_bucket`** — if the checkpoint is also absent from `bucket`, Feldera
   falls back to `read_bucket` (when configured).
4. **Start fresh or fail** — if no checkpoint is found in any source, behavior
   is controlled by `fail_if_no_checkpoint`.

## S3 permissions

The following minimum permissions are required on the bucket being written to:

- `ListBucket`
- `DeleteObject`
- `GetObject`
- `PutObject`
- `PutObjectACL`

Example policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::USER_SID:user/USER_NAME"
      },
      "Action": [
        "s3:ListBucket",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": ["arn:aws:s3:::BUCKET_NAME/*", "arn:aws:s3:::BUCKET_NAME"]
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListAllMyBuckets",
      "Resource": "arn:aws:s3:::*"
    }
  ]
}
```

For more details, refer to [rclone S3 permissions](https://rclone.org/s3/#s3-permissions).

## IRSA

To use **IRSA** (IAM Roles for Service Accounts), **omit** the `access_key`
and `secret_key` fields. This loads credentials from the environment.

## Automatic checkpoint synchronization

Feldera can automatically synchronize checkpoints to the configured object store
at regular intervals by setting the `push_interval` field in the `sync`
configuration.

When enabled, Feldera periodically pushes the latest local checkpoint to the
object store every `push_interval` seconds after the previous sync operation
completes.

Automatic sync is **disabled by default**.

The status of the most recent automatic sync operation can be queried with:

```bash
curl http://localhost/v0/pipelines/{PIPELINE_NAME}/checkpoint/sync_status | jq '.periodic'
```

It is recommended to set `push_interval` greater than
`fault_tolerance.checkpoint_interval_secs` to avoid syncing more frequently
than checkpoints are created.

### Trigger conditions

An automatic checkpoint synchronization is triggered only when all of the
following conditions are met:

- The configured `push_interval` has elapsed.
- No checkpoint sync is currently in progress.
- Checkpoint sync has not been manually requested.
- A valid checkpoint exists.
- The checkpoint has not already been synced.

## Triggering a checkpoint sync

A sync operation can be triggered by making a `POST` request to:

```bash
curl -X POST http://localhost/v0/pipelines/{PIPELINE_NAME}/checkpoint/sync
```

This initiates the sync and returns the UUID of the checkpoint being synced:

```json
{ "checkpoint_uuid": "019779b4-8760-75f2-bdf0-71b825e63610" }
```

## Checking sync status

The status of the sync operation can be checked with:

```bash
curl http://localhost/v0/pipelines/{PIPELINE_NAME}/checkpoint/sync_status
```

### Response fields

| Field      | Type            | Description                                                                                                  |
| ---------- | --------------- | ------------------------------------------------------------------------------------------------------------ |
| `success`  | `uuid \| null`  | UUID of the most recently successful manually triggered checkpoint sync (`POST /checkpoint/sync`).           |
| `failure`  | `object \| null`| Details of the most recently failed manually triggered checkpoint sync. Contains `uuid` and `error` fields.  |
| `periodic` | `uuid \| null`  | UUID of the most recently successful automatic periodic checkpoint sync (configured via `push_interval`).    |

`success` and `periodic` track different sync mechanisms:
- `success` is updated only by manual syncs triggered via `POST /checkpoint/sync`.
- `periodic` is updated only by automatic syncs configured via `push_interval`.

### Response examples

**No syncs yet:**

```json
{ "success": null, "failure": null, "periodic": null }
```

**Successful manual sync:**

```json
{ "success": "019779b4-8760-75f2-bdf0-71b825e63610", "failure": null, "periodic": null }
```

**Failed manual sync:**

```json
{
  "success": null,
  "failure": {
    "uuid": "019779c1-8317-7a71-bd78-7b971f4a3c43",
    "error": "Error pushing checkpoint to object store: ... SignatureDoesNotMatch ..."
  },
  "periodic": null
}
```

**Automatic periodic sync only (no manual syncs):**

```json
{ "success": null, "failure": null, "periodic": "019779c1-8317-7a71-bd78-7b971f4a3c43" }
```

**Both manual and automatic syncs:**

```json
{
  "success": "019779b4-8760-75f2-bdf0-71b825e63610",
  "failure": null,
  "periodic": "019779c1-8317-7a71-bd78-7b971f4a3c43"
}
```

## Buckets with server side encryption

If the bucket has server side encryption enabled, set the flag
[--s3-server-side-encryption](https://rclone.org/s3/#s3-server-side-encryption)
in the `flags` field.

Example:

```json
"sync": {
  "bucket": "BUCKET_NAME/DIRECTORY_NAME",
  "provider": "AWS",
  "start_from_checkpoint": "latest",
  "flags": ["--s3-server-side-encryption", "aws:kms"]
}
```

## Performance

Sync performance varies based on configuration and environment. The following
average speeds were observed in testing:

| Storage Type | Avg Upload Speed | Avg Download Speed | Avg Download Speed (Ignore Checksum) |
| ------------ | ---------------- | ------------------ | ------------------------------------ |
| GP3          | 650 MiB/s        | 650 MiB/s          | 850 MiB/s                            |
| GP2          | 125 MiB/s        | 125 MiB/s          | 250 MiB/s                            |
| NVMe         | 1.5 GiB/s        | 2.2 GiB/s          | 2.3 GiB/s                            |

- GP3 was configured with throughput of 1000 MB/s, and IOPS of 10,000.
- GP2 has a max throughput of 250 MB/s.
- Both GP2 and GP3 can briefly hit maximum download speeds of around 1 GiB/s, but performance drops off quickly.
- NVMe speeds were tested on an i4i.4xlarge instance type.
- Performance may improve by tuning sync parameters such as `transfers`, `checkers`, `upload_concurrency`, etc.
