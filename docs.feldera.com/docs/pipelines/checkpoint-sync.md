# Synchronizing checkpoints to object store

:::caution Experimental feature
Synchronizing checkpoints to object store is a highly experimental feature.
:::

Feldera allows synchronizing pipeline checkpoints to an object store and
restoring them at startup.

## Storage configuration

Checkpoint sync is supported **only** with the `file` backend and uses
[`rclone`](https://rclone.org/) under the hood to interact with S3-compatible
object stores.

Here is a sample configuration:

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
        "fail_if_no_checkpoint": false,
        "flags": ["--s3-server-side-encryption", "aws:kms"]
      }
    }
  }
}
```

### `sync` configuration fields

| Field                   | Type            | Default     | Description                                                                                                                                                                                                      |
|-------------------------|-----------------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `endpoint`              | `string`        |             | The S3-compatible object store endpoint (e.g., `http://localhost:9000` for MinIO).                                                                                                                               |
| `bucket` \*             | `string`        |             | The bucket name and optional prefix to store checkpoints (e.g., `mybucket/checkpoints`).                                                                                                                         |
| `region`                | `string`        | `us-east-1` | The region of the bucket. Leave empty for MinIO. If `provider` is AWS, and no region is specified, `us-east-1` is used.                                                                                          |
| `provider` \*           | `string`        |             | The S3 provider identifier. Must match [rclone’s list](https://rclone.org/s3/#providers). Case-sensitive. Use `"Other"` if unsure.                                                                               |
| `access_key`            | `string`        |             | S3 access key. Not required if using environment-based auth (e.g., IRSA).                                                                                                                                        |
| `secret_key`            | `string`        |             | S3 secret key. Not required if using environment-based auth.                                                                                                                                                     |
| `start_from_checkpoint` | `string`        |             | Checkpoint UUID to resume from, or `latest` to restore from the latest checkpoint.                                                                                                                               |
| `fail_if_no_checkpoint` | `boolean`       | `false`     | When `true` the pipeline will fail to initialize if fetching the specified checkpoint fails. <p> When `false`, the pipeline will start from scratch instead. Ignored if `start_from_checkpoint` is not set. </p> |
| `transfers`             | `integer (u8)`  | `20`        | Number of concurrent file transfers.                                                                                                                                                                             |
| `checkers`              | `integer (u8)`  | `20`        | Number of parallel checkers for verification.                                                                                                                                                                    |
| `ignore_checksum`       | `boolean`       | `false`     | Skip checksum verification after transfer and only check the file size. Might improve throughput.                                                                                                                |
| `multi_thread_streams`  | `integer (u8)`  | `10`        | Number of streams for multi-threaded downloads.                                                                                                                                                                  |
| `multi_thread_cutoff`   | `string`        | `100M`      | File size threshold to enable multi-threaded downloads (e.g., `100M`, `1G`). Supported suffixes: `k`, `M`, `G`, `T`.                                                                                             |
| `upload_concurrency`    | `integer (u8)`  | `10`        | Number of concurrent chunks to upload during multipart uploads.                                                                                                                                                  |
| `flags`                 | `array[string]` |             | Extra flags to pass to `rclone`.<p> ⚠️ Incorrect or conflicting flags may break behavior. See [rclone flags](https://rclone.org/flags/) and [S3 flags](https://rclone.org/s3/). </p>                             |


*Fields marked with an asterisk are required.

## S3 permissions

The following minimum permissions are required to be available on the bucket
being written to:

- `ListBucket`
- `DeleteBucket`
- `GetObject`
- `PutObject`
- `PutObjectACL`
- `CreateBucket`

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
            "Resource": [
              "arn:aws:s3:::BUCKET_NAME/*",
              "arn:aws:s3:::BUCKET_NAME"
            ]
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

To use **IRSA** (IAM Roles for Service Accounts) **omit** fields `access_key`
and `secret_key`. This loads credentials from the environment.

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

## Triggering a checkpoint sync

A sync operation can be trigged by making a `POST` request to:

```bash
curl -X POST http://localhost/v0/pipelines/{PIPELINE_NAME}/checkpoint/sync
```

This initiates the sync and returns the UUID of the checkpoint being synced:

```json
{"checkpoint_uuid":"019779b4-8760-75f2-bdf0-71b825e63610"}
```

## Checking sync status

The status of the sync operation can be checked by making a `GET` request to:

```bash
curl http://localhost/v0/pipelines/{PIPELINE_NAME}/checkpoint/sync_status
```

### Response examples

**In Progress:**

```json
{"success":null,"failure":null}
```

**Success:**

```json
{"success":"019779b4-8760-75f2-bdf0-71b825e63610","failure":null}
```

**Failure:**

```json
{
  "success": null,
  "failure": {
    "uuid": "019779c1-8317-7a71-bd78-7b971f4a3c43",
    "error": "Error pushing checkpoint to object store: ... SignatureDoesNotMatch ..."
  }
}
```
