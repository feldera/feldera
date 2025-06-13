# Synchronizing checkpoints to object store

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
        "start_from_checkpoint": true
      }
    }
  }
}
```

### `sync` configuration fields

| Field                     | Type      | Description                                                                                                                        |
| ------------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`                | `string`  | The S3-compatible object store endpoint (e.g., for MinIO, AWS).                                                                    |
| `bucket`\*                | `string`  | The bucket name and optional prefix to store checkpoints (e.g., `mybucket/checkpoints`).                                           |
| `provider`\*              | `string`  | The S3 provider identifier. Must match [rclone's list](https://rclone.org/s3/#providers). Case-sensitive. Use `"Other"` if unsure. |
| `access_key`              | `string`  | Your S3 access key. Not required if using environment-based authentication (e.g., IRSA).                                           |
| `secret_key`              | `string`  | Your S3 secret key. Same rules as `access_key`.                                                                                    |
| `start_from_checkpoint`\* | `boolean` | If `true`, Feldera will restore the latest checkpoint from the object store on startup.                                            |

## IRSA

To use **IRSA** (IAM Roles for Service Accounts) **omit** fields `access_key`
and `secret_key`. This loads credentials from the environment.

## triggering a checkpoint sync

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
