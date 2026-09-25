# Data Sent to Feldera

Feldera Enterprise sends license checks and pipeline lifecycle events to the Feldera cloud API.

| Channel               | Edition    | Sender            | Destination                           | Enabled when                               |
| --------------------- | ---------- | ----------------- | ------------------------------------- | ------------------------------------------ |
| License check         | Enterprise | Control plane     | `cloud1.feldera.com`                  | Always (Enterprise requires a license key) |
| Pipeline telemetry    | Enterprise | Kubernetes runner | `cloud1.feldera.com`                  | Always                                     |

## License check

The control plane validates the Enterprise license key with an HTTPS request that carries only the
license key:

```http
GET https://cloud1.feldera.com/license
Authorization: Bearer <license key>
```

The server replies with the license status, which confirms validity via time and expiry.

## Pipeline telemetry

Feldera sends pipeline telemetry to measure usage. Pipeline IDs are hashed.

```json
{
  "account_id": "<account ID>",
  "license_key": "<license key>",
  "event": { "PipelineProvisioned": { "id_hash": "<hash of the pipeline ID>" } }
}
```

| Field         | Meaning                                                                   |
| ------------- | ------------------------------------------------------------------------- |
| `account_id`  | The Feldera account that owns the license (Helm value `felderaAccountId`) |
| `license_key` | The Enterprise license key (Helm value `felderaLicenseKey`)               |
| `event`       | One of the events below                                                   |

| Event                      | Sent when                                     | Payload                 |
| -------------------------- | --------------------------------------------- | ----------------------- |
| `PipelineProvisioned`      | A pipeline finishes provisioning              | `id_hash`               |
| `PipelineShutdownFinished` | The runner deletes a pipeline's resources     | `id_hash`               |
| `PipelineStatistics`       | Periodically while a pipeline is provisioned  | `id_hash`, `statistics` |

`id_hash` is the hex-encoded SHA-256 hash of the pipeline ID: it lets Feldera correlate events from
one pipeline without receiving the ID.

### Pipeline statistics

The `global_metrics` object from the [`/stats`](/api/get-pipeline-stats) response is sent to Feldera. Feldera also includes the input and output connector counts.

```json
{
  "PipelineStatistics": {
    "id_hash": "<hash of the pipeline ID>",
    "statistics": {
      "num_inputs": 2,
      "num_outputs": 1,
      "global_metrics": { "state": "Running", "rss_bytes": 1073741824, "...": "..." }
    }
  }
}
```

`global_metrics` holds statistics about the pipeline. The fields depend on the pipeline's Feldera version. The `global_metrics` section of the [`/stats`](/api/get-pipeline-stats) API
reference defines each field.

A statistics event carries no SQL program, table data, pipeline name, or connector configuration.
