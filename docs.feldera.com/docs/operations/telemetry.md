# Data Sent to Feldera

Feldera Enterprise sends license checks and pipeline lifecycle events to the Feldera cloud API.

| Channel               | Edition    | Sender            | Destination                           | Enabled when                               |
| --------------------- | ---------- | ----------------- | ------------------------------------- | ------------------------------------------ |
| License check         | Enterprise | Control plane     | `cloud1.feldera.com`                  | Always (Enterprise requires a license key) |
| Pipeline telemetry    | Enterprise | Kubernetes runner | `cloud1.feldera.com`                  | Always                                     |
| Crash reports         | Enterprise | Control plane     | Feldera's Sentry installation         | `felderaSentryEnabled: true` (default off) |

The Helm value `cloudApiEndpoint` sets the cloud API address; the default is
`https://cloud1.feldera.com`. See the
[Helm chart reference](/get-started/enterprise/helm-chart-reference#miscellaneous) for the value and
[Add Feldera domains to your network](/operations/required-domains) for allowlisting.

## License check

The control plane validates the Enterprise license key with an HTTPS request that carries only the
license key:

```http
GET https://cloud1.feldera.com/license
Authorization: Bearer <license key>
```

The server replies with the license status, which the web console uses to show expiry reminders:

| Field                | Meaning                                                    |
| -------------------- | ---------------------------------------------------------- |
| `current`            | Server time of the response                                |
| `valid_until`        | Expiry time of the license                                 |
| `is_trial`           | Whether the license is a trial                             |
| `description_html`   | Text that describes the benefits of extending or upgrading |
| `extension_url`      | Link to extend or upgrade the license                      |
| `remind_starting_at` | Time from which the web console reminds users of expiry    |
| `remind_schedule`    | How often the web console repeats the reminder             |

## Pipeline telemetry

The Kubernetes runner reports pipeline lifecycle events with a JSON `POST` to
`https://cloud1.feldera.com/telemetry/event`:

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

The Kubernetes runner builds each `PipelineStatistics` event from the pipeline's
[`/stats`](/api/get-pipeline-stats) response. The runner counts the input and output connectors,
copies the `global_metrics` object, and drops the rest of the response, including each connector's
configuration and metrics:

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

## Crash reports

Enterprise deployments can send crash reports and logs to Feldera's Sentry installation. Sentry
reporting is off by default; set the Helm value `felderaSentryEnabled` to `true` to turn it on. See
the [Helm chart reference](/get-started/enterprise/helm-chart-reference#miscellaneous).
