---
title: JSON logging
sidebar_position: 25
---

# Logging (JSON)

This page documents the structured JSON log format emitted when the `FELDERA_LOG_JSON` environment variable is set (for example, `FELDERA_LOG_JSON=1`). The default pretty text logs are unchanged. Logs come from two sources:

- Control plane components (manager, runner, compiler-server, kubernetes-runner, control-plane): these carry `feldera-service` set to the component name.
- Pipelines (the dataflow processes): these carry `feldera-service: "pipeline"` and `pipeline-name`/`pipeline-id`. Control plane logs that are about a specific pipeline also include the pipeline identifiers.

## Identity fields

These identity fields are lifted alongside the standard top-level metadata (`timestamp`, `level`, `target`):

| Field              | Meaning                                                                          |
| ------------------ | -------------------------------------------------------------------------------- |
| `feldera-service`  | Source: `manager` \| `runner` \| `compiler-server` \| `kubernetes-runner` \| `api-server` \| `control-plane` \| `pipeline` (auto-tagged by module path). This identifies which Feldera component produced the log. |
| `pipeline-name`    | Human-friendly pipeline name when available; if it is not immediately available it is set to `N/A`. Present for pipeline events. Control plane events tied to a specific pipeline also include this name. |
| `pipeline-id`      | Pipeline UUID when the event relates to a specific pipeline. Present for pipeline events. Control plane events tied to a specific pipeline also include this ID. |

## JSON object members (spec)

Each log entry is a JSON object whose members are:

- `timestamp` (required): string value, UTC with microsecond precision (e.g. `2025-12-06T02:08:14.902292Z`).
- `level` (required): string value, one of `TRACE` \| `DEBUG` \| `INFO` \| `WARN` \| `ERROR`.
- `target` (required): string value, Rust module path of the log source.
- `fields` (required): object value containing the event payload (one of `message` or `line`).
- `feldera-service` (optional): string value, present for control plane events and for pipelines (as `pipeline`).
- `pipeline-name` (optional): string value, present when the event is tied to a pipeline.
- `pipeline-id` (optional): string value, present when the event is tied to a pipeline.

> Practical rule: every log line has `timestamp`, `level`, `target`, and `fields`. Control plane logs add `feldera-service`; pipeline-related logs add `feldera-service: "pipeline"` plus `pipeline-name` and `pipeline-id`.

## SQL shadow compilation

:::warning

Shadow compilation is experimental. Its options and log events can change or be removed.

:::

To compare SQL compiler results, enable the `sql_shadow_compiler`
[unstable feature](/get-started/enterprise/helm-chart-reference#miscellaneous) and start the
compiler server with `--shadow-sql-compiler java`. Shadow compilation is off by default.
Without the unstable feature, the server skips each shadow and logs the reason.

| Compiler server option | Type | Default | Description |
|---|---|---|---|
| `--shadow-sql-compiler` | `java`, `hybrid` or `rust-optimizer` | not set (off) | The second compiler. `java` runs the Java compiler again. `hybrid` runs the `sql2dbsp` driver, which supervises the Java compiler. `rust-optimizer` is `hybrid` with the Rust optimizer passes. |
| `--shadow-sql-compiler-timeout-secs` | integer, at least 1 | `30` | Deadline of one shadow compilation. |
| `--retain-sql-shadow-mismatches` | flag | off | Keep mismatch bundles. Only for `rust-optimizer`. |

Each server runs at most one shadow at a time, after a stored pipeline compilation.
The Java heap limit is 512 MiB. In `rust-optimizer` mode, the Rust passes use
native memory through JNI outside this heap limit. Without an explicit Java heap limit,
the native side reached 2.3 GiB peak RSS for a 500 KB SQL program. On Linux,
the supervised worker has a 1 GiB address-space limit.

The server removes temporary output after the compiler process group exits.
If cleanup fails, the next shadow retries it. The `skipped` counter includes
`dropped_busy`, `skipped_sql_too_large` (over 1 MiB), and primary observations
over 4 MiB. The `skipped_observation_too_large` counter includes both primary and
shadow observations over 4 MiB. Completion events report `completed`, `compared`,
`equal`, `different`, `timed_out`, and `failed_to_start`. The deadline defaults
to 30 seconds. Set `--shadow-sql-compiler-timeout-secs` to a positive value to
change it.

Each mismatch event identifies the first difference in `difference_class`.
A warning identifies each distinct system failure. These logs omit SQL text
and compiler diagnostics. For each compiler package and UTC day, a stored
pipeline and its SQL text can each qualify only once for `matched_distinct`.

| `message` | Level | Fields |
|---|---|---|
| `SQL shadow skipped` | `INFO` | `reason` and the counters `skipped`, `dropped_busy`, `skipped_sql_too_large`, `skipped_observation_too_large`. |
| `SQL shadow completed` | `INFO` | `outcome` (`Equal`, `Different`, `TimedOut` or `Failed`), `failure_class` for `Failed`, and the counters `completed`, `compared`, `equal`, `different`, `timed_out`, `failed_to_start`, `skipped_observation_too_large`. |
| `SQL shadow mismatch` | `INFO` | `difference_class`: `Verdict`, `Diagnostics`, `Schema`, `Dataflow`, `CircuitIr` or `ProgramInfo`. |
| `Rust optimizer daily shadow sample` | `INFO` | `date`, `compiler_build`, `package_sha256`, `matched_distinct`, `differences`, `difference_this_attempt`, `incomplete`. Written after each `rust-optimizer` shadow. |
| `SQL shadow compiler failed. ...` | `WARN` | `program`, `cause`, and one of `io_error`, `exit_code`, `signal`. Written once for each distinct cause. |
| `SQL shadow cleanup failed. ...` | `WARN` | `kind` and `directory`. |
| `SQL shadow cleanup recovered. ...` | `INFO` | none |

To keep local evidence for investigating a Rust optimizer mismatch, enable
`--retain-sql-shadow-mismatches` alongside `--shadow-sql-compiler rust-optimizer`.
Each difference then writes a private JSON file under
`<compiler-working-directory>/sql-shadow-mismatches/`. The
server keeps the latest 32 bundles. Each file records the compiler manager
build, compiler package digest when available, runtime, engine, platform
compiler override and first differing output.
Within the remaining shadow deadline, the platform compiler's `--anonymize`
option renames identifiers and removes connector properties. Its output can
contain SQL literals. Administrators can inspect a bundle locally and export
it manually. If anonymization fails, the bundle contains only review metadata.
The daily counter records the difference even if bundle creation or cleanup fails.
Turning retention off stops new writes. Previously saved bundles stay on disk
until an administrator removes them or a later retained mismatch rotates them out.

## Examples

Manager pipeline lifecycle:

```json
{
  "timestamp": "2025-12-05T18:54:07.231095Z",
  "level": "INFO",
  "target": "pipeline_manager::api::endpoints::pipeline_management",
  "feldera-service": "manager",
  "pipeline-name": "MyPipeline",
  "pipeline-id": "019af011-5282-7751-98c2-f61478d0df63",
  "fields": {
    "message": "Created pipeline \"MyPipeline\" (019af011-5282-7751-98c2-f61478d0df63) (tenant: 00000000-0000-0000-0000-000000000000)"
  }
}
```

Runner log stream starting up:

```json
{
  "timestamp": "2025-12-06T02:08:14.902292Z",
  "level": "INFO",
  "target": "pipeline_manager::runner::pipeline_logs",
  "feldera-service": "runner",
  "pipeline-name": "N/A",
  "pipeline-id": "019af16a-ba26-7933-a6e5-65d9d717cb7a",
  "fields": { "line": "Fresh start of pipeline logs" }
}
```

Compiler server log:

```json
{
  "timestamp": "2025-12-05T18:01:22.996459Z",
  "level": "INFO",
  "target": "pipeline_manager::compiler::sql_compiler",
  "feldera-service": "compiler-server",
  "pipeline-name": "MyPipeline",
  "pipeline-id": "019aefac-fc78-75b0-9089-0f7496f8ac1f",
  "fields": { "message": "SQL compilation started: pipeline 019aefac-fc78-75b0-9089-0f7496f8ac1f (program version: 1)" }
}
```

Pipeline log:

```json
{
  "timestamp": "2025-12-09T21:26:17.362514Z",
  "level": "INFO",
  "target": "dbsp_connectors::server",
  "feldera-service": "pipeline",
  "pipeline-name": "MyPipeline",
  "pipeline-id": "019af16a-ba26-7933-a6e5-65d9d717cb7a",
  "fields": { "message": "Pipeline initialization complete" }
}
```

## Example: enabling JSON

```bash
FELDERA_LOG_JSON=1 cargo run --bin=pipeline-manager
```

Helm:

```bash
helm upgrade --install feldera <chart> --namespace feldera --create-namespace --set logJson=true
```

## Notes

- Plain-text logging remains the default; JSON is opt-in via `FELDERA_LOG_JSON`.
- The event payload lives under `fields` (`message` or `line`); identity fields (`feldera-service`, `pipeline-name`, `pipeline-id`) are at the top level.
