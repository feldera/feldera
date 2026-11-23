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

To compare SQL compiler results, start the compiler server with
`--shadow-sql-compiler java`. Shadow compilation is off by default.
Each server runs at most one shadow at a time. Allow up to about 700 MiB of
additional resident memory (RSS) and one CPU per compiler server.

The server removes temporary compilation output and logs a count and an outcome or skip reason.
It skips a sample if the worker is busy, the SQL exceeds 1 MiB, or the primary
observation exceeds 4 MiB. The default deadline is 30 seconds. Set
`--shadow-sql-compiler-timeout-secs` to change it.

With `--shadow-sql-compiler rust-optimizer`, a difference also writes a private
JSON file under `<compiler-working-directory>/sql-shadow-mismatches/`. The
server keeps the latest 32 bundles. Each file records the compiler manager
build, runtime, engine, platform compiler override and first differing output.
Within the remaining shadow deadline, the platform compiler's `--anonymize`
option renames identifiers and removes connector properties. Its output can
contain SQL literals. Administrators can inspect a bundle locally and export
it manually. If anonymization fails, the bundle contains only review metadata.
The daily counter records the difference even if bundle creation or cleanup fails.

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
