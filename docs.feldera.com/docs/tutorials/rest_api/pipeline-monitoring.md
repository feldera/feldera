# Pipeline monitoring

A Feldera instance regularly monitors its pipelines and stores these
__pipeline monitor events__ in the internal database.
Only a limited number of monitor events (720) are retained in the database for each pipeline.
With this, it is possible to access both the latest health check of the pipeline
and its health in the recent past. The events are accessible through the API.

## API usage

The pipeline monitor events can be retrieved via two endpoints:

- [**GET /v0/pipelines/[pipeline]/events**](/api/list-pipeline-events):
  retrieves all pipeline monitor events stored in the database, sorted from the latest to the earliest.
  It returns only the status fields to limit its response size, and further individual event
  details can be retrieved via the individual endpoint.

- [**GET /v0/pipelines/[pipeline]/events/[latest|event-id]?selector=[all|status]**](/api/get-pipeline-event):
  retrieves the details of the latest recorded event, or that of a specific event.
  Specify `?selector=all` to retrieve more detailed status information
  reported by the pipeline service itself and of the Kubernetes resources that back it.

## Examples

### All events

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/pipelines/example/events | jq
```

**Response**
```
[
  {
    "id": "019bfa9c-1e45-7ae0-b23b-97928e5a78f2",
    "recorded_at": "2026-01-26T14:01:34.021208Z",
    "resources_status": "Provisioned",
    "resources_desired_status": "Provisioned",
    "runtime_status": "Running",
    "runtime_desired_status": "Running",
    "program_status": "Success",
    "storage_status": "InUse"
  },
  (...)
  {
    "id": "019bfa96-8ec3-7b33-9c6e-e2329f965bd7",
    "recorded_at": "2026-01-26T13:55:29.601071Z",
    "resources_status": "Stopped",
    "resources_desired_status": "Stopped",
    "runtime_status": null,
    "runtime_desired_status": null,
    "program_status": "CompilingSql",
    "storage_status": "Cleared"
  },
  {
    "id": "019bfa96-8eaa-7683-b8b4-193773aa1033",
    "recorded_at": "2026-01-26T13:55:29.565222Z",
    "resources_status": "Stopped",
    "resources_desired_status": "Stopped",
    "runtime_status": null,
    "runtime_desired_status": null,
    "program_status": "Pending",
    "storage_status": "Cleared"
  }
]

```

### Latest event

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/pipelines/example/events/latest | jq
```

**Response**
```
{
  "id": "019bfa9c-1e45-7ae0-b23b-97928e5a78f2",
  "recorded_at": "2026-01-26T14:01:34.021208Z",
  "resources_status": "Provisioned",
  "resources_desired_status": "Provisioned",
  "runtime_status": "Running",
  "runtime_desired_status": "Running",
  "program_status": "Success",
  "storage_status": "InUse"
}
```

### Specific event with all details

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/pipelines/example/events/019bfa9c-1e45-7ae0-b23b-97928e5a78f2?selector=all | jq
```

**Response**
```
{
  "id": "019bfa9c-1e45-7ae0-b23b-97928e5a78f2",
  "recorded_at": "2026-01-26T14:01:34.021208Z",
  "resources_status": "Provisioned",
  "resources_status_details": "(...)",
  "resources_desired_status": "Provisioned",
  "runtime_status": "Running",
  "runtime_status_details": "(...)",
  "runtime_desired_status": "Running",
  "program_status": "Success",
  "storage_status": "InUse"
}
```

... with `"(...)"` representing JSON status explanations (omitted for brevity).
