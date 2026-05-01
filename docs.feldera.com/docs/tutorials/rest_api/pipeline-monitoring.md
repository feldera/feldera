# Pipeline monitoring

A Feldera instance regularly monitors its pipelines and stores these
__pipeline monitor events__ in the internal database.
Only a limited number of monitor events (by default, approximately 720) are retained in the database for each pipeline.
With this, it is possible to access both the latest health check of the pipeline
and its health in the recent past. The events are accessible through the API.

## API usage

The pipeline monitor events can be retrieved via two endpoints:

- [**GET /v0/pipelines/[pipeline]/events?selector=[all|status]**](/api/list-pipeline-events):
  retrieves all pipeline monitor events stored in the database, sorted from the latest to the earliest.

- [**GET /v0/pipelines/[pipeline]/events/[latest|event-id]?selector=[all|status]**](/api/get-pipeline-event):
  retrieves the latest recorded event, or that of a specific event.

For either endpoint, specify `?selector=all` to retrieve more detailed status information
reported by the pipeline service itself and by the Kubernetes resources that back it.
To limit response size, it is recommended to retrieve the status of all events, and then
get further details using the individual endpoint as needed.

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
    "event_id": "019dd514-9fbd-7a40-804c-9f9165007fc3",
    "recorded_at": "2026-04-28T17:13:11.868182Z",
    "deployment_resources_status": "Provisioned",
    "deployment_resources_desired_status": "Provisioned",
    "deployment_runtime_status": "Running",
    "deployment_runtime_desired_status": "Running",
    "deployment_has_error": false,
    "program_status": "Success",
    "storage_status": "InUse"
  },
  (...)
  {
    "event_id": "019dd510-b013-7be0-8bfe-61e3e98b7938",
    "recorded_at": "2026-04-28T17:08:53.905286Z",
    "deployment_resources_status": "Stopped",
    "deployment_resources_desired_status": "Stopped",
    "deployment_runtime_status": null,
    "deployment_runtime_desired_status": null,
    "deployment_has_error": false,
    "program_status": "CompilingSql",
    "storage_status": "Cleared"
  },
  {
    "event_id": "019dd510-af58-7ea3-aeb1-61d74adafbeb",
    "recorded_at": "2026-04-28T17:08:53.716611Z",
    "deployment_resources_status": "Stopped",
    "deployment_resources_desired_status": "Stopped",
    "deployment_runtime_status": null,
    "deployment_runtime_desired_status": null,
    "deployment_has_error": false,
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
  "event_id": "019dd514-9fbd-7a40-804c-9f9165007fc3",
  "recorded_at": "2026-04-28T17:13:11.868182Z",
  "deployment_resources_status": "Provisioned",
  "deployment_resources_desired_status": "Provisioned",
  "deployment_runtime_status": "Running",
  "deployment_runtime_desired_status": "Running",
  "deployment_has_error": false,
  "program_status": "Success",
  "storage_status": "InUse"
}
```

### Specific event with all details

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/pipelines/example/events/019dd514-9fbd-7a40-804c-9f9165007fc3?selector=all | jq
```

**Response**
```
{
  "event_id": "019dd514-9fbd-7a40-804c-9f9165007fc3",
  "recorded_at": "2026-04-28T17:13:11.868182Z",
  "deployment_resources_status": "Provisioned",
  "deployment_resources_status_details": (JSON value),
  "deployment_resources_desired_status": "Provisioned",
  "deployment_runtime_status": "Running",
  "deployment_runtime_status_details": (JSON value),
  "deployment_runtime_desired_status": "Running",
  "deployment_has_error": false,
  "deployment_error": null,
  "program_status": "Success",
  "storage_status": "InUse",
  "storage_status_details": (JSON value)
}
```

... with `(JSON value)` representing JSON status details (omitted for brevity)
which can be any JSON value (e.g., string, object, etc.).
