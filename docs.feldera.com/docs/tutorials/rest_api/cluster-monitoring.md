# Cluster monitoring

A Feldera instance regularly monitors its three primary components
(API server, compiler server, and runner) and stores these
__cluster monitor events__ in the internal database.
Only a limited number of cluster monitor events are retained in the database,
notably at most 1000 and with a time limit of 72 hours (whichever comes first).
With this, it is possible to access both the latest health check of the cluster
and its health in the recent past. The events are accessible through the API.

The resources monitoring feature is not yet stabilized, but can already be activated by adding
`cluster_monitor_resources` to the Helm chart `unstableFeatures` array value.

## API usage

The cluster monitor events can be retrieved via two endpoints:

- [**GET /v0/cluster/events**](/api/list-cluster-events):
  retrieves all cluster monitor events stored in the database, sorted from the latest to the earliest.
  It returns only the status fields to limit its response size, and further individual event
  details can be retrieved via the individual endpoint.

- [**GET /v0/cluster/events/[latest|event-id]?selector=[all|status]**](/api/get-cluster-event):
  retrieves the details of the latest recorded event, or that of a specific event.
  Specify `?selector=all` to retrieve more detailed status information for each service
  including a human-readable description of the status reported
  by the services themselves and of the Kubernetes resources that back them.

## Examples

### All events

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/cluster/events | jq
```

**Response**
```
[
  {
    "id": "019afe45-ec1f-7de0-9cd1-3a6a4350b5e9",
    "recorded_at": "2025-12-08T14:03:06.655736Z",
    "all_healthy": true,
    "api_status": "Healthy",
    "compiler_status": "Healthy",
    "runner_status": "Healthy"
  },
  {
    "id": "019afe45-850c-7c82-8271-96a81e843dea",
    "recorded_at": "2025-12-08T14:02:40.268247Z",
    "all_healthy": true,
    "api_status": "Healthy",
    "compiler_status": "Healthy",
    "runner_status": "Healthy"
  },
  (...)
]
```

### Latest event

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/cluster/events/latest | jq
```

**Response**
```
{
  "id": "019afe45-ec1f-7de0-9cd1-3a6a4350b5e9",
  "recorded_at": "2025-12-08T14:03:06.655736Z",
  "all_healthy": true,
  "api_status": "Healthy",
  "compiler_status": "Healthy",
  "runner_status": "Healthy"
}
```

### Specific event with all details

**Request**
```
curl -X GET http://127.0.0.1:8080/v0/cluster/events/019afe45-ec1f-7de0-9cd1-3a6a4350b5e9?selector=all | jq
```

**Response**
```
{
  "id": "019afe45-ec1f-7de0-9cd1-3a6a4350b5e9",
  "recorded_at": "2025-12-08T14:03:06.655736Z",
  "all_healthy": true,
  "api_status": "Healthy",
  "api_self_info": "(...)",
  "api_resources_info": "(...)",
  "compiler_status": "Healthy",
  "compiler_self_info": "(...)",
  "compiler_resources_info": "(...)",
  "runner_status": "Healthy",
  "runner_self_info": "(...)",
  "runner_resources_info": "(...)"
}
```

... with `"(...)"` representing human-readable status explanations (omitted for brevity).
