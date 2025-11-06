# Troubleshooting

This guide covers issues Feldera Enterprise users and operators might run into in production, and steps to remedy them.

## Common Error Messages

### Delta Lake Connection Errors

**Error**: `Table metadata is invalid: Number of checkpoint files '0' is not equal to number of checkpoint metadata parts 'None'`

**Solution**: This usually happens when the Delta Table uses features unsupported by `delta-rs` like liquid clustering or deletion vectors. Check the table properties and set the checkpoint policy to "classic":

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'checkpointPolicy' = 'classic'
)
```

### Out-of-Memory Errors

**Error**: `The pipeline container has restarted. This was likely caused by an Out-Of-Memory (OOM) crash.`

Feldera runs each pipeline in a separate container with configurable memory limits. Here are some knobs to
control memory usage:

1. Adjust the pipeline’s memory reservation and limit:
   ```json
   "resources": {
       "memory_mb_min": 32000,
       "memory_mb_max": 32000
   }
   ```

2. Throttle the amount of records buffered by the connector using the [`max_queued_records`](https://docs.feldera.com/connectors/#generic-attributes) setting:
   ```json
   "max_queued_records": 100000
   ```

3. Ensure that storage is enabled (it's on by default):
   ```json
   "storage": {
       "backend": {
         "name": "default"
       },
       "min_storage_bytes": null,
       "compression": "default",
       "cache_mib": null
   },
   ```
4. Optimize your SQL queries to avoid expensive
   cross-products. Use functions like
   [NOW()](https://docs.feldera.com/sql/datetime/#now) sparingly on large relations.

### Out-of-storage Errors

**Error**: The pipeline logs contain messages like:
```
DBSP error: runtime error: One or more worker threads terminated unexpectedly
worker thread 0 panicked
panic message: called `Result::unwrap()` on an `Err` value: StdIo(StorageFull)
```

**Solution**: Increase pipeline storage capacity

In the Enterprise edition, Feldera runs each pipeline in a separate pod and, by
default, attaches PVC volumes for storage. The default volume size is 30 GB,
and if your pipelines are encountering `StorageFull` errors, you should
explicitly request larger volumes for each pipeline:

   ```json
   "resources": {
       "storage_mb_max": 128000
   }
   ```

### Kubernetes evictions

**Error**: the pipeline becomes `UNAVAILABLE` with no errors in the logs.

**Solution**: configure resource reservations and limits for the Pipeline.

Kubernetes may evict Pipeline pods under node resource pressure. To confirm, run:
    ```
    kubectl describe pipeline-<pipeline-id>-0
    ```

and look for
```
Status: Failed
Reason: Evicted
```
You can also view the eviction event in your cluster monitoring stack (e.g. Datadog).

Evictions typically happen only when running Feldera in shared Kubernetes
clusters. The pods to evict are determined by [Kubernetes Quality-of-Service
classes](https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod/).

By default, Feldera Pipelines do not reserve any CPU or memory resources, which
puts them in the [`BestEffort`](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#besteffort) priority class,
making them eviction candidates. To raise their priority:

1. [`Burstable`](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#burstable) class: reserve a minimum amount of memory and CPU:

   ```json
   "resources": {
       "cpu_cores_min": 16,
       "memory_mb_min": 32000,
   }
   ```

2. [`Guaranteed`](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#guaranteed) class: set minimum and maximum resources to the same value, for memory and CPU:

   ```json
   "resources": {
       "cpu_cores_min": 16,
       "cpu_cores_max": 16,
       "memory_mb_min": 32000,
       "memory_mb_max": 32000,
   }
   ```

### Rust Compilation Errors

**Error**: `No space left on device` during Rust compilation

**Solution**: Ensure the compiler-server has sufficient disk space (20Gib by default, configured via the `compilerPvcStorageSize` value in the Helm chart).

## Diagnosing Performance Issues

When investigating pipeline performance, Feldera support will typically request a support-bundle.  The bundle can be downloaded from your installation with the following:

* The `support-bundle` [fda command](/interface/cli):

 ```bash
 fda support-bundle affected-pipeline-name
 ```
* the `support_bundle` [function in the Python SDK](https://docs.feldera.com/python/examples.html#retrieve-a-support-bundle-for-a-pipeline).
* or the `support_bundle` [endpoint in the REST API](/api/download-support-bundle).

The support bundle contains the following content:

1. **Pipeline Logs**: for warnings and errors from the [logs](/api/stream-pipeline-logs) endpoint.

2. **Pipeline Configuration**: the [pipeline configuration](/api/get-pipeline), including the SQL code and connector settings.

3. **Pipeline Metrics**: from the [pipeline metrics](/api/get-pipeline-metrics) endpoint.

3. **Endpoint Stats**: from the [stats](/api/get-pipeline-stats) endpoint.

4. **Circuit Profile**: from the [circuit profile](/api/get-performance-profile) endpoint.

5. **Heap Profile**: from [heap usage](/api/get-heap-profile) endpoint.
