# Running Pipelines with Sidecar Containers

:::note Enterprise-only feature
This feature is only available in Feldera Enterprise Edition.
:::

It is possible to run a [sidecar container](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)
alongside the main pipeline container in the Enterprise edition.
It can be used for example to ingest data from data sources or using data formats
that are not currently supported by Feldera connectors. The sidecar can ingest
such data, convert it into one of the supported formats (e.g., JSON), and push it
to the pipeline using the pipeline's internal HTTP API (*).

Sidecar containers are configured through the runtime configuration (`runtime_config`)
of the pipeline by setting its `runtime_config.init_containers` field to
the [_PodSpec.initContainers_](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#containers)
Kubernetes specification formatted as JSON.

This is an advanced feature, so please note the following:
- The internal pipeline API is under active development, and as such it might be that
  certain endpoints change backward incompatibly and thus require edits to your
  sidecar implementation when upgrading Feldera.
- Sidecar containers are colocated on the same pod as the pipeline main container,
  and as such are fate-sharing: if the pod goes down, both containers will go down.
- Sidecar containers share (and thus contend for) resources with the pipeline main
  container, such as CPU and memory. If one of them causes the node of the pod to go OOM,
  both of them will go down. It is advised to set resources requests and limits for
  sidecar containers.
- Sidecar containers are currently not tracked or monitored by the Feldera control plane,
  and as such debugging (e.g., retrieving their logs) must be done out-of-band.

:::info
It is possible to disallow sidecar containers by setting `pipeline.allowInitContainers`
(default: `true`) in the Helm chart to `false`. In that case, the pipeline will fail
to start if the user specifies `runtime_config.init_containers`.
:::

:::info
 (*) Documentation for the pipeline's internal HTTP API is not yet available.
For now, it requires reading the Rust source code at:
https://github.com/feldera/feldera/blob/main/crates/adapters/src/server.rs
:::

## Example

In the pipeline `runtime_config.init_containers`, you must provide an array
following the [_PodSpec.initContainers_](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#containers)
Kubernetes specification formatted as JSON (more examples can be found on
[the Kubernetes sidecar containers documentation page](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)):

```
{
    "workers": 8,
    "storage": ...,
    "fault_tolerance": ...,
    "resources": ...,
    "init_containers": [{
        "name": "example",
        "image": "ubuntu:24.04",
        "restartPolicy": "Always",
        "resources": {
            "requests": {
                "memory": "256Mi",
                "cpu": "250m"
            },
            "limits": {
                "memory": "512Mi",
                "cpu": "500m"
            }
        },
        "command": ["bash", "-c", "echo 'Hello, world!'; sleep infinity"]
    }],
    ...
}
```

Check the sidecar logs via:
```bash
kubectl logs -n NAMESPACE pipeline-UUID-0 -c example
```
(replacing NAMESPACE and UUID with the namespace and pipeline identifier respectively)

... which should yield:
```
Hello, world!
```
