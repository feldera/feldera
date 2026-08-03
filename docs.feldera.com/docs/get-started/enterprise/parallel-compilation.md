# Parallel Compilation

Parallel compilation allows Feldera to compile multiple pipelines concurrently by distributing the workload across several compiler server pods. This dramatically reduces total compile time for large numbers of pipelines, especially in production environments.

---

## How Parallel Compilation Works

Feldera deploys the compiler server as a Kubernetes StatefulSet with **N** replicas. Each replica (pod) acts as a worker responsible for compiling a subset of pipelines. The assignment is deterministic:

- Each pipeline is assigned to a worker using:
  `pipeline_id % N == worker_id`
  The worker ID is the pod index (e.g., `feldera-compiler-server-1` has worker ID `1`).
- Each pod compiles only the pipelines assigned to it.
- **Worker 0** (the pod with index 0) acts as the leader. All other workers transfer their compiled binaries to the leader over HTTP/HTTPS before marking the program as successfully compiled.

To further accelerate builds, Feldera optionally supports [sccache](https://github.com/mozilla/sccache) with an S3-compatible backend. This allows workers to share compiled operator artifacts instead of rebuilding identical code.

:::info
Workload-based autoscaling is available as an experimental feature; see [Autoscaling (experimental)](#autoscaling-experimental). Without it, you set the number of compiler server replicas at install time or scale them manually later.
:::

---

## Configuration

### Enabling Parallel Compilation

To enable parallel compilation with 3 compiler server replicas:

**Via file `values.yaml`**

```yaml
parallelCompilation:
  # Enable parallel compilation features
  enabled: true
  # Number of compiler server replicas when parallel compilation is enabled
  replicas: 3
  # ...other configuration
```

**In Helm command**

```bash
helm upgrade --install feldera \
  oci://public.ecr.aws/feldera/feldera-chart --version "${VERSION}" \
  --namespace feldera \
  --set parallelCompilation.enabled=true \
  --set parallelCompilation.replicas=3 \
  # ...other configuration
```

You should see multiple compiler server pods, for example:

```bash
kubectl get pods -n feldera
```

```
NAME                          READY   STATUS    RESTARTS   AGE
feldera-compiler-server-0     1/1     Running   0          2m
feldera-compiler-server-1     1/1     Running   0          2m
feldera-compiler-server-2     1/1     Running   0          2m
feldera-api-server-xxx        1/1     Running   0          2m
feldera-kubernetes-runner-xxx 1/1     Running   0          2m
feldera-db-0                  1/1     Running   0          2m
```

---

### Setting Up sccache (Optional, Recommended)

Compiler server has all dependcies percompiled in it's target directory. We only need to perform compilation of the program generated based on the pipeline SQL.

If there is just 1 compiler server,  sccache does not provide any benefit as dependencies are already there and as all pipelines are compiled in same workspace, the operators and other compiled artifacts are shared.

When there are multiple compiler server, that is when we want to make sure operators compiled on a server are reusable by others, and sccache achieves that.

Example:
Pipeline A uses operator `xx` and is assigned to pod 0. Pod 0 builds `xx`. Later pipeline B, assigned to pod 1, also needs `xx`. Without sccache, pod 1 rebuilds `xx` from scratch. With sccache (S3/MinIO backend), pod 1 fetches the cached object files, avoiding a full rebuild.

**1. Provision S3 Credentials**

Use either IRSA (IAM Roles for Service Accounts) or a Kubernetes secret with S3 credentials. sccache uses these credentials to access the cache bucket.

- **IRSA**: The compiler server checks for `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`.
- **Kubernetes Secret**: Create a secret containing your S3 access keys:

  ```bash
  kubectl create secret generic sccache-s3-secret -n feldera \
    --from-literal=access_key_id="your-access-key" \
    --from-literal=secret_access_key="your-secret-key"
  ```
The secret must define keys `access_key_id` and `secret_access_key`. You can configure the secret name in `values.yaml`.


**2. Configure sccache in `values.yaml`**

```yaml
parallelCompilation:
  enabled: true
  replicas: 3
  # sccache configuration for sharing compilation artifacts between compiler servers
  sccache:
    # Enable sccache for compilation artifact caching (optional, recommended)
    enabled: true
    # S3 backend configuration for sccache
    s3:
      # S3 bucket name for cache storage
      bucket: "sccache-bucket"
      # Use SSL for S3 connections
      # set to true to use HTTPS/TLS
      useSSL: false
      # Key prefix for cache objects used by sccache
      keyPrefix: "sccache"
      # AWS region of bucket
      region: "us-east-1"
      # custom URL ( <ip>:<port> ) of a server you want to use, such as MinIO.
      # Defaults to ${BUCKET}.s3-{REGION}.amazonaws.com for AWS S3 if not set.
      # endpoint: "minio.extra.svc.cluster.local:9000"
      #
      # Server-side encryption (optional)
      # serverSideEncryption: false
      #
      # Existing secret containing S3 credentials
      # The secret must have keys: access_key_id and secret_access_key
      # If IRSA is setup, you don't need to specify existingSecret,
      # credentials would be configured via AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE
      # environment variables automatically.
      # existingSecret: "sccache-s3-secret"
```


---

## Autoscaling (experimental)

Compiler autoscaling scales the compiler server StatefulSet between 0 and N replicas so that idle deployments stop paying for compiler nodes. The kubernetes-runner drives the scaling:

- Every `pollIntervalSeconds` the runner counts pipelines that need compilation. A pipeline counts when its deployment resources are stopped and its program status is `Pending`, `CompilingSql`, `SqlCompiled`, or `CompilingRust`.
- When the count is greater than zero, the runner scales the StatefulSet to N. N is `parallelCompilation.replicas` when parallel compilation is enabled, otherwise 1.
- After `idleTimeoutSeconds` without pending compilation work, the runner scales the StatefulSet to zero.

A small always-on artifact server Deployment (`<release>-compiler-artifact-server`) owns the binary store and serves compiled pipeline binaries and SQL program validation. The `<release>-compiler-server-0` service routes to it, so the api-server, the runner, and pipeline pods keep working while the compiler workers are scaled to zero. All compiler workers, including worker 0, upload their binaries to the artifact server.

### Configuration Defaults

```yaml
compilerAutoscaling:
  # Experimental: scale the compiler server StatefulSet to zero when idle.
  enabled: false
  # Seconds without pending compilation work before scaling to zero.
  idleTimeoutSeconds: 1800
  # Seconds between checks for pending compilation work.
  pollIntervalSeconds: 10
  artifactServer:
    httpWorkers: 2
    # Budget roughly 200 to 300 MB per optimized program version.
    pvcSize: 50Gi
    # The artifact server also serves SQL program validation (a JVM); the CPU
    # limit lets the JVM size its heap.
    resources:
      requests:
        cpu: "1"
        memory: 2000Mi
      limits:
        cpu: "1"
        memory: 2000Mi
    # Seed the artifact store once from the compiler-server-0 PVC of an
    # existing installation. Keep false on fresh installs.
    seedFromCompilerServer0: false
```

### Latency Expectations

A compilation submitted while the compiler workers are scaled to zero waits for the full cold start: the autoscaler poll (up to `pollIntervalSeconds`), node provisioning if the cluster must add compiler nodes, image pull, and compiler server startup including precompiled dependency extraction. The pipeline shows `Pending` during this time; that is expected, not a stall. Compilations submitted while workers are already up behave exactly as without autoscaling.

Starting or restarting a pipeline whose binary is already compiled does not wake the compiler workers; the artifact server serves the stored binary directly.

### Enabling on an Existing Installation

Toggling the feature changes the StatefulSet `podManagementPolicy`, an immutable field. A pre-upgrade hook (`compilerAutoscaling.kubectlImage`) handles that automatically: on the one upgrade that flips the policy it deletes the compiler StatefulSet, and the same upgrade recreates it with the new policy while the retained per-replica volumes re-attach. The hook renders only on such transition upgrades; steady-state upgrades and installations that never toggle the feature run no hook at all. The hook image must be reachable from your cluster (mirror it for air-gapped installations).

1. Upgrade to images that support autoscaling, with `compilerAutoscaling.enabled` still `false`. Older images do not understand the new flags and would crash loop.
2. Run the enabling upgrade with `compilerAutoscaling.enabled=true` and `compilerAutoscaling.artifactServer.seedFromCompilerServer0=true`. While the seed setting is true the chart omits the compiler StatefulSet and frees its ReadWriteOnce `compiler-storage-<release>-compiler-server-0` volume; the artifact server then copies the compiled binaries from that volume into the artifact store, so existing pipelines keep their binaries. No compilation runs during this window.
3. Wait until the artifact server pod is `Running` (the copy happens in its init container).
4. Run a follow-up upgrade with `seedFromCompilerServer0` back to `false`. This recreates the compiler StatefulSet and detaches the old volume from the artifact server.

To enable without preserving existing binaries, skip the seeding: a single upgrade with `compilerAutoscaling.enabled=true` suffices. The artifact store then starts empty: stopped pipelines recompile on their next start, but a running pipeline whose pod restarts cannot fetch its binary until its program is recompiled (stop and start it once). Stop running pipelines first if that is not acceptable.

Fresh installations need no procedure: set `compilerAutoscaling.enabled=true` at install time.

### Disabling

Without preserving binaries compiled while autoscaling was enabled, disabling is a single upgrade; stopped pipelines recompile on their next start:

1. Run `helm upgrade` with `compilerAutoscaling.enabled=false`. The pre-upgrade hook recreates the StatefulSet and helm restores the configured replica count. The artifact store PVC carries `helm.sh/resource-policy: keep`, so it survives disabling and is re-adopted if you re-enable later.
2. Verify with `kubectl get statefulset <release>-compiler-server -n <namespace>` that the replica count matches your configuration.

To preserve the binaries (required if pipelines are running and must survive pod restarts without a recompile), copy them back before the upgrade:

1. Wait until the compiler workers are parked at zero replicas (or scale the StatefulSet to zero), then scale the artifact server down so both ReadWriteOnce volumes are free:

   ```bash
   kubectl scale deployment <release>-compiler-artifact-server -n <namespace> --replicas=0
   ```

2. Run a one-shot pod that mounts both the `<release>-compiler-artifact-server` and the `compiler-storage-<release>-compiler-server-0` PVCs and copies `rust-compilation/pipeline-binaries` across (the mirror image of the seed init container).
3. Run `helm upgrade` with `compilerAutoscaling.enabled=false` and verify the replica count as above.

If the runner starts with autoscaling disabled and finds the compiler StatefulSet scaled to zero (for example after a disable that raced the old autoscaler), it patches the StatefulSet to 1 replica so compilation never stays wedged; the next `helm upgrade` restores the configured count.

:::warning
Avoid `helm upgrade --force` while autoscaling is enabled: it replaces the StatefulSet, which resets `spec.replicas` and restarts compiler pods; the autoscaler restores the correct count within one poll interval, but in-flight compilations restart. Do not `helm rollback` across the enable boundary either: the rollback tries to revert the immutable `podManagementPolicy` field and fails on the StatefulSet, leaving the release half rolled back. To return to a pre-autoscaling revision, follow the Disabling procedure instead.
:::

### Autoscaling Troubleshooting

- **`/cluster_healthz` reports `scaled_to_zero`:**

While the compiler workers are parked at zero, the health endpoint reports the compiler section as healthy with a `"scaled_to_zero": true` marker. This is the intended idle state, not a failure.

- **`/cluster_healthz` reports the compiler not ready during scale-up:**

During a 0 to N cold start the compiler section reports not ready together with a note that autoscaling is active; this resolves once the compiler pods pass their startup probes. Only a not-ready state that persists well beyond the expected cold start indicates a real problem (for example unschedulable pods or exhausted quota).

- **`SCALING DETECTED` restarts during transitions:**

Compiler pods that observe a replica change exit with `SCALING DETECTED` and restart with the new worker count. During 0 to N and N to 0 transitions these restarts are expected and bounded; the pods converge as soon as the transition completes.

- **`/cluster_healthz` reports the compiler unhealthy with a storage message:**

The compiler health check fails once the binary store filesystem is 95% full, before uploads start failing with `No space left on device`. Grow the artifact server PVC (the storage class must support volume expansion) or delete unused pipelines so the garbage collector reclaims their binaries. Budget roughly 200 to 300 MB per optimized program version when sizing `compilerAutoscaling.artifactServer.pvcSize`.

- **Compilers never scale down:**

A compilation that never reaches a terminal status keeps demand pending and keeps the workers up. The typical cause is a compile that is OOM-killed on every attempt: the pipeline cycles between `SqlCompiled` and `CompilingRust` forever. Give the compiler pods more memory or remove the offending pipeline.

---

## Troubleshooting & FAQs

- **Resource requirements:**

Ensure your cluster nodes have enough resources to run the desired number of compiler server replicas.

- **Pipeline stuck on some status:**

If a pipeline is assigned to a worker pod that is not yet running or is unhealthy, it will not be compiled until that pod is available and running. Make sure to validate all pods are running.

- **Failed to upload binary:**

Upload failures fall into three classes:

  - Transient (network errors, HTTP 5xx): retried with exponential backoff inside the compilation attempt; the default retry settings absorb roughly half an hour of outage, for example a restart of the upload target during an upgrade.
  - Permanent: surface immediately as `SystemError` with the underlying cause and skip the retry budget: an HTTP 4xx rejection (for example a proxy body-size limit), or HTTP 507 when the binary store volume is full (`Insufficient storage on the binary store`).
  - Retry budget exhausted: a retryable failure that outlives the retry budget also ends in `SystemError`.

After fixing the cause (for example growing the artifact store PVC), recompile affected pipelines by editing or re-saving their program. Check `<release>-compiler-server-0` health via the `/cluster_healthz` endpoint; if your upgrades take long, raise the upload retry settings so the upload target has time to come back up before the retry budget runs out.

- **error: process didn't exit successfully: `sccache .. rustc -vV`:**

Check the `Errors` tab in web console ( enable `Verbatim errors` if required ) to check full error regarding why sccache failed.

Comman causes can be misconfigured S3 bucket / endpoint / credentials.

- **Scaling with kubectl:** If you scale the compiler server StatefulSet using `kubectl` without restarting, the compiler server will detect the change and panic with `SCALING DETECTED: StatefulSet has X replicas but compiler was started with Y workers`. This would trigger a restart to ensure correct work distribution.

---

### Configuration Options Reference

| Key | Description | Default/Example |
|-----|-------------|-----------------|
| `parallelCompilation.enabled` | Enable parallel compilation | `false` (ex: `true`) |
| `parallelCompilation.replicas` | Number of compiler server pods | `1` (ex: `3`) |
| `parallelCompilation.sccache.enabled` | Enable sccache build cache | `false` (ex: `true`) |
| `parallelCompilation.sccache.s3.bucket` | S3/MinIO bucket for cache | `"sccache-bucket"` (ex: `"feldera-sccache"`) |
| `parallelCompilation.sccache.s3.useSSL` | Use SSL/TLS for S3/MinIO | `false` (ex: `true`) |
| `parallelCompilation.sccache.s3.region` | Bucket region | `"us-east-1"` (ex: `"us-east-1"`) |
| `parallelCompilation.sccache.s3.keyPrefix` | Cache object key prefix | `"sccache"` (ex: `"sccache"` or `""`) |


**Optional Configurations**
| Key | Description | Example |
|-----|-------------|---------|
| `parallelCompilation.sccache.s3.existingSecret` | Secret for S3 credentials (omit if using IRSA) | `sccache-s3-secret` |
| `parallelCompilation.sccache.s3.serverSideEncryption` | Enable server-side encryption with s3 managed key (SSE-S3) | `false` |
| `parallelCompilation.sccache.s3.endpoint` | Custom endpoint (e.g. MinIO) | `minio.mydomain.com:9000` |

**Autoscaling (experimental)**
| Key | Description | Default/Example |
|-----|-------------|-----------------|
| `compilerAutoscaling.enabled` | Scale the compiler server StatefulSet to zero when idle | `false` (ex: `true`) |
| `compilerAutoscaling.idleTimeoutSeconds` | Seconds without pending compilation work before scaling to zero | `1800` |
| `compilerAutoscaling.pollIntervalSeconds` | Seconds between checks for pending compilation work | `10` |
| `compilerAutoscaling.artifactServer.httpWorkers` | HTTP worker threads of the artifact server | `2` |
| `compilerAutoscaling.artifactServer.pvcSize` | Size of the artifact store volume | `50Gi` |
| `compilerAutoscaling.artifactServer.resources` | Artifact server pod resources | `1` CPU, `2000Mi` memory |
| `compilerAutoscaling.artifactServer.seedFromCompilerServer0` | Seed the artifact store from the compiler-server-0 PVC during the enabling upgrade | `false` (ex: `true`) |

---
