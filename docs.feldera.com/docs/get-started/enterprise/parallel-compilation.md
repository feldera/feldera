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
Autoscaling based on workload is not yet supported. You must set the number of compiler server replicas at install time or scale them manually later.
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

## Troubleshooting & FAQs

- **Resource requirements:**

Ensure your cluster nodes have enough resources to run the desired number of compiler server replicas.

- **Pipeline stuck on some status:**

If a pipeline is assigned to a worker pod that is not yet running or is unhealthy, it will not be compiled until that pod is available and running. Make sure to validate all pods are running.

- **SystemError: Failed to upload binary:**

If the pipeline gets this status, that means pod N failed to upload its binary to `<_>-compiler-server-0`.

You can check if `<_>-compiler-server-0` is healthy or not by `/cluster_healthz` endpoint. Make sure to adjust binary upload related configuration as per your needs, e.g. if your _upgrade_ takes a while, we should configure retries and backoff interval to sane values such that pods get time to come up to receive the binary.

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

---
