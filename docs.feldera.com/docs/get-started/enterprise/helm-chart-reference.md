# Helm Chart Reference

This page documents all configurable values for the Feldera Enterprise Helm chart.
See the [Helm guide](./helm-guide) for installation instructions.

## Required Values

### Version
| Key | Description |
|-----|-------------|
| `felderaVersion` | Feldera image tag to deploy |

### Feldera License
Feldera Enterprise requires a valid account ID and license key. Contact `learnmore@feldera.com` to obtain one. Users may supply either the `felderaAccountId` and `felderaLicenseKey` or provide the `cloudSecretRef` that contains `account-id` and `license-key` data keys.

| Key | Description |
|-----|-------------|
| `felderaAccountId` | Feldera Account ID |
| `felderaLicenseKey`| Feldera License Key |
| `cloudSecretRef` | Name of an existing Kubernetes secret containing `account-id` and `license-key` data keys. When set, `felderaAccountId` and `felderaLicenseKey` are ignored. |


---

## Image Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `felderaImageRepository` | `"public.ecr.aws/feldera"` | Image registry for Feldera components. |
| `imagePullPolicy` | `"IfNotPresent"` | Kubernetes image pull policy for all Feldera images. |
| `imagePullSecret` | `null` | Name of the Kubernetes secret for pulling images. Leave `null` for public registries. |

---

## Database

Feldera uses PostgreSQL for persistent state. By default the chart deploys its own PostgreSQL instance.

| Key | Default | Description |
|-----|---------|-------------|
| `postgresExternal` | `false` | Set to `true` to connect to an existing external PostgreSQL instance instead of deploying one. |
| `felderaDatabaseSecretRef` | `""` | Secret name for database credentials. When `postgresExternal` is `false`, the secret must have `.user` and `.password` keys. When `true`, it must have a `.connection_url` key. The insecure default secret `<release-name>-db-insecure-secret` is provided for testing only — **do not use in production**. |
| `postgresVersion` | `"16.0"` | PostgreSQL image tag (only used when `postgresExternal` is `false`). |
| `postgresImageRepository` | `"postgres"` | PostgreSQL image repository. |
| `postgresImagePullPolicy` | `"IfNotPresent"` | Image pull policy for PostgreSQL. |
| `postgresPort` | `5431` | Port of the internal PostgreSQL service. |

### External PostgreSQL TLS

When `postgresExternal` is `true`, TLS can be configured for the database connection. If `postgresTlsConfigMapRef` is not set, the system CA bundle is used.

| Key | Default | Description |
|-----|---------|-------------|
| `postgresTlsConfigMapRef` | *(unset)* | Name of a ConfigMap containing the PostgreSQL CA certificate, mounted at `/config/`. Create with: `kubectl create configmap postgres-pem -n feldera --from-file=postgresql-ca.pem=postgresql-ca.pem` |
| `postgresTlsCertificateFile` | `"postgresql-ca.pem"` | Filename of the CA certificate within the ConfigMap. |
| `postgresTlsHostnameVerify` | `true` | Set to `false` to disable hostname verification for the TLS connection. |

---

## Kubernetes Runner

The Kubernetes runner manages pipeline pod lifecycle.

| Key | Default | Description |
|-----|---------|-------------|
| `kubernetesRunner.enablePerPipelineNamespace` | `false` | When `true`, pipelines can run in their own namespaces (requires `ClusterRole`/`ClusterRoleBinding`). When `false`, pipelines run in the same namespace as Feldera. |
| `kubernetesRunner.allowCustomPipelineTemplate` | `false` | **Experimental.** Allow pipelines to reference a ConfigMap containing a custom StatefulSet YAML template. The template format may change incompatibly across Feldera upgrades. |
| `kubernetesRunner.pipelineStatefulSetLabels` | `[]` | Labels added to pipeline StatefulSet `metadata.labels`. Format: `["key=value"]`. |
| `kubernetesRunner.pipelineStatefulSetPodLabels` | `[]` | Labels added to pipeline StatefulSet `spec.template.metadata.labels`. Format: `["key=value"]`. |

### Pipeline Resource Limits

These values set cluster-wide bounds for pipeline resource requests. See [pipeline configuration](/pipelines/configuration) for per-pipeline resource settings.

| Key | Default | Description |
|-----|---------|-------------|
| `kubernetesRunner.lowerLimitMemoryMegabyte` | `100` | Minimum allowed memory (MB) for a pipeline pod. Also the default minimum if none is specified in pipeline resources. |
| `kubernetesRunner.upperLimitMemoryMegabyte` | `32000` | Maximum allowed memory (MB) for a pipeline pod. Also the default maximum if none is specified. |
| `kubernetesRunner.defaultStorageMegabyte` | `30000` | Default persistent storage volume size (MB) per pipeline if not specified in pipeline resources. |
| `kubernetesRunner.upperLimitStorageMegabyte` | `128000` | Maximum allowed persistent storage volume size (MB) per pipeline. |
| `kubernetesRunner.lowerLimitEphemeralStorageMegabyte` | `500` | Minimum ephemeral storage (MB) for a pipeline pod. Must be large enough to hold the pipeline executable. Also the default. |
| `kubernetesRunner.upperLimitEphemeralStorageMegabyte` | `2000` | Maximum ephemeral storage (MB) for a pipeline pod. |

### Pipeline Storage Class

| Key | Default | Description |
|-----|---------|-------------|
| `kubernetesRunner.storageClassName` | `null` | Storage class for pipeline persistent volumes. `null` uses the cluster default. |
| `kubernetesRunner.enableStorageClassNamesSupported` | `false` | When `true`, only storage classes listed in `storageClassNamesSupported` can be used. |
| `kubernetesRunner.storageClassNamesSupported` | `[]` | Allowlist of permitted storage class names. Only effective when `enableStorageClassNamesSupported` is `true`. |

### Connector Kubernetes Secrets

Controls which Kubernetes secrets can be mounted in connectors via [secret references](/connectors/secret-references).

| Key | Default | Description |
|-----|---------|-------------|
| `kubernetesRunner.connectorKubernetesSecrets.enableAllowlist` | `false` | When `true`, only secrets listed in `allowlist` can be referenced in connector configuration. When `false`, any secret in the namespace can be referenced. |
| `kubernetesRunner.connectorKubernetesSecrets.allowlist` | `[]` | List of Kubernetes secret names that connectors are permitted to reference. |

---

## Compiler

| Key | Default | Description |
|-----|---------|-------------|
| `compilerPvcStorageSize` | `20Gi` | Size of the persistent volume used by the compiler server for build artifacts. |
| `compilationProfile` | `"optimized"` | Default compilation profile. `"optimized"` produces faster pipelines at the cost of longer compile times. See [pipeline configuration](/pipelines/configuration) for available profiles. |

---

## Parallel Compilation

Parallel compilation runs multiple compiler server replicas to reduce total compile time. See the [parallel compilation guide](./parallel-compilation) for additional details.

| Key | Default | Description |
|-----|---------|-------------|
| `parallelCompilation.enabled` | `false` | Enable multiple compiler server replicas. When `false`, only one replica is used regardless of `replicas`. |
| `parallelCompilation.replicas` | `1` | Number of compiler server pods when parallel compilation is enabled. |

### sccache (Optional)

When running multiple compiler replicas, sccache allows them to share compiled artifacts via an S3-compatible backend.

| Key | Default | Description |
|-----|---------|-------------|
| `parallelCompilation.sccache.enabled` | `false` | Enable sccache for sharing compilation artifacts between compiler pods. |
| `parallelCompilation.sccache.s3.bucket` | `"sccache-bucket"` | S3 bucket name for the sccache backend. |
| `parallelCompilation.sccache.s3.region` | `"us-east-1"` | AWS region of the S3 bucket. |
| `parallelCompilation.sccache.s3.keyPrefix` | `"sccache"` | Key prefix for cache objects in the bucket. |
| `parallelCompilation.sccache.s3.useSSL` | `false` | Use HTTPS/TLS for S3 connections. |
| `parallelCompilation.sccache.s3.endpoint` | *(unset)* | Custom S3-compatible server endpoint (e.g., `minio.svc.cluster.local:9000`). Omit for AWS S3. |
| `parallelCompilation.sccache.s3.existingSecret` | *(unset)* | Name of a Kubernetes secret with `access_key_id` and `secret_access_key` keys. Omit if using IRSA. |
| `parallelCompilation.sccache.s3.serverSideEncryption` | *(unset)* | Enable server-side encryption with S3-managed keys (SSE-S3). |

---

## Authentication

When `auth.enabled` is `true`, the API server requires OIDC authentication. See the guides for [AWS Cognito](./authentication/aws-cognito), [Okta](./authentication/okta-sso), or [generic OIDC providers](./authentication/generic-oidc).

| Key | Default | Description |
|-----|---------|-------------|
| `auth.enabled` | `false` | Enable authentication for the API server. |
| `auth.provider` | `"aws-cognito"` | OIDC provider type. Set the appropriate value based on your [supported authentication provider](./authentication) |
| `auth.clientId` | `""` | OIDC application client ID. |
| `auth.issuer` | `""` | OIDC issuer URL. |
| `auth.cognitoLoginUrl` | `""` | Cognito hosted UI login URL (Cognito only). |
| `auth.cognitoLogoutUrl` | `""` | Cognito hosted UI logout URL (Cognito only). |

---

## Authorization

Tenant-based authorization for the API server. Requires `auth.enabled: true`.

| Key | Default | Description |
|-----|---------|-------------|
| `authorization.individualTenant` | `null` (→ `true`) | Each authenticated user gets their own tenant. |
| `authorization.issuerTenant` | `null` (→ `false`) | Assign tenants based on the OIDC token issuer. |
| `authorization.authorizedGroups` | `null` (→ `[]`) | List of OIDC `groups` claim values. A user must belong to at least one group to be authorized. Empty list allows all authenticated users. |
| `authorization.authAudience` | `null` (→ `"feldera-api"`) | Expected audience claim in authentication tokens. |

---

## API Server

| Key | Default | Description |
|-----|---------|-------------|
| `allowedOrigins` | `null` | List of allowed CORS origins. `null` permits all origins. Example: `["https://app.example.com"]`. |
| `posthogTelemetryKey` | `null` | PostHog API key for usage telemetry. No data is collected when `null`. |

### Demos

| Key | Default | Description |
|-----|---------|-------------|
| `demos.showDefault` | `true` | Show the built-in demo pipelines in the UI. |
| `demos.configMapRef` | `null` | Name of a ConfigMap containing custom demo files (mounted at `/etc/demos`). Custom demos are shown before the defaults. |

---

## Pipeline

Default settings applied to all pipeline pods.

| Key | Default | Description |
|-----|---------|-------------|
| `pipeline.serviceAccountName` | `null` | Default Kubernetes service account for pipeline pods. Typically used for IRSA-based cloud authentication. |
| `pipeline.env` | `null` | List of environment variables injected into all pipeline pods. |
| `pipeline.allowInitContainers` | `true` | Allow pipelines to specify init containers (e.g., sidecars) via runtime configuration. See [sidecar containers](/pipelines/sidecar). |
| `pipeline.allowProfiling` | `false` | Grant the `PERFMON` capability to pipeline containers, enabling performance profiling. |

---

## Control Plane

Settings applied to API server, Kubernetes runner, and compiler server pods.

| Key | Default | Description |
|-----|---------|-------------|
| `controlPlane.env` | `null` | List of environment variables injected into all control-plane pods (API server, runner, compiler). |
| `controlPlane.deploymentTimeAnnotation` | `true` | Add a `deployment-time` annotation to StatefulSets/Deployments to force a rollout on every `helm upgrade`. Set to `false` to avoid forced rollouts. |

---

## Resources

Resource requests and optional limits for each control-plane component. Requests are reservations — allocated resources are unavailable to other pods. Limits cap maximum usage.

The defaults require a cluster minimum of **14 CPU** and **22,000 MiB** of memory for the control plane alone; additional capacity is needed for pipeline pods.

| Component | CPU Request | Memory Request |
|-----------|-------------|----------------|
| `resources.postgres` | `"2"` | `"2000Mi"` |
| `resources.apiServer` | `"2"` | `"2000Mi"` |
| `resources.compilerServer` | `"8"` | `"16000Mi"` |
| `resources.kubernetesRunner` | `"2"` | `"2000Mi"` |

Each component accepts optional `limits`:

```yaml
resources:
  compilerServer:
    requests:
      cpu: "8"
      memory: "16000Mi"
    limits:
      cpu: "16"
      memory: "32000Mi"
```

`resources.postgres` is only used when `postgresExternal` is `false`.

---

## Scheduling

Apply node selectors, tolerations, affinity rules, labels, and annotations to control-plane or pipeline pods. `controlPlane` applies to the database, API server, compiler server, and runner.

### Node Selectors

```yaml
nodeSelectors:
  controlPlane:
    - key: "kubernetes.io/arch"
      value: "amd64"
  pipeline:
    - key: "node-type"
      value: "compute"
```

### Tolerations

```yaml
tolerations:
  controlPlane:
    - effect: NoSchedule
      key: "dedicated"
      operator: Equal
      value: "feldera"
  pipeline:
    - effect: NoExecute
      key: "spot"
      operator: Exists
```

### Affinity

```yaml
affinity:
  controlPlane: null   # Any valid Kubernetes affinity object
  pipeline: null
```

### Labels & Annotations

```yaml
labels:
  controlPlane:
    - key: "team"
      value: "data-platform"
  pipeline:
    - key: "team"
      value: "data-platform"

annotations:
  controlPlane: []
  pipeline: []
```

---

## HTTPS / TLS

Configure HTTPS for all Feldera components. See the [HTTPS guide](./https) for certificate generation and setup.

| Key | Default | Description |
|-----|---------|-------------|
| `httpsSecretRef` | `null` | Name of a TLS secret (`kubectl create secret tls ...`) containing `tls.crt` and `tls.key`. When set, HTTPS is enabled for the API server, runner, compiler, and all pipelines. |

---

## Networking

| Key | Default | Description |
|-----|---------|-------------|
| `ipFamilyPolicy` | `null` | IP family policy for all Feldera services. One of `SingleStack`, `PreferDualStack`, `RequireDualStack`. Set to `"PreferDualStack"` to enable IPv6. |
| `enableIPv6` | `false` | Enable IPv6 support. |
| `enableStatefulSetServiceName` | `true` | Specify `serviceName` for StatefulSets. Disable only if required by your cluster configuration. |

---

## Logging

| Key | Default | Description |
|-----|---------|-------------|
| `logJson` | `false` | Emit structured JSON logs from all Feldera components. |

---

## Miscellaneous

| Key | Default | Description |
|-----|---------|-------------|
| `unstableFeatures` | `[]` | List of unstable feature flags to enable. Possible values: `"testing"`, `"runtime_version"`, `"cluster_monitor_resources"`. Do not also set `FELDERA_UNSTABLE_FEATURES` in `controlPlane.env`. |
| `felderaSentryEnabled` | `false` | Send crash reports and logs to Feldera's Sentry installation. |
| `cloudApiEndpoint` | `"https://cloud1.feldera.com"` | Feldera cloud API endpoint used for license verification and runner telemetry. |
