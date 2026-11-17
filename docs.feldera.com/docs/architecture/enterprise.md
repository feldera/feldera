---
title: Feldera Enterprise Edition
sidebar_label: Enterprise Edition
---

Feldera Enterprise runs Feldera in your own Kubernetes clusters, in the cloud or on-prem, so your data never leaves your perimeter. It is built for teams that run many pipelines in production and need them to be isolated, resilient and secure.

<div style={{ textAlign: 'center' }}>
<img
  src="/img/enterprise-architecture.png"
  alt="Feldera Enterprise Architecture"
  style={{ width: '100%' }}
/>
</div>

### Scale-out across a cluster
  All your pipelines can share a cluster of resources. Individual pipelines can also scale out across multiple hosts for intensive backfills. Multi-host pipelines are in [preview](/feature-maturity).

### Multi-tenancy
  One Feldera Enterprise installation serves many teams. Each team works in its own [tenant](/get-started/enterprise/authentication/#tenant-assignment-strategies), and users see and manage only the pipelines in the tenants they are members of. Each membership has a [role](/get-started/enterprise/authentication/roles) that sets what the user can do in that tenant. Together with [pipeline isolation](#pipeline-isolation) and [per-pipeline resource limits](#fine-grained-resource-management), this lets many teams share one cluster safely.

### Pipeline isolation
  Each pipeline runs in its own Kubernetes pod, compute- and fault-isolated from other pipelines and from the control plane. Each pipeline can use a **Persistent Volume** or an **S3 bucket** for its storage.

### Fine-grained resource management
  You can set [resource reservations and limits](/pipelines/configuration#resource-limits) for CPU, memory and storage, for the control plane and for each pipeline. When a pipeline uses a Persistent Volume, it can also specify the storage class. These controls help you enforce SLAs and isolate noisy neighbors in shared clusters.

### Fault tolerance
  Pipelines [checkpoint](/pipelines/fault-tolerance) their connector and operator state to a Persistent Volume or to S3. If a node fails or Kubernetes evicts a pipeline pod, Feldera restarts the pipeline automatically, and the pipeline resumes from where it stopped. The control plane also recovers automatically from cluster failures.

### S3-backed persistence and backfill
  [Checkpoint sync](/pipelines/checkpoint-sync) copies pipeline state to S3, Google Cloud Storage or Azure Blob Storage. A new pipeline can start from this state on a different node or cluster, and a standby pipeline can take over from it.

  When you change a pipeline's SQL, Feldera [bootstraps](/pipelines/modifying#bootstrapping) only the views that changed, and keeps the state of all other views. [Concurrent bootstrapping](/pipelines/modifying#concurrent-bootstrapping) ([preview](/feature-maturity)) lets the pipeline continue to update its existing views while the new and modified views backfill.

### Dynamic resizing
  Use different resources for different phases of a pipeline's lifetime. For example, reserve more CPU and memory for the historical backfill, and fewer resources for live traffic in steady state. You can [resize a running pipeline](/pipelines/configuration#resizing-a-running-pipeline) ([preview](/feature-maturity)) without a restart.

### Custom sidecar extensions
  Add custom connectors, log shippers or external API clients as a [sidecar container](/pipelines/sidecar) next to your pipeline. The sidecar runs in the same pod, and starts and stops with the pipeline.

### Metrics and logs
  The control plane and the pipelines expose [Prometheus-compatible metrics](/operations/metrics) and [structured logs](/operations/json-logging). Connect your Feldera cluster to your Prometheus, Grafana or Datadog dashboards without extra agents.

### Authentication and access control
  Configure [single sign-on](/get-started/enterprise/authentication/) with Okta, AWS Cognito or any OIDC provider. Use [roles](/get-started/enterprise/authentication/roles) in each [tenant](#multi-tenancy) so that users and teams can see and manage only the pipelines that they are authorized to access.

### Least-privilege access
  Feldera integrates with [Kubernetes Secrets](/connectors/secret-references), [Service Accounts](https://kubernetes.io/docs/concepts/security/service-accounts/) and [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html). You can run each pipeline with only the permissions it needs.

### Compliance
  Compliance reports, penetration test results and certifications are available in the [Feldera Trust Center](https://trust.feldera.com).

### Support
  Enterprise customers get a dedicated Slack channel, support SLAs and forward-deployed engineering. Open source users get help from the [Feldera Slack community](https://join.slack.com/t/felderacommunity/shared_invite/zt-3vf3n5dj5-HJuC1DFFY2wE1_AZUyNxqw).

---
:::info
   Feldera Enterprise requires a license. [Contact us](https://www.feldera.com/see-a-demo) to get one, then follow the [installation guide](/get-started/enterprise/) to deploy Feldera into your Kubernetes cluster.
:::
