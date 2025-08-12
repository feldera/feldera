---
title: Feldera Enterprise Edition
sidebar_label: Enterprise Edition
---

Feldera Enterprise brings all the power of our incremental compute platform into your own Kubernetes cluster (cloud or on-prem). It’s designed for production use by teams that need resource efficiency, isolation and resilience for their Pipelines. Below is a high-level summary of the architecture and the extra features.


<div style={{ textAlign: 'center' }}>
<img
  src="/img/enterprise-architecture.png"
  alt="Feldera Enterprise Architecture"
  style={{ width: '70%', maxWidth: '800px' }}
/>
</div>


### One Pipeline = One Pod
  Each Pipeline runs in its own Kubernetes Pod, resource- and fault-isolated from other pipelines and the control plane. Each Pipeline can use a **Persistent Volume** or **S3 bucket** for its storage.

### Fine-Grained Resource Management
   The control plane and each Pipeline can be configured with resource reservations and limits for CPU, memory, and storage. When using Persistent Volumes for storage, Pipelines can also specify the specific storage class to use. These controls help enforce SLAs and deal with noisy neighbours in shared clusters.

### Least-privilege access and security controls
   Feldera integrates with [Kubernetes Secrets](/connectors/secret-references), [Service Accounts](https://kubernetes.io/docs/concepts/security/service-accounts/), and [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html), so teams can run Pipelines with only the **least-privilege** permissions they need and nothing more. Configure OpenID-based authentication so users and teams can only view and manage pipelines they're authorized to access.

### Fault-tolerance
  Pipelines can checkpoint their connector and operator state to a PersistentVolume or S3. If a node running a Pipeline goes down or the Pipeline Pod gets evicted, Feldera restarts the computation automatically which resumes from where it left off. The control plane itself is resilient and recovers seamlessly from cluster failures.

### Checkpoint, Resume and Dynamic Resizing
  Dynamically reconfigure pipelines to use different resource reservations for different phases. For example, reserve higher CPU and memory resources for the historical backfill, and fewer resources for processing a lower rate of live-traffic in steady state.

### Custom Sidecar Extensions
  Add custom connectors, log shippers, or external API clients by declaring a sidecar container alongside your Pipeline. It runs in the same Pod and shares the same lifecycle, starting and stopping in sync with the Pipeline.

### Metrics and Logs
  The control-plane and Pipelines expose Prometheus-compatible metrics and logs. Plug your Feldera cluster straight into your existing Prometheus/Grafana or Datadog dashboards without extra agents.

---
:::info
   Ready to try? Follow the [installation guide](/get-started/enterprise/) to deploy Feldera into your Kubernetes cluster.
:::
