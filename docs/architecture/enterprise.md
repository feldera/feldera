---
title: Feldera Enterprise Edition
sidebar_label: Enterprise Edition
---

Feldera Enterprise brings all the power of our incremental compute platform into your own Kubernetes cluster (cloud or on-prem). It’s designed for production Pipelines that need resource efficiency, isolation and resilience. Below is a high-level summary of the architecture and how it works.


<div style={{ textAlign: 'center' }}>
<img
  src="/img/enterprise-architecture.png"
  alt="Feldera Enterprise Architecture"
  style={{ width: '70%', maxWidth: '800px' }}
/>
</div>

---

## Features, at a glance

- **One Pipeline = One Pod**  
  Each Pipeline lives in its own Kubernetes Pod, resource- and fault-isolated from other pipelines and the control plane. Each Pipeline can use a **Persistent Volume** or **S3 bucket** for its storage.

- **Fine-Grained Resource & Security Controls**  
   In your Pipeline settings, you declare the CPU, memory, and storage resources (and storage class) you want to use, which are enforced by the control plane. For cloud permissions, use [Kubernetes Secrets](/connectors/secret-references), [Service Accounts](https://kubernetes.io/docs/concepts/security/service-accounts/), and [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html), so each Pipeline has exactly the **least-privilege** permissions it needs and nothing more.

- **Fault-tolerance and Crash Recovery**  
  Pipelines can checkpoint their connector and dataflow state to a PersistentVolume or S3. If a node rrunning a Pipeline goes down or the Pipeline Pod gets evictede, the control plane restarts the computation automatically and it resumes from where it left off.

- **Suspend, Resume and Dynamic Resizing**  
  Dynamically reconfigure pipelines to use different resource reservations fordifferent phases. For example, reserve higher CPU and memory resources for the historical backfill, and fewer resources for processing a lower rate of live-traffic in steady state.

- **Sidecar Extensions**  
  Want a custom connector, a log shipper, or an external API client alongside your Pipeline? Just declare a sidecar container for the Pipeline. It starts and stops in lockstep with the Pipeline within the same Pod.

- **Metrics and Logs**  
  The control-plane and Pipelines expose Prometheus-compatible metrics and logs. Plug your Feldera cluster straight into your existing Prometheus/Grafana or Datadog dashboards without extra agents.

## Next Steps

   Follow the [installation guide](/get-started/enterprise/) to deploy Feldera into your Kubernetes cluster.
