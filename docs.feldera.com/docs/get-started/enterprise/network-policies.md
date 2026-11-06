# Network policies

Pipelines run user programs and connect to external systems. Kubernetes
[NetworkPolicies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
can limit which pods a pipeline can reach, and which pods can reach a
pipeline.

The Feldera Helm chart does not create NetworkPolicies, because the right
rules depend on your cluster: the CNI, where the database runs, which
systems the connectors use, and whether you run multihost pipelines. This
page shows an example to start from.

:::note

NetworkPolicies take effect only if the cluster's network plugin enforces
them. For example, on Amazon EKS with the VPC CNI, enable network policy
support in the CNI. Check the documentation of your CNI.

:::

## Traffic between Feldera components

| From | To | Port (TCP) | Why |
| --- | --- | --- | --- |
| API server, Kubernetes runner | Pipeline | 80 | Control and data API of the pipeline |
| Pipeline | Compiler server | 8085 | Download the pipeline binary and program info |
| Pipeline | Pipeline | 8080, 9000 | Only multihost pipelines: coordinator and data exchange between hosts |
| Pipeline | DNS (`kube-dns` in `kube-system`) | 53 (UDP and TCP) | Name resolution |
| Pipeline | Systems that connectors use | depends on the connector | Kafka, object storage, databases, and other connector endpoints |
| API server, compiler server, Kubernetes runner | Postgres | 5432 | Control plane database |

NetworkPolicies match the port of the pod, not of the Service. The
`<release>-db` Service listens on port 5431 and forwards to port 5432 of
the database pod, so the policies use 5432.

With `compilerAutoscaling.enabled`, pipelines download their binary from
`<release>-compiler-artifact-server` instead of the compiler server, and
that pod also connects to the database. Allow it in the compiler rule of
the `pipeline` policy and in the `db` policy below.

## Example

The example below isolates pipeline pods and the database in namespace
`feldera`. Replace `<release>` with the name of your Helm release. The
selectors use the `feldera.service` label that the chart puts on each
control plane pod, and the `feldera.com/part-of: pipeline` label that the
runner puts on each pipeline pod.

```yaml
# Pipelines: ingress only from the control plane, egress only to DNS, the
# compiler, and the public internet.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pipeline
  namespace: feldera
spec:
  podSelector:
    matchLabels:
      feldera.com/part-of: pipeline
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              feldera.service: <release>-api-server
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
      ports:
        - protocol: TCP
          port: 80
  egress:
    # DNS. Namespace and pod selector are one peer, so both must match.
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # Pipeline binary and program info.
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-compiler-server
      ports:
        - protocol: TCP
          port: 8085
    # Connectors: the public internet, without private, shared, loopback,
    # link-local (instance metadata), multicast, and reserved addresses.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 0.0.0.0/8
              - 10.0.0.0/8
              - 100.64.0.0/10
              - 127.0.0.0/8
              - 169.254.0.0/16
              - 172.16.0.0/12
              - 192.168.0.0/16
              - 224.0.0.0/4
              - 240.0.0.0/4
---
# Database deployed by the chart: ingress only from the control plane.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: db
  namespace: feldera
spec:
  podSelector:
    matchLabels:
      feldera.service: <release>-db
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              feldera.service: <release>-api-server
        - podSelector:
            matchLabels:
              feldera.service: <release>-compiler-server
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
      ports:
        - protocol: TCP
          port: 5432
```

Change the example to fit your deployment:

- **Multihost pipelines.** The hosts of a multihost pipeline must reach
  each other. Add these rules to the `pipeline` policy. A static policy
  cannot limit this to the hosts of one pipeline, so any pipeline can then
  reach any other pipeline on these ports. To limit what a pipeline can do
  with that access, enable [client certificates](https.md#client-certificates).

  ```yaml
  ingress:
    - from:
        - podSelector:
            matchLabels:
              feldera.com/part-of: pipeline
      ports:
        - protocol: TCP
          port: 8080
        - protocol: TCP
          port: 9000
  egress:
    - to:
        - podSelector:
            matchLabels:
              feldera.com/part-of: pipeline
      ports:
        - protocol: TCP
          port: 8080
        - protocol: TCP
          port: 9000
  ```

- **Systems in the cluster or the private network.** The example blocks
  private addresses. If pipelines use systems in your cluster or VPC, add
  an egress rule for each of them. These include connector endpoints,
  such as Kafka, and the object storage for
  [checkpoint sync](../../pipelines/checkpoint-sync.md), such as MinIO.
  This also applies to cloud endpoints with private addresses, such as
  AWS interface VPC endpoints.
- **Pod, Service, or node addresses that are not private.** The `ipBlock`
  rule allows every address that is not private. If the cluster uses
  addresses outside the private ranges for pods, Services, or nodes, such
  as privately used public IP ranges on GKE, add these ranges to `except`.
  Otherwise, pipelines can reach the database and the API server. A public
  endpoint of the Kubernetes API, as on EKS or GKE with public access, is
  also reachable through this rule. Add its addresses to `except` if
  pipelines must not reach it.
- **IPv6.** The `ipBlock` rule allows only IPv4 addresses. On dual-stack
  or IPv6 clusters, pipelines cannot connect to IPv6 addresses. To allow
  them, add an `ipBlock` with `::/0` and an `except` list for the private
  IPv6 ranges of your cluster, such as `fc00::/7` and `fe80::/10`.
- **NodeLocal DNSCache.** With NodeLocal DNSCache, pods send DNS queries
  to a link-local address on the node, usually `169.254.20.10`, instead of
  to the `kube-dns` pods. The DNS rule does not match this traffic, and the
  `ipBlock` rule excludes `169.254.0.0/16`. Allow that address on port 53,
  UDP and TCP.
- **Cloud credentials from link-local addresses.** The example blocks
  `169.254.0.0/16`, which includes the instance metadata service
  (`169.254.169.254`) and the EKS Pod Identity agent (`169.254.170.23`).
  If pipelines get their cloud credentials from one of these, allow that
  address. GKE Workload Identity uses the metadata service. IAM roles for
  service accounts (IRSA) on EKS do not need either.
- **One namespace per pipeline.** With
  `kubernetesRunner.enablePerPipelineNamespace`, pipelines run outside the
  `feldera` namespace. Apply the `pipeline` policy in each pipeline
  namespace, and add a `namespaceSelector` for the `feldera` namespace to
  the rules that select control plane pods.
- **External database.** If the control plane uses an external Postgres
  database, leave out the `db` policy.
- **Default deny.** Some clusters deny all traffic to a pod that no policy
  selects. In that case, also add policies that allow the traffic of the
  API server, the compiler server, and the runner.

## Check the policies

After you apply the policies, start a pipeline and check that it reaches
the running state. Then check one connector of each kind that you use.

Also check that the policies block traffic. If the network plugin does
not enforce NetworkPolicies, all the checks above pass, but the pipelines
are not isolated.

1. From a running pipeline pod, connect to the compiler server, which the
   `pipeline` policy allows:

   ```
   kubectl exec -n feldera <pipeline-pod> -- timeout 3 bash -c '</dev/tcp/<release>-compiler-server-0/8085' \
     && echo "connected" || echo "failed"
   ```

   The output must be `connected`. If it is `failed`, the check itself
   does not work, for example because of a wrong pod name or missing
   permission for `kubectl exec`. Fix that before step 2.

2. From the same pod, connect to the API server, which the `pipeline`
   policy blocks:

   ```
   kubectl exec -n feldera <pipeline-pod> -- timeout 3 bash -c '</dev/tcp/<release>-api-server/8080' \
     && echo "not isolated" || echo "isolated"
   ```

   The output must be `isolated`.
