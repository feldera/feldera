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

NetworkPolicies take effect only if the network plugin of the cluster
enforces them. Configure it to enforce them before you apply the
policies. For example, on Amazon EKS with the VPC CNI, enable network
policy support and set `NETWORK_POLICY_ENFORCING_MODE` to `strict`. In
strict mode, a new pod gets no traffic until its policies apply. Refer to
the documentation of your CNI.

:::

## Traffic between Feldera components

| From | To | Port (TCP) | Why |
| --- | --- | --- | --- |
| Clients, through an ingress controller or a gateway | API server | 8080 | Feldera API and web console |
| API server | Kubernetes runner | 8089 | Runner API |
| API server, Kubernetes runner | Pipeline | 80 | Control and data API of the pipeline |
| Kubernetes runner | API server, compiler server, Kubernetes runner | 8080, 8085, 8089 | Health checks of the control plane |
| Kubernetes runner | Compiler server | 8085 | Check that the pipeline binary exists |
| Pipeline | Compiler server | 8085 | Download the pipeline binary and program info |
| Pipeline | Pipeline | 8080, 9000 | Only multihost pipelines: coordinator and data exchange between hosts |
| Kubernetes runner, compiler server | Kubernetes API | 443 or 6443 | Create pipeline objects, read the compiler StatefulSet |
| API server, Kubernetes runner | License server (`cloudApiEndpoint`) | 443 | License check and telemetry |
| API server, compiler server, Kubernetes runner | Postgres | 5432 (configurable) | Control plane database |
| All pods | DNS (`kube-dns` in `kube-system`) | 53 (UDP and TCP) | Name resolution |
| Pipeline | Systems that connectors use | depends on the connector | Kafka, object storage, databases, and other connector endpoints |

## Example

The example below is the most restrictive set of policies for namespace
`feldera`. A `default-deny` policy blocks all traffic of all pods in the
namespace. The other policies allow only the traffic in the table above,
except connector traffic. Add the connectors of your pipelines as
described in [Fitting the policies to your
deployment](#fitting-the-policies-to-your-deployment).

The selectors use the `feldera.service` label that the chart puts on each
control plane pod, and the `feldera.com/part-of: pipeline` label that the
runner puts on each pipeline pod. Replace these values:

- `<release>`: the name of your Helm release.
- `<gateway-namespace>`: the namespace of your ingress controller or
  gateway.
- `<kubernetes-api-ip>` and `<kubernetes-api-port>`: the address and port
  of the Kubernetes API server. NetworkPolicies match the address of the
  API server, not the address of the `kubernetes` Service. Get it with
  `kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes`.
  If there is more than one address, add an `ipBlock` for each.

```yaml
# Deny all traffic to and from every pod in the namespace. The policies
# below allow only the traffic that Feldera needs.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny
  namespace: feldera
spec:
  podSelector: {}
  policyTypes:
    - Ingress
    - Egress
---
# DNS for all pods.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: dns
  namespace: feldera
spec:
  podSelector: {}
  policyTypes:
    - Egress
  egress:
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
---
# API server: clients and the runner, then the database, the runner,
# pipelines, and the license server.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: api-server
  namespace: feldera
spec:
  podSelector:
    matchLabels:
      feldera.service: <release>-api-server
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Clients. Replace with the namespace of your ingress controller or
    # gateway.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: <gateway-namespace>
      ports:
        - protocol: TCP
          port: 8080
    # Health checks of the runner.
    - from:
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
      ports:
        - protocol: TCP
          port: 8080
  egress:
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-db
      ports:
        - protocol: TCP
          port: 5432
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
      ports:
        - protocol: TCP
          port: 8089
    - to:
        - podSelector:
            matchLabels:
              feldera.com/part-of: pipeline
      ports:
        - protocol: TCP
          port: 80
    # License server (cloudApiEndpoint) and the identity provider,
    # which have public addresses.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
              - 172.16.0.0/12
              - 192.168.0.0/16
              - 100.64.0.0/10
              - 169.254.0.0/16
              - 127.0.0.0/8
      ports:
        - protocol: TCP
          port: 443
---
# Runner: the API server and its own health checks, then the database,
# the compiler, pipelines, the Kubernetes API, and the license server.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kubernetes-runner
  namespace: feldera
spec:
  podSelector:
    matchLabels:
      feldera.service: <release>-kubernetes-runner
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
          port: 8089
  egress:
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-db
      ports:
        - protocol: TCP
          port: 5432
    # Health checks of the API server, the runner itself, and the
    # compiler.
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-api-server
      ports:
        - protocol: TCP
          port: 8080
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
      ports:
        - protocol: TCP
          port: 8089
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-compiler-server
      ports:
        - protocol: TCP
          port: 8085
    - to:
        - podSelector:
            matchLabels:
              feldera.com/part-of: pipeline
      ports:
        - protocol: TCP
          port: 80
    # Kubernetes API.
    - to:
        - ipBlock:
            cidr: <kubernetes-api-ip>/32
      ports:
        - protocol: TCP
          port: <kubernetes-api-port>
    # License server (cloudApiEndpoint), which has a public address.
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
              - 172.16.0.0/12
              - 192.168.0.0/16
              - 100.64.0.0/10
              - 169.254.0.0/16
              - 127.0.0.0/8
      ports:
        - protocol: TCP
          port: 443
---
# Compiler: the runner and pipelines, then the database and the
# Kubernetes API.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: compiler-server
  namespace: feldera
spec:
  podSelector:
    matchLabels:
      feldera.service: <release>-compiler-server
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              feldera.service: <release>-kubernetes-runner
        - podSelector:
            matchLabels:
              feldera.com/part-of: pipeline
      ports:
        - protocol: TCP
          port: 8085
  egress:
    - to:
        - podSelector:
            matchLabels:
              feldera.service: <release>-db
      ports:
        - protocol: TCP
          port: 5432
    # Kubernetes API.
    - to:
        - ipBlock:
            cidr: <kubernetes-api-ip>/32
      ports:
        - protocol: TCP
          port: <kubernetes-api-port>
---
# Database deployed by the chart. Leave it out if you use an external
# database.
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
---
# Pipelines: the control plane and the other hosts of a multihost
# pipeline, then the compiler and the other hosts.
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
              feldera.service: <release>-compiler-server
      ports:
        - protocol: TCP
          port: 8085
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

The `db` policy applies only to the Postgres pod that the chart deploys.
Most installations use an external database instead. In that case, leave
out the `db` policy, and in the other policies, change the rules for
`<release>-db` to an `ipBlock` with the address and the port of your
database. NetworkPolicies match the port of the pod, not of the Service.
The `<release>-db` Service listens on port 5431 and forwards to port 5432
of the database pod, so the policies use 5432.

The license server and the identity provider for [API
authentication](authentication/index.mdx) have public addresses that can
change. NetworkPolicies cannot select them by name, so the example allows
all public addresses on port 443 for the API server and the runner. If
these components connect through a proxy, allow only the proxy instead.

## Fitting the policies to your deployment

- **Connectors.** The `pipeline` policy allows no connector traffic. Add
  an egress rule for each system that pipelines use. These include
  internal connector endpoints. For example, for a Kafka cluster in
  namespace `kafka` and an object store with public addresses:

  ```yaml
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kafka
      ports:
        - protocol: TCP
          port: 9092
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
              - 172.16.0.0/12
              - 192.168.0.0/16
              - 100.64.0.0/10
              - 169.254.0.0/16
              - 127.0.0.0/8
      ports:
        - protocol: TCP
          port: 443
  ```

- **Cloud credentials from link-local addresses.** If pipelines get their
  cloud credentials from the instance metadata service
  (`169.254.169.254`) or from the EKS Pod Identity agent
  (`169.254.170.23`), allow that address in the `pipeline` policy. GKE
  Workload Identity uses the metadata service. IAM roles for service
  accounts (IRSA) on EKS do not need either.
- **NodeLocal DNSCache.** With NodeLocal DNSCache, pods send DNS queries
  to a link-local address on the node, usually `169.254.20.10`, instead of
  to the `kube-dns` pods. Allow that address on port 53, UDP and TCP, in
  the `dns` policy.
- **Compiler autoscaling.** With `compilerAutoscaling.enabled`, pipelines
  and the runner connect to `<release>-compiler-artifact-server` instead
  of the compiler server, and that pod also connects to the database.
  Add `<release>-compiler-artifact-server` wherever the example selects
  `<release>-compiler-server`, and give the artifact server a policy like
  the `compiler-server` policy.
- **One namespace per pipeline.** With
  `kubernetesRunner.enablePerPipelineNamespace`, pipelines run outside the
  `feldera` namespace. Apply the `default-deny`, `dns`, and `pipeline`
  policies in each pipeline namespace. Add a `namespaceSelector` for the
  `feldera` namespace to the rules in the `pipeline` policy that select
  control plane pods, and a `namespaceSelector` for the pipeline
  namespaces to the rules in the control plane policies that select
  pipelines.

## Check the policies

After you apply the policies, do these checks:

1. Start a pipeline and check that it reaches the running state. Then
   check one connector of each kind that you use.

2. Check that the control plane is healthy. The output must be `true`:

   ```
   curl https://<feldera-api>/v0/cluster_healthz | jq .all_healthy
   ```

   The health checks run every few minutes. If the runner cannot reach
   the other components, pipelines can still run, but this check fails.

3. Check that the policies block traffic. If the network plugin does not
   enforce NetworkPolicies, the checks above pass, but the pipelines are
   not isolated. From a running pipeline pod, connect to the compiler
   server, which the `pipeline` policy allows:

   ```
   kubectl exec -n feldera <pipeline-pod> -- timeout 3 bash -c '</dev/tcp/<release>-compiler-server-0/8085' \
     && echo "connected" || echo "failed"
   ```

   The output must be `connected`. If it is `failed`, the check itself
   does not work, for example because of a wrong pod name or missing
   permission for `kubectl exec`. Fix that before you continue.

4. From the same pod, connect to the API server, which the `pipeline`
   policy blocks:

   ```
   kubectl exec -n feldera <pipeline-pod> -- timeout 3 bash -c '</dev/tcp/<release>-api-server/8080' \
     && echo "not isolated" || echo "isolated"
   ```

   The output must be `isolated`.
