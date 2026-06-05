# EKS ingress with Envoy Gateway

To allow external access to Feldera and other services running inside the EKS
cluster, we recommend setting up [Envoy Gateway](https://gateway.envoyproxy.io/),
which implements the Kubernetes Gateway API. This lets you expose the Feldera
Web Console and REST API through an AWS load balancer.

:::tip
By default, creating a Gateway provisions an external load balancer. Most
Feldera Enterprise users should expose Feldera services only over an internal
load balancer, reachable from inside the VPC.
:::

## Installation

### 1. Install Envoy Gateway

Install Envoy Gateway using Helm:

```bash
helm upgrade --install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
    --version v1.7.2 \
    --namespace envoy-gateway-system --create-namespace \
    --set-string config.envoyGateway.logging.level.default=warn
```

Check that it's running:

```bash
kubectl get pods -n envoy-gateway-system
```

### 2. Create a GatewayClass

A GatewayClass tells Kubernetes which controller handles your Gateways. Create
a file named `gatewayclass.yaml`:

```yaml
# Filename: gatewayclass.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: eg
spec:
  controllerName: gateway.envoyproxy.io/gatewayclass-controller
```

Apply it:

```bash
kubectl apply -f gatewayclass.yaml
```

### 3. Configure the load balancer

Create a file named `envoyproxy.yaml` to configure the underlying service as an
internal AWS NLB:

```yaml
# Filename: envoyproxy.yaml
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyProxy
metadata:
  name: feldera-proxy
  namespace: feldera
spec:
  provider:
    type: Kubernetes
    kubernetes:
      envoyService:
        type: LoadBalancer
        annotations:
          service.beta.kubernetes.io/aws-load-balancer-internal: "true"
          service.beta.kubernetes.io/aws-load-balancer-type: nlb
```

Apply it:

```bash
kubectl apply -f envoyproxy.yaml
```

### 4. Create a Gateway

Create a file named `gateway.yaml`. Replace `feldera.example.com` with your
actual hostname:

```yaml
# Filename: gateway.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: feldera
  namespace: feldera
spec:
  gatewayClassName: eg
  infrastructure:
    parametersRef:
      group: gateway.envoyproxy.io
      kind: EnvoyProxy
      name: feldera-proxy
  listeners:
    - name: http
      port: 80
      protocol: HTTP
      hostname: "feldera.example.com"
```

Apply it:

```bash
kubectl apply -f gateway.yaml
```

### 5. Create an HTTPRoute for Feldera

Create a file named `httproute.yaml`:

```yaml
# Filename: httproute.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: feldera
  namespace: feldera
spec:
  parentRefs:
    - name: feldera
      namespace: feldera
  hostnames:
    - "feldera.example.com"
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - name: feldera-api-server
          port: 8080
```

Apply it:

```bash
kubectl apply -f httproute.yaml
```

### 6. Usage: accessing the Web Console and API

#### Inside the VPC

Find the load balancer endpoint:

```bash
kubectl get gateway feldera -n feldera \
    -o jsonpath="{.status.addresses[*].value}"
```

This returns a hostname like:

```
abcd-efgh.elb.us-west-1.amazonaws.com
```

From inside your VPC (e.g., via VPN), visit the load balancer hostname in a
browser to see the Feldera Web Console.

#### Via kubectl port-forwarding

If you have `kubectl` access but are not inside the VPC, you can reach Feldera
via port-forwarding:

```bash
kubectl port-forward -n feldera service/feldera-api-server 8080:8080
```

While keeping it running, visit http://127.0.0.1:8080 in a browser to see the
Feldera Web Console.

## Additional resources

- [Envoy Gateway documentation](https://gateway.envoyproxy.io/docs/)
- [Kubernetes Gateway API](https://gateway-api.sigs.k8s.io/)
- [AWS load balancer annotations](https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.7/guide/service/annotations/)
