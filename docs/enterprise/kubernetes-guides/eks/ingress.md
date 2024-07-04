# EKS ingress

To allow external access to Feldera and other services running inside the EKS
cluster, we recommend setting up a Kubernetes Ingress controller. This is
useful to allow access to the Feldera Web Console or to issue REST API calls
directly.

:::tip
When you install an ingress controller, it uses an AWS load balancer to expose
an IP address via a generated DNS name for external access. By default,
creating ingress controllers leads to both an internal and external load
balancer being created, with the external one being preferred to expose
services. Most Feldera Enterprise users should need to expose Feldera services only
over an internal load balancer.
:::

## Ingress installation

### 1. Ingress controller configuration file

To install an ingress controller where the external load balancer is disabled,
create a configuration values file named `ingress-nginx.yaml` with the following content:

```bash
# Filename: ingress-nginx.yaml
controller:
  ingressClassByName: true

  ingressClassResource:
    name: nginx
    enabled: true
    default: false  # To choose this controller, an Ingress must explicitly add in its spec: ingressClassName: nginx
    controllerValue: "k8s.io/ingress-nginx-internal"

  service:
    external:
      enabled: false

    internal:
      enabled: true
      annotations:
        service.beta.kubernetes.io/aws-load-balancer-internal: "true"
        service.beta.kubernetes.io/aws-load-balancer-backend-protocol: tcp
        service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: 'true'
        service.beta.kubernetes.io/aws-load-balancer-type: nlb
```

### 2. Ingress controller installation

Install the nginx Ingress controller with `helm`:

```bash
helm upgrade --install --wait --timeout 2m0s ingress-nginx-internal ingress-nginx \
    --repo https://kubernetes.github.io/ingress-nginx \
    --namespace ingress-nginx-internal --create-namespace \
    -f ingress-nginx.yaml
```

... you can check the status afterward by running:
```bash
kubectl get pods -n ingress-nginx-internal
kubectl get services -n ingress-nginx-internal
```

### 3. Feldera ingress configuration file

Create a configuration file named `ingress.yaml` with the following content:
```yaml
# Filename: ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: feldera-ingress
spec:
  ingressClassName: "nginx"
  rules:
  - http:
      paths:
      - pathType: Prefix
        path: /
        backend:
          service:
            name: feldera-api-server
            port:
              number: 8080
```

### 4. Feldera ingress installation

Use `kubectl` to install the ingress with the values configuration file:

```bash
kubectl apply -n feldera -f ingress.yaml
```

... you can check the status afterward by running:
```bash
kubectl get ingress -n feldera
```

### 5. Usage: accessing the Web Console and API

#### Inside the VPC

If you set up the recommended internal-only load balancer, you can now access the web console
from inside your VPC over the load balancer's endpoint, which can be found with:

Find the endpoint of the internal load balancer inside your VPC:

```bash
kubectl get svc -n ingress-nginx-internal ingress-nginx-internal-controller-internal \
    -o jsonpath="{.status.loadBalancer.ingress[*].hostname}"
```
This should return a hostname like:

```
abcd-efgh.elb.us-west-1.amazonaws.com
```

From within your VPC (e.g., via VPN) visit the load balancer endpoint hostname in a browser
to see the Feldera Web Console.

#### Via kubectl port-forwarding

If you are not located in the VPC but do have `kubectl` access,
you can also reach the load balancer endpoint via kubectl port-forwarding:

```bash
kubectl port-forward -n ingress-nginx-internal service/ingress-nginx-internal-controller-internal 8080:80
```

While keeping it running in a terminal, visit http://localhost:8080
in a browser to see the Feldera Web Console.

## Additional resources

Documentation regarding AWS-specific load balancer annotations:
- https://github.com/kubernetes/ingress-nginx/blob/main/deploy/static/provider/aws/deploy.yaml
- https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.7/guide/service/annotations/
- https://docs.aws.amazon.com/eks/latest/userguide/aws-load-balancer-controller.html
- https://docs.aws.amazon.com/elasticloadbalancing/latest/userguide/how-elastic-load-balancing-works.html
