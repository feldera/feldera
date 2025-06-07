# K3D cluster

Feldera Enterprise can also be installed on a local [k3d cluster](https://k3d.io/)
for testing and development purposes. A k3d cluster is a lightweight minimal
Kubernetes cluster which makes use of Docker.

## Prerequisites

* [**k3d**](https://k3d.io/v5.6.0/):
  `k3d version`

  To set up a local Kubernetes cluster for testing.

* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/): `kubectl version`

  Used to interact with the deployed EKS cluster.

## K3D cluster creation

### Creation

```
k3d cluster create local-test1 \
    --k3s-arg "--disable=traefik@server:*"
```
_Note:_ it disables the traefik ingress controller such that you can install
your own.

If you wish to expose a specific port of the load balancer (e.g., for your
own ingress controller), you can use the following:
```bash
k3d cluster create local-test1 \
    --k3s-arg "--disable=traefik@server:*" \
    --port "80:80@loadbalancer" \
    --port "443:443@loadbalancer"
```
_Note:_ this will locally bind port 80 and 443.

### Check

The cluster creation will automatically configure `kubectl`.
Check the cluster status:
```bash
kubectl cluster-info
kubectl config current-context
kubectl get nodes
kubectl get namespace
```

### Deletion

```bash
k3d cluster delete local-test1
kubectl config unset current-context  # Optional
```

_Note:_ the kubectl current-context is unset because after the k3d-local-test context is no
longer available, it automatically switches to one of the others available, which is likely
undesirable during testing.
