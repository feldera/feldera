#!/bin/sh

set -e

#### Refresh kind
kind delete cluster

# create a cluster with the local registry enabled in containerd
cat <<EOF | kind create cluster --image=kindest/node:v1.25.0 --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    kubeadmConfigPatches:
    - |
      kind: InitConfiguration
      nodeRegistration:
        kubeletExtraArgs:
          node-labels: "ingress-ready=true"
    extraPortMappings:
    - containerPort: 80
      hostPort: 80
      protocol: TCP
    - containerPort: 443
      hostPort: 443
      protocol: TCP
  - role: worker
  - role: worker
  - role: worker
EOF

#### Install Redpanda helm charts
helm repo add redpanda https://charts.redpanda.com/
helm repo update
helm install redpanda redpanda/redpanda --version 2.6.1 --set statefulset.replicas=1

#### Set up ingress controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

#### Set up DBSP namespace and database secrets
kubectl create namespace dbsp
kubectl create secret generic db-creds --from-literal=postgres-password='postgres' -n dbsp
