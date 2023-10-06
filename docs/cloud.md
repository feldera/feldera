# Feldera Cloud

These instructions explain how to run the Feldera Platform on a AWS Elastic Kubernetes Service (EKS). 

## Prerequisites

* A dedicated AWS account (e.g. created using AWS organizations).
* [AWS CLI](https://aws.amazon.com/cli/)
* [eksctl](https://github.com/weaveworks/eksctl): to bring up and configure AWS Elastic Kubernetes Service.
* [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/): to interact with the deployed EKS cluster.
* [Helm](https://helm.sh/docs/intro/quickstart/): to deploy and upgrade Feldera Cloud on EKS.
* A Feldera license, as part of which you will receive a Helm Chart and container artifacts to run Feldera Cloud.

## Deployment overview

We will go through the following steps to deploy Feldera Cloud on EKS:

* Use `eksctl` to create a dedicated VPC and bring up an EKS cluster inside it.
* Use `Helm` to deploy Feldera Cloud inside the EKS cluster.
* Configure input and output data sources on EKS.
* Connect these data sources to Feldera.


## Create an EKS cluster

Make sure your `aws` CLI is configured to work with your AWS account. If you haven't done so already,
run `aws configure` and enter the required credentials.

Next, create a configuration file named `eks-config.yaml` for running `eksctl`.


```
# File name: eks-config.yaml

apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: feldera-eks
  region: us-west-1
  version: "1.27"

# OIDC provider is needed to use EBS
iam:
  withOIDC: true

managedNodeGroups:
  - name: managed-ng
    desiredCapacity: 3
    instanceType: m5.large
    privateNetworking: true

vpc:
  clusterEndpoints:
    privateAccess: true
    publicAccess:  true
    # publicAccessCIDRs: ["<a.b.c.d/32>"] # Change to your IP address if using publicAccess: true

addons:
- name: aws-ebs-csi-driver
  wellKnownPolicies:
    ebsCSIController: true
```

To create an EKS cluster with this configuration, run:

```
eksctl create cluster -f eks-config.yaml
```

This uses CloudFormation behind-the-scenes to bring up an EKS cluster named
`feldera-eks` with three EC2 on-demand instances as worker nodes, 
running in a newly created dedicated VPC. It should take
roughly 15-20 minutes.

`eksctl` will create the VPC in the `us-west-1` region which will run the EKS
cluster. The cluster will be deployed across two availability zones (AZs). Each
AZ will have two subnets, one private and one public. The private subnets will
have outbound Internet connectivity via a NAT gateway. The public subnets will
have inbound and outbound connectivity via an Internet Gateway. The worker
nodes will only be on the private subnet.


:::tip
Unless you are running `eksctl` from a machine which is in the same VPC and
subnet as the EKS control plane, you will want to set `publicAccess: true`.
If so, we strongly recommend using the `publicAccessCIDRs` setting to limit IP ranges
which are allowed to interact with the EKS control plane. For example, set it
to the publicly visible IP of the machine where you are running these commands.

See the [eksctl cluster access](https://eksctl.io/usage/vpc-cluster-access/)
documentation to learn more.
:::

:::tip
`eksctl` allows additional cluster setup and access configuration beyond what
we've shown here. Check out the [configuration file
schema](https://eksctl.io/usage/schema/) to learn more.
:::

To verify that the EKS cluster is running, run:

```
kubectl get nodes
```

You should see a three node cluster, all with the status `Ready`.

## Cluster ingress

To allow external access to Feldera and other services running inside the EKS
cluster, we recommend setting up a Kubernetes Ingress controller. This is
useful to allow access to the Feldera Web Console or to issue REST API calls
directly. 

:::tip
When you install an ingress controller, it uses an AWS load balancer to expose
an IP address via a generated DNS name for external access. By default,
creating ingress controllers leads to both an internal and external load
balancer being created, with the external one being prefered to expose
services. Most Feldera Cloud users should need to expose Feldera services only
over an internal load balancer.
:::

To install an ingress controller where the external load balancer is disabled,
first create the following file:

```
# File name: ingress-controller-config.yaml

controller:
  ingressClassByName: true

  ingressClassResource:
    name: nginx
    enabled: true
    default: false
    controllerValue: "k8s.io/ingress-nginx-internal"

  service:
    # Disable the external LB
    external:
      enabled: false

    # Enable the internal LB.
    internal:
      enabled: true
      annotations:
        service.beta.kubernetes.io/aws-load-balancer-internal: "true"
        service.beta.kubernetes.io/aws-load-balancer-backend-protocol: tcp
        service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled: 'true'
        service.beta.kubernetes.io/aws-load-balancer-type: nlb
```

Then use Helm to install the nginx Ingress controller:

```
kubectl create ns ingress-nginx-internal
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm upgrade --install ingress-nginx-internal ingress-nginx/ingress-nginx \
  -n ingress-nginx-internal \
  -f ingress-nginx.yaml
```
 
## Installing Feldera Cloud

To install Feldera Cloud, first get the Helm package and container images that
are provided with our license.

Next, let's create a Kubernetes namespace to deploy Feldera Cloud in.

```
kubectl create ns feldera
```

Next, let's use Helm to install Feldera. We will create a Helm release named 
`feldera` in the `feldera` namespace we just created.

```
helm upgrade --install feldera ./helm/feldera \
    --set felderaImageRepo=$FELDERA_REPO
    --set felderaVersion=$FELDERA_VERSION \
    -n feldera
```

You should see the following output:

```
Release "feldera" does not exist. Installing it now.
NAME: feldera
LAST DEPLOYED: Fri Oct  6 13:21:38 2023
NAMESPACE: feldera
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

That's it. Now let's use `kubectl` to see the status of our deployment:

```
kubectl get pods -n feldera

```

...and you should see some output like:

```
NAME                                        READY   STATUS    RESTARTS      AGE
feldera-api-server-5f7dcc8b5f-4qc2d         1/1     Running   0             82s
feldera-compiler-server-555749567c-6qjch    1/1     Running   0             82s
feldera-db-0                                1/1     Running   0             82s
feldera-kubernetes-runner-7d74558f4-4gkph   1/1     Running   0             82s
```

## Accessing the Web Console

If you set up the recommended internal-only load balancer, you can now access the web console
from inside your VPC over the load balancer's endpoint, which can be found with:

```
kubectl get svc -n ingress-nginx-internal ingress-nginx-internal-controller-internal -o jsonpath="{.status.loadBalancer.ingress[*].hostname}"
```
This should return a hostname like:

```
abcd-efgh.elb.us-west-1.amazonaws.com
```

Open a browser and then visit that URL to see the Feldera Web Console.

If you're not in the VPC but have `kubectl` access, you can also reach this endpoint via `kubectl` port forwarding:

```
kubectl port-forward -n ingress-nginx-internal service/ingress-nginx-internal-controller-internal 8080:80
```
You can then visit `http://localhost:8080` from your browser to access the Feldera Web Console.
