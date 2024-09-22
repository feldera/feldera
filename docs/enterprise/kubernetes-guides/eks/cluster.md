# EKS cluster

Feldera Enterprise can be installed on Amazon's Elastic Kubernetes Service (EKS).
These instructions detail how to set up a basic EKS Kubernetes cluster.

## Prerequisites

* A dedicated AWS account (e.g., created using AWS organizations)

* [**aws** (AWS CLI)](https://aws.amazon.com/cli/): `aws --version`

  Used by eksctl to interact with AWS. Should be configured to work with AWS account.

* [**eksctl**](https://github.com/weaveworks/eksctl): `eksctl version`

  Used to bring up and configure EKS cluster

* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/): `kubectl version`

  Used to interact with the deployed EKS cluster

## EKS cluster creation

### 1. AWS access

Make sure your `aws` CLI is configured to work with your AWS account.
If you haven't done so already, run `aws configure` and enter the required credentials.
The currently configured user can be checked via:
```
aws sts get-caller-identity
```
More information can be found in the [AWS documentation for getting started with eksctl](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html).
`eksctl` uses `aws` behind-the-scenes.
- Add `--profile [profile]` to your `eksctl` calls if `aws` CLI is configured with
  multiple profiles.
- `eksctl` requires a set of minimum IAM policies to function as
  [outlined in the documentation](https://eksctl.io/usage/minimum-iam-policies/).

### 2. Cluster configuration

We will define the cluster using a YAML configuration file which will be applied
using `eksctl`. In this configuration file, we will specify cluster aspects, most notably:
* Name, region and version
* Use [EBS](https://aws.amazon.com/ebs/) for the allocation of
  [persistent volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
* Node groups which define the machines the cluster runs on
* A dedicated [VPC](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html)

Create a configuration file named `eks-config.yaml` with the following content:

```yaml
# Filename: eks-config.yaml

apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: feldera-cluster
  region: us-west-1
  version: "1.30"

# Node groups that can be used to scale the cluster up and down
managedNodeGroups:
  - name: ng-m5-4xlarge
    desiredCapacity: 1
    minSize: 0
    maxSize: 3
    instanceType: m5.4xlarge
    privateNetworking: true

# Virtual private cloud (VPC)
vpc:
  clusterEndpoints:
    privateAccess: true
    publicAccess:  true
  # If publicAccess is set to true, it is recommended to limit the
  # IPs that can connect using this field
  publicAccessCIDRs: ["<a.b.c.d/32>"]  # Change to your IP range

# Addons with policies attached (e.g., EBS) require OIDC enabled
iam:
  withOIDC: true

# Addon: AWS Elastic Block Store (EBS) Container Storage Interface (CSI) driver.
# This addon makes EBS volumes be the storage for Kubernetes persistent volumes.
addons:
  - name: aws-ebs-csi-driver
    wellKnownPolicies:
      ebsCSIController: true
```

:::tip
Unless you are running `eksctl` and `kubectl` from a machine which is in the
same VPC and subnet as the EKS control plane, you will want to set
`publicAccess: true`. If so, we strongly recommend using the
`publicAccessCIDRs` setting to limit IP ranges which are allowed to interact
with the EKS control plane. We recommend customers set this to a well-scoped
network range as they see fit to restrict access to one or more operators in your
corporate network.

See the [eksctl cluster access](https://eksctl.io/usage/vpc-cluster-access/)
documentation to learn more.
:::

:::tip
`eksctl` allows additional cluster setup and access configuration beyond what
we've shown here. Check out the [configuration file schema](https://eksctl.io/usage/schema/)
to learn more.
:::

### 3. Cluster creation

Create the EKS cluster with this configuration.
```
eksctl create cluster -f eks-config.yaml
```
It should take roughly 15-20 minutes. This uses CloudFormation behind-the-scenes
to bring up an EKS cluster named `feldera-cluster` with one EC2 instance as worker node,
running in a newly created dedicated VPC.

`eksctl` will create the VPC in the `us-west-1` region which will run the EKS
cluster. The cluster will be deployed across two availability zones (AZs). Each
AZ will have two subnets, one private and one public. The private subnets will
have outbound Internet connectivity via a NAT gateway. The public subnets will
have inbound and outbound connectivity via an Internet Gateway. The worker
nodes will only be on the private subnet.

### 4. Cluster access and status with kubectl

* Follow the [AWS instructions](https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html)
  to configure `kubectl` with the newly created EKS cluster

* Verify that the EKS cluster is running:
  ```
  kubectl get nodes
  ```
  You should see a cluster of a single node, with the status `Ready`.

* Feldera needs at least a default storage class to allocate persistent volume claims (PVCs).
  Users can also explicitly specify the storage class per pipeline directly in their configuration.

  If you installed the EBS CSI driver as per the instructions above, you should see the `gp2` storage
  class in your cluster already, marked as the default:
  ```bash
  $ kubectl get sc
  NAME            PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
  gp2 (default)   kubernetes.io/aws-ebs   Delete          WaitForFirstConsumer   false                  64m
  ```

  If it has not been automatically marked as default (visible next to its name), set it as such using:
  ```bash
  kubectl patch sc gp2 -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
  ```

### 5. Cluster deletion

Cluster deletion can be done by running:
```
eksctl delete cluster -f eks-config.yaml --disable-nodegroup-eviction
```

## Kubernetes cluster considerations

Several important aspects to consider (with some useful links for AWS):

- **General best practices** (e.g.,
  [AWS EKS best practices](https://aws.github.io/aws-eks-best-practices/))
- **Networking**
  * The worker nodes need to access the container registry
  * The worker nodes need to access the control plane API server
  * The Feldera installation deployed on the worker nodes need to access
    the data input sources and output sinks (e.g., databases, Kafka)
  * The cluster must be reachable with `kubectl` either directly if publicly accessible
    or indirectly otherwise
    (e.g., via
    [bastion](https://docs.aws.amazon.com/eks/latest/userguide/cluster-endpoint.html#private-access),
    [VPN over a VPC endpoint](https://docs.aws.amazon.com/vpn/latest/clientvpn-admin/what-is.html))
  * In general, whether endpoints should be
    public and/or private (see
    [best practices](https://aws.github.io/aws-eks-best-practices/security/docs/iam/#make-the-eks-cluster-endpoint-private)
    and [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/cluster-endpoint.html))
    or potentially fully-private (see
    [AWS](https://docs.aws.amazon.com/eks/latest/userguide/private-clusters.html)
    or [eksctl](https://eksctl.io/usage/eks-private-cluster/) documentation)

- **Volumes**

  The Kubernetes Volumes will need to be backed by a storage method.
  The Volumes are used both by the Feldera services themselves and the pipelines that are started.
  For AWS, Elastic Block Store (EBS) can be used (see
  [driver repository](https://github.com/kubernetes-sigs/aws-ebs-csi-driver),
  [AWS documentation](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html),
  [eksctl addons](https://eksctl.io/usage/addons/)).

- **Scale**

  It is important to scale the cluster proportional to the workload.

- **Security**

  Control access to the server API, the worker nodes and the applications that run on them.
  For example, in EKS setting up IAM identity mappings (see
  [eksctl documentation](https://eksctl.io/usage/iam-identity-mappings/)).
