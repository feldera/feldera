# EKS scaling

## Status

View the current node groups capacity (current, desired, min, max):
```bash
eksctl get nodegroup --cluster CLUSTERNAME --region us-west-2
```

... or for a specific node group only:
```bash
eksctl get nodegroup --cluster CLUSTERNAME --region us-west-2 --name ng-m5-4xlarge
```

## Adding a node group

1. In your `eks-config.yaml` configuration, add the new node group to the managed node groups:

   ```
   managedNodeGroups:
     (...)
     - name: ng-m5-8xlarge
       desiredCapacity: 0
       minSize: 0
       maxSize: 2
       instanceType: m5.8xlarge
       privateNetworking: true
       availabilityZones: ["us-west-2a"]
     (...)
   ```
   Note: adding `spot: true` will enable the use of spot instances (see also
   [documentation](https://eksctl.io/usage/spot-instances/)).

2. Create the new node group:
   ```
   eksctl create nodegroup --config-file=eks-config.yaml
   ```

## Changing instance types

For example, we wish to change the capacity of node group `ng-m5-4xlarge` to 0,
and that of `ng-m5-8xlarge` to 1.

1. First we scale up the target node group (`ng-m5-8xlarge`):
   ```bash
   eksctl scale nodegroup --cluster=CLUSTERNAME --nodes=1 ng-m5-8xlarge --region us-west-2 --nodes-min=0
   ```

2. Follow the deployment of the node group by [checking the status](#status) and
   calling `kubectl get nodes`

3. Once the new node group has been scaled up, scale down the original node group (`ng-m5-4xlarge`):
   ```bash
   eksctl scale nodegroup --cluster=CLUSTERNAME --nodes=0 ng-m5-4xlarge --region us-west-2 --nodes-min=0
   ```
