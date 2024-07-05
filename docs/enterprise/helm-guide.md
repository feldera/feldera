# Helm guide

These instructions explain how to install and run *Feldera Enterprise*
on a Kubernetes cluster. It requires a valid Feldera Enterprise license
-- please request one at `learnmore@feldera.com` if you are interested.

## Prerequisites

* [**kubectl**](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/): `kubectl version`

  Used to interact with the Kubernetes cluster

* [**helm**](https://helm.sh/docs/intro/install/): `helm version`

  Package manager to conveniently manage deployments on Kubernetes

* Feldera license, notably the **account id** and **license key**.
  They are required in the installation. Please reach out to us at
  `learnmore@feldera.com` if you are interested.

* The Feldera Helm chart and Docker images are hosted on
  [AWS ECR public registry](https://gallery.ecr.aws/n4h1j7h1).
  We will use the chart and images by referring to their online repository
  naming `public.ecr.aws/n4h1j7h1/<image>:<version>`.

## Installing Feldera Enterprise

1. **Kubernetes access:** check that your `kubectl` is configured
   to your Kubernetes cluster.
   ```bash
   kubectl cluster-info
   kubectl get namespace
   ```

2. **Installation:**
   we use our Helm chart, which internally refers to the other images, to
   perform the installation in namespace `feldera` with release name `feldera`.

   ```bash
   ACCOUNT_ID="00000000-0000-0000-0000-000000000000"  # Set to own
   LICENSE_KEY="00000000-0000-0000-0000-000000000000"  # Set to own
   
   helm upgrade --install feldera \
       oci://public.ecr.aws/n4h1j7h1/feldera-chart --version 0.7.3 \
       --namespace feldera --create-namespace \
       --set felderaVersion="0.7.3" \
       --set felderaAccountId="${ACCOUNT_ID}" \
       --set felderaLicenseKey="${LICENSE_KEY}" \
       --set felderaDatabaseSecretRef="feldera-db-insecure-secret"
   ```
   _Note:_ the license verification is done by interacting with our online 
   license server at `cloud.feldera.com`.

   _Note:_ how to configure your own database credentials is explained in
   a further section.

3. **Check:** check the status of the deployment.
   ```
   kubectl get pods -n feldera
   ```
   
   ... which should output approximately the following:
   ```
   NAME                                         READY   STATUS    RESTARTS   AGE
   feldera-db-0                                 1/1     Running   0          3m9s
   feldera-kubernetes-runner-6447b8f56d-86j4w   1/1     Running   0          3m9s
   feldera-prober-server-5c945b7fcf-9r727       1/1     Running   0          3m9s
   feldera-compiler-server-0                    1/1     Running   0          3m9s
   feldera-api-server-c546499bc-wdpkm           1/1     Running   0          3m9s
   ```

4. **Usage:** 

   Interaction with Feldera happens through the API server, which has an associated
   service named `<release name>-api-server.<namespace>.svc.cluster.local:8080`.
   With kubectl, this can be port-forwarded to be accessible locally:

   ```
   kubectl port-forward -n feldera svc/feldera-api-server 8080:8080
   ```

   ... after which you can (leaving the forwarding running in a terminal):
   * Visit Web Console in browser: **http://localhost:8080**
   * Interact with the API: [**http://localhost:8080/v0/...**](http://localhost:8080/v0/...)

   _Note:_ access through kubectl port-forwarding is mostly useful for test and development.
   In other cases, setting up an ingress (e.g., [in EKS](kubernetes-guides/eks/ingress.md)) is likely preferable.

## Extra

### Configure custom database credentials

Instead of the insecure default DB credentials, you can supply your own custom database credentials.

1. **Secret configuration file:** create a file `feldera-db-secret.yaml`
  with custom database credentials.
   ```yaml
   # Filename: feldera-db-secret.yaml
   apiVersion: v1
   kind: Secret
   type: Opaque
   metadata:
     name: feldera-db-secret
   stringData:
     .user: "..."  # Fill in
     .password: "..."  # Fill in
   ```

2. **Create secret:** with the configuration file, create the secret:
   ```bash
   #kubectl create namespace feldera  # If the namespace does not exist yet
   kubectl apply -n feldera -f feldera-db-secret.yaml
   ```

3. **Specify secret during installation:** in the `helm` installation command, set
   the following:
   ```
   --set felderaDatabaseSecretRef="feldera-db-secret"
   ```
   _Note:_ it must be a new installation.

### Feldera installation overview

**Services:**
```
$ kubectl get svc -n feldera
NAME                        TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)     AGE
feldera-prober-server       ClusterIP   10.43.67.233    <none>        44444/TCP   5m10s
feldera-kubernetes-runner   ClusterIP   10.43.175.228   <none>        8080/TCP    5m10s
feldera-api-server          ClusterIP   10.43.176.120   <none>        8080/TCP    5m10s
feldera-db                  ClusterIP   10.43.60.190    <none>        5431/TCP    5m10s
feldera-compiler-server     ClusterIP   10.43.60.125    <none>        8085/TCP    5m10s
```

**Pods:**
```
$ kubectl get pods -n feldera
NAME                                         READY   STATUS    RESTARTS   AGE
feldera-db-0                                 1/1     Running   0          5m31s
feldera-kubernetes-runner-6447b8f56d-86j4w   1/1     Running   0          5m31s
feldera-prober-server-5c945b7fcf-9r727       1/1     Running   0          5m31s
feldera-compiler-server-0                    1/1     Running   0          5m31s
feldera-api-server-c546499bc-wdpkm           1/1     Running   0          5m31s
```

**StatefulSets:**
```
$ kubectl get sts -n feldera
NAME                      READY   AGE
feldera-db                1/1     5m41s
feldera-compiler-server   1/1     5m41s
```

**Deployments:**
```
$ kubectl get deployments -n feldera
NAME                        READY   UP-TO-DATE   AVAILABLE   AGE
feldera-kubernetes-runner   1/1     1            1           5m55s
feldera-prober-server       1/1     1            1           5m55s
feldera-api-server          1/1     1            1           5m55s
```

**Persistent volume claims:**
```
$ kubectl get pvc -n feldera
NAME                                         STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
db-storage-feldera-db-0                      Bound    pvc-a5acb4ef-1d3c-4773-ad42-528657f11c94   20Gi       RWO            local-path     6m11s
compiler-storage-feldera-compiler-server-0   Bound    pvc-07369993-c20a-4f8a-abd1-52bf3be413c3   20Gi       RWO            local-path     6m11s
```

### Uninstallation

If you installed Feldera in its own dedicated namespace,
the most thorough way to uninstall Feldera is to delete the entire namespace:
`kubectl delete namespace feldera`

If you wish to preserve the namespace, it can be uninstalled through
the usual `helm` method:
```
helm uninstall -n feldera feldera
```

This does not delete the persistent volumes, which need to be deleted separately:
```
kubectl get pvc -n feldera
kubectl delete pvc -n feldera db-storage-feldera-db-0
kubectl delete pvc -n feldera compiler-storage-feldera-compiler-server-0
kubectl get pvc -n feldera
```

## Troubleshooting

### Incorrect license

If an incorrect account id and/or license key was provided, the kubernetes-runner
will fail to start:

```
$ kubectl get pods -n feldera
NAME                                         READY   STATUS             RESTARTS     AGE
feldera-db-0                                 1/1     Running            0            2m34s
feldera-compiler-server-0                    1/1     Running            0            2m34s
feldera-kubernetes-runner-854446fb84-rg66z   0/1     CrashLoopBackOff   1 (9s ago)   2m34s
feldera-api-server-7bb757f685-jwn5v          1/1     Running            0            2m34s
feldera-prober-server-698c999557-f8nsr       1/1     Running            0            2m34s
```

... and an explanation will be visible in its log:

```
$ kubectl logs -n feldera deployment/feldera-kubernetes-runner
...
FAIL: license verification failed: a valid license is required for the Enterprise version of Feldera. Please contact Feldera support (support@feldera.com) to resolve this issue.
...
```

To resolve this, provide a correct account id and license key
during the [helm installation](#installing-feldera-enterprise).
If you do not have a license, please reach out to us at `support@feldera.com`.
