# Quickstart

Brief instructions explaining how to install and run Feldera Enterprise
in a Kubernetes cluster.

1. **License:** obtain a Feldera account id and license key
   (reach out to `learnmore@feldera.com`)

2. **Installation using Helm:**
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
   Please view the [**Helm guide**](helm-guide.md) for further details.

3. **Status check:** run `kubectl get pods -n feldera` which should output:
   ```
   NAME                                         READY   STATUS    RESTARTS   AGE
   feldera-db-0                                 1/1     Running   0          3m9s
   feldera-kubernetes-runner-6447b8f56d-86j4w   1/1     Running   0          3m9s
   feldera-prober-server-5c945b7fcf-9r727       1/1     Running   0          3m9s
   feldera-compiler-server-0                    1/1     Running   0          3m9s
   feldera-api-server-c546499bc-wdpkm           1/1     Running   0          3m9s
   ```

4. **Port forward API server:**
   ```
   kubectl port-forward -n feldera svc/feldera-api-server 8080:8080
   ```
   ... after which the Web Console and API are accessible at: **http://localhost:8080**

## Additional resources

* [Helm guide](helm-guide.md)
* [EKS ingress](kubernetes-guides/eks/ingress.md)
* [Secret management](kubernetes-guides/secret-management.md)
