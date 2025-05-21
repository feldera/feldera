# Quickstart

Brief instructions explaining how to install and run Feldera Enterprise
in a Kubernetes cluster.

1. **License:** [contact sales](https://calendly.com/d/cn7m-grv-mzm/feldera-demo) to obtain a Feldera account ID and license key.

2. **Installation using Helm:**
   ```bash
   ACCOUNT_ID="00000000-0000-0000-0000-000000000000"  # Set to your own
   LICENSE_KEY="00000000-0000-0000-0000-000000000000"  # Set to your own

   helm upgrade --install feldera \
       oci://public.ecr.aws/feldera/feldera-chart --version 0.64.0 \
       --namespace feldera --create-namespace \
       --set felderaVersion="0.64.0" \
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
   feldera-compiler-server-0                    1/1     Running   0          3m9s
   feldera-api-server-c546499bc-wdpkm           1/1     Running   0          3m9s
   ```

4. **Port forward API server:**
   ```
   kubectl port-forward -n feldera svc/feldera-api-server 8080:8080
   ```
   ... after which the Web Console and API are accessible at: http://127.0.0.1:8080

## Additional resources

* [Helm guide](helm-guide.md)
* [EKS ingress](kubernetes-guides/eks/ingress.md)
