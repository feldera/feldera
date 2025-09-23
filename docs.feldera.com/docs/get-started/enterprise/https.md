# HTTPS

This document describes how to configure the endpoints of Feldera components (API server, compiler,
runner, pipelines) to serve HTTPS in the Enterprise edition.

- The same certificate (and thus private key) are used by all components
- Authentication (e.g., mTLS) is currently not yet supported (besides for the
  [existing authentication methods of the API server](authentication/index.mdx))

This document does not apply to the connection to the Postgres database used by the control plane,
which can be [configured separately](helm-guide.md).

:::warning

Before switching an existing Feldera installation to using HTTPS, it is required to stop
all existing running pipelines because otherwise the still running pipelines will continue
to serve their HTTP endpoint -- which the runner will no longer be able to communicate with
as it expects HTTPS. The pipelines will need to be recompiled.

:::

## Certificate

A certificate and associated private key need to be generated for the relevant hostnames.

### Certificate structure

- The Feldera components communicate with each other in Kubernetes using the
  `SERVICE.NAMESPACE.svc.cluster.local` hostnames.
- As such, a single certificate should be generated for the wildcard hostname
  `*.NAMESPACE.svc.cluster.local`
- (Optional) When using `kubectl` port forwarding, it is also useful to add
  hostname `localhost` and/or `127.0.0.1` to the certificate

### Example (self-signed)

For example, suppose that we install Feldera in namespace `feldera` and will
be using `kubectl` port forwarding.

1. Create `x509_v3.ext` to specify the SANs (Subject Alternative Names):
   ```
   [x509_v3]
   subjectAltName = @alt_names

   [alt_names]
   DNS.1 = localhost
   DNS.2 = *.feldera.svc.cluster.local
   IP.1 = 127.0.0.1
   ```

2. Generate the certificate and private key using `openssl`:
   ```
   openssl req -x509 -newkey rsa:4096 -nodes \
     -keyout tls.key -out tls.crt -days 365 \
     -subj '/CN=localhost' -config x509_v3.ext -extensions x509_v3
   ```

3. This will create the `tls.key` and `tls.crt` files.

### Not self-signed

If the certificate is not self-signed, but instead is signed by a root CA or an intermediate CA,
the `tls.crt` should contain the complete bundle of the entire chain up until the root certificate
authority. As such, it should contain multiple  `-----BEGIN CERTIFICATE----- (...) -----END CERTIFICATE-----`
sections, starting with the leaf certificate and ending with the root certificate. The `tls.key`
should only contain the single private key of the leaf certificate.

## Configure Kubernetes

1. Create the Kubernetes TLS Secret (e.g., in namespace `feldera`, and named `feldera-https-config`)
   with the local `tls.key` and `tls.crt` files:
   ```
   kubectl create -n feldera secret tls feldera-https-config --key tls.key --cert tls.crt
   ```
   The secret (i.e., certificate and key) will be mounted as a directory with corresponding files
   by each of the Feldera component (API server, compiler, runner, pipelines) pods.

2. Provide in the Helm installation the reference for the `httpsSecretRef` value.

   - **Via file `values.yaml`:**
     ```
     httpsSecretRef: "feldera-https-config"
     ```

   - **In the command:**
     ```
     helm upgrade --install feldera \
       ... \
       --set httpsSecretRef="feldera-https-config"
     ```

3. (Optional) Port forward:
   ```
   kubectl port-forward -n feldera svc/feldera-api-server 8080:8080
   ```
   ... and then open in the browser: https://localhost:8080

   Note that the certificate will need to be trusted to not encounter
   errors about an insecure connection (or self-signed certificates).
   For example, when using curl:
   ```
   curl --cacert tls.crt https://localhost:8080/v0/pipelines
   ```

   It is no longer possible to reach the endpoints via HTTP.
   Attempting to do so will result in a failure to parse the HTTPS response;
   this will for example elicit error messages such as
   "localhost sent an invalid response" (Chrome) or
   "Received HTTP/0.9 when not allowed" (curl).
   Use `https://` as the protocol.
