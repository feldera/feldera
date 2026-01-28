# HTTPS

This document describes how to configure the endpoints of Feldera components (API server, compiler,
runner, pipelines) to serve HTTPS in the Enterprise edition.

For Feldera to support HTTPS with single-host pipelines, the
administrator must configure Feldera with a [wildcard
certificate](#wildcard-certificate-generation) and a private key for
its main DNS domain, e.g. for `*.feldera.svc.cluster.local`.  The
Feldera API server, compiler runner, pipeline runner, pipeline
stateful sets, and other top-level components share this certificate
and key.

For Feldera to also support HTTPS with multihost pipelines, the
administrator must additionally configure Feldera with a [private CA
certificate chain](#private-ca-certificate-chain-generation) and
private key.  This allows the Feldera pipeline runner to generate keys
for multihost pipeline pods in their nested DNS domains,
e.g. `<pipeline>-<ordinal>.<pipeline>.feldera.svc.cluster.local`,
which a single wildcard certificate cannot cover.  The generated
certificates are only used for connections between Feldera components,
such as between the pipeline pods and the pipeline manager, not
external software.

:::note

Feldera does not support mTLS authentication.  The API server supports
[API authentication](authentication/index.mdx).

:::

This document does not apply to the connection to the Postgres database used by the control plane,
which can be [configured separately](helm-guide.md).

:::warning

Before switching an existing Feldera installation to using HTTPS, it is required to stop
all existing running pipelines because otherwise the still running pipelines will continue
to serve their HTTP endpoint -- which the runner will no longer be able to communicate with
as it expects HTTPS. Restart the pipelines after configuring HTTPS.

:::

## Wildcard Certificate Generation

To support HTTPS, the administrator must generate and install a
wildcard certificate for `*.<namespace>.svc.cluster.local`, where
`<namespace>` is the Kubernetes namespace in use, such as `feldera`.

:::info

When using `kubectl` port forwarding, it is also useful to add
hostname `localhost` and/or `127.0.0.1` to the certificate.

:::

Any method for generating a signed certificate is acceptable.  We
assume later that the certificate file is named `tls.crt` and the
private key file `tls.key`.  The following sections describe two ways
to generate these files.

### Generating a self-signed certificate with OpenSSL

Suppose that we install Feldera in namespace `feldera` and will be
using `kubectl` port forwarding.

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

### Using a certificate signed by an external CA

If the certificate is not self-signed, but instead is signed by a root CA or an intermediate CA,
the `tls.crt` should contain the complete bundle of the entire chain up until the root certificate
authority. As such, it should contain multiple  `-----BEGIN CERTIFICATE----- (...) -----END CERTIFICATE-----`
sections, starting with the leaf certificate and ending with the root certificate. The `tls.key`
should only contain the single private key of the leaf certificate.

## Private CA Certificate Chain Generation

To support HTTPS for multihost pipelines, the administrator must
additionally configure Feldera with a private CA certificate chain and
private key.  This allows the Feldera pipeline runner to generate keys
for multihost pipeline pods in their nested DNS domains,
e.g. `<pipeline>-<ordinal>.<pipeline>.feldera.svc.cluster.local`,
which a single wildcard certificate cannot cover.

The private CA certificate chain can be and should be separate from
any other PKI, because it is used only for connections between Feldera
components, such as between the pipeline pods and the pipeline
manager, not external software.

Any method for generating a private CA certificate chain is
acceptable.  We assume later that the CA certificate chain file is
named `private_ca.crt` and the private key file `private_ca.key`.  The
section below describes one way to generate these files.

### Generating a private CA with OpenSSL

1. Create `root_x509_v3.ext` to specify options for the root private CA:

   ```
   [x509_v3]
   basicConstraints = CA:TRUE,pathlen:1
   ```

2. Create `intermediate_x509_v3.ext` to specify options for the
   intermediate private CA:

   ```
   [x509_v3]
   basicConstraints = CA:TRUE,pathlen:0
   ```

3. Generate the root CA with `openssl`:

   ```
   openssl req -x509 -newkey rsa:4096 -nodes \
         -keyout private_root_tls.key -out private_root_tls.crt -days 365 \
         -subj '/CN=FelderaTest Private Root CA' \
         -config root_x509_v3.ext -extensions x509_v3
   ```

4. Generate and sign the intermediate CA with `openssl`:

   ```
   openssl req -new -newkey rsa:4096 -nodes \
       -keyout private_intermediate_tls.key -out private_intermediate_tls.csr \
       -subj '/CN=FelderaTest Intermediate CA'
   openssl x509 -req \
       -in private_intermediate_tls.csr -CA private_root_tls.crt -CAkey private_root_tls.key \
       -CAcreateserial -out private_intermediate_tls.crt \
       -days 360 -sha256 \
       -extfile intermediate_x509_v3.ext -extensions x509_v3
   ```

5. Produce the secret to install into Kubernetes:

   ```
   cat private_intermediate_tls.crt private_root_tls.crt > private_ca.crt
   cat private_intermediate_tls.key > private_ca.key
   ```

## Configure Kubernetes

1. Create the Kubernetes TLS secret for `tls.key` and `tls.crt` files
   from [wildcard certificate
   generation](#wildcard-certificate-generation):

   ```
   kubectl create -n <namespace> secret tls feldera-https-config --key tls.key --cert tls.crt
   ```

   The secret (i.e., certificate and key) will be mounted as a directory with corresponding files
   by each of the Feldera component (API server, compiler, runner, pipelines) pods.

2. To additionally support multihost pipelines, create a Kubernetes
   TLS secret for `private_ca.key` and `private_ca.crt` from [private
   CA certificate chain
   generation](#private-ca-certificate-chain-generation):

   ```
   kubectl create -n <namespace> secret tls feldera-ca-config --key private_ca.key --cert private_ca.crt
   ```

3. Provide in the Helm installation the reference for the
   `httpsSecretRef` and, for multihost support, `caSecretRef`, value.

   If you did not create the private CA for multihost pipeline support:

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

   If you did create the private CA for multihost pipeline support:


   - **Via file `values.yaml`:**
     ```
     httpsSecretRef: "feldera-https-config"
     caSecretRef: "feldera-ca-config"
     ```

   - **In the command:**
     ```
     helm upgrade --install feldera \
       ... \
       --set httpsSecretRef="feldera-https-config" \
       --set caSecretRef="feldera-ca-config"
     ```

4. (Optional) Port forward:
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
