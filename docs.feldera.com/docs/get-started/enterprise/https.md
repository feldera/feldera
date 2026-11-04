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

The API server does not accept client certificates; it supports
[API authentication](authentication/index.mdx).  The compiler and runner
servers can require [client certificates](#client-certificates)
from the other Feldera components.

:::

We recommend [cert-manager](https://cert-manager.io) to issue and renew
these certificates, see [Using cert-manager](#using-cert-manager).  The
sections after it describe how to create them by hand with OpenSSL.

This document does not apply to the connection to the Postgres database used by the control plane,
which can be [configured separately](helm-guide.md).

:::warning

Before switching an existing Feldera installation to using HTTPS, it is required to stop
all existing running pipelines because otherwise the still running pipelines will continue
to serve their HTTP endpoint -- which the runner will no longer be able to communicate with
as it expects HTTPS. Restart the pipelines after configuring HTTPS.

:::

## Using cert-manager

[cert-manager](https://cert-manager.io/docs/installation/) issues the
certificates as Kubernetes secrets and renews them before they
expire.  The example below creates one self-signed root CA in the
`feldera` namespace with two branches:

```
feldera-root-ca (self-signed)
├── feldera-https-ca ── feldera-https-config   wildcard server certificate (httpsSecretRef)
└── feldera-ca-config                          private CA for multihost pipelines and
                                               client certificates (caSecretRef)
```

An existing `ClusterIssuer` of your organization can take the place of
the self-signed root.

1. Install cert-manager in the cluster, if it is not there yet.

2. Apply the issuers and certificates.  Replace `feldera` with your
   namespace in `namespace`, `commonName` and `dnsNames`:

   ```yaml
   apiVersion: cert-manager.io/v1
   kind: Issuer
   metadata:
     name: feldera-selfsigned
     namespace: feldera
   spec:
     selfSigned: {}
   ---
   apiVersion: cert-manager.io/v1
   kind: Certificate
   metadata:
     name: feldera-root-ca
     namespace: feldera
   spec:
     isCA: true
     commonName: feldera-root-ca
     secretName: feldera-root-ca
     duration: 87600h # 10 years
     privateKey:
       algorithm: ECDSA
       size: 256
       encoding: PKCS8
     issuerRef:
       name: feldera-selfsigned
       kind: Issuer
   ---
   apiVersion: cert-manager.io/v1
   kind: Issuer
   metadata:
     name: feldera-root-ca
     namespace: feldera
   spec:
     ca:
       secretName: feldera-root-ca
   ---
   apiVersion: cert-manager.io/v1
   kind: Certificate
   metadata:
     name: feldera-https-ca
     namespace: feldera
   spec:
     isCA: true
     commonName: feldera-https-ca
     secretName: feldera-https-ca
     duration: 43800h # 5 years
     privateKey:
       algorithm: ECDSA
       size: 256
       encoding: PKCS8
     issuerRef:
       name: feldera-root-ca
       kind: Issuer
   ---
   apiVersion: cert-manager.io/v1
   kind: Issuer
   metadata:
     name: feldera-https-ca
     namespace: feldera
   spec:
     ca:
       secretName: feldera-https-ca
   ---
   # Wildcard server certificate of the API server, compiler, runner and pipelines.
   apiVersion: cert-manager.io/v1
   kind: Certificate
   metadata:
     name: feldera-https-config
     namespace: feldera
   spec:
     secretName: feldera-https-config
     commonName: feldera.svc.cluster.local
     dnsNames:
       - "*.feldera.svc.cluster.local"
       - localhost # for kubectl port forwarding
     ipAddresses:
       - 127.0.0.1
     usages:
       - digital signature
       - server auth
     duration: 8760h # 1 year
     renewBefore: 720h # 30 days
     privateKey:
       algorithm: ECDSA
       size: 256
       encoding: PKCS8
       rotationPolicy: Always
     issuerRef:
       name: feldera-https-ca
       kind: Issuer
   ---
   # Private CA: the runner signs a certificate for each pipeline with it.
   apiVersion: cert-manager.io/v1
   kind: Certificate
   metadata:
     name: feldera-ca-config
     namespace: feldera
   spec:
     isCA: true
     commonName: feldera-private-ca
     secretName: feldera-ca-config
     duration: 43800h # 5 years
     privateKey:
       algorithm: ECDSA
       size: 256
       encoding: PKCS8
     issuerRef:
       name: feldera-root-ca
       kind: Issuer
   ```

   The runner reads the private CA key only as PKCS#8, so keep
   `encoding: PKCS8` on `feldera-ca-config`.

3. Wait until all certificates are ready:

   ```
   kubectl get certificates -n feldera
   ```

4. Set the secrets in the Helm values, as in step 3 of [Configure
   Kubernetes](#configure-kubernetes).  To also enable
   [client certificates](#client-certificates), add `mtls.enabled: true`:

   ```yaml
   httpsSecretRef: "feldera-https-config"
   caSecretRef: "feldera-ca-config"
   ```

:::info

The Feldera components read the server certificate
(`feldera-https-config`) when they start.  A renewed certificate takes
effect after the next restart of the components, which must happen
before the old certificate expires: within `renewBefore`, 30 days in the
example above.  A Feldera upgrade restarts them.

:::

To trust the server certificate from outside the cluster, for example
with `curl` over port forwarding, use the root CA:

```
kubectl get secret -n feldera feldera-root-ca -o jsonpath='{.data.tls\.crt}' | base64 -d > feldera-root-ca.crt
curl --cacert feldera-root-ca.crt 'https://localhost:8080/v0/pipelines'
```

If an ingress or a Gateway API gateway forwards traffic to the API
server, configure it to connect over HTTPS and to verify the API
server against the `feldera-root-ca` secret.

## Wildcard Certificate Generation

To support HTTPS, the administrator must generate and install a
wildcard certificate for `*.<namespace>.svc.cluster.local`, where
`<namespace>` is the Kubernetes namespace in use, such as `feldera`.

:::info

When using `kubectl` port forwarding, it is also useful to add
hostname `localhost` and/or `127.0.0.1` to the certificate.

:::

This section and the next one create the certificates by hand.  Skip
them if you [use cert-manager](#using-cert-manager).

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
   curl --cacert tls.crt 'https://localhost:8080/v0/pipelines'
   ```

   It is no longer possible to reach the endpoints via HTTP.
   Attempting to do so will result in a failure to parse the HTTPS response;
   this will for example elicit error messages such as
   "localhost sent an invalid response" (Chrome) or
   "Received HTTP/0.9 when not allowed" (curl).
   Use `https://` as the protocol.

## Client certificates

With the Helm value `mtls.enabled: true`, the compiler, runner, pipeline and
coordinator servers require a client certificate that the private CA signed,
and admit each caller only to its own routes:

| Caller | Certificate | Compiler | Runner | Pipeline `<p>` and its coordinator |
|---|---|---|---|---|
| API server, runner, compiler replicas | control-plane certificate (URI SAN `urn:feldera:control-plane`), issued by cert-manager from the private CA | all routes | all routes | all routes |
| Pipeline `<id>` | pipeline certificate (URI SAN `urn:feldera:pipeline:<id>`), issued by the runner from the private CA | download its own binary and program info, `/healthz` | `/healthz` | all routes if `<id>` is `<p>` (the multihost coordinator), else none |
| External client, such as a sidecar | certificate with one URI SAN listed in `mtls.extraClientUris`, issued from the private CA | `/healthz` | `/healthz` | all routes |
| No certificate | none | `/healthz` | `/healthz` | none |

Clients that reach a pipeline directly, instead of through the API server,
are refused, except [external clients](#external-clients).  Pipelines on an older runtime version (`runtime_version`) do
not check client certificates.

The hosts of a multihost pipeline also use TLS to exchange data with each
other.  Each host presents its pipeline certificate and admits only a host
whose certificate names the same pipeline.  To exchange data in plaintext,
set `dev_tweaks.tls_exchange` to `false` in the pipeline's runtime
configuration.

The servers trust only the private CA, the first certificate in `tls.crt` of
the `caSecretRef` secret.  They refuse certificates from the root or from other
CAs under the same root, so the root may also sign certificates for other uses.

Requirements:

- `httpsSecretRef` and `caSecretRef` are set.
- [cert-manager](https://cert-manager.io) runs in the cluster.  The chart
  creates an `Issuer` for the private CA and a `Certificate` for the control
  plane.  The components read a renewed certificate without a restart.

```
httpsSecretRef: "feldera-https-config"
caSecretRef: "feldera-ca-config"
mtls:
  enabled: true
```

Pipelines get their certificate when they start, so restart pipelines that
were running before you set `mtls.enabled`.  Otherwise a pipeline pod that
Kubernetes moves to another node cannot download its binary.

### External clients

To let a client in the cluster, such as a sidecar, call pipelines directly,
list the URI SAN of its certificate in `mtls.extraClientUris`, and issue the
certificate from the private CA, for example with the chart's
`<release>-private-ca` Issuer:

```
mtls:
  enabled: true
  extraClientUris:
    - "urn:example:sidecar"
```

```
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: sidecar-client
spec:
  secretName: sidecar-client
  uris:
    - urn:example:sidecar
  usages:
    - digital signature
    - client auth
  issuerRef:
    kind: Issuer
    name: <release>-private-ca
```

A listed client reaches every pipeline, like the control plane.  The URI must
match exactly, and the certificate must have no other URI SAN.  URIs under
`urn:feldera:` are refused, because Feldera issues those to its own
components.
