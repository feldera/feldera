# HTTPS

This document describes how to configure the endpoints of Feldera components (API server, compiler,
runner, pipelines) to serve HTTPS in the Enterprise edition.

For Feldera to support HTTPS with single-host pipelines, the
administrator must configure Feldera with a wildcard
certificate and a private key for
its main DNS domain, e.g. for `*.feldera.svc.cluster.local`.  The
Feldera API server, compiler runner, pipeline runner, pipeline
stateful sets, and other top-level components share this certificate
and key.

For Feldera to also support HTTPS with multihost pipelines, the
administrator must additionally configure Feldera with a private CA
certificate chain and private key.  This allows the Feldera pipeline runner to generate keys
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

This page uses [cert-manager](https://cert-manager.io) to issue and
renew these certificates.  Certificates from another source must meet
these requirements:

- The server certificate covers `*.<namespace>.svc.cluster.local`, and
  `localhost` and `127.0.0.1` if you use `kubectl` port forwarding.
- In the server certificate secret, `tls.crt` holds the complete chain,
  from the leaf certificate to the root CA, and `tls.key` holds only the
  private key of the leaf certificate.
- In the private CA secret, `tls.crt` starts with the private CA
  certificate, followed by its chain, and `tls.key` holds the private CA
  key in PKCS#8 format.

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
   # Wildcard server certificate of the API server, compiler, runner, and pipelines.
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

4. Set the secrets in the Helm values, as in [Configure
   Kubernetes](#configure-kubernetes).  To also enable
   [client certificates](#client-certificates), add `mtls.enabled: true`:

   ```yaml
   httpsSecretRef: "feldera-https-config"
   caSecretRef: "feldera-ca-config"
   # mtls:
   #   enabled: true
   ```


To trust the server certificate from outside the cluster, for example
with `curl` over port forwarding, use the root CA:

```
kubectl get secret -n feldera feldera-root-ca -o jsonpath='{.data.tls\.crt}' | base64 -d > feldera-root-ca.crt
curl --cacert feldera-root-ca.crt 'https://localhost:8080/v0/pipelines'
```

If an ingress or a Gateway API gateway forwards traffic to the API
server, configure it to connect over HTTPS and to verify the API
server against the `feldera-root-ca` secret.

### Renew the server certificate

cert-manager renews the server certificate (`feldera-https-config`) in
the secret when `renewBefore` starts, 30 days before it expires in the
example above.  The API server, the compiler, and the runner read the
certificate only when they start, so they keep the old certificate until
they restart.  A Feldera upgrade restarts them.

If the old certificate expires before a restart:

- Clients outside the cluster cannot connect to the API server.  For
  example, `curl` fails with `certificate has expired`.
- A pipeline that starts cannot download its binary from the compiler.
  It stays in the `Unavailable` state, and its log shows
  `Issued certificate has expired`.
- Pipelines that are already running continue to run.

To use the renewed certificate, do these steps before the old
certificate expires.  If it has already expired, the same steps fix it.

1. Get the time of the next renewal:

   ```
   kubectl get certificate -n feldera feldera-https-config -o jsonpath='{.status.renewalTime}'
   ```

2. After that time, restart the control plane.  Replace `<release>`
   with the name of your Helm release:

   ```
   kubectl rollout restart -n feldera \
     deployment/<release>-api-server \
     deployment/<release>-kubernetes-runner \
     statefulset/<release>-compiler-server
   ```

   With `compilerAutoscaling.enabled`, also restart
   `deployment/<release>-compiler-artifact-server`.

3. Stop and start pipelines that are in the `Unavailable` state.
   Pipelines that run correctly do not need a restart.

This applies only to the server certificate.  The control plane client
certificate that the chart issues for [client
certificates](#client-certificates) takes effect without a restart.

## Configure Kubernetes

1. Provide in the Helm installation the reference for the
   `httpsSecretRef` and, for multihost support, `caSecretRef`, value.

   Without the private CA:

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

   With the private CA:

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

2. (Optional) Port forward:
   ```
   kubectl port-forward -n feldera svc/feldera-api-server 8080:8080
   ```
   ... and then open in the browser: https://localhost:8080

   Clients must trust the root CA to connect without certificate
   errors.  For example, with `curl` and the root CA from [Using
   cert-manager](#using-cert-manager):
   ```
   curl --cacert feldera-root-ca.crt 'https://localhost:8080/v0/pipelines'
   ```

   It is no longer possible to reach the endpoints via HTTP.
   Attempting to do so will result in a failure to parse the HTTPS response;
   this will for example elicit error messages such as
   "localhost sent an invalid response" (Chrome) or
   "Received HTTP/0.9 when not allowed" (curl).
   Use `https://` as the protocol.

:::note

With `caSecretRef` and `kubernetesRunner.enablePerPipelineNamespace`, a
pipeline in another namespace needs the server certificate in its own
namespace. The pipeline uses it to verify the control plane. Without it,
the pipeline pod stays in `ContainerCreating` with the event
`secret "feldera-https-config" not found`. Copy only the certificate,
not the private key, into each pipeline namespace:

```
kubectl get secret -n feldera feldera-https-config -o jsonpath='{.data.tls\.crt}' | base64 -d > tls.crt
kubectl create secret generic feldera-https-config -n <pipeline-namespace> --from-file=tls.crt=tls.crt
```

Copy the certificate again each time it is renewed.

:::

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
  plane.  The components read a renewed control plane certificate without
  a restart.  The server certificate is different: see [Renew the server
  certificate](#renew-the-server-certificate).

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
