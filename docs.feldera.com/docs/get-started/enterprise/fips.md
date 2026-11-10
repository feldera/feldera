# FIPS 140-3

The container images of the Enterprise edition run their cryptography on FIPS 140-3
validated cryptographic modules. This page lists the modules, shows how to run Feldera in
FIPS mode, and states what FIPS mode covers and what it leaves to the operator.

Feldera uses validated modules, Feldera itself is not a validated module.

## Cryptographic modules

| Module | Version | CMVP certificate | Used by | Used for |
|--------|---------|------------------|---------|----------|
| AWS-LC 3 Cryptographic Module (dynamic) | AWS-LC FIPS 3.1.0 | [#5298](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5298), Level 1 | Control-plane and pipeline binaries | TLS and mutual TLS of every Feldera server and client, pipeline certificates, API keys, checksums of programs and binaries |
| AWS-LC 3 Cryptographic Module (static) | AWS-LC FIPS 3.1.0 | [#5314](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5314), Level 1 | `librdkafka` | Kafka connectors, which FIPS mode [does not allow yet](#connectors) |
| Go Cryptographic Module | v1.0.0 | [#5247](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5247), Level 1 | `rclone` | [Checkpoint synchronization](/pipelines/checkpoint-sync) to object storage |

Each image ships the dynamic AWS-LC module as
`/usr/local/lib/libaws_lc_fips_0_13_11_crypto.so`. The module tests its integrity and its
algorithms when a process loads it, and the process does not start if a test fails.

The libraries that connect Feldera to the modules:

| Library | Version | Role |
|---------|---------|------|
| `aws-lc-rs` | 1.17.1 | Rust interface to AWS-LC, built with its `fips` feature |
| `aws-lc-fips-sys` | 0.13.11 | Builds AWS-LC FIPS 3.1.0 from the unmodified sources |
| `rustls` | 0.23.46 | TLS, in its FIPS mode. A [Feldera fork](https://github.com/feldera/rustls/tree/fips-aws-lc-3) of this release that builds with `aws-lc-rs` 1.17 |
| `rustls-webpki` | 0.103.16 | Certificate verification. A [Feldera fork](https://github.com/feldera/webpki/tree/fips-aws-lc-3) of this release, for the same reason |
| `rclone` | 1.76.0 | Built with Go 1.27.3 and `GOFIPS140=v1.0.0`, which makes FIPS mode (`GODEBUG=fips140=on`) its default |

The forks exist to keep the security fixes of the current releases: the last upstream
releases that build with `aws-lc-rs` 1.17, `rustls` 0.23.43 and `rustls-webpki` 0.103.13,
have known vulnerabilities. The forks change one thing: ML-DSA, which AWS-LC FIPS 3.1.0
does not provide, is off. Feldera stays on AWS-LC FIPS 3.1.0 until a later AWS-LC version
receives a CMVP certificate.

## Run Feldera in FIPS mode

Set `fips.enabled` in the [Helm chart values](helm-chart-reference.md#fips) and configure
[HTTPS](https.md):

```yaml
fips:
  enabled: true
# Certificate and key that the Feldera components serve
httpsSecretRef: feldera-https-config
# Private CA that signs a certificate for each pipeline
caSecretRef: feldera-ca-config
# Feldera components authenticate each other with client certificates
mtls:
  enabled: true
```

| With `fips.enabled: true` | Effect |
|---------------------------|--------|
| The chart requires `httpsSecretRef` | Feldera components exchange no data in plaintext |
| The API server, the compiler server, the Kubernetes runner, the pipelines and their coordinators start with `--fips` | Each one exits unless its TLS runs on the module |
| The chart refuses the `runtime_version` unstable feature | A pipeline on another runtime version builds without the module, and `cargo` downloads its crates with another TLS library |
| The chart refuses `parallelCompilation.sccache.enabled` | `sccache` transfers build artifacts with its own TLS library |
| The chart requires `--fips` in a `codeartifact-token` command of `compilerServer.cargo.config` | `codeartifact-token` exits unless its TLS runs on the module, and it refuses `--no-verify-ssl` |
| The chart refuses `pipeline.allowProfiling` | The security policies of the modules forbid profilers and debuggers on a running module |
| The runner checks the connectors of each pipeline | A pipeline that uses a connector outside the [FIPS list](#connectors) fails to start |

`caSecretRef` and `mtls.enabled` are not required, but they give each pipeline its own
certificate and make the compiler server, the runner and the pipelines admit only
authenticated Feldera components.

### Connectors {#connectors}

In FIPS mode a pipeline may use these connectors:

| Connector | Cryptography |
|-----------|--------------|
| [HTTP input and output](/connectors/sources/http) (`http_input`, `http_output`) | TLS of the pipeline server, on the module |
| [URL input](/connectors/sources/http-get) (`url_input`) | TLS client, on the module |
| `file_input`, `file_output`, `datagen` | None |

FIPS mode denies every other connector. The TLS of these connectors runs on the module,
except where the table says otherwise, but each one has a path that computes a signature
or a password proof with other code:

| Connector | Cryptography outside the module |
|-----------|---------------------------------|
| `kafka_input`, `kafka_output` | AWS MSK IAM tokens, GSSAPI, and the SCRAM client nonce |
| `s3_input`, `dynamodb_output` | AWS Signature Version 4 |
| `delta_table_input`, `delta_table_output` | Request signing for S3, Azure and Google Cloud Storage |
| `iceberg_input` | Request signing for S3 and Google Cloud Storage; decryption of encrypted tables |
| `postgres_input`, `postgres_cdc_input`, `postgres_output` | Password authentication (SCRAM-SHA-256); `postgres_input` has no TLS |
| `nats_input`, `nats_output` | NKey signatures of credentials files |
| `redis_output` | No TLS |
| `pub_sub_input`, `s2_input`, `s2_output` | Not yet verified |

:::info
If you need one of these connectors in FIPS mode, contact Feldera support. Support advises
how to run the connector in a FIPS compliant way, or provides a FIPS compliant alternative
in the Enterprise edition.
:::

## What FIPS mode does not cover

The chart cannot check the items below. Review them for your deployment.

| Item | Detail | What to do |
|------|--------|------------|
| Control-plane database | The connection to PostgreSQL uses TLS on the module when [TLS is configured](helm-chart-reference.md#external-postgresql-tls), note that the password proof (SCRAM-SHA-256) is computed outside the module | Configure TLS for the database connection |
| Checkpoint synchronization | `rclone` in FIPS mode runs TLS and request signing on the Go module. It still computes MD5 checksums, which S3 and Google Cloud Storage use to detect corrupted transfers, not for security | None |
| Private Cargo registry | With `--fips`, `codeartifact-token` runs the HTTPS connections to AWS CodeArtifact and AWS STS on the module. It computes the AWS request signature (Signature Version 4, HMAC-SHA-256) outside the module; the signed request travels inside the TLS connection | Add `--fips` to the [`codeartifact-token` command](private-cargo-registry.md) |
| Rust UDFs | Crates that a UDF adds may bring their own cryptography | Set `pipeline.allowRustUdfs: false`, or review the crates |
| Compiler server tools | The image holds the crates of the precompiled dependencies. `cargo` and `git` download the crates that a Rust UDF adds, with their own TLS libraries. They do not handle pipeline data | Set `pipeline.allowRustUdfs: false`, or review the crates |
| Clients | Browsers, the `fda` CLI, the Python SDK and ingress controllers use their own TLS libraries | Use FIPS validated clients and ingress |
| Init containers and sidecars | Containers that you add to pipeline pods are outside Feldera | Set `pipeline.allowInitContainers: false`, or review the containers |
| SQL functions | `MD5` and other hash functions in SQL programs run outside the module to process data | Do not use them to protect sensitive data |
| Debugging tools | The security policies of the modules forbid `ptrace`, `gdb`, `strace` and similar tools on a running module. The chart refuses `pipeline.allowProfiling`, but it cannot see tools that you attach yourself | Do not attach debuggers to Feldera containers |
| Advisories against AWS-LC FIPS 3.1.0 | Scanners report CVE-2026-3337 (timing of AES-CCM tag verification) and CVE-2026-4428 (CRL distribution point check) for `aws-lc-fips-sys` 0.13.11. Later AWS-LC versions fix both but have no certificate. Feldera uses neither code path: its TLS offers no AES-CCM cipher suite, and the X.509 code of AWS-LC, which holds the CRL check, does not run. `rustls-webpki` parses certificates and builds the chain, and verifies every signature with the module | None |
| Operating environment | The certificates list the environments that the laboratories tested: Amazon Linux 2023 on AWS Graviton4 and Intel Xeon for AWS-LC. Feldera images use Ubuntu 24.04. The CMVP makes no statement about a module in an environment that its certificate does not list | Work with Feldera support on finding a host and image combination that meets your requirements |

## Verify a deployment

Each component logs the state of its TLS at startup:

```bash
kubectl logs -n feldera deploy/feldera-api-server | grep "FIPS is enabled"
kubectl logs -n feldera statefulset/feldera-compiler-server | grep "FIPS is enabled"
kubectl logs -n feldera deploy/feldera-kubernetes-runner | grep -e "FIPS is enabled" -e "FIPS mode"
```

| Log line | Meaning |
|----------|---------|
| `Rustls HTTPS server config: FIPS is enabled for connections.` | The server offers only FIPS approved TLS versions, cipher suites and key exchange groups, all on the module |
| `Rustls startup default provider: FIPS is enabled for cryptography.` | A pipeline runs its TLS on the module |
| `FIPS mode: pipelines are allowed to use only these connectors: ...` | The runner enforces the FIPS connector list |

To check the modules inside an image:

```bash
# The AWS-LC module reports its version when it is loaded
kubectl exec -n feldera deploy/feldera-api-server -- \
  grep -a -o -m1 "AWS-LC FIPS [0-9.]*" /usr/local/lib/libaws_lc_fips_0_13_11_crypto.so
# rclone reports the Go module and its default mode
kubectl exec -n feldera pipeline-<pipeline-id>-0 -- sh -c \
  'grep -a -o -m1 "GOFIPS140=v[0-9.]*" /usr/local/bin/rclone; grep -a -o -m1 "fips140=on" /usr/local/bin/rclone'
```
