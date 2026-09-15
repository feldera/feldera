# Private Cargo Registry (experimental)

By default, UDF dependencies come from [crates.io](https://crates.io). This guide
points the compiler at a private AWS CodeArtifact registry instead, so pipelines
can depend on your own crates.

Requires Feldera 0.349.0 or later. This feature is experimental: the
`compilerServer` values and the `workload-identity` profile name may change
incompatibly across Feldera upgrades.

---

## IAM role

The compiler needs a role assumable by the ServiceAccount the chart creates:
`<release>-compiler-server` in the release namespace, which is
`feldera-compiler-server` in namespace `feldera` if you followed the
[installation guide](./helm-guide.md). Grant the role:

| Action | Resource |
|---|---|
| `codeartifact:GetAuthorizationToken` | `arn:aws:codeartifact:us-west-2:123456789012:domain/feldera` |
| `codeartifact:GetRepositoryEndpoint`, `codeartifact:ReadFromRepository` | `arn:aws:codeartifact:us-west-2:123456789012:repository/feldera/private-cargo` |
| `sts:GetServiceBearerToken` | `*` |

`sts:GetServiceBearerToken` accepts no resource other than `*`. Scope it with a
condition on `sts:AWSServiceName` equal to `codeartifact.amazonaws.com`.

---

## Configure the compiler

Get the repository's cargo index URL:

```bash
aws codeartifact get-repository-endpoint --domain feldera --repository private-cargo \
    --format cargo --region us-west-2 --output text
```

Set that URL and the role ARN in your chart values:

```yaml
compilerServer:
  serviceAccountAnnotations:
    - key: "eks.amazonaws.com/role-arn"
      value: "arn:aws:iam::123456789012:role/feldera-compiler-codeartifact-read"
  cargo:
    config: |
      [registries.private-cargo]
      index = "sparse+https://feldera-123456789012.d.codeartifact.us-west-2.amazonaws.com/cargo/private-cargo/"
      credential-provider = "cargo:token-from-stdout aws codeartifact get-authorization-token --domain feldera --domain-owner 123456789012 --region us-west-2 --profile workload-identity --query authorizationToken --output text"
```

Keep `--profile workload-identity` as written: that is the profile the compiler
creates from the ServiceAccount's identity.

Upgrade the release, then check that the compiler picked the identity up:

```bash
kubectl logs -n feldera feldera-compiler-server-0 | grep workload-identity
```

---

## Use the crate in a pipeline

Add the dependency to the pipeline's `udf.toml`, naming the registry exactly as
the chart values do:

```toml
my-private-crate = { version = "0.1.0", registry = "private-cargo" }
```

See the [Helm chart reference](./helm-chart-reference.md#compiler) for the
`compilerServer` values.
