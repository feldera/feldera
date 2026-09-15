# Private Cargo Registry

:::warning
This feature is experimental and may have breaking changes in future releases.
:::

:::info
This feature requires Feldera 0.349.0 or later.
:::

By default, UDF dependencies come from [crates.io](https://crates.io).

This guide shows how to enable the compiler server to use crates from a private package registry.

We will be using AWS EKS and CodeArtifact, and assume they are already set up.

---

## Setting up the IAM OIDC provider

We need to set up the IAM OIDC provider for the EKS cluster. The official AWS guide to set this up can be found [here](https://docs.aws.amazon.com/eks/latest/userguide/enable-iam-roles-for-service-accounts.html).

To see if you already have the provider set up, run the following:

```bash
oidc_id=$(aws eks describe-cluster --name <cluster> --query "cluster.identity.oidc.issuer" --output text | cut -d '/' -f 5)
aws iam list-open-id-connect-providers | grep $oidc_id | cut -d "/" -f4
```

If this command returns any output, the provider is already set up.

---

## Setting up the IAM role

We need to set up the IAM role for the compiler server pod to assume. The official AWS guide to set this up can be found [here](https://docs.aws.amazon.com/eks/latest/userguide/associate-service-account-role.html).

The role will require permissions to access CodeArtifact ([reference](https://docs.aws.amazon.com/codeartifact/latest/ug/security_iam_id-based-policy-examples.html)):

```json
{
   "Version":"2012-10-17",
   "Statement": [
      {
         "Action": [
            "codeartifact:Describe*",
            "codeartifact:Get*",
            "codeartifact:List*",
            "codeartifact:ReadFromRepository"
         ],
         "Effect": "Allow",
         "Resource": "*"
      },
      {
         "Effect": "Allow",
         "Action": "sts:GetServiceBearerToken",
         "Resource": "*",
         "Condition": {
            "StringEquals": {
               "sts:AWSServiceName": "codeartifact.amazonaws.com"
            }
         }
      }
   ]
}
```

Then the role needs to be associated with Feldera compiler server pod's Kubernetes service account:

```json
{
  "Version":"2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<account id>:oidc-provider/<oidc provider>"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "<oidc provider>:aud": "sts.amazonaws.com",
          "<oidc provider>:sub": "system:serviceaccount:feldera:<service account>"
        }
      }
    }
  ]
}
```

You can get the OIDC provider by running the following:

```bash
aws eks describe-cluster --name <cluster> --region <cluster region> --query "cluster.identity.oidc.issuer" --output text | sed -e "s/^https:\/\///"
```

By default, the service account name is `feldera-compiler-server`.

---

## Configuring the compiler server

We need to configure the compiler server's service account and Cargo config through Helm values.

We will need the CodeArtifact's Cargo index URL, which can be found by running the following:

```bash
aws codeartifact get-repository-endpoint --domain <domain> \
    --repository <repository> --format cargo \
    --region <codeartifact region> --output text
```

The index URL should look something like this:

```text
https://feldera-123456789012.d.codeartifact.us-west-2.amazonaws.com/cargo/private-cargo/
```

Then in the Helm values file:

```yaml
compilerServer:
  serviceAccountAnnotations:
    - key: "eks.amazonaws.com/role-arn"
      value: "<role arn>"
  cargo:
    config: |
      [registries.<custom name>]
      index = "sparse+<index url>"
      credential-provider = "cargo:token-from-stdout aws codeartifact get-authorization-token --domain <domain> --domain-owner <account id> --region <codeartifact region> --profile workload-identity --query authorizationToken --output text"
```

Note the `--profile workload-identity` in the `credential-provider` value. This is the profile for the AWS role that was set up in previous steps and must be included.

Upgrade the release, then check that the compiler server pod picked up the credentials by running:

```bash
kubectl logs -n feldera <release>-compiler-server-0 | grep workload-identity
```

and verifying that the logs contain the message

```text
Writing the 'workload-identity' AWS profile
```

---

## Using the crate in a pipeline

Add the dependency to the pipeline's `udf.toml` using the same registry name configured in the Helm values.

```toml
my-private-crate = { version = "0.1.0", registry = "<custom name>" }
```
