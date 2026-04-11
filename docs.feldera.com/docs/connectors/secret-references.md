# Secret references

Rather than directly supplying a secret (e.g., passwords, PEM, etc.) in the connector
configuration as a string, it is possible to refer to (externalize) them. This mechanism
in Feldera is called a **secret reference**.

Feldera supports two types of references in connector configuration strings:

- **Secret references** — resolved from an external secret provider (e.g., Kubernetes):
    ```
    ${secret:<provider>:<identifier>}
    ```

It refers to an identifiable secret provided by a provider. Feldera's control plane mounts the secret
into the pipeline. When the pipeline initializes, it will replace the secret references in the
configuration with their values. We currently only support a single secret provider, Kubernetes.

- **Environment variable references** — resolved from the pipeline process environment:
    ```
    ${env:<name>}
    ```

When the pipeline initializes, it replaces all references in the connector configuration
with their resolved values. Feldera resolves references when a pipeline starts, as well as
each time it resumes. Feldera does not write resolved values to checkpoints or journals.

Use environment variables for non-sensitive deployment configuration only.
Storing secrets in environment variables is generally discouraged; use a dedicated secret manager or secret store instead.

## Kubernetes

### Usage

```
${secret:kubernetes:<name>/<data key>}
```

... in which:
- `<name>` should correspond to a Kubernetes `Secret`
- `<data key>` should correspond to a data key value (i.e., in its `data:` section)

Upon provisioning the pipeline, the runner will mount the Kubernetes Secret data keys
as files at: `/etc/feldera-secrets/kubernetes/<name>/<data key>`.

:::info

In the Enterprise edition, it is possible to limit secret references to only
the Secrets on an allowlist by setting in the Helm chart
`kubernetesRunner.connectorKubernetesSecrets.enableAllowlist` to `true` and
`kubernetesRunner.connectorKubernetesSecrets.allowlist` to the list of allowed
Secret names. See the Helm chart `values.yaml` for more information.

:::

:::info

The automatic resolution of Kubernetes Secrets through mounting is Enterprise-only.
In the Open Source edition, it is still possible to refer to secrets by carefully
mounting them yourself out-of-band at `/etc/feldera-secrets/kubernetes/<name>/<data key>`.

:::

### Example

Suppose we have a Kubernetes secret as follows (`Zmx5aW5nZmlzaA==` = base64-encoded string `flyingfish`)
in the same namespace as the Feldera deployment.

```
apiVersion: v1
kind: Secret
metadata:
  name: example1
data:
  value2: Zmx5aW5nZmlzaA==
```

We can then specify a connector configuration that refers to it using `${secret:kubernetes:example1/value2}`.

```
{
    "transport": {
        "name": "kafka_input",
        "config": {
            ...
            "sasl.password": "${secret:kubernetes:example1/value2}"
        },
    }
    "format": ...
}
```

## Environment variables

### Usage

```
${env:<name>}
```

Here, `<name>` is the name of an environment variable following POSIX naming
rules (letters, digits, and underscores, must start with a letter or underscore).

The reference is resolved at pipeline startup by reading the named variable from the pipeline
process environment. This is useful for injecting configuration values (e.g., hostnames,
credentials) via environment variables set in the `env` field of
[`RuntimeConfig`](/api/patch-pipeline#body-runtime_config) or through the deployment environment.

### Example

```json
{
    "transport": {
        "name": "kafka_input",
        "config": {
            "bootstrap.servers": "${env:KAFKA_BOOTSTRAP_SERVERS}",
            "sasl.password": "${env:KAFKA_SASL_PASSWORD}"
        }
    },
    "format": ...
}
```

### Restrictions

- The environment variable name must follow POSIX rules: only letters (`a`–`z`, `A`–`Z`),
  digits (`0`–`9`), and underscores (`_`), and must start with a letter or underscore
- If the referenced environment variable is not set when the pipeline starts, the pipeline
  will fail to initialize with an error
- It is not possible to have string values starting with `${env:` and ending with `}`
  without them being identified as an environment variable reference

## Restrictions (secret references)

- The secret name may only contain lowercase alphanumeric characters or hyphens, must start and end
  with a lowercase alphanumeric character and can be at most 63 characters long
- The secret data key has the same restrictions except that it can be at most 255 characters long
- Only when the pipeline is started will secret references be checked and resolved
- It is not possible to have any plain string value which starts with `${secret:` and ends with `}`
  without it being identified to be a secret reference.
- Only string values in the connector configuration JSON under `transport.config` and `format.config`
  can be identified to be secret or environment variable references (this excludes keys), for example
  (secret named `a` at data key `b` has value `value`):
    ```
    {
      "transport": {
        "name": "some_transport",
        "config": {
          "${secret:kubernetes:a/b}": "${secret:kubernetes:a/b}",
          "v1": "${secret:kubernetes:a/b}",
          "v2": [ "${secret:kubernetes:a/b}" ]
        }
      },
      "format": {
        "name": "some_format",
        "config": {
          "v3": "${secret:kubernetes:a/b}"
        }
      },
      "index": "${secret:kubernetes:a/b}"
    }
    ```
    ... will be resolved to:
    ```
    {
      "transport": {
        "name": "some_transport",
        "config": {
          "${secret:kubernetes:a/b}": "value",
          "v1": "value",
          "v2": [ "value" ]
        }
      },
      "format": {
        "name": "some_format",
        "config": {
          "v3": "value"
        }
      },
      "index": "${secret:kubernetes:a/b}"
    }
    ```
- Because connector configuration is validated during SQL compilation without secret
  resolution, string values that require certain format for the connector configuration
  to be valid will not allow secret references (enumerations in particular, such as for
  the datagen connector `strategy` field)
- It is not possible to specify a reference value type other than string
- It is not possible to specify a reference as a substring, for example
  `abc${secret:kubernetes:a/b}def` and `abc${env:MY_VAR}def` do not work
