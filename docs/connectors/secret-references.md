# Secret references

Rather than directly supplying a secret (e.g., passwords, PEM, etc.) in the connector
configuration as a string, it is possible to refer to (externalize) them. This mechanism
in Feldera is called a **secret reference**.

A secret reference is a string in the connector configuration JSON which takes a specific format:

```
${secret:<provider>:<identifier>}
```

It refers to an identifiable secret provided by a provider. Feldera's control plane mounts the secret
into the pipeline. When the pipeline initializes, it will replace the secret references in the
configuration with their values. We currently only support a single secret provider, Kubernetes.

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

:::info

In the Enterprise edition, it is possible to limit secret references to only
the Secrets on an allowlist by setting in the Helm chart
`kubernetesRunner.connectorKubernetesSecrets.enableAllowlist` to `true` and 
`kubernetesRunner.connectorKubernetesSecrets.allowlist` to the list of allowed
Secret names. See the Helm chart `values.yaml` for more information.

:::

## Restrictions

- The secret name may only contain lowercase alphanumeric characters or hyphens, must start and end
  with a lowercase alphanumeric character and can be at most 63 characters long
- The secret data key has the same restrictions except that it can be at most 255 characters long
- Only when the pipeline is started will secret references be checked and resolved
- It is not possible to have any plain string value which starts with `${secret:` and ends with `}`
  without it being identified to be a secret reference.
- Only string values in the connector configuration JSON under `transport.config` and `format.config`
  can be identified to be secret references (this excludes keys), for example (secret named `a` at
  data key `b` has value `value`):
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
- It is not possible to specify a secret value type other than string
- It is not possible to specify a secret as a substring, for example
  `abc${secret:kubernetes:a/b}def` does not work
