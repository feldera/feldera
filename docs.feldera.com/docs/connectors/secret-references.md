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
Connector error messages are written to checkpoints; see [Diagnostics](#diagnostics)
for how resolved values are kept out of them.

Use environment variables for non-sensitive deployment configuration only.
Storing secrets in environment variables is generally discouraged; use a dedicated secret manager or secret store instead.

## Diagnostics

A user who may reference a secret is not necessarily allowed to read it. The
pipeline therefore keeps resolved values out of what it reports:

| Output | What is shown instead of a resolved value |
|---|---|
| Pipeline log | the reference, `${secret:...}`, for the value as resolved; `[REDACTED ${secret:...}]` for a derived form of it, such as a percent-encoded or escaped copy or the password of a URL value |
| `deployment_error`, pipeline events, `/status` | the reference, `${secret:...}`, for the value as resolved; `[REDACTED ${secret:...}]` for a derived form of it, such as a percent-encoded or escaped copy or the password of a URL value. This applies to a pipeline on a runtime with this fix |
| Connector errors in `/stats` and in the support bundle | the reference, `${secret:...}`, for the value as resolved; `[REDACTED ${secret:...}]` for a derived form of it, such as a percent-encoded or escaped copy or the password of a URL value. This applies to a pipeline on a runtime with this fix |
| Connection errors and debug log lines of the Postgres input, Delta Lake and Kafka connectors, and of the Avro format | the endpoint name and option names only, never the URI, option value, or configuration |
| Format and connector configuration errors, and the pipeline startup summary | the error category, the expected values and field names from the schema, the endpoint, transport and format names; never a rejected value or a configuration dump |

The API server does not resolve secret references, but an older runtime can
send it an error containing a resolved value. The server replaces the
password of a URL and the value of a `password=` parameter in stored
deployment errors, returned status details, and exported diagnostics with
`[REDACTED]` and keeps the text around them, including for quoted and
escaped keyword passwords that span lines. Where the end of a credential
cannot be established, such as a password with whitespace in it, a query
value with nothing after it, or a percent-encoded parameter name, the
diagnostic is cut off at the credential: the text before it stays. A URL
that holds an `@` is taken as complete, so that an email address later in a
log does not cut the log off. A password with both an unencoded `@` and
whitespace is therefore replaced only up to its first `@`:
`postgres://u:pa@ss word@host/db` shows as
`postgres://u:[REDACTED]@ss word@host/db`. Percent-encode such a password
(`pa%40ss%20word`), and it is replaced whole. This also
covers failure reasons in support bundle metadata and collections stored
before the fix. Error codes and endpoint identities in separate fields are
preserved.

Format configuration errors report a safe error category and a corrective
action instead of quoting the rejected value, including for short secrets.
JSON log fields are redacted before serialization adds JSON escaping.

An error from a driver or client library can echo the connection string or
the option value it was given. The Postgres connectors show errors from the
Postgres driver through a wrapper whose text has connection passwords,
`password=` values and rejected Kafka option values removed and registered
secret values replaced, so a log line or a message that includes the error
is safe. The raw error is kept for classification only. Errors from the
replication library of the Postgres CDC connector, and library errors in
other connectors, still show as the library writes them, within the
registry's limits.

The pipeline also registers values from declared credential fields and replaces
matching diagnostic text with `[REDACTED]`. The same length and token limits
apply. A Secret reference remains protected regardless of its destination field,
including usernames, topics, and broker addresses; its reference takes precedence
over the generic marker when a value appears in both forms. A URL with
credentials in it is itself a declared credential, so a diagnostic that
copies the whole URL shows `[REDACTED]`, not the reference inside it.

| Connector or format | Additional credential fields covered in diagnostics |
|---|---|
| Kafka | SASL password, TLS private key and key passwords, OAuth client secret and JWT assertion private key/passphrase; also fault-tolerance consumer/producer options |
| Postgres and Redis | the credentials of a connection string: a URL with userinfo or a query as a whole, and the `password` and `sslpassword` values of a libpq keyword string; a connection string without credentials stays visible; Postgres inline TLS client key |
| S3 and DynamoDB | AWS secret access key and credential-bearing endpoint URLs |
| Delta Lake | Known object-store secret keys, session/bearer tokens, SAS tokens, client secrets, service-account keys, customer encryption keys, and Unity catalog access tokens and client secrets, including supported aliases |
| Iceberg | Catalog credentials/tokens, authentication headers, and known FileIO credential keys |
| NATS | Inline credentials, JWT, NKey seed, token, password, and credential-bearing server URL |
| S2 and Pub/Sub | Authentication token and inline credentials, respectively |
| URL input and Avro | Credential-bearing URLs, including token-only userinfo; Avro registry password, authorization token, credential-bearing proxy URL, and `Authorization`, `Proxy-Authorization`, or `X-API-Key` headers |

Registration preserves the configuration supplied to connectors. It does not
redact SQL or operational configuration in support bundles. Unknown option keys
and custom headers are not covered merely because their names resemble a
credential; source diagnostics continue to omit opaque option values.

The following limits apply:

- The pipeline log, `/status` and `/stats` show a resolved value of 8 bytes
  or more as its reference, where the value stands as a whole word or token.
  A shorter value is shown as it is.
- A secret whose value is a common word or a non-sensitive value, such as a
  user name or a broker address, is replaced by its reference in every
  diagnostic that uses the word on its own. Keep such values out of secrets,
  or expect them to read as references.
- A value that a connector transforms before printing it, other than trimming,
  percent-encoding, or escaping, may not be recognized.
- Environment references are not registered merely because they came from
  the environment. A value in a declared credential field receives the same
  supplemental treatment as an inline value. Use Secret references for secrets.
- Pipelines started on a runtime from before this fix (see the
  [changelog](/changelog)) do not apply these rules. Restart such pipelines
  after an upgrade.
- A connector error message written to a checkpoint by an older runtime can
  contain the resolved value. After the upgrade, the restored message shows the
  reference if the secret still has that value. A rotated value is not
  recognized.
- A value that is part of SQL, UDF code, an endpoint name, or table data is
  shown as written.
- A value written inline in the `connectors` property is SQL text, not a
  resolved value. It shows as written in the program and in compiler messages
  and their SQL excerpts. The support bundle exports the pipeline
  configuration as supplied, and its connector settings stay when the bundle
  is collected without source code. Use a secret reference for a credential.

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
