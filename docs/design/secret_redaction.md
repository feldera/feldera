# Keeping secrets out of diagnostics

## Overview

A connector configuration can hold credentials. They arrive in two ways:

- as a secret reference, `${secret:kubernetes:pg/password}`, which the pipeline
  resolves when it starts;
- inline, written in the `connectors` property of the SQL program.

A user who may reference a secret is not always allowed to read it. So a
resolved value must not appear in anything the pipeline or the API server
reports: error responses, `/stats`, pipeline events, logs, and support bundles.

Three layers do this. Each one assumes that the layer before it can fail.

| Layer | What it does | Where it runs |
|---|---|---|
| 1. Safe sources | A diagnostic is built from reviewed fields only. It never prints a URI, an option value, or a whole configuration. | pipeline, API server |
| 2. Secret registry | The pipeline records each secret value it knows, and replaces that value in all text that leaves the process. | pipeline |
| 3. Credential scrub | A pattern match replaces URL passwords and `password=` values in text whose author did not know the secrets. | pipeline, API server |

Layer 1 is the fix. Layers 2 and 3 are safety nets: new code must not rely on
them.

## Data flow

```
 SQL program                      Secret store
 (inline values)                  (referenced values)
      |                                 |
      v                                 v
 +-----------------------------------------------------------+
 | Pipeline process                                          |
 |                                                           |
 |  secret resolver ---- registers values ---> registry (2)  |
 |        |                                        |         |
 |        v                                        |         |
 |  connector ---> its own errors and logs (1)     |         |
 |        |                                        |         |
 |        +------> library errors ---> scrub (3) --+         |
 |                                                 |         |
 |             all outgoing text is filtered <-----+         |
 +-----------------------------------------------------------+
      |               |                |              |
      v               v                v              v
 error response    /stats        panic message     log lines
      |               |
      v               v
 +-----------------------------------------------------------+
 | API server (does not know any secret value)               |
 |                                                           |
 |  scrub (3) when it stores a diagnostic, when it reads one |
 |  back, and when it collects or exports a support bundle   |
 +-----------------------------------------------------------+
      |                         |
      v                         v
 deployment error,          support bundle
 status, events
```

## Layer 1: safe sources

Each connector reports the endpoint name, the option name, and a URL without
its credentials. It does not format the configuration or the library's own
message where that message can repeat the input.

| Helper | Use |
|---|---|
| `feldera_types::display_url` | Shows a URL without userinfo, query string and fragment. |
| `feldera_types::config_diagnostics` | Reports a configuration parse error without the rejected value. |
| `Debug` of the Kafka and Avro configurations | Prints option and header names, with `[REDACTED]` as each value. |

## Layer 2: secret registry

Code: `crates/feldera-types/src/secret_redaction.rs`.

Two callers add values to the registry:

- The secret resolver (`secret_resolver.rs`) adds each value it resolves,
  together with its reference.
- Each connector configuration lists its credential fields through the
  `SensitiveValues` trait (`sensitive.rs`). These are added before the connector
  is created, so that inline credentials are covered too.

The registry then replaces a value wherever it stands as a whole token:

| The text holds | It becomes |
|---|---|
| a referenced value, as resolved | `${secret:kubernetes:pg/password}` |
| a derived form of it: percent-encoded, escaped, or the password of a URL value | `[REDACTED ${secret:kubernetes:pg/password}]` |
| a declared credential with no reference | `[REDACTED]` |

Values shorter than 8 characters are not registered: replacing them would
damage ordinary words.

The filter is applied at every exit of the pipeline process:

| Exit | Code |
|---|---|
| Error responses | `ErrorResponse` in `feldera-types/src/error.rs` and `connectors/src/server/error.rs` |
| Connector errors in `/stats` | `connectors/src/controller/stats.rs` |
| Panic messages | `connectors/src/panic.rs` |
| Log lines, text and JSON | `feldera-observability/src/log_redaction.rs`, installed in `connectors/src/server.rs` |

## Layer 3: credential scrub

Code: `crates/feldera-types/src/credential_scrub.rs`.

The scrub does not know any secret. It looks for the shape of a credential: the
password in `scheme://user:password@host`, the value of a `password=`
parameter, and the quoted value of a rejected Kafka option. It replaces that
part with `[REDACTED]` and keeps the text around it. If it cannot tell where a
credential ends, it cuts the text off at that point.

It has two users:

- `LibraryError` (`feldera-types/src/library_error.rs`) wraps an error from a
  driver or client library where it enters our code. Its text passes through
  the scrub, then through the registry. Today it wraps the Postgres driver.
- The API server (`pipeline-manager/src/diagnostic_redaction.rs`). It does not
  resolve secrets, so it has no registry. A pipeline on an older runtime can
  still send it a connection string in an error. The server scrubs a diagnostic
  when it stores it, when it reads it back, and when it collects or exports a
  support bundle.

## Limits

- Inline values are part of the SQL program. They show as written in the
  program, in compiler messages, and in the pipeline configuration of a support
  bundle.
- The registry matches text. It does not find a value in a form it does not
  know, or a value shorter than 8 characters.
- A password with both an unencoded `@` and whitespace is scrubbed only up to
  its first `@`.
- Library errors of connectors other than Postgres are not wrapped yet. The
  registry still filters them.

The user documentation lists what each output shows:
`docs.feldera.com/docs/connectors/secret-references.md`.
