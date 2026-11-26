# Typed secrets for connectors

Proposal for [feldera/cloud#2214](https://github.com/feldera/cloud/issues/2214),
a follow-up to [feldera/cloud#2136](https://github.com/feldera/cloud/issues/2136).

## Summary

- What: a `Secret` type for credential fields, a redactor for each
  endpoint, and a `ConnectorDiagnostic` type that is the only way a
  connector reports an error.
- Why: the compiler then stops the common mistakes, and each endpoint
  makes its own diagnostics safe before they reach the controller.
- What stays: the process-wide registry, as a backstop for logs, panics,
  HTTP errors and older paths. Removing it is not a goal.

```
 connector config   resolver                   connector code        controller
 ----------------   --------                   --------------        ----------
 password: Secret --> endpoint redactor --+--> expose_secret()
                      (and global copy)   |         |
                                          |    driver error
                                          |         |
                                          +--> ConnectorDiagnostic --> status,
                                               (safe text only)        stats,
                                                                       events
```

## Problem

Today the pipeline keeps credentials out of diagnostics in two ways:

1. Some diagnostic sites leave credentials out.
2. A process-wide registry holds the secret values and searches for them
   in outgoing text.

Nothing in the types stops a connector from formatting a credential or a
raw driver error. An example from cloud#2136: a failed Postgres input
connection copied the resolved URI into the log, the deployment error,
the events and the support bundle. That failure does not go through the
consumer error callback. The worker sends it to the thread that opens
the reader, through `init_status_sender`
([Postgres input](../../crates/connectors/src/integrated/postgres/input.rs)).
So the design must cover startup paths, not only the callbacks.

Out of scope:

- The configuration wire format and the REST schema do not change.
- Credentials written inline in SQL are a separate decision about the
  support bundle policy.

The current rules are in the
[diagnostics contract](../../docs.feldera.com/docs/connectors/secret-references.md#diagnostics).

## Configuration

### The `Secret` type

Add `Secret` to `feldera-types`. Use it for a field whose whole value is a
credential, such as an S2 token or a Postgres TLS private key.

| Property | Behavior |
|---|---|
| `Debug` | prints `[REDACTED]` |
| `Display` | not implemented: `{}` does not compile |
| Access | `expose_secret(&self) -> &str`, at client setup and registration |
| Serde | transparent: same JSON as a `String`, references included |
| Schema | `type: string`: the OpenAPI schema does not change |

This is a breaking change for Rust code that builds or reads these fields:
`feldera-types` is a published crate. Each migrated field gets a
changelog entry.

### Credentials inside other values

Some credentials are part of a larger value. `Secret` cannot type them.
Keep the current visitors for these
([credential visitors](../../crates/feldera-types/src/sensitive.rs)):

| Value | What the visitor finds |
|---|---|
| libpq connection string | the password, in its raw and unescaped forms |
| Kafka, object store and Iceberg options | the values of known credential keys |
| Avro registry settings | the credentials, read from the raw JSON before typed parsing can fail ([Avro settings](../../crates/feldera-types/src/format/avro.rs)) |

Serialization still writes the original values. Startup summaries and
configuration errors must select safe fields one by one, also when a
configuration does not deserialize.

## The endpoint redactor

Create one `SecretRedactor` for each attempt to build an endpoint. Use the
existing implementation
([SecretRedactor](../../crates/feldera-types/src/secret_redaction.rs)).
The resolver fills it, because only the resolver has both the reference
and its value ([resolver](../../crates/feldera-types/src/secret_resolver.rs)).

1. Resolve the transport and the format with the same redactor. Register
   each resolved Kubernetes value with its reference, in the endpoint
   redactor and in the global one. Do this for every field, also an
   ordinary one such as a topic or a custom header.
2. Visit the resolved configuration. Register the declared credentials in
   both redactors, before any validation or client creation. This covers
   inline credentials and environment values in declared fields. Other
   environment references stay non-sensitive, as today. A reference wins
   over the inline marker.
3. Give the resolved configuration and the endpoint redactor together to
   the factories, the format code and the workers. The controller and the
   factories use the same resolution result, so the redactor holds the
   values that the client received. The stored configuration keeps the
   references. A resolution failure reports the reference or the field,
   and a safe reason.

The matcher rules do not change: escaped and encoded forms, token
boundaries, and the minimum length `MIN_REDACTED_LEN`. A value that is
shorter, or that the driver transforms in an unknown way, cannot be found.
So driver text is never reported as it is.

What the endpoint redactor adds to the global one:

- Attribution: two endpoints can use the same value under two references.
  Each endpoint then shows its own reference.
- The error path does not depend on global state.

## Reporting a diagnostic

Add `ConnectorDiagnostic` to `feldera-adapterlib`. Its fields are private.
The fatal flag and the tag stay on the reporting call. The controller
already knows the endpoint name. For endpoint statistics, the controller
copies the safe message into the existing `adapter_stats::ConnectorError`
record, with the tag, the timestamp and the sequence number
([statistics types](../../crates/feldera-types/src/adapter_stats.rs)).

| Constructor | Input | Example |
|---|---|---|
| Static | a fixed message | "replication slot is missing" |
| Specific failure | fields from a fixed list, counts, codes | schema field name, attempt count, SQLSTATE |
| Config value | a value from the endpoint configuration, sent through the endpoint redactor | topic, table, file path, S3 key |
| Library | a driver error, classified at the call that returned it | Postgres connection error |

The library constructor selects the details that are known to be safe,
also from nested causes. It then applies the credential scrub and the
endpoint redactor. For driver text that it cannot read, it reports the
operation, a stable code if one exists, and a corrective action.

There is no constructor that takes a `String`, a `Display`, format
arguments or an `anyhow::Error`. The config-value constructor is the
exception for dynamic text, and it always redacts.

The diagnostic keeps its rendered message. Its `Display`, `Debug`,
serialized details and error chain show only that message, also after the
endpoint is gone. Postgres keeps the raw cause long enough to decide on a
retry by SQLSTATE, then drops it.

### `LibraryError`

The Postgres `LibraryError` wrapper and its Clippy rule exist since
[feldera/feldera#7857](https://github.com/feldera/feldera/pull/7857).
Step 2 extends the wrapper to use the endpoint redactor, and allows its
conversion to a diagnostic only through the library constructors.

### What the types cannot stop

| Gap | Protection |
|---|---|
| Formatting an inferred driver error before it is wrapped | Clippy `disallowed-types` catches the explicit type only; code review; global backstop |
| `format!` on `expose_secret()` | Code review; few call sites; global backstop |
| Serializing a resolved configuration | Safe `Debug` impls; global backstop |

Each library boundary needs a regression test.

## Reporting paths

| Path | Change |
|---|---|
| Startup | Transport and format construction, input `open()`, output `connect()` and worker init channels return a diagnostic. |
| Running connector | Callbacks, `OutputProbe` failures and `InputConsumer::check_flush` take a diagnostic, also in `feldera-iceberg`. |
| Health | The unhealthy callback takes a diagnostic. `ConnectorHealth.description` comes from it. |
| Parse and encode | Avro converts a registry lookup failure before it caches it or puts it in a `ParseError`. |
| Stored errors and checkpoints | Redact before truncating and storing. Redact again on restore, before the length and count limits. |
| Logs, panics, HTTP errors, bundles | No change: the global filters stay. The manager keeps its credential scrub. |

Notes on these paths:

- Health: Delta input reports health during a retry. A successful
  `/stats` response shows that description, also when it omits the error
  lists ([Delta retries](../../crates/connectors/src/integrated/delta_table/input.rs)).
  Descriptions from callers that are not migrated yet are redacted before
  they are stored.
- Parse: today the Avro schema cache stores lookup failures as text
  ([Avro lookup](../../crates/connectors/src/format/avro/input.rs)).
  The rules for record data in parse errors do not change.
- Checkpoints: a secret that changed after the checkpoint is found only if
  its old value is known
  ([error storage and restore](../../crates/connectors/src/controller/stats.rs)).
- Manager: it does not resolve connector secrets. Its scrub stays for
  diagnostics from older runtimes and for older bundle collections.

The callbacks and fallible operations are in
[adapterlib](../../crates/adapterlib/src/transport.rs) and the
[controller](../../crates/connectors/src/controller.rs).

## Plan and acceptance tests

1. `Secret` and the first fields. Start with fields whose whole value
   is a credential. For each migrated configuration:
   - old JSON with inline values and with references still deserializes
     and serializes the same;
   - `Debug` shows `[REDACTED]`;
   - a `compile_fail` doctest shows that `{}` does not compile;
   - the regenerated OpenAPI has no diff;
   - the changelog names the changed Rust fields.
2. Postgres input, from resolution to startup failure. Add the
   endpoint redactor and the diagnostic constructors. Add typed reporting
   next to the current API. Each new callback method gets a default that
   forwards the rendered diagnostic to an old consumer. Code that is not
   migrated keeps its current redaction. Tests:
   - take the `ConnectorDiagnostic` of a real failed open, before any log
     writer or global filter. It names the endpoint and the reason, and
     has no URI or password;
   - retry decisions by SQLSTATE do not change;
   - the same failure in the log, `/status`, `deployment_error`, the
     stored events and the exported bundle shows the same safe message
     and details
     ([status test](../../crates/connectors/src/server.rs),
     [bundle test](../../crates/pipeline-manager/src/api/endpoints/pipeline_interaction/support_bundle.rs)).
3. One connector or library group per PR. Move its remaining
   credential fields to `Secret`, and cover each reporting path it uses.
   For each driver, cause a failure that echoes a credential, and check
   the diagnostic before global filtering. Across the groups, test:
   - a Kubernetes reference in an ordinary field;
   - an escaped libpq password;
   - malformed Avro settings, and an Avro registry lookup failure;
   - a short credential;
   - a synchronous write failure;
   - health in a successful `/stats` response during a retry;
   - two endpoints with the same value under two references: each shows
     its own reference.

   This step also does
   [the remaining driver work](https://github.com/feldera/cloud/issues/2213).
4. Remove the old entry points. When all callers have moved, remove
   the forwarding defaults and the raw entry points. Compile all connector
   groups, Iceberg included. Document the change of the public adapter
   traits. Add `compile_fail` doctests: a raw driver error or a formatted
   string passed to typed reporting does not compile. Keep the
   stored-error redaction. Restore an old checkpoint that holds a current
   secret: the endpoint status and the next checkpoint show the reference.
   Error indices, tags and truncation limits do not change.

## Open questions

1. Does `Secret` belong in `feldera-types`, or in its own small crate?
   `fda` and the manager compile `feldera-types` but do not need it.
2. Should `ConnectorDiagnostic` carry a retry kind (temporary, permanent,
   configuration)? Retry decisions could then stop reading driver codes
   in each connector.
3. How many specific-failure constructors does step 3 need? If the number
   is large, the config-value constructor may cover most of them.
