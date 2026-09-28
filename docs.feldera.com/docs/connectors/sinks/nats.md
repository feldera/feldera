# NATS output connector

Feldera can write a stream of changes to a SQL view to a NATS subject with the
`nats_output` connector.

The connector publishes to [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream):
each message is published to the configured subject with an acknowledgment
request, and the publish is retried until the server acknowledges that it
stored the message. The subject must be bound to a JetStream stream on the
target server: the connector checks the binding when it starts, so a
misconfigured subject fails the pipeline at startup rather than on the first
record.

::::note
After a restart the pipeline resumes from its last checkpoint and
re-publishes the output produced since then. See [Idempotence](#idempotence)
for how you can tell the server to drop these duplicates within a window.
::::

## Configuration

| Property                | Type   | Required | Description |
|------------------------|--------|----------|-------------|
| `connection_config`    | object | Yes      | Connection options (see below) |
| `subject`              | string | Yes      | NATS subject to publish to (e.g., `orders.created`). The subject must be bound to a JetStream stream |
| `headers`              | list   | No       | Headers attached to every message, as `key`/`value` string pairs. Default: none |
| `message_id`           | string | No       | How the `Nats-Msg-Id` header is chosen: `content_hash` or `none` (see [Idempotence](#idempotence)). Default: `content_hash` |
| `publish_timeout_secs` | integer | No      | How long to wait for the server's publish acknowledgment before retrying. Must be at least 1. Default: 5 |
| `max_message_size`     | integer | No      | Override for the maximum message size in bytes. When unset, the connector discovers the limit at connect time: the bound stream's `max_message_size` when the stream sets one, else the server's max payload. Default: discovered |

### Connection options

The `connection_config` section is the same as for the
[NATS input connector](/connectors/sources/nats): `server_url`, `auth`
(credentials, jwt + nkey, nkey, token, or user_and_password) and `tls`
(`require_tls`, `root_certificates_file`).

## Format

The connector publishes each buffer the output format produces as one
JetStream message and does not inspect its contents, so any format that
produces plain buffers works: `json` (except the `debezium` update format),
`csv`, and `parquet`. Formats that produce key/value messages (`avro`,
`json` with the `debezium` update format) fail on every message: a NATS
message has no key.

By default the `json` format packs up to `buffer_size_records` records
(10,000) into one newline-delimited message. To publish one record per
message, set `buffer_size_records` to 1:

```json
"format": {
    "name": "json",
    "config": {
        "update_format": "insert_delete",
        "buffer_size_records": 1
    }
}
```

With this configuration an insert and a later delete of the same row are
published as two messages:

```json
{"insert": {"id": 1, "name": "alice"}}
{"delete": {"id": 1, "name": "alice"}}
```

## Delivery

A publish the server does not acknowledge within `publish_timeout_secs`, or
that fails with a transient error (a lost connection, a server-side timeout),
is retried with exponential backoff until it is acknowledged or the pipeline
stops. The retry is safe because of the message id described below: a copy
the server did store is dropped as a duplicate.

A publish the server rejects outright (the stream was deleted, or the
message violates a stream constraint) is not retried: the connector reports
the error, the pipeline records it against the connector and drops the
buffer, and output continues with the next buffer.

## Message size

The connector offers the discovered message size limit, minus the bytes its
headers take on the wire, to the output format, which splits output across
messages accordingly. A buffer larger than the limit fails with an error
naming the limit rather than reaching the server, where an oversized message
would surface only as an opaque publish failure.

The limit is discovered at connect time — the bound stream's
`max_message_size` when the stream sets one, else the server's max payload —
and can be overridden with the connector's `max_message_size`.

## Idempotence

JetStream keeps a [duplicate window](https://docs.nats.io/nats-concepts/jetstream/streams#msg-dedup)
per stream: when a message arrives whose `Nats-Msg-Id` header matches a
message already stored within the window, the server discards it. The
connector uses this to make replayed output idempotent without knowing the
data format.

With `message_id: content_hash` (the default), every message carries an id
of the form `<transaction>-<hash>-<n>`:

- `transaction` is the number of the pipeline transaction (by default, the
  pipeline step) that produced the output. Feldera reproduces it when it
  replays output after a restart, and it keeps an insert of a row apart from
  an identical insert in a later transaction.
- `hash` is a 128-bit hash of the message payload.
- `n` counts earlier messages in the same transaction with the same payload,
  so a row that appears twice in the same change set is stored twice.

Replayed output after a restart therefore reproduces the ids of the
messages already stored, and the server drops them, provided that the
stream's `duplicate_window` (2 minutes by default) is at least as long as
the restart takes.

Two messages are deduplicated only when their bytes are identical, so exact
replay deduplication needs the format to emit the same buffers on replay:
with `json`, set `buffer_size_records` to 1 (see [Format](#format)); a
larger buffer may be split differently on replay.

With `message_id: none`, no header is sent, replayed output is stored again,
and every consumer of the stream must tolerate duplicates.

## Example

Consider a Feldera pipeline with table `t0` and view `v0` as defined below.

```sql
CREATE TABLE t0 (c0 INT, c1 VARCHAR);

CREATE MATERIALIZED VIEW v0 WITH (
'connectors' = '[
  {
    "transport": {
      "name": "nats_output",
      "config": {
        "connection_config": {
            "server_url": "nats://localhost:4222"
        },
        "subject": "demo.v0"
      }
    },
    "format": {
        "name": "json",
        "config": {
            "update_format": "insert_delete",
            "buffer_size_records": 1
        }
    }
  }
]'
) AS SELECT * FROM t0;
```

Assuming a JetStream stream bound to subject `demo.v0` on the server at
`nats://localhost:4222`, inserting a row into `t0`:

```sql
INSERT INTO t0 VALUES (1, 'first')
```

publishes one message to `demo.v0` with payload `{"insert":{"c0":1,"c1":"first"}}`
and a `Nats-Msg-Id` header derived from the step and the payload.
