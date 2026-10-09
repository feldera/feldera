# NATS input connector

Feldera can consume a stream of changes to a SQL table from NATS JetStream
with the `nats_input` connector.

The NATS input connector supports exactly-once [fault
tolerance](/pipelines/fault-tolerance) using JetStream's ordered pull consumer.

## How it works

The NATS input connector uses JetStream's **ordered pull consumer**, which provides:
- **Strict ordering**: Messages delivered in exact stream order without gaps.
- **Automatic recovery**: On gap detection, heartbeat loss, or deletion, the consumer automatically recreates itself and resumes from the last processed position.
- **Retry loop with health checks**: On transient startup/runtime failures, the connector enters retry mode and reconnects with exponential backoff.
- **Exactly-once semantics**: Combined with Feldera's checkpoint mechanism, ensures each message is processed exactly once.

## NATS Input Connector Configuration

The connector configuration consists of three main sections:

### Connection Options

| Property                | Type   | Required | Description |
|------------------------|--------|----------|-------------|
| `server_url`           | string | Yes      | NATS server URL (e.g., `nats://localhost:4222`) |
| `auth`                 | object | No       | Authentication configuration (see [Authentication](#authentication)) |
| `tls`                  | object | No       | TLS configuration (see [TLS](#tls)) |
| `connection_timeout_secs` | integer | No    | Connection timeout in seconds. How long to wait when establishing the initial connection to the NATS server. Default: 10 |
| `request_timeout_secs` | integer | No       | Request timeout in seconds. How long to wait for responses to requests. Default: 10 |

### Stream Configuration

| Property      | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `stream_name` | string | Yes      | The name of the NATS JetStream stream to consume from |
| `inactivity_timeout_secs` | integer | No | Maximum idle time while waiting for the next message before running a stream/server health check. Must be at least 1. Default: 60 |
| `retry_interval_secs` | integer | No | Delay before the first automatic retry attempt in retry mode. The delay doubles after each consecutive failure. Must be at least 1. Default: 5 |
| `retry_max_interval_secs` | integer | No | Upper bound on the delay between retry attempts. Must be at least `retry_interval_secs`. Set it equal to `retry_interval_secs` to retry at a fixed interval. Default: 300 |
| `retry_max_attempts` | integer | No | Number of consecutive failed retry attempts after which the connector gives up with a fatal error. Must be at least 1. Default: unset (retry forever) |

### Consumer Configuration

| Property           | Type                    | Required | Description |
|-------------------|-------------------------|----------|-------------|
| `name`            | string                  | No       | Prefix for the names of the consumers the connector creates (see [Consumer lifecycle](#consumer-lifecycle)). Default: the table name |
| `description`     | string                  | No       | Consumer description |
| `filter_subjects` | string list             | No       | Filter messages by subject(s). If empty, consumes all subjects in the stream |
| `replay_policy`   | variant                 | No       | Message replay speed: `"Instant"` (default, fast) or `"Original"` (rate-limited at original timing) |
| `rate_limit`      | integer                 | No       | Rate limit in bytes per second. Default: 0 (unlimited) |
| `deliver_policy`  | variant                 | Yes      | Starting point for reading from the stream (see [Deliver Policy](#deliver-policy)) |
| `max_waiting`     | integer                 | No       | Maximum outstanding pull requests. Default: 0 |
| `metadata`        | map (string → string)   | No       | Consumer metadata key-value pairs |
| `max_batch`       | integer                 | No       | Maximum messages per batch |
| `max_bytes`       | integer                 | No       | Maximum bytes per batch |
| `max_expires`     | duration                | No       | Maximum duration for pull requests |

#### Deliver Policy

The `deliver_policy` field determines where in the stream to start consuming messages:

- `"All"` - Start from the earliest available message in the stream
- `"Last"` - Start from the last message in the stream
- `"New"` - Start from new messages only (messages arriving after consumer creation)
- `"LastPerSubject"` - Start with the last message for all subjects received (useful for KV-like workloads)
- `{"ByStartSequence": {"start_sequence": 100}}` - Start from a specific sequence number
- `{"ByStartTime": {"start_time": "2024-01-01T12:00:00Z"}}` - Start from messages at or after the specified timestamp (RFC 3339 format)

#### Replay Policy

The `replay_policy` field controls how fast messages are delivered to the consumer:

- `"Instant"` (default) - Delivers messages as quickly as possible. Use for maximum throughput in production workloads.
- `"Original"` - Delivers messages at the rate they were originally received, preserving the timing between messages. Useful for:
  - Replaying production traffic patterns in test/staging environments
  - Load testing with realistic timing
  - Debugging scenarios where message timing matters

If not specified, defaults to `"Instant"`.

## Retry, startup and replay behavior

The connector distinguishes between **retryable** and **fatal** errors:

- **Retryable errors** (temporary network/server issues, missing stream during startup, transient message-stream failures, and temporary failures while fetching JetStream stream metadata used during startup, resume, or replay validation) move the connector into retry mode. It reports non-fatal endpoint errors and retries automatically with backoff (see [Retry backoff](#retry-backoff)).
- **Fatal errors** stop the connector and report a fatal endpoint error. This is used when checkpoint/replay metadata is incompatible with the current stream sequence space.

Before reading after startup or resume, the connector validates the checkpoint resume cursor against the stream's available sequence range. During replay, it validates that the requested replay range still exists.

Only transient I/O failures during these validation checks are retried. Once the connector successfully reads the stream metadata, logical validation failures remain fatal.

Typical fatal scenarios include:

- **Stream deleted or recreated**: The checkpoint references sequence numbers that no longer exist in the current stream. For example, the resume cursor is before the stream's earliest available sequence, or after the stream's latest sequence.
- **Stream purged**: The stream exists but required replay messages have been removed. The connector detects that the requested replay range falls outside the available sequence range.
- **Stream emptied**: The checkpoint says to resume from a specific sequence, but the stream now contains zero messages.
- **Unexpected sequence during replay**: While replaying a checkpoint batch, the connector receives a message with a sequence number beyond the expected replay range, indicating that earlier messages may have been deleted mid-replay.

In all of these cases the connector fails fast with a fatal error instead of retrying
forever, because the data needed to maintain exactly-once guarantees is permanently gone.

:::tip Recovery from fatal errors
To recover from a fatal error caused by stream data loss, you typically need to
reset the pipeline's checkpoint state (e.g., by recreating the pipeline) so it
starts fresh without referencing the now-invalid sequence numbers.
:::

### Retry backoff

Each retry attempt opens a new connection to the NATS server and creates a new
JetStream consumer. Creating a consumer at a checkpointed position makes the
server scan the stream to compute the consumer's pending count, which is
expensive on large streams. If many connectors retried at a short, fixed
interval against a struggling cluster, they would add load exactly when the
cluster can least afford it.

The connector therefore backs off exponentially. The first retry waits
`retry_interval_secs`, and each consecutive failure doubles the wait, up to
`retry_max_interval_secs`. Every wait below the maximum is lengthened by a
random amount of up to 25%, so that connectors that failed together (for
example, after a NATS node restart) do not retry in lockstep. The randomization
never shortens a wait below `retry_interval_secs` or lengthens it beyond
`retry_max_interval_secs`. With the defaults, the nominal waits are 5s, 10s,
20s, 40s, 80s, 160s, and then 300s for every later attempt.
A successful reconnect resets the backoff.

By default the connector retries forever. Set `retry_max_attempts` to stop
after that many consecutive failed retries with a fatal error, which leaves the
decision to resume to an operator. The same limit applies to retries during
replay.

### Consumer lifecycle

The connector creates a new ephemeral JetStream consumer when it starts,
resumes, replays a checkpoint, or retries. Each consumer is named
`<prefix>_<uuid>`, where the prefix is the configured `consumer_config.name`,
or the table name if none is set. Characters that JetStream does not allow in
consumer names are replaced with `_`. The unique suffix prevents "consumer
already exists" errors on quick restarts, and the prefix identifies which
connector owns a consumer when you inspect the stream with `nats consumer ls`.
For example, to list the consumers of a connector whose prefix is `orders`:

```bash
nats consumer ls <stream> --names | grep '^orders_'
```

The connector deletes its consumer when it pauses, stops, abandons a failed
reader before retrying, or finishes a replay. It also deletes the consumer
after a create request that timed out, because an overloaded server may still
create the consumer after the client has given up. Deletion is best-effort: if
the server cannot be reached, the consumer expires on its own after 30 seconds
of inactivity.

### Metrics

The connector exports the following metrics on the pipeline's metrics
endpoint, in addition to the standard input connector metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `input_connector_nats_consumers_created_total` | counter | JetStream consumers created (start, resume, replay, and every retry) |
| `input_connector_nats_consumers_deleted_total` | counter | JetStream consumers explicitly deleted |
| `input_connector_nats_retries_total` | counter | Reconnect attempts made in retry mode |
| `input_connector_nats_consecutive_failures` | gauge | Consecutive failed attempts in the current retry episode; 0 when healthy |
| `input_connector_nats_retry_state` | gauge | `0` healthy or paused, `1` retrying, `2` stopped after a fatal error |

A steadily rising `input_connector_nats_consumers_created_total` while the
connector ingests no records signals that it is stuck reconnecting.

## Authentication

The NATS connector supports the standard NATS authentication methods through
the `auth` object. Configure exactly one method: `credentials`, `jwt` (with
`nkey`), `nkey`, `token`, or `user_and_password`. Configuring more than one
is rejected.

### Credentials File Authentication

Use a credentials file containing JWT and NKey seed:

```json
{
  "auth": {
    "credentials": {
      "FromFile": "/path/to/credentials.creds"
    }
  }
}
```

Or provide credentials directly as a string:

```json
{
  "auth": {
    "credentials": {
      "FromString": "-----BEGIN NATS USER JWT-----\n...\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\n..."
    }
  }
}
```

### JWT Authentication

For decentralized (operator-mode) authentication with the user JWT and NKey
seed stored separately (e.g. as two secrets) rather than as one `.creds`
file. The seed signs the connection nonce:

```json
{
  "auth": {
    "jwt": "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ...",
    "nkey": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  }
}
```

### NKey Authentication

For a bare NKey user declared in the server configuration (`nkey:` user):
the server issues a nonce and the connector signs it with the seed.

```json
{
  "auth": {
    "nkey": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  }
}
```

### Token Authentication

```json
{
  "auth": {
    "token": "s3cret"
  }
}
```

### Username and Password Authentication

```json
{
  "auth": {
    "user_and_password": {
      "user": "myuser",
      "password": "mypassword"
    }
  }
}
```

:::tip
For production environments, it is strongly recommended to use [secret references](/connectors/secret-references) instead of hardcoding credentials in the configuration.
:::

## TLS

TLS options are configured through the `tls` object:

| Property                  | Type    | Required | Description |
|---------------------------|---------|----------|-------------|
| `require_tls`             | boolean | No       | Require an encrypted connection; refuse to connect to servers that do not offer TLS. Default: `false` |
| `root_certificates_file`  | string  | No       | Path to a PEM file with additional root certificates to trust when verifying the server certificate, for servers whose certificates are not signed by a public CA |

```json
{
  "connection_config": {
    "server_url": "tls://nats.example.com:4222",
    "tls": {
      "require_tls": true,
      "root_certificates_file": "/etc/nats-ca/ca.crt"
    }
  }
}
```

## Setting up NATS JetStream

Before using the NATS input connector, you need a NATS server with JetStream enabled and a stream created.

### Quickstart
The quickest way to start experimenting with Feldera and NATS is to use Docker Compose:

```bash
curl -L 'https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml' -o docker-compose.yml
docker compose --profile nats up
```

This starts a Feldera pipeline manager, NATS server, and the NATS CLI. Connect to the CLI container with:

```bash
docker compose exec nats-cli sh
```

You can then easily publish messages to the NATS server using the `nats` CLI.

### Creating a Stream
Once installed, create a stream and publish test messages:

```bash
# Create a stream
nats stream add my_texts --subjects "text.>" --defaults

# Publish test messages
nats pub -J --count 100 text.area.1 '{"unix": {{UnixNano}}, "text": "{{Random 0 20}}"}'
```

## Example usage

### Basic example with raw JSON format

Create a NATS input connector that reads from the `my_texts` stream:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Only receive new NATS messages

If you only want to receive messages published after the Feldera pipeline starts,
change `deliver_policy` to `New`.

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "New"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Filtering by subject

Use `filter_subjects` to only consume messages from specific subjects `text.area.2` and `text.*.3`:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All",
                     "filter_subjects": ["text.area.2", "text.*.3"]
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```

### Replaying at original timing

You can use `"Original"` replay policy to replay production traffic in a test environment with realistic timing:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    TEXT STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_texts",
                "consumer_config": {
                    "deliver_policy": "All",
                    "replay_policy": "Original"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);

CREATE MATERIALIZED VIEW summary AS
    SELECT
        LEN(TEXT) AS text_length,
        (MAX(unix)/1e6)::TIMESTAMP AS last_received,
        COUNT(*) AS COUNT
    FROM raw_text
    GROUP BY text_length
```
## Pipeline metadata

When the pipeline configuration includes a `given_name` field, Feldera
automatically sets `metadata["pipeline"]` on the NATS JetStream consumer to the
value of `given_name`. If you already set `metadata.pipeline` in your connector
configuration, your value takes precedence and is not overwritten.

For example, a pipeline named `"my_pipeline"` will produce a consumer whose
metadata contains `{"pipeline": "my_pipeline"}`:

```sql
CREATE TABLE events (
    id BIGINT,
    payload STRING
) WITH (
    'connectors' = '[{
        "name": "nats_in",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://nats:4222"
                },
                "stream_name": "my_stream",
                "consumer_config": {
                    "deliver_policy": "All"
                }
            }
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw"
            }
        }
    }]'
);
```

This is useful for correlating NATS consumer metrics with Feldera pipeline
status in monitoring systems. For example, with
[`prometheus-nats-exporter`](https://github.com/nats-io/prometheus-nats-exporter),
use the `-jsz_consumer_meta_keys=pipeline` flag to surface the pipeline name as
a Prometheus label on all `nats_consumer_*` metrics. This allows alert rules
that join on the `pipeline` label with `feldera_pipeline_status`.

## Additional resources

For more information, see:

* [Top-level connector documentation](/connectors/)
* [Fault tolerance](/pipelines/fault-tolerance)
* Data formats such as [JSON](/formats/json) and [CSV](/formats/csv)
* [NATS JetStream documentation](https://docs.nats.io/nats-concepts/jetstream)
* [NATS Ordered Consumer documentation](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#orderedconsumer)
