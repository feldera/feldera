# NATS input connector

:::warning

NATS support is still experimental, and it may be substantially modified in the future.

:::

Feldera can consume a stream of changes to a SQL table from NATS JetStream
with the `nats_input` connector.

The NATS input connector supports exactly-once [fault
tolerance](/pipelines/fault-tolerance) using JetStream's ordered pull consumer.

## How it works

The NATS input connector uses JetStream's **ordered pull consumer**, which provides:
- **Strict ordering**: Messages delivered in exact stream order without gaps.
- **Automatic recovery**: On gap detection, heartbeat loss, or deletion, the consumer automatically recreates itself and resumes from the last processed position
- **Exactly-once semantics**: Combined with Feldera's checkpoint mechanism, ensures each message is processed exactly once

## NATS Input Connector Configuration

The connector configuration consists of three main sections:

### Connection Options

| Property                | Type   | Required | Description |
|------------------------|--------|----------|-------------|
| `server_url`           | string | Yes      | NATS server URL (e.g., `nats://localhost:4222`) |
| `auth`                 | object | No       | Authentication configuration (see [Authentication](#authentication)) |

### Stream Configuration

| Property      | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `stream_name` | string | Yes      | The name of the NATS JetStream stream to consume from |

### Consumer Configuration

| Property           | Type                    | Required | Description |
|-------------------|-------------------------|----------|-------------|
| `name`            | string                  | No       | Consumer name for identification |
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

## Authentication

The NATS connector currently supports credentials-based authentication through the `auth` object.

### Credentials File Authentication

Use a credentials file containing JWT and NKey seed:

```json
{
  "credentials": {
    "FromFile": "/path/to/credentials.creds"
  }
}
```

Or provide credentials directly as a string:

```json
{
  "credentials": {
    "FromString": "-----BEGIN NATS USER JWT-----\n...\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\n..."
  }
}
```

:::note
Additional authentication methods (JWT, NKey, token, username/password) are defined in the configuration schema but not yet implemented. Only credentials-based authentication is currently supported.
:::

:::tip
For production environments, it is strongly recommended to use [secret references](/connectors/secret-references) instead of hardcoding credentials in the configuration.
:::

## Setting up NATS JetStream

Before using the NATS input connector, you need a NATS server with JetStream enabled and a stream created.

### Running NATS Server with JetStream

[Download the NATS server binary](https://nats.io/download/) for your platform. NATS is a small, standalone binary that's easy to install and run.

```bash
# Run NATS server with JetStream enabled
nats-server -js
```

### Creating a Stream

[Download the NATS CLI](https://github.com/nats-io/natscli/releases) to manage streams and publish test messages. For other installation methods (Homebrew, package managers), see the [NATS CLI installation guide](https://github.com/nats-io/natscli#installation).

Once installed, create a stream and publish test messages:

```bash
# Create a stream
nats -s localhost:4222 stream add my_texts --subjects "text.>" --defaults

# Publish test messages
nats -s localhost:4222 pub -J --count 100 text.area.1 '{"unix": {{UnixNano}}, "text": "{{Random 0 20}}"}'
```

## Example usage

### Basic example with raw JSON format

Create a NATS input connector that reads from the `my_texts` stream:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://localhost:4222"
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

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
    ORDER BY text_length;
```

### Only revice messages NATS messages

If you only want to recive messages published after Feldera pipline start, 
change `deliver_policy` to `New`.

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://localhost:4222"
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

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
    ORDER BY text_length;
```

### Filtering by subject

Use `filter_subjects` to only conssume messages from spesific subjects `text.area.2` and `text.*.3`:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://localhost:4222"
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

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
    ORDER BY text_length;
```

### Replaying at original timing

You can use `"Original"` replay policy to replay production traffic in a test environment with realistic timing:

```sql
CREATE TABLE raw_text (
    unix BIGINT,
    text STRING
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "my_text",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": "nats://localhost:4222"
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

CREATE MATERIALIZED VIEW summary as
    SELECT
        len(text) as text_length,
        (max(unix)/1e6)::TIMESTAMP as last_recived,
        count(*) as count
    FROM raw_text
    GROUP BY text_length
    ORDER BY text_length;
```
## Additional resources

For more information, see:

* [Top-level connector documentation](/connectors/)
* [Fault tolerance](/pipelines/fault-tolerance)
* Data formats such as [JSON](/formats/json) and [CSV](/formats/csv)
* [NATS JetStream documentation](https://docs.nats.io/nats-concepts/jetstream)
* [NATS Ordered Consumer documentation](https://docs.nats.io/using-nats/developer/develop_jetstream/consumers#orderedconsumer)
