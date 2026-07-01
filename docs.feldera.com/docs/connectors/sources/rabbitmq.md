# RabbitMQ input connector

Feldera can consume records from a **RabbitMQ** queue or stream over **AMQP 1.0**
(the native protocol of RabbitMQ 4.x on port 5672, or `amqps` on 5671) with the
`rabbitmq_input` connector.

The connector attaches an AMQP 1.0 receiver to the address `/queues/{queue}`.
When an `offset` is configured the queue is treated as a **stream**: the
`rabbitmq:stream-offset-spec` filter selects the start position and per-message
`x-stream-offset` annotations drive exactly-once checkpoint/replay. Without an
`offset` it is an at-least-once queue consumer.

The target queue or stream must already exist; AMQP 1.0 does not declare it.

## Example

```sql
CREATE TABLE events (
    id BIGINT,
    payload VARCHAR
) WITH ('connectors' = '[{
    "transport": {
        "name": "rabbitmq_input",
        "config": {
            "host": "rabbitmq",
            "port": 5672,
            "username": "guest",
            "password": "guest",
            "queue": "my.events",
            "offset": { "policy": "next" }
        }
    },
    "format": { "name": "json" }
}]');
```

## Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `host` | string | yes | — | Broker hostname |
| `port` | integer | no | `5672` | AMQP port (use `5671` with `tls`) |
| `vhost` | string | no | `/` | Virtual host (sent as the AMQP `hostname` `vhost:<name>`) |
| `username` | string | yes | — | SASL PLAIN user |
| `password` | string | yes | — | SASL PLAIN password |
| `queue` | string | yes | — | Queue or stream name (`/queues/{queue}`) |
| `offset` | object | no | — | Stream start position (see below). Presence marks the source as a stream |
| `tls` | boolean | no | `false` | Use `amqps` (rustls with the system roots) |
| `tls_ca_pem` | string | no | — | PEM CA certificate(s) to trust for TLS (for a private/self-signed CA); trusts only these when set |
| `consumer_name` | string | no | — | AMQP link name; make it unique per consumer to fan out |

### Offset (streams only)

`offset` maps to the `rabbitmq:stream-offset-spec` filter:

* `{ "policy": "first" }`, `{ "policy": "last" }`, or `{ "policy": "next" }`
* `{ "offset": N }` — absolute stream offset
* `{ "timestamp": "RFC3339" }` — first message at or after the timestamp

## Fault tolerance

For streams (`offset` set), the connector uses Feldera's exactly-once fault
tolerance model: stream offsets are stored in checkpoint metadata and replay
re-attaches with the stored offset. Classic and quorum queues (no `offset`)
provide at-least-once delivery.
