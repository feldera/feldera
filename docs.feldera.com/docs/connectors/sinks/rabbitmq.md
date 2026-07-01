# RabbitMQ output connector

Feldera can publish view changes to a RabbitMQ exchange over **AMQP 1.0** (the
native protocol of RabbitMQ 4.x on port 5672, or `amqps` on 5671) with the
`rabbitmq_output` connector.

The connector attaches an AMQP 1.0 sender to the address
`/exchanges/{exchange}/{routing_key}` and sets each message's **subject** to the
routing key, which RabbitMQ uses to match exchange bindings. Change payloads are
sent as AMQP `Data` sections; any per-record headers are mapped to
application-properties. The exchange must already exist.

## Example

```sql
CREATE VIEW results WITH ('connectors' = '[{
    "transport": {
        "name": "rabbitmq_output",
        "config": {
            "host": "rabbitmq",
            "port": 5672,
            "username": "guest",
            "password": "guest",
            "exchange": "analytics_exchange",
            "routing_key": "analytics.results.v1"
        }
    },
    "format": { "name": "json" }
}]') AS SELECT * FROM events;
```

## Configuration

| Property | Type | Required | Default | Description |
|----------|------|----------|---------|-------------|
| `host` | string | yes | — | Broker hostname |
| `port` | integer | no | `5672` | AMQP port (use `5671` with `tls`) |
| `vhost` | string | no | `/` | Virtual host (sent as the AMQP `hostname` `vhost:<name>`) |
| `username` | string | yes | — | SASL PLAIN user |
| `password` | string | yes | — | SASL PLAIN password |
| `exchange` | string | yes | — | Target exchange (`/exchanges/{exchange}`) |
| `routing_key` | string | yes | — | Routing key, set as the message subject |
| `delivery_mode` | string | no | `persistent` | `persistent` or `transient` (message durable header) |
| `tls` | boolean | no | `false` | Use `amqps` (rustls with the system roots) |

Delivery is **at-least-once**; downstream consumers should deduplicate if needed.
