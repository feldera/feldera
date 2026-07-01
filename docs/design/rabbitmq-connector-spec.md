# RabbitMQ connector — design spec

**Status:** Implemented (AMQP 1.0).
**Target:** Feldera input/output adapters (`crates/adapters/src/transport/rabbitmq`)
**User docs:** `docs.feldera.com/docs/connectors/sources/rabbitmq.md`, `sinks/rabbitmq.md`

---

## 1. Problem

Feldera ships Kafka and NATS transports but no RabbitMQ connector. RabbitMQ 4.x
speaks **AMQP 1.0** natively (port 5672, or `amqps` on 5671) and exposes queues,
quorum queues, and streams through a uniform address model. A single AMQP 1.0
connector covers all of them without a Kafka/Redpanda shim.

## 2. Protocol decision

**AMQP 1.0 only**, via the `fe2o3-amqp` Rust client. Not AMQP 0-9-1, not the
native Stream binary protocol. AMQP 1.0 reaches classic/quorum queues *and*
streams (with the `rabbitmq:stream-offset-spec` filter), so one protocol serves
every source, and message framing (subject, application-properties) maps cleanly
onto Feldera's routing-key and header concepts.

## 3. Address & mapping model

| Concept | AMQP 1.0 mapping |
|---------|------------------|
| Virtual host | connection `hostname` = `vhost:<name>` |
| Auth | SASL PLAIN (`username`/`password`) |
| Input source | receiver attached to `/queues/{queue}` |
| Output target | sender attached to `/exchanges/{exchange}/{routing_key}` |
| Routing key | message **subject** |
| Headers | **application-properties** |
| Payload | AMQP `Data` section (opaque bytes → format parser) |
| Stream start | `rabbitmq:stream-offset-spec` filter (`first`/`last`/`next`/offset/timestamp) |
| Stream offset | per-message `x-stream-offset` annotation |

## 4. Fault tolerance

* **Streams** (`offset` configured): exactly-once. The reader tracks
  `x-stream-offset` per message; checkpoint metadata stores the `[start, end)`
  offset range; replay re-attaches with `stream-offset-spec = end`.
* **Classic / quorum queues** (no `offset`): at-least-once (deliveries settled
  with `accept` after they are queued).
* **Output**: at-least-once; each publish waits for the `accepted` outcome.

## 5. TLS

`tls: true` switches the connection to `amqps` using rustls with the system /
webpki roots (the `fe2o3-amqp` `rustls` feature). Self-signed broker certs need
a broker configured with a CA trusted by those roots.

## 6. Out of scope

* Competing consumers on quorum queues (breaks exactly-once resume).
* Headers-exchange / RPC request-reply patterns.
* Dynamic (per-record) routing keys on output — the routing key is static config.
