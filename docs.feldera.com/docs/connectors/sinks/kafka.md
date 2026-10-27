# Kafka output connector

Feldera can output a stream of changes to a SQL table or view to a Kafka topic.

The Kafka output connector supports [fault tolerance](/pipelines/fault-tolerance).

## Kafka Output Connector Configuration

| Property                       | Type             | Default | Description |
|--------------------------------|------------------|---------|-------------|
| `topic` (required)             | string           |         | The Kafka topic to send to. |
| `bootstrap.servers` (required) | string           |         | A comma separated list of Kafka brokers to connect to. |
| `headers`                      | array of objects |         | Kafka headers to be added to each message produced by this connector. A header specifies a key and, optionally, a value. The value must be a string or an array of bytes, e.g., `[{"key": "key1", "value": "val1"}, {"key": "key2", "value": [1,2,3,4,5]}, {"key": "key3"}]`|
| `log_level`                    | string           |         | The log level for the Kafka client. |
| `initialization_timeout` | duration | `60s` | Maximum time to wait for the connector to connect to a Kafka broker, for example `60s`. Replaces the deprecated `initialization_timeout_secs`. |
| `kafka_service`                | string           |         | If specified, this service is used to provide defaults for the Kafka options. |
| `region`                       | string           |         | The AWS region to use while connecting to AWS Managed Streaming for Kafka (MSK). |
| `fault_tolerance`              | object           |         | librdkafka options the connector uses when the pipeline runs with [exactly-once fault tolerance](/pipelines/fault-tolerance). See [Fault tolerance options](#fault-tolerance-options). |

The connector passes additional options directly to [**librdkafka**](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
See [relevant options for producers](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

### Fault tolerance options

A pipeline configured for exactly-once
[fault tolerance](/pipelines/fault-tolerance) writes to Kafka through
transactions, which needs a consumer as well as a producer. `fault_tolerance`
adds librdkafka settings for those two clients:

| Property           | Type   | Default | Description |
|--------------------|--------|---------|-------------|
| `consumer_options` | object | `{}`    | Options passed to librdkafka for consumers only, overriding the connector's other Kafka options. |
| `producer_options` | object | `{}`    | Options passed to librdkafka for producers only, overriding the connector's other Kafka options. |

`fault_tolerance` does not turn transactional writes on or off; the pipeline's
fault tolerance model does. Omitting `fault_tolerance` leaves the consumer and
producer with the connector's other Kafka options and the librdkafka defaults.

```json
"fault_tolerance": {
  "consumer_options": {"fetch.max.bytes": "10485760"},
  "producer_options": {"compression.type": "lz4"}
}
```

## Delivery, retries, and backpressure

For pipelines without exactly-once fault tolerance, the connector defaults
`message.timeout.ms` to `0`: temporary broker failures are retried without a
delivery deadline. When the producer queue fills, the connector waits for room.
The controller's output queue then fills to `max_queued_records`, and the
pipeline stops advancing until the sink catches up. Configure
`queue.buffering.max.messages` and `queue.buffering.max.kbytes` to bound the
producer queue; the byte limit takes priority. These bounds apply per connector.

To restore the former five-minute delivery deadline, add either
`"message.timeout.ms": "300000"` or `"delivery.timeout.ms": "300000"` to the
transport's `config` object. The two names are aliases; configure only one.
An explicit finite timeout or retry count can cause undelivered records to be
dropped. Permanent errors, such as a rejected oversized message, can also lose
records. The connector reports these losses as errors. Shutdown does not
guarantee delivery of records still in the non-fault-tolerant producer queue.

The connector reports unhealthy when a pending message has waited at least
10 seconds for delivery and no delivery completed in the last 10 seconds, or
when a delivery fails. A slow broker that keeps acknowledging is not a stall,
however long its queue. This check also runs on a quiet connector with spare
queue capacity and when statistics are disabled. Records purged from the
producer queue when the pipeline stops count as delivery errors in the
metrics but are not reported as connector errors.
Enqueuing another message does not establish recovery. A successful delivery
restores healthy status only if no other message is overdue and the producer
has no fatal error. After a fatal error, fix the underlying broker or
configuration problem and restart the pipeline. Broker connection failures
during an outage are logged as warnings. They are not counted as connector
errors, because the queued records are retained.

These delivery defaults and health rules apply to the non-fault-tolerant sink.
The exactly-once sink uses Kafka transactions and its own producer settings.

### Output metrics

The pipeline's `/metrics` endpoint exports the following metrics per output
endpoint. Queue gauges and broker request counters refresh every
`statistics.interval.ms`, which defaults to 10 seconds. With that option set to
`0`, those statistics are disabled and the corresponding metrics stay at their
initial zero values. Delivery counters and health checks remain active. The
full-queue wait counter updates during retries and when a wait ends, including
cancellation; its refresh delay is at most the retry polling interval (5 seconds).

| Metric | Type | Meaning |
|---|---|---|
| `kafka_output_queue_messages` | gauge | Messages in the producer queue. |
| `kafka_output_queue_bytes` | gauge | Bytes in the producer queue. |
| `kafka_output_queue_max_messages` | gauge | Configured message limit; zero means no message-count limit. |
| `kafka_output_queue_max_bytes` | gauge | Configured byte limit. |
| `kafka_output_produce_retries_total` | counter | Broker request retries, including metadata requests as well as produce requests. |
| `kafka_output_request_timeouts_total` | counter | Broker requests that timed out. |
| `kafka_output_delivered_messages_total` | counter | Successful delivery reports from librdkafka, according to the configured acknowledgement policy. |
| `kafka_output_delivery_errors_total` | counter | Failed asynchronous deliveries. |
| `kafka_output_queue_full_wait_seconds_total` | counter | Time spent waiting for producer queue space. |

## Example usage

We will create a Kafka output connector named `total-sales`.
Kafka broker is located at `example.com:9092` and the topic is `total-sales`.

```sql
CREATE VIEW v
WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "example.com:9092",
              "topic": "total-sales"
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "insert_delete",
              "array": false
          }
      }
   }
   ]'
)
AS ...
```

### Authentication & Encryption

#### SSL with PEM keys

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topic": "book-fair-sales",
    "security.protocol": "SSL",
    "ssl.ca.pem": "-----BEGIN CERTIFICATE-----TOPSECRET0\n-----END CERTIFICATE-----\n",
    "ssl.key.pem": "-----BEGIN CERTIFICATE-----TOPSECRET1\n-----END CERTIFICATE-----\n",
    "ssl.certificate.pem": "-----BEGIN CERTIFICATE-----TOPSECRET2\n-----END CERTIFICATE-----\n",
}
```

PEM-encoded certificates can be passed directly in the configuration using
`ssl.*.pem` keys.

#### Multiple certificates

librdkafka only accepts multiple certificates when provided via
`ssl.certificate.location` keys (file paths) rather than directly
with `ssl.certificate.pem`.

This is documented in
[librdkafka issue #3225](https://github.com/confluentinc/librdkafka/issues/3225).

To work around this limitation, Feldera handles PEM-encoded certificates and
keys by:

1. Storing the value passed to `ssl.certificate.pem` into a file.
2. Naming the file using the SHA256 hash of the data.
3. Replacing the `ssl.certificate.pem` configuration option with
   `ssl.certificate.location` option that points to the newly saved file.

**Example:**

The updated configuration would look like:

```json
"config": {
    ...,
    "ssl.certificate.location": "path/to/certificate.pem"
}
```

> :warning: If both `ssl.certificate.pem` and `ssl.certificate.location` are set
the latter will be overwritten.

### Connecting to AWS MSK with IAM SASL

Example of writing data to AWS MSK with IAM SASL.

:::important
- AWS credentials must either be set as Environment Variables or present in `~/.aws/credentials`.
- Ensure that the defined output topic either exists in AWS MSK or, automatic topic creation is enabled.
- `sasl.mechanism` must be set to `OAUTHBEARER`.
- `security.protocol` must be set to `SASL_SSL`.
- When the `sasl.mechanism` is `OAUTHBEARER`, the AWS region for MSK must either be set via the environment
  variable `AWS_REGION` or the `region` field in connector definition as in the example below.

Other protocols and mechanisms aren't supported.
:::

```sql

CREATE VIEW output
WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "broker-1.kafka.region.amazonaws.com:9098,broker-2.kafka.region.amazonaws.com:9098",
              "sasl.mechanism": "OAUTHBEARER",
              "security.protocol": "SASL_SSL",
              "region": "<AWS_REGION>",
              "topic": "<TOPIC>"
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "insert_delete",
              "array": false
          }
      }
   }
   ]'
) AS SELECT * FROM input;
```

Example of writing data to GCP Managed Service for Apache Kafka with OAUTHBEARER.

:::important
- Ensure that the defined output topic either exists in GCP Managed Service for Apache Kafka or automatic
  topic creation is enabled.
- `sasl.mechanism` must be set to `OAUTHBEARER`.
- `security.protocol` must be set to `SASL_SSL`.
- `oauth_provider` must be set to `gcp`.
- Google credentials are resolved from Application Default Credentials: a service account key or user
  credentials file (e.g., as set up by `gcloud auth application-default login`), or, on GKE with Workload
  Identity configured, the metadata server.
- GCP Managed Kafka authenticates a named principal, which Feldera takes from the service account email
  in the credentials, or from the metadata server. Credentials that name no principal, including the user
  credentials written by `gcloud auth application-default login`, require the principal in the
  `GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL` environment variable. Setting that variable also overrides the
  principal derived from the credentials.

Other protocols and mechanisms aren't supported.
:::

```sql

CREATE VIEW output
WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "bootstrap.<CLUSTER>.<REGION>.managedkafka.<PROJECT>.cloud.goog:9092",
              "sasl.mechanism": "OAUTHBEARER",
              "security.protocol": "SASL_SSL",
              "oauth_provider": "gcp",
              "topic": "<TOPIC>"
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "insert_delete",
              "array": false
          }
      }
   }
   ]'
) AS SELECT * FROM input;
```

## Additional resources

For more information, see:

* [Kafka input connector](/connectors/sources/kafka#how-to-write-connector-config)
  for examples on how to write the configuration to connect to Kafka brokers (e.g., how to
  specify authentication and encryption).

* [Tutorial section](/tutorials/basics/part3#step-2-configure-kafkaredpanda-connectors) which involves
  creating a Kafka output connector.

* Data formats such as [JSON](/formats/json) and
  [CSV](/formats/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
