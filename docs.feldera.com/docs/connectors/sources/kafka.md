# Kafka input connector

Feldera can consume a stream of changes to a SQL table from Kafka with
the `kafka_input` connector.

The Kafka input connector supports [fault
tolerance](/pipelines/fault-tolerance).

## Kafka Input Connector Configuration

| Property                       | Type             | Default | Description |
|--------------------------------|------------------|---------|-------------|
| `topic` (required)             | string           |         | The Kafka topic to subscribe to. |
| `bootstrap.servers` (required) | string           |         | A comma separated list of Kafka brokers to connect to.
| `start_from`                   | variant          | latest  | The starting point for reading from the topic, one of `"earliest"`, `"latest"`, or `{"offsets": [1, 2, 3, ...]}`, where `[1, 2, 3, ...]` are particular offsets within each partition in the topic. |
| `partitions`                   | integer list     |         | <p> The list of Kafka partitions to read from. </p> <p> Only the specified partitions will be consumed. If this field is not set, the connector will consume from all available partitions. </p><p> If `start_from` is set to `offsets` and this field is provided, the number of partitions must exactly match the number of offsets, and the order of partitions must correspond to the order of offsets. </p><p> If offsets are provided for all partitions, this field can be omitted. </p> |
| `log_level`                    | string           |         | The log level for the Kafka client. |
| `group_join_timeout_secs`      | seconds          | 10      | Maximum timeout (in seconds) for the endpoint to join the Kafka consumer group during initialization. |
| `poller_threads`               | positive integer | 3       | Number of threads used to poll Kafka messages. Setting it to multiple threads can improve performance with small messages. Default is 3. |
| `resume_earliest_if_data_expires` | boolean       | false   | See [Tolerating missing data on resume](#tolerating-missing-data-on-resume). |

The connector passes additional options directly to [**librdkafka**](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).  Some of the relevant options:

* `group.id` is ignored.  The connector never uses consumer groups, so `enable.auto.commit`, `enable.auto.offset.store`, and other options related to consumer groups are also ignored.
* `auto.offset.reset` is ignored.  Use `start_from` (described in the table above) instead.

## Example usage

We will create a Kafka connector named `book-fair-sales`.
Kafka broker is located at `example.com:9092` and the topic is `book-fair-sales`.

Format in this example is newline-delimited JSON (NDJSON).
For example, there can be two messages containing three rows:

Message 1:
```text
{"insert": {"sid": 123, pid": 2, "sold_at": "2024-01-01 12:00:04", "price": 5.0}}
{"insert": {"sid": 124, pid": 12, "sold_at": "2024-01-01 12:00:08", "price": 20.5}}
```

Message 2:
```
{"insert": {"sid": 125, pid": 8, "sold_at": "2024-01-01 12:01:02", "price": 1.10}}
```

```sql
CREATE TABLE INPUT (
   ... -- columns omitted
) WITH (
  'connectors' = '[
    {
      "transport": {
          "name": "kafka_input",
          "config": {
              "topic": "book-fair-sales",
              "start_from": "earliest",
              "bootstrap.servers": "example.com:9092"
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "insert_delete",
              "array": false
          }
      }
  }]'
)
```

### Starting from a specific offset

Feldera supports starting a Kafka connector from a specific offset in a specific
partition.

```sql
CREATE TABLE INPUT (
   ... -- columns omitted
) WITH (
  'connectors' = '[
    {
      "transport": {
          "name": "kafka_input",
          "config": {
              "topic": "book-fair-sales",
              "start_from": {"offsets": [42]},
              "bootstrap.servers": "example.com:9092"
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "insert_delete",
              "array": false
          }
      }
  }]'
)
```

### How to write connector config

Below are a couple of examples on how to connect to a Kafka broker
by specifying
[the options passed to librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
underneath.
Note that it is strongly recommended outside of test/dev
environments to have encrypted and authenticated communication.
In addition, in order for connectors to not have secrets in plaintext in their configuration,
it is recommended to use [secret references](/connectors/secret-references).

#### No authentication and plaintext (no encryption)

Default value for `security.protocol` is `PLAINTEXT`, as such
it does not need to be explicitly specified.

```json
"config": {
    "topic": "book-fair-sales",
    "start_from": "earliest",
    "bootstrap.servers": "example.com:9092"
}
```

#### SASL authentication and plaintext (no encryption)

```json
"config": {
    "topic": "book-fair-sales",
    "start_from": "earliest",
    "bootstrap.servers": "example.com:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "<USER>",
    "sasl.password": "<PASSWORD>"
}
```

#### No authentication and with encryption (SSL)

```json
"config": {
    "topic": "book-fair-sales",
    "start_from": "earliest",
    "bootstrap.servers": "example.com:9092",
    "security.protocol": "SSL"
}
```

#### SASL authentication and with encryption (SSL)

```json
"config": {
    "topic": "book-fair-sales",
    "start_from": "earliest",
    "bootstrap.servers": "example.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "<USER>",
    "sasl.password": "<PASSWORD>"
}
```

For example, at the time of writing (15 May 2024),
Confluent Cloud Kafka C client tutorial configuration
uses `SASL_SSL` with SASL mechanism `PLAIN` (see
[their tutorial](https://developer.confluent.io/get-started/c/#kafka-setup),
select Kafka Location: Confluent Cloud, and then go to the Configuration tab).

#### SSL with PEM keys

```json
"config": {
    "topic": "book-fair-sales",
    "start_from": "earliest",
    "bootstrap.servers": "example.com:9092",
    "security.protocol": "SSL",
    "ssl.ca.pem": "-----BEGIN CERTIFICATE-----TOPSECRET0\n-----END CERTIFICATE-----\n",
    "ssl.key.pem": "-----BEGIN CERTIFICATE-----TOPSECRET1\n-----END CERTIFICATE-----\n",
    "ssl.certificate.pem": "-----BEGIN CERTIFICATE-----TOPSECRET2\n-----END CERTIFICATE-----\n"
}
```

PEM-encoded certificates can be passed directly in the configuration using
`ssl.*.pem` keys.

##### Multiple certificates

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

### Using Kafka as a Debezium transport

The Kafka connector can be used to ingest data from a source via
Debezium.  For information on how to setup Debezium integration for Feldera, see
[Debezium connector documentation](debezium).

### Connecting to AWS MSK with IAM SASL

Example of reading data from AWS MSK with IAM SASL.

:::important
- AWS credentials must either be set as Environment Variables or present in `~/.aws/credentials`.
- `sasl.mechanism` must be set to `OAUTHBEARER`.
- `security.protocol` must be set to `SASL_SSL`.
- When the `sasl.mechanism` is `OAUTHBEARER`, the AWS region for MSK must either be set via the environment
  variable `AWS_REGION` or the `region` field in connector definition as in the example below.

Other protocols and mechanisms aren't supported.
:::

```sql
CREATE TABLE INPUT (
   ... -- columns omitted
) WITH (
   'connectors' = '[
    {
      "transport": {
          "name": "kafka_input",
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
);
```

## Tolerating missing data on resume

The `resume_earliest_if_data_expires` setting controls how the Kafka
input connector behaves when a pipeline configured with at-least-once
[fault tolerance](/pipelines/fault-tolerance) resumes from a
checkpoint and the configured Kafka topic no longer has data at the
offsets saved in the checkpoints:

- If `resume_earliest_if_data_expires` is false, which is the default,
  then the connector will report an error and the pipeline will fail
  to start.  This behavior makes sense because it is no longer
  possible to continue the pipeline from where it left off.

- If `resume_earliest_if_data_expires` is true, then the connector
  will log a warning and start reading data from the earliest offsets
  now available in the topic.  This is reasonable behavior in the
  special case where some errors were detected in the data in the
  Kafka topic and the topic was deleted and recreated with correct
  data starting at the point of an earlier checkpoint, and the
  pipeline was restarted from that checkpoint.

This setting only has an effect when a Kafka topic cannot be read at
the checkpointed offsets.  It has no effect if fault tolerance is not
enabled, or if exactly once fault tolerance is enabled, or at any time
other than the point of resuming from a checkpoint.

## Additional resources

For more information, see:

* [Tutorial section](/tutorials/basics/part3#step-2-configure-kafkaredpanda-connectors) which involves
  creating a Kafka input connector.

* Data formats such as [JSON](/formats/json) and
  [CSV](/formats/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
