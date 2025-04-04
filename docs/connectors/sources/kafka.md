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
| `log_level`                    | string           |         | The log level for the Kafka client. |
| `group_join_timeout_secs`      | seconds          | 10      | Maximum timeout (in seconds) for the endpoint to join the Kafka consumer group during initialization. |
| `poller_threads`               | positive integer | 3       | Number of threads used to poll Kafka messages. Setting it to multiple threads can improve performance with small messages. Default is 3. |

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

Feldera supports starting a Kafka connector from a specific offset in a specifc
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
it is recommended to use [secret management](/get-started/enterprise/kubernetes-guides/secret-management).

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

## Additional resources

For more information, see:

* [Tutorial section](/tutorials/basics/part3#step-2-configure-kafkaredpanda-connectors) which involves
  creating a Kafka input connector.

* Data formats such as [JSON](/formats/json) and
  [CSV](/formats/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
