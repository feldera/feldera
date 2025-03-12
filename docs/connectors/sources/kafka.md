# Kafka input connector

Feldera can consume a stream of changes to a SQL table from Kafka with
the `kafka_input` connector.

The Kafka input connector supports [fault
tolerance](..#fault-tolerance).

* One or more Kafka topics can be defined.
* The Kafka connector uses **librdkafka** in its implementation.
  [Relevant options supported by it](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
  can be defined in the connector configuration.

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
              "bootstrap.servers": "example.com:9092",
              "auto.offset.reset": "earliest",
              "topics": ["book-fair-sales"]
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

:::caution Experimental feature
Reading Kafka messages from a specific offset is an experimental feature.

**Fault tolerant** kafka connectors **do not** yet support this.
:::

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
              "bootstrap.servers": "example.com:9092",
              "topics": ["book-fair-sales"],
              "start_from": [{
                "topic": "book-fair-sales",
                "partition": 0,
                "offset": 42
              }]
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

The `start_from` field takes a list of JSON objects that must include the
following fields:
- `topic`: The name of the Kafka topic.
- `partition`: The partition number within the topic.
- `offset`: The specific offset from which to start consuming messages.

:::important
- The topic specified in `start_from` must also be included in `topics` field.
- If a topic has multiple partitions but only one partition is defined in
  `start_from`, only that partition will be read.
:::

### How to write connector config

Below are a couple of examples on how to connect to a Kafka broker
by specifying
[the options passed to librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
underneath.
Note that it is strongly recommended outside of test/dev
environments to have encrypted and authenticated communication.
In addition, in order for connectors to not have secrets in plaintext in their configuration,
it is recommended to use [secret management](/enterprise/kubernetes-guides/secret-management).

#### No authentication and plaintext (no encryption)

Default value for `security.protocol` is `PLAINTEXT`, as such
it does not need to be explicitly specified.

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topics": ["book-fair-sales"]
}
```

#### SASL authentication and plaintext (no encryption)

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topics": ["book-fair-sales"],
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "<USER>",
    "sasl.password": "<PASSWORD>"
}
```

#### No authentication and with encryption (SSL)

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topics": ["book-fair-sales"],
    "security.protocol": "SSL"
}
```

#### SASL authentication and with encryption (SSL)

```json
"config": {
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topics": ["book-fair-sales"],
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
    "bootstrap.servers": "example.com:9092",
    "auto.offset.reset": "earliest",
    "topics": ["book-fair-sales"],
    "security.protocol": "SSL",
    "ssl.ca.pem": "-----BEGIN CERTIFICATE-----TOPSECRET0\n-----END CERTIFICATE-----\n",
    "ssl.key.pem": "-----BEGIN CERTIFICATE-----TOPSECRET1\n-----END CERTIFICATE-----\n",
    "ssl.certificate.pem": "-----BEGIN CERTIFICATE-----TOPSECRET2\n-----END CERTIFICATE-----\n",
}
```

PEM-encoded certificates can be passed directly in the configuration using
`ssl.*.pem` keys.

During development, we encountered an issue with `librdkafka` where the SSL
handshake fails if the certificate PEM string contain more than one certificate.

Feldera circumvents this issue as follows:

###### Issue with librdkafka

librdkafka only accepts multiple certificates when provided via
`ssl.certificate.location` keys (file paths) rather than directly
with `ssl.certificate.pem`.

This is documented in
[librdkafka issue #3225](https://github.com/confluentinc/librdkafka/issues/3225).

##### Feldera's Approach

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
