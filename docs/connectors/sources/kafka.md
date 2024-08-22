# Kafka input connector

Feldera can consume a stream of changes to a SQL table from Kafka.

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
  'connectors = '[
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

### How to write connector config

Below are a couple of examples on how to connect to a Kafka broker
by specifying
[the options passed to librdkafka](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
underneath.
Note that it is strongly recommended outside of test/dev
environments to have encrypted and authenticated communication.
In addition, in order for connectors to not have secrets in plaintext in their configuration,
it is recommended to use [secret management](/docs/enterprise/kubernetes-guides/secret-management).

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

### Using Kafka as a Debezium transport

The Kafka connector can be used to ingest data from a source via
Debezium.  For information on how to setup Debezium for MySQL see the
[Debezium MySQL](debezium-mysql.md) connector.

## Additional resources

For more information, see:

* [Tutorial section](/docs/tutorials/basics/part3#step-2-create-kafkaredpanda-connectors) which involves
  creating a Kafka input connector.

* Data formats such as [JSON](https://www.feldera.com/docs/formats/json) and
  [CSV](https://www.feldera.com/docs/formats/csv)

* Overview of Kafka configuration options:
  [librdkafka options](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
