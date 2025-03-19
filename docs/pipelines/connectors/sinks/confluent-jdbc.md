# Confluent JDBC output connector

:::note
This page describes configuration options specific to integration with the Confluent JDBC sink connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

The [Confluent JDBC Sink Connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html) for
[Kafka Connect](https://docs.confluent.io/platform/current/connect/) allows exporting data from Kafka
topics to any relational database with a JDBC driver.  Feldera integrates with this connector
to write a stream of changes to a SQL view to an external database via a Kafka topic.

The Confluent JDBC connector supports JSON and Avro-based data formats.  Feldera currently only supports the
Avro format expected by this connector.  In this format, the key of the Kafka message contains the primary key
of the target table. The value component of the message contains the new or updated value for this primary key
or `null` if the key is to be deleted from the table.

Because this connector uses the [Kafka output adapter](kafka), it
supports [fault tolerance](..#fault-tolerance) too.

Setting up a Confluent JDBC Sink Connector integration involves three steps:

1. [**Create Kafka topics**](#step-1-create-kafka-topics)
2. [**Create a JDBC Sink Connector**](#step-2-create-a-jdbc-sink-connector)
3. [**Configure Feldera output connectors**](#step-3-create-feldera-output-connectors)

## Prerequisites

The Confluent JDBC Sink connector is built on top of the Kafka Connect platform.
You will need to configure the following services:

* Kafka
* Kafka Connect
* A Kafka schema registry
* Confluent JDBC plugin for Kafka Connect (see [Confluent documentation](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/) for details).

## Step 1: Create Kafka topics

Create a separate Kafka topic for each SQL view you would like to connect to an external database.

## Step 2: Create a JDBC Sink Connector

Use the Kafka Connect REST API to create a sink connector to stream
changes to the database change from Kafka topics.  The following command was
tested with the Confluent JDBC Sink Connector v10.7.11 and Kafka Connect v3.7.0:

```
curl -i -X \
  POST -H "Accept:application/json" -H "Content-Type:application/json" \
  [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
  '{
      "name": "my-connector",
      "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "dialect.name": "PostgreSqlDatabaseDialect",
            "connection.url": "jdbc:postgresql://postgres:5432/my_database",
            "connection.user": "postgres",
            "connection.password": "postgres",
            "insert.mode": "upsert",
            "delete.enabled": true,
            "pk.mode": "record_key",
            "auto.create": true,
            "auto.evolve": true,
            "schema.evolution": "basic",
            "database.time_zone": "UTC",
            "topics": "my_table1,my_table2,my_table3",
            "errors.deadletterqueue.topic.name": "dlq",
            "errors.deadletterqueue.context.headers.enable": true,
            "errors.deadletterqueue.topic.replication.factor": 1,
            "errors.tolerance": "all",
            "key.converter.key.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy",
            "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schemas.enable": true,
            "value.converter.schemas.enable": true,
            "key.converter.schema.registry.url": "http://redpanda:8081",
            "value.converter.schema.registry.url": "http://redpanda:8081"
        }
  }'
```

We can break this down into required settings, that must be used for the connector to work correctly with Feldera,
and recommended settings, which can be tuned to your specific use case.

Required settings:
* Use the JDBC Sink connector plugin:
  * `"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"`
* Use Avro encoding for keys and values; retrieve Avro schemas from a schema registry:
  * `"key.converter": "io.confluent.connect.avro.AvroConverter"`
  * `"value.converter": "io.confluent.connect.avro.AvroConverter"`
  * `"key.converter.schemas.enable": true`
  * `"value.converter.schemas.enable": true`
  * `"key.converter.schema.registry.url": "<schema_registry_url>"`
  * `"value.converter.schema.registry.url": "<schema_registry_url>"`
* Derive schema subject name from the Kafka topic name:
  * `"key.converter.key.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy"`
  * `"value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy"`
* Configure the connector to support arbitrary database changes (inserts, deletes, and updates); retrieve
  primary key value for the target table from the Kafka message key:
  * `"insert.mode": "upsert"`
  * `"delete.enabled": true`
  * `"pk.mode": "record_key"`
* Database connectivity (specific settings depend on your database type)
  * `"dialect.name": "PostgreSqlDatabaseDialect"` - specify your database type
  * `"connection.url": "jdbc:postgresql://postgres:5432/my_database"`
  * `"connection.user": "postgres"`
  * `"connection.password": "postgres"`

Recommended settings:
* Automatically derive
  * `"auto.create": true`
  * `"auto.evolve": true`
  * `"schema.evolution": "basic"`
* Configure Dead Letter Queue for Kafka messages that couldn't be successfully processed by the JDBC connector:
  * `"errors.deadletterqueue.topic.name": "dlq"`
  * `"errors.deadletterqueue.context.headers.enable": true`
  * `"errors.deadletterqueue.topic.replication.factor": 1`
  * `"errors.tolerance": "all"`

## Step 3: Create Feldera output connectors

Configure an output connector for each Feldera SQL view that will send changes to the JDBC connector.
Use the `kafka_output` transport with `avro` output format. Set the following Avro encoder parameters:

* `"update_format": "confluent_jdbc"` - use Confluent JDBC connector-compatible Avro encoding.
* `"registry_urls": ["<registry_url2>", "<registry_url2>", ...]` - the connector will publish Avro
  schemas for key and value components of Kafka messages to the specified schema registry.
* `"key_fields": ["key_field1", "key_field2", ...]` - list of SQL view columns that form the primary
  key in the external database table. When not specified, the encoder assumes that all columns in the
  view are part of the primary key.

```sql
create view my_view
WITH (
  'connectors' = '[{
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "redpanda:9092",
        "topic": "my_topic"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "confluent_jdbc",
        "registry_urls": ["http://redpanda:8081"],
        "key_fields": ["id"]
      }
    }
  }]'
)
as select * from test_table;
```

:::note
The user is responsible for selecting a set of columns for the `key_fields` property that are
guaranteed to have unique values.  Failure to choose a unique key may lead to data loss.
:::

* For more details on Avro support in Feldera, please refer to the [Avro Format Documentation](/formats/avro).
* For more information on configuring Kafka transport, visit the [Kafka Sink Connector Documentation](/connectors/sinks/kafka).
