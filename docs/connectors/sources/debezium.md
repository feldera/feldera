# Debezium input connector

:::note
This page describes configuration options specific to the Debezium source connector.
See [top-level connector documentation](/docs/connectors/) for general information
about configuring input and output connectors.
:::

Debezium is a widely-used **Change Data Capture** (CDC) technology that streams real-time changes from databases such
as PostgreSQL, MySQL, and Oracle to Kafka topics. Feldera can consume these change streams as inputs. We support
Debezium streams encoded in both [JSON](/docs/formats/json) and [Avro](/docs/formats/avro) formats. Synchronizing
a set of database tables with Feldera using Debezium involves three steps:

1. [**Configure your database to work with Debezium**](#step-1-configure-your-database-to-work-with-debezium)
2. [**Create the Kafka Connect input connector**](#step-2-create-kafka-connect-input-connector)
3. [**Create Feldera input connectors**](#step-3-create-feldera-input-connector)

## Step 1: Configure your database to work with Debezium

Each database type requires specific configuration settings to integrate with Debezium.
For detailed instructions on configuring your database, refer to the
[Debezium documentation](https://debezium.io/documentation/reference/).

## Step 2: Create Kafka Connect input connector

Debezium is built on top of
[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html).  You will need
to install Kafka Connect along with a Debezium plugin for your database.
Next you will need to use the Kafka Connect REST API to create a source connector to stream
changes from the database change log to a Kafka topic.

Refer to the [Debezium documentation](https://debezium.io/documentation/reference/) to configure
the connector according to your requirements, including database connectivity and selecting the
subset of tables to synchronize. Debezium can produce data change events in either JSON or Avro
formats, both of which are supported by Feldera (see [examples](#examples) below). When using the
JSON format, ensure the following Kafka Connect properties are set to enable Feldera to correctly
deserialize data change events from JSON:

* Set `"decimal.handling.mode": "string"` - required for Feldera to correctly parse decimal values.
* In addition, for Postgres, Oracle, and SQL Server set `"time.precision.mode": "connect"`

### Examples

Create a Debezium connector to read changes from a Postgres database into JSON-encoded Kafka topics:

```
curl -i -X \
  POST -H "Accept:application/json" -H "Content-Type:application/json" \
  [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
  '{
      "name": "my-connector",
      "config": {
          "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
          "database.hostname": "[POSTGRES HOST NAME]",
          "database.port": "[POSTGRES PORT]",
          "database.user": "[DEBEZIUM USERNAME]",
          "database.password": "[DEBEZIUM PASSWORD]",
          "database.dbname": "[DATABASE NAME]",
          "table.include.list": "[TABLE LIST]",
          "topic.prefix": "[KAFKA TOPIC PREFIX]",
          "decimal.handling.mode": "string",
          "time.precision.mode": "connect"
      }
  }'
```

Create a Debezium connector to read changes from a Postgres database into Avro-encoded Kafka topics.  Note that connector configuration must include a schema registry URL, used to publish
Avro message schemas used to encode Debezium Kafka messages.

```
curl -i -X \
  POST -H "Accept:application/json" -H "Content-Type:application/json" \
  [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
  '{
      "name": "my-connector",
      "config": {
          "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
          "database.hostname": "[POSTGRES HOST NAME]",
          "database.port": "[POSTGRES PORT]",
          "database.user": "[DEBEZIUM USERNAME]",
          "database.password": "[DEBEZIUM PASSWORD]",
          "database.dbname": "[DATABASE NAME]",
          "table.include.list": "[TABLE LIST]",
          "topic.prefix": "[KAFKA TOPIC PREFIX]",
          "key.converter": "io.confluent.connect.avro.AvroConverter",
          "value.converter": "io.confluent.connect.avro.AvroConverter",
          "key.converter.schemas.enable": "true",
          "value.converter.schemas.enable": "true",
          "key.converter.schema.registry.url": [SCHEMA REGISTRY URL],
          "value.converter.schema.registry.url": [SCHEMA REGISTRY URL]
      }
  }'
```

Create a Debezium connector to read changes from a MySQL database into JSON-encoded Kafka topics:

```
curl -i -X \
    POST -H "Accept:application/json" -H "Content-Type:application/json" \
    [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
    '{ "name": "my-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "[MYSQL HOST NAME]",
            "database.port": "[MYSQL PORT]",
            "database.user": "[DEBEZIUM USERNAME]",
            "database.password": "[DEBEZIUM PASSWORD]",
            "database.server.id": "[UNIQUE DATABASE SERVER ID]",
            "database.server.name": "[UNIQUE DATABASE SERVER NAME]",
            "database.include.list": "[DATABASES TO CONNECT]",
            "database.history.kafka.bootstrap.servers": "[KAFKA HOSTNAME:PORT]",
            "topic.prefix": "[DATABASE SERVER NAME]",
            "schema.history.internal.kafka.topic": "schema-changes.[DATABASE SERVER NAME].internal",
            "schema.history.internal.kafka.bootstrap.servers": "[KAFKA HOSTNAME:PORT]",
            "include.schema.changes": "true",
            "decimal.handling.mode": "string",
        }
    }'
```

:::tip
Refer to the [secret management](../../enterprise/kubernetes-guides/secret-management) guide
to externalize secrets such as DBMS passwords via Kubernetes.
:::

## Step 3: Create Feldera input connector

Configure an input connector for each Feldera SQL table that must ingest changes from Debezium.
Use the `kafka_input` transport with either `json` or `avro` format. Debezium automatically
creates a Kafka topic for each database table.

### JSON

When using JSON encoding, make sure to set the following connector properties:

* `"update_format": "debezium"`
* `"json_flavor"` depending on the database:
  * For MySQL and MariaDB, set `"json_flavor": "debezium_mysql"`
  * For all other databases set `"json_flavor": "debezium_postgres"`

Configure a Feldera connector to ingest changes from a Postgres DB via a JSON-encoded Kafka topics:

```sql
CREATE TABLE my_table (
  example_field: INT
) WITH (
  'connectors' = '[{
      "transport": {
          "name": "kafka_input",
          "config": {
              "bootstrap.servers": "localhost:9092",
              "auto.offset.reset": "earliest",
              "topics": ["my_topic"]
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "debezium",
              "json_flavor": "debezium_postgres"
          }
      }
  }]'
)
```

Configure a Feldera connector to ingest changes from a Postgres DB via a JSON-encoded Kafka topics:

```sql
CREATE TABLE my_table (
  example_field: INT
) WITH (
  'connectors' = '[{
      "transport": {
          "name": "kafka_input",
          "config": {
              "bootstrap.servers": "localhost:9092",
              "auto.offset.reset": "earliest",
              "topics": ["my_topic"]
          }
      },
      "format": {
          "name": "json",
          "config": {
              "update_format": "debezium",
              "json_flavor": "debezium_mysql"
          }
      }
  }]'
)
```

### Avro

Configure a Feldera connector to ingest changes from an Avro-encoded Kafka topic.
Make sure to specify the URL of the schema registry to retrieve the Avro schema
for decoding the messages as part of Avro format configuration.

```sql
CREATE TABLE my_table (
    id INT NOT NULL PRIMARY KEY,
    ts TIMESTAMP
) with (
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "bootstrap.servers": "localhost:9092",
        "auto.offset.reset": "earliest",
        "topics": ["my_topic"]
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "registry_urls": ["http://localhost:8081"],
        "update_format": "debezium"
      }
    }
}]');
```

* For more details on JSON support in Feldera, please refer to the [JSON Format Documentation](/docs/formats/json).
* For more details on Avro support in Feldera, please refer to the [Avro Format Documentation](/docs/formats/avro).
* For more information on configuring Kafka transport, visit the [Kafka Source Connector Documentation](/docs/connectors/sources/kafka).

