# Debezium-Postgres input connector

Feldera can consume a stream of changes to a Postgres database through the use of
[Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
with the [Debezium](https://debezium.io/) plug-in installed.
This connection to Feldera can be done in three steps:

1. **Configure your Postgres database to work with Debezium**.

2. **Create the Kafka Connect input connector:** contact the Kafka Connect instance
   to create a Kafka Connect Debezium input connector which is connected to the Postgres database.
   In turn, Kafka Connect will create Kafka topics automatically based on the database
   schema it detects. Each Kafka topic corresponds to a table.

3. **Create Feldera input connectors:** create a Feldera input connector to the
   Kafka topic with Debezium-Postgres formatting. For each table, a Feldera input
   connector must be created.

## Step 1: Configure Postgres to work with Debezium

Follow [Debezium documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#setting-up-postgresql)
to configure your Postgres database to work with Debezium.

## Step 2: Create Kafka Connect input connector

Create the Debezium-Postgres connector using an HTTP POST request to the
Kafka Connect instance.

**NOTE: the curl command below is only for demo purposes.
Passwords or other secrets should not be put in plaintext in connector configurations.
They should be managed/externalized with appropriate secret management.
For more information, see the notes in the
[official Debezium tutorial](https://debezium.io/documentation/reference/tutorial.html)
as well as [KIP-297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations)
to which they refer. There is an [experimental secret management](../../enterprise/kubernetes-guides/secret-management) guide
to externalize secrets via Kubernetes.**

```
curl -i -X \
  POST -H "Accept:application/json" -H "Content-Type:application/json" \
  [KAFKA CONNECT HOSTNAME:PORT]/connectors/ -d \
  '{ "name": "my-connector",
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

where
* `[KAFKA CONNECT HOSTNAME:PORT]` : The Kafka Connect hostname and port separated by a colon.
* `[KAFKA HOSTNAME:PORT]` : The Kafka broker hostname and port separated by a colon.
* `[POSTGRES HOST NAME]` : The hostname of your Postgres instance.
* `[POSTGRES PORT]` : The port of your Postgres instance (generally 5432)
* `[DEBEZIUM USERNAME]` : Postgres username to be used by the Debezium connector (typically created during Step 1)
* `[DEBEZIUM PASSWORD]` : Postgres password for the Debezium connector
* `[DATABASE NAME]` : Name of the Postgres database to connect to
* `[TABLE LIST]` : A comma-separated list of regular expressions that match fully-qualified table identifiers
   for tables whose changes you want to capture
* `[KAFKA TOPIC PREFIX]` : Prefix to be added to each per-table Kafka topic created by this connector.

:::note

Note that the last two configuration options are essential and must always be
specified when configuring the Postgres Debezium connector to work with Feldera:

* "decimal.handling.mode": "string" - required for Feldera to correctly parse decimal values received from Postgres.

* "time.precision.mode": "connect" - required for Feldera to correctly parse timestamp values received from Postgres.

:::

See [Debezium documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties)
for a complete list of available configuration options.

## Step 3: Feldera input connector

Finally, you must create an input connector for each table:

```bash
curl -i -X PUT http://localhost:8080/v0/connectors/my_connector \
-H "Authorization: Bearer <API-KEY>" \
-H 'Content-Type: application/json' \
-d '{
  "description": "Debezium connector for a Postgres table",
  "config": {
      "format": {
          "name": "json",
          "config": {
              "update_format": "debezium",
              "json_flavor": "debezium_postgres"
          }
      },
      "transport": {
          "name": "kafka_input",
          "config": {
              "bootstrap.servers": [KAFKA HOST NAME:PORT],
              "auto.offset.reset": "earliest",
              "topics": [KAFKA TOPIC NAME]
          }
      }
  }
}'
```

You can now attach this connector to the corresponding table in your Feldera pipeline.
