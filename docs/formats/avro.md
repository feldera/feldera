# Avro Format

:::note
This page describes configuration options specific to the Avro data format.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

Feldera supports sending and receiving data in the Avro format. While Avro-encoded messages are commonly
transmitted over Kafka, any other supported [transport](/connectors/) can be used.
Avro is a strongly-typed format that requires a shared **schema** between the sender and receiver for successful
data encoding and decoding.

Feldera supports the streaming variant of the Avro format, where message schemas are managed
out-of-band by a **schema registry** service.  Instead of carrying the entire schema, Kafka messages
include only schema identifiers:

* **Sender**: Before sending the first message with a new schema, the sender registers the schema
  in the schema registry.
* **Receiver**: Upon receiving a message, the receiver retrieves the associated schema from the
  registry based on the schema identifier before decoding the message.

We support several Avro-based formats:

* **Raw** - every message contains a single Avro-encoded record that represents a row in a SQL
  table or view. This format does not carry any additional metadata that can be used to distinguish
  inserts and deletes.  It is therefore only suitable for representing inserts and upserts, but not
  deletions.

* **Debezium** (input only) - used to synchronize a Feldera table with an external database using
  [Debezium](https://debezium.io/).  See [Debezium source connector documentation](/connectors/sources/debezium)
  for more details.

* **Confluent JDBC** (output only) - used to send incremental changes computed by Feldera
  to an external database using the [Confluent JDBC connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/).
  See [Confluent JDBC sink documentation](/connectors/sinks/confluent-jdbc) for details.


## Avro input

### Schema management and schema evolution

To decode an Avro message, Feldera must obtain the Avro schema that was used to produce it.
Typically, this schema is retrieved from the schema registry. Alternatively, users can manually
provide the schema as a JSON string as part of connector configuration.

When utilizing a schema registry, each message contains an embedded schema ID, which is used to
look up the corresponding schema in the registry. Usually, all messages in the input stream have
the same schema ID. However, the data producer can modify the message structure and its corresponding
schema ID mid-stream. For instance, Debezium updates the schema whenever there is a change in the
connected database schema. This process is known as **schema evolution**.

Feldera supports schema evolution, provided that all schemas in the input stream are compatible
with the SQL table declaration to which the stream is connected, as described below.

### Schema compatibility

The Avro schema consists of metadata specific to the format (e.g., raw or Debezium) and
a table record schema.  The record schema must match the schema of the SQL table that the
connector is attached to:

* The Avro schema must be of type `record`.

* For every non-nullable column in the table, a field with the same name and a compatible type must be present in the Avro schema
  Note that the Avro schema is allowed to contain fields that don't exist in the SQL table.  Such fields are ignored by the parser.
  Conversely, the SQL table can contain **nullable** columns that are not present in the schema.  Such columns will
  be set to `NULL` during deserialization.

A SQL column and a field in the Avro schema are compatible if the following conditions are satisfied:

* If the Avro field is nullable, the SQL column is also nullable (however, a non-nullable
  field can be deserialized into either nullable or non-nullable column).

* The SQL column type and Avro field type must match according to the following table:

| SQL                           | Avro           | Comment                                                             |
|-------------------------------|----------------|---------------------------------------------------------------------|
| `BOOLEAN`                     | `boolean`      |                                                                     |
| `TINYINT`, `SMALLINT`, `INT`  | `int`          |                                                                     |
| `BIGINT`                      | `long`         |                                                                     |
| `REAL`                        | `float`        |                                                                     |
| `DOUBLE`                      | `double`       |                                                                     |
| `DECIMAL`                     |                | not yet supported                                                   |
| `CHAR`, `VARCHAR`             | `string`       |                                                                     |
| `BINARY`, `VARBINARY`         | `bytes`        |                                                                     |
| `DATE`                        | `int`          |                                                                     |
| `TIME`                        | `long` or `int`| logical type must be set to `time-millis` or `time-micros`          |
| `TIMESTAMP`                   | `long`         | logical type must be set to `timestamp-millis` or `timestamp-micros`|
| `ARRAY`                       | `array`        | Avro and SQL array element schemas must match                       |
| `MAP`                         | `map`          | SQL map keys must be of type `CHAR` or `VARCHAR`; Avro and SQL value schemas must match|
| `VARIANT`                     | `string`       | values of type `VARIANT` are deserialized from JSON-encoded strings (see [`VARIANT` documetation](/sql/json)) |
| user-defined types            | `record`       | Avro record schema must match SQL user-defined type definition according to the same schema compatibility rules as for SQL tables|

### Configuration

The following properties can be used to configure the Avro parser. All of these properties are optional.  However,
either `registry_urls` or `schema` properties must be specified.

| Property                      | Type                        |Default | Description                                                 |
|-------------------------------|-----------------------------|--------|----------------------------------------------------------|
| `update_format`               | `"raw"` or `"debezium"`|`"raw"` | Format used to encode data change events in this stream|
| `schema`                      | string | | Avro schema used to encode all records in this stream, specified as a JSON-encoded string. When this property is set, the connector uses the provided schema instead of retrieving the schema from the schema registry. This setting is mutually exclusive with `registry_urls`. |
| `skip_schema_id` | Boolean | `false` | `true` if serialized messages only contain raw data without the header carrying schema ID. See [Confluent documentation](<https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format>) for more details|
| `registry_urls`               | array of strings|`[]`| List of schema registry URLs. When non-empty, the connector retrieves Avro message schemas from the registry.|
| `registry_proxy`              | string          | | Proxy that will be used to access the schema registry. Requires `registry_urls` to be set.|
| `registry_timeout_secs`       | string          | | Timeout in seconds used to connect to the registry. Requires `registry_urls` to be set.|
| `registry_username`           | string          | | Username used to authenticate with the registry.Requires `registry_urls` to be set. This option is mutually exclusive with token-based authentication (see `registry_authorization_token`).|
| `registry_password`           | string          | | Password used to authenticate with the registry. Requires `registry_urls` to be set.|
| `registry_authorization_token`| string          | | Token used to authenticate with the registry. Requires `registry_urls` to be set. This option is mutually exclusive with password-based authentication (see `registry_username` and `registry_password`).|

### Examples

Configure the Avro parser to receive raw Avro records without embedded schema ids using a static user-provided schema.

```sql
CREATE TABLE my_table (
    id INT NOT NULL PRIMARY KEY,
    ts TIMESTAMP
) with (
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "bootstrap.servers": "localhost:19092",
        "auto.offset.reset": "earliest",
        "topics": ["my_topic"]
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "schema": "{\"type\":\"record\",\"name\":\"ExampleSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}",
        "skip_schema_id": true,
        "update_format": "raw"
      }
    }
}]');
```

Configure the Avro parser to ingest data change events from Debezium (refer to [Debezium connector documentation](/connectors/sources/debezium) for additional details on setting up the Debezium source connector).

```sql
CREATE TABLE my_table (
    id INT NOT NULL PRIMARY KEY,
    ts TIMESTAMP
) with (
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "bootstrap.servers": "localhost:19092,
        "auto.offset.reset": "earliest",
        "topics": ["my_topic"]
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "registry_urls": ["http://localhost:18081"],
        "update_format": "debezium"
      }
    }
}]');
```

## Avro output

### Schema management

The Avro encoder generates an Avro schema, which the consumer can use to decode messages produced
by the encoder. For message formats that include key and value components with different schemas,
e.g., the Confluent JDBC connector format, the encoder generates both key and value schemas.
If the connector configuration specifies a schema registry, the encoder publishes both
key and value schemas in the registry (see [Configuration](#configuration) below).

The encoder supports an alternative workflow where users provide the schema as a JSON string
as part of connector configuration and the encoder produces messages using this schema instead of
generating it automatically. If the connector configuration specifies a schema registry, the encoder
publishes both the user-provided schema to the registry.

### Configuration

The following properties can be used to configure the Avro encoder. All of these properties are optional.
However, exactly one of `registry_urls` and `schema` properties must be specified.


| Property                       | Type                        |Default | Description                                                 |
|--------------------------------|-----------------------------|--------|----------------------------------------------------------|
| `update_format`                | `"raw"` or `"confluent_jdbc"`|`"raw"` | Format used to encode data change events in this stream|
| `schema`                       | string | | Avro schema used to encode output records. When specified, the encoder will use this schema; otherwise it will automatically generate an Avro schema based on the SQL view definition. Specified as a string containing schema definition in JSON format. This schema must match precisely the SQL view definition, modulo nullability of columns.|
| `namespace`                    | string | | Avro namespace for the generated Avro schemas.|
| `subject_name_strategy`        | `"topic_name"`, `"record_name"`, or `"topic_record_name"` | see description| Subject name strategy used to publish Avro schemas used by the connector in the schema registry. When this property is not specified, the connector chooses subject name strategy automatically, `topic_name` for `confluent_jdbc` update format or `record_name` for `raw` update format.|
| `key_fields`                   | array of strings| | When this option is set, only the listed fields appear in the Debezium message key. This option is only valid with the `confluent_jdbc` update format. It is used when writing to a table with primary keys. For such tables, the Confluent JDBC sink connector expects the message key to contain only the primary key columns. When this field is set, the connector generates a separate Avro schema, containing only the listed fields, and uses this schema to encode Kafka message keys.|
| `skip_schema_id`               | Boolean | `false` | Set to `true` if serialized messages should only contain raw data without the header carrying schema ID. `False` by default. See https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format|
| `registry_urls`                | array of strings| `[]` | List of schema registry URLs. When non-empty, the connector retrieves Avro message schemas from the registry.|
| `registry_proxy`               | string          | | Proxy that will be used to access the schema registry. Requires `registry_urls` to be set.|
| `registry_timeout_secs`        | string          | | Timeout in seconds used to connect to the registry. Requires `registry_urls` to be set.|
| `registry_username`            | string          | | Username used to authenticate with the registry.Requires `registry_urls` to be set. This option is mutually exclusive with token-based authentication (see `registry_authorization_token`).|
| `registry_password`            | string          | | Password used to authenticate with the registry. Requires `registry_urls` to be set.|
| `registry_authorization_token` | string          | | Token used to authenticate with the registry. Requires `registry_urls` to be set. This option is mutually exclusive with password-based authentication (see `registry_username` and `registry_password`).|

### Examples

Configure the Avro encoder to send raw Avro records using a static user-provided schema.  The configuration does not
specify a schema registry URL, so the encoder will not try to publush the schema.

```sql
CREATE VIEW my_view
WITH (
  'connectors' = '[{
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "localhost:19092",
        "topic": "my_topic",
        "auto.offset.reset": "earliest"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "schema": "{\"type\":\"record\",\"name\":\"ExampleSchema\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]}]}"
      }
    }
  }]'
)
AS
    SELECT * from my_table;
```

Configure the Avro encoder to output changes in the format expected by the
[Confluent JDBC Kakfa Connect connector](https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/).  The encoder
will generate two separate schemas for the key and value components of the message and publish them in the
schema registry.

```sql
create view my_view
WITH (
  'connectors' = '[{
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "localhost:19092",
        "topic": "my_topic"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "confluent_jdbc",
        "registry_urls": ["http://localhost:18081"],
        "key_fields": ["id"]
      }
    }
  }]'
)
as select * from test_table;
```
