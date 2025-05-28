# Iceberg Sink Kafka Connector

Direct Iceberg output is not currently supported in Feldera.
However, you can write to Iceberg indirectly via the Iceberg Sink Kafka Connect Connector.

## Writing to Iceberg via Kafka Connect

Feldera enables integration with Apache Iceberg by first writing
change data capture (CDC) records to Kafka, and then using the
[Iceberg Sink Connector for Kafka Connect](https://github.com/databricks/iceberg-kafka-connect) to persist these changes
to Iceberg tables.

#### Workflow

1. Feldera writes Avro-encoded CDC data to a Kafka topic.
2. Kafka Connect consumes this topic using the Iceberg Sink Connector.
3. The connector writes updates to an Iceberg table.

##### Avro format configuration

The Avro format must be configured with:
- An index on the identifying columns of the view, consistent with `iceberg.tables.default-id-columns`. 
- `update_format` set to `raw`
- `cdc_field` defined (e.g. set to `op`)
  - This `cdc_field` should match the `iceberg.tables.cdc-field` config in Iceberg sink connector for Kafka Connect.
  - If `cdc_field` is not set, it will be append-only log of output records without any CDC metadata.
  - Each output record will contain the field `op` which will have one of the following values:
    - `I` for **Insert**
    - `D` for **Delete**
    - `U` for **Upsert**

Example:

```sql
create materialized view pizzas with (
   'connectors' = '[
    {
      "index": "idx1",
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "localhost:29092",
              "topic": "pizzas"
          }
      },
      "format": {
          "name": "avro",
          "config": {
              "registry_urls": ["http://localhost:18081"],
              "update_format": "raw",
              "cdc_field": "op",
              "subject_name_strategy": "topic_name"
          }
      }
    }
   ]'
) as select * from tbl order by order_number desc limit 10;
create index idx1 on pizzas(order_number);
```

:::important
The index attribute is required and ensures proper materialization of the Iceberg table.
For more information, see documentation](/connectors/unique_keys#views-with-unique-keys).
:::

#### Iceberg sink connector configuration for Kafka Connect

Sample configuration for setting up Iceberg Sink Connector using Kafka
Connect.

```json
{
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "6",
    "topics": "pizzas",
    "iceberg.tables": "rpc.pizzas",
    "iceberg.catalog": "demo",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://iceberg-rest:8181",
    "iceberg.catalog.client.region": "us-east-1",
    "iceberg.catalog.s3.endpoint": "http://minio:9000",
    "iceberg.catalog.s3.path-style-access": "true",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.tables.evolve-schema-enabled": "true",
    "iceberg.control.commit.interval-ms": 30000,
    "iceberg.tables.cdc-field": "op",
    "iceberg.tables.default-id-columns": "order_number",
    "iceberg.tables.upsert-mode-enabled": "true",
    "key.converter.schema.registry.url": "http://registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schema.registry.url": "http://registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schemas.enable": "true"
}
```
- Ensure that `iceberg.tables.default-id-columns` is consistent with the index
  definition in Feldera.
- Ensure that the Iceberg table is accessible from Kafka Connect.
- Ensure that `iceberg.tables.auto-create-enabled` is set to `true` if the
  table doesn't already exist.
- Ensure that `iceberg.tables.evolve-schema-enabled` is set to `true` if you
  want schema to evolve as the schema of the output view evolves.
- Ensure that `iceberg.tables.cdc-field` is set to the same value as `cdc_field`
  in the output connector configuration in Feldera.
