# Iceberg output connector

:::caution Experimental feature
Direct Iceberg output is not currently supported in Feldera.
However, you can write to Iceberg indirectly via Kafka Connect.
:::

### Writing to Iceberg via Kafka Connect

Feldera enables integration with Apache Iceberg by first writing
change data capture (CDC) records to Kafka, and then using the
Iceberg Sink Connector for Kafka Connect to persist these changes
to Iceberg tables.

#### Workflow

1. Feldera writes Avro-encoded CDC data to a Kafka topic.
2. Kafka Connect consumes this topic using the Iceberg Sink Connector.
3. The connector writes updates to an Iceberg table.

##### Avro format configuration

The Avro format must be configured with:
- `update_format` set to `raw`
- `cdc_field` defined (e.g. set to `op`)
  - This `cdc_field` should match the `iceberg.tables.cdc-field` config in Iceberg sink connector for Kafka Connect.
  - If `cdc_field` is not set, the Iceberg table will not be materialized.
  - Each output record will contain the field `op` which will have one of the following values:
    - `I` for **Insert**
    - `D` for **Delete**
    - `U` for **Upsert**

Sample setup with `datagen` as a data source:

```sql
create table tbl (
    order_number int not null primary key,
    pizza_name varchar,
    quantity int,
    toppings varchar array
) with (
        'connectors' = '[{ "transport": { "name": "datagen", "config": { "plan": [{ "limit": 100, "rate": 5 }] } } }]'
);

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
