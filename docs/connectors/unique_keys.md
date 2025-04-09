# Uniqueness Constraints

Feldera does not require tables or views to have unique keys. However, specifying
them where applicable can improve the reliability of data ingestion from external
sources and data delivery to external sinks in two key ways:

1. **Idempotent updates.** Unique keys enable idempotent updates by ensuring that
 each record in a table or view has a unique and stable identifier. This allows systems
 to deterministically overwrite existing records, avoiding duplication when processing
 repeated updates.

2. **Atomic updates.** Unique keys allow updates to be applied as single atomic
 operations, rather than as separate delete and insert events.

Below, we describe how unique keys are defined and used for tables and views in Feldera.

## Ingesting Data into Tables with Primary Keys

Feldera supports `PRIMARY KEY` constraints on tables using standard SQL
[syntax](/sql/grammar/#creating-tables). These constraints are enforced at runtime by
maintaining an internal index on the specified key columns:

- On **insert events**, Feldera atomically overwrites any existing record with the same primary key.

- On **delete events**, Feldera verifies that the specified key exists before applying the deletion.
 If no matching record is found, the delete is ignored.

## Views with Unique Keys

As Feldera processes changes to input tables, it emits incremental updates to output views as a stream of inserts and deletes. An update to an existing record is represented as a delete of the old value followed by an insert of the new value. In many cases, it is preferable to combine these two events into a single atomic update, which requires identifying them as pertaining to the same key.

The SQL language does not support primary or unique key constraints on views. Instead we use the [`CREATE INDEX` statement](/sql/grammar/) to define the key
as follows:

1. Use the `CREATE INDEX ON <view_name>(<column1>, ..., <columnN>)` statement to create an index that groups updates by the specified key columns.

2. Associate the index with an output connector by setting the connector’s `index` property to the name of the index. This allows the connector to merge insert and delete events for the same key into a single update event.

The specific behavior of this transformation depends on the data format and transport protocol used. Currently, the `index` property is supported only for:
- Kafka output connectors configured with the [Avro format](/formats/avro/)
- Postgres output connector [Postgres Output](/connectors/sinks/postgresql)

:::info

Do you need unique key support for other transports and data formats?
Let us know by [filing an
issue](https://github.com/feldera/feldera/issues) on GitHub.

:::

### Example

#### Postgres

See the [Postgres Output Connector documentation](/connectors/sinks/postgresql)
for more examples.

```sql
create table t0 (id int, s varchar);

create materialized view v1 with (
    'connectors' = '[{
        "index": "v1_idx",
        "transport": {
            "name": "postgres_output",
            "config": {
                "uri": "postgres://postgres:password@localhost:5432/postgres",
                "table": "feldera_out"
            }
        }
    }]'
) as select * from t0;
create index v1_idx on v1(id);
```

#### Avro Format

See the [Avro format documentation](/formats/avro#examples-1) for more examples.


```sql
create table my_table (
   id bigint,
   name string
);

create view my_view
with (
  -- Associate `my_index` (see below) with an output connector via the connector’s `index` property.
  'connectors' = '[{
    "index": "my_index",
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "127.0.0.1:19092",
        "topic": "my_topic",
        "auto.offset.reset": "earliest"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "raw",
        "registry_urls": ["http://127.0.0.1:18081"]
      }
    }
  }]'
)
as
   select * from my_view;

-- Create an index over `my_view`.
create index my_index on my_view(id);
```

:::warning

Unlike primary key constraints on tables, uniqueness of indexes on views is **not enforced** by the Feldera query engine. If an index is defined on columns that do not guarantee uniqueness, this may lead to incorrect behavior or data loss.

:::


