# Snowflake output connector (experimental)

The Feldera Snowflake connector ingests data change events produced by a Feldera
pipeline into a Snowflake database in near-realtime.

Because this connector uses the [Kafka output adapter](kafka), it
supports [fault tolerance](/pipelines/fault-tolerance) too.

:::caution Experimental feature

Snowflake support is an experimental feature of Feldera.

:::

## Architecture

We use the [Snowflake Connector for
Kafka](https://docs.snowflake.com/user-guide/kafka-connector) to stream changes
from Feldera to Snowflake with low latency.

1. Feldera outputs a stream of changes to a table or view to a Kafka topic.

1. The Snowflake Connector for Kafka reads the changes from Kafka and pushes them to a
Snowflake **landing table** using the Snowpipe Streaming API.  The landing table can
be seen as a change log containing a sequence of insert and delete commands to
be applied to the **target table**.

1. A periodic data ingestion task removes updates buffered in the landing tables
and applies them to the target tables.

## Terminology

We use the following terms throughput this document.

* **Target schema**: a Snowflake schema that contains one or more target tables,
  i.e., tables that are the final recipients of updates from Feldera.  We use the
  `&{schema}` variable in SQL scripts below to refer to the target schema.  For
  simplicity, the instructions below assume a single target schema, but they can
  be easily adapted for multiple schemas.

* **Landing schema**: a Snowflake schema that contains intermediate tables
  that store data change events until they are ingested
  into the target tables.  Below we assume that the landing schema is called
  `&{schema}_landing`, but the user can choose any other name for it.

* **The Feldera user**: a Snowflake user account used by the Feldera Snowflake
  connector to stream data change events to Snowflake.  This account
  must be configured with read/write access to the landing schema.  It does not
  require access to the target schema.

## Configure Snowflake

Run the following SQL commands to create the landing schema in the
same database that stores the target schema.  We assume that the `&{db}`
and `&{schema}` variables contain respectively the name of the database and
the target schema.

```sql
!set variable_substitution=true

-- Create a schema for landing tables.
USE ROLE accountadmin;
CREATE SCHEMA IF NOT EXISTS &{db}.&{schema}_landing;

-- Create a role and a user account that Feldera will use
-- to access the landing schema.
USE ROLE securityadmin;

CREATE ROLE IF NOT EXISTS feldera;
GRANT ALL ON SCHEMA &{db}.&{schema}_landing TO ROLE feldera;

CREATE USER IF NOT EXISTS feldera;
GRANT ROLE feldera TO USER feldera;
ALTER USER feldera SET DEFAULT_ROLE = feldera;
```

Follow [Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth)
to setup private key authentication for the `feldera` user.

## Create landing tables

Create a landing table for each target table.  The landing table has the same
columns as the target, but none of its constraints (`UNIQUE`, `PRIMARY KEY`,
`FOREIGN KEY`, `DEFAULT`, `NOT NULL`).  It also contains several metadata columns
used by Feldera to apply updates in the correct order.

For example, given a target table `t1` with the following definition:

```sql
CREATE TABLE t1 (
    id NUMBER NOT NULL PRIMARY KEY,
    seq NUMBER DEFAULT seq1.NEXTVAL,
    foreign_id NUMBER,
    FOREIGN KEY (foreign_id) REFERENCES other_table(id)
);
```

we create the following table in the landing schema:

```sql
!set variable_substitution=true

USE SCHEMA &{db}.&{schema}_landing;

CREATE TABLE t1 (
    -- Columns from the target table with all constraints removed.
    id NUMBER,
    seq NUMBER,
    foreign_id NUMBER,

    -- Additional metadata columns.
    __action STRING NOT NULL,
    __stream_id NUMBER NOT NULL,
    __seq_number NUMBER NOT NULL,
    UNIQUE (__stream_id, __seq_number)
)
-- Required by Snowpipe Streaming.
ENABLE_SCHEMA_EVOLUTION=TRUE;

-- Create a Snowflake stream to track changes
-- to the landing table.
CREATE STREAM T1_STREAM ON TABLE T1 APPEND_ONLY = TRUE;
```

The last statement in this snippet attaches a
[Snowflake stream](https://docs.snowflake.com/en/user-guide/streams-intro)
to the landing table.  This stream will be used by
the data ingestion task to track changes to the table.

## Create the data ingestion task

:::caution

For the data ingestion process to work correctly, the output views computed
by Feldera must respect constraints defined for the target table in Snowflake.
For example, if the target table defines a primary or a unique key constraint,
then the corresponding view computed by Feldera should never contain more than
one record with the same value of the key columns.

:::

Create a data ingestion task to periodically
apply updates buffered in the landing tables to the target tables.
The role used to execute this SQL script must have write privileges
for both the landing and the target tables, as well as
the privileges listed in
[Snowflake Access Control requirements](https://docs.snowflake.com/en/sql-reference/sql/create-task#access-control-requirements)
for task creation.  In addition, if the user chooses to run the
task in the [serverless mode](https://docs.snowflake.com/en/user-guide/tasks-intro#serverless-tasks)
the role must have the `EXECUTE MANAGED TASK` privilege.

```sql
!set variable_substitution=true

USE SCHEMA &{db}.&{schema}_landing;

!set sql_delimiter=/

CREATE TASK INGEST_DATA
  -- Run the task once a minute.
  SCHEDULE = '1 minute'
  -- By not specifying a warehouse in which to run the task, we
  -- opt for the serverless model (requires the `EXECUTE MANAGED TASK`
  -- privilege).  Uncomment the following line in order to run the
  -- task in a user-managed warehouse instead.
  --WAREHOUSE = <your_warehouse_name>
  WHEN
    ((SYSTEM$STREAM_HAS_DATA('T1_STREAM'))
    -- When synchronizing multiple tables, add a clause for each additional table below
    -- or (SYSTEM$STREAM_HAS_DATA('<table>_STREAM')))
  AS
    BEGIN
        START TRANSACTION;

        -- Merge data from the stream into the target table.
        MERGE INTO &{schema}.T1 AS T
        USING (
            SELECT * FROM T1_STREAM where (__stream_id, __seq_number)
                in (SELECT __stream_id, max(__seq_number) as __seq_number
                    FROM PRICE_STREAM GROUP BY (id, __stream_id))
        ) AS S ON (T.id = S.id)
        WHEN MATCHED AND S.__action = 'delete' THEN
            DELETE
        WHEN MATCHED AND S.__action = 'insert' THEN
            UPDATE SET T.seq = S.seq, T.foreign_id = S.foreign_id
        WHEN NOT MATCHED AND S.__action = 'insert' THEN
            INSERT (id, seq, foreign_id)
            VALUES (S.id, S.seq, S.foreign_id);

        -- Delete ingested records from the landing table.
        DELETE from T1 WHERE (__stream_id, __seq_number) in (SELECT __stream_id, __seq_number FROM T1_STREAM);

        COMMIT;
    END;/

!set sql_delimiter=";"

-- Start running the task periodically.
ALTER TASK ingest_data RESUME;
```

## Create a Kafka Connector for Snowflake

Use the Kafka Connect REST API to create a Snowflake Connector
configured to read data change events from a set of Kafka topics and store them
in the landing tables.

```bash
curl -X POST <kafka_connect_url> -d '{
    "name": "my-snowflake-connector",
    "config": {
        "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
        "tasks.max": "8",
        "topics": "snowflake.t1",
        "snowflake.topic2table.map": "snowflake.t1:t1",
        "errors.tolerance": "none",
        "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
        "snowflake.enable.schematization": "TRUE",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.url.name": "<account_name>.snowflakecomputing.com:443",
        "snowflake.user.name": "feldera",
        "snowflake.role.name": "feldera",
        "snowflake.private.key": <private_key>,
        "snowflake.private.key.passphrase": <passphrase>,
        "snowflake.database.name": <database_name>,
        "snowflake.schema.name": <landing_schema>,
        "buffer.flush.time": "1",
        "max.poll.interval.ms": "10000",
        "buffer.count.records": "10000",
    }
}'
```

We explain the configuration options below.
See also [Kafka Connector for Snowflake documentation](https://docs.snowflake.com/en/user-guide/kafka-connector-install#configuring-the-kafka-connector).

* `topics` - comma-separate list of Kafka topics to read from, with one topic per table.

* `snowflake.topic2table.map` - comma-separated list of Kafka topic names to table name mappings.

* `errors.tolerance` - determines how many parsing errors the connector can accept before
  going into the failed state.  Setting this parameter to `none` will cause the connector to
  stop after encountering the first error, giving the operator a chance to fix the problem
  before restarting the connector. Setting it to `all` configures the connector to continue
  working after encountering any number of invalid Kafka messages.  If you choose this
  option, we strongly recommend enabling the [Dead Letter Queues](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka#dead-letter-queues)
  feature in order to record problematic messages in a separate Kafka topic.

* `snowflake.ingestion.method` - must be set to `SNOWPIPE_STREAMING` to load data into
   Snowflake using [Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-kafka).

* `snowflake.enable.schematization` - must be set to `TRUE` to ingest data into strongly
  type columns rather than storing it as raw JSON objects.

* `snowflake.url.name` - URL for accessing your Snowflake account, which has the following
  format: `<account_name>.snowflakecomputing.com:443`, where `<account_name>` is an account
  identifier for an account in your organization using the
  [`<orgname>-<account_name>` format](https://docs.snowflake.com/en/user-guide/admin-account-identifier#using-an-account-name-as-an-identifier)

* `snowflake.user.name` - Snowflake user account created for the Feldera Snowflake connector
  during the [Snowflake Configuration step](#configure-snowflake).

* `snowflake.role.name` - Snowflake role created for the Feldera Snowflake connector
  during the [Snowflake Configuration step](#configure-snowflake).

* `snowflake.private.key` - private key created for the Feldera user by following
  [Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth).
  Include only the key, not the header or footer. If the key is split across multiple
  lines, remove the line breaks. You can provide an unencrypted key, or you can provide
  an encrypted key and provide the `snowflake.private.key.passphrase` parameter to
  enable Snowflake to decrypt the key.

* `snowflake.private.key.passphrase` - specify this parameter when using an encrypted
  private key.  The connector will use this string to decrypt the password.

* `snowflake.database.name` - Snowflake database that contains the landing schema
  (see [Create Landing Tables](#create-landing-tables)).

* `snowflake.schema.name` - landing schema name
  (see [Create Landing Tables](#create-landing-tables)).

* `buffer.flush.time` - maximum number of seconds the connector will buffer Kafka messages
   before sending them to Snowflake.  The default value is 120 seconds.

* `buffer.count.records` - maximum number of Kafka messages buffered by the connector.

* `max.poll.interval.ms` - determines the frequency with which the connector polls
  Kafka for new messages.


## Create Feldera Snowflake connector

The Snowflake connector uses a Kafka output transport, so the specification
of the connector is the same as for [Kafka outputs](kafka.md).
For example, in the view declaration we can specify the connector properties:

```sql
CREATE VIEW V AS ...
WITH (
   'connectors' = '[{
      "transport": {
          "name": "kafka_output",
          "config": {
              "bootstrap.servers": "redpanda:9092",
              "topic": "snowflake.price",
              "security.protocol": "plaintext"
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

As you start the pipeline, updates to the output view attached to the Snowflake
connector should get ingested into Snowflake and appear in your target tables.
The end-to-end ingestion latency is currently bounded by the frequency of running
the data ingestion task, e.g., 1 minute in the [example](#create-the-data-ingestion-task)
above.

## Troubleshooting

There are several things you can check if the data does not show up in the target tables.

### Is the Kafka connector in the `RUNNING` state?

Check that the Snowflake Kafka connector is running by polling its `/status`
endpoint.  The connector can fail due to a misconfiguration or invalid input data.

```bash
curl -X GET <kafka-connect-server>/connectors/<snowflake-connector-name>/status
```

### Is data being produced to the Kafka topic?

Check that the pipeline outputs data change events to the Kafka topic.  For
instance, using Redpanda as a Kafka broker and the `rpk` command line utility:

```bash
rpk topic consume <topic_name>
```

### Is the data ingestion task running in Snowflake?

Use the following Snowflake SQL command to retrieve the list
of tasks in the landing schema.

```sql
SHOW TASKS in <db>.<landing_schema_name>;
```

Make sure that the data ingestion task is in the `started` state.
