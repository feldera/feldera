# PostgreSQL input connector

:::note
This page describes configuration options specific to the PostgreSQL connector.
See [top-level connector documentation](/connectors/) for general information
about configuring input and output connectors.
:::

[PostgreSQL](https://postgresql.org/) is an open-source SQL database.
We support loading PostgreSQL data into Feldera using a custom connector that
allows to execute SQL queries against a PostgreSQL database and stream the
results into Feldera. The connector is based on the [tokio-postgres](https://docs.rs/tokio-postgres/) Rust library.

The PostgreSQL connector does not yet support fault tolerance.

## PostgreSQL input connector configuration

| Property | Type   | Default    | Description                                                                                                                                                                                                                                     |
|----------|--------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uri`*   | string |            | A PostgreSQL connection URL, e.g., "postgresql://postgres:1234@localhost:7373/postgres" (see the tokio-postgres [Config](https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html) struct for a detailed list of options) |
| `query`* | string |            | A PostgreSQL query which returns a list of rows to be ingested, e.g., "select a, b from table where a = 1 limit 100;" (check the [SELECT](https://www.postgresql.org/docs/current/sql-select.html) documentation in PostgreSQL for the syntax)  |


[*]: Required fields

## Data type mapping

We currently serialize PostgreSQL records to JSON before sending them to Feldera. This means that the ingestion rules are
dictated by the deserialization rules of the [JSON format](formats/json.md).

:::info
The following table lists supported PostgreSQL data types and the corresponding Feldera type where a conversion is guaranteed to work.
More complex types from PostgreSQL might work but are not yet officially supported and tested.
Please [let us know](https://github.com/feldera/feldera/issues) if you need support for a specific type.
:::

| PostgreSQL type | Feldera SQL type | Comment                                                        |
|-----------------|------------------|----------------------------------------------------------------|
| `BOOL`          | `BOOLEAN`        |                                                                |
| `BYTEA`         | `BYTEA`          |                                                                |
| `CHAR`          | `CHAR`           |                                                                |
| `VARCHAR`       | `VARCHAR`        |                                                                |
| `BPCHAR`        | `STRING`         |                                                                |
| `NAME`          | `STRING`         |                                                                |
| `DATE`          | `DATE`           |                                                                |
| `TIME`          | `TIME`           |                                                                |
| `TIMESTAMP`     | `TIMESTAMP`      | Feldera currently does not support timestamps with time zones. |
| `INT2`          | `INT2`           |                                                                |
| `INT4`          | `INT4`           |                                                                |
| `INT8`          | `INT8`           |                                                                |
| `FLOAT4`        | `FLOAT4`         |                                                                |
| `FLOAT8`        | `FLOAT8`         |                                                                |
| `TEXT`          | `TEXT`           |                                                                |
| `UUID`          | `UUID`           |                                                                |
| `JSON`          | `VARIANT`        |                                                                |
| `NUMERIC(P,S)`  | `DECIMAL(P,S)`   | The largest supported precision `P` is 28 and scale `S` is 10. |
| `T[]`           | `T ARRAY`        |                                                                |

## A simple example

We first connect to a PostgreSQL database running on `localhost:7373` with the username `postgres` and password `1234`.

```bash
psql postgresql://postgres:1234@localhost:7373/postgres
```

Next we create a table `people` with columns `id`, `name`, and `age` by pasting the following SQL in `psql`:

```sql
create table people (
    id varchar(36),
    name varchar(36),
    age bigint
);

insert into people (id, name, age)
values
    ('11111111-1111-1111-1111-111111111111', 'Alice', 30),
    ('22222222-2222-2222-2222-222222222222', 'Bob', 25),
    ('33333333-3333-3333-3333-333333333333', 'Charlie', 40);
```

We can load this table from PostgreSQL into Feldera using the `postgres_input` connector:

```sql
create table people (
    id varchar(36),
    name varchar(36),
    the_age bigint,
    extra bigint not null
) WITH (
    'materialized' = 'true',
    'connectors' = '[{  
    "transport": { 
      "name": "postgres_input", 
      "config": {
        "uri": "postgresql://postgres:1234@localhost:7373/postgres",
        "query": "select id, name, age as the_age, 1 as extra from people;"
      }
    } 
  }]'
);
```

* Column names need to match the source table in feldera, hence the `age` column in the `people` table is renamed to `the_age` in the PostgreSQL query.
* The feldera table contains an `extra` non-nullable column. Failing to provide a value for this column in the query will result in an ingest error. We use `1` as a placeholder value in the PostgreSQL query.

## An example for every type

Here is an example of a table with every supported PostgreSQL type.
You can create this table and insert a record in PostgreSQL by pasting the following SQL in `psql`:

```sql
CREATE TABLE all_types_example (
    my_bool              BOOL,
    my_bool_array        BOOL[],
    my_bytea             BYTEA,
    my_bytea_array       BYTEA[],
    my_char              CHAR,
    my_char_array        CHAR[],
    my_varchar           VARCHAR(50),
    my_varchar_array     VARCHAR(50)[],
    my_name              NAME,
    my_name_array        NAME[],
    my_date              DATE,
    my_date_array        DATE[],
    my_time              TIME,
    my_time_array        TIME[],
    my_timestamp         TIMESTAMP,
    my_timestamp_array   TIMESTAMP[],
    my_int2              INT2,
    my_int2_array        INT2[],
    my_int4              INT4,
    my_int4_array        INT4[],
    my_int8              INT8,
    my_int8_array        INT8[],
    my_float4            FLOAT4,
    my_float4_array      FLOAT4[],
    my_float8            FLOAT8,
    my_float8_array      FLOAT8[],
    my_text              TEXT,
    my_text_array        TEXT[],
    my_uuid              UUID,
    my_uuid_array        UUID[],
    my_json              JSON,
    my_decimal           DECIMAL(28, 2),
    my_decimal_array     DECIMAL(28, 2)[]
);

INSERT INTO all_types_example (
    my_bool,
    my_bool_array,
    my_bytea,
    my_bytea_array,
    my_char,
    my_char_array,
    my_varchar,
    my_varchar_array,
    my_name,
    my_name_array,
    my_date,
    my_date_array,
    my_time,
    my_time_array,
    my_timestamp,
    my_timestamp_array,
    my_int2,
    my_int2_array,
    my_int4,
    my_int4_array,
    my_int8,
    my_int8_array,
    my_float4,
    my_float4_array,
    my_float8,
    my_float8_array,
    my_text,
    my_text_array,
    my_uuid,
    my_uuid_array,
    my_json,
    my_decimal,
    my_decimal_array
)
VALUES (
    -- BOOL, BOOL[]
    TRUE,
    '{TRUE, FALSE, TRUE}',

    -- BYTEA, BYTEA[]
    E'\\xDEADBEEF',
    '{"\\\\xABCD","\\\\x1234"}',

    -- CHAR, CHAR[]
    'A',
    '{"B","C"}',

    -- VARCHAR(50), VARCHAR(50)[]
    'Hello, World!',
    '{"Hello","Array"}',

    -- NAME, NAME[]
    'SomeStringValue',
    '{"StringValue1","StringValue2"}',

    -- DATE, DATE[]
    '2025-01-31',
    '{"2025-01-01","2025-12-31"}',

    -- TIME, TIME[]
    '12:34:56',
    '{"01:02:03","23:59:59"}',

    -- TIMESTAMP, TIMESTAMP[]
    '2025-01-31 12:34:56',
    '{"2025-01-31 00:00:00","2025-12-31 23:59:59"}',

    -- INT2, INT2[]
    12,
    '{1,2,3}',

    -- INT4, INT4[]
    1234,
    '{10,20,30}',

    -- INT8, INT8[]
    1234567890123,
    '{999999999999,888888888888}',

    -- FLOAT4, FLOAT4[]
    3.14,
    '{1.1,2.2,3.3}',

    -- FLOAT8, FLOAT8[]
    2.718281828,
    '{123.456,789.012}',

    -- TEXT, TEXT[]
    'This is some text.',
    '{"Text one","Text two"}',

    -- UUID, UUID[]
    '123e4567-e89b-12d3-a456-426614174000',
    '{"123e4567-e89b-12d3-a456-426614174001","123e4567-e89b-12d3-a456-426614174002"}',

    -- JSON, JSON[]
    '{"key":"value"}',

    -- DECIMAL(28,2), DECIMAL(28,2)[]
    12345.67,
    '{123.45,6789.01}'
);
```

You can load this from PostgreSQL into Feldera using the following table definition and `postgres_input` connector:

```sql
CREATE TABLE all_types_example (
    my_bool              BOOL,
    my_bool_array        BOOL ARRAY,
    my_bytea             BYTEA,
    my_bytea_array       BYTEA ARRAY,
    my_char              CHAR,
    my_char_array        CHAR ARRAY,
    my_varchar           VARCHAR(50),
    my_varchar_array     VARCHAR(50) ARRAY,
    my_name              STRING,
    my_name_array        STRING ARRAY,
    my_date              DATE,
    my_date_array        DATE ARRAY,
    my_time              TIME,
    my_time_array        TIME ARRAY,
    my_timestamp         TIMESTAMP,
    my_timestamp_array   TIMESTAMP ARRAY,
    my_int2              INT2,
    my_int2_array        INT2 ARRAY,
    my_int4              INT4,
    my_int4_array        INT4 ARRAY,
    my_int8              INT8,
    my_int8_array        INT8 ARRAY,
    my_float4            FLOAT4,
    my_float4_array      FLOAT4 ARRAY,
    my_float8            FLOAT8,
    my_float8_array      FLOAT8 ARRAY,
    my_text              TEXT,
    my_text_array        TEXT ARRAY,
    my_uuid              UUID,
    my_uuid_array        UUID ARRAY,
    my_json              VARIANT,
    my_decimal           DECIMAL(28, 2),
    my_decimal_array     DECIMAL(28, 2) ARRAY
) WITH (
  'materialized' = 'true',
  'connectors' = '[{  
    "transport": {
      "name": "postgres_input", 
      "config": {
        "uri": "postgresql://postgres:1234@localhost:7373/postgres",
        "query": "select * from all_types_example;"
      }
    }
  }]'
);
```

## Connecting with TLS/SSL

TBD


