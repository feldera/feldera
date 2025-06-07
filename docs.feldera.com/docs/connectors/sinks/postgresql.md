# PostgreSQL output connector

:::caution Experimental feature
PostgreSQL support is an experimental feature of Feldera.
:::

Feldera allows you to output data from a SQL view to a PostgreSQL database.
These outputs are made as a series of transactions.

:::important
Only SQL views with [Uniqueness Constraints](/connectors/unique_keys) can output
data to a PostgreSQL table.

It is recommended that these unique keys in Feldera be defined as PRIMARY KEYs
in the PostgreSQL table.
:::

## PostgreSQL output configuration

| Property | Type   | Default | Description                                                                                                                                                                                                                                     |
|----------|--------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `uri`*   | string |         | A PostgreSQL connection URL, e.g., "postgresql://postgres:1234@127.0.0.1:7373/postgres" (see the tokio-postgres [Config](https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html) struct for a detailed list of options) |
| `table`* | string |         | The PostgreSQL table to write the outputs to. The schema of this table should be compatible with the schema of the output view.                                                                                                                 |

[*]: Required fields

The schema of the PostgreSQL table should match the schema of the Feldera view, as outlined in the table below, with the following exceptions::
- Narrower Feldera types such as `INT2` and `FLOAT4` can be stored in wider PostgreSQL column types like `INT8` and `FLOAT8` respectively.
- Columns in the PostgreSQL table that are **nullable** or have **default** values may be omitted from the Feldera view.

### Connecting with TLS / SSL

Currently, we do not support connecting to PostgreSQL with TLS / SSL.
Please [let us know](https://github.com/feldera/feldera/issues) if you need TLS / SSL support.

## Data type mapping

:::info
The following table lists supported PostgreSQL data types.
Please [let us know](https://github.com/feldera/feldera/issues) if you need support for a specific type.
:::

| Feldera Type      | PostgreSQL Type                      | Comments                                               |
|-------------------|--------------------------------------|--------------------------------------------------------|
| BOOL              | BOOL                                 |                                                        |
| TINYINT           | SMALLINT, INT, BIGINT                | No direct equivalent but can be stored in wider types. |
| SMALLINT          | SMALLINT                             |                                                        |
| INT               | INT                                  |                                                        |
| BIGINT            | BIGINT                               |                                                        |
| DECIMAL           | DECIMAL                              |                                                        |
| REAL              | FLOAT                                |                                                        |
| DOUBLE            | DOUBLE PRECISION                     |                                                        |
| VARCHAR           | VARCHAR                              |                                                        |
| TIME              | TIME                                 |                                                        |
| DATE              | DATE                                 |                                                        |
| TIMESTAMP         | TIMESTAMP                            | Feldera TIMESTAMPs do not have timezone information.   |
| VARIANT           | JSON, JSONB                          |                                                        |
| UUID              | UUID                                 |                                                        |
| VARBINARY         | BYTEA                                |                                                        |
| ARRAY             | ARRAY                                |                                                        |
| User Defined Type | Equivalent PostgreSQL Composite Type |                                                        |
| MAP               | JSON, JSONB                          | No direct equivalent type, but can be stored as JSON / JSONB.             |

## Example

Let's first connect to a PostgreSQL database running in `localhost:5432` with
the username `postgres` and password `password`.

```sh
psql postgres://postgres:password@localhost:5432/postgres
```

Now, let's create a table `feldera_out` with columns `id` and `s` that Feldera
will write to.


```sql
-- PostgreSQL
CREATE TABLE feldera_out (id INT PRIMARY KEY, s VARCHAR);
```

We can output data from Feldera to this table using the `postgres_output`
connector.

```sql
-- Feldera SQL
-- Create a table and fill it with 5 randomly generated records.
create table t0 (id int, s varchar) with (
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
          "rate": 1,
          "limit": 5
        }]
      }
    }
  }]'
);

-- Create a view that will contain a copy of all records in table `t0` and
-- attach a Postgres output connector to it.
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

-- Index `v1` using `id` column as a key. The Postgres connector requires this
-- index to group updates by key.
create index v1_idx on v1(id);
```

:::important
Column names in Feldera SQL view and PostgreSQL table need to match.
:::


## Example demonstrating all supported types

We create a Feldera pipeline including every supported PostgreSQL type:

```sql
-- Feldera SQL
CREATE TABLE all_types_example (
    my_bool              BOOL,
    my_bool_array        BOOL ARRAY,
    my_bytea             BYTEA,
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
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": {
        "plan": [{
          "rate": 1,
          "limit": 5
        }]
      }
    }
  }]'
);

create materialized view v1
with (
    'connectors' = '[{
        "index": "v1_idx",
        "transport": {
            "name": "postgres_output",
            "config": {
                "uri": "postgres://postgres:password@localhost:5432/postgres",
                "table": "all_types_example"
            }
        }
    }]'
)
as select * from all_types_example;
create index v1_idx on v1(my_int2);
```

Now we create the equivalent table in PostgreSQL.

```sql
-- PostgreSQL
CREATE TABLE all_types_example (
    my_bool              BOOL,
    my_bool_array        BOOL[],
    my_bytea             BYTEA,
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
```

After the pipeline completes, we can inspect the PostgreSQL table to ensure
that Feldera has written to it:


```sql
-- PostgreSQL
SELECT count(*) FROM all_types_example;
 count
-------
     5
(1 row)
```

### Outputting multi-dimensional arrays

When working with multi-dimensional arrays, like `BYTEA ARRAY`, PostgreSQL
expects the sub arrays to be of the same length. If not, the following error is
raised:

```
Multidimensional arrays must have sub-arrays with matching dimensions.
```

:::danger
If the sub-arrays are not of the same dimension, the transaction will fail and
the data will be lost.
:::
