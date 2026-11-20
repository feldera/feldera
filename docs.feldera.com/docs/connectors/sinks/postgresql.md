# PostgreSQL output connector

Feldera allows you to output data from a SQL view to a PostgreSQL database.
These outputs are made as a series of transactions.

:::important
Only SQL views with [Uniqueness Constraints](/connectors/unique_keys) can output
data to a PostgreSQL table.

It is recommended that these unique keys in Feldera be defined as PRIMARY KEYs
in the PostgreSQL table.
:::

## PostgreSQL output configuration

| Property                         | Type    | Default        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------- | ------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`\*                          | string  |                | A PostgreSQL connection URL, e.g., `"postgresql://postgres:1234@127.0.0.1:7373/postgres"` (see the tokio-postgres [Config](https://docs.rs/tokio-postgres/0.7.12/tokio_postgres/config/struct.Config.html) struct for a detailed list of options).                                                                                                                                                                                                                                                                                                                          |
| `table`\*                        | string  |                | The PostgreSQL table to write the outputs to. The schema of this table should be compatible with the schema of the output view.                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `mode`                           | string  | `materialized` | [Mode](#write-modes) to run the connector in (`materialized` or `"cdc"`).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `cdc_op_column`                  | string  | `__feldera_op` | Name of the operation metadata column in CDC mode. This column in the output row will contain `"i"` (insert), `"u"` (upsert), or `"d"` (delete). Only used when `mode = "cdc"`. This column must exist in the PostgreSQL target table and store character values.                                                                                                                                                                                                                                                                                                           |
| `cdc_ts_column`                  | string  | `__feldera_ts` | Name of the timestamp metadata column in CDC mode. Contains timestamp when the record was output. Only used when `mode = "cdc"`. This column must exist in the PostgreSQL target table, and store **timestamp in microseconds**.                                                                                                                                                                                                                                                                                                                                            |
| `ssl_ca_pem`                     | string  |                | A sequence of CA certificates in PEM format. Required for TLS, and takes precedence over `ssl_ca_location`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `ssl_ca_location`                | string  |                | Path to a file containing a sequence of CA certificates in PEM format. Required for TLS, takes lower precedence than `ssl_ca_pem`.                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `ssl_client_pem`                 | string  |                | The client certificate in PEM format. Takes precedence over `ssl_client_location`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `ssl_client_location`            | string  |                | The path to the client certificate file in PEM format. Takes lower precedence than `ssl_client_pem`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `ssl_client_key`                 | string  |                | The client certificate key in PEM format. Takes precedence over `ssl_client_key_location`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `ssl_client_key_location`        | string  |                | The path to the client certificate key file in PEM format. Takes lower precedence than `ssl_client_key`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `ssl_certificate_chain_location` | string  |                | The path to the certificate chain file. The file must contain a sequence of PEM-formatted certificates, the first being the leaf certificate, and the remainder forming the chain of certificates up to and including the trusted root certificate.                                                                                                                                                                                                                                                                                                                         |
| `verify_hostname`                | boolean | `true`         | True to enable hostname verification when using TLS. True by default.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `max_records_in_buffer`          | integer |                | The maximum number of records in a single buffer. If not specified, there is no explicit limit on the number of records, but the buffer may still be constrained by `max_buffer_size`.                                                                                                                                                                                                                                                                                                                                                                                |
| `max_buffer_size`                | string  | `"1Mi"`        | The maximum buffer size for a single operation, such as `"1Mi"`. `max_buffer_size_bytes`, a number of bytes, is a deprecated alias. Buffers for `INSERT`, `UPDATE`, and `DELETE` queries are maintained separately. Default is 1 MiB (`1048576` bytes).                                                                                                                                                                                                                                                                                                                                                                                              |
| `on_conflict_do_nothing`         | bool    | `false`        | Specifies how the connector handles conflicts when executing an `INSERT` into a table with a primary key. By default, an existing row with the same key is overwritten. Setting this flag to `true` preserves the existing row and ignores the new insert. <p> This setting does not affect `UPDATE` statements, which always replace the value associated with the key.</p><p>**This setting has no effect when `mode = "cdc"`, since all operations are performed as append-only `INSERT`s into the target table. Any conflict in CDC mode will result in an error.**</p> |
| `threads`                        | integer | `1`            | Number of parallel worker threads used to write to PostgreSQL. Each worker opens its own connection and handles a disjoint partition of the data. Increasing this value can improve throughput for large batches. Must be at least 1.                                                                                                                                                                                                                                                                                                                                       |
| `extra_columns`                  | array of strings |        | The list of PostgreSQL columns that the Feldera view does not write. The connector writes the value last set by the `set_extra_columns` command into each of them; see [Extra columns](#extra-columns).                                                                                                                                                                                                                                                                                                                                                                                                                                                |

[*]: Required fields

The schema of the PostgreSQL table should match the schema of the Feldera view, as outlined in the table below, with the following exceptions:

- Narrower Feldera types such as `INT2` and `FLOAT4` can be stored in wider PostgreSQL column types like `INT8` and `FLOAT8` respectively.
- Columns in the PostgreSQL table that are **nullable** or have **default** values may be omitted from the Feldera view.

### Extra columns

A PostgreSQL table may have more columns than the Feldera view that writes to
it, e.g., a batch label. The value of `extra_columns` is the list of these
columns. The `set_extra_columns` connector command sets the values written to
them while the pipeline runs:

```bash
curl -X POST \
  "http://127.0.0.1:8080/v0/pipelines/{pipeline_name}/views/{view_name}/connectors/{connector_name}/command" \
  -H "Content-Type: application/json" \
  -d '{"set_extra_columns": {"batch_label": "2026-01-17-morning", "loaded_by": "nightly-job"}}'
```

The command names a subset of `extra_columns`; a name outside that list is
rejected, and the error lists the permitted names. Every row the connector
writes from then on carries the values in effect at the time, until a later
command changes them. A `null` value writes SQL NULL. The response echoes the
values of every extra column set so far.

Values set this way are not part of the pipeline's checkpointed state, so a
pipeline that restarts writes NULL into these columns until the command is
issued again. Each extra column must therefore be nullable; a column default
does not help, because the connector always names the column in its `INSERT`
and so writes an explicit NULL rather than letting PostgreSQL apply the default.

### Write modes

The PostgreSQL connector supports two write modes:

#### Materialized mode (default)

In materialized mode (`"mode": "materialized"`), the connector performs direct `INSERT`, `UPDATE`, and `DELETE` operations on the target table. This mode:

- Maintains the target table as a materialized snapshot of the view
- Updates existing records in-place and removes deleted records

#### CDC mode

In CDC mode (`"mode": "cdc"`), the connector writes all operations as `INSERT` operations into an append-only event log. This mode:

- Preserves a complete history of all changes
- Adds metadata columns to track operation type and timestamp

When using CDC mode, you must ensure your PostgreSQL table includes the metadata columns:

- Operation column (default: `__feldera_op`): Contains `"i"` (insert), `"u"` (upsert), or `"d"` (delete)
- Timestamp column (default: `__feldera_ts`): Contains timestamp when the record was output; in microseconds since January 1, 1970 UTC

You can customize the names of these columns using the `cdc_op_column` and `cdc_ts_column` configuration options.

### Connecting with TLS / SSL

Feldera supports connecting to PostgreSQL over TLS / SSL.
To enable a secure connection, you **must** provide the following field:

- `ssl_ca_pem`: The CA certificate in PEM format, used to verify the server certificate.

You can optionally provide the following fields:

- `ssl_client_pem`: The client certificate in PEM format (if the server requires client authentication).
- `ssl_client_key`: The private key corresponding to the client certificate.
- `verify_hostname`: Set to `false` to disable hostname verification (not recommended). Defaults to `true`.

If `ssl_ca_pem` is not specified, the connection will default to **plaintext**.

Example:

```sql
CREATE MATERIALIZED VIEW v1 WITH (
    'connectors' = '[{
        "index": "v1_idx",
        "transport": {
            "name": "postgres_output",
            "config": {
                "uri": "postgres://postgres:secret@db.badhostname.feldera.com:5432/postgres",
                "table": "feldera_out",
                "ssl_client_key": "[REDACTED]",
                "ssl_client_pem": "[REDACTED]",
                "ssl_ca_pem": "[REDACTED]",
                "verify_hostname": false
            }
        }
    }]'
) AS SELECT * FROM t0;
CREATE INDEX v1_idx ON v1(id);
```

### Connection loss and connection poolers

Each worker thread writes its part of a batch of changes in one transaction.
If the thread loses the connection to PostgreSQL before the commit, PostgreSQL
rolls the transaction back. The thread then connects again and writes its part
of the batch again from the start. The thread does the same when a statement
fails for a cause that is usually on the server side and not in the statement
or its rows.

| Event | What the connector does |
|---|---|
| The connection drops, or the server restarts or is not reachable. | Connects again. The wait between attempts grows to a maximum of 60 seconds. Then writes the batch again. |
| A connection pooler, such as PgBouncer, reports an error and ends the session. An example is a pooler that cannot log in to the server. | The same. |
| The server cannot write through this session. An example is a session that a pooler holds on the old primary after a failover (SQLSTATE `25006`). | Opens a new connection and writes the batch again. From the third attempt, waits before each attempt: 1 second, then 1 second more each time. |
| The server cancels a statement, cannot get a lock in time, has a deadlock or a serialization failure, has no disk space or memory, or reports a system error or an internal error (SQLSTATE `55P03` and classes `40`, `53`, `57`, `58`, `XX`). | The same. This also applies when the server answers and does not prepare the statements of the connector after a new connection. |
| The same batch fails in 10 attempts, the first one included, for one of the causes above. | Reports an error and drops the batch. |
| The server rejects the credentials, or the database does not exist. | Reports an error and drops the batch. |
| The server rejects a statement, for example because of a constraint. | Reports an error and drops the batch. A new connection cannot correct this failure. |

The wait for a connection has no limit: the thread waits for as long as the
server or the pooler is not available. The limit of 10 is for the attempts to
write a batch through a connection that opens.

This section applies to a pipeline that runs. A pipeline that starts while the
server or the pooler is not available fails at start.

Some of the server-side errors come back each time. Examples are a statement
that is always slower than the statement timeout of the server, and a role
that is configured as read-only. The connector then uses its 10 attempts,
which can take 10 times the statement timeout, and drops the batch.

With more than one thread, a failure drops the part of the batch that the
failed thread had. The other threads commit their parts.

The connector reports each error that drops a batch in its
[status](/api/get-output-status). The pipeline log also has the errors that
the connector recovered from.

A [completion token](/connectors/completion-tokens) is `complete` also when
the connector dropped a batch. Check the status of the connector to find a
dropped batch.

The threads of a connector wait for each other at the start and at the end
of a batch, each with its transaction open. This has two consequences:

- Behind a connection pooler in transaction mode, each open transaction
  holds a server connection of the pool. If the pool has fewer server
  connections than the threads that use it, the threads that cannot get one
  wait until the pooler stops them (in PgBouncer, after `query_wait_timeout`,
  120 seconds by default), and then write their part again. This occurs on
  each batch. Make sure that the pool size is at least the sum of `threads` of
  all the connectors that use the pool.
- If the table has a constraint other than the key of the view, such as a
  second unique index, a foreign key, or a trigger, one thread can wait for a
  row lock that another thread holds, while that thread waits for the end of
  the batch. PostgreSQL does not detect this as a deadlock. Set
  `lock_timeout` or `statement_timeout` for the role of the connector to stop
  such a wait. Then the connector writes the part of the batch again.

In CDC mode, when a commit fails because the connection drops, the connector
cannot know whether PostgreSQL applied the commit. It writes the batch again,
so the table can then contain the rows of that batch more than one time. The
same is possible when PostgreSQL reports a temporary error for a commit that
it applied in part, for example for a table with partitions on foreign
servers.

## Data type mapping

:::info
The following table lists supported PostgreSQL data types.
Please [let us know](https://github.com/feldera/feldera/issues) if you need support for a specific type.
:::

| Feldera Type      | PostgreSQL Type                      | Comments                                                      |
| ----------------- | ------------------------------------ | ------------------------------------------------------------- |
| BOOL              | BOOL                                 |                                                               |
| TINYINT           | SMALLINT, INT, BIGINT                | No direct equivalent but can be stored in wider types.        |
| SMALLINT          | SMALLINT                             |                                                               |
| INT               | INT                                  |                                                               |
| BIGINT            | BIGINT                               |                                                               |
| DECIMAL           | DECIMAL                              |                                                               |
| REAL              | FLOAT                                |                                                               |
| DOUBLE            | DOUBLE PRECISION                     |                                                               |
| VARCHAR           | VARCHAR                              |                                                               |
| TIME              | TIME                                 |                                                               |
| DATE              | DATE                                 |                                                               |
| TIMESTAMP         | TIMESTAMP                            | Feldera TIMESTAMPs do not have timezone information.          |
| VARIANT           | JSON, JSONB                          |                                                               |
| UUID              | UUID                                 |                                                               |
| VARBINARY         | BYTEA                                |                                                               |
| ARRAY             | ARRAY                                |                                                               |
| User Defined Type | Equivalent PostgreSQL Composite Type |                                                               |
| MAP               | JSON, JSONB                          | No direct equivalent type, but can be stored as JSON / JSONB. |

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
CREATE TABLE t0 (id INT, s VARCHAR) WITH (
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
CREATE MATERIALIZED VIEW v1 WITH (
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
) AS SELECT * FROM t0;

-- Index `v1` using `id` column as a key. The Postgres connector requires this
-- index to group updates by key.
CREATE INDEX v1_idx ON v1(id);
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

CREATE MATERIALIZED VIEW v1
WITH (
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
AS SELECT * FROM all_types_example;
CREATE INDEX v1_idx ON v1(my_int2);
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
    my_name              name,
    my_name_array        name[],
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
SELECT COUNT(*) FROM all_types_example;
 COUNT
-------
     5
(1 ROW)
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

