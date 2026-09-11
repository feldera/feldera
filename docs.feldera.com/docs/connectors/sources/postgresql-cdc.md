# PostgreSQL CDC input connector

:::caution Experimental feature
PostgreSQL CDC support is an experimental feature of Feldera.
:::

:::note
This page describes configuration options specific to the PostgreSQL CDC
connector. See [top-level connector documentation](/connectors/) for general
information about configuring input and output connectors.
:::

The PostgreSQL CDC input connector reads changes from a PostgreSQL table using
logical replication and streams them into a Feldera table. Unlike the
[PostgreSQL input connector](/connectors/sources/postgresql), which runs a
query once, this connector first snapshots the source table and then continues
to ingest inserts, updates, and deletes from PostgreSQL's write-ahead log.

The connector uses a PostgreSQL publication that must exist before the
pipeline starts. The connecting PostgreSQL user must be a superuser (see
[Objects the connector installs](#objects-the-connector-installs)).

## PostgreSQL CDC input connector configuration

Use transport name `postgres_cdc_input`.

| Property          | Type   | Default | Description                                                                                                                                                                                     |
| ----------------- | ------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`\*           | string |         | PostgreSQL connection URL, e.g. `"postgres://postgres:password@localhost:5432/postgres"`. The URL must include a username, host, and database name. The user must be a superuser (see [PostgreSQL setup](#postgresql-setup)). |
| `publication`\*   | string |         | Name of an existing PostgreSQL publication. The publication must include `source_table`.                                                                                                        |
| `source_table`\*  | string |         | PostgreSQL table to replicate, schema-qualified, e.g. `"public.orders"`. A name given without a schema refers to a table in `public`.                                                                                                                |
| `ssl_ca_pem`      | string |         | CA certificates in PEM format. Setting this enables TLS and takes precedence over `ssl_ca_location`.                                                                                            |
| `ssl_ca_location` | string |         | Path to a PEM file containing CA certificates. Used when `ssl_ca_pem` is not set.                                                                                                               |

[*]: Required fields

The CDC connector does not support client-certificate TLS options
(`ssl_client_pem`, `ssl_client_location`, `ssl_client_key`,
`ssl_client_key_location`, or `ssl_certificate_chain_location`).

## PostgreSQL setup

The PostgreSQL server must have logical replication enabled:

```sql
SHOW wal_level;
```

The value must be `logical`. If it is not, configure PostgreSQL with
`wal_level = logical` and restart the server.

The connecting user must be a superuser (see
[Objects the connector installs](#objects-the-connector-installs)). A superuser
may use logical replication and read every table without further attributes or
grants, so the role needs nothing else. For example:

```sql
CREATE ROLE feldera WITH LOGIN PASSWORD 'password' SUPERUSER;
```

Create the source table and publication before starting the Feldera pipeline:

```sql
CREATE TABLE public.orders (
    id BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    amount DECIMAL(10, 2),
    status TEXT NOT NULL
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

CREATE publication feldera_orders FOR TABLE public.orders;
```

`REPLICA IDENTITY FULL` is recommended so update and delete events include the
old row values needed to retract records from the Feldera input table.

### Objects the connector installs

The connector embeds the [etl](https://github.com/supabase/etl) replication
library, which keeps its state in the source database. Every time the pipeline
starts, the connector connects as the configured user and applies the etl
migrations that the database does not yet hold. etl divides its migrations into
two sets: the state-store migrations create the `etl` schema and the state
tables, and the source migrations create the functions and the event trigger.
Of the objects the migrations create, these matter to an operator:

| Object                              | Name                                                                                                                     | Purpose                                                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| Schema                              | `etl`                                                                                                                    | Holds the state tables and functions listed in this table.                                                       |
| State tables                        | `etl.replication_state`, `etl.replication_progress`, `etl.table_schemas`, `etl.table_columns`, `etl.destination_tables_metadata` | Record each table's replication phase and column schema, and how far the connector has acknowledged the replication stream. |
| Migration log                       | `etl._sqlx_migrations`                                                                                                   | Records which migrations have run, so a later start applies only new ones.                                        |
| Functions                           | `etl.describe_table_schema`, `etl.describe_table_identity`, `etl.emit_schema_change_messages`                            | Describe a published table's columns and replica identity from the catalog and emit the description as a logical replication message. |
| Event trigger                       | `supabase_etl_ddl_message_trigger`                                                                                       | Fires on `ddl_command_end` for `ALTER TABLE` and `ALTER PUBLICATION` and calls `etl.emit_schema_change_messages`. |

The migrations also create enum types and indexes that only these tables use.
The etl repository holds the complete DDL under `crates/etl/migrations/source`
and `crates/etl/migrations/postgres_store`.

The event trigger lets the connector learn about schema changes to a published
table in order with the row changes around them. It fires for every
`ALTER TABLE` and `ALTER PUBLICATION` in the database but emits a message only
for tables that some publication contains.

PostgreSQL allows only a superuser to create an event trigger, so the
connecting user must be a superuser. A start that cannot apply a migration
fails, and the connector offers no option to skip the migrations. A read-only
standby cannot serve as the source: etl skips the source migrations when
`pg_is_in_recovery()` reports a standby, but it runs the state-store migrations
on every start, and the standby rejects their writes, so the start fails. The
objects stay in the database after the pipeline stops or the user deletes it,
and pipelines that read from the same database share them.

## Schema requirements

Feldera matches columns by name.

- Every non-nullable Feldera column must exist in the PostgreSQL source table.
- Nullable Feldera columns may be absent from the PostgreSQL source table.
- Extra PostgreSQL columns that do not exist in Feldera are ignored.
- If a required Feldera column is removed from PostgreSQL while the connector is
  running, the connector reports a fatal error.

## Example

First, create a PostgreSQL table and publication:

```sql
CREATE TABLE public.orders (
    id BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    amount DECIMAL(10, 2),
    status TEXT NOT NULL
);

ALTER TABLE public.orders replica identity FULL;

CREATE publication feldera_orders FOR TABLE public.orders;

INSERT INTO public.orders VALUES
    (1, 'Alice', 25.00, 'new'),
    (2, 'Bob', 40.50, 'new');
```

Then create a Feldera table that reads from the PostgreSQL publication:

```sql
CREATE TABLE orders (
    id BIGINT NOT NULL,
    customer TEXT NOT NULL,
    amount DECIMAL(10, 2),
    status TEXT NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "postgres_cdc_input",
            "config": {
                "uri": "postgres://feldera:password@localhost:5432/postgres",
                "publication": "feldera_orders",
                "source_table": "public.orders"
            }
        }
    }]'
);
```

When the pipeline starts, Feldera ingests the existing rows in `public.orders`.
Subsequent PostgreSQL changes are streamed into the Feldera table:

```sql
INSERT INTO public.orders VALUES (3, 'Carol', 19.99, 'new');
UPDATE public.orders SET status = 'shipped' WHERE id = 1;
DELETE FROM public.orders WHERE id = 2;
```

## TLS example

To connect over TLS, provide the trusted root certificate:

```sql
CREATE TABLE orders (
    id BIGINT NOT NULL,
    customer TEXT NOT NULL,
    amount DECIMAL(10, 2),
    status TEXT NOT NULL
) WITH (
    'materialized' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "postgres_cdc_input",
            "config": {
                "uri": "postgres://feldera:password@db.example.com:5432/postgres",
                "publication": "feldera_orders",
                "source_table": "public.orders",
                "ssl_ca_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"
            }
        }
    }]'
);
```

## Resume behavior

The connector stores replication state in PostgreSQL and uses logical
replication slots managed by the connector. Restarting a pipeline with the same
database host, port, database, publication, and source table resumes from the
existing replication state. Changing any of those values creates a different
replication identity and can cause a new snapshot.

Rotating the PostgreSQL username or password does not change the replication
identity.

With [fault tolerance](/pipelines/fault-tolerance) enabled, the connector
delivers every change at least once: the replication slot advances only past
changes that a Feldera checkpoint contains.

The initial read of a table is all or nothing. A checkpoint holding only part
of it would be unusable, because PostgreSQL streams changes from the
replication slot once the read is complete and cannot supply the missing rows
later. While a read is in progress the connector therefore blocks checkpoints
and suspend requests, and the pipeline reports the connector as the reason.
Reading a large table can hold checkpoints back for as long as the read takes,
and pausing the pipeline during the initial read extends that block until the
pipeline runs again. A suspend, or a stop that takes a checkpoint first, waits
the same way, so stopping a pipeline during its first read of a large table may
need a forced stop; the next start reads the table again, so nothing is lost.
When the publication holds several tables, the changes the connector has
already read stay unacknowledged for the same period, and PostgreSQL retains
the write-ahead log that covers them.

A pipeline that stops or crashes before its initial read of a table finishes
reads that table again on the next start, with or without fault tolerance. The
connector's replication state records the read as complete only after the last
row of the read has reached the circuit, so an interrupted read leaves the table
marked incomplete and the next start reads it again from a fresh snapshot. The
connector then delivers the rows it read before the stop a second time; a
primary key on the Feldera table makes the repeated rows replace the originals
instead of appearing twice.

Once the initial read has finished, what a restart does with the rows of that
read depends on how the pipeline starts:

| Start                                                                       | Reads the table again | Rows from the initial read                                                                                                      |
| --------------------------------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Resume from a checkpoint or from a suspend, with or without fault tolerance | No                    | Kept: the connector blocks checkpoints and suspend requests until the read is complete, so the checkpoint holds every row.      |
| Start with no checkpoint to resume from, fault tolerance enabled            | Yes                   | Read again from a fresh snapshot.                                                                                               |
| Start with no checkpoint to resume from, fault tolerance disabled           | No                    | Absent from Feldera; only changes streamed from the slot arrive.                                                                |

The first row has an exception: the pipeline discards the connector's
checkpointed state when a program change requires a backfill of the Feldera
table the connector feeds, or when the connector's configuration changed. The
connector then starts as in the two rows for a start with no checkpoint to
resume from.

With [fault tolerance](/pipelines/fault-tolerance) enabled, a start that has
no checkpoint to resume from, for example after a forced stop or a crash
before the pipeline has taken a checkpoint, begins with an empty circuit. The
connector detects that no checkpoint holds the initial read, logs a warning to
that effect, and reads the table again before resuming from the replication
slot, so the Feldera table is complete. The pipeline delivers rows that reached
output connectors before the stop a second time.

Without fault tolerance, a start that has no checkpoint to resume from resumes
from the replication slot alone: the connector does not read the table again,
so the rows the table held when the connector first read it are absent from
Feldera, although the changes PostgreSQL streams from the slot still arrive.
Whatever the fault tolerance setting, a pipeline with storage configured resumes
from the latest checkpoint in its storage, so a stop that takes a checkpoint, a
suspend, and a crash or a forced stop after a checkpoint all keep those rows. A
pipeline without storage starts fresh every time, and only Feldera Enterprise
Edition takes checkpoints (see
[fault tolerance](/pipelines/fault-tolerance)). Without fault
tolerance the pipeline takes a checkpoint only when the user requests one or
suspends the pipeline, so the rows survive a crash or a forced stop only if the
pipeline took a checkpoint after the initial read finished. With fault tolerance
the pipeline also takes checkpoints on its own, by default every 60 seconds; the
`checkpoint_interval_secs` setting described on the
[fault tolerance](/pipelines/fault-tolerance) changes the interval, and
`null` disables automatic checkpoints.
