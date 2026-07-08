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

The connector uses a PostgreSQL publication that must be created before the
pipeline starts. The connecting PostgreSQL user must have replication
privileges.

## PostgreSQL CDC input connector configuration

Use transport name `postgres_cdc_input`.

| Property                  | Type    | Default | Description                                                                                                                                                                        |
| ------------------------- | ------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`\*                   | string  |         | PostgreSQL connection URI, e.g. `"postgres://postgres:password@localhost:5432/postgres"`. It must include a username, host, and database name. The user needs `REPLICATION` privilege. |
| `publication`\*           | string  |         | Name of an existing PostgreSQL publication. The publication must include `source_table`.                                                                                           |
| `source_table`\*          | string  |         | PostgreSQL table to replicate, usually schema-qualified, e.g. `"public.orders"`.                                                                                                  |
| `ssl_ca_pem`              | string  |         | CA certificates in PEM format. Setting this enables TLS and takes precedence over `ssl_ca_location`.                                                                               |
| `ssl_ca_location`         | string  |         | Path to a PEM file containing CA certificates. Used when `ssl_ca_pem` is not set.                                                                                                  |
| `streaming_ack_hold_ms`   | integer | `2000`  | Time to wait for a nonterminal CDC batch to become durable before ingestion may continue without advancing its PostgreSQL WAL position. Must be greater than zero.                  |
| `discard_shutdown_errors` | boolean | `true`  | Whether to retry a table when the previous connector run stopped before acknowledging pending data.                                                                                |
| `discard_table_errors`    | boolean | `false` | Whether to retry tables with persisted replication errors at startup. See [Discarding table errors](#discarding-table-errors).                                                      |

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

The replication user needs permission to connect to the database and use
logical replication. For example:

```sql
CREATE ROLE feldera WITH LOGIN PASSWORD 'password' REPLICATION;
GRANT CONNECT ON DATABASE postgres TO feldera;
GRANT USAGE ON SCHEMA public TO feldera;
GRANT SELECT ON TABLE public.orders TO feldera;
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

CREATE PUBLICATION feldera_orders FOR TABLE public.orders;
```

`REPLICA IDENTITY FULL` is recommended so update and delete events include the
old row values needed to retract records from the Feldera input table.

## Schema requirements

Feldera matches columns by name.

- Every non-nullable Feldera column must exist in the PostgreSQL source table.
- Nullable Feldera columns may be absent from the PostgreSQL source table.
- Extra PostgreSQL columns that do not exist in Feldera are ignored.
- If a required Feldera column is removed from PostgreSQL while the connector is
  running, the connector reports a fatal error.

We recommend defining a `PRIMARY KEY` on the Feldera input relation because the
connector provides at-least-once delivery. If the initial copy is interrupted,
the connector may read the source snapshot again from the beginning. This does
not create another copy inside PostgreSQL; it sends the same source rows to
Feldera again. If a Feldera checkpoint already contains rows from the earlier
attempt, a table with a primary key retains one row per key. Without a primary
key, the repeated rows can appear as duplicates.

## Discarding table errors

While the connector is stopping, its Feldera destination may close before it
sends an acknowledgment for pending data. On the next startup,
`discard_shutdown_errors` defaults to `true`, so the connector retries the
table from its last durable PostgreSQL position. Set the option to `false` to
disable this recovery.

Other replication errors remain persisted by default. Set
`discard_table_errors` to `true` to discard persisted errors and retry the
affected tables at startup. This recovery may repeat the initial copy, so
define a primary key on the input relation (see [Schema requirements](#schema-requirements))
to prevent duplicate rows.

## Example

First, create a PostgreSQL table and publication:

```sql
CREATE TABLE public.orders (
    id BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    amount DECIMAL(10, 2),
    status TEXT NOT NULL
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

CREATE PUBLICATION feldera_orders FOR TABLE public.orders;

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

The connector derives a stable identity from the source database host, port,
and database name together with `publication` and `source_table`. On an
ordinary restart with the same identity, it resumes from the last durable
position. If the connector stops before the initial copy is complete, it may
repeat that copy.

Changing any of those identity fields starts a new initial copy. The PostgreSQL
username and password are not part of the identity, so rotating credentials
does not start a new copy.

Do not manually reset the connector's replication state. Feldera does not
provide a supported partial-reset procedure, and resetting state can replay
rows already present in the Feldera table.
