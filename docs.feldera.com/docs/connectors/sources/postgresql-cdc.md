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

| Property          | Type   | Default | Description                                                                                                                                                                                     |
| ----------------- | ------ | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`\*           | string |         | PostgreSQL connection URL, e.g. `"postgres://postgres:password@localhost:5432/postgres"`. The URL must include a username, host, and database name. The user must have `REPLICATION` privilege. |
| `publication`\*   | string |         | Name of an existing PostgreSQL publication. The publication must include `source_table`.                                                                                                        |
| `source_table`\*  | string |         | PostgreSQL table to replicate, usually schema-qualified, e.g. `"public.orders"`.                                                                                                                |
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

The connector stores replication state in PostgreSQL and uses logical
replication slots managed by the connector. Restarting a pipeline with the same
database host, port, database, publication, and source table resumes from the
existing replication state. Changing any of those values creates a different
replication identity and can cause a new snapshot.

Rotating the PostgreSQL username or password does not change the replication
identity.
