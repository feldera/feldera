# dbt-feldera

The [dbt](https://www.getdbt.com/) adapter for
[Feldera](https://www.feldera.com/).

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to
transform their data using the same practices that software engineers use to
build applications.

**[Feldera](https://www.feldera.com/)** is a streaming SQL engine powered by
the DBSP incremental computation engine. It automatically incrementalizes
_every_ SQL query without watermarks, scans, or `MERGE`. When input data
changes, only affected output rows are recomputed.

> [!IMPORTANT]
> **This adapter deploys
> [continuous pipelines](https://docs.feldera.com/pipelines), not
> [ad-hoc queries](https://docs.feldera.com/sql/ad-hoc).**
>
> Feldera supports two modes of query execution:
>
> - **Continuous pipelines** compile SQL into an incremental dataflow that runs
>   indefinitely, processing every input change as it arrives in near real-time.
> - **Ad-hoc queries** are one-shot batch queries executed by
>   [DataFusion](https://datafusion.apache.org/) against the state of
>   [materialized tables and views](https://docs.feldera.com/sql/materialized).
>   They exist primarily for development and debugging.
>
> When you run `dbt run`, this adapter assembles your models into a Feldera
> pipeline program, compiles it, and **starts a continuously running pipeline**.
> The pipeline keeps processing input changes and updating outputs until it is
> explicitly stopped. This differs from typical batch-oriented dbt adapters where `dbt run` executes
> a query once, processes a batch of data and exits.

## Key features

- **Automatic incremental view maintenance (IVM)** — Feldera's DBSP engine
  incrementalizes any SQL query out of the box. No manual merge logic or
  watermark tuning required.
- **Continuous pipeline deployment** — `dbt run` compiles and starts a
  long-running Feldera pipeline; it does not execute one-shot queries.
- **Connector integration** — attach Kafka, Delta Lake, S3, and HTTP
  connectors directly to models via configuration.
- **Easy setup** — pure Python adapter with no ODBC driver needed.

## Installation

```bash
pip install dbt-feldera
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add dbt-feldera
```

Requires Python 3.10+ and dbt-core ~1.9.

## Configuration

Add a Feldera target to your `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: feldera
      host: "http://localhost:8080"
      api_key: "apikey:..."          # optional — for authenticated instances
      database: "default"
      schema: "my_pipeline"          # maps to the Feldera pipeline name
      compilation_profile: dev       # dev | unoptimized | optimized
      workers: 4
      timeout: 300
```

### Concept mapping

Feldera uses different terminology than traditional databases. Here's how dbt
concepts map to Feldera. Every materialization contributes SQL to a
**continuously running pipeline** — nothing is executed as a one-shot batch
query.

| dbt concept                   | Feldera concept   | Description                                                                                                                       |
| ----------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `database`                    | _(unused)_        | Set to any string (e.g. `"default"`)                                                                                              |
| `schema`                      | Pipeline name     | Each dbt schema maps to one [Feldera pipeline](https://docs.feldera.com/pipelines) (a continuously running SQL program)           |
| `table` materialization       | Input table       | External data source (Kafka, HTTP, S3)                                                                                            |
| `view` materialization        | View              | SQL view inside the continuous pipeline (all views are incrementally maintained)                                                  |
| `view` + `stored: true`       | Materialized view | Queryable via [ad-hoc queries](https://docs.feldera.com/sql/ad-hoc)                                                               |
| `seed`                        | Table + HTTP push | Schema registered, data pushed via HTTP ingress                                                                                   |

### Configuration options

| Option                | Default                 | Description                                                                                                      |
| --------------------- | ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `host`                | `http://localhost:8080` | Feldera API base URL                                                                                             |
| `api_key`             | _(none)_                | API key for authenticated Feldera instances                                                                      |
| `schema`              | _(required)_            | Pipeline name in Feldera                                                                                         |
| `compilation_profile` | `dev`                   | SQL compilation profile: `dev` (fast compile), `unoptimized`, or `optimized` (best runtime performance)          |
| `workers`             | `4`                     | Number of pipeline worker threads                                                                                |
| `timeout`             | `300`                   | Max wait (seconds) for pipeline compilation + startup                                                            |

## Materializations

### `view` — Intermediate transform / Materialized view

Creates a `CREATE VIEW` in the pipeline. Use for intermediate transformations
that don't need to be queried directly or connected to an output.

```sql
-- models/orders_enriched.sql
{{ config(materialized='view') }}

SELECT o.id, o.total, c.name AS customer_name
FROM {{ ref('orders') }} o
JOIN {{ ref('customers') }} c ON o.customer_id = c.id
```

Set `stored: true` to promote to a `CREATE MATERIALIZED VIEW` — a view backed
by persistent storage, enabling ad-hoc queries:

```sql
-- models/sales_summary.sql
{{ config(
    materialized='view',
    stored=true,
    connectors=[{'transport': {'name': 'my_delta_connector'}}]
) }}

SELECT
    region,
    product_category,
    SUM(amount) AS total_sales,
    COUNT(*) AS order_count
FROM {{ ref('orders') }}
GROUP BY region, product_category
```

> [!NOTE]
> Every view in Feldera is automatically incrementally maintained by the DBSP
> engine. When inputs change, only affected output rows are recomputed — no
> watermarks, merge logic, or special configuration required. The
> `stored` flag controls only whether the view's state is
> **queryable** (via ad-hoc queries); it does **not** change how the view is
> computed.

On `--full-refresh`, the pipeline is stopped, all stored state (including
connector offsets) is cleared, and the pipeline is redeployed from scratch.

### `table` — Input source

Creates a `CREATE TABLE` — an input source for external data ingress. The model
SQL defines the **column schema**, not a SELECT query. Attach connectors for
Kafka, S3, HTTP, or other input sources.

```sql
-- models/raw_events.sql
{{ config(
    materialized='table',
    connectors=[{
        'transport': {
            'name': 'kafka_in',
            'config': {
                'bootstrap.servers': 'redpanda:29092',
                'topics': ['events']
            }
        },
        'format': {'name': 'json'}
    }]
) }}

event_id BIGINT NOT NULL,
event_type VARCHAR NOT NULL,
payload VARCHAR,
created_at TIMESTAMP NOT NULL
```

### `incremental` — Unsupported

> [!IMPORTANT]
> `dbt-feldera` does not support the `incremental` materialization because
> **all** views in Feldera are natively maintained incrementally by the DBSP
> engine.
>
> Use `materialized='view'` with `stored=true` instead:
>
> ```sql
> {{ config(materialized='view', stored=true) }}
> ```

### `streaming_pipeline` — Full pipeline as a single model

Deploys an entire Feldera pipeline as one dbt model. The model SQL **is** the
complete pipeline program — containing `CREATE TABLE` and `CREATE VIEW`
statements. Useful for complex multi-table, multi-view pipelines managed as a
single unit.

```sql
-- models/my_pipeline.sql
{{ config(materialized='streaming_pipeline') }}

CREATE TABLE orders (
    id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
);

CREATE TABLE customers (
    id BIGINT NOT NULL,
    name VARCHAR NOT NULL
);

CREATE MATERIALIZED VIEW enriched_orders AS
SELECT o.id, o.amount, c.name AS customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

### `seed` — Reference data via HTTP push

Seeds register a `CREATE TABLE` and push row data via Feldera's HTTP ingress
API after the pipeline is deployed. Use for small reference datasets (CSVs).

```bash
dbt seed                # push seed data
dbt seed --full-refresh # stop, clear storage, redeploy, then push
```

### Summary

| Materialization                    | Feldera SQL                | Best for                                                                    |
| ---------------------------------- | -------------------------- | --------------------------------------------------------------------------- |
| `view`                             | `CREATE VIEW`              | Incrementally maintained intermediate transforms                            |
| `view` + `stored: true`            | `CREATE MATERIALIZED VIEW` | Queryable outputs                                                           |
| `table`                            | `CREATE TABLE`             | External input sources (Kafka, S3, HTTP)                                    |
| `streaming_pipeline`               | Full program               | Multi-table/view pipelines as a single unit                                 |
| `seed`                             | `CREATE TABLE` + data push | Small reference datasets (HTTP ingress; any connector can also be attached) |

## Documentation

- **[Feldera documentation](https://docs.feldera.com/)** — platform docs, SQL reference, connectors
  - [Pipelines (continuous queries)](https://docs.feldera.com/pipelines) — how Feldera compiles SQL into an incremental dataflow
  - [Ad-hoc queries](https://docs.feldera.com/sql/ad-hoc) — one-shot DataFusion queries for debugging materialized state
  - [Materialized tables and views](https://docs.feldera.com/sql/materialized) — prerequisite for ad-hoc query access
- **[dbt documentation](https://docs.getdbt.com/)** — general dbt usage and concepts

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and
project layout.

## License

Apache-2.0 — see [LICENSE](../../LICENSE) for details.
