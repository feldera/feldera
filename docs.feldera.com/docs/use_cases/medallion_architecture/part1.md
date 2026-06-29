# Part 1: Create the Feldera Pipeline

## Overview

This demo shows how Feldera maintains a complete **medallion architecture (Bronze → Silver → Gold)** as a single, always-on **incremental** pipeline. Instead of running batch jobs on a schedule to rebuild each layer, Feldera computes every layer continuously: as new data arrives, only the affected results are recomputed, and downstream Silver and Gold views update within milliseconds.

The scenario is a retail e-commerce business. Raw operational data — orders, order line items, the product catalog, customers, suppliers, inventory movements, and clickstream events — lands in **Bronze** tables. Feldera cleans, validates, and joins it into **Silver** views, then rolls it up into **Gold** business metrics like supplier performance, inventory risk, weekly revenue trends, and cancellation impact.

The focus of the demo is *incremental view maintenance over a changing dataset*. Feldera supports a wide variety of [connectors](/connectors). In this demo, the pipeline first backfills a historical snapshot from Delta tables on S3, then consumes a stream of **CDC (change data capture)** events — including updates and deletes (e.g. an order transitioning to `cancelled` or `returned`). Because Gold views are maintained incrementally, a single order status change moves correctly through window functions, moving averages, and cumulative totals — historical weekly revenue decreases, cancellation rates re-compute, and inventory alerts appear or clear — all without recomputing anything from scratch.

A Spark notebook is provided so you can compare a traditional batch refresh job against Feldera's incremental view maintenance (see [Part 3](./part3.md)).

### How data flows

1. **Snapshot backfill** — On start, each Bronze table reads a Delta table snapshot from S3 (`s3://feldera-demos/ecommerce-cdc-0-01/snapshot/...`) via Feldera's [Delta input connector](/connectors/sources/delta), loading the historical state into the pipeline.
2. **CDC stream** — `push_changes.py` replays hourly insert/delete NDJSON files into Feldera's HTTP ingress (`?update_format=insert_delete`), applying inserts, updates, and deletes against the Bronze tables (an update is a delete of the old row image followed by an insert of the new one).
3. **Continuous computation** — Every change propagates incrementally through the Silver and Gold layers. Ad-hoc queries (and a Grafana dashboard, if connected) reflect the new state immediately. Gold views are updated within milliseconds of Bronze tables receiving new input.

## Demo files

All of the demo's code is in the Feldera repository. You can download it directly:

| File | Purpose |
|------|---------|
| [`01-medallion-architecture.sql`](https://github.com/feldera/feldera/blob/main/crates/pipeline-manager/demos/sql/01-medallion-architecture.sql) | The full pipeline: Bronze tables, Silver views, Gold materialized views. |
| [`push_changes.py`](https://github.com/feldera/feldera/blob/main/docs.feldera.com/docs/use_cases/medallion_architecture/push_changes.py) | Replays CDC events from S3 into the running pipeline ([Part 2](./part2.md)). |
| [`spark_notebook.py`](https://github.com/feldera/feldera/blob/main/docs.feldera.com/docs/use_cases/medallion_architecture/spark_notebook.py) | The equivalent batch job in Spark, for comparison ([Part 3](./part3.md)). |

:::tip
Each GitHub page above has a **Download raw file** button. To fetch a file from a
terminal, use its raw URL, e.g.:

```bash
curl -O 'https://raw.githubusercontent.com/feldera/feldera/main/docs.feldera.com/docs/use_cases/medallion_architecture/push_changes.py'
```
:::

## The medallion layers

The SQL pipeline is organized into three medallion layers. Bronze is the only layer that touches the raw source; Silver and Gold are pure transformations expressed as Feldera views, so they are maintained incrementally.

### Bronze — raw ingestion (`CREATE TABLE`)

Append-and-update tables that mirror the source systems with no transformation. Each is backfilled from a Delta snapshot and then kept current by the CDC stream.

| Table | Description |
|-------|-------------|
| `bronze_clickstream_events` | Page views, sessions, and product interactions |
| `bronze_orders` | Order headers with status, totals, payment, and coupons |
| `bronze_order_items` | Order line items (product, quantity, unit price, discount) |
| `bronze_products` | Product catalog with cost and list pricing |
| `bronze_inventory_events` | Inventory movements (restock, sale_reserve, returns) |
| `bronze_customers` | Customer dimension with tier and geography |
| `bronze_suppliers` | Supplier dimension with lead times |

### Silver — cleaned, validated, enriched (`LOCAL VIEW`)

All cleaning, validation, deduplication, and joins happen here, so that Gold never references Bronze directly.

- `silver_customers` — validated customer dimension (known tiers only)
- `silver_products` — cleaned product dimension (product grain: id, name, category, brand, list price)
- `silver_enriched_clickstream` — interaction events, cleaned and filtered (product/customer enrichment is deferred to Gold)
- `silver_orders_enriched` — orders joined to customers and aggregated line-item metrics
- `silver_order_items_enriched` — line items enriched with product, supplier, and customer context; computes line-level revenue and margin
- `silver_confirmed_order_items` — line items excluding cancelled/returned orders
- `silver_inventory_current` — running per-product/per-warehouse stock from cumulative inventory events
- `silver_inventory_by_supplier` — supplier-level inventory rollup

### Gold — business metrics and analytics (`MATERIALIZED VIEW`)

Aggregation and final filtering over Silver — no Bronze references. These are the views a BI tool or dashboard consumes.

- `gold_supplier_performance` — revenue, margin, reliability, and inventory by supplier
- `gold_inventory_risk` — days-of-stock vs. lead time, scored CRITICAL / WARNING / OK
- `gold_order_status_summary` — order count and revenue by status (changes visibly with every CDC commit)
- `gold_weekly_revenue_trend` — weekly revenue with WoW change, 4-week moving average, and cumulative YTD
- `gold_cancellation_impact` — cancellation rates with cumulative and 4-week moving windows
- `gold_realtime_inventory_alerts` — filtered stream of CRITICAL inventory items
- `gold_product_demand_surge` — recent (24-hour window from the latest event) cart velocity vs. stock and lead time

## Create the pipeline

You have two options.

### Option A — from the Web Console (recommended)

This pipeline ships as a packaged demo, so you don't have to copy any SQL:

1. Open the Feldera Web Console.
2. On the home page, find **Maintaining a Medallion Architecture in Real Time** in the **Explore use cases and tutorials** section and click it.
3. The Web Console creates a pipeline named **`ecommerce-medallion-architecture`** with the full SQL pre-loaded.
4. Click **▶ Start** to run the pipeline.

### Option B — paste the SQL

1. Create a new pipeline in the Web Console.
2. Paste the contents of [`01-medallion-architecture.sql`](https://github.com/feldera/feldera/blob/main/crates/pipeline-manager/demos/sql/01-medallion-architecture.sql).
3. Click **▶ Start**.

When the pipeline starts, the `delta_table_input` connectors backfill the historical snapshot from S3. Once ingestion settles, query a Gold view from the **Ad-hoc query** tab to confirm the backfill succeeded:

```sql
SELECT * FROM gold_supplier_performance ORDER BY total_net_revenue DESC LIMIT 10;
```

After the pipeline has loaded the snapshot, continue to [Part 2](./part2.md) to push CDC changes and watch the Gold views update in real time.

