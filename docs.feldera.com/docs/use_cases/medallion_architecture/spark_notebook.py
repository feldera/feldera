# Databricks notebook source
# MAGIC %md
# MAGIC # Compute Silver & Gold Delta Tables — Spark Batch
# MAGIC
# MAGIC Batch job that materializes the silver and gold layers as Delta tables on S3.
# MAGIC All tables are full-refresh (overwrite) — simple, always correct, and fast at
# MAGIC small scale. At production scale, every run re-scans the entire dataset.
# MAGIC
# MAGIC **Feldera equivalent:** The same business logic is defined in `ecommerce.sql` as
# MAGIC SQL views. Feldera maintains all views incrementally — no batch scheduling,
# MAGIC no full re-scans, no orchestration.
# MAGIC
# MAGIC Run this notebook after `generate_bronze_tables.py` has produced bronze data.
# MAGIC The initial load runs the full silver/gold pipeline once over the entire
# MAGIC dataset. The CDC comparison section further down then processes progressively
# MAGIC more CDC data to make batch cost scaling visible — each run re-scans a larger
# MAGIC dataset, while Feldera's incremental engine avoids this entirely: cost is
# MAGIC proportional to change size, not total size.

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_step_times = []
_t0 = time.time()


def timed_step(name):
    """Print elapsed time for the previous step and start timing the next one."""
    now = time.time()
    if _step_times:
        prev_name, prev_start = _step_times[-1]
        print(f"  [{prev_name}] {now - prev_start:.1f}s")
    _step_times.append((name, now))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import os

SCALE_FACTOR = 0.01

# Bronze input: always read from feldera-demos in us-west-1.
BRONZE_BUCKET = "s3://feldera-demos"
BRONZE_REGION = "us-west-1"
BRONZE_PREFIX = f"ecommerce-cdc-{str(SCALE_FACTOR).replace('.', '-')}"

# Output control. By default the pipeline computes every layer but PERSISTS
# NOTHING: the full Spark plan still executes (through a noop sink) so the
# batch-cost timing stays real, while no Delta tables are written to S3. Set
# WRITE_OUTPUT = True to materialize the silver/gold tables.
WRITE_OUTPUT = False

# OPTIONAL: configure if writing output is desired.
# Silver/Gold output: bucket + region come from env vars so the downstream
# lakehouse can live in a different region than the bronze source.
WRITE_BUCKET = os.environ.get("SILVER_GOLD_BUCKET", "s3://feldera-demos").rstrip("/")
WRITE_REGION = os.environ.get("SILVER_GOLD_REGION", "us-west-1")
WRITE_PREFIX = f"ecommerce-{str(SCALE_FACTOR).replace('.', '-')}"


def s3a(path: str) -> str:
    return path.replace("s3://", "s3a://")


# Region pinning note: on Databricks Serverless, spark.hadoop.* configs are
# not settable from a notebook (SQLSTATE 42K0I). S3A auto-resolves each
# bucket's region via a HEAD request, so BRONZE_REGION / WRITE_REGION here
# are documentation. To force-pin a region, configure it on the cluster
# (spark.hadoop.fs.s3a.bucket.<bucket>.endpoint.region) or via a Unity
# Catalog external location, not here.

BRONZE_ROOT = s3a(f"{BRONZE_BUCKET}/{BRONZE_PREFIX}/snapshot")
# optional paths if writing output is desired
SILVER_ROOT = s3a(f"{WRITE_BUCKET}/{WRITE_PREFIX}/silver")
GOLD_ROOT = s3a(f"{WRITE_BUCKET}/{WRITE_PREFIX}/gold")

print(f"Bronze (read):  {BRONZE_ROOT}  [region={BRONZE_REGION}]")
print(f"Silver (write): {SILVER_ROOT}  [region={WRITE_REGION}]")
print(f"Gold   (write): {GOLD_ROOT}  [region={WRITE_REGION}]")

BRONZE_VERSIONS = {
    "bronze_suppliers": "latest",
    "bronze_products": "latest",
    "bronze_customers": "latest",
    "bronze_orders": "latest",
    "bronze_order_items": "latest",
    "bronze_clickstream_events": "latest",
    "bronze_inventory_events": "latest",
}

# Controls the narrative output of the CDC comparison section below.
DEMO_MODE = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def bronze_path(name: str) -> str:
    return f"{BRONZE_ROOT}/{name}"


def silver_path(name: str) -> str:
    return f"{SILVER_ROOT}/{name}"


def gold_path(name: str) -> str:
    return f"{GOLD_ROOT}/{name}"


def materialize(df, path):
    """Force computation of `df`. Writes Delta to `path` when WRITE_OUTPUT is set;
    otherwise runs the full plan through a noop sink so the batch-cost timing stays
    accurate without persisting any output. Plain function (not a DataFrame method)
    so it works under Spark Connect on Databricks Serverless."""
    if WRITE_OUTPUT:
        df.write.format("delta").mode("overwrite").save(path)
    else:
        df.write.format("noop").mode("overwrite").save()


def materialize_and_count(df, path):
    """Like materialize(), but also return the row count of `df`.

    With WRITE_OUTPUT off (the default), df.count() is the single action that
    forces the full Spark plan — the same role the noop sink plays in
    materialize() — and also yields the row count, so each view is still computed
    exactly once. That matters because caching is unavailable on Databricks
    Serverless: counting a view separately would re-run its whole bronze→silver→gold
    DAG. When writing output, the Delta table is persisted and the count is read
    back from it (cheap, served from Delta stats)."""
    if WRITE_OUTPUT:
        df.write.format("delta").mode("overwrite").save(path)
        return spark.read.format("delta").load(path).count()
    return df.count()


def read_bronze(table_name: str):
    """Read a bronze table at the configured version."""
    version = BRONZE_VERSIONS[table_name]
    path = bronze_path(table_name)
    if version == "latest":
        return spark.read.format("delta").load(path)
    return spark.read.format("delta").option("versionAsOf", int(version)).load(path)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver & Gold Pipeline
# MAGIC
# MAGIC ### Silver layer (8 tables)
# MAGIC
# MAGIC | Table | Key logic | Feldera equivalent |
# MAGIC |---|---|---|
# MAGIC | `silver_customers` | Filter invalid records | `CREATE VIEW` — incremental filter |
# MAGIC | `silver_products` | Cleaned product dimension (product grain) | `CREATE VIEW` — incremental filter |
# MAGIC | `silver_enriched_clickstream` | Filter to interaction events (clean-only) | Incremental filter |
# MAGIC | `silver_orders_enriched` | Join + aggregate order items | Incremental join + GROUP BY |
# MAGIC | `silver_order_items_enriched` | 4-table join with margins | Incremental multi-join |
# MAGIC | `silver_confirmed_order_items` | Exclude cancelled/returned | Incremental filter |
# MAGIC | `silver_inventory_current` | Running stock per product/warehouse | Incremental SUM — only affected rows update |
# MAGIC | `silver_inventory_by_supplier` | Stock rolled up to supplier | Incremental GROUP BY |
# MAGIC
# MAGIC ### Gold layer (7 tables)
# MAGIC
# MAGIC | Table | Key logic | Feldera demo point |
# MAGIC |---|---|---|
# MAGIC | `gold_order_status_summary` | Status distribution | Real-time lifecycle transitions |
# MAGIC | `gold_supplier_performance` | Revenue + margin per supplier | Multi-table DAG |
# MAGIC | `gold_inventory_risk` | Stock risk scoring | Products enter/leave risk levels in real-time |
# MAGIC | `gold_realtime_inventory_alerts` | CRITICAL stock filter | Real-time alerting view |
# MAGIC | `gold_weekly_revenue_trend` | Window functions (WoW, moving avg, YTD) | Incremental window recomputation |
# MAGIC | `gold_cancellation_impact` | Cancellation rate windows | Incremental running totals |
# MAGIC | `gold_product_demand_surge` | Recent (24h-window) cart velocity vs stock & lead time | Sliding-window surge: products enter/leave in real-time |
# MAGIC
# MAGIC **Orders deduplication:** Orders go through a lifecycle (pending -> confirmed ->
# MAGIC shipped -> delivered). Each transition appends a new row. Spark uses
# MAGIC `ROW_NUMBER()` to keep the latest; Feldera uses `PRIMARY KEY` for automatic dedup.

# COMMAND ----------


def run_pipeline():
    """Run the full silver -> gold batch pipeline over the full dataset.

    Returns:
        dict with step_times (list of (name, seconds)), total_time, and row_counts.
    """
    global _step_times, _t0
    _step_times = []
    _t0 = time.time()
    row_counts = {}

    read = read_bronze

    # ── Read Bronze ────────────────────────────────────────────────────
    timed_step("read_bronze")

    products_df = read("bronze_products")
    suppliers_df = read("bronze_suppliers")
    customers_df = read("bronze_customers")
    clickstream_df = read("bronze_clickstream_events")
    order_items_df = read("bronze_order_items")
    inventory_df = read("bronze_inventory_events")

    products_df.createOrReplaceTempView("bronze_products")
    suppliers_df.createOrReplaceTempView("bronze_suppliers")
    order_items_df.createOrReplaceTempView("bronze_order_items")

    # Orders deduplication — keep latest status per order_id
    orders_raw = read("bronze_orders")
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    orders_df = (
        orders_raw.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    orders_df.createOrReplaceTempView("bronze_orders")

    row_counts["orders"] = orders_df.count()
    row_counts["order_items"] = order_items_df.count()
    row_counts["clickstream"] = clickstream_df.count()
    row_counts["inventory"] = inventory_df.count()

    print(f"  bronze_products:            {products_df.count():>10,} rows")
    print(f"  bronze_suppliers:           {suppliers_df.count():>10,} rows")
    print(f"  bronze_customers:           {customers_df.count():>10,} rows")
    print(f"  bronze_orders (deduped):    {row_counts['orders']:>10,} rows")
    print(f"  bronze_order_items:         {row_counts['order_items']:>10,} rows")
    print(f"  bronze_clickstream_events:  {row_counts['clickstream']:>10,} rows")
    print(f"  bronze_inventory_events:    {row_counts['inventory']:>10,} rows")

    # ── Silver: silver_customers ───────────────────────────────────────
    timed_step("silver_customers")

    silver_customers = customers_df.filter(
        F.col("customer_id").isNotNull()
        & F.col("customer_tier").isin("standard", "silver", "gold", "platinum")
    )
    materialize(silver_customers, silver_path("silver_customers"))
    silver_customers.createOrReplaceTempView("silver_customers")
    print(f"  silver_customers: {silver_customers.count()} rows")

    # ── Silver: silver_products ─────────────────────────────────────────
    timed_step("silver_products")

    silver_products = spark.sql("""
        SELECT product_id, product_name, category, brand, list_price
        FROM bronze_products
        WHERE product_id IS NOT NULL
    """)
    materialize(silver_products, silver_path("silver_products"))
    silver_products.createOrReplaceTempView("silver_products")
    print(f"  silver_products: {silver_products.count()} rows")

    # ── Silver: silver_enriched_clickstream ─────────────────────────────
    timed_step("silver_enriched_clickstream")

    clickstream_df.createOrReplaceTempView("bronze_clickstream_events")

    # Clean-only: dimension enrichment moved to gold (aggregate-then-join via
    # silver_products). See ecommerce.sql for rationale.
    enriched_clickstream = spark.sql("""
        SELECT
            ce.event_id, ce.user_id, ce.session_id, ce.event_type, ce.page_url,
            ce.product_id, ce.device_type, ce.geo_country, ce.geo_region,
            ce.event_timestamp
        FROM bronze_clickstream_events ce
        WHERE ce.event_type IS NOT NULL
            AND ce.user_id IS NOT NULL
            AND ce.event_type IN ('page_view', 'product_view', 'add_to_cart', 'begin_checkout', 'purchase')
    """)
    materialize(enriched_clickstream, silver_path("silver_enriched_clickstream"))
    enriched_clickstream.createOrReplaceTempView("silver_enriched_clickstream")
    print(f"  silver_enriched_clickstream: {enriched_clickstream.count()} rows")

    # ── Silver: silver_orders_enriched ──────────────────────────────────
    timed_step("silver_orders_enriched")

    enriched_orders = spark.sql("""
        SELECT
            o.order_id, o.user_id, o.order_status, o.order_total,
            o.discount_amount, o.shipping_cost, o.payment_method, o.coupon_code,
            o.created_at, o.updated_at,
            c.customer_tier,
            c.geo_country AS customer_country,
            c.geo_region AS customer_region,
            c.signup_date,
            oi.item_count, oi.total_quantity, oi.gross_item_revenue, oi.avg_discount_pct
        FROM bronze_orders o
        JOIN silver_customers c ON o.user_id = c.customer_id
        JOIN (
            SELECT order_id,
                COUNT(*) AS item_count,
                SUM(quantity) AS total_quantity,
                SUM(quantity * unit_price) AS gross_item_revenue,
                AVG(discount_pct) AS avg_discount_pct
            FROM bronze_order_items
            WHERE quantity > 0 AND unit_price >= 0
            GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
        WHERE o.order_id IS NOT NULL
            AND o.user_id IS NOT NULL
            AND o.order_total >= 0
            AND o.order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned')
    """)
    materialize(enriched_orders, silver_path("silver_orders_enriched"))
    enriched_orders.createOrReplaceTempView("silver_orders_enriched")
    print(f"  silver_orders_enriched: {enriched_orders.count()} rows")

    # ── Silver: silver_order_items_enriched ─────────────────────────────
    timed_step("silver_order_items_enriched")

    enriched_items = spark.sql("""
        SELECT
            oi.order_item_id, oi.order_id, oi.product_id,
            oi.quantity, oi.unit_price, oi.discount_pct,
            oi.quantity * oi.unit_price AS line_gross_revenue,
            oi.quantity * oi.unit_price * (1.0 - COALESCE(oi.discount_pct, 0) / 100.0) AS line_net_revenue,
            oi.quantity * (oi.unit_price - p.cost_price) AS line_gross_margin,
            p.product_name, p.category, p.brand, p.cost_price, p.list_price,
            s.supplier_id, s.supplier_name, s.country AS supplier_country, s.lead_time_days,
            o.created_at AS order_created_at,
            o.order_status, o.user_id,
            c.customer_tier
        FROM bronze_order_items oi
        JOIN bronze_products p ON oi.product_id = p.product_id
        JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
        JOIN bronze_orders o ON oi.order_id = o.order_id
        JOIN silver_customers c ON o.user_id = c.customer_id
        WHERE oi.quantity > 0
            AND oi.unit_price >= 0
            AND p.is_active = TRUE
            AND o.order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned')
    """)
    materialize(enriched_items, silver_path("silver_order_items_enriched"))
    enriched_items.createOrReplaceTempView("silver_order_items_enriched")
    print(f"  silver_order_items_enriched: {enriched_items.count()} rows")

    # ── Silver: silver_confirmed_order_items ────────────────────────────
    timed_step("silver_confirmed_order_items")

    confirmed_items = spark.sql("""
        SELECT * FROM silver_order_items_enriched
        WHERE order_status NOT IN ('cancelled', 'returned')
    """)
    materialize(confirmed_items, silver_path("silver_confirmed_order_items"))
    confirmed_items.createOrReplaceTempView("silver_confirmed_order_items")
    print(f"  silver_confirmed_order_items: {confirmed_items.count()} rows")

    # ── Silver: silver_inventory_current ─────────────────────────────────
    timed_step("silver_inventory_current")

    inventory_df.createOrReplaceTempView("bronze_inventory_events")

    full_inventory = spark.sql("""
        SELECT
            ie.product_id, ie.warehouse_id,
            p.product_name, p.category, p.brand,
            s.supplier_id, s.supplier_name, s.lead_time_days,
            SUM(ie.quantity_change) AS current_stock,
            SUM(CASE WHEN ie.event_type = 'restock' THEN ie.quantity_change ELSE 0 END) AS total_restocked,
            SUM(CASE WHEN ie.event_type = 'sale_reserve' THEN ABS(ie.quantity_change) ELSE 0 END)
                - SUM(CASE WHEN ie.event_type = 'cancellation_restock' THEN ie.quantity_change ELSE 0 END)
                AS total_sold,
            SUM(CASE WHEN ie.event_type = 'return_restock' THEN ie.quantity_change ELSE 0 END) AS total_returned
        FROM bronze_inventory_events ie
        JOIN bronze_products p ON ie.product_id = p.product_id
        JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
        WHERE ie.quantity_change IS NOT NULL AND ie.product_id IS NOT NULL
        GROUP BY ie.product_id, ie.warehouse_id, p.product_name, p.category, p.brand, s.supplier_id, s.supplier_name, s.lead_time_days
    """)
    materialize(full_inventory, silver_path("silver_inventory_current"))
    full_inventory.createOrReplaceTempView("silver_inventory_current")
    print(f"  silver_inventory_current: {full_inventory.count()} rows")

    # ── Silver: silver_inventory_by_supplier ─────────────────────────────
    timed_step("silver_inventory_by_supplier")

    sibs = spark.sql("""
        SELECT supplier_id, supplier_name,
            SUM(current_stock) AS total_current_stock,
            SUM(total_sold) AS total_sold,
            SUM(total_restocked) AS total_restocked,
            SUM(total_returned) AS total_returned
        FROM silver_inventory_current
        GROUP BY supplier_id, supplier_name
    """)
    materialize(sibs, silver_path("silver_inventory_by_supplier"))
    sibs.createOrReplaceTempView("silver_inventory_by_supplier")
    print(f"  silver_inventory_by_supplier: {sibs.count()} rows")

    # ── Gold: gold_order_status_summary ──────────────────────────────────
    timed_step("gold_order_status_summary")

    materialize(
        spark.sql("""
        SELECT
            o.order_status,
            COUNT(DISTINCT o.order_id) AS order_count,
            COALESCE(SUM(o.order_total), 0) AS total_revenue,
            AVG(o.order_total) AS avg_order_value,
            COUNT(DISTINCT o.user_id) AS unique_customers
        FROM silver_orders_enriched o
        GROUP BY o.order_status
    """),
        gold_path("gold_order_status_summary"),
    )
    print("  gold_order_status_summary: done")

    # ── Gold: gold_supplier_performance ──────────────────────────────────
    timed_step("gold_supplier_performance")

    materialize(
        spark.sql("""
        WITH orders_by_supplier AS (
            SELECT
                oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days,
                SUM(oi.quantity) AS total_units_sold,
                SUM(oi.line_net_revenue) AS total_net_revenue,
                SUM(oi.line_gross_margin) AS total_gross_margin,
                SUM(oi.line_gross_margin) / NULLIF(SUM(oi.line_net_revenue), 0) AS avg_margin_pct,
                AVG(oi.discount_pct) AS avg_discount_applied,
                CAST(SUM(CASE WHEN oi.order_status = 'delivered' THEN 1 ELSE 0 END) AS DOUBLE)
                    / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) AS reliability_score
            FROM silver_confirmed_order_items oi
            GROUP BY oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days
        ),
        products_by_supplier AS (
            SELECT supplier_id, COUNT(*) AS products_sold
            FROM (SELECT DISTINCT supplier_id, product_id FROM silver_confirmed_order_items)
            GROUP BY supplier_id
        ),
        orders_count_by_supplier AS (
            SELECT supplier_id, COUNT(*) AS orders_fulfilled
            FROM (SELECT DISTINCT supplier_id, order_id FROM silver_confirmed_order_items)
            GROUP BY supplier_id
        )
        SELECT
            oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days,
            p.products_sold,
            o.orders_fulfilled,
            oi.total_units_sold,
            oi.total_net_revenue,
            oi.total_gross_margin,
            oi.avg_margin_pct,
            oi.avg_discount_applied,
            oi.reliability_score,
            inv.total_current_stock,
            inv.total_sold AS inventory_units_sold,
            inv.total_restocked AS inventory_units_restocked,
            inv.total_returned AS inventory_units_returned
        FROM orders_by_supplier oi
        JOIN silver_inventory_by_supplier inv ON oi.supplier_id = inv.supplier_id
        JOIN products_by_supplier p ON oi.supplier_id = p.supplier_id
        JOIN orders_count_by_supplier o ON oi.supplier_id = o.supplier_id
    """),
        gold_path("gold_supplier_performance"),
    )
    print("  gold_supplier_performance: done")

    # ── Gold: gold_inventory_risk ────────────────────────────────────────
    timed_step("gold_inventory_risk")

    inventory_risk = spark.sql("""
        SELECT
            inv.product_id, inv.product_name, inv.category, inv.brand,
            inv.supplier_name, inv.lead_time_days,
            inv.total_stock_all_warehouses,
            sales.units_sold, sales.avg_daily_units,
            CASE WHEN sales.avg_daily_units > 0
                THEN inv.total_stock_all_warehouses / sales.avg_daily_units ELSE NULL END AS days_of_stock_remaining,
            CASE
                WHEN sales.avg_daily_units > 0 AND (inv.total_stock_all_warehouses / sales.avg_daily_units) < inv.lead_time_days * 1.5 THEN 'CRITICAL'
                WHEN sales.avg_daily_units > 0 AND (inv.total_stock_all_warehouses / sales.avg_daily_units) < inv.lead_time_days * 3.0 THEN 'WARNING'
                ELSE 'OK'
            END AS stock_risk_level,
            sales.net_revenue, sales.gross_margin
        FROM (
            SELECT product_id, product_name, category, brand, supplier_name, lead_time_days,
                SUM(current_stock) AS total_stock_all_warehouses
            FROM silver_inventory_current
            GROUP BY product_id, product_name, category, brand, supplier_name, lead_time_days
        ) inv
        JOIN (
            SELECT product_id,
                SUM(quantity) AS units_sold,
                -- avg_daily_units must be a genuine per-day rate. SUM(quantity) spans
                -- the entire history (no date filter), so dividing by a hard-coded 30
                -- is only correct for exactly 30 days of data; dividing by the number
                -- of distinct days actually observed yields a correct rate regardless
                -- of the dataset's span.
                SUM(quantity) / NULLIF(COUNT(DISTINCT date_trunc('day', order_created_at)), 0) AS avg_daily_units,
                SUM(line_net_revenue) AS net_revenue,
                SUM(line_gross_margin) AS gross_margin
            FROM silver_confirmed_order_items
            GROUP BY product_id
        ) sales ON inv.product_id = sales.product_id
    """)
    materialize(inventory_risk, gold_path("gold_inventory_risk"))
    inventory_risk.createOrReplaceTempView("gold_inventory_risk")
    print("  gold_inventory_risk: done")

    # ── Gold: gold_realtime_inventory_alerts ─────────────────────────────
    timed_step("gold_realtime_inventory_alerts")

    materialize(
        spark.sql("""
        SELECT * FROM gold_inventory_risk WHERE stock_risk_level = 'CRITICAL'
    """),
        gold_path("gold_realtime_inventory_alerts"),
    )
    print("  gold_realtime_inventory_alerts: done")

    # ── Gold: gold_weekly_revenue_trend ──────────────────────────────────
    timed_step("gold_weekly_revenue_trend")

    materialize(
        spark.sql("""
        SELECT
            week_start, category,
            weekly_net_revenue, weekly_gross_margin, order_count, units_sold,
            weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start)
                AS revenue_wow_change,
            (weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start))
                / NULLIF(LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start), 0)
                AS revenue_wow_pct_change,
            AVG(weekly_net_revenue) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW)
                AS revenue_4wk_moving_avg,
            AVG(weekly_gross_margin) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW)
                AS margin_4wk_moving_avg,
            SUM(weekly_net_revenue) OVER (PARTITION BY category, EXTRACT(YEAR FROM week_start) ORDER BY week_start RANGE UNBOUNDED PRECEDING)
                AS cumulative_ytd_revenue
        FROM (
            SELECT
                date_trunc('week', oi.order_created_at) AS week_start,
                oi.category,
                SUM(oi.line_net_revenue) AS weekly_net_revenue,
                SUM(oi.line_gross_margin) AS weekly_gross_margin,
                COUNT(DISTINCT oi.order_id) AS order_count,
                SUM(oi.quantity) AS units_sold
            FROM silver_confirmed_order_items oi
            GROUP BY date_trunc('week', oi.order_created_at), oi.category
        )
    """),
        gold_path("gold_weekly_revenue_trend"),
    )
    print("  gold_weekly_revenue_trend: done")

    # ── Gold: gold_cancellation_impact ───────────────────────────────────
    timed_step("gold_cancellation_impact")

    materialize(
        spark.sql("""
        SELECT
            category, week_start,
            weekly_cancelled_orders, weekly_total_orders,
            weekly_cancelled_revenue, weekly_total_revenue,
            CAST(SUM(weekly_cancelled_orders) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS DOUBLE)
                / NULLIF(CAST(SUM(weekly_total_orders) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS DOUBLE), 0)
                AS cumulative_cancellation_rate,
            SUM(weekly_cancelled_revenue) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING)
                AS cumulative_cancelled_revenue,
            CAST(SUM(weekly_cancelled_orders) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS DOUBLE)
                / NULLIF(CAST(SUM(weekly_total_orders) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS DOUBLE), 0)
                AS cancellation_rate_4wk
        FROM (
            SELECT
                oi.category,
                date_trunc('week', oi.order_created_at) AS week_start,
                COUNT(DISTINCT CASE WHEN oi.order_status = 'cancelled' THEN oi.order_id END) AS weekly_cancelled_orders,
                COUNT(DISTINCT oi.order_id) AS weekly_total_orders,
                SUM(CASE WHEN oi.order_status = 'cancelled' THEN oi.line_net_revenue ELSE 0 END) AS weekly_cancelled_revenue,
                SUM(oi.line_net_revenue) AS weekly_total_revenue
            FROM silver_order_items_enriched oi
            GROUP BY oi.category, date_trunc('week', oi.order_created_at)
        )
    """),
        gold_path("gold_cancellation_impact"),
    )
    print("  gold_cancellation_impact: done")

    # ── Gold: gold_product_demand_surge ──────────────────────────────────
    timed_step("gold_product_demand_surge")

    materialize(
        spark.sql("""
        WITH recent_demand AS (
            -- AGGREGATE-THEN-JOIN: collapse to product grain FIRST with pure SUM
            -- counts (no MAX of attributes), then join the product dimension at
            -- product grain below. Browsing demand over a trailing 1-day window
            -- relative to the latest event in the stream (scalar subquery; not
            -- wall-clock NOW(), which is empty on a historical snapshot). The
            -- window slides as events arrive, so products enter AND leave in
            -- real time.
            SELECT
                ce.product_id,
                SUM(CASE WHEN ce.event_type = 'product_view' THEN 1 ELSE 0 END) AS recent_views,
                SUM(CASE WHEN ce.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS recent_add_to_carts,
                SUM(CASE WHEN ce.event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS recent_checkouts
            FROM silver_enriched_clickstream ce
            WHERE ce.product_id IS NOT NULL
                AND ce.event_timestamp >
                    (SELECT MAX(event_timestamp) FROM silver_enriched_clickstream) - INTERVAL 1 DAY
            GROUP BY ce.product_id
        ),
        stock_by_product AS (
            SELECT product_id,
                SUM(current_stock) AS total_stock_all_warehouses,
                MAX(lead_time_days) AS lead_time_days
            FROM silver_inventory_current
            GROUP BY product_id
        )
        SELECT
            d.product_id, p.product_name, p.category AS product_category, p.brand AS product_brand,
            d.recent_views, d.recent_add_to_carts, d.recent_checkouts,
            COALESCE(s.total_stock_all_warehouses, 0) AS total_stock_all_warehouses,
            s.lead_time_days,
            -- Days of stock cover at the current daily add-to-cart rate (1-day window
            -- => recent_add_to_carts is a per-day rate). Heuristic: clickstream has no
            -- quantity, so 1 add-to-cart ~= 1 unit of demand. Stock clamped at >= 0.
            CAST(GREATEST(COALESCE(s.total_stock_all_warehouses, 0), 0) AS DOUBLE)
                / NULLIF(CAST(d.recent_add_to_carts AS DOUBLE), 0) AS days_of_stock_cover,
            -- DEMO-ONLY: valid because the generator attaches one product_id per funnel
            -- journey; in production begin_checkout is cart-level with no product_id.
            CAST(d.recent_checkouts AS DOUBLE)
                / NULLIF(CAST(d.recent_views AS DOUBLE), 0) AS view_to_checkout_rate,
            -- Stockout risk: stock cover shorter than a supplier restock cycle.
            -- lead_time_days defaults to 7 when unknown.
            CASE
                WHEN d.recent_add_to_carts = 0 THEN 'OK'
                WHEN COALESCE(s.total_stock_all_warehouses, 0) <= 0 THEN 'SURGE_STOCKOUT_RISK'
                WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE)
                    < COALESCE(s.lead_time_days, 7) THEN 'SURGE_STOCKOUT_RISK'
                WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE)
                    < COALESCE(s.lead_time_days, 7) * 2 THEN 'WARNING'
                ELSE 'OK'
            END AS demand_alert
        FROM recent_demand d
        LEFT JOIN silver_products p ON d.product_id = p.product_id
        LEFT JOIN stock_by_product s ON d.product_id = s.product_id
    """),
        gold_path("gold_product_demand_surge"),
    )
    print("  gold_product_demand_surge: done")

    # ── Finalize ────────────────────────────────────────────────────────
    timed_step("done")
    total = time.time() - _t0

    step_times = []
    for i in range(len(_step_times) - 1):
        name, start = _step_times[i]
        _, end = _step_times[i + 1]
        step_times.append((name, end - start))

    return {"step_times": step_times, "total_time": total, "row_counts": row_counts}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

result = run_pipeline()
total = result["total_time"]

print("\n" + "=" * 70)
print("Silver & Gold computation complete.")
print("-" * 70)
print("\nStep timing:")
for name, elapsed in result["step_times"]:
    print(f"  {name:40s} {elapsed:>8.1f}s")
print(f"  {'TOTAL':40s} {total:>8.1f}s")
print("-" * 70)

if WRITE_OUTPUT:
    for layer, root in [("SILVER", SILVER_ROOT), ("GOLD", GOLD_ROOT)]:
        print(f"\n{layer}:")
        for name in sorted(
            [
                d.name.replace("/", "")
                for d in dbutils.fs.ls(root.replace("s3a://", "s3://"))
                if d.isDir()
            ]
        ):
            path = f"{root}/{name}"
            count = spark.read.format("delta").load(path).count()
            print(f"  {name:45s} {count:>12,} rows")
else:
    print("\nWRITE_OUTPUT = False — computed all layers, persisted nothing.")
    print("Set WRITE_OUTPUT = True to write the silver/gold Delta tables.")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Incremental Comparison
# MAGIC
# MAGIC Starting from the snapshot (day 90), process CDC data hour by hour.
# MAGIC Each run reads ALL data (snapshot + all CDC files to date) and full-refreshes
# MAGIC all silver and gold tables. This is what batch processing looks like when
# MAGIC handling incremental change data — **the entire dataset is re-scanned** even
# MAGIC though only a few hundred rows changed.
# MAGIC
# MAGIC Compare these times against `push_changes.py` which pushes the same CDC data
# MAGIC to Feldera, where only the changed rows propagate through the view DAG.

# COMMAND ----------

SNAPSHOT_ROOT = BRONZE_ROOT
CDC_ROOT = s3a(f"{BRONZE_BUCKET}/{BRONZE_PREFIX}/cdc")

# CDC tables in Feldera insert_delete JSON format
CDC_TABLES = {
    "bronze_orders": "orders",
    "bronze_order_items": "order_items",
    "bronze_clickstream_events": "clickstream_events",
    "bronze_inventory_events": "inventory_events",
}


def read_snapshot(table_name):
    """Read a snapshot Delta table."""
    return spark.read.format("delta").load(f"{SNAPSHOT_ROOT}/{table_name}")


def read_cdc_files(table_short_name, up_to_hour=None, target_schema=None):
    """Read CDC NDJSON files in Feldera's insert_delete JSON format and return
    the inserted row state.

    Each line is a change record wrapped with an "insert" or "delete" key
    (https://docs.feldera.com/formats/json). An order-status update is encoded as
    a delete of the old row followed by an insert of the new row.

    Args:
        table_short_name: e.g. 'orders', 'order_items'
        up_to_hour: e.g. '2025-11-30T12' — include all files up to this hour (inclusive).
                    None = include all CDC files.
        target_schema: Schema from the snapshot Delta table. When provided, CDC columns
                       are cast to match (JSON serializes decimals as strings,
                       timestamps as strings, etc.).

    Returns:
        DataFrame with the inserted row state (same schema as snapshot) plus an 'op'
        column. Delete records are dropped: this demo never permanently removes a row —
        an update re-inserts the new state, which wins over the old one via the
        latest-updated_at dedup in read_snapshot_plus_cdc — so the inserted state alone
        reconstructs the current table.
    """
    cdc_path = f"{CDC_ROOT}/{table_short_name}/"

    # Read all JSON files
    raw = spark.read.json(cdc_path)

    if not raw.head(1):
        return None

    # Filter to files up to the specified hour
    if up_to_hour is not None:
        filename = F.substring_index(F.col("_metadata.file_path"), "/", -1)
        raw = raw.filter(filename <= f"{up_to_hour}.json")

    # Feldera insert_delete format wraps each change with an "insert" or "delete"
    # key: {"insert": {...}} / {"delete": {...}}. The new row state lives in the
    # "insert" field (null on delete records).
    if "insert" not in raw.columns:
        return None

    # Keep inserts (new/updated row state); drop deletes (old state being removed).
    # Expand the 'insert' struct into columns; synthesize 'op' for compatibility.
    records = raw.filter(F.col("insert").isNotNull())
    after_cols = records.select("insert.*", F.lit("insert").alias("op"))

    # Cast columns to match snapshot schema (JSON uses strings for decimals/timestamps)
    if target_schema:
        for field in target_schema.fields:
            if field.name in after_cols.columns:
                after_cols = after_cols.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )

    return after_cols


def read_snapshot_plus_cdc(table_name, up_to_hour=None):
    """Read snapshot + CDC data, merge them, and return a unified DataFrame.

    For tables with a PRIMARY KEY (orders), deduplicates by keeping the latest
    updated_at per key. For append-only tables, unions all rows.
    """
    snapshot_df = read_snapshot(table_name)

    cdc_short = CDC_TABLES.get(table_name)
    if cdc_short is None:
        return snapshot_df

    cdc_df = read_cdc_files(cdc_short, up_to_hour, target_schema=snapshot_df.schema)
    if cdc_df is None or not cdc_df.head(1):
        return snapshot_df

    # Drop the 'op' column — we only need the data
    cdc_data = cdc_df.drop("op")

    # Union snapshot + CDC
    combined = snapshot_df.unionByName(cdc_data, allowMissingColumns=True)

    # For orders (has PRIMARY KEY): dedup by order_id keeping latest updated_at
    if table_name == "bronze_orders":
        w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
        combined = (
            combined.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    return combined


def run_pipeline_with_cdc(up_to_hour=None):
    """Run the full silver -> gold pipeline using snapshot + CDC data.

    This is the batch equivalent of pushing CDC data to Feldera — except Spark
    must re-scan and recompute EVERYTHING, not just the changed rows.
    """
    global _step_times, _t0
    _step_times = []
    _t0 = time.time()
    row_counts = {}
    silver_counts = {}
    gold_counts = {}

    # Read all tables from snapshot + CDC
    timed_step("read_snapshot_plus_cdc")

    products_df = read_snapshot("bronze_products")
    suppliers_df = read_snapshot("bronze_suppliers")
    customers_df = read_snapshot("bronze_customers")

    clickstream_df = read_snapshot_plus_cdc("bronze_clickstream_events", up_to_hour)
    order_items_df = read_snapshot_plus_cdc("bronze_order_items", up_to_hour)
    inventory_df = read_snapshot_plus_cdc("bronze_inventory_events", up_to_hour)
    orders_df = read_snapshot_plus_cdc("bronze_orders", up_to_hour)

    products_df.createOrReplaceTempView("bronze_products")
    suppliers_df.createOrReplaceTempView("bronze_suppliers")
    order_items_df.createOrReplaceTempView("bronze_order_items")
    orders_df.createOrReplaceTempView("bronze_orders")

    row_counts["orders"] = orders_df.count()
    row_counts["order_items"] = order_items_df.count()
    row_counts["clickstream"] = clickstream_df.count()
    row_counts["inventory"] = inventory_df.count()

    print(f"  bronze_orders (deduped):    {row_counts['orders']:>10,} rows")
    print(f"  bronze_order_items:         {row_counts['order_items']:>10,} rows")
    print(f"  bronze_clickstream_events:  {row_counts['clickstream']:>10,} rows")
    print(f"  bronze_inventory_events:    {row_counts['inventory']:>10,} rows")

    # Silver: silver_customers
    timed_step("silver_customers")
    silver_customers = customers_df.filter(
        F.col("customer_id").isNotNull()
        & F.col("customer_tier").isin("standard", "silver", "gold", "platinum")
    )
    materialize(silver_customers, silver_path("silver_customers"))
    silver_customers.createOrReplaceTempView("silver_customers")

    # Silver: silver_products
    timed_step("silver_products")
    silver_products = spark.sql("""
        SELECT product_id, product_name, category, brand, list_price
        FROM bronze_products
        WHERE product_id IS NOT NULL
    """)
    materialize(silver_products, silver_path("silver_products"))
    silver_products.createOrReplaceTempView("silver_products")

    # Silver: silver_enriched_clickstream (clean-only; enrichment moved to gold)
    timed_step("silver_enriched_clickstream")
    clickstream_df.createOrReplaceTempView("bronze_clickstream_events")
    enriched_clickstream = spark.sql("""
        SELECT ce.event_id, ce.user_id, ce.session_id, ce.event_type, ce.page_url,
            ce.product_id, ce.device_type, ce.geo_country, ce.geo_region,
            ce.event_timestamp
        FROM bronze_clickstream_events ce
        WHERE ce.event_type IS NOT NULL AND ce.user_id IS NOT NULL
            AND ce.event_type IN ('page_view', 'product_view', 'add_to_cart', 'begin_checkout', 'purchase')
    """)
    materialize(enriched_clickstream, silver_path("silver_enriched_clickstream"))
    enriched_clickstream.createOrReplaceTempView("silver_enriched_clickstream")

    # Silver: silver_orders_enriched
    timed_step("silver_orders_enriched")
    enriched_orders = spark.sql("""
        SELECT o.order_id, o.user_id, o.order_status, o.order_total,
            o.discount_amount, o.shipping_cost, o.payment_method, o.coupon_code,
            o.created_at, o.updated_at, c.customer_tier,
            c.geo_country AS customer_country, c.geo_region AS customer_region,
            c.signup_date, oi.item_count, oi.total_quantity, oi.gross_item_revenue, oi.avg_discount_pct
        FROM bronze_orders o
        JOIN silver_customers c ON o.user_id = c.customer_id
        JOIN (
            SELECT order_id, COUNT(*) AS item_count, SUM(quantity) AS total_quantity,
                SUM(quantity * unit_price) AS gross_item_revenue, AVG(discount_pct) AS avg_discount_pct
            FROM bronze_order_items WHERE quantity > 0 AND unit_price >= 0 GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
        WHERE o.order_id IS NOT NULL AND o.user_id IS NOT NULL AND o.order_total >= 0
            AND o.order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned')
    """)
    silver_counts["silver_orders_enriched"] = materialize_and_count(
        enriched_orders, silver_path("silver_orders_enriched")
    )
    enriched_orders.createOrReplaceTempView("silver_orders_enriched")

    # Silver: silver_order_items_enriched
    timed_step("silver_order_items_enriched")
    enriched_items = spark.sql("""
        SELECT oi.order_item_id, oi.order_id, oi.product_id, oi.quantity, oi.unit_price, oi.discount_pct,
            oi.quantity * oi.unit_price AS line_gross_revenue,
            oi.quantity * oi.unit_price * (1.0 - COALESCE(oi.discount_pct, 0) / 100.0) AS line_net_revenue,
            oi.quantity * (oi.unit_price - p.cost_price) AS line_gross_margin,
            p.product_name, p.category, p.brand, p.cost_price, p.list_price,
            s.supplier_id, s.supplier_name, s.country AS supplier_country, s.lead_time_days,
            o.created_at AS order_created_at, o.order_status, o.user_id, c.customer_tier
        FROM bronze_order_items oi
        JOIN bronze_products p ON oi.product_id = p.product_id
        JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
        JOIN bronze_orders o ON oi.order_id = o.order_id
        JOIN silver_customers c ON o.user_id = c.customer_id
        WHERE oi.quantity > 0 AND oi.unit_price >= 0 AND p.is_active = TRUE
            AND o.order_status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned')
    """)
    silver_counts["silver_order_items_enriched"] = materialize_and_count(
        enriched_items, silver_path("silver_order_items_enriched")
    )
    enriched_items.createOrReplaceTempView("silver_order_items_enriched")

    # Silver: silver_confirmed_order_items
    timed_step("silver_confirmed_order_items")
    confirmed_items = spark.sql(
        "SELECT * FROM silver_order_items_enriched WHERE order_status NOT IN ('cancelled', 'returned')"
    )
    silver_counts["silver_confirmed_order_items"] = materialize_and_count(
        confirmed_items, silver_path("silver_confirmed_order_items")
    )
    confirmed_items.createOrReplaceTempView("silver_confirmed_order_items")

    # Silver: silver_inventory_current
    timed_step("silver_inventory_current")
    inventory_df.createOrReplaceTempView("bronze_inventory_events")
    full_inventory = spark.sql("""
        SELECT ie.product_id, ie.warehouse_id, p.product_name, p.category, p.brand,
            s.supplier_id, s.supplier_name, s.lead_time_days, SUM(ie.quantity_change) AS current_stock,
            SUM(CASE WHEN ie.event_type = 'restock' THEN ie.quantity_change ELSE 0 END) AS total_restocked,
            SUM(CASE WHEN ie.event_type = 'sale_reserve' THEN ABS(ie.quantity_change) ELSE 0 END)
                - SUM(CASE WHEN ie.event_type = 'cancellation_restock' THEN ie.quantity_change ELSE 0 END) AS total_sold,
            SUM(CASE WHEN ie.event_type = 'return_restock' THEN ie.quantity_change ELSE 0 END) AS total_returned
        FROM bronze_inventory_events ie
        JOIN bronze_products p ON ie.product_id = p.product_id
        JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
        WHERE ie.quantity_change IS NOT NULL AND ie.product_id IS NOT NULL
        GROUP BY ie.product_id, ie.warehouse_id, p.product_name, p.category, p.brand, s.supplier_id, s.supplier_name, s.lead_time_days
    """)
    materialize(full_inventory, silver_path("silver_inventory_current"))
    full_inventory.createOrReplaceTempView("silver_inventory_current")

    # Silver: silver_inventory_by_supplier
    timed_step("silver_inventory_by_supplier")
    sibs = spark.sql("""
        SELECT supplier_id, supplier_name, SUM(current_stock) AS total_current_stock,
            SUM(total_sold) AS total_sold, SUM(total_restocked) AS total_restocked,
            SUM(total_returned) AS total_returned
        FROM silver_inventory_current GROUP BY supplier_id, supplier_name
    """)
    materialize(sibs, silver_path("silver_inventory_by_supplier"))
    sibs.createOrReplaceTempView("silver_inventory_by_supplier")

    # Gold tables — same SQL as the non-CDC pipeline
    timed_step("gold_order_status_summary")
    gold_counts["gold_order_status_summary"] = materialize_and_count(
        spark.sql("""
        SELECT o.order_status, COUNT(DISTINCT o.order_id) AS order_count,
            COALESCE(SUM(o.order_total), 0) AS total_revenue,
            AVG(o.order_total) AS avg_order_value, COUNT(DISTINCT o.user_id) AS unique_customers
        FROM silver_orders_enriched o GROUP BY o.order_status
    """),
        gold_path("gold_order_status_summary"),
    )

    timed_step("gold_supplier_performance")
    gold_counts["gold_supplier_performance"] = materialize_and_count(
        spark.sql("""
        WITH orders_by_supplier AS (
            SELECT oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days,
                SUM(oi.quantity) AS total_units_sold, SUM(oi.line_net_revenue) AS total_net_revenue,
                SUM(oi.line_gross_margin) AS total_gross_margin,
                SUM(oi.line_gross_margin) / NULLIF(SUM(oi.line_net_revenue), 0) AS avg_margin_pct,
                AVG(oi.discount_pct) AS avg_discount_applied,
                CAST(SUM(CASE WHEN oi.order_status = 'delivered' THEN 1 ELSE 0 END) AS DOUBLE)
                    / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) AS reliability_score
            FROM silver_confirmed_order_items oi
            GROUP BY oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days
        ),
        products_by_supplier AS (
            SELECT supplier_id, COUNT(*) AS products_sold
            FROM (SELECT DISTINCT supplier_id, product_id FROM silver_confirmed_order_items)
            GROUP BY supplier_id
        ),
        orders_count_by_supplier AS (
            SELECT supplier_id, COUNT(*) AS orders_fulfilled
            FROM (SELECT DISTINCT supplier_id, order_id FROM silver_confirmed_order_items)
            GROUP BY supplier_id
        )
        SELECT oi.supplier_id, oi.supplier_name, oi.supplier_country, oi.lead_time_days,
            p.products_sold, o.orders_fulfilled,
            oi.total_units_sold, oi.total_net_revenue, oi.total_gross_margin,
            oi.avg_margin_pct, oi.avg_discount_applied, oi.reliability_score,
            inv.total_current_stock, inv.total_sold AS inventory_units_sold,
            inv.total_restocked AS inventory_units_restocked, inv.total_returned AS inventory_units_returned
        FROM orders_by_supplier oi
        JOIN silver_inventory_by_supplier inv ON oi.supplier_id = inv.supplier_id
        JOIN products_by_supplier p ON oi.supplier_id = p.supplier_id
        JOIN orders_count_by_supplier o ON oi.supplier_id = o.supplier_id
    """),
        gold_path("gold_supplier_performance"),
    )

    timed_step("gold_inventory_risk")
    inventory_risk = spark.sql("""
        SELECT inv.product_id, inv.product_name, inv.category, inv.brand,
            inv.supplier_name, inv.lead_time_days, inv.total_stock_all_warehouses,
            sales.units_sold, sales.avg_daily_units,
            CASE WHEN sales.avg_daily_units > 0 THEN inv.total_stock_all_warehouses / sales.avg_daily_units ELSE NULL END AS days_of_stock_remaining,
            CASE
                WHEN sales.avg_daily_units > 0 AND (inv.total_stock_all_warehouses / sales.avg_daily_units) < inv.lead_time_days * 1.5 THEN 'CRITICAL'
                WHEN sales.avg_daily_units > 0 AND (inv.total_stock_all_warehouses / sales.avg_daily_units) < inv.lead_time_days * 3.0 THEN 'WARNING'
                ELSE 'OK'
            END AS stock_risk_level,
            sales.net_revenue, sales.gross_margin
        FROM (
            SELECT product_id, product_name, category, brand, supplier_name, lead_time_days,
                SUM(current_stock) AS total_stock_all_warehouses
            FROM silver_inventory_current GROUP BY product_id, product_name, category, brand, supplier_name, lead_time_days
        ) inv
        JOIN (
            SELECT product_id, SUM(quantity) AS units_sold,
                SUM(quantity) / NULLIF(COUNT(DISTINCT date_trunc('day', order_created_at)), 0) AS avg_daily_units,
                SUM(line_net_revenue) AS net_revenue, SUM(line_gross_margin) AS gross_margin
            FROM silver_confirmed_order_items GROUP BY product_id
        ) sales ON inv.product_id = sales.product_id
    """)
    inventory_risk.createOrReplaceTempView("gold_inventory_risk")
    gold_counts["gold_inventory_risk"] = materialize_and_count(
        inventory_risk, gold_path("gold_inventory_risk")
    )

    timed_step("gold_realtime_inventory_alerts")
    gold_counts["gold_realtime_inventory_alerts"] = materialize_and_count(
        spark.sql(
            "SELECT * FROM gold_inventory_risk WHERE stock_risk_level = 'CRITICAL'"
        ),
        gold_path("gold_realtime_inventory_alerts"),
    )

    timed_step("gold_weekly_revenue_trend")
    gold_counts["gold_weekly_revenue_trend"] = materialize_and_count(
        spark.sql("""
        SELECT week_start, category, weekly_net_revenue, weekly_gross_margin, order_count, units_sold,
            weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start) AS revenue_wow_change,
            (weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start))
                / NULLIF(LAG(weekly_net_revenue, 1) OVER (PARTITION BY category ORDER BY week_start), 0) AS revenue_wow_pct_change,
            AVG(weekly_net_revenue) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS revenue_4wk_moving_avg,
            AVG(weekly_gross_margin) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS margin_4wk_moving_avg,
            SUM(weekly_net_revenue) OVER (PARTITION BY category, EXTRACT(YEAR FROM week_start) ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS cumulative_ytd_revenue
        FROM (
            SELECT date_trunc('week', oi.order_created_at) AS week_start, oi.category,
                SUM(oi.line_net_revenue) AS weekly_net_revenue, SUM(oi.line_gross_margin) AS weekly_gross_margin,
                COUNT(DISTINCT oi.order_id) AS order_count, SUM(oi.quantity) AS units_sold
            FROM silver_confirmed_order_items oi GROUP BY date_trunc('week', oi.order_created_at), oi.category
        )
    """),
        gold_path("gold_weekly_revenue_trend"),
    )

    timed_step("gold_cancellation_impact")
    gold_counts["gold_cancellation_impact"] = materialize_and_count(
        spark.sql("""
        SELECT category, week_start, weekly_cancelled_orders, weekly_total_orders,
            weekly_cancelled_revenue, weekly_total_revenue,
            CAST(SUM(weekly_cancelled_orders) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS DOUBLE)
                / NULLIF(CAST(SUM(weekly_total_orders) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS DOUBLE), 0) AS cumulative_cancellation_rate,
            SUM(weekly_cancelled_revenue) OVER (PARTITION BY category ORDER BY week_start RANGE UNBOUNDED PRECEDING) AS cumulative_cancelled_revenue,
            CAST(SUM(weekly_cancelled_orders) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS DOUBLE)
                / NULLIF(CAST(SUM(weekly_total_orders) OVER (PARTITION BY category ORDER BY week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING AND CURRENT ROW) AS DOUBLE), 0) AS cancellation_rate_4wk
        FROM (
            SELECT oi.category, date_trunc('week', oi.order_created_at) AS week_start,
                COUNT(DISTINCT CASE WHEN oi.order_status = 'cancelled' THEN oi.order_id END) AS weekly_cancelled_orders,
                COUNT(DISTINCT oi.order_id) AS weekly_total_orders,
                SUM(CASE WHEN oi.order_status = 'cancelled' THEN oi.line_net_revenue ELSE 0 END) AS weekly_cancelled_revenue,
                SUM(oi.line_net_revenue) AS weekly_total_revenue
            FROM silver_order_items_enriched oi GROUP BY oi.category, date_trunc('week', oi.order_created_at)
        )
    """),
        gold_path("gold_cancellation_impact"),
    )

    timed_step("gold_product_demand_surge")
    gold_counts["gold_product_demand_surge"] = materialize_and_count(
        spark.sql("""
        WITH recent_demand AS (
            -- AGGREGATE-THEN-JOIN: collapse to product grain FIRST with pure SUM
            -- counts (no MAX of attributes), then join the product dimension at
            -- product grain below. Browsing demand over a trailing 1-day window
            -- relative to the latest event in the stream (scalar subquery; not
            -- wall-clock NOW(), which is empty on a historical snapshot). The
            -- window slides as events arrive, so products enter AND leave in
            -- real time.
            SELECT
                ce.product_id,
                SUM(CASE WHEN ce.event_type = 'product_view' THEN 1 ELSE 0 END) AS recent_views,
                SUM(CASE WHEN ce.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS recent_add_to_carts,
                SUM(CASE WHEN ce.event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS recent_checkouts
            FROM silver_enriched_clickstream ce
            WHERE ce.product_id IS NOT NULL
                AND ce.event_timestamp >
                    (SELECT MAX(event_timestamp) FROM silver_enriched_clickstream) - INTERVAL 1 DAY
            GROUP BY ce.product_id
        ),
        stock_by_product AS (
            SELECT product_id,
                SUM(current_stock) AS total_stock_all_warehouses,
                MAX(lead_time_days) AS lead_time_days
            FROM silver_inventory_current
            GROUP BY product_id
        )
        SELECT
            d.product_id, p.product_name, p.category AS product_category, p.brand AS product_brand,
            d.recent_views, d.recent_add_to_carts, d.recent_checkouts,
            COALESCE(s.total_stock_all_warehouses, 0) AS total_stock_all_warehouses,
            s.lead_time_days,
            -- Days of stock cover at the current daily add-to-cart rate (1-day window
            -- => recent_add_to_carts is a per-day rate). Heuristic: clickstream has no
            -- quantity, so 1 add-to-cart ~= 1 unit of demand. Stock clamped at >= 0.
            CAST(GREATEST(COALESCE(s.total_stock_all_warehouses, 0), 0) AS DOUBLE)
                / NULLIF(CAST(d.recent_add_to_carts AS DOUBLE), 0) AS days_of_stock_cover,
            -- DEMO-ONLY: valid because the generator attaches one product_id per funnel
            -- journey; in production begin_checkout is cart-level with no product_id.
            CAST(d.recent_checkouts AS DOUBLE)
                / NULLIF(CAST(d.recent_views AS DOUBLE), 0) AS view_to_checkout_rate,
            -- Stockout risk: stock cover shorter than a supplier restock cycle.
            -- lead_time_days defaults to 7 when unknown.
            CASE
                WHEN d.recent_add_to_carts = 0 THEN 'OK'
                WHEN COALESCE(s.total_stock_all_warehouses, 0) <= 0 THEN 'SURGE_STOCKOUT_RISK'
                WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE)
                    < COALESCE(s.lead_time_days, 7) THEN 'SURGE_STOCKOUT_RISK'
                WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE)
                    < COALESCE(s.lead_time_days, 7) * 2 THEN 'WARNING'
                ELSE 'OK'
            END AS demand_alert
        FROM recent_demand d
        LEFT JOIN silver_products p ON d.product_id = p.product_id
        LEFT JOIN stock_by_product s ON d.product_id = s.product_id
    """),
        gold_path("gold_product_demand_surge"),
    )

    timed_step("done")
    total = time.time() - _t0
    step_times = []
    for i in range(len(_step_times) - 1):
        name, start = _step_times[i]
        _, end = _step_times[i + 1]
        step_times.append((name, end - start))
    return {
        "step_times": step_times,
        "total_time": total,
        "row_counts": row_counts,
        "silver_counts": silver_counts,
        "gold_counts": gold_counts,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run CDC Comparison

# COMMAND ----------

# CDC comparison: advance the lakehouse ONE HOUR at a time. Each hourly batch
# full-refreshes the entire silver/gold layer from snapshot + all CDC up to that
# hour, so the per-hour cost stays roughly constant and large — no matter that
# only a few hundred rows changed that hour. That's the batch tax: cost tracks
# total dataset size, not change size. (Compare push_changes.py --hour, where
# Feldera applies the same one-hour change incrementally in milliseconds.)
CDC_FIRST_DAY = "2025-11-30"
CDC_NUM_HOURS = 6  # number of consecutive hourly batches to time; raise to extend
CDC_HOURS = [f"{CDC_FIRST_DAY}T{h:02d}" for h in range(CDC_NUM_HOURS)]

cdc_results = []
for hour in CDC_HOURS:
    print(f"\n{'=' * 70}")
    print(f"  HOURLY BATCH: state through {hour}")
    print(f"{'=' * 70}\n")
    result = run_pipeline_with_cdc(up_to_hour=hour)
    cdc_results.append((hour, result))
    print(f"\n  {hour}: {result['total_time']:.1f}s\n")

# Print comparison table
print(f"\n{'=' * 70}")
print("  PER-HOUR BATCH COST — Spark Full-Refresh Pipeline")
print(f"{'=' * 70}\n")
header = f"  {'Through hour':<16} {'Orders':>9} {'Items':>9} {'Clicks':>9} {'Inv Evts':>9} {'Time':>8}"
print(header)
print("  " + "-" * (len(header) - 2))
for hour, r in cdc_results:
    rc = r["row_counts"]
    print(
        f"  {hour:<16} {rc['orders']:>9,} {rc['order_items']:>9,}"
        f" {rc['clickstream']:>9,} {rc['inventory']:>9,} {r['total_time']:>7.1f}s"
    )

# Silver detail-grain row counts after each hourly batch — these tables add one row
# per order / order item, so they climb as CDC arrives. Read against the flat gold
# aggregates below: data IS changing (silver grows), but aggregate cardinality is
# stable (gold counts are pinned to fixed dimension grains).
print(f"\n{'=' * 70}")
print(f"  SILVER DETAIL ROW COUNTS — per hour ({CDC_FIRST_DAY})")
print(f"{'=' * 70}\n")
SILVER_DETAIL = [
    "silver_orders_enriched",
    "silver_order_items_enriched",
    "silver_confirmed_order_items",
]
s_hdr = f"  {'silver table':<34}" + "".join(
    f"{h.split('T')[-1]:>9}" for h, _ in cdc_results
)
print(s_hdr)
print("  " + "-" * (len(s_hdr) - 2))
for v in SILVER_DETAIL:
    cells = "".join(f"{r['silver_counts'].get(v, 0):>9,}" for _, r in cdc_results)
    print(f"  {v:<34}{cells}")
print(
    "\n  (detail-grain tables grow as orders/items arrive — contrast with the"
    " flat gold aggregates below)"
)

# Gold view row counts after each hourly batch — shows how each materialized view
# grows/shifts as change data is applied (e.g. CRITICAL alerts entering and
# leaving, status mix shifting). Feldera maintains these same counts incrementally
# per commit instead of recomputing them from scratch each batch.
print(f"\n{'=' * 70}")
print(f"  GOLD VIEW ROW COUNTS — per hour ({CDC_FIRST_DAY})")
print(f"{'=' * 70}\n")
GOLD_VIEWS = [
    "gold_order_status_summary",
    "gold_supplier_performance",
    "gold_inventory_risk",
    "gold_realtime_inventory_alerts",
    "gold_weekly_revenue_trend",
    "gold_cancellation_impact",
    "gold_product_demand_surge",
]
gold_hdr = f"  {'gold view':<34}" + "".join(
    f"{h.split('T')[-1]:>9}" for h, _ in cdc_results
)
print(gold_hdr)
print("  " + "-" * (len(gold_hdr) - 2))
for v in GOLD_VIEWS:
    cells = "".join(f"{r['gold_counts'].get(v, 0):>9,}" for _, r in cdc_results)
    print(f"  {v:<34}{cells}")

print(f"""
{"=" * 70}
WHAT THIS SHOWS
{"=" * 70}

Each row advances the lakehouse by ONE hour of CDC, yet every run re-scanned the
ENTIRE dataset (snapshot + all CDC to date) and recomputed all silver/gold views
from scratch. The per-hour time is roughly constant and large — even though only
a few hundred rows changed each hour. Batch cost tracks TOTAL dataset size, not
change size, so it does NOT shrink as the hourly change shrinks.

Feldera applies the same one-hour change incrementally — only the changed rows
propagate through the views, in milliseconds. Cost is proportional to CHANGE
SIZE, not total size.

Run the Feldera side, one hour at a time:
  python push_changes.py --hour 2025-11-30T00
{"=" * 70}
""")
