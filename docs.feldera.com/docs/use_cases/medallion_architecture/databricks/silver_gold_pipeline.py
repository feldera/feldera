# Databricks notebook source
# MAGIC %md
# MAGIC # Silver & Gold — Databricks Materialized Views (Lakeflow Declarative Pipeline)
# MAGIC
# MAGIC The silver and gold layers from `compute_silver_gold.py`, expressed as a Lakeflow
# MAGIC Declarative Pipeline. Each silver/gold dataset is a **materialized view** that
# MAGIC Databricks refreshes incrementally where the query allows (Enzyme), falling back
# MAGIC to a full recompute otherwise.
# MAGIC
# MAGIC - **Bronze** is exposed as lightweight `@dp.temporary_view`s that read the working
# MAGIC   bronze Delta tables on S3 (written by `ingest_bronze.py`). They are not persisted —
# MAGIC   the bronze tables already live in S3.
# MAGIC - **Silver / Gold** are `@dp.materialized_view`s, persisted in the pipeline's Unity
# MAGIC   Catalog target schema.
# MAGIC
# MAGIC The business logic (SQL) is identical to the batch script and to the Feldera views
# MAGIC in `ecommerce/sql/ecommerce.sql`. The workflow refreshes this pipeline as its second
# MAGIC step with a single normal refresh: it updates incrementally after a CDC hour, and
# MAGIC Enzyme automatically full-recomputes after a `clean_start` rewrites bronze.
# MAGIC
# MAGIC **Feldera equivalent:** the same DAG, maintained incrementally on every commit with
# MAGIC no pipeline scheduling and no full-refresh fallback.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# scale_factor is supplied as pipeline configuration; it selects the bronze S3 path the
# ingest step wrote to. Defaults to 0.01 to match the rest of the demo.
SCALE_FACTOR = float(spark.conf.get("scale_factor", "0.01"))
SCALE_STR = str(SCALE_FACTOR).replace(".", "-")

WAREHOUSE_BUCKET = spark.conf.get("warehouse_bucket")
BRONZE_ROOT = f"{WAREHOUSE_BUCKET}/ecommerce-pipeline-{SCALE_STR}/bronze".replace(
    "s3://", "s3a://"
)

# The silver/gold materialized views publish to the pipeline's configured catalog/schema
# (set on the pipeline when it's created — see pipeline.yaml catalog/schema). Datasets use
# bare names and resolve within that destination.


def bronze_path(name: str) -> str:
    return f"{BRONZE_ROOT}/{name}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze source views
# MAGIC
# MAGIC Pass-through views over the working bronze Delta tables on S3. Declaring them as
# MAGIC pipeline datasets lets the silver/gold materialized views reference them by name
# MAGIC (in `spark.sql`) and lets Enzyme reason about their changes.

# COMMAND ----------

_BRONZE = [
    "bronze_suppliers",
    "bronze_products",
    "bronze_customers",
    "bronze_orders",
    "bronze_order_items",
    "bronze_clickstream_events",
    "bronze_inventory_events",
]


def _make_bronze_view(table_name):
    @dp.temporary_view(name=table_name)
    def _view():
        return spark.read.format("delta").load(bronze_path(table_name))

    return _view


for _name in _BRONZE:
    _make_bronze_view(_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver layer
# MAGIC
# MAGIC Each function runs the exact SQL from `compute_silver_gold.py`, referencing its
# MAGIC upstream pipeline datasets directly by name — Lakeflow resolves those names against
# MAGIC the pipeline graph and registers them as dependencies, so the logic stays in
# MAGIC lock-step with the batch script and the Feldera views.

# COMMAND ----------


@dp.materialized_view(
    name="silver_customers",
    comment="Cleaned customer dimension (valid tiers).",
    refresh_policy="incremental",
)
def silver_customers():
    return spark.read.table("bronze_customers").filter(
        F.col("customer_id").isNotNull()
        & F.col("customer_tier").isin("standard", "silver", "gold", "platinum")
    )


@dp.materialized_view(
    name="silver_products",
    comment="Cleaned product dimension (product grain).",
    refresh_policy="incremental",
)
def silver_products():
    return spark.sql("""
        SELECT product_id, product_name, category, brand, list_price
        FROM bronze_products
        WHERE product_id IS NOT NULL
    """)


@dp.materialized_view(
    name="silver_enriched_clickstream",
    comment="Clickstream filtered to interaction events (clean-only).",
    refresh_policy="incremental",
)
def silver_enriched_clickstream():
    return spark.sql("""
        SELECT
            ce.event_id, ce.user_id, ce.session_id, ce.event_type, ce.page_url,
            ce.product_id, ce.device_type, ce.geo_country, ce.geo_region,
            ce.event_timestamp
        FROM bronze_clickstream_events ce
        WHERE ce.event_type IS NOT NULL
            AND ce.user_id IS NOT NULL
            AND ce.event_type IN ('page_view', 'product_view', 'add_to_cart', 'begin_checkout', 'purchase')
    """)


@dp.materialized_view(
    name="silver_orders_enriched",
    comment="Orders joined to customers + aggregated order items.",
    refresh_policy="incremental",
)
def silver_orders_enriched():
    return spark.sql("""
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


@dp.materialized_view(
    name="silver_order_items_enriched",
    comment="Order items joined to products, suppliers, orders, customers with margins.",
    refresh_policy="incremental",
)
def silver_order_items_enriched():
    return spark.sql("""
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


@dp.materialized_view(
    name="silver_confirmed_order_items",
    comment="Order items excluding cancelled/returned orders.",
    refresh_policy="incremental",
)
def silver_confirmed_order_items():
    return spark.sql("""
        SELECT * FROM silver_order_items_enriched
        WHERE order_status NOT IN ('cancelled', 'returned')
    """)


@dp.materialized_view(
    name="silver_inventory_current",
    comment="Running stock per product/warehouse from inventory events.",
    refresh_policy="incremental",
)
def silver_inventory_current():
    return spark.sql("""
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


@dp.materialized_view(
    name="silver_inventory_by_supplier",
    comment="Stock rolled up to supplier grain.",
    refresh_policy="incremental",
)
def silver_inventory_by_supplier():
    return spark.sql("""
        SELECT supplier_id, supplier_name,
            SUM(current_stock) AS total_current_stock,
            SUM(total_sold) AS total_sold,
            SUM(total_restocked) AS total_restocked,
            SUM(total_returned) AS total_returned
        FROM silver_inventory_current
        GROUP BY supplier_id, supplier_name
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold layer

# COMMAND ----------


@dp.materialized_view(
    name="gold_order_status_summary",
    comment="Order status distribution.",
    refresh_policy="incremental",
)
def gold_order_status_summary():
    return spark.sql("""
        SELECT
            o.order_status,
            COUNT(DISTINCT o.order_id) AS order_count,
            COALESCE(SUM(o.order_total), 0) AS total_revenue,
            AVG(o.order_total) AS avg_order_value,
            COUNT(DISTINCT o.user_id) AS unique_customers
        FROM silver_orders_enriched o
        GROUP BY o.order_status
    """)


@dp.materialized_view(
    name="gold_supplier_performance",
    comment="Revenue + margin per supplier.",
    refresh_policy="incremental",
)
def gold_supplier_performance():
    return spark.sql("""
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
    """)


@dp.materialized_view(
    name="gold_inventory_risk",
    comment="Stock risk scoring per product.",
    refresh_policy="incremental",
)
def gold_inventory_risk():
    return spark.sql("""
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
                SUM(quantity) / NULLIF(COUNT(DISTINCT date_trunc('day', order_created_at)), 0) AS avg_daily_units,
                SUM(line_net_revenue) AS net_revenue,
                SUM(line_gross_margin) AS gross_margin
            FROM silver_confirmed_order_items
            GROUP BY product_id
        ) sales ON inv.product_id = sales.product_id
    """)


@dp.materialized_view(
    name="gold_realtime_inventory_alerts",
    comment="CRITICAL stock filter.",
    refresh_policy="incremental",
)
def gold_realtime_inventory_alerts():
    return spark.sql(
        "SELECT * FROM gold_inventory_risk WHERE stock_risk_level = 'CRITICAL'"
    )


@dp.materialized_view(
    name="gold_weekly_revenue_trend",
    comment="Weekly revenue with WoW change, moving avg, YTD (window functions).",
    refresh_policy="incremental",
)
def gold_weekly_revenue_trend():
    return spark.sql("""
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
    """)


@dp.materialized_view(
    name="gold_cancellation_impact",
    comment="Cancellation rate windows (running totals).",
    refresh_policy="incremental",
)
def gold_cancellation_impact():
    return spark.sql("""
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
    """)


@dp.materialized_view(
    name="gold_product_demand_surge",
    comment="Trailing-24h cart velocity vs stock & lead time (sliding window).",
    refresh_policy="auto",
)
def gold_product_demand_surge():
    return spark.sql("""
        WITH recent_demand AS (
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
            CAST(GREATEST(COALESCE(s.total_stock_all_warehouses, 0), 0) AS DOUBLE)
                / NULLIF(CAST(d.recent_add_to_carts AS DOUBLE), 0) AS days_of_stock_cover,
            CAST(d.recent_checkouts AS DOUBLE)
                / NULLIF(CAST(d.recent_views AS DOUBLE), 0) AS view_to_checkout_rate,
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
    """)
