-- Use Case: Maintaining a Medallion Architecture in Real Time (ecommerce-medallion-architecture)
--
-- Update your most complex analytical views as soon as data arrives in the bronze layer.
--
/*
 ## Detailed Description

 Instead of recomputing silver and gold layers on a schedule, Feldera will maintain everything incrementally, using a fraction of the compute required by batch engines.

 ### Bronze — Raw Ingestion (`CREATE TABLE`)
 Upsert tables that mirror the source systems with no transformation. Each is fed by a Delta snapshot connector and then kept current by the CDC stream.

 | Table | Description |
 |-------|-------------|
 | `bronze_clickstream_events` | Page views, sessions, and product interactions |
 | `bronze_orders` | Order headers with status, totals, payment, and coupons |
 | `bronze_order_items` | Order line items (product, quantity, unit price, discount) |
 | `bronze_products` | Product catalog with cost and list pricing |
 | `bronze_inventory_events` | Inventory movements (restock, sale_reserve, returns) |
 | `bronze_customers` | Customer dimension with tier and geography |
 | `bronze_suppliers` | Supplier dimension with lead times |

 ### Silver — Cleaned, Validated, Enriched (`LOCAL VIEW`)
 All cleaning, validation, deduplication, and joins happen here so that Gold never references Bronze directly.

 - `silver_customers` — validated customer dimension (known tiers only)
 - `silver_orders_enriched` — orders joined to customers and aggregated line-item metrics
 - `silver_order_items_enriched` — line items enriched with product, supplier, and customer context; computes line-level revenue and margin
 - `silver_confirmed_order_items` — line items excluding cancelled/returned orders
 - `silver_inventory_current` — running per-product/per-warehouse stock from cumulative inventory events
 - `silver_inventory_by_supplier` — supplier-level inventory rollup

 ### Gold — Business Metrics & Analytics (`MATERIALIZED VIEW`)
 Pure aggregation over Silver — no filtering, no Bronze references. These are the views a BI tool or dashboard consumes.

 - `gold_supplier_performance` — revenue, margin, reliability, and inventory by supplier
 - `gold_inventory_risk` — days-of-stock vs. lead time, scored CRITICAL / WARNING / OK
 - `gold_order_status_summary` — order count and revenue by status (changes visibly with every CDC commit)
 - `gold_weekly_revenue_trend` — weekly revenue with WoW change, 4-week moving average, and cumulative YTD
 - `gold_cancellation_impact` — cancellation rates with cumulative and 4-week moving windows
 - `gold_realtime_inventory_alerts` — filtered stream of CRITICAL inventory items

 ### Change Data
 Push Changes to the Pipeline in the UI
 In the adhoc queries pane, run the following queries.
 select count(*) from gold_realtime_inventory_alerts;

 # change a lead time to a huge value to generate more alerts
 INSERT INTO bronze_suppliers VALUES (
 7, 'Blake and Sons', 'DE', 10000, now()
 )

 # immediately run to show that the gold view updated in milliseconds
 select count(*) from gold_realtime_inventory_alerts;

 Push CDC Data
 Follow the demo documentation here (https://docs.feldera.com/use_cases/medallion_architecture/part1) to push a higher volume of changes to tables programmatically.

============================================================================
 E-COMMERCE REAL-TIME MEDALLION ARCHITECTURE
 Feldera Incremental SQL Pipeline
 ============================================================================
 ============================================================================
 BRONZE LAYER — Raw Ingestion Tables
 Raw data, no transformations
 ============================================================================
 */

CREATE TABLE bronze_clickstream_events (
    event_id BIGINT NOT NULL PRIMARY KEY,
    user_id BIGINT,
    session_id VARCHAR,
    event_type VARCHAR,
    page_url VARCHAR,
    product_id BIGINT,
    referrer VARCHAR,
    device_type VARCHAR,
    geo_country VARCHAR,
    geo_region VARCHAR,
    -- LATENESS bounds in-memory state: events older than (max event - 1h) are
    -- considered complete, letting Feldera GC history that the trailing-1-day
    -- demand window can no longer change. The CDC generator buckets events into
    -- hourly files (split_into_hours), so intra-batch disorder is <= ~1h. Raise
    -- this if you ever see late clickstream events being dropped.
    event_timestamp TIMESTAMP NOT NULL LATENESS INTERVAL '1' HOUR,
    ingested_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_clickstream_events",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_orders (
    order_id BIGINT NOT NULL PRIMARY KEY,
    user_id BIGINT,
    order_status VARCHAR,
    order_total DECIMAL(12, 2),
    discount_amount DECIMAL(12, 2),
    shipping_cost DECIMAL(12, 2),
    payment_method VARCHAR,
    coupon_code VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_orders",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_order_items (
    order_item_id BIGINT NOT NULL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_pct DECIMAL(5, 2),
    ingested_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_order_items",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_products (
    product_id BIGINT NOT NULL PRIMARY KEY,
    product_name VARCHAR NOT NULL,
    category VARCHAR,
    brand VARCHAR,
    supplier_id BIGINT,
    list_price DECIMAL(10, 2),
    cost_price DECIMAL(10, 2),
    weight_kg DECIMAL(6, 2),
    is_active BOOLEAN,
    updated_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '
    [{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_products",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_inventory_events (
    inventory_event_id BIGINT NOT NULL PRIMARY KEY,
    product_id BIGINT NOT NULL,
    warehouse_id BIGINT NOT NULL,
    event_type VARCHAR NOT NULL,
    quantity_change INT NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_inventory_events",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_customers (
    customer_id BIGINT NOT NULL PRIMARY KEY,
    email VARCHAR,
    signup_date DATE,
    customer_tier VARCHAR,
    geo_country VARCHAR,
    geo_region VARCHAR,
    updated_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_customers",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

CREATE TABLE bronze_suppliers (
    supplier_id BIGINT NOT NULL PRIMARY KEY,
    supplier_name VARCHAR NOT NULL,
    country VARCHAR,
    lead_time_days INT,
    updated_at TIMESTAMP NOT NULL
) WITH (
    'skip_unused_columns' = 'true',
    'connectors' = '[{
        "transport": {
            "name": "delta_table_input",
            "config": {
                "uri": "s3://feldera-demos/ecommerce-cdc-0-01/snapshot/bronze_suppliers",
                "mode": "snapshot",
"aws_skip_signature": "true",
"aws_region": "us-west-1",
"transaction_mode": "catchup"
            }
        }
    }]'
);

-- ============================================================================
-- SILVER LAYER — Cleaned, Validated, Enriched Views
-- All cleaning, validation, deduplication, and joins happen here.
-- ============================================================================
-- ----------------------------------------------------------------------------
-- silver_customers
-- Cleaned customer dimension. All downstream views reference this
-- instead of bronze_customers to ensure gold never touches bronze.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_customers AS
SELECT
    customer_id,
    email,
    signup_date,
    customer_tier,
    geo_country,
    geo_region,
    updated_at
FROM
    bronze_customers
WHERE
    customer_id IS NOT NULL
    AND customer_tier IN ('standard', 'silver', 'gold', 'platinum');

-- ----------------------------------------------------------------------------
-- silver_products
-- Cleaned product dimension at product grain (one row per product_id). Gold
-- views join this instead of bronze_products so gold never touches bronze.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_products AS
SELECT
    product_id,
    product_name,
    category,
    brand,
    list_price
FROM
    bronze_products
WHERE
    product_id IS NOT NULL;

-- ----------------------------------------------------------------------------
-- silver_enriched_clickstream
-- Cleaned clickstream events -- restricts to known interaction event types and
-- non-null users. Dimension enrichment (product via bronze_products, customer
-- via silver_customers) was REMOVED: gold_product_demand_surge now aggregates to
-- product grain FIRST and joins silver_products afterward, so attaching product/
-- customer attributes per event was wasted incremental work -- an event-grain
-- join feeding an aggregation that immediately collapsed the cardinality, plus a
-- MAX(string) per attribute. Re-add a join here only if a consumer genuinely
-- needs event-grain enriched rows. (Name kept for stability; now clean-only.)
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_enriched_clickstream AS
SELECT
    ce.event_id,
    ce.user_id,
    ce.session_id,
    ce.event_type,
    ce.page_url,
    ce.product_id,
    ce.device_type,
    ce.geo_country,
    ce.geo_region,
    ce.event_timestamp
FROM
    bronze_clickstream_events ce
WHERE
    ce.event_type IS NOT NULL
    AND ce.user_id IS NOT NULL
    AND ce.event_type IN (
        'page_view',
        'product_view',
        'add_to_cart',
        'begin_checkout',
        'purchase'
    );

-- ----------------------------------------------------------------------------
-- silver_orders_enriched
-- Orders joined with customer profile and aggregated line-item metrics.
-- Validates order_total >= 0 and restricts to known statuses.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_orders_enriched AS
SELECT
    o.order_id,
    o.user_id,
    o.order_status,
    o.order_total,
    o.discount_amount,
    o.shipping_cost,
    o.payment_method,
    o.coupon_code,
    o.created_at,
    o.updated_at,
    c.customer_tier,
    c.geo_country AS customer_country,
    c.geo_region AS customer_region,
    c.signup_date,
    oi.item_count,
    oi.total_quantity,
    oi.gross_item_revenue,
    oi.avg_discount_pct
FROM
    bronze_orders o
    JOIN silver_customers c ON o.user_id = c.customer_id
    JOIN (
        SELECT
            order_id,
            COUNT(*) AS item_count,
            SUM(quantity) AS total_quantity,
            SUM(quantity * unit_price) AS gross_item_revenue,
            AVG(discount_pct) AS avg_discount_pct
        FROM
            bronze_order_items
        WHERE
            quantity > 0
            AND unit_price >= 0
        GROUP BY
            order_id
    ) oi ON o.order_id = oi.order_id
WHERE
    o.order_id IS NOT NULL
    AND o.user_id IS NOT NULL
    AND o.order_total >= 0
    AND o.order_status IN (
        'pending',
        'confirmed',
        'shipped',
        'delivered',
        'cancelled',
        'returned'
    );

-- ----------------------------------------------------------------------------
-- silver_order_items_enriched
-- Line items enriched with product, supplier, order, and customer context.
-- Computes line-level revenue and margin. Filters inactive products.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_order_items_enriched AS
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.discount_pct,
    oi.quantity * oi.unit_price AS line_gross_revenue,
    oi.quantity * oi.unit_price * (1.0 - COALESCE(oi.discount_pct, 0) / 100.0) AS line_net_revenue,
    oi.quantity * (oi.unit_price - p.cost_price) AS line_gross_margin,
    p.product_name,
    p.category,
    p.brand,
    p.cost_price,
    p.list_price,
    s.supplier_id,
    s.supplier_name,
    s.country AS supplier_country,
    s.lead_time_days,
    o.created_at AS order_created_at,
    o.order_status,
    o.user_id,
    c.customer_tier
FROM
    bronze_order_items oi
    JOIN bronze_products p ON oi.product_id = p.product_id
    JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
    JOIN bronze_orders o ON oi.order_id = o.order_id
    JOIN silver_customers c ON o.user_id = c.customer_id
WHERE
    oi.quantity > 0
    AND oi.unit_price >= 0
    AND p.is_active = TRUE
    AND o.order_status IN (
        'pending',
        'confirmed',
        'shipped',
        'delivered',
        'cancelled',
        'returned'
    );

-- ----------------------------------------------------------------------------
-- silver_confirmed_order_items
-- Subset of order items excluding cancelled and returned orders.
-- Business rule filtering lives here so gold only aggregates.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_confirmed_order_items AS
SELECT
    *
FROM
    silver_order_items_enriched
WHERE
    order_status NOT IN ('cancelled', 'returned');

-- ----------------------------------------------------------------------------
-- silver_inventory_current
-- Running inventory position per product per warehouse.
-- Computed from cumulative sum of inventory events.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_inventory_current AS
SELECT
    ie.product_id,
    ie.warehouse_id,
    p.product_name,
    p.category,
    p.brand,
    s.supplier_id,
    s.supplier_name,
    s.lead_time_days,
    SUM(ie.quantity_change) AS current_stock,
    SUM(
        CASE
            WHEN ie.event_type = 'restock' THEN ie.quantity_change
            ELSE 0
        END
    ) AS total_restocked,
    SUM(
        CASE
            WHEN ie.event_type = 'sale_reserve' THEN ABS(ie.quantity_change)
            ELSE 0
        END
    ) - SUM(
        CASE
            WHEN ie.event_type = 'cancellation_restock' THEN ie.quantity_change
            ELSE 0
        END
    ) AS total_sold,
    SUM(
        CASE
            WHEN ie.event_type = 'return_restock' THEN ie.quantity_change
            ELSE 0
        END
    ) AS total_returned
FROM
    bronze_inventory_events ie
    JOIN bronze_products p ON ie.product_id = p.product_id
    JOIN bronze_suppliers s ON p.supplier_id = s.supplier_id
WHERE
    ie.quantity_change IS NOT NULL
    AND ie.product_id IS NOT NULL
GROUP BY
    ie.product_id,
    ie.warehouse_id,
    p.product_name,
    p.category,
    p.brand,
    s.supplier_id,
    s.supplier_name,
    s.lead_time_days;

-- ----------------------------------------------------------------------------
-- silver_inventory_by_supplier
-- Supplier-level inventory rollup. Keeps gold views free of inline subqueries.
-- ----------------------------------------------------------------------------
CREATE LOCAL VIEW silver_inventory_by_supplier AS -- Grain is supplier_id, not supplier_name: two suppliers can share a name, and
-- rolling up by name alone would merge their inventory into one row. The name
-- is carried along (functionally dependent on the id) purely for display.
SELECT
    supplier_id,
    supplier_name,
    SUM(current_stock) AS total_current_stock,
    SUM(total_sold) AS total_sold,
    SUM(total_restocked) AS total_restocked,
    SUM(total_returned) AS total_returned
FROM
    silver_inventory_current
GROUP BY
    supplier_id,
    supplier_name;

-- ============================================================================
-- GOLD LAYER — Business Metrics & Analytics
-- Pure aggregation over silver views. No filtering, no bronze references.
-- ============================================================================
-- ----------------------------------------------------------------------------
-- gold_supplier_performance
-- Supplier-level metrics combining order revenue/margin with inventory.
-- ----------------------------------------------------------------------------
-- supplier_id is the grain throughout. Grouping on (supplier_name,
-- supplier_country, lead_time_days) instead would silently merge two distinct
-- suppliers that happen to share that tuple into a single row. Every CTE keys
-- off supplier_id and joins on supplier_id alone; the descriptive columns
-- (name, country, lead time) are functionally dependent on the id and ride
-- along only for display.
CREATE MATERIALIZED VIEW gold_supplier_performance AS with orders_by_supplier as (
    select
        oi.supplier_id,
        oi.supplier_name,
        oi.supplier_country,
        oi.lead_time_days,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.line_net_revenue) AS total_net_revenue,
        SUM(oi.line_gross_margin) AS total_gross_margin,
        SUM(oi.line_gross_margin) / NULLIF(SUM(oi.line_net_revenue), 0) AS avg_margin_pct,
        AVG(CAST(oi.discount_pct AS DECIMAL(38, 6))) AS avg_discount_applied,
        CAST(
            SUM(
                CASE
                    WHEN oi.order_status = 'delivered' THEN 1
                    ELSE 0
                END
            ) AS DOUBLE
        ) / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) AS reliability_score
    from
        silver_confirmed_order_items oi
    group by
        oi.supplier_id,
        oi.supplier_name,
        oi.supplier_country,
        oi.lead_time_days
),
products_by_supplier as (
    select
        supplier_id,
        COUNT(*) AS products_sold
    from
        (
            select
                distinct supplier_id,
                product_id
            from
                silver_confirmed_order_items
        )
    group by
        supplier_id
),
orders_count_by_supplier as (
    select
        supplier_id,
        COUNT(*) AS orders_fulfilled
    from
        (
            select
                distinct supplier_id,
                order_id
            from
                silver_confirmed_order_items
        )
    group by
        supplier_id
)
SELECT
    oi.supplier_id,
    oi.supplier_name,
    oi.supplier_country,
    oi.lead_time_days,
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
FROM
    orders_by_supplier oi
    JOIN silver_inventory_by_supplier inv ON oi.supplier_id = inv.supplier_id
    JOIN products_by_supplier p ON oi.supplier_id = p.supplier_id
    JOIN orders_count_by_supplier o ON oi.supplier_id = o.supplier_id;

-- ----------------------------------------------------------------------------
-- gold_inventory_risk
-- Stock risk scoring: compares days of stock remaining against supplier
-- lead time to flag CRITICAL / WARNING / OK products.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_inventory_risk AS
SELECT
    inv.product_id,
    inv.product_name,
    inv.category,
    inv.brand,
    inv.supplier_name,
    inv.lead_time_days,
    inv.total_stock_all_warehouses,
    sales.units_sold,
    sales.avg_daily_units,
    CASE
        WHEN sales.avg_daily_units > 0 THEN inv.total_stock_all_warehouses / sales.avg_daily_units
        ELSE NULL
    END AS days_of_stock_remaining,
    CASE
        WHEN sales.avg_daily_units > 0
        AND (
            inv.total_stock_all_warehouses / sales.avg_daily_units
        ) < inv.lead_time_days * 1.5 THEN 'CRITICAL'
        WHEN sales.avg_daily_units > 0
        AND (
            inv.total_stock_all_warehouses / sales.avg_daily_units
        ) < inv.lead_time_days * 3.0 THEN 'WARNING'
        ELSE 'OK'
    END AS stock_risk_level,
    sales.net_revenue,
    sales.gross_margin
FROM
    (
        SELECT
            product_id,
            product_name,
            category,
            brand,
            supplier_name,
            lead_time_days,
            SUM(current_stock) AS total_stock_all_warehouses
        FROM
            silver_inventory_current
        GROUP BY
            product_id,
            product_name,
            category,
            brand,
            supplier_name,
            lead_time_days
    ) inv
    JOIN (
        SELECT
            product_id,
            SUM(quantity) AS units_sold,
            -- avg_daily_units must be a genuine per-day rate. SUM(quantity) spans
            -- the entire history of silver_confirmed_order_items (there is no date
            -- filter), so dividing by a hard-coded 30 is only correct when the data
            -- happens to hold exactly 30 days of orders: with 90 days it overstates
            -- the rate 3x, with 10 days it understates it 3x, which miscalibrates
            -- the CRITICAL / WARNING / OK scoring below against a fictitious
            -- denominator. Dividing by the number of distinct days actually observed
            -- yields a correct rate regardless of the dataset's span or age.
            -- (The alternative -- WHERE order_created_at > NOW() - INTERVAL '30' DAY,
            -- then divide by 30 -- is avoided here because the demo loads a historical
            -- snapshot whose orders may all predate a 30-day window, which would leave
            -- this view, and the alerts derived from it, empty.)
            CAST(SUM(quantity) AS DECIMAL(38, 6)) / NULLIF(
                COUNT(DISTINCT DATE_TRUNC(order_created_at, DAY)),
                0
            ) AS avg_daily_units,
            SUM(line_net_revenue) AS net_revenue,
            SUM(line_gross_margin) AS gross_margin
        FROM
            silver_confirmed_order_items
        GROUP BY
            product_id
    ) sales ON inv.product_id = sales.product_id;

-- ----------------------------------------------------------------------------
-- gold_order_status_summary
-- Status distribution across all orders. Changes visibly with every MERGE
-- commit that transitions order statuses (the core incremental demo).
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_order_status_summary AS
SELECT
    o.order_status,
    COUNT(DISTINCT o.order_id) AS order_count,
    COALESCE(SUM(o.order_total), 0) AS total_revenue,
    AVG(CAST(o.order_total AS DECIMAL(38, 6))) AS avg_order_value,
    COUNT(DISTINCT o.user_id) AS unique_customers
FROM
    silver_orders_enriched o
GROUP BY
    o.order_status;

-- ----------------------------------------------------------------------------
-- gold_weekly_revenue_trend
-- Revenue time-series with window functions: week-over-week change, 4-week
-- moving average, and cumulative YTD. When orders are cancelled via MERGE,
-- historical weeks' revenue decreases and all window computations cascade.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_weekly_revenue_trend AS
SELECT
    week_start,
    category,
    weekly_net_revenue,
    weekly_gross_margin,
    order_count,
    units_sold,
    weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (
        PARTITION BY category
        ORDER BY
            week_start
    ) AS revenue_wow_change,
    (
        weekly_net_revenue - LAG(weekly_net_revenue, 1) OVER (
            PARTITION BY category
            ORDER BY
                week_start
        )
    ) / NULLIF(
        LAG(weekly_net_revenue, 1) OVER (
            PARTITION BY category
            ORDER BY
                week_start
        ),
        0
    ) AS revenue_wow_pct_change,
    AVG(CAST(weekly_net_revenue AS DECIMAL(38, 6))) OVER (
        PARTITION BY category
        ORDER BY
            week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING
            AND CURRENT ROW
    ) AS revenue_4wk_moving_avg,
    AVG(CAST(weekly_gross_margin AS DECIMAL(38, 6))) OVER (
        PARTITION BY category
        ORDER BY
            week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING
            AND CURRENT ROW
    ) AS margin_4wk_moving_avg,
    SUM(weekly_net_revenue) OVER (
        PARTITION BY category,
        EXTRACT(
            YEAR
            FROM
                week_start
        )
        ORDER BY
            week_start RANGE UNBOUNDED PRECEDING
    ) AS cumulative_ytd_revenue
FROM
    (
        SELECT
            (
                DATE_TRUNC(oi.order_created_at, DAY) - (
                    CAST(
                        EXTRACT(
                            ISODOW
                            FROM
                                oi.order_created_at
                        ) AS INTEGER
                    ) - 1
                ) * INTERVAL '1' DAY
            ) AS week_start,
            oi.category,
            SUM(oi.line_net_revenue) AS weekly_net_revenue,
            SUM(oi.line_gross_margin) AS weekly_gross_margin,
            COUNT(DISTINCT oi.order_id) AS order_count,
            SUM(oi.quantity) AS units_sold
        FROM
            silver_confirmed_order_items oi
        GROUP BY
            (
                DATE_TRUNC(oi.order_created_at, DAY) - (
                    CAST(
                        EXTRACT(
                            ISODOW
                            FROM
                                oi.order_created_at
                        ) AS INTEGER
                    ) - 1
                ) * INTERVAL '1' DAY
            ),
            oi.category
    );

-- ----------------------------------------------------------------------------
-- gold_cancellation_impact
-- Cancellation rates with cumulative and 4-week moving windows. Each MERGE
-- that moves orders to cancelled shifts the running totals incrementally.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_cancellation_impact AS
SELECT
    category,
    week_start,
    weekly_cancelled_orders,
    weekly_total_orders,
    weekly_cancelled_revenue,
    weekly_total_revenue,
    CAST(
        SUM(weekly_cancelled_orders) OVER (
            PARTITION BY category
            ORDER BY
                week_start RANGE UNBOUNDED PRECEDING
        ) AS DOUBLE
    ) / NULLIF(
        CAST(
            SUM(weekly_total_orders) OVER (
                PARTITION BY category
                ORDER BY
                    week_start RANGE UNBOUNDED PRECEDING
            ) AS DOUBLE
        ),
        0
    ) AS cumulative_cancellation_rate,
    SUM(weekly_cancelled_revenue) OVER (
        PARTITION BY category
        ORDER BY
            week_start RANGE UNBOUNDED PRECEDING
    ) AS cumulative_cancelled_revenue,
    CAST(
        SUM(weekly_cancelled_orders) OVER (
            PARTITION BY category
            ORDER BY
                week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING
                AND CURRENT ROW
        ) AS DOUBLE
    ) / NULLIF(
        CAST(
            SUM(weekly_total_orders) OVER (
                PARTITION BY category
                ORDER BY
                    week_start RANGE BETWEEN INTERVAL 3 WEEKS PRECEDING
                    AND CURRENT ROW
            ) AS DOUBLE
        ),
        0
    ) AS cancellation_rate_4wk
FROM
    (
        SELECT
            oi.category,
            (
                DATE_TRUNC(oi.order_created_at, DAY) - (
                    CAST(
                        EXTRACT(
                            ISODOW
                            FROM
                                oi.order_created_at
                        ) AS INTEGER
                    ) - 1
                ) * INTERVAL '1' DAY
            ) AS week_start,
            COUNT(
                DISTINCT CASE
                    WHEN oi.order_status = 'cancelled' THEN oi.order_id
                END
            ) AS weekly_cancelled_orders,
            COUNT(DISTINCT oi.order_id) AS weekly_total_orders,
            SUM(
                CASE
                    WHEN oi.order_status = 'cancelled' THEN oi.line_net_revenue
                    ELSE 0
                END
            ) AS weekly_cancelled_revenue,
            SUM(oi.line_net_revenue) AS weekly_total_revenue
        FROM
            silver_order_items_enriched oi
        GROUP BY
            oi.category,
            (
                DATE_TRUNC(oi.order_created_at, DAY) - (
                    CAST(
                        EXTRACT(
                            ISODOW
                            FROM
                                oi.order_created_at
                        ) AS INTEGER
                    ) - 1
                ) * INTERVAL '1' DAY
            )
    );

-- ----------------------------------------------------------------------------
-- gold_realtime_inventory_alerts
-- Filtered view of CRITICAL inventory items. Products enter and leave this
-- view as stock is reserved (sale_reserve) or restored (cancellation_restock).
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_realtime_inventory_alerts AS
SELECT
    *
FROM
    gold_inventory_risk
WHERE
    stock_risk_level = 'CRITICAL';

-- ----------------------------------------------------------------------------
-- gold_product_demand_surge
-- Stockout-risk signal driven by RECENT browsing demand -- a leading indicator
-- that moves before sales do. Demand is measured over a trailing 1-day window
-- relative to the latest event in the stream (MAX(event_timestamp), not
-- wall-clock NOW(), which would be empty on a historical snapshot); the window
-- slides as events arrive, so products enter AND leave the alert in real time.
-- Risk is framed like gold_inventory_risk -- stock cover vs supplier lead time --
-- but fed by clickstream cart velocity rather than confirmed sales. Sourced
-- entirely from silver so gold never touches bronze.
--
-- Heuristic: clickstream carries no quantity, so one add-to-cart is treated as
-- ~one unit of demand; lead_time_days defaults to 7 when unknown.
-- ----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW gold_product_demand_surge AS
WITH recent_demand AS (
    -- AGGREGATE-THEN-JOIN: collapse to product grain FIRST with pure SUM counts
    -- (no MAX of descriptive attributes), then attach product attributes via a
    -- product-grain join below. This keeps the dimension join off the event-grain
    -- hot path and drops the three MAX(string) aggregates -- the costly part under
    -- windowed eviction. Window is relative to the latest event in the stream
    -- (uncorrelated scalar subquery; the global MAX is inherently broadcast
    -- against every event no matter how it's written).
    SELECT
        ce.product_id,
        SUM(CASE WHEN ce.event_type = 'product_view' THEN 1 ELSE 0 END) AS recent_views,
        SUM(CASE WHEN ce.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS recent_add_to_carts,
        SUM(CASE WHEN ce.event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS recent_checkouts
    FROM
        silver_enriched_clickstream ce
    WHERE
        ce.product_id IS NOT NULL
        AND ce.event_timestamp >
            (SELECT MAX(event_timestamp) FROM silver_enriched_clickstream) - INTERVAL '1' DAY
    GROUP BY
        ce.product_id
),
stock_by_product AS (
    SELECT
        product_id,
        SUM(current_stock) AS total_stock_all_warehouses,
        MAX(lead_time_days) AS lead_time_days
    FROM
        silver_inventory_current
    GROUP BY
        product_id
)
SELECT
    d.product_id,
    p.product_name,
    p.category AS product_category,
    p.brand AS product_brand,
    d.recent_views,
    d.recent_add_to_carts,
    d.recent_checkouts,
    COALESCE(s.total_stock_all_warehouses, 0) AS total_stock_all_warehouses,
    s.lead_time_days,
    -- Days of stock cover at the current daily add-to-cart rate (1-day window
    -- => recent_add_to_carts is a per-day rate). Stock clamped at >= 0.
    CAST(GREATEST(COALESCE(s.total_stock_all_warehouses, 0), 0) AS DOUBLE) / NULLIF(CAST(d.recent_add_to_carts AS DOUBLE), 0) AS days_of_stock_cover,
    -- DEMO-ONLY: valid because the generator attaches one product_id per funnel
    -- journey; in production begin_checkout is cart-level with no product_id.
    CAST(d.recent_checkouts AS DOUBLE) / NULLIF(CAST(d.recent_views AS DOUBLE), 0) AS view_to_checkout_rate,
    -- Stockout risk: stock cover shorter than a supplier restock cycle.
    CASE
        WHEN d.recent_add_to_carts = 0 THEN 'OK'
        WHEN COALESCE(s.total_stock_all_warehouses, 0) <= 0 THEN 'SURGE_STOCKOUT_RISK'
        WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE) < COALESCE(s.lead_time_days, 7) THEN 'SURGE_STOCKOUT_RISK'
        WHEN CAST(GREATEST(s.total_stock_all_warehouses, 0) AS DOUBLE) / CAST(d.recent_add_to_carts AS DOUBLE) < COALESCE(s.lead_time_days, 7) * 2 THEN 'WARNING'
        ELSE 'OK'
    END AS demand_alert
FROM
    recent_demand d
    LEFT JOIN silver_products p ON d.product_id = p.product_id
    LEFT JOIN stock_by_product s ON d.product_id = s.product_id;