---
categories: [datetime, aggregates]
---

# Aggregation + date_trunc

Spark:
```sql
CREATE TABLE orders (
  order_id BIGINT, customer_id BIGINT, region STRING,
  amount DECIMAL(12,2), status STRING, created_at TIMESTAMP
) USING parquet;

CREATE OR REPLACE TEMP VIEW bm01_revenue_by_region_month AS
SELECT region, date_trunc('MONTH', created_at) AS order_month,
  COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM orders WHERE status IN ('PAID', 'SHIPPED')
GROUP BY region, date_trunc('MONTH', created_at);
```

Feldera:
```sql
CREATE TABLE orders (
  order_id BIGINT, customer_id BIGINT, region VARCHAR,
  amount DECIMAL(12,2), status VARCHAR, created_at TIMESTAMP
);

CREATE VIEW bm01_revenue_by_region_month AS
SELECT region, TIMESTAMP_TRUNC(created_at, MONTH) AS order_month,
  COUNT(*) AS order_count, SUM(amount) AS total_amount
FROM orders WHERE status IN ('PAID', 'SHIPPED')
GROUP BY region, TIMESTAMP_TRUNC(created_at, MONTH);
```

