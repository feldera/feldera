---
categories: [window]
---

# Window functions

Most window functions translate directly. Key constraints in Feldera:
- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()` are only supported in a **TopK pattern**:
  the ranked subquery must have an outer WHERE that filters on the rank column.
  Without the outer filter these are unsupported.
- `ROWS BETWEEN` / `RANGE BETWEEN` frame clauses are **not supported** — Feldera
  only supports whole-partition (unbounded) windows.
- `LAG`, `LEAD`, `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` over whole partitions work
  identically and translate directly.

Spark:
```sql
CREATE TABLE sales (
  sale_id    BIGINT,
  rep_id     BIGINT,
  region     VARCHAR,
  sale_date  DATE,
  amount     DECIMAL(12,2)
) USING parquet;

-- ROW_NUMBER TopK: top 3 sales per region
SELECT sale_id, rep_id, region, amount
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
  FROM sales
)
WHERE rn <= 3;

-- LAG / LEAD: compare each sale to the previous and next in time per rep
SELECT
  sale_id,
  rep_id,
  amount,
  LAG(amount,  1, 0) OVER (PARTITION BY rep_id ORDER BY sale_date) AS prev_amount,
  LEAD(amount, 1, 0) OVER (PARTITION BY rep_id ORDER BY sale_date) AS next_amount
FROM sales;

-- Running partition total (whole-partition SUM)
SELECT
  sale_id,
  rep_id,
  amount,
  SUM(amount) OVER (PARTITION BY rep_id) AS rep_total
FROM sales;
```

Feldera:
```sql
CREATE TABLE sales (
  sale_id    BIGINT,
  rep_id     BIGINT,
  region     VARCHAR,
  sale_date  DATE,
  amount     DECIMAL(12,2)
);

-- ROW_NUMBER TopK: outer WHERE on rank column is required.
SELECT sale_id, rep_id, region, amount
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
  FROM sales
)
WHERE rn <= 3;

-- NOTE: LAG/LEAD translate directly.
SELECT
  sale_id,
  rep_id,
  amount,
  LAG(amount,  1, 0) OVER (PARTITION BY rep_id ORDER BY sale_date) AS prev_amount,
  LEAD(amount, 1, 0) OVER (PARTITION BY rep_id ORDER BY sale_date) AS next_amount
FROM sales;

-- NOTE: Whole-partition SUM translates directly. ROWS/RANGE frames are unsupported.
SELECT
  sale_id,
  rep_id,
  amount,
  SUM(amount) OVER (PARTITION BY rep_id) AS rep_total
FROM sales;
```
