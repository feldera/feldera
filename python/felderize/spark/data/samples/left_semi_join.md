---
categories: [comparisons]
---

# LEFT SEMI JOIN

Feldera does not support LEFT SEMI JOIN. Rewrite as INNER JOIN with DISTINCT.
Any filtering on right-table columns belongs in the ON condition to preserve semi-join semantics — only left-side rows are returned.

Spark:
```sql
CREATE TABLE customer_dimension (
  customer_id BIGINT, customer_name VARCHAR, segment VARCHAR, region VARCHAR
) USING parquet;

CREATE TABLE order_facts (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), status VARCHAR
) USING parquet;

SELECT c.customer_id, c.customer_name, c.segment
FROM customer_dimension c
LEFT SEMI JOIN order_facts o
  ON c.customer_id = o.customer_id AND o.amount >= 500;
```

Feldera:
```sql
CREATE TABLE customer_dimension (
  customer_id BIGINT, customer_name VARCHAR, segment VARCHAR, region VARCHAR
);

CREATE TABLE order_facts (
  order_id BIGINT, customer_id BIGINT, amount DECIMAL(12,2), status VARCHAR
);

-- NOTE: LEFT SEMI JOIN → INNER JOIN + DISTINCT.
-- Right-table filters stay in the ON clause; DISTINCT ensures only left-side columns are returned.
SELECT DISTINCT c.customer_id, c.customer_name, c.segment
FROM customer_dimension c
INNER JOIN order_facts o
  ON c.customer_id = o.customer_id AND o.amount >= 500;
```
