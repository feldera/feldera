---
categories: [math]
---

# pmod, try_divide, try_subtract

Three arithmetic patterns with divergent NULL/overflow behavior between Spark and Feldera.

- `pmod(a, b)`: Spark's "positive modulo" — result always has the sign of the divisor.
  Feldera's `MOD` follows C-style (sign of dividend), so rewrite with CASE WHEN.
  With a positive divisor the result is always ≥ 0; with a negative divisor the
  result has the same sign as the dividend.

- `try_divide(a, b)`: Spark always returns DOUBLE and returns NULL on divide-by-zero.
  Feldera integer division returns INT and **drops the row** on divide-by-zero —
  not safely rewritable in general. The CASE WHEN rewrite below approximates Spark
  behavior only for integer dividends where NULL-on-zero is acceptable and the
  DOUBLE return type is not required.

- `try_subtract(a, b)`: Spark returns NULL on integer overflow. Feldera also drops
  the row on overflow for most numeric types, but for types supported natively
  (BIGINT etc.) overflow behavior differs. Translates directly as `a - b` — flag
  if overflow protection is critical to business logic.

Spark:
```sql
CREATE TABLE metrics (
  metric_id  BIGINT,
  value      BIGINT,
  bucket     BIGINT,
  divisor    BIGINT,
  baseline   BIGINT
) USING parquet;

SELECT
  metric_id,
  pmod(value, bucket)            AS bucketed,
  try_divide(value, divisor)     AS safe_ratio,
  try_subtract(value, baseline)  AS delta
FROM metrics;
```

Feldera:
```sql
CREATE TABLE metrics (
  metric_id  BIGINT,
  value      BIGINT,
  bucket     BIGINT,
  divisor    BIGINT,
  baseline   BIGINT
);

-- NOTE: pmod → CASE WHEN to ensure non-negative result with positive divisor.
-- NOTE: try_divide → CASE WHEN b = 0 THEN NULL approximation; Spark returns DOUBLE,
--       this rewrite returns BIGINT. If DOUBLE output is required, add CAST(value AS DOUBLE).
-- NOTE: try_subtract → direct subtraction; Feldera drops rows on overflow rather than
--       returning NULL. Flag if overflow safety is critical.
SELECT
  metric_id,
  CASE WHEN MOD(value, bucket) < 0 AND bucket > 0
       THEN MOD(value, bucket) + bucket
       ELSE MOD(value, bucket)
  END                                              AS bucketed,
  CASE WHEN divisor = 0 THEN NULL
       ELSE value / divisor
  END                                              AS safe_ratio,
  value - baseline                                 AS delta
FROM metrics;
```
