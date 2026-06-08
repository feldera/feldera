-- rule: functional_cast
-- spark: bigint(expr) / string(expr) / int(expr) / double(expr) / boolean(expr) / date(str) / timestamp(date_expr) — Spark type-name-as-function casting syntax
-- feldera: CAST(expr AS BIGINT) / CAST(expr AS VARCHAR) / CAST(expr AS INT) / CAST(expr AS DOUBLE) / CAST(expr AS BOOLEAN) / DATE 'yyyy-MM-dd' or CAST(str AS DATE) / CAST(date_expr AS TIMESTAMP) — use standard SQL CAST; Feldera does not support function-call cast syntax
CREATE TABLE customer_metrics_002 (
  customer_id INT,
  revenue_str STRING,
  date_str STRING,
  flag_str STRING
);

CREATE TABLE conversion_002 (
  value_str STRING,
  multiplier_str STRING
);
