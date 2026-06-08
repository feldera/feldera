-- rule: functional_cast
-- spark: bigint(expr) / string(expr) / int(expr) / double(expr) / boolean(expr) / date(str) / timestamp(date_expr) — Spark type-name-as-function casting syntax
-- feldera: CAST(expr AS BIGINT) / CAST(expr AS VARCHAR) / CAST(expr AS INT) / CAST(expr AS DOUBLE) / CAST(expr AS BOOLEAN) / DATE 'yyyy-MM-dd' or CAST(str AS DATE) / CAST(date_expr AS TIMESTAMP) — use standard SQL CAST; Feldera does not support function-call cast syntax
CREATE OR REPLACE TEMP VIEW cast_results_v1 AS
SELECT
  id,
  bigint(amount) AS amount_bigint,
  int(quantity) AS qty_int,
  double(discount_str) AS discount_double,
  boolean(is_active) AS is_active_bool
FROM sales_data_001
LEFT JOIN price_log_001 ON id = 1;
