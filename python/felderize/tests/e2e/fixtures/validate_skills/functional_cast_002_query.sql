-- rule: functional_cast
-- spark: bigint(expr) / string(expr) / int(expr) / double(expr) / boolean(expr) / date(str) / timestamp(date_expr) — Spark type-name-as-function casting syntax
-- feldera: CAST(expr AS BIGINT) / CAST(expr AS VARCHAR) / CAST(expr AS INT) / CAST(expr AS DOUBLE) / CAST(expr AS BOOLEAN) / DATE 'yyyy-MM-dd' or CAST(str AS DATE) / CAST(date_expr AS TIMESTAMP) — use standard SQL CAST; Feldera does not support function-call cast syntax
CREATE OR REPLACE TEMP VIEW cast_results_v2 AS
SELECT
  customer_id,
  bigint(revenue_str) AS total_revenue,
  double(multiplier_str) AS multiplied_value,
  string(customer_id) AS cust_id_string,
  boolean(flag_str) AS is_flagged
FROM customer_metrics_002
LEFT JOIN conversion_002 ON customer_id = 1;
