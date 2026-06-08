-- rule: functional_cast
-- spark: bigint(expr) / string(expr) / int(expr) / double(expr) / boolean(expr) / date(str) / timestamp(date_expr) — Spark type-name-as-function casting syntax
-- feldera: CAST(expr AS BIGINT) / CAST(expr AS VARCHAR) / CAST(expr AS INT) / CAST(expr AS DOUBLE) / CAST(expr AS BOOLEAN) / DATE 'yyyy-MM-dd' or CAST(str AS DATE) / CAST(date_expr AS TIMESTAMP) — use standard SQL CAST; Feldera does not support function-call cast syntax
CREATE TABLE sales_data_001 (
  id INT,
  amount STRING,
  is_active STRING,
  quantity STRING
);

CREATE TABLE price_log_001 (
  price_str STRING,
  discount_str STRING
);
