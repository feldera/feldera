-- rule: functional_cast
-- spark: bigint(expr) / string(expr) / int(expr) / double(expr) / boolean(expr) / date(str) / timestamp(date_expr) — Spark type-name-as-function casting syntax
-- feldera: CAST(expr AS BIGINT) / CAST(expr AS VARCHAR) / CAST(expr AS INT) / CAST(expr AS DOUBLE) / CAST(expr AS BOOLEAN) / DATE 'yyyy-MM-dd' or CAST(str AS DATE) / CAST(date_expr AS TIMESTAMP) — use standard SQL CAST; Feldera does not support function-call cast syntax
CREATE OR REPLACE TEMP VIEW cast_results_v3 AS
SELECT
  txn_id,
  int(amount_str) AS amount_int,
  double(rate_str) AS exchange_rate,
  string(txn_id) AS txn_id_str,
  boolean(status_str) AS is_processed,
  double(base_rate) AS base_rate_double
FROM transaction_log_003
LEFT JOIN rate_master_003 ON txn_id = 1;
