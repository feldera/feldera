-- rule: bool_and_window
-- spark: every(bool_col) OVER (ORDER BY bool_col) / bool_and(bool_col) OVER (ORDER BY bool_col) — boolean aggregate used as window function with ORDER BY on a BOOLEAN column
-- feldera: UNSUPPORTED — Feldera compiler error: 'OVER currently cannot sort on columns with type BOOL'. Mark as unsupported when the window ORDER BY is on a BOOLEAN column.
CREATE OR REPLACE TEMP VIEW status_log_v2 AS
SELECT
  log_id,
  is_valid,
  priority,
  bool_and(is_valid) OVER (ORDER BY is_valid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS all_valid_window
FROM status_log
ORDER BY log_id;
