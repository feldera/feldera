-- rule: first_last_agg
-- spark: first(col) / last(col) — return first or last value in a group
-- feldera: MAX(col) — Feldera has no first()/last() aggregate; use MAX(col) as an approximation
CREATE OR REPLACE TEMP VIEW sales_summary AS
SELECT
  region,
  first(amount) AS first_amount,
  last(amount) AS last_amount
FROM sales_log
GROUP BY region;
