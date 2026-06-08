-- rule: trunc_date
-- spark: trunc(d, 'YYYY'/'MM'/'QUARTER'/'WEEK') — truncate date using string unit; ONLY these formats work in Spark 4.x trunc() — do NOT use 'Q', 'DD', or 'DAY' as they return NULL
-- feldera: DATE_TRUNC(d, YEAR/MONTH/QUARTER/WEEK) — string unit becomes SQL keyword
CREATE OR REPLACE TEMP VIEW order_summary_v1 AS
SELECT
  trunc(order_date, 'YYYY') AS year_start,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM sales_orders
GROUP BY trunc(order_date, 'YYYY')
ORDER BY year_start;
