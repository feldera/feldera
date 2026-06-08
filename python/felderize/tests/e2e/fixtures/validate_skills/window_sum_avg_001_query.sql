-- rule: window_sum_avg
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ...) / AVG(col) OVER (...) — running aggregate window functions
-- feldera: SUM(col) OVER (...) / AVG(CAST(col AS DOUBLE)) OVER (...) — same window syntax; note AVG on integer input needs CAST to return DOUBLE
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT sale_id, dept, amount, SUM(amount) OVER (PARTITION BY dept ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total, AVG(amount) OVER (PARTITION BY dept ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg FROM sales_data;
