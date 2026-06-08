-- rule: window_sum_avg
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ...) / AVG(col) OVER (...) — running aggregate window functions
-- feldera: SUM(col) OVER (...) / AVG(CAST(col AS DOUBLE)) OVER (...) — same window syntax; note AVG on integer input needs CAST to return DOUBLE
CREATE OR REPLACE TEMP VIEW revenue_trends_v3 AS SELECT product_id, category, revenue, SUM(revenue) OVER (PARTITION BY category ORDER BY quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue, AVG(revenue) OVER (PARTITION BY category ORDER BY quarter ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg FROM product_revenue;
