-- rule: window_sum_avg
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ...) / AVG(col) OVER (...) — running aggregate window functions
-- feldera: SUM(col) OVER (...) / AVG(CAST(col AS DOUBLE)) OVER (...) — same window syntax; note AVG on integer input needs CAST to return DOUBLE
CREATE TABLE sales_data (sale_id INT, dept STRING, amount INT, sale_date DATE);
