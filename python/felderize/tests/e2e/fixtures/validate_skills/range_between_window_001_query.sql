-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE OR REPLACE TEMP VIEW sales_summary_v1 AS SELECT product_id, sale_date, amount, SUM(amount) OVER (PARTITION BY product_id ORDER BY sale_date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total FROM sales_data;
