-- rule: range_between_window
-- spark: SUM(col) OVER (PARTITION BY ... ORDER BY ... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) — running sum with RANGE frame
-- feldera: Same — RANGE BETWEEN is fully supported in Feldera
CREATE TABLE sales_data (product_id INT, sale_date DATE, amount DECIMAL(10,2));
CREATE TABLE expected_output (product_id INT, sale_date DATE, amount DECIMAL(10,2), running_total DECIMAL(10,2));
