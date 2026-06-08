-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE TABLE sales_data_v1 (region STRING, product STRING, amount DECIMAL(10,2), sale_date DATE);
