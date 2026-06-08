-- rule: row_number_topk
-- spark: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) — must be used in TopK subquery pattern
-- feldera: ROW_NUMBER() same; wrap in subquery with WHERE rn <= N filter
CREATE TABLE sales_data (
  sale_id INT,
  region STRING,
  amount DECIMAL(10,2),
  sale_date DATE
);

CREATE TABLE regions (
  region_name STRING,
  country STRING
);
