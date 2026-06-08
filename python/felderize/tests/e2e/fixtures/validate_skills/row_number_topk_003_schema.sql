-- rule: row_number_topk
-- spark: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) — must be used in TopK subquery pattern
-- feldera: ROW_NUMBER() same; wrap in subquery with WHERE rn <= N filter
CREATE TABLE product_sales (
  transaction_id BIGINT,
  category STRING,
  product_name STRING,
  quantity INT,
  revenue DECIMAL(12,2)
);
