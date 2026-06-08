-- rule: row_number_topk
-- spark: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) — must be used in TopK subquery pattern
-- feldera: ROW_NUMBER() same; wrap in subquery with WHERE rn <= N filter
CREATE OR REPLACE TEMP VIEW top_products_by_category AS
SELECT transaction_id, category, product_name, quantity, revenue
FROM (
  SELECT transaction_id, category, product_name, quantity, revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) as rn
  FROM product_sales
)
WHERE rn <= 1;
