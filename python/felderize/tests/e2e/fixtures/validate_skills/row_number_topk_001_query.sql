-- rule: row_number_topk
-- spark: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) — must be used in TopK subquery pattern
-- feldera: ROW_NUMBER() same; wrap in subquery with WHERE rn <= N filter
CREATE OR REPLACE TEMP VIEW top_sales_by_region AS
SELECT sale_id, region, amount, sale_date
FROM (
  SELECT sale_id, region, amount, sale_date,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn
  FROM sales_data
)
WHERE rn <= 2;
