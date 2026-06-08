CREATE OR REPLACE TEMP VIEW bm33_global_ordering_rank AS
SELECT order_id, revenue, RANK() OVER (ORDER BY revenue DESC, created_at ASC) AS global_rank
FROM global_sales;
