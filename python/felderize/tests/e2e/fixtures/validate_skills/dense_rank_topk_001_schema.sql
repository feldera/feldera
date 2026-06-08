-- rule: dense_rank_topk
-- spark: DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in TopK pattern — must be in subquery with WHERE filter
-- feldera: DENSE_RANK() same; wrap in subquery with WHERE dr <= N filter
CREATE TABLE sales_records (
  region STRING,
  product STRING,
  revenue DECIMAL(10,2),
  quarter INT
);

CREATE TABLE regions (
  region_name STRING,
  country STRING
);
