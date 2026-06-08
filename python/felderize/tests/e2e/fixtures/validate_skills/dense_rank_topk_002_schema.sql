-- rule: dense_rank_topk
-- spark: DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in TopK pattern — must be in subquery with WHERE filter
-- feldera: DENSE_RANK() same; wrap in subquery with WHERE dr <= N filter
CREATE TABLE employee_scores (
  department STRING,
  employee_name STRING,
  score INT,
  year INT
);
