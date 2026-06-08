-- rule: dense_rank_topk
-- spark: DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...) in TopK pattern — must be in subquery with WHERE filter
-- feldera: DENSE_RANK() same; wrap in subquery with WHERE dr <= N filter
CREATE OR REPLACE TEMP VIEW top_performers_v2 AS
SELECT department, employee_name, score, year
FROM (
  SELECT department, employee_name, score, year,
         DENSE_RANK() OVER (PARTITION BY department ORDER BY score DESC) AS dr
  FROM employee_scores
)
WHERE dr <= 3;
