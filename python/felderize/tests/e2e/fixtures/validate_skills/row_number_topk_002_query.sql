-- rule: row_number_topk
-- spark: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) — must be used in TopK subquery pattern
-- feldera: ROW_NUMBER() same; wrap in subquery with WHERE rn <= N filter
CREATE OR REPLACE TEMP VIEW top_performers_per_dept AS
SELECT emp_id, department, score, eval_date
FROM (
  SELECT emp_id, department, score, eval_date,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY score DESC, eval_date DESC) as rn
  FROM employee_scores
)
WHERE rn <= 3;
