-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE OR REPLACE TEMP VIEW top_earners_v1 AS SELECT emp_id, department, salary FROM (SELECT emp_id, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank FROM sales_dept_v1) sub WHERE sub.salary_rank <= 2;
