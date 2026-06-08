-- rule: rank_topk
-- spark: RANK() OVER (PARTITION BY ... ORDER BY ...) — same TopK restriction
-- feldera: RANK() same; must be in subquery with WHERE filter on result
CREATE TABLE sales_dept_v1 (emp_id INT, department STRING, salary INT, hire_date DATE);
