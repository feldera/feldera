-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE OR REPLACE TEMP VIEW dept_agg_v2 AS SELECT department, job_title, COUNT(*) as emp_count, AVG(salary) as avg_salary, grouping_id(department, job_title) as grouping_mask FROM dept_salaries_2 GROUP BY CUBE(department, job_title);
