-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE OR REPLACE TEMP VIEW dept_job_analysis_v2 AS SELECT department, job_title, SUM(salary) as total_salary, COUNT(*) as emp_count FROM employee_metrics_v2 GROUP BY GROUPING SETS((department, job_title), (department), (job_title), ());
