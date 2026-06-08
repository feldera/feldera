-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE OR REPLACE TEMP VIEW dept_team_summary_v2 AS SELECT dept, team, COUNT(employee_id) as emp_count, ROUND(AVG(salary), 2) as avg_salary FROM department_salary_v2 GROUP BY ROLLUP(dept, team) ORDER BY dept, team;
