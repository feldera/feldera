-- rule: rollup
-- spark: ROLLUP(a, b) — hierarchical grouping
-- feldera: ROLLUP(a, b) — same
CREATE TABLE department_salary_v2 (dept STRING, team STRING, salary DECIMAL(12,2), employee_id INT);
