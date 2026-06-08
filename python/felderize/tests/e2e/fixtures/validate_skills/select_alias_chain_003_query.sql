-- rule: select_alias_chain
-- spark: SELECT col1 AS a, a + 1 AS b FROM t — reference an earlier SELECT alias in a later expression
-- feldera: Expand the alias reference: SELECT col1 AS a, col1 + 1 AS b FROM t — replace alias references with the original expression
CREATE OR REPLACE TEMP VIEW payroll_v3 AS SELECT emp_id, employee_name, hours_worked AS hrs, hrs * hourly_rate AS gross_pay, gross_pay * 0.85 AS net_pay FROM employee_hours;
