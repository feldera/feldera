-- rule: equal_null
-- spark: equal_null(a, b) — null-safe equality returning true when both args are NULL
-- feldera: a <=> b — same semantics, Feldera supports <=> operator
CREATE OR REPLACE TEMP VIEW supervisor_check_v3 AS SELECT emp_id, supervisor_id, manager_id, (supervisor_id <=> manager_id) AS same_supervisor FROM employee_review;
