-- rule: equal_null
-- spark: equal_null(a, b) — null-safe equality returning true when both args are NULL
-- feldera: a <=> b — same semantics, Feldera supports <=> operator
CREATE TABLE employee_review (emp_id INT, supervisor_id INT, manager_id INT);
