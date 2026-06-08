-- rule: select_no_from
-- spark: SELECT expr GROUP BY ... HAVING ... with no FROM clause
-- feldera: Add dummy FROM: FROM (VALUES (1)) AS t(x)
CREATE TABLE employee_shifts (emp_id INT, hours_worked DOUBLE, shift_date DATE);
