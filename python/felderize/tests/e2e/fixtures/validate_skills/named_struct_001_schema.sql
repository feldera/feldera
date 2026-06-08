-- rule: named_struct
-- spark: named_struct('a', v1, 'b', v2) — create a named struct
-- feldera: CAST(ROW(v1, v2) AS ROW(a T, b S)) — use CAST to assign field names
CREATE TABLE employee_info (emp_id INT, first_name STRING, last_name STRING, salary INT);
