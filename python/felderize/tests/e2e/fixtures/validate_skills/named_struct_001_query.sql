-- rule: named_struct
-- spark: named_struct('a', v1, 'b', v2) — create a named struct
-- feldera: CAST(ROW(v1, v2) AS ROW(a T, b S)) — use CAST to assign field names
CREATE OR REPLACE TEMP VIEW emp_struct_v1 AS SELECT emp_id, named_struct('fname', first_name, 'lname', last_name, 'pay', salary) AS employee FROM employee_info;
