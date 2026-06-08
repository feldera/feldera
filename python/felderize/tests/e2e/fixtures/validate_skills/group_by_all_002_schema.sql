-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE TABLE employee_shifts_v2 (emp_name STRING, department STRING, shift_date DATE, hours_worked INT);
