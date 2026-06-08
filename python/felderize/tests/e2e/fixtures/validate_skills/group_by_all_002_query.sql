-- rule: group_by_all
-- spark: GROUP BY ALL — automatically group by all non-aggregate expressions
-- feldera: Expand to explicit column list: GROUP BY col1, col2, ... (all non-aggregate SELECT expressions)
CREATE OR REPLACE TEMP VIEW shift_totals_v2 AS SELECT emp_name, department, shift_date, COUNT(*) as shift_count, SUM(hours_worked) as total_hours FROM employee_shifts_v2 GROUP BY ALL;
