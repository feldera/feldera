-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE TABLE employee_stats (emp_id INT, salary DECIMAL(10,2), department STRING);
