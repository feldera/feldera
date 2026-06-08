-- rule: grouping_id
-- spark: grouping_id(col, ...) — bitmask identifying which columns are aggregated
-- feldera: grouping_id(col, ...) — same
CREATE TABLE dept_salaries_2 (department STRING, job_title STRING, salary DECIMAL(10,2));
