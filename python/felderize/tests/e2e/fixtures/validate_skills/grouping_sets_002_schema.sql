-- rule: grouping_sets
-- spark: GROUPING SETS((a,b),(a),(b),()) — multi-level grouping
-- feldera: GROUPING SETS — same syntax, supported in Feldera
CREATE TABLE employee_metrics_v2 (department STRING, job_title STRING, salary DECIMAL(10,2), years_employed INT);
