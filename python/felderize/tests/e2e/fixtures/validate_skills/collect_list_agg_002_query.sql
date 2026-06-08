-- rule: collect_list_agg
-- spark: collect_list(col) — aggregate all column values per group into an array (preserves duplicates)
-- feldera: ARRAY_AGG(col) — order may differ when source collection is unordered
CREATE OR REPLACE TEMP VIEW dept_employee_names_v2 AS SELECT dept_code, collect_list(employee_name) as team_members FROM department_employees GROUP BY dept_code;
