-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE OR REPLACE TEMP VIEW anti_join_result_v1 AS
SELECT e.emp_id, e.emp_name, e.dept_id
FROM employees_t1 e
LEFT ANTI JOIN excluded_depts d ON e.dept_id = d.dept_id;
