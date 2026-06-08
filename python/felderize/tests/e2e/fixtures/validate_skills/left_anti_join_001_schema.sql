-- rule: left_anti_join
-- spark: LEFT ANTI JOIN t2 ON cond — rows in t1 with no match in t2
-- feldera: WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE cond)
CREATE TABLE employees_t1 (emp_id INT, emp_name STRING, dept_id INT);
CREATE TABLE excluded_depts (dept_id INT);
