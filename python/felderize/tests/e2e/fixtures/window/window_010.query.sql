CREATE VIEW window_010 AS
SELECT
	employee_name,
	department,
	salary,
	FIRST_VALUE(employee_name) OVER w highest_salary,
	NTH_VALUE(employee_name, 2) OVER w second_highest_salary
FROM
	basic_pays
WINDOW w AS (
  PARTITION BY department
  ORDER BY salary DESC
  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY department;
