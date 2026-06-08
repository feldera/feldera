CREATE VIEW window_005 AS
SELECT
    employee_name,
    salary,
    first_value(employee_name) OVER w highest_salary,
    any_value(employee_name) OVER w highest_salary,
    nth_value(employee_name, 2) OVER w second_highest_salary
FROM
    basic_pays
WINDOW w AS (ORDER BY salary DESC ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)
ORDER BY salary DESC;
