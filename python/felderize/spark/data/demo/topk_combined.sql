-- Demo: TopK pattern, QUALIFY clause, and datediff
-- Covers: ROW_NUMBER in subquery (TopK), QUALIFY, datediff → DATEDIFF(unit, start, end)

CREATE TABLE IF NOT EXISTS employee (
  emp_id STRING NOT NULL,
  dept STRING,
  salary DECIMAL(12, 2),
  hire_date TIMESTAMP,
  CONSTRAINT employee_pk PRIMARY KEY (emp_id)
)
USING DELTA;

CREATE TABLE IF NOT EXISTS review (
  emp_id STRING NOT NULL,
  review_date TIMESTAMP NOT NULL,
  score INT,
  CONSTRAINT review_pk PRIMARY KEY (emp_id, review_date)
)
USING DELTA;

-- Top 3 earners per department using ROW_NUMBER subquery (TopK pattern)
CREATE OR REPLACE TEMP VIEW top_earners_per_dept AS
SELECT dept, emp_id, salary, rank_in_dept
FROM (
  SELECT
    dept,
    emp_id,
    salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank_in_dept
  FROM employee
) ranked
WHERE rank_in_dept <= 3;

-- Latest review per employee using QUALIFY
CREATE OR REPLACE TEMP VIEW latest_review AS
SELECT
  emp_id,
  review_date,
  score
FROM review
QUALIFY ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY review_date DESC) = 1;

-- Employee tenure in years using datediff
CREATE OR REPLACE TEMP VIEW employee_tenure AS
SELECT
  emp_id,
  dept,
  hire_date,
  datediff(CURRENT_DATE, hire_date) AS tenure_days
FROM employee;
