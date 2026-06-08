-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW annual_review_quarters_v3 AS SELECT emp_id, review_date, quarter(review_date) AS review_quarter, rating FROM employee_reviews WHERE quarter(review_date) IN (1, 2);
