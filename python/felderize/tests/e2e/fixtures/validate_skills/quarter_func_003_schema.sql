-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE TABLE employee_reviews (emp_id INT, review_date DATE, rating INT);
