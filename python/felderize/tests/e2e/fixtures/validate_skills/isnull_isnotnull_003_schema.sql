-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE TABLE customer_feedback (feedback_id INT, customer_name STRING, email STRING, rating INT, comments STRING);
