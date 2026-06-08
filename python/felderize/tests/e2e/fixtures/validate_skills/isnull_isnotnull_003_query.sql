-- rule: isnull_isnotnull
-- spark: isnull(x) and isnotnull(x) — null check functions
-- feldera: x IS NULL  /  x IS NOT NULL
CREATE OR REPLACE TEMP VIEW feedback_validation_v3 AS SELECT feedback_id, customer_name, CASE WHEN isnotnull(email) THEN 'Valid' ELSE 'Missing' END as email_status, CASE WHEN isnull(rating) THEN 0 ELSE rating END as feedback_rating, CASE WHEN isnotnull(comments) AND isnotnull(rating) THEN 'Complete' ELSE 'Incomplete' END as feedback_completeness FROM customer_feedback;
