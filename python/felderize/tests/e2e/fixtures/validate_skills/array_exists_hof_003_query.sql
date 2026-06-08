-- rule: array_exists_hof
-- spark: exists(arr, x -> expr) — true if any element in array satisfies the predicate
-- feldera: ARRAY_EXISTS(arr, x -> expr) — Feldera uses ARRAY_EXISTS instead of exists
CREATE OR REPLACE TEMP VIEW user_emails_view AS SELECT user_id, email_addresses FROM user_emails WHERE exists(email_addresses, x -> x LIKE '%@company.com');
