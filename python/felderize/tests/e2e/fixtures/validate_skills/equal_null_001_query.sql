-- rule: equal_null
-- spark: equal_null(a, b) — null-safe equality returning true when both args are NULL
-- feldera: a <=> b — same semantics, Feldera supports <=> operator
CREATE OR REPLACE TEMP VIEW user_comparison_v1 AS SELECT id, name, email, (name <=> email) AS names_equal FROM users_t1;
