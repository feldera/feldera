-- rule: equal_null
-- spark: equal_null(a, b) — null-safe equality returning true when both args are NULL
-- feldera: a <=> b — same semantics, Feldera supports <=> operator
CREATE TABLE users_t1 (id INT, name STRING, email STRING);
