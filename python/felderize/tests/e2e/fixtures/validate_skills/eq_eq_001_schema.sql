-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE TABLE users_t1 (user_id INT, status STRING, age INT);
