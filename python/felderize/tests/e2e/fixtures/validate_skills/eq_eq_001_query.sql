-- rule: eq_eq
-- spark: a == b — double-equals equality operator (Spark allows this)
-- feldera: a = b — use single = in Feldera
CREATE OR REPLACE TEMP VIEW user_status_v1 AS SELECT user_id, status, age FROM users_t1 WHERE status == 'active' AND age == 25;
