-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE OR REPLACE TEMP VIEW users_view AS SELECT user_id, username, email, created_date FROM users_tab WHERE user_id > 0;
