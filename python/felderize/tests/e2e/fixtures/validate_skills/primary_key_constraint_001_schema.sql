-- rule: primary_key_constraint
-- spark: CONSTRAINT pk_name PRIMARY KEY (col) — named constraint syntax
-- feldera: PRIMARY KEY (col) — drop the CONSTRAINT name wrapper; also ensure all PK columns are NOT NULL
CREATE TABLE users_tab (
  user_id INT NOT NULL,
  username STRING NOT NULL,
  email STRING,
  created_date DATE,
  CONSTRAINT pk_users PRIMARY KEY (user_id)
);
