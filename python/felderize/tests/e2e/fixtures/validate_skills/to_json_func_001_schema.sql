-- rule: to_json_func
-- spark: to_json(v) — serialize a struct/map/array value to a JSON string
-- feldera: TO_JSON(v) — same function, supported directly in Feldera
CREATE TABLE users_t1 (
  id INT,
  name STRING,
  age INT
);
