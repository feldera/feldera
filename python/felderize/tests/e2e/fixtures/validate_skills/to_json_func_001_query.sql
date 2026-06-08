-- rule: to_json_func
-- spark: to_json(v) — serialize a struct/map/array value to a JSON string
-- feldera: TO_JSON(v) — same function, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW json_output_v1 AS SELECT id, name, to_json(named_struct('name', name, 'age', age, 'user_id', id)) AS user_json FROM users_t1;
