-- rule: lateral_view_from_json_array_map
-- spark: LATERAL VIEW explode(from_json(col, 'array<map<string,string>>')) AS m — explode a JSON array of string-to-string maps; each row receives one map m
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR> ARRAY)) AS t(m) — cast JSON array of maps to typed array then UNNEST; access values via m['key']
CREATE TABLE sensor_readings_3 (sensor_id INT, location STRING, readings STRING);
