-- rule: lateral_view_from_json_array_map
-- spark: LATERAL VIEW explode(from_json(col, 'array<map<string,string>>')) AS m — explode a JSON array of string-to-string maps; each row receives one map m
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR> ARRAY)) AS t(m) — cast JSON array of maps to typed array then UNNEST; access values via m['key']
CREATE OR REPLACE TEMP VIEW log_attrs_v2 AS SELECT log_id, log_type, m['key'] AS attr_key, m['value'] AS attr_value FROM log_entries_2 LATERAL VIEW explode(from_json(attributes, 'array<map<string,string>>')) AS m WHERE log_id > 1;
