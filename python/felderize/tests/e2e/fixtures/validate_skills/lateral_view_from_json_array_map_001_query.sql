-- rule: lateral_view_from_json_array_map
-- spark: LATERAL VIEW explode(from_json(col, 'array<map<string,string>>')) AS m — explode a JSON array of string-to-string maps; each row receives one map m
-- feldera: CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR> ARRAY)) AS t(m) — cast JSON array of maps to typed array then UNNEST; access values via m['key']
CREATE OR REPLACE TEMP VIEW page_actions_v1 AS SELECT page_id, page_name, m['action'] AS action, m['user'] AS user FROM page_events_1 LATERAL VIEW explode(from_json(event_data, 'array<map<string,string>>')) AS m;
