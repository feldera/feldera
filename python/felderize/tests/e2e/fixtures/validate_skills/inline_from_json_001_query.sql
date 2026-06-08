-- rule: inline_from_json
-- spark: LATERAL VIEW inline(array(from_json(col, 'f1 T1, f2 T2'))) AS (f1, f2) — extract struct fields from a JSON string column using inline/from_json row expansion
-- feldera: CAST(PARSE_JSON(col)['f1'] AS T1) AS f1, CAST(PARSE_JSON(col)['f2'] AS T2) AS f2 — drop the LATERAL VIEW; extract each field directly from the JSON via PARSE_JSON and CAST
CREATE OR REPLACE TEMP VIEW user_info_v1 AS SELECT user_id, name, age FROM user_profiles_1 LATERAL VIEW inline(array(from_json(profile_json, 'name STRING, age INT'))) AS (name, age);
