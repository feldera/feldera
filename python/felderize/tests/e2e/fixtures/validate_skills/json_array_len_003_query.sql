-- rule: json_array_len
-- spark: json_array_length(json_str) — length of a JSON array string
-- feldera: CARDINALITY(CAST(PARSE_JSON(json_str) AS VARIANT ARRAY))
CREATE OR REPLACE TEMP VIEW survey_responses_v3 AS SELECT response_id, json_array_length(answers) AS answer_count, CASE WHEN json_array_length(answers) >= 5 THEN 'complete' ELSE 'incomplete' END AS status FROM survey_responses;
